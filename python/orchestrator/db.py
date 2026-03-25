from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import Any

import pymysql
import pymysql.cursors

from .json_utils import json_dumps_safe


def _priority_rank(priority: str) -> int:
    normalized = priority.strip().lower()
    if normalized == "highest":
        return 400
    if normalized == "high":
        return 300
    if normalized == "medium":
        return 200
    if normalized == "normal":
        return 100
    return 50


class SupplyCoreDb:
    def __init__(self, config: dict[str, Any]):
        self._config = config

    def connect(self, *, stream: bool = False, autocommit: bool = True):
        cursorclass = pymysql.cursors.SSDictCursor if stream else pymysql.cursors.DictCursor
        return pymysql.connect(
            host=str(self._config.get("host", "127.0.0.1")),
            port=int(self._config.get("port", 3306)),
            user=str(self._config.get("username", "root")),
            password=str(self._config.get("password", "")),
            database=str(self._config.get("database", "supplycore")),
            charset=str(self._config.get("charset", "utf8mb4")),
            unix_socket=(str(self._config.get("socket", "")).strip() or None),
            autocommit=autocommit,
            cursorclass=cursorclass,
        )

    @contextmanager
    def cursor(self, *, stream: bool = False):
        connection = self.connect(stream=stream)
        try:
            with connection.cursor() as cursor:
                yield connection, cursor
        finally:
            connection.close()

    def fetch_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any] | None:
        with self.cursor() as (_, cursor):
            cursor.execute(sql, params or ())
            row = cursor.fetchone()
            return dict(row) if row else None

    def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self.cursor() as (_, cursor):
            cursor.execute(sql, params or ())
            return [dict(row) for row in cursor.fetchall()]

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        with self.cursor() as (_, cursor):
            return int(cursor.execute(sql, params or ()))

    def insert(self, sql: str, params: Sequence[Any] | None = None) -> int:
        with self.cursor() as (connection, cursor):
            cursor.execute(sql, params or ())
            connection.commit()
            return int(cursor.lastrowid or 0)

    def iterate_batches(self, sql: str, params: Sequence[Any] | None = None, *, batch_size: int = 1000) -> Generator[list[dict[str, Any]], None, None]:
        with self.cursor(stream=True) as (_, cursor):
            cursor.execute(sql, params or ())
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(row) for row in rows]

    @contextmanager
    def transaction(self):
        connection = self.connect(autocommit=False)
        try:
            with connection.cursor() as cursor:
                yield connection, cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def queue_due_recurring_jobs(self, definitions: dict[str, dict[str, Any]], *, only_job_keys: list[str] | None = None) -> dict[str, int]:
        scoped = definitions
        if only_job_keys:
            keyset = {key for key in only_job_keys if key in definitions}
            scoped = {key: value for key, value in definitions.items() if key in keyset}
        queued = 0
        skipped = 0
        decision_logged = 0
        for job_key, definition in scoped.items():
            existing = self.fetch_one(
                "SELECT id FROM worker_jobs WHERE job_key = %s AND status IN ('queued', 'running', 'retry') LIMIT 1",
                (job_key,),
            )
            if existing:
                skipped += 1
                self.insert_scheduler_planner_decision(
                    schedule_id=None,
                    job_key=job_key,
                    decision_type="rolling_skipped_existing",
                    pressure_state="healthy",
                    reason_text="Skipped because queued/running/retry work already exists for this recurring job.",
                    decision_json={"job_key": job_key, "existing_worker_job_id": int(existing.get("id") or 0)},
                )
                decision_logged += 1
                continue

            min_interval_seconds = max(60, int(definition.get("min_interval_seconds") or definition.get("interval_seconds") or 300))
            max_staleness_seconds = max(min_interval_seconds, int(definition.get("max_staleness_seconds") or (min_interval_seconds * 2)))
            cooldown_seconds = max(0, int(definition.get("cooldown_seconds") or 0))
            opportunistic_background = bool(definition.get("opportunistic_background", False))
            freshness_sensitivity = str(definition.get("freshness_sensitivity") or "background")
            last_finished = self.fetch_one(
                "SELECT MAX(last_finished_at) AS last_finished_at FROM worker_jobs WHERE job_key = %s",
                (job_key,),
            )
            next_due_seconds = 0
            staleness_seconds = max_staleness_seconds
            if last_finished and last_finished.get("last_finished_at"):
                timing = self.fetch_one(
                    """SELECT
                        GREATEST(0, TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), DATE_ADD(%s, INTERVAL %s SECOND))) AS next_due_seconds,
                        GREATEST(0, TIMESTAMPDIFF(SECOND, DATE_ADD(%s, INTERVAL %s SECOND), UTC_TIMESTAMP())) AS staleness_seconds
                        """,
                    (last_finished["last_finished_at"], min_interval_seconds, last_finished["last_finished_at"], min_interval_seconds),
                ) or {}
                next_due_seconds = max(0, int(timing.get("next_due_seconds") or 0))
                staleness_seconds = max(0, int(timing.get("staleness_seconds") or 0))
                if next_due_seconds > 0 and staleness_seconds < max_staleness_seconds:
                    skipped += 1
                    self.insert_scheduler_planner_decision(
                        schedule_id=None,
                        job_key=job_key,
                        decision_type="rolling_deferred_cooldown",
                        pressure_state="healthy",
                        reason_text="Deferred because minimum interval has not elapsed and max staleness has not been reached.",
                        decision_json={
                            "job_key": job_key,
                            "next_due_seconds": next_due_seconds,
                            "staleness_seconds": staleness_seconds,
                            "min_interval_seconds": min_interval_seconds,
                            "max_staleness_seconds": max_staleness_seconds,
                        },
                    )
                    decision_logged += 1
                    continue

            urgency_score = (
                _priority_rank(str(definition.get("priority") or "normal")) * 1000
                + min(99_999, int(staleness_seconds))
                + (150_000 if freshness_sensitivity == "immediate" else 0)
                + (50_000 if not opportunistic_background else 0)
            )
            available_seconds = cooldown_seconds if staleness_seconds <= 0 else 0
            payload = json_dumps_safe(
                {
                    "recurring": True,
                    "rolling_planner": True,
                    "min_interval_seconds": min_interval_seconds,
                    "max_staleness_seconds": max_staleness_seconds,
                    "cooldown_seconds": cooldown_seconds,
                    "freshness_sensitivity": freshness_sensitivity,
                    "runtime_class": str(definition.get("runtime_class") or ""),
                    "resource_cost": str(definition.get("resource_cost") or ""),
                    "lock_group": str(definition.get("lock_group") or ""),
                    "opportunistic_background": opportunistic_background,
                    "staleness_seconds": staleness_seconds,
                    "urgency_score": urgency_score,
                }
            )
            self.execute(
                """INSERT INTO worker_jobs (
                        job_key, queue_name, workload_class, execution_mode, priority, status, unique_key,
                        payload_json, available_at, max_attempts, timeout_seconds, retry_delay_seconds, memory_limit_mb
                    ) VALUES (%s, %s, %s, 'python', %s, 'queued', %s, %s, DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND), %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        queue_name = VALUES(queue_name),
                        workload_class = VALUES(workload_class),
                        execution_mode = 'python',
                        priority = VALUES(priority),
                        payload_json = VALUES(payload_json),
                        available_at = CASE WHEN worker_jobs.status IN ('completed', 'failed', 'dead') THEN VALUES(available_at) ELSE worker_jobs.available_at END,
                        status = CASE WHEN worker_jobs.status IN ('completed', 'failed', 'dead') THEN 'queued' ELSE worker_jobs.status END,
                        locked_at = CASE WHEN worker_jobs.status IN ('completed', 'failed', 'dead') THEN NULL ELSE worker_jobs.locked_at END,
                        lock_expires_at = CASE WHEN worker_jobs.status IN ('completed', 'failed', 'dead') THEN NULL ELSE worker_jobs.lock_expires_at END,
                        locked_by = CASE WHEN worker_jobs.status IN ('completed', 'failed', 'dead') THEN NULL ELSE worker_jobs.locked_by END,
                        heartbeat_at = CASE WHEN worker_jobs.status IN ('completed', 'failed', 'dead') THEN NULL ELSE worker_jobs.heartbeat_at END,
                        attempts = CASE WHEN worker_jobs.status IN ('completed', 'failed', 'dead') THEN 0 ELSE worker_jobs.attempts END""",
                (
                    job_key,
                    str(definition.get("queue_name") or "default"),
                    str(definition.get("workload_class") or "sync"),
                    str(definition.get("priority") or "normal"),
                    f"recurring:{job_key}",
                    payload,
                    available_seconds,
                    max(1, int(definition.get("max_attempts") or 5)),
                    max(30, int(definition.get("timeout_seconds") or 300)),
                    max(5, int(definition.get("retry_delay_seconds") or 30)),
                    max(128, int(definition.get("memory_limit_mb") or 512)),
                ),
            )
            self.insert_scheduler_planner_decision(
                schedule_id=None,
                job_key=job_key,
                decision_type="rolling_queued",
                pressure_state="healthy",
                reason_text="Queued by rolling planner because a worker was free and this job met freshness/cooldown requirements.",
                decision_json={
                    "job_key": job_key,
                    "urgency_score": urgency_score,
                    "staleness_seconds": staleness_seconds,
                    "min_interval_seconds": min_interval_seconds,
                    "max_staleness_seconds": max_staleness_seconds,
                    "cooldown_seconds": cooldown_seconds,
                    "opportunistic_background": opportunistic_background,
                },
            )
            decision_logged += 1
            queued += 1
        return {"queued": queued, "skipped": skipped, "job_count": len(scoped), "planner_decisions_logged": decision_logged}

    def claim_next_worker_job(
        self,
        worker_id: str,
        *,
        queues: list[str],
        workload_classes: list[str],
        execution_modes: list[str],
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        safe_worker_id = worker_id.strip()[:190]
        if not safe_worker_id:
            return None
        mode_values = [mode for mode in execution_modes if mode == "python"]
        if not mode_values:
            return None

        conditions = ["status IN ('queued', 'retry')", "available_at <= UTC_TIMESTAMP()", "execution_mode = 'python'"]
        params: list[Any] = []
        if queues:
            conditions.append("queue_name IN (" + ",".join(["%s"] * len(queues)) + ")")
            params.extend(queues)
        if workload_classes:
            conditions.append("workload_class IN (" + ",".join(["%s"] * len(workload_classes)) + ")")
            params.extend(workload_classes)

        with self.transaction() as (_, cursor):
            cursor.execute(
                f"""SELECT id FROM worker_jobs
                    WHERE {' AND '.join(conditions)}
                    ORDER BY CASE priority
                        WHEN 'highest' THEN 400
                        WHEN 'high' THEN 300
                        WHEN 'medium' THEN 200
                        WHEN 'normal' THEN 100
                        ELSE 50 END DESC,
                        CAST(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.urgency_score')) AS UNSIGNED) DESC,
                        available_at ASC,
                        id ASC
                    LIMIT 1
                    FOR UPDATE""",
                tuple(params),
            )
            row = cursor.fetchone()
            if not row:
                return None
            job_id = int(row["id"])
            cursor.execute(
                """UPDATE worker_jobs
                    SET status='running', attempts=attempts+1, locked_at=UTC_TIMESTAMP(),
                        lock_expires_at=DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND),
                        heartbeat_at=UTC_TIMESTAMP(), locked_by=%s, last_started_at=UTC_TIMESTAMP(),
                        last_error=NULL
                    WHERE id=%s AND status IN ('queued', 'retry')""",
                (max(30, min(3600, lease_seconds)), safe_worker_id, job_id),
            )
            if int(cursor.rowcount or 0) != 1:
                return None
            cursor.execute("SELECT * FROM worker_jobs WHERE id = %s LIMIT 1", (job_id,))
            claimed = cursor.fetchone()
            return dict(claimed) if claimed else None

    def insert_scheduler_planner_decision(
        self,
        *,
        schedule_id: int | None,
        job_key: str,
        decision_type: str,
        pressure_state: str,
        reason_text: str,
        decision_json: dict[str, Any],
    ) -> None:
        try:
            self.execute(
                """INSERT INTO scheduler_planner_decisions (
                    schedule_id, job_key, decision_type, pressure_state, reason_text, decision_json
                ) VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    schedule_id,
                    job_key[:120],
                    decision_type[:40],
                    pressure_state[:32],
                    reason_text[:500],
                    json_dumps_safe(decision_json),
                ),
            )
        except Exception:
            return

    def worker_claim_diagnostics(self, *, queues: list[str], workload_classes: list[str]) -> dict[str, Any]:
        clauses = ["execution_mode='python'"]
        params: list[Any] = []
        if queues:
            clauses.append("queue_name IN (" + ",".join(["%s"] * len(queues)) + ")")
            params.extend(queues)
        if workload_classes:
            clauses.append("workload_class IN (" + ",".join(["%s"] * len(workload_classes)) + ")")
            params.extend(workload_classes)
        where_filtered = " AND " + " AND ".join(clauses)
        repeated = params * 4
        counts = self.fetch_one(
            f"""SELECT
                SUM(CASE WHEN status IN ('queued','retry') AND available_at <= UTC_TIMESTAMP() THEN 1 ELSE 0 END) AS ready_all,
                SUM(CASE WHEN status IN ('queued','retry') AND available_at <= UTC_TIMESTAMP() {where_filtered} THEN 1 ELSE 0 END) AS ready_filtered,
                SUM(CASE WHEN status IN ('queued','retry') AND available_at > UTC_TIMESTAMP() {where_filtered} THEN 1 ELSE 0 END) AS delayed_filtered,
                SUM(CASE WHEN status = 'running' {where_filtered} THEN 1 ELSE 0 END) AS running_filtered
                FROM worker_jobs""",
            tuple(repeated),
        ) or {}
        return {
            "reason": "no_matching_jobs",
            "ready_jobs_all": int(counts.get("ready_all") or 0),
            "ready_jobs_filtered": int(counts.get("ready_filtered") or 0),
            "delayed_jobs_filtered": int(counts.get("delayed_filtered") or 0),
            "running_jobs_filtered": int(counts.get("running_filtered") or 0),
            "filters": {"queues": queues, "workload_classes": workload_classes, "execution_modes": ["python"]},
        }

    def heartbeat_worker_job(self, job_id: int, worker_id: str, lease_seconds: int) -> None:
        self.execute(
            """UPDATE worker_jobs
                SET heartbeat_at = UTC_TIMESTAMP(),
                    lock_expires_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND)
                WHERE id = %s AND status = 'running' AND locked_by = %s""",
            (max(30, min(3600, lease_seconds)), job_id, worker_id[:190]),
        )

    def complete_worker_job(self, job_id: int, worker_id: str, result: dict[str, Any]) -> None:
        self.execute(
            """UPDATE worker_jobs
                SET status='completed', locked_at=NULL, lock_expires_at=NULL, locked_by=NULL, heartbeat_at=NULL,
                    last_error=NULL, last_result_json=%s, last_finished_at=UTC_TIMESTAMP()
                WHERE id=%s AND status='running' AND locked_by=%s""",
            (json_dumps_safe(result), job_id, worker_id[:190]),
        )

    def retry_worker_job(self, job_id: int, worker_id: str, error: str, retry_delay_seconds: int, result: dict[str, Any]) -> None:
        row = self.fetch_one("SELECT attempts,max_attempts,retry_delay_seconds FROM worker_jobs WHERE id=%s LIMIT 1", (job_id,)) or {}
        attempts = int(row.get("attempts") or 0)
        max_attempts = max(1, int(row.get("max_attempts") or 1))
        next_status = "dead" if attempts >= max_attempts else "retry"
        delay = max(5, int(retry_delay_seconds or row.get("retry_delay_seconds") or 30))
        self.execute(
            """UPDATE worker_jobs
                SET status=%s,
                    available_at=CASE WHEN %s='retry' THEN DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND) ELSE available_at END,
                    locked_at=NULL, lock_expires_at=NULL, locked_by=NULL, heartbeat_at=NULL,
                    last_error=%s, last_result_json=%s, last_finished_at=UTC_TIMESTAMP()
                WHERE id=%s AND locked_by=%s""",
            (next_status, next_status, delay, error[:500], json_dumps_safe(result), job_id, worker_id[:190]),
        )


def json_dumps(value: Any) -> str:
    return json_dumps_safe(value)
