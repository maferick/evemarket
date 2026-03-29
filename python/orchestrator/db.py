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
            user=str(self._config.get("username", "supplycore")),
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
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            row = cursor.fetchone()
            return dict(row) if row else None

    def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        with self.cursor() as (_, cursor):
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        with self.cursor() as (_, cursor):
            if params is None:
                return int(cursor.execute(sql))
            return int(cursor.execute(sql, params))

    def insert(self, sql: str, params: Sequence[Any] | None = None) -> int:
        with self.cursor() as (connection, cursor):
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
            connection.commit()
            return int(cursor.lastrowid or 0)

    def iterate_batches(self, sql: str, params: Sequence[Any] | None = None, *, batch_size: int = 1000) -> Generator[list[dict[str, Any]], None, None]:
        with self.cursor(stream=True) as (_, cursor):
            if params is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, params)
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

    def reap_stale_running_jobs(self) -> int:
        """Mark stuck running jobs as dead when their lock has expired."""
        return self.execute(
            """UPDATE worker_jobs
               SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'failed' END,
                   last_error = 'Reaped: lock expired while still running (worker likely crashed)',
                   last_finished_at = UTC_TIMESTAMP(),
                   locked_at = NULL,
                   lock_expires_at = NULL,
                   locked_by = NULL,
                   heartbeat_at = NULL
               WHERE status = 'running'
                 AND lock_expires_at IS NOT NULL
                 AND lock_expires_at < UTC_TIMESTAMP()"""
        )

    def count_active_workers(self, queue_name: str | None = None) -> dict[str, int]:
        """Count distinct workers that have claimed jobs in the last 10 minutes, by queue."""
        rows = self.fetch_all(
            """SELECT queue_name,
                      COUNT(DISTINCT locked_by) AS active_workers
               FROM worker_jobs
               WHERE locked_by IS NOT NULL
                 AND status IN ('running', 'queued')
                 AND (lock_expires_at IS NULL OR lock_expires_at > UTC_TIMESTAMP())
                 AND locked_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
               GROUP BY queue_name"""
        )
        counts: dict[str, int] = {}
        for row in rows:
            qn = str(row.get("queue_name") or "default")
            counts[qn] = int(row.get("active_workers") or 0)
        if queue_name is not None:
            return {"total": counts.get(queue_name, 0)}
        return counts

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

            # Freshness-aware scheduling: if all upstream ESI data is still within
            # its Expires window, defer the job — there's nothing new to fetch.
            from .scheduler_pressure import compute_esi_freshness, compute_pressure_state
            source_systems = definition.get("source_systems") or []
            if "esi" in source_systems:
                esi_freshness = compute_esi_freshness(self, job_key)
                if esi_freshness and esi_freshness.all_fresh:
                    skipped += 1
                    self.insert_scheduler_planner_decision(
                        schedule_id=None,
                        job_key=job_key,
                        decision_type="rolling_deferred_esi_fresh",
                        pressure_state="healthy",
                        reason_text=(
                            f"Deferred because all {esi_freshness.total_endpoints} upstream ESI endpoints "
                            f"still have valid data (earliest expiry in {esi_freshness.earliest_expires_seconds:.0f}s)."
                        ),
                        decision_json={
                            "job_key": job_key,
                            "total_endpoints": esi_freshness.total_endpoints,
                            "fresh_endpoints": esi_freshness.fresh_endpoints,
                            "earliest_expires_seconds": esi_freshness.earliest_expires_seconds,
                        },
                    )
                    decision_logged += 1
                    continue

            # Pressure-aware urgency: scale down under load to reduce contention.
            pressure = compute_pressure_state(self)
            urgency_score = int(
                (
                    _priority_rank(str(definition.get("priority") or "normal")) * 1000
                    + min(99_999, int(staleness_seconds))
                    + (150_000 if freshness_sensitivity == "immediate" else 0)
                    + (50_000 if not opportunistic_background else 0)
                )
                * pressure.urgency_multiplier
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
                pressure_state=pressure.state,
                reason_text="Queued by rolling planner because a worker was free and this job met freshness/cooldown requirements.",
                decision_json={
                    "job_key": job_key,
                    "urgency_score": urgency_score,
                    "staleness_seconds": staleness_seconds,
                    "min_interval_seconds": min_interval_seconds,
                    "max_staleness_seconds": max_staleness_seconds,
                    "cooldown_seconds": cooldown_seconds,
                    "opportunistic_background": opportunistic_background,
                    "pressure_state": pressure.state,
                    "pressure_running": pressure.running_jobs,
                    "pressure_queued": pressure.queued_jobs,
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
        # Lock-group enforcement: don't claim a job if another job in the
        # same lock_group is already running.  Jobs with no lock_group (empty
        # string) are not constrained.
        conditions.append(
            """(
                COALESCE(JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.lock_group')), '') = ''
                OR JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.lock_group')) NOT IN (
                    SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(rj.payload_json, '$.lock_group'))
                    FROM worker_jobs rj
                    WHERE rj.status = 'running'
                      AND JSON_UNQUOTE(JSON_EXTRACT(rj.payload_json, '$.lock_group')) != ''
                      AND JSON_UNQUOTE(JSON_EXTRACT(rj.payload_json, '$.lock_group')) IS NOT NULL
                )
            )"""
        )

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
        repeated = params * 3
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
        row = self.fetch_one("SELECT attempts,max_attempts,retry_delay_seconds,job_key FROM worker_jobs WHERE id=%s LIMIT 1", (job_id,)) or {}
        attempts = int(row.get("attempts") or 0)
        max_attempts = max(1, int(row.get("max_attempts") or 1))
        next_status = "dead" if attempts >= max_attempts else "retry"
        base_delay = max(5, int(retry_delay_seconds or row.get("retry_delay_seconds") or 30))
        # Exponential backoff: scale delay by attempt number, capped at 300s.
        delay = min(300, base_delay * max(1, attempts))
        # Circuit breaker: if 3+ consecutive failures for this job_key in the
        # last 10 minutes, extend the cooldown significantly.
        job_key = str(row.get("job_key") or "")
        if job_key and next_status == "retry":
            recent_failures = self.fetch_scalar(
                """SELECT COUNT(*) FROM worker_jobs
                   WHERE job_key = %s AND status IN ('retry', 'dead', 'failed')
                     AND last_finished_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)""",
                (job_key,),
            )
            if recent_failures >= 3:
                delay = min(600, delay * 3)  # Triple the delay under circuit breaker
        self.execute(
            """UPDATE worker_jobs
                SET status=%s,
                    available_at=CASE WHEN %s='retry' THEN DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND) ELSE available_at END,
                    locked_at=NULL, lock_expires_at=NULL, locked_by=NULL, heartbeat_at=NULL,
                    last_error=%s, last_result_json=%s, last_finished_at=UTC_TIMESTAMP()
                WHERE id=%s AND locked_by=%s""",
            (next_status, next_status, delay, error[:500], json_dumps_safe(result), job_id, worker_id[:190]),
        )

    def fetch_scalar(self, sql: str, params: Sequence[Any] | None = None, *, default: int = 0) -> int:
        row = self.fetch_one(sql, params)
        if not row:
            return default
        first = next(iter(row.values()), default)
        return int(first or 0)

    def upsert_sync_state(
        self,
        *,
        dataset_key: str,
        status: str,
        row_count: int,
        checksum: str | None = None,
        cursor: str | None = None,
        error_message: str | None = None,
    ) -> int:
        return self.execute(
            """INSERT INTO sync_state (
                    dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message
                ) VALUES (
                    %s, 'incremental', %s, CASE WHEN %s='success' THEN UTC_TIMESTAMP() ELSE NULL END, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    last_success_at = CASE WHEN VALUES(status)='success' THEN UTC_TIMESTAMP() ELSE sync_state.last_success_at END,
                    last_cursor = VALUES(last_cursor),
                    last_row_count = VALUES(last_row_count),
                    last_checksum = VALUES(last_checksum),
                    last_error_message = VALUES(last_error_message)""",
            (dataset_key[:190], status[:20], status[:20], cursor, max(0, row_count), checksum, error_message),
        )

    def upsert_intelligence_snapshot(
        self,
        *,
        snapshot_key: str,
        payload_json: str,
        metadata_json: str,
        expires_seconds: int = 900,
    ) -> int:
        return self.execute(
            """INSERT INTO intelligence_snapshots (
                    snapshot_key, snapshot_status, payload_json, metadata_json, computed_at, refresh_started_at, expires_at
                ) VALUES (
                    %s, 'ready', %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s SECOND)
                )
                ON DUPLICATE KEY UPDATE
                    snapshot_status='ready',
                    payload_json=VALUES(payload_json),
                    metadata_json=VALUES(metadata_json),
                    computed_at=VALUES(computed_at),
                    refresh_started_at=VALUES(refresh_started_at),
                    expires_at=VALUES(expires_at)""",
            (snapshot_key[:190], payload_json, metadata_json, max(60, expires_seconds)),
        )

    def insert_ui_refresh_event(
        self,
        *,
        job_key: str,
        job_status: str,
        domains_json: str | None = None,
        ui_sections_json: str | None = None,
        section_versions_json: str | None = None,
    ) -> int:
        return self.insert(
            """INSERT INTO ui_refresh_events (
                    event_type, event_key, job_key, job_status, finished_at,
                    domains_json, ui_sections_json, section_versions_json
                ) VALUES (
                    'job_completed', %s, %s, %s, UTC_TIMESTAMP(), %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    updated_at=CURRENT_TIMESTAMP,
                    id=LAST_INSERT_ID(id),
                    section_versions_json=COALESCE(VALUES(section_versions_json), section_versions_json)""",
            (
                f"worker_pool:{job_key}"[:190],
                job_key[:190],
                job_status[:40],
                domains_json,
                ui_sections_json,
                section_versions_json,
            ),
        )

    def bump_ui_refresh_section_versions(
        self,
        *,
        version_keys: list[str],
        job_key: str,
        job_status: str,
        event_id: int,
    ) -> int:
        bumped = 0
        for version_key in version_keys:
            bumped += self.execute(
                """INSERT INTO ui_refresh_section_versions (
                        section_key, version_counter, last_job_key, last_status, last_event_id, last_finished_at
                    ) VALUES (%s, 1, %s, %s, %s, UTC_TIMESTAMP())
                    ON DUPLICATE KEY UPDATE
                        version_counter = version_counter + 1,
                        last_job_key = VALUES(last_job_key),
                        last_status = VALUES(last_status),
                        last_event_id = VALUES(last_event_id),
                        last_finished_at = VALUES(last_finished_at),
                        updated_at = CURRENT_TIMESTAMP""",
                (version_key[:190], job_key[:190], job_status[:40], event_id),
            )
        return bumped

    def insert_sync_run(self, *, dataset_key: str, rows_seen: int, rows_written: int, status: str, error: str | None = None) -> int:
        return self.insert(
            """INSERT INTO sync_runs (dataset_key, run_mode, run_status, started_at, finished_at, cursor_start, cursor_end, source_rows, written_rows, error_message)
               VALUES (%s, 'incremental', %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), NULL, NULL, %s, %s, %s)""",
            (dataset_key[:190], status[:20], max(0, rows_seen), max(0, rows_written), error[:500] if error else None),
        )

    def insert_scheduler_job_event(self, *, job_key: str, event_type: str, payload_json: str, duration_seconds: float) -> int:
        return self.insert(
            """INSERT INTO scheduler_job_events (job_key, event_type, detail_json, duration_seconds)
               VALUES (%s, %s, %s, %s)""",
            (job_key[:120], event_type[:40], payload_json, round(duration_seconds, 2)),
        )

    def update_sync_schedule_status(self, *, job_key: str, status: str, snapshot_json: str) -> int:
        return self.execute(
            """UPDATE sync_schedules
               SET last_status = %s,
                   last_run_at = UTC_TIMESTAMP(),
                   locked_until = NULL
               WHERE job_key = %s AND execution_mode = 'python'""",
            (status[:20], job_key[:120]),
        )

    def refresh_market_order_current_projection(self, *, source_type: str) -> dict[str, int]:
        rows_processed = self.fetch_scalar(
            "SELECT COUNT(*) AS c FROM market_orders_current WHERE source_type = %s",
            (source_type,),
        )
        rows_written = self.execute(
            """INSERT INTO market_order_current_projection (
                    source_type, source_id, type_id, observed_at, best_sell_price, best_buy_price,
                    total_sell_volume, total_buy_volume, sell_order_count, buy_order_count, total_volume
                )
                SELECT
                    source_type,
                    source_id,
                    type_id,
                    MAX(observed_at) AS observed_at,
                    MIN(CASE WHEN is_buy_order = 0 THEN price END) AS best_sell_price,
                    MAX(CASE WHEN is_buy_order = 1 THEN price END) AS best_buy_price,
                    SUM(CASE WHEN is_buy_order = 0 THEN volume_remain ELSE 0 END) AS total_sell_volume,
                    SUM(CASE WHEN is_buy_order = 1 THEN volume_remain ELSE 0 END) AS total_buy_volume,
                    SUM(CASE WHEN is_buy_order = 0 THEN 1 ELSE 0 END) AS sell_order_count,
                    SUM(CASE WHEN is_buy_order = 1 THEN 1 ELSE 0 END) AS buy_order_count,
                    SUM(volume_remain) AS total_volume
                FROM market_orders_current
                WHERE source_type = %s
                GROUP BY source_type, source_id, type_id
                ON DUPLICATE KEY UPDATE
                    observed_at = VALUES(observed_at),
                    best_sell_price = VALUES(best_sell_price),
                    best_buy_price = VALUES(best_buy_price),
                    total_sell_volume = VALUES(total_sell_volume),
                    total_buy_volume = VALUES(total_buy_volume),
                    sell_order_count = VALUES(sell_order_count),
                    buy_order_count = VALUES(buy_order_count),
                    total_volume = VALUES(total_volume)""",
            (source_type,),
        )
        self.execute(
            """INSERT INTO market_source_snapshot_state (
                    source_type, source_id, latest_current_observed_at, current_order_count, current_distinct_type_count, summary_row_count, last_synced_at
                )
                SELECT
                    source_type,
                    source_id,
                    MAX(observed_at) AS latest_current_observed_at,
                    COUNT(*) AS current_order_count,
                    COUNT(DISTINCT type_id) AS current_distinct_type_count,
                    0 AS summary_row_count,
                    UTC_TIMESTAMP() AS last_synced_at
                FROM market_orders_current
                WHERE source_type = %s
                GROUP BY source_type, source_id
                ON DUPLICATE KEY UPDATE
                    latest_current_observed_at = VALUES(latest_current_observed_at),
                    current_order_count = VALUES(current_order_count),
                    current_distinct_type_count = VALUES(current_distinct_type_count),
                    last_synced_at = VALUES(last_synced_at)""",
            (source_type,),
        )
        return {"rows_processed": rows_processed, "rows_written": rows_written}

    def materialize_market_history_from_projection(self, *, source_type: str) -> dict[str, int]:
        rows_processed = self.fetch_scalar(
            "SELECT COUNT(*) FROM market_order_current_projection WHERE source_type = %s",
            (source_type,),
        )
        rows_written = self.execute(
            """INSERT INTO market_order_snapshots_summary (
                    source_type, source_id, type_id, observed_at, best_sell_price, best_buy_price,
                    total_buy_volume, total_sell_volume, total_volume, buy_order_count, sell_order_count
                )
                SELECT
                    source_type,
                    source_id,
                    type_id,
                    observed_at,
                    best_sell_price,
                    best_buy_price,
                    total_buy_volume,
                    total_sell_volume,
                    total_volume,
                    buy_order_count,
                    sell_order_count
                FROM market_order_current_projection
                WHERE source_type = %s
                ON DUPLICATE KEY UPDATE
                    best_sell_price = VALUES(best_sell_price),
                    best_buy_price = VALUES(best_buy_price),
                    total_buy_volume = VALUES(total_buy_volume),
                    total_sell_volume = VALUES(total_sell_volume),
                    total_volume = VALUES(total_volume),
                    buy_order_count = VALUES(buy_order_count),
                    sell_order_count = VALUES(sell_order_count)""",
            (source_type,),
        )
        return {"rows_processed": rows_processed, "rows_written": rows_written}

    def fetch_market_hub_sources(self, *, limit: int = 5) -> list[dict[str, int]]:
        rows = self.fetch_all(
            """SELECT DISTINCT rs.station_id AS source_id, rs.region_id
                FROM ref_npc_stations rs
                INNER JOIN trading_stations ts ON ts.id = rs.station_id
                WHERE ts.station_type = 'market'
                ORDER BY rs.station_id ASC
                LIMIT %s""",
            (max(1, limit),),
        )
        return [{"source_id": int(row.get("source_id") or 0), "region_id": int(row.get("region_id") or 0)} for row in rows]

    def fetch_app_setting(self, key: str, default: str = "") -> str:
        row = self.fetch_one("SELECT setting_value FROM app_settings WHERE setting_key = %s LIMIT 1", (key[:120],)) or {}
        value = str(row.get("setting_value") or "").strip()
        return value if value != "" else default

    def fetch_market_hub_sources_from_settings(self, *, limit: int = 4) -> list[dict[str, object]]:
        configured = self.fetch_app_setting("market_station_id", "").strip()
        if configured == "":
            return []
        source_id = int(configured) if configured.isdigit() else 0
        if source_id <= 0:
            return []

        npc = self.fetch_one(
            """SELECT ns.station_id, ns.system_id,
                      COALESCE(rs.region_id, ns.region_id) AS region_id
               FROM ref_npc_stations ns
               LEFT JOIN ref_systems rs ON rs.system_id = ns.system_id
               WHERE ns.station_id = %s
               LIMIT 1""",
            (source_id,),
        ) or {}
        station_id = int(npc.get("station_id") or 0)
        region_id = int(npc.get("region_id") or 0)
        if station_id > 0 and 10000000 < region_id < 11000000:
            return [{"source_id": source_id, "region_id": region_id, "source_kind": "npc_station"}]
        if station_id > 0 and region_id > 0:
            # region_id from ref_npc_stations is likely corrupt (e.g. station_id stored
            # instead of region); fall back to ref_systems via system_id.
            system_id = int(npc.get("system_id") or 0)
            if system_id > 0:
                sys_row = self.fetch_one(
                    "SELECT region_id FROM ref_systems WHERE system_id = %s LIMIT 1",
                    (system_id,),
                ) or {}
                resolved = int(sys_row.get("region_id") or 0)
                if 10000000 < resolved < 11000000:
                    return [{"source_id": source_id, "region_id": resolved, "source_kind": "npc_station"}]

        return [{"source_id": source_id, "region_id": 0, "source_kind": "player_structure"}]

    def fetch_region_id_for_npc_station(self, *, station_id: int) -> int:
        if station_id <= 0:
            return 0
        row = self.fetch_one(
            "SELECT region_id FROM ref_npc_stations WHERE station_id = %s LIMIT 1",
            (station_id,),
        ) or {}
        return int(row.get("region_id") or 0)

    def fetch_region_id_for_system(self, *, system_id: int) -> int:
        if system_id <= 0:
            return 0
        row = self.fetch_one(
            "SELECT region_id FROM ref_systems WHERE system_id = %s LIMIT 1",
            (system_id,),
        ) or {}
        return int(row.get("region_id") or 0)

    def fetch_alliance_structure_sources(self, *, limit: int = 5) -> list[int]:
        rows = self.fetch_all(
            """SELECT structure_id
                FROM alliance_structure_metadata
                ORDER BY last_verified_at DESC
                LIMIT %s""",
            (max(1, limit),),
        )
        return [int(row.get("structure_id") or 0) for row in rows if int(row.get("structure_id") or 0) > 0]

    def fetch_alliance_structure_sources_from_settings(self, *, limit: int = 3) -> list[int]:
        configured = self.fetch_app_setting("alliance_station_id", "").strip()
        structure_id = int(configured) if configured.isdigit() else 0
        if structure_id > 0:
            return [structure_id]
        return self.fetch_alliance_structure_sources(limit=max(1, limit))

    def fetch_latest_esi_access_token(self) -> str | None:
        row = self.fetch_one(
            """SELECT access_token
                FROM esi_oauth_tokens
                WHERE expires_at > UTC_TIMESTAMP()
                ORDER BY expires_at DESC
                LIMIT 1"""
        ) or {}
        token = str(row.get("access_token") or "").strip()
        if token != "":
            return token

        cached = self.fetch_one(
            """SELECT JSON_UNQUOTE(JSON_EXTRACT(payload_json, '$.access_token')) AS access_token
                FROM esi_cache_entries
                WHERE namespace_key = 'cache.esi.oauth.token'
                  AND cache_key = 'latest'
                  AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP())
                ORDER BY updated_at DESC, id DESC
                LIMIT 1"""
        ) or {}
        cached_token = str(cached.get("access_token") or "").strip()
        if cached_token != "":
            return cached_token

        return None

    def replace_market_orders_for_source(
        self,
        *,
        source_type: str,
        source_id: int,
        observed_at: str,
        orders: list[dict[str, Any]],
    ) -> dict[str, int]:
        safe_orders: list[tuple[Any, ...]] = []
        for row in orders:
            order_id = int(row.get("order_id") or 0)
            type_id = int(row.get("type_id") or 0)
            if order_id <= 0 or type_id <= 0:
                continue
            safe_orders.append(
                (
                    source_type,
                    max(0, source_id),
                    type_id,
                    order_id,
                    1 if bool(row.get("is_buy_order")) else 0,
                    float(row.get("price") or 0.0),
                    max(0, int(row.get("volume_remain") or 0)),
                    max(0, int(row.get("volume_total") or 0)),
                    max(1, int(row.get("min_volume") or 1)),
                    str(row.get("range") or "region")[:20],
                    max(1, int(row.get("duration") or 1)),
                    str(row.get("issued") or observed_at)[:19],
                    str(row.get("expires") or observed_at)[:19],
                    observed_at,
                )
            )

        with self.transaction() as (_, cursor):
            cursor.execute(
                "DELETE FROM market_orders_current WHERE source_type = %s AND source_id = %s",
                (source_type, max(0, source_id)),
            )
            if safe_orders:
                cursor.executemany(
                    """INSERT INTO market_orders_current (
                            source_type, source_id, type_id, order_id, is_buy_order, price, volume_remain, volume_total,
                            min_volume, `range`, duration, issued, expires, observed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    safe_orders,
                )
                cursor.executemany(
                    """INSERT INTO market_orders_history (
                            source_type, source_id, type_id, order_id, is_buy_order, price, volume_remain, volume_total,
                            min_volume, `range`, duration, issued, expires, observed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            price = VALUES(price),
                            volume_remain = VALUES(volume_remain),
                            volume_total = VALUES(volume_total)""",
                    safe_orders,
                )
        return {"rows_processed": len(orders), "rows_written": len(safe_orders)}


def json_dumps(value: Any) -> str:
    return json_dumps_safe(value)
