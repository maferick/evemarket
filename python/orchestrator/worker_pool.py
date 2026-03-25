from __future__ import annotations

import argparse
import json
import os
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pymysql

from .config import load_php_runtime_config
from .db import SupplyCoreDb
from .json_utils import json_dumps_safe, make_json_safe
from .logging_utils import LoggerAdapter, configure_logging
from .processor_registry import audit_enabled_python_jobs, run_compute_processor
from .worker_registry import WORKER_JOB_DEFINITIONS
from .worker_runtime import resident_memory_bytes, utc_now_iso


@dataclass(slots=True)
class WorkerPoolContext:
    job: dict[str, Any]
    app_root: Path
    worker_id: str
    logger: LoggerAdapter
    raw_config: dict[str, Any]
    timeout_seconds: int
    memory_abort_threshold_bytes: int
    db: SupplyCoreDb

    @property
    def schedule_id(self) -> int:
        return int(self.job.get("id") or 0)

    @property
    def job_key(self) -> str:
        return str(self.job.get("job_key") or "").strip()

    def emit(self, event: str, payload: dict[str, Any]) -> None:
        self.logger.info(event, payload={"event": event, **payload})


class HeartbeatThread(threading.Thread):
    def __init__(self, db: SupplyCoreDb, worker_id: str, job_id: int, lease_seconds: int, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.db = db
        self.worker_id = worker_id
        self.job_id = job_id
        self.lease_seconds = lease_seconds
        self.stop_event = stop_event

    def run(self) -> None:
        interval = max(10, self.lease_seconds // 3)
        while not self.stop_event.wait(interval):
            try:
                self.db.heartbeat_worker_job(self.job_id, self.worker_id, self.lease_seconds)
            except Exception:
                return


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the SupplyCore Python-only worker pool.")
    parser.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--worker-id", default="")
    parser.add_argument("--queues", default="sync,compute")
    parser.add_argument("--workload-classes", default="sync,compute")
    parser.add_argument("--execution-modes", default="python")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _parse_csv(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def _write_state_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json_dumps_safe(payload, indent=2) + "\n", encoding="utf-8")


def _process_job(context: WorkerPoolContext) -> dict[str, Any]:
    started = time.monotonic()
    result = run_compute_processor(context.job_key, context.db, context.raw_config)
    result.setdefault("duration_ms", int((time.monotonic() - started) * 1000))
    result.setdefault("started_at", utc_now_iso())
    result.setdefault("finished_at", utc_now_iso())
    result["execution_language"] = "python"
    result["subprocess_invoked"] = False
    return make_json_safe(result)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    raw_config = load_php_runtime_config(app_root).raw
    queue_names = _parse_csv(args.queues)
    workload_classes = _parse_csv(args.workload_classes)
    execution_modes = [mode for mode in _parse_csv(args.execution_modes) if mode == "python"]
    worker_settings = dict(raw_config.get("workers") or {})

    log_file = Path(worker_settings.get("compute_log_file" if "compute" in workload_classes and "sync" not in workload_classes else "worker_log_file", app_root / "storage/logs/worker.log"))
    logger = configure_logging(verbose=args.verbose, log_file=log_file)
    worker_id = args.worker_id.strip() or f"{socket.gethostname()}-{os.getpid()}"
    state_file = Path(worker_settings.get("pool_state_file", app_root / "storage/run/worker-pool-heartbeat.json"))
    db = SupplyCoreDb(dict(raw_config.get("db") or {}))
    audit = audit_enabled_python_jobs(db)
    if audit["issues"]:
        for issue in audit["issues"]:
            logger.error("python worker binding audit failure", payload={"event": "worker_pool.binding_audit.failed", "issue": issue})
        return 1

    lease_seconds = max(30, int(worker_settings.get("claim_ttl_seconds", 300)))
    idle_sleep = max(0, int(worker_settings.get("idle_sleep_seconds", 10)))
    pause_threshold = max(128 * 1024 * 1024, int(worker_settings.get("memory_pause_threshold_bytes", 384 * 1024 * 1024)))
    abort_threshold = max(pause_threshold, int(worker_settings.get("memory_abort_threshold_bytes", 512 * 1024 * 1024)))
    retry_backoff = max(5, int(worker_settings.get("retry_backoff_seconds", 30)))

    while True:
        memory_usage = resident_memory_bytes()
        if memory_usage >= abort_threshold:
            logger.warning("memory abort threshold reached", payload={"event": "worker_pool.memory_abort", "worker_id": worker_id, "memory_usage_bytes": memory_usage})
            return 1
        if memory_usage >= pause_threshold and idle_sleep > 0:
            time.sleep(min(30, idle_sleep))

        try:
            seed_result = db.queue_due_recurring_jobs(WORKER_JOB_DEFINITIONS)
            job = db.claim_next_worker_job(worker_id, queues=queue_names, workload_classes=workload_classes, execution_modes=execution_modes, lease_seconds=lease_seconds)
            diagnostics = db.worker_claim_diagnostics(queues=queue_names, workload_classes=workload_classes)
        except pymysql.MySQLError as error:
            logger.warning(
                "database unavailable while polling worker queue",
                payload={
                    "event": "worker_pool.db_unavailable",
                    "worker_id": worker_id,
                    "error": str(error),
                    "retry_in_seconds": retry_backoff,
                },
            )
            _write_state_file(
                state_file,
                {
                    "ts": utc_now_iso(),
                    "worker_id": worker_id,
                    "queues": queue_names,
                    "workload_classes": workload_classes,
                    "execution_modes": execution_modes,
                    "seed_result": None,
                    "job": None,
                    "memory_usage_bytes": memory_usage,
                    "last_error": str(error),
                },
            )
            if args.once:
                return 1
            time.sleep(retry_backoff)
            continue

        _write_state_file(state_file, {"ts": utc_now_iso(), "worker_id": worker_id, "queues": queue_names, "workload_classes": workload_classes, "execution_modes": execution_modes, "seed_result": seed_result, "job": job, "memory_usage_bytes": memory_usage})

        if not job:
            if args.once:
                return 0
            if idle_sleep > 0:
                time.sleep(idle_sleep)
            continue
        payload = {}
        try:
            payload = json.loads(str(job.get("payload_json") or "{}"))
        except Exception:
            payload = {}
        logger.info(
            "worker job claimed",
            payload={
                "event": "worker_pool.job_claimed",
                "worker_id": worker_id,
                "job_id": int(job.get("id") or 0),
                "job_key": str(job.get("job_key") or ""),
                "priority": str(job.get("priority") or "normal"),
                "urgency_score": int(payload.get("urgency_score") or 0),
                "staleness_seconds": int(payload.get("staleness_seconds") or 0),
                "freshness_sensitivity": str(payload.get("freshness_sensitivity") or ""),
                "opportunistic_background": bool(payload.get("opportunistic_background", False)),
                "diagnostics": diagnostics,
            },
        )

        context = WorkerPoolContext(
            job=job,
            app_root=app_root,
            worker_id=worker_id,
            logger=logger,
            raw_config=raw_config,
            timeout_seconds=max(30, int(job.get("timeout_seconds") or 300)),
            memory_abort_threshold_bytes=min(abort_threshold, max(128, int(job.get("memory_limit_mb") or 512)) * 1024 * 1024),
            db=db,
        )

        stop_event = threading.Event()
        heartbeat = HeartbeatThread(db, worker_id, int(job.get("id") or 0), lease_seconds, stop_event)
        heartbeat.start()
        try:
            result = _process_job(context)
            db.complete_worker_job(int(job.get("id") or 0), worker_id, result)
            logger.info("worker job completed", payload={"event": "worker_pool.job_completed", "worker_id": worker_id, "job_id": int(job.get("id") or 0), "job_key": context.job_key, "status": result.get("status", "success"), "execution_language": "python", "subprocess_invoked": False, "duration_ms": result.get("duration_ms", 0), "rows_processed": result.get("rows_processed", 0), "rows_written": result.get("rows_written", 0)})
        except Exception as error:  # noqa: BLE001
            db.retry_worker_job(int(job.get("id") or 0), worker_id, str(error), retry_backoff, {"status": "failed", "error": str(error), "summary": str(error), "finished_at": utc_now_iso(), "execution_language": "python", "subprocess_invoked": False})
            logger.warning("worker job failed", payload={"event": "worker_pool.job_failed", "worker_id": worker_id, "job_id": int(job.get("id") or 0), "job_key": context.job_key, "error": str(error)})
            if args.once:
                return 1
        finally:
            stop_event.set()
            heartbeat.join(timeout=1)

        if args.once:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
