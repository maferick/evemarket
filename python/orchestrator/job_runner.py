from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .bridge import PhpBridge
from .config import load_php_runtime_config
from .db import SupplyCoreDb
from .jobs import (
    run_compute_graph_insights,
    run_compute_graph_sync,
    run_killmail_r2z2_stream,
    run_market_comparison_summary,
    run_market_hub_local_history,
)
from .worker_runtime import resident_memory_bytes, utc_now_iso


@dataclass(slots=True)
class PythonWorkerContext:
    schedule_id: int
    app_root: Path
    raw_config: dict[str, Any]
    php_binary: str
    db: SupplyCoreDb
    job: dict[str, Any]

    @property
    def db_config(self) -> dict[str, Any]:
        return dict(self.raw_config.get("db", {}))

    @property
    def scheduler_config(self) -> dict[str, Any]:
        return dict(self.raw_config.get("scheduler", {}))

    @property
    def batch_size(self) -> int:
        return 1_000

    @property
    def timeout_seconds(self) -> int:
        return int(self.job.get("timeout_seconds") or self.scheduler_config.get("default_timeout_seconds", 300))

    @property
    def memory_abort_threshold_bytes(self) -> int:
        return int(self.scheduler_config.get("memory_abort_threshold_bytes", 512 * 1024 * 1024))

    @property
    def job_key(self) -> str:
        return str(self.job.get("job_key", ""))

    def emit(self, event: str, payload: dict[str, Any]) -> None:
        emit(event, payload)


PROCESSORS: dict[str, Callable[[PythonWorkerContext], dict[str, Any]]] = {
    "killmail_r2z2_sync": run_killmail_r2z2_stream,
    "market_comparison_summary_sync": run_market_comparison_summary,
    "market_hub_local_history_sync": run_market_hub_local_history,
    "compute_graph_sync": lambda context: _graph_result_shape(
        run_compute_graph_sync(context.db, dict(context.raw_config.get("neo4j") or {})),
        "compute_graph_sync",
    ),
    "compute_graph_insights": lambda context: _graph_result_shape(
        run_compute_graph_insights(context.db, dict(context.raw_config.get("neo4j") or {})),
        "compute_graph_insights",
    ),
}


def _graph_result_shape(result: dict[str, Any], job_key: str) -> dict[str, Any]:
    rows_seen = max(0, int(result.get("rows_processed") or result.get("rows_seen") or 0))
    rows_written = max(0, int(result.get("rows_written") or 0))
    status = str(result.get("status") or "success")
    summary = str(result.get("summary") or f"{job_key} finished with status {status}.")
    return {
        "status": status,
        "summary": summary,
        "rows_seen": rows_seen,
        "rows_written": rows_written,
        "warnings": list(result.get("warnings") or []),
        "meta": dict(result.get("meta") or {}),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a SupplyCore scheduler job in Python worker mode.")
    parser.add_argument("--schedule-id", type=int, required=True, help="Claimed sync_schedules.id to execute.")
    parser.add_argument(
        "--app-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Path to the SupplyCore repository/app root.",
    )
    return parser.parse_args()


def emit(event: str, payload: dict[str, Any]) -> None:
    print(json.dumps({"event": event, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **payload}))


def _fetch_claimed_job(db: SupplyCoreDb, schedule_id: int) -> dict[str, Any]:
    job = db.fetch_one(
        "SELECT * FROM sync_schedules WHERE id = %s LIMIT 1",
        (schedule_id,),
    )
    if not job:
        raise RuntimeError(f"No scheduler job found for schedule ID {schedule_id}.")
    status = str(job.get("last_status") or "")
    locked_until = str(job.get("locked_until") or "")
    if status != "running" or locked_until.strip() == "":
        raise RuntimeError(f'Scheduler job "{job.get("job_key", "unknown_job")}" is no longer claimed for background execution.')
    return job


def _start_sync_run(db: SupplyCoreDb, job_key: str) -> int:
    dataset_key = f"scheduler.job.{job_key}"
    return db.insert(
        "INSERT INTO sync_runs (dataset_key, run_mode, run_status, started_at, cursor_start) VALUES (%s, %s, %s, UTC_TIMESTAMP(), NULL)",
        (dataset_key, "incremental", "running"),
    )


def _run_php_fallback(context: PythonWorkerContext, bridge: PhpBridge) -> dict[str, Any]:
    response = bridge.call(
        "run-job-handler",
        args=[f"--job-key={context.job_key}", "--reason=python-fallback"],
    )
    result = dict(response.get("result") or {})
    result.setdefault("status", "success")
    result.setdefault("summary", f"Executed {context.job_key} via PHP fallback inside Python worker mode.")
    result.setdefault("duration_ms", 0)
    result.setdefault("started_at", utc_now_iso())
    result.setdefault("finished_at", utc_now_iso())
    result.setdefault("meta", {})
    meta = dict(result.get("meta") or {})
    meta["execution_mode"] = "python"
    meta["fallback_runtime"] = "php"
    meta.setdefault("outcome_reason", "Job used the PHP handler through the Python-worker fallback bridge.")
    result["meta"] = meta
    return result


def process_job(context: PythonWorkerContext) -> dict[str, Any]:
    start = time.time()
    bridge = PhpBridge(context.php_binary, context.app_root)
    processor = PROCESSORS.get(context.job_key)

    context.emit(
        "python_worker.started",
        {
            "schedule_id": context.schedule_id,
            "job_key": context.job_key,
            "batch_size": context.batch_size,
            "timeout_seconds": context.timeout_seconds,
            "memory_abort_threshold_bytes": context.memory_abort_threshold_bytes,
            "memory_usage_bytes": resident_memory_bytes(),
            "db_host": context.db_config.get("host"),
            "db_port": context.db_config.get("port"),
            "db_database": context.db_config.get("database"),
        },
    )

    if processor is None:
        if not bool(context.scheduler_config.get("python_php_fallback_enabled", True)):
            raise RuntimeError(f"No Python processor is registered for job {context.job_key} and PHP fallback is disabled.")
        result = _run_php_fallback(context, bridge)
    else:
        result = processor(context)

    result.setdefault("duration_ms", int((time.time() - start) * 1000))
    result.setdefault("started_at", utc_now_iso())
    result.setdefault("finished_at", utc_now_iso())
    return result


def main() -> int:
    args = parse_args()
    app_root = Path(args.app_root).resolve()
    config = load_php_runtime_config(app_root)
    db = SupplyCoreDb(config.raw.get("db", {}))
    job = _fetch_claimed_job(db, max(0, args.schedule_id))
    context = PythonWorkerContext(
        schedule_id=max(0, args.schedule_id),
        app_root=app_root,
        raw_config=config.raw,
        php_binary=config.php_binary,
        db=db,
        job=job,
    )

    if context.schedule_id <= 0:
        emit("python_worker.error", {"error": "Argument --schedule-id must be a positive integer."})
        return 1

    os.environ.setdefault("APP_ENV", str(config.raw.get("app", {}).get("env", "development")))
    os.environ.setdefault("APP_TIMEZONE", str(config.raw.get("app", {}).get("timezone", "UTC")))

    run_id = _start_sync_run(db, context.job_key)
    bridge = PhpBridge(config.php_binary, app_root)
    exit_code = 0
    try:
        result = process_job(context)
        result["run_id"] = run_id
    except Exception as error:  # noqa: BLE001
        result = {
            "status": "failed",
            "error": str(error),
            "summary": str(error),
            "rows_seen": 0,
            "rows_written": 0,
            "warnings": [],
            "duration_ms": 0,
            "started_at": utc_now_iso(),
            "finished_at": utc_now_iso(),
            "run_id": run_id,
            "meta": {
                "execution_mode": "python",
                "memory_usage_bytes": resident_memory_bytes(),
            },
        }
        exit_code = 1

    finalized = bridge.call("finalize-job", args=[f"--schedule-id={context.schedule_id}"], payload=result)
    final_result = dict(finalized.get("result") or {})
    context.emit(
        "python_worker.finished",
        {
            "schedule_id": context.schedule_id,
            "job_key": context.job_key,
            **final_result,
            "memory_usage_bytes": resident_memory_bytes(),
        },
    )
    return 0 if final_result.get("status") != "failed" and exit_code == 0 else 1
