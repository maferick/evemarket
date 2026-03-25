from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .bridge import PhpBridge
from .config import load_php_runtime_config
from .db import SupplyCoreDb
from .json_utils import json_dumps_safe
from .jobs import (
    run_killmail_r2z2_stream,
    run_market_comparison_summary,
    run_market_hub_local_history,
)
from .processor_registry import PYTHON_COMPUTE_PROCESSOR_JOB_KEYS, audit_enabled_python_jobs, run_compute_processor
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
        return str(self.job.get("job_key", "")).strip()

    def emit(self, event: str, payload: dict[str, Any]) -> None:
        emit(event, payload)


PROCESSORS = {
    "killmail_r2z2_sync": run_killmail_r2z2_stream,
    "market_comparison_summary_sync": run_market_comparison_summary,
    "market_hub_local_history_sync": run_market_hub_local_history,
}

# Jobs that intentionally execute via the PHP bridge while still running under the
# Python scheduler/runtime process.
PHP_BRIDGED_JOB_KEYS: set[str] = {
    "market_hub_current_sync",
    "deal_alerts_sync",
    "alliance_current_sync",
    "current_state_refresh_sync",
    "doctrine_intelligence_sync",
    "market_comparison_summary_sync",
    "loss_demand_summary_sync",
    "dashboard_summary_sync",
    "rebuild_ai_briefings",
    "activity_priority_summary_sync",
    "market_hub_local_history_sync",
    "analytics_bucket_1h_sync",
    "analytics_bucket_1d_sync",
    "alliance_historical_sync",
    "market_hub_historical_sync",
    "forecasting_ai_sync",
    "killmail_r2z2_sync",
}

INVALID_BRIDGED_JOB_KEYS = sorted(job_key for job_key in PHP_BRIDGED_JOB_KEYS if job_key.startswith("compute_"))
if INVALID_BRIDGED_JOB_KEYS:
    raise RuntimeError(f"Compute jobs must be Python-native and cannot use PHP bridge fallback: {', '.join(INVALID_BRIDGED_JOB_KEYS)}")


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
    print(json_dumps_safe({"event": event, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **payload}))


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
        if context.job_key.startswith("compute_"):
            raise RuntimeError(f"No Python processor is registered for compute job {context.job_key}. Compute jobs cannot use PHP fallback.")
        if context.job_key in PHP_BRIDGED_JOB_KEYS:
            result = _run_php_fallback(context, bridge)
        elif not bool(context.scheduler_config.get("python_php_fallback_enabled", True)):
            raise RuntimeError(f"No Python processor is registered for job {context.job_key} and PHP fallback is disabled.")
        else:
            result = _run_php_fallback(context, bridge)
    else:
        if context.job_key in PYTHON_COMPUTE_PROCESSOR_JOB_KEYS:
            result = run_compute_processor(context.job_key, context.db, context.raw_config)
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
    audit = audit_enabled_python_jobs(db)
    if audit["issues"]:
        raise RuntimeError("Enabled Python job binding audit failed: " + "; ".join(audit["issues"]))
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
