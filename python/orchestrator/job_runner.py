from __future__ import annotations

import argparse
import os
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
from .processor_registry import PYTHON_PROCESSOR_JOB_KEYS, audit_enabled_python_jobs, run_registered_processor
from .worker_runtime import resident_memory_bytes, utc_now_iso


@dataclass(slots=True)
class PythonWorkerContext:
    schedule_id: int
    app_root: Path
    raw_config: dict[str, Any]
    php_binary: str
    db: SupplyCoreDb
    job: dict[str, Any]
    cli_options: dict[str, Any]

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
    "market_comparison_summary_sync",
    "market_hub_local_history_sync",
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
    parser.add_argument("--dry-run", action="store_true", help="Run job in dry-run mode when supported.")
    parser.add_argument("--batch-size", type=int, default=0, help="Override batch size for compatible jobs.")
    parser.add_argument("--max-batches", type=int, default=0, help="Maximum number of batches to run for compatible jobs.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose structured worker output.")
    return parser.parse_args()


def emit(event: str, payload: dict[str, Any]) -> None:
    line = json_dumps_safe({"event": event, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **payload})
    print(line)
    emit_log = _ACTIVE_EMIT_LOG
    if emit_log is not None:
        emit_log.write_line(line)


class SyncJobLogWriter:
    def __init__(self, app_root: Path, job_key: str) -> None:
        safe_job = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in job_key).strip("_") or "unknown_job"
        self._job_key = safe_job
        self._log_dir = app_root / "storage" / "logs" / "sync-jobs"
        self._active_log_path = self._log_dir / f"{safe_job}.log"
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._rotate_daily_archive()
        self._prune_archives()

    def write_line(self, line: str) -> None:
        with self._active_log_path.open("a", encoding="utf-8") as handle:
            handle.write(line.rstrip("\n") + "\n")

    def _archive_path_for_date(self, day: datetime) -> Path:
        return self._log_dir / f"{self._job_key}-{day.strftime('%Y-%m-%d')}.zip"

    def _rotate_daily_archive(self) -> None:
        if not self._active_log_path.exists() or self._active_log_path.stat().st_size == 0:
            return

        mtime = datetime.fromtimestamp(self._active_log_path.stat().st_mtime, tz=timezone.utc)
        today = datetime.now(tz=timezone.utc).date()
        if mtime.date() >= today:
            return

        archive_day = datetime.combine(mtime.date(), datetime.min.time(), tzinfo=timezone.utc)
        archive_path = self._archive_path_for_date(archive_day)
        archive_entry_name = f"{self._job_key}-{archive_day.strftime('%Y-%m-%d')}.log"
        with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(self._active_log_path, arcname=archive_entry_name)
        self._active_log_path.unlink(missing_ok=True)

    def _prune_archives(self) -> None:
        cutoff_day = datetime.now(tz=timezone.utc).date() - timedelta(days=2)
        for archive in self._log_dir.glob(f"{self._job_key}-*.zip"):
            stamp = archive.stem.removeprefix(f"{self._job_key}-")
            try:
                archive_day = datetime.strptime(stamp, "%Y-%m-%d").date()
            except ValueError:
                continue
            if archive_day < cutoff_day:
                archive.unlink(missing_ok=True)


_ACTIVE_EMIT_LOG: SyncJobLogWriter | None = None


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

    if context.job_key in PYTHON_PROCESSOR_JOB_KEYS:
        result = run_registered_processor(context.job_key, context.db, context.raw_config)
    elif processor is None:
        if context.job_key in PHP_BRIDGED_JOB_KEYS:
            result = _run_php_fallback(context, bridge)
        else:
            raise RuntimeError(f"No Python processor is registered for job {context.job_key}.")
    else:
        result = processor(context)

    result.setdefault("duration_ms", int((time.time() - start) * 1000))
    result.setdefault("started_at", utc_now_iso())
    result.setdefault("finished_at", utc_now_iso())
    result.setdefault("meta", {})
    result["meta"] = dict(result.get("meta") or {})
    result["meta"]["cli_options"] = dict(context.cli_options)
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
        cli_options={
            "app_root": str(app_root),
            "dry_run": bool(args.dry_run),
            "batch_size": max(0, int(args.batch_size or 0)),
            "max_batches": max(0, int(args.max_batches or 0)),
            "verbose": bool(args.verbose),
        },
    )

    global _ACTIVE_EMIT_LOG
    _ACTIVE_EMIT_LOG = SyncJobLogWriter(app_root=app_root, job_key=context.job_key)

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

    try:
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
    finally:
        _ACTIVE_EMIT_LOG = None
