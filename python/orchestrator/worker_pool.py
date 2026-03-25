from __future__ import annotations

import argparse
import json
import os
import socket
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .bridge import PhpBridge
from .config import load_php_runtime_config
from .db import SupplyCoreDb
from .jobs import (
    run_compute_buy_all,
    run_compute_graph_insights,
    run_compute_graph_sync,
    run_compute_signals,
    run_killmail_r2z2_stream,
    run_market_comparison_summary,
    run_market_hub_local_history,
)
from .json_utils import make_json_safe
from .logging_utils import LoggerAdapter, configure_logging
from .worker_runtime import resident_memory_bytes, utc_now_iso


@dataclass(slots=True)
class WorkerPoolContext:
    job: dict[str, Any]
    app_root: Path
    php_binary: str
    worker_id: str
    logger: LoggerAdapter
    raw_config: dict[str, Any]
    batch_size: int
    timeout_seconds: int
    memory_abort_threshold_bytes: int
    db: SupplyCoreDb
    allowed_execution_modes: list[str]

    @property
    def schedule_id(self) -> int:
        return int(self.job.get("id") or 0)

    @property
    def job_key(self) -> str:
        return str(self.job.get("job_key") or "").strip()

    @property
    def db_config(self) -> dict[str, Any]:
        return dict(self.raw_config.get("db") or {})

    def emit(self, event: str, payload: dict[str, Any]) -> None:
        self.logger.info(event, payload={"event": event, **payload})


PROCESSORS: dict[str, Callable[[WorkerPoolContext], dict[str, Any]]] = {
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
    "compute_buy_all": lambda context: _compute_result_shape(
        run_compute_buy_all(context.db),
        "compute_buy_all",
    ),
    "compute_signals": lambda context: _compute_result_shape(
        run_compute_signals(context.db, dict(context.raw_config.get("influx") or {})),
        "compute_signals",
    ),
}

PYTHON_PRIMARY_JOB_KEYS = {
    "market_hub_current_sync",
    "dashboard_summary_sync",
    "market_comparison_summary_sync",
    "market_hub_local_history_sync",
    "killmail_r2z2_sync",
    "compute_graph_sync",
    "compute_graph_insights",
    "compute_buy_all",
    "compute_signals",
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


def _compute_result_shape(result: dict[str, Any], job_key: str) -> dict[str, Any]:
    rows_processed = max(0, int(result.get("rows_processed") or 0))
    rows_written = max(0, int(result.get("rows_written") or 0))
    return {
        "status": "success",
        "summary": f"{job_key} completed successfully.",
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "meta": {
            "job_name": job_key,
            "computed_at": str(result.get("computed_at") or ""),
            "result": dict(result),
        },
    }


class HeartbeatThread(threading.Thread):
    def __init__(self, bridge: PhpBridge, worker_id: str, job_id: int, lease_seconds: int, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.bridge = bridge
        self.worker_id = worker_id
        self.job_id = job_id
        self.lease_seconds = lease_seconds
        self.stop_event = stop_event

    def run(self) -> None:
        interval = max(10, self.lease_seconds // 3)
        while not self.stop_event.wait(interval):
            try:
                self.bridge.call(
                    "heartbeat-worker-job",
                    payload={
                        "job_id": self.job_id,
                        "worker_id": self.worker_id,
                        "lease_seconds": self.lease_seconds,
                    },
                )
            except Exception:
                return


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the SupplyCore continuous worker pool.")
    parser.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--worker-id", default="")
    parser.add_argument("--queues", default="sync,compute")
    parser.add_argument("--workload-classes", default="sync,compute")
    parser.add_argument("--execution-modes", default="python,php")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _parse_csv(raw: str) -> list[str]:
    return [part.strip() for part in raw.split(",") if part.strip()]


def _runtime_settings(app_root: Path) -> tuple[dict[str, Any], dict[str, Any], str, dict[str, Any]]:
    config = load_php_runtime_config(app_root)
    bridge = PhpBridge(config.php_binary, app_root)
    response = bridge.call("worker-runtime-config")
    return dict(response.get("workers") or {}), dict(response.get("definitions") or {}), config.php_binary, dict(config.raw or {})


def _write_state_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _bridge_php_job(context: WorkerPoolContext) -> dict[str, Any]:
    bridge = PhpBridge(context.php_binary, context.app_root)
    reason = "python-primary" if context.job_key in PYTHON_PRIMARY_JOB_KEYS else "worker-pool-php"
    response = bridge.call("run-job-handler", args=[f"--job-key={context.job_key}", f"--reason={reason}"])
    result = dict(response.get("result") or {})
    result.setdefault("status", "success")
    result.setdefault("summary", f"Executed {context.job_key} through the worker pool PHP bridge.")
    meta = dict(result.get("meta") or {})
    meta.setdefault("execution_mode", str(context.job.get("execution_mode") or "php"))
    meta.setdefault("worker_runtime", "python_pool")
    meta.setdefault("php_bridge", True)
    if context.job_key in PYTHON_PRIMARY_JOB_KEYS:
        meta.setdefault("outcome_reason", "Python worker pool owned dispatch, retry, locking, and memory controls for this heavy job.")
    result["meta"] = meta
    result["execution_language"] = "php"
    result["subprocess_invoked"] = True
    return result


def _process_job(context: WorkerPoolContext) -> dict[str, Any]:
    processor = PROCESSORS.get(context.job_key)
    started = time.monotonic()
    context.emit(
        "worker_pool.job_started",
        {
            "job_id": context.schedule_id,
            "job_key": context.job_key,
            "worker_id": context.worker_id,
            "execution_mode": str(context.job.get("execution_mode") or "php"),
            "allowed_execution_modes": context.allowed_execution_modes,
            "memory_usage_bytes": resident_memory_bytes(),
        },
    )
    if processor is None:
        if "php" not in context.allowed_execution_modes:
            raise RuntimeError(f"Job {context.job_key} requires PHP execution but this worker accepts only {context.allowed_execution_modes}.")
        result = _bridge_php_job(context)
    else:
        result = processor(context)
        result.setdefault("execution_language", "python")
        result.setdefault("subprocess_invoked", False)
    result.setdefault("duration_ms", int((time.monotonic() - started) * 1000))
    result.setdefault("started_at", utc_now_iso())
    result.setdefault("finished_at", utc_now_iso())
    return make_json_safe(result)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_root = Path(args.app_root).resolve()
    worker_settings, definitions, php_binary, raw_config = _runtime_settings(app_root)
    queue_names = _parse_csv(args.queues)
    workload_classes = _parse_csv(args.workload_classes)
    execution_modes = _parse_csv(args.execution_modes)
    log_file = Path(
        worker_settings.get(
            "compute_log_file" if "compute" in workload_classes and "sync" not in workload_classes else "worker_log_file",
            app_root / "storage/logs/worker.log",
        )
    )
    logger = configure_logging(verbose=args.verbose, log_file=log_file)
    worker_id = args.worker_id.strip() or f"{socket.gethostname()}-{os.getpid()}"
    state_file = Path(worker_settings.get("pool_state_file", app_root / "storage/run/worker-pool-heartbeat.json"))
    bridge = PhpBridge(php_binary, app_root)
    db = SupplyCoreDb(dict(raw_config.get("db") or {}))
    lease_seconds = max(30, int(worker_settings.get("claim_ttl_seconds", 300)))
    idle_sleep = max(1, int(worker_settings.get("idle_sleep_seconds", 10)))
    sync_idle_sleep = max(1, int(worker_settings.get("sync_idle_sleep_seconds", idle_sleep)))
    compute_idle_sleep = max(1, int(worker_settings.get("compute_idle_sleep_seconds", idle_sleep)))
    pause_threshold = max(128 * 1024 * 1024, int(worker_settings.get("memory_pause_threshold_bytes", 384 * 1024 * 1024)))
    abort_threshold = max(pause_threshold, int(worker_settings.get("memory_abort_threshold_bytes", 512 * 1024 * 1024)))
    retry_backoff = max(5, int(worker_settings.get("retry_backoff_seconds", 30)))

    logger.info(
        "worker pool started",
        payload={
            "event": "worker_pool.started",
            "worker_id": worker_id,
            "queues": queue_names,
            "workload_classes": workload_classes,
            "execution_modes": execution_modes,
            "lease_seconds": lease_seconds,
        },
    )

    while True:
        seed_result = bridge.call("queue-recurring-jobs", payload={"job_keys": list(definitions.keys())})
        memory_usage = resident_memory_bytes()
        if memory_usage >= abort_threshold:
            logger.warning(
                "memory abort threshold reached",
                payload={"event": "worker_pool.memory_abort", "worker_id": worker_id, "memory_usage_bytes": memory_usage},
            )
            return 1
        if memory_usage >= pause_threshold:
            logger.warning(
                "memory pause threshold reached",
                payload={"event": "worker_pool.memory_pause", "worker_id": worker_id, "memory_usage_bytes": memory_usage},
            )
            time.sleep(min(30, compute_idle_sleep))

        claim = bridge.call(
            "claim-worker-job",
            payload={
                "worker_id": worker_id,
                "queues": queue_names,
                "workload_classes": workload_classes,
                "execution_modes": execution_modes,
                "lease_seconds": lease_seconds,
            },
        )
        job = claim.get("job")
        _write_state_file(
            state_file,
            {
                "ts": utc_now_iso(),
                "worker_id": worker_id,
                "queues": queue_names,
                "workload_classes": workload_classes,
                "execution_modes": execution_modes,
                "seed_result": seed_result.get("result"),
                "job": job,
                "memory_usage_bytes": memory_usage,
            },
        )

        if not isinstance(job, dict) or not job:
            sleep_for = compute_idle_sleep if workload_classes == ["compute"] else sync_idle_sleep if workload_classes == ["sync"] else idle_sleep
            logger.info(
                "worker pool idle",
                payload={"event": "worker_pool.idle", "worker_id": worker_id, "sleep_seconds": sleep_for},
            )
            if args.once:
                return 0
            time.sleep(sleep_for)
            continue

        timeout_seconds = max(30, int(job.get("timeout_seconds") or 300))
        memory_limit_mb = max(128, int(job.get("memory_limit_mb") or 512))
        context = WorkerPoolContext(
            job=job,
            app_root=app_root,
            php_binary=php_binary,
            worker_id=worker_id,
            logger=logger,
            raw_config=raw_config,
            batch_size=1000,
            timeout_seconds=timeout_seconds,
            memory_abort_threshold_bytes=min(abort_threshold, memory_limit_mb * 1024 * 1024),
            db=db,
            allowed_execution_modes=execution_modes,
        )

        stop_event = threading.Event()
        heartbeat = HeartbeatThread(bridge, worker_id, int(job.get("id") or 0), lease_seconds, stop_event)
        heartbeat.start()
        try:
            result = _process_job(context)
            bridge.call(
                "complete-worker-job",
                payload={"job_id": int(job.get("id") or 0), "worker_id": worker_id, "result": result},
            )
            logger.info(
                "worker job completed",
                payload={
                    "event": "worker_pool.job_completed",
                    "worker_id": worker_id,
                    "job_id": int(job.get("id") or 0),
                    "job_key": context.job_key,
                    "status": result.get("status", "success"),
                    "execution_language": result.get("execution_language", "python"),
                    "subprocess_invoked": bool(result.get("subprocess_invoked", False)),
                    "duration_ms": result.get("duration_ms", 0),
                },
            )
        except Exception as error:  # noqa: BLE001
            bridge.call(
                "retry-worker-job",
                payload={
                    "job_id": int(job.get("id") or 0),
                    "worker_id": worker_id,
                    "error": str(error),
                    "retry_delay_seconds": retry_backoff,
                    "result": {
                        "status": "failed",
                        "error": str(error),
                        "summary": str(error),
                        "finished_at": utc_now_iso(),
                    },
                },
            )
            logger.warning(
                "worker job failed",
                payload={
                    "event": "worker_pool.job_failed",
                    "worker_id": worker_id,
                    "job_id": int(job.get("id") or 0),
                    "job_key": context.job_key,
                    "error": str(error),
                },
            )
            if args.once:
                return 1
        finally:
            stop_event.set()
            heartbeat.join(timeout=1)

        if args.once:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
