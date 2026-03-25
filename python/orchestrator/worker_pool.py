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

from .config import load_php_runtime_config
from .db import SupplyCoreDb
from .jobs import (
    run_compute_battle_actor_features,
    run_compute_battle_anomalies,
    run_compute_battle_rollups,
    run_compute_battle_target_metrics,
    run_compute_behavioral_baselines,
    run_compute_buy_all,
    run_compute_graph_derived_relationships,
    run_compute_graph_insights,
    run_compute_graph_prune,
    run_compute_graph_sync,
    run_compute_graph_sync_battle_intelligence,
    run_compute_graph_sync_doctrine_dependency,
    run_compute_graph_topology_metrics,
    run_compute_signals,
    run_compute_suspicion_scores,
    run_compute_suspicion_scores_v2,
)
from .json_utils import make_json_safe
from .logging_utils import LoggerAdapter, configure_logging
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


PROCESSORS: dict[str, Callable[[WorkerPoolContext], dict[str, Any]]] = {
    "compute_graph_sync": lambda context: _graph_result_shape(run_compute_graph_sync(context.db, dict(context.raw_config.get("neo4j") or {})), "compute_graph_sync"),
    "compute_graph_sync_doctrine_dependency": lambda context: _graph_result_shape(run_compute_graph_sync_doctrine_dependency(context.db, dict(context.raw_config.get("neo4j") or {})), "compute_graph_sync_doctrine_dependency"),
    "compute_graph_sync_battle_intelligence": lambda context: _graph_result_shape(run_compute_graph_sync_battle_intelligence(context.db, dict(context.raw_config.get("neo4j") or {})), "compute_graph_sync_battle_intelligence"),
    "compute_graph_derived_relationships": lambda context: _graph_result_shape(run_compute_graph_derived_relationships(context.db, dict(context.raw_config.get("neo4j") or {})), "compute_graph_derived_relationships"),
    "compute_graph_insights": lambda context: _graph_result_shape(run_compute_graph_insights(context.db, dict(context.raw_config.get("neo4j") or {})), "compute_graph_insights"),
    "compute_graph_prune": lambda context: _graph_result_shape(run_compute_graph_prune(context.db, dict(context.raw_config.get("neo4j") or {})), "compute_graph_prune"),
    "compute_graph_topology_metrics": lambda context: _graph_result_shape(run_compute_graph_topology_metrics(context.db, dict(context.raw_config.get("neo4j") or {})), "compute_graph_topology_metrics"),
    "compute_behavioral_baselines": lambda context: _compute_result_shape(run_compute_behavioral_baselines(context.db, dict(context.raw_config.get("battle_intelligence") or {})), "compute_behavioral_baselines"),
    "compute_suspicion_scores_v2": lambda context: _compute_result_shape(run_compute_suspicion_scores_v2(context.db, dict(context.raw_config.get("battle_intelligence") or {})), "compute_suspicion_scores_v2"),
    "compute_buy_all": lambda context: _compute_result_shape(run_compute_buy_all(context.db), "compute_buy_all"),
    "compute_signals": lambda context: _compute_result_shape(run_compute_signals(context.db, dict(context.raw_config.get("influx") or {})), "compute_signals"),
    "compute_battle_rollups": lambda context: _compute_result_shape(run_compute_battle_rollups(context.db, dict(context.raw_config.get("battle_intelligence") or {})), "compute_battle_rollups"),
    "compute_battle_target_metrics": lambda context: _compute_result_shape(run_compute_battle_target_metrics(context.db, dict(context.raw_config.get("battle_intelligence") or {})), "compute_battle_target_metrics"),
    "compute_battle_anomalies": lambda context: _compute_result_shape(run_compute_battle_anomalies(context.db, dict(context.raw_config.get("battle_intelligence") or {})), "compute_battle_anomalies"),
    "compute_battle_actor_features": lambda context: _compute_result_shape(run_compute_battle_actor_features(context.db, dict(context.raw_config.get("neo4j") or {}), dict(context.raw_config.get("battle_intelligence") or {})), "compute_battle_actor_features"),
    "compute_suspicion_scores": lambda context: _compute_result_shape(run_compute_suspicion_scores(context.db, dict(context.raw_config.get("battle_intelligence") or {})), "compute_suspicion_scores"),
}


def _graph_result_shape(result: dict[str, Any], job_key: str) -> dict[str, Any]:
    return {
        "status": str(result.get("status") or "success"),
        "summary": str(result.get("summary") or f"{job_key} finished."),
        "rows_processed": max(0, int(result.get("rows_processed") or result.get("rows_seen") or 0)),
        "rows_written": max(0, int(result.get("rows_written") or 0)),
        "warnings": list(result.get("warnings") or []),
        "meta": dict(result.get("meta") or {}),
    }


def _compute_result_shape(result: dict[str, Any], job_key: str) -> dict[str, Any]:
    status = str(result.get("status") or "success")
    return {
        "status": status,
        "summary": str(result.get("summary") or f"{job_key} completed with status {status}."),
        "rows_processed": max(0, int(result.get("rows_processed") or 0)),
        "rows_written": max(0, int(result.get("rows_written") or 0)),
        "warnings": list(result.get("warnings") or []),
        "meta": {"job_name": job_key, "result": dict(result)},
    }


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
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _process_job(context: WorkerPoolContext) -> dict[str, Any]:
    processor = PROCESSORS.get(context.job_key)
    if processor is None:
        raise RuntimeError(f"No Python processor registered for job {context.job_key}; job is disabled until ported.")
    started = time.monotonic()
    result = processor(context)
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

    lease_seconds = max(30, int(worker_settings.get("claim_ttl_seconds", 300)))
    idle_sleep = max(0, int(worker_settings.get("idle_sleep_seconds", 10)))
    pause_threshold = max(128 * 1024 * 1024, int(worker_settings.get("memory_pause_threshold_bytes", 384 * 1024 * 1024)))
    abort_threshold = max(pause_threshold, int(worker_settings.get("memory_abort_threshold_bytes", 512 * 1024 * 1024)))
    retry_backoff = max(5, int(worker_settings.get("retry_backoff_seconds", 30)))

    while True:
        seed_result = db.queue_due_recurring_jobs(WORKER_JOB_DEFINITIONS)
        memory_usage = resident_memory_bytes()
        if memory_usage >= abort_threshold:
            logger.warning("memory abort threshold reached", payload={"event": "worker_pool.memory_abort", "worker_id": worker_id, "memory_usage_bytes": memory_usage})
            return 1
        if memory_usage >= pause_threshold and idle_sleep > 0:
            time.sleep(min(30, idle_sleep))

        job = db.claim_next_worker_job(worker_id, queues=queue_names, workload_classes=workload_classes, execution_modes=execution_modes, lease_seconds=lease_seconds)
        diagnostics = db.worker_claim_diagnostics(queues=queue_names, workload_classes=workload_classes)

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
