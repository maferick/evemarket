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

import zipfile
from datetime import datetime, timedelta, timezone

from .config import load_php_runtime_config, resolve_app_root
from .db import SupplyCoreDb
from .job_result import JobResult
from .json_utils import json_dumps_safe, make_json_safe
from .logging_utils import LoggerAdapter, configure_logging
from .processor_registry import audit_enabled_python_jobs, run_registered_processor
from .scheduling_graph import (
    build_graph,
    compute_scheduling_plan,
    filter_by_concurrency_groups,
)
from .worker_registry import WORKER_JOB_DEFINITIONS
from .worker_runtime import resident_memory_bytes, utc_now_iso


# Maps job keys to the UI refresh domains/sections/version_keys they affect.
# Must stay in sync with supplycore_ui_refresh_job_domain_map() in PHP.
_UI_REFRESH_JOB_MAP: dict[str, dict[str, list[str]]] = {
    "killmail_r2z2_sync": {
        "domains": ["doctrine_activity", "loss_aware_views", "activity_priority", "killmail_overview"],
        "ui_sections": ["activity-doctrines", "activity-sidebar", "activity-items", "doctrine-fit-history", "killmail-overview-summary", "killmail-overview-status", "killmail-overview-table"],
        "version_keys": ["killmail_activity_version", "activity_priority_version", "killmail_overview_version"],
    },
    "alliance_current_sync": {
        "domains": ["alliance_stock", "doctrine_readiness", "fit_availability", "buyall"],
        "ui_sections": ["dashboard-queues", "buyall-overview", "buyall-results", "doctrine-fit-items", "current-alliance-main"],
        "version_keys": ["alliance_stock_version", "buyall_version"],
    },
    "market_hub_current_sync": {
        "domains": ["market_prices", "opportunity_queue", "risk_queue", "buyall"],
        "ui_sections": ["dashboard-queues", "buyall-overview", "buyall-results", "reference-comparison-main", "missing-items-main", "price-deviations-main"],
        "version_keys": ["market_prices_version", "buyall_version"],
    },
    "dashboard_summary_sync": {
        "domains": ["dashboard"],
        "ui_sections": ["page-freshness", "dashboard-kpis", "dashboard-buyall", "dashboard-doctrine", "dashboard-queues"],
        "version_keys": ["dashboard_summary_version"],
    },
    "doctrine_intelligence_sync": {
        "domains": ["doctrine_readiness", "doctrine_details", "fit_availability", "bottlenecks"],
        "ui_sections": ["dashboard-doctrine", "buyall-overview", "buyall-results", "activity-doctrines", "activity-sidebar", "doctrine-fit-summary", "doctrine-fit-history", "doctrine-fit-items", "doctrine-index-main", "doctrine-group-main"],
        "version_keys": ["doctrine_readiness_version", "buyall_version"],
    },
    "market_comparison_summary_sync": {
        "domains": ["market_comparison", "overlap", "pricing_summaries", "buyall", "dashboard"],
        "ui_sections": ["dashboard-queues", "dashboard-kpis", "dashboard-buyall", "current-alliance-main", "reference-comparison-main", "missing-items-main", "price-deviations-main", "buyall-results"],
        "version_keys": ["market_comparison_version", "buyall_version", "dashboard_summary_version"],
    },
    "loss_demand_summary_sync": {
        "domains": ["loss_aware_views", "dashboard"],
        "ui_sections": ["dashboard-queues"],
        "version_keys": ["loss_demand_version", "dashboard_summary_version"],
    },
    "activity_priority_summary_sync": {
        "domains": ["activity_priority", "doctrine_activity"],
        "ui_sections": ["activity-summary", "activity-doctrines", "activity-sidebar", "activity-items", "dashboard-doctrine"],
        "version_keys": ["activity_priority_version"],
    },
    "current_state_refresh_sync": {
        "domains": ["dashboard", "market_comparison", "loss_aware_views"],
        "ui_sections": ["page-freshness", "dashboard-kpis", "dashboard-buyall", "dashboard-doctrine", "dashboard-queues", "current-alliance-main", "reference-comparison-main", "missing-items-main", "price-deviations-main"],
        "version_keys": ["dashboard_summary_version", "market_comparison_version", "loss_demand_version"],
    },
    "deal_alerts_sync": {
        "domains": ["dashboard", "deal_alerts"],
        "ui_sections": ["dashboard-queues", "deal-alerts-summary", "deal-alerts-status", "deal-alerts-table"],
        "version_keys": ["deal_alerts_version", "dashboard_summary_version"],
    },
    "compute_buy_all": {
        "domains": ["buyall", "dashboard"],
        "ui_sections": ["dashboard-buyall", "buyall-overview", "buyall-results"],
        "version_keys": ["buyall_version", "dashboard_summary_version"],
    },
}


class _JobLogWriter:
    """Per-job log writer for worker pool jobs. Writes to storage/logs/sync-jobs/{job_key}.log."""

    def __init__(self, app_root: Path, job_key: str) -> None:
        safe_job = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in job_key).strip("_") or "unknown_job"
        self._job_key = safe_job
        self._log_dir = app_root / "storage" / "logs" / "sync-jobs"
        self._log_path = self._log_dir / f"{safe_job}.log"
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._rotate_if_stale()

    def write_line(self, line: str) -> None:
        with self._log_path.open("a", encoding="utf-8") as handle:
            handle.write(line.rstrip("\n") + "\n")

    def write_event(self, event: str, payload: dict[str, Any]) -> None:
        line = json_dumps_safe({"event": event, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **payload})
        self.write_line(line)

    def _rotate_if_stale(self) -> None:
        if not self._log_path.exists() or self._log_path.stat().st_size == 0:
            return
        mtime = datetime.fromtimestamp(self._log_path.stat().st_mtime, tz=timezone.utc)
        if mtime.date() >= datetime.now(tz=timezone.utc).date():
            return
        archive_day = datetime.combine(mtime.date(), datetime.min.time(), tzinfo=timezone.utc)
        archive_path = self._log_dir / f"{self._job_key}-{archive_day.strftime('%Y-%m-%d')}.zip"
        entry_name = f"{self._job_key}-{archive_day.strftime('%Y-%m-%d')}.log"
        try:
            with zipfile.ZipFile(archive_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(self._log_path, arcname=entry_name)
            self._log_path.unlink(missing_ok=True)
        except OSError:
            pass
        cutoff = datetime.now(tz=timezone.utc).date() - timedelta(days=2)
        for old_archive in self._log_dir.glob(f"{self._job_key}-*.zip"):
            stamp = old_archive.stem.removeprefix(f"{self._job_key}-")
            try:
                if datetime.strptime(stamp, "%Y-%m-%d").date() < cutoff:
                    old_archive.unlink(missing_ok=True)
            except ValueError:
                pass


def _finalize_job(db: SupplyCoreDb, job_key: str, result: dict[str, Any], logger: LoggerAdapter) -> None:
    """Post-job side effects: sync_state, sync_runs, UI refresh events, scheduler status."""
    status = str(result.get("status") or "success")
    rows_seen = max(0, int(result.get("rows_seen") or result.get("rows_processed") or 0))
    rows_written = max(0, int(result.get("rows_written") or 0))
    duration_ms = max(0, int(result.get("duration_ms") or 0))
    error_text = str(result.get("error_text") or result.get("error") or result.get("summary") or "") if status == "failed" else None

    dataset_key = f"scheduler.job.{job_key}"
    try:
        db.upsert_sync_state(
            dataset_key=dataset_key,
            status=status,
            row_count=rows_written,
            checksum=result.get("checksum"),
            cursor=str(result.get("cursor") or "") or None,
            error_message=error_text,
        )
    except Exception as exc:
        logger.warning("sync_state upsert failed", payload={"event": "worker_pool.finalize.sync_state_error", "job_key": job_key, "error": str(exc)})

    try:
        db.insert_sync_run(
            dataset_key=dataset_key,
            rows_seen=rows_seen,
            rows_written=rows_written,
            status=status,
            error=error_text,
        )
    except Exception as exc:
        logger.warning("sync_run insert failed", payload={"event": "worker_pool.finalize.sync_run_error", "job_key": job_key, "error": str(exc)})

    try:
        db.insert_scheduler_job_event(
            job_key=job_key,
            event_type="finished" if status != "failed" else "failure",
            payload_json=json_dumps_safe({
                "status": status,
                "execution_mode": "python",
                "rows_seen": rows_seen,
                "rows_written": rows_written,
                "summary": str(result.get("summary") or ""),
            }),
            duration_seconds=duration_ms / 1000.0,
        )
    except Exception as exc:
        logger.warning("scheduler_job_event insert failed", payload={"event": "worker_pool.finalize.event_error", "job_key": job_key, "error": str(exc)})

    try:
        db.update_sync_schedule_status(
            job_key=job_key,
            status=status,
            snapshot_json=json_dumps_safe({
                "last_status": status,
                "last_duration_seconds": round(duration_ms / 1000.0, 2),
                "rows_written": rows_written,
                "execution_mode": "python",
            }),
        )
    except Exception as exc:
        logger.warning("sync_schedule status update failed", payload={"event": "worker_pool.finalize.schedule_error", "job_key": job_key, "error": str(exc)})

    try:
        db.upsert_scheduler_job_current_status(
            job_key=job_key,
            status=status,
            event_type="finished" if status != "failed" else "failure",
            failure_message=error_text,
        )
    except Exception as exc:
        logger.warning("scheduler_job_current_status upsert failed", payload={"event": "worker_pool.finalize.current_status_error", "job_key": job_key, "error": str(exc)})

    mapping = _UI_REFRESH_JOB_MAP.get(job_key)
    if mapping and status in ("success", "skipped"):
        try:
            domains = mapping.get("domains", [])
            ui_sections = mapping.get("ui_sections", [])
            version_keys = mapping.get("version_keys", [])
            event_id = db.insert_ui_refresh_event(
                job_key=job_key,
                job_status=status,
                domains_json=json_dumps_safe(domains),
                ui_sections_json=json_dumps_safe(ui_sections),
            )
            if version_keys:
                db.bump_ui_refresh_section_versions(
                    version_keys=version_keys,
                    job_key=job_key,
                    job_status=status,
                    event_id=event_id,
                )
        except Exception as exc:
            logger.warning("ui_refresh publish failed", payload={"event": "worker_pool.finalize.ui_refresh_error", "job_key": job_key, "error": str(exc)})


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
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
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
    result = run_registered_processor(context.job_key, context.db, context.raw_config)
    # Back-fill timing if the processor didn't provide it.
    elapsed = int((time.monotonic() - started) * 1000)
    if not result.get("duration_ms"):
        result["duration_ms"] = elapsed
    if not result.get("started_at"):
        result["started_at"] = utc_now_iso()
    if not result.get("finished_at"):
        result["finished_at"] = utc_now_iso()
    result.setdefault("meta", {})
    result["meta"]["execution_language"] = "python"
    result["meta"]["subprocess_invoked"] = False
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
    idle_sleep = max(0, int(worker_settings.get("idle_sleep_seconds", 2)))
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
            reaped = db.reap_stale_running_jobs()
            if reaped > 0:
                logger.info("reaped stale running jobs", payload={"event": "worker_pool.reaped_stale", "worker_id": worker_id, "reaped": reaped})
            seed_result = db.queue_due_recurring_jobs(WORKER_JOB_DEFINITIONS)

            # ── DAG-aware dispatch ────────────────────────────────────
            # 1. Gather current state from the database.
            running_keys = db.get_running_job_keys()
            completed_keys = db.get_recently_completed_job_keys(within_seconds=7200)
            queued_keys = db.get_queued_job_keys()

            # 2. Compute the scheduling plan — which jobs are ready?
            plan = compute_scheduling_plan(
                definitions=WORKER_JOB_DEFINITIONS,
                due_job_keys=queued_keys,
                completed_job_keys=completed_keys,
                running_job_keys=running_keys,
            )

            # 3. Filter by concurrency groups to avoid resource contention.
            graph_nodes = build_graph(WORKER_JOB_DEFINITIONS)
            dispatchable, cg_deferred = filter_by_concurrency_groups(
                plan.ready_jobs, running_keys, graph_nodes,
            )

            # Log scheduling decisions periodically for visibility.
            if plan.blocked_jobs or cg_deferred:
                logger.info(
                    "dag scheduler plan",
                    payload={
                        "event": "worker_pool.dag_plan",
                        "worker_id": worker_id,
                        "ready_count": len(plan.ready_jobs),
                        "dispatchable_count": len(dispatchable),
                        "blocked_count": len(plan.blocked_jobs),
                        "cg_deferred_count": len(cg_deferred),
                        "blocked_jobs": {k: v for k, v in list(plan.blocked_jobs.items())[:10]},
                        "cg_deferred": {k: v for k, v in list(cg_deferred.items())[:10]},
                        "running": sorted(running_keys)[:15],
                    },
                )

            # 4. Claim one job from the dispatchable set.
            job = db.claim_next_worker_job(
                worker_id,
                queues=queue_names,
                workload_classes=workload_classes,
                execution_modes=execution_modes,
                lease_seconds=lease_seconds,
                dispatchable_job_keys=dispatchable if dispatchable else None,
            )
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

        _write_state_file(state_file, {
            "ts": utc_now_iso(),
            "worker_id": worker_id,
            "queues": queue_names,
            "workload_classes": workload_classes,
            "execution_modes": execution_modes,
            "seed_result": seed_result,
            "job": job,
            "memory_usage_bytes": memory_usage,
            "worker_counts": diagnostics,
            "dag_scheduler": {
                "dispatchable": dispatchable[:20] if dispatchable else [],
                "blocked_count": len(plan.blocked_jobs) if plan else 0,
                "running_count": len(running_keys),
            },
        })

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

        job_log = _JobLogWriter(app_root, context.job_key)
        stop_event = threading.Event()
        heartbeat = HeartbeatThread(db, worker_id, int(job.get("id") or 0), lease_seconds, stop_event)
        heartbeat.start()
        try:
            job_log.write_event("worker_pool.job_started", {"worker_id": worker_id, "job_id": int(job.get("id") or 0), "job_key": context.job_key})
            result = _process_job(context)
            db.complete_worker_job(int(job.get("id") or 0), worker_id, result)
            completed_payload = {"event": "worker_pool.job_completed", "worker_id": worker_id, "job_id": int(job.get("id") or 0), "job_key": context.job_key, "status": result.get("status", "success"), "execution_language": "python", "subprocess_invoked": False, "duration_ms": result.get("duration_ms", 0), "rows_processed": result.get("rows_processed", 0), "rows_written": result.get("rows_written", 0)}
            if result.get("status") == "failed":
                completed_payload["error"] = str(result.get("error_text") or result.get("summary") or "")
            logger.info("worker job completed", payload=completed_payload)
            job_log.write_event("worker_pool.job_completed", completed_payload)
            _finalize_job(db, context.job_key, result, logger)
        except Exception as error:  # noqa: BLE001
            fail_result = JobResult.failed(
                job_key=context.job_key,
                error=error,
                meta={"execution_language": "python", "subprocess_invoked": False},
            ).to_dict()
            db.retry_worker_job(int(job.get("id") or 0), worker_id, str(error), retry_backoff, fail_result)
            failed_payload = {"event": "worker_pool.job_failed", "worker_id": worker_id, "job_id": int(job.get("id") or 0), "job_key": context.job_key, "error": str(error)}
            logger.warning("worker job failed", payload=failed_payload)
            job_log.write_event("worker_pool.job_failed", failed_payload)
            _finalize_job(db, context.job_key, fail_result, logger)
            if args.once:
                return 1
        finally:
            stop_event.set()
            heartbeat.join(timeout=1)

        if args.once:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
