from __future__ import annotations

import logging

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job

logger = logging.getLogger(__name__)

_STALE_RUN_GRACE_SECONDS = 900  # 15 minutes — generous for streaming jobs


def _reap_stale_sync_runs(db: SupplyCoreDb) -> int:
    """Mark orphaned 'running' sync_runs as failed so they stop appearing as stuck."""
    return db.execute(
        """UPDATE sync_runs
           SET run_status = 'failed',
               finished_at = UTC_TIMESTAMP(),
               error_message = 'Reaped: run exceeded timeout while still marked as running (worker likely crashed).',
               updated_at = CURRENT_TIMESTAMP
           WHERE run_status = 'running'
             AND started_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s SECOND)""",
        (_STALE_RUN_GRACE_SECONDS,),
    )


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    reaped = _reap_stale_sync_runs(db)
    if reaped > 0:
        logger.info("Reaped %d stale sync_runs row(s).", reaped)

    rows_processed = db.fetch_scalar("SELECT COUNT(*) FROM sync_schedules")
    rows_written = db.execute(
        """INSERT INTO scheduler_job_current_status (
                job_key, dataset_key, latest_status, last_started_at, last_finished_at, last_success_at, last_failure_at,
                last_failure_message, current_pressure_state, last_event_at
            )
            SELECT
                job_key,
                CONCAT('scheduler.job.', job_key) AS dataset_key,
                COALESCE(last_status, 'unknown') AS latest_status,
                last_started_at,
                last_finished_at,
                CASE WHEN last_status = 'success' THEN last_finished_at ELSE NULL END,
                CASE WHEN last_status = 'failed' THEN last_finished_at ELSE NULL END,
                CASE WHEN last_status = 'failed' THEN LEFT(COALESCE(last_error, ''), 500) ELSE NULL END,
                CASE
                    WHEN COALESCE(last_status, 'unknown') = 'failed' THEN 'elevated'
                    WHEN locked_until IS NOT NULL AND locked_until > UTC_TIMESTAMP() THEN 'busy'
                    ELSE 'healthy'
                END AS current_pressure_state,
                COALESCE(last_finished_at, last_started_at) AS last_event_at
            FROM sync_schedules
            ON DUPLICATE KEY UPDATE
                latest_status = VALUES(latest_status),
                last_started_at = VALUES(last_started_at),
                last_finished_at = VALUES(last_finished_at),
                last_success_at = CASE WHEN VALUES(latest_status) = 'success' THEN VALUES(last_finished_at) ELSE scheduler_job_current_status.last_success_at END,
                last_failure_at = CASE WHEN VALUES(latest_status) = 'failed' THEN VALUES(last_finished_at) ELSE scheduler_job_current_status.last_failure_at END,
                last_failure_message = VALUES(last_failure_message),
                current_pressure_state = VALUES(current_pressure_state),
                last_event_at = VALUES(last_event_at)"""
    )
    payload = {"rows_processed": rows_processed, "rows_written": rows_written, "reaped_stale_runs": reaped}
    db.upsert_intelligence_snapshot(
        snapshot_key="scheduler_current_state",
        payload_json=json_dumps_safe(payload),
        metadata_json=json_dumps_safe({"source": "sync_schedules"}),
        expires_seconds=600,
    )
    summary = f"Refreshed scheduler current-state rows for {rows_written} jobs."
    if reaped > 0:
        summary += f" Reaped {reaped} stale sync_runs."
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [],
        "summary": summary,
        "meta": {"snapshot_key": "scheduler_current_state", "reaped_stale_runs": reaped},
    }


def run_current_state_refresh_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="current_state_refresh_sync", phase="A", objective="current state refresh", processor=_processor)
