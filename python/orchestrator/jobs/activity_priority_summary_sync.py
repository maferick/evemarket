from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    """Retired — this sync used to fan doctrine_fit_activity_1d rows out
    into doctrine_activity_snapshots, both of which have been dropped.
    It is replaced by the auto doctrine pipeline (compute_auto_doctrines +
    the auto_doctrine_fit_demand_1d rollup).

    We still materialise an empty intelligence_snapshots row so the
    freshness tracker keeps a heartbeat for activity_priority consumers.
    """
    snapshot_payload = {
        "active_doctrines": [],
        "total_rows": 0,
        "retired": True,
    }
    db.upsert_intelligence_snapshot(
        snapshot_key="doctrine_activity_db_state",
        payload_json=json_dumps_safe(snapshot_payload),
        metadata_json=json_dumps_safe({
            "source": "retired",
            "reason": "scheduler:python",
            "row_count": 0,
        }),
        expires_seconds=900,
    )

    return {
        "rows_processed": 0,
        "rows_written": 0,
        "warnings": ["activity_priority_summary_sync is retired; auto_doctrines feeds /doctrines/ directly."],
        "summary": "activity_priority_summary_sync retired — no-op.",
        "meta": {"entity_type": "fit", "snapshot_key": "activity_priority_summaries", "retired": True},
    }


def run_activity_priority_summary_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="activity_priority_summary_sync", phase="B", objective="activity summaries", processor=_processor)
