from __future__ import annotations

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    """Retired — doctrine AI briefings were sourced from
    ``doctrine_fit_activity_1d`` and written into ``doctrine_ai_briefings``,
    both of which have been dropped along with the hand-maintained doctrine
    system. We keep a heartbeat intelligence_snapshots row so downstream
    freshness probes stay green.
    """
    db.upsert_intelligence_snapshot(
        snapshot_key="doctrine_ai_briefings_status",
        payload_json=json_dumps_safe({"row_count": 0, "retired": True}),
        metadata_json=json_dumps_safe({"source": "retired", "job": "rebuild_ai_briefings"}),
        expires_seconds=1200,
    )
    return {
        "rows_processed": 0,
        "rows_written": 0,
        "warnings": ["rebuild_ai_briefings is retired; doctrine AI briefings are no longer generated."],
        "summary": "rebuild_ai_briefings retired — no-op.",
        "meta": {"entity_type": "fit", "snapshot_key": "doctrine_ai_briefings_status", "retired": True},
    }


def run_rebuild_ai_briefings(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="rebuild_ai_briefings", phase="C", objective="ai briefings", processor=_processor)
