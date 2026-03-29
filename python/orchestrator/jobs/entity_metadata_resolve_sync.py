"""Resolve pending/failed/expired entities in entity_metadata_cache.

Calls the PHP bridge to run ESI lookups for entities that were not resolved
during killmail ingest (due to ESI timeouts, rate limits, or errors).
This ensures page-load paths never need to make ESI network calls.
"""
from __future__ import annotations

from ..bridge import PhpBridge
from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


BATCH_SIZE = 500
RETRY_AFTER_MINUTES = 30


def _processor(db: SupplyCoreDb, bridge: PhpBridge) -> dict[str, object]:
    result = bridge.call("resolve-pending-entities", payload={
        "batch_size": BATCH_SIZE,
        "retry_after_minutes": RETRY_AFTER_MINUTES,
    })

    stats = result.get("result", {})
    total_pending = int(stats.get("total_pending", 0))
    resolved = int(stats.get("resolved", 0))
    failed = int(stats.get("failed", 0))

    # Count remaining pending for reporting
    remaining = db.fetch_scalar(
        "SELECT COUNT(*) FROM entity_metadata_cache "
        "WHERE resolution_status IN ('pending', 'failed') "
        "AND entity_type IN ('alliance', 'corporation', 'character')"
    )

    return {
        "rows_processed": total_pending,
        "rows_written": resolved,
        "rows_failed": failed,
        "warnings": [],
        "summary": (
            f"Resolved {resolved}/{total_pending} pending entities "
            f"({failed} failed, {remaining} still pending)."
        ),
        "meta": {
            "total_pending": total_pending,
            "resolved": resolved,
            "failed": failed,
            "remaining_pending": remaining,
        },
    }


def run_entity_metadata_resolve_sync(db: SupplyCoreDb, bridge: PhpBridge) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="entity_metadata_resolve_sync",
        phase="A",
        objective="Resolve pending entity metadata via ESI",
        processor=lambda d: _processor(d, bridge),
    )
