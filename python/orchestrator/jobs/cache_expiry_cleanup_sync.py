from __future__ import annotations

import logging
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job

logger = logging.getLogger(__name__)


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    """Delete expired rows from character org history cache and alliance adjacency snapshots.

    The ``character_org_alliance_adjacency_snapshots`` table has a FK with
    ``ON DELETE CASCADE`` pointing at ``character_org_history_cache``, and
    ``character_org_history_events`` likewise cascades.  We therefore only need
    to delete from the parent table; MariaDB/InnoDB cascades the children.

    To keep lock duration short we delete in bounded batches.
    """
    total_deleted = 0
    batch_limit = 2000

    # -- character_org_history_cache (cascades to events + adjacency snapshots) --
    while True:
        deleted = db.execute(
            "DELETE FROM character_org_history_cache "
            "WHERE expires_at IS NOT NULL AND expires_at < UTC_TIMESTAMP() "
            "LIMIT %s",
            (batch_limit,),
        )
        total_deleted += deleted
        if deleted < batch_limit:
            break

    # -- character_org_alliance_adjacency_snapshots (orphan safety net) --
    # Rows with their own expires_at that may survive if the parent row is
    # still fresh but the snapshot itself is stale.
    adj_deleted = 0
    while True:
        deleted = db.execute(
            "DELETE FROM character_org_alliance_adjacency_snapshots "
            "WHERE expires_at IS NOT NULL AND expires_at < UTC_TIMESTAMP() "
            "LIMIT %s",
            (batch_limit,),
        )
        adj_deleted += deleted
        if deleted < batch_limit:
            break
    total_deleted += adj_deleted

    return {
        "rows_processed": total_deleted,
        "rows_written": total_deleted,
        "warnings": [],
        "summary": f"Deleted {total_deleted} expired cache rows (adj snapshots: {adj_deleted}).",
        "meta": {"dataset_key": "cache_expiry_cleanup"},
    }


def run_cache_expiry_cleanup_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="cache_expiry_cleanup_sync",
        phase="A",
        objective="expired cache cleanup",
        processor=_processor,
    )
