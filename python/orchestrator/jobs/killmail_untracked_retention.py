"""Retention cleanup for untracked killmails.

Deletes killmail_events with mail_type='untracked' older than 90 days.
Tracked killmails (kill, loss, opponent_kill, opponent_loss, third_party)
are kept indefinitely.

Attackers and items are cascade-deleted via the sequence_id FK, or cleaned
up explicitly if no FK cascade is configured.
"""

from __future__ import annotations

import logging
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job

logger = logging.getLogger(__name__)

RETENTION_DAYS = 90
BATCH_LIMIT = 5000


def _processor(db: SupplyCoreDb) -> dict[str, Any]:
    """Delete untracked killmails older than RETENTION_DAYS in bounded batches."""
    total_deleted_events = 0
    total_deleted_attackers = 0
    total_deleted_items = 0

    while True:
        # Find sequences to delete
        rows = db.fetch_all(
            """
            SELECT sequence_id FROM killmail_events
            WHERE mail_type = 'untracked'
              AND killmail_time < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
            LIMIT %s
            """,
            (RETENTION_DAYS, BATCH_LIMIT),
        )
        if not rows:
            break

        sequence_ids = [int(r["sequence_id"]) for r in rows]
        placeholders = ",".join(["%s"] * len(sequence_ids))

        # Delete attackers and items first (child rows)
        deleted_attackers = db.execute(
            f"DELETE FROM killmail_attackers WHERE sequence_id IN ({placeholders})",
            tuple(sequence_ids),
        )
        total_deleted_attackers += deleted_attackers

        deleted_items = db.execute(
            f"DELETE FROM killmail_items WHERE sequence_id IN ({placeholders})",
            tuple(sequence_ids),
        )
        total_deleted_items += deleted_items

        # Delete the events
        deleted_events = db.execute(
            f"DELETE FROM killmail_events WHERE sequence_id IN ({placeholders})",
            tuple(sequence_ids),
        )
        total_deleted_events += deleted_events

        if deleted_events < BATCH_LIMIT:
            break

    total_deleted = total_deleted_events + total_deleted_attackers + total_deleted_items
    summary = (
        f"Retention cleanup: deleted {total_deleted_events} untracked killmails "
        f"older than {RETENTION_DAYS}d "
        f"(+ {total_deleted_attackers} attackers, {total_deleted_items} items)."
    )
    logger.info(summary)

    return {
        "rows_processed": total_deleted_events,
        "rows_written": total_deleted,
        "warnings": [],
        "summary": summary,
        "meta": {
            "dataset_key": "killmail_untracked_retention",
            "retention_days": RETENTION_DAYS,
            "deleted_events": total_deleted_events,
            "deleted_attackers": total_deleted_attackers,
            "deleted_items": total_deleted_items,
        },
    }


def run_killmail_untracked_retention(db: SupplyCoreDb) -> dict[str, Any]:
    return run_sync_phase_job(
        db,
        job_key="killmail_untracked_retention",
        phase="A",
        objective="prune untracked killmails older than 90 days",
        processor=_processor,
    )
