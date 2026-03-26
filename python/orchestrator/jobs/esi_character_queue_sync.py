"""Populate the ESI character queue from killmail attacker data.

Every attacker on every lossmail of a tracked member is a character of interest
for the historical alliance overlap feature.  Their character IDs are staged in
``esi_character_queue`` for subsequent ESI lookup.
"""
from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    # Insert distinct attacker character IDs from loss killmails that are not
    # already queued or fetched.
    rows_written = db.execute(
        """INSERT IGNORE INTO esi_character_queue (character_id)
           SELECT DISTINCT ka.character_id
           FROM killmail_attackers ka
           INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
           WHERE ke.mail_type = 'loss'
             AND ka.character_id IS NOT NULL
             AND ka.character_id > 0"""
    )
    total_pending = db.fetch_scalar(
        "SELECT COUNT(*) FROM esi_character_queue WHERE fetch_status = 'pending'"
    )
    total_done = db.fetch_scalar(
        "SELECT COUNT(*) FROM esi_character_queue WHERE fetch_status = 'done'"
    )
    return {
        "rows_processed": total_pending + total_done,
        "rows_written": rows_written,
        "warnings": [],
        "summary": f"Queued {rows_written} new attacker character IDs for ESI lookup ({total_pending} pending, {total_done} done).",
        "meta": {"pending": total_pending, "done": total_done},
    }


def run_esi_character_queue_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="esi_character_queue_sync",
        phase="A",
        objective="ESI character queue population",
        processor=_processor,
    )
