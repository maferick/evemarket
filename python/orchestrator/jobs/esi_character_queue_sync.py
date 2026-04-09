"""Populate the ESI character queue from all killmail participants.

Every character seen in any killmail (attacker or victim, all mail types) is a
character of interest for the intelligence corpus.  Their character IDs are
staged in ``esi_character_queue`` for subsequent ESI lookup (affiliation,
corporation history, alliance history).

This job is the **repair/backfill path**.  The primary discovery path is inline
queueing at killmail ingest time (see ``python_bridge_process_killmail_batch``).
"""
from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    # Insert all distinct character IDs from killmails — both attackers and
    # victims, across all mail types.  INSERT IGNORE skips already-queued IDs.
    attackers_written = db.execute(
        """INSERT IGNORE INTO esi_character_queue (character_id, first_queue_reason, last_queue_reason)
           SELECT DISTINCT ka.character_id, 'periodic_sync', 'periodic_sync'
           FROM killmail_attackers ka
           WHERE ka.character_id IS NOT NULL
             AND ka.character_id > 0"""
    )
    victims_written = db.execute(
        """INSERT IGNORE INTO esi_character_queue (character_id, first_queue_reason, last_queue_reason)
           SELECT DISTINCT ke.victim_character_id, 'periodic_sync', 'periodic_sync'
           FROM killmail_events ke
           WHERE ke.victim_character_id IS NOT NULL
             AND ke.victim_character_id > 0"""
    )
    rows_written = attackers_written + victims_written
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
        "summary": f"Queued {rows_written} new character IDs ({attackers_written} attackers, {victims_written} victims) for ESI lookup ({total_pending} pending, {total_done} done).",
        "meta": {"pending": total_pending, "done": total_done, "attackers_written": attackers_written, "victims_written": victims_written},
    }


def run_esi_character_queue_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="esi_character_queue_sync",
        phase="A",
        objective="ESI character queue population",
        processor=_processor,
    )
