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

    # Seed character_killmail_queue from the same participant set so the
    # per-character zKillboard backfill job can pick them up.  This closes
    # the historical gap: prior to this, inline ingest only wrote to
    # esi_character_queue and participants were never enrolled for backfill.
    km_attackers = db.execute(
        """INSERT IGNORE INTO character_killmail_queue
               (character_id, priority, priority_reason, mode)
           SELECT DISTINCT ka.character_id, 1.0000, 'repair_backfill', 'backfill'
           FROM killmail_attackers ka
           WHERE ka.character_id IS NOT NULL
             AND ka.character_id > 0"""
    )
    km_victims = db.execute(
        """INSERT IGNORE INTO character_killmail_queue
               (character_id, priority, priority_reason, mode)
           SELECT DISTINCT ke.victim_character_id, 1.0000, 'repair_backfill', 'backfill'
           FROM killmail_events ke
           WHERE ke.victim_character_id IS NOT NULL
             AND ke.victim_character_id > 0"""
    )
    km_queue_written = km_attackers + km_victims

    total_pending = db.fetch_scalar(
        "SELECT COUNT(*) FROM esi_character_queue WHERE fetch_status = 'pending'"
    )
    total_done = db.fetch_scalar(
        "SELECT COUNT(*) FROM esi_character_queue WHERE fetch_status = 'done'"
    )
    km_queue_total = db.fetch_scalar(
        "SELECT COUNT(*) FROM character_killmail_queue"
    )
    return {
        "rows_processed": total_pending + total_done,
        "rows_written": rows_written + km_queue_written,
        "warnings": [],
        "summary": (
            f"Queued {rows_written} new ESI character IDs "
            f"({attackers_written} attackers, {victims_written} victims); "
            f"enrolled {km_queue_written} new participants for per-character killmail backfill. "
            f"ESI queue: {total_pending} pending, {total_done} done. "
            f"Killmail queue: {km_queue_total} total."
        ),
        "meta": {
            "pending": total_pending,
            "done": total_done,
            "attackers_written": attackers_written,
            "victims_written": victims_written,
            "killmail_queue_attackers_written": km_attackers,
            "killmail_queue_victims_written": km_victims,
            "killmail_queue_total": km_queue_total,
        },
    }


def run_esi_character_queue_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="esi_character_queue_sync",
        phase="A",
        objective="ESI character queue population",
        processor=_processor,
    )
