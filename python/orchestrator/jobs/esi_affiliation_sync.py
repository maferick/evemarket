"""Bulk-fetch current corporation/alliance affiliation for all known characters.

Uses ESI ``POST /v2/characters/affiliation/`` to resolve the current
corporation, alliance, and faction for every character in the intelligence
corpus.  The endpoint accepts up to 1000 character IDs per call and requires
no authentication.

Characters are prioritised by refresh tier (hot > warm > cold) and staleness
so that actively-observed pilots are refreshed most frequently.  When an
alliance change is detected the character is flagged for a full history
re-fetch.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from ..db import SupplyCoreDb
from ..esi_client import EsiClient
from ..esi_rate_limiter import shared_limiter
from ..job_result import JobResult

logger = logging.getLogger("supplycore.esi_affiliation_sync")

ESI_USER_AGENT = "SupplyCore intelligence-pipeline/1.0 (affiliation-sync)"
ESI_AFFILIATION_PATH = "/v2/characters/affiliation/"
BATCH_SIZE = 1000
MAX_IDS_PER_RUN = 10_000
MAX_RETRIES_PER_BATCH = 3
RETRY_BASE_DELAY_SECONDS = 2.0

# Refresh-tier thresholds (hours) — characters are stale when their
# fetched_at is older than these intervals.
_TIER_THRESHOLDS_HOURS = {
    "hot": 1,
    "warm": 6,
    "cold": 24,
}

# Module-level EsiClient — shared across all calls within this job.
_esi_client = EsiClient(
    user_agent=ESI_USER_AGENT,
    timeout_seconds=30,
    limiter=shared_limiter,
)


def _fetch_stale_character_ids(db: SupplyCoreDb) -> list[int]:
    """Return character IDs that need an affiliation refresh.

    Selects characters that either have no affiliation row at all, or
    whose ``fetched_at`` is older than their refresh tier allows.
    Orders by tier priority (hot first) then staleness (oldest first).
    """
    rows = db.fetch_all(
        """SELECT q.character_id
           FROM esi_character_queue q
           LEFT JOIN character_current_affiliation a
               ON a.character_id = q.character_id
           WHERE a.character_id IS NULL
              OR (a.refresh_tier = 'hot'  AND a.fetched_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %(hot)s HOUR))
              OR (a.refresh_tier = 'warm' AND a.fetched_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %(warm)s HOUR))
              OR (a.refresh_tier = 'cold' AND a.fetched_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %(cold)s HOUR))
           ORDER BY
               FIELD(COALESCE(a.refresh_tier, 'hot'), 'hot', 'warm', 'cold') ASC,
               a.fetched_at ASC
           LIMIT %(limit)s""",
        {
            "hot": _TIER_THRESHOLDS_HOURS["hot"],
            "warm": _TIER_THRESHOLDS_HOURS["warm"],
            "cold": _TIER_THRESHOLDS_HOURS["cold"],
            "limit": MAX_IDS_PER_RUN,
        },
    )
    return [int(r["character_id"]) for r in rows]


def _post_affiliation_batch(character_ids: list[int]) -> list[dict[str, Any]] | None:
    """POST a batch of character IDs to ESI and return the parsed response.

    Retries on 429 (rate-limited) and 5xx responses with exponential
    back-off.  Returns ``None`` if all retries are exhausted.
    """
    for attempt in range(MAX_RETRIES_PER_BATCH):
        resp = _esi_client.post(ESI_AFFILIATION_PATH, body=character_ids)

        if resp.ok and isinstance(resp.body, list):
            return resp.body

        if resp.is_rate_limited:
            retry_after = 1.0
            try:
                retry_after = float(resp.headers.get("Retry-After", "1"))
            except (ValueError, TypeError):
                pass
            logger.warning(
                "ESI 429 on affiliation batch (attempt %d/%d), sleeping %.1fs",
                attempt + 1, MAX_RETRIES_PER_BATCH, retry_after,
            )
            time.sleep(retry_after)
            continue

        if resp.status_code >= 500:
            delay = RETRY_BASE_DELAY_SECONDS * (2 ** attempt)
            logger.warning(
                "ESI %d on affiliation batch (attempt %d/%d), retrying in %.1fs",
                resp.status_code, attempt + 1, MAX_RETRIES_PER_BATCH, delay,
            )
            time.sleep(delay)
            continue

        # Non-retryable client error (4xx other than 429).
        logger.error(
            "ESI %d on affiliation batch — not retryable, skipping batch of %d IDs",
            resp.status_code, len(character_ids),
        )
        return None

    logger.error(
        "Exhausted %d retries for affiliation batch of %d IDs",
        MAX_RETRIES_PER_BATCH, len(character_ids),
    )
    return None


def _compute_refresh_tier(db: SupplyCoreDb, character_id: int) -> str:
    """Determine the refresh tier based on last killmail activity.

    - hot:  killmail within the last 7 days
    - warm: killmail within the last 30 days
    - cold: everything else (or no killmail data)
    """
    last_km = db.fetch_scalar(
        """SELECT MAX(ke.killmail_time) FROM killmail_events ke
           LEFT JOIN killmail_attackers ka ON ka.killmail_id = ke.killmail_id
           WHERE ke.victim_character_id = %s OR ka.character_id = %s""",
        (character_id, character_id),
    )
    if last_km is None:
        return "cold"

    is_hot = db.fetch_scalar(
        "SELECT %s >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)", (last_km,)
    )
    if is_hot:
        return "hot"

    is_warm = db.fetch_scalar(
        "SELECT %s >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)", (last_km,)
    )
    if is_warm:
        return "warm"

    return "cold"


def _bulk_compute_refresh_tiers(db: SupplyCoreDb, character_ids: list[int]) -> dict[int, str]:
    """Compute refresh tiers for a set of character IDs in bulk.

    Queries the most recent killmail timestamp for each character and
    classifies into hot (<=7d), warm (<=30d), or cold (>30d / none).
    """
    if not character_ids:
        return {}

    tiers: dict[int, str] = {}

    # Process in sub-batches to keep IN-clause size reasonable.
    for i in range(0, len(character_ids), 500):
        sub_batch = character_ids[i : i + 500]
        placeholders = ",".join(["%s"] * len(sub_batch))

        # Find the most recent killmail time per character across both
        # victim and attacker roles.
        rows = db.fetch_all(
            f"""SELECT character_id,
                       CASE
                           WHEN last_km >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY) THEN 'hot'
                           WHEN last_km >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY) THEN 'warm'
                           ELSE 'cold'
                       END AS tier
                FROM (
                    SELECT character_id, MAX(km_time) AS last_km
                    FROM (
                        SELECT ke.victim_character_id AS character_id, ke.killmail_time AS km_time
                        FROM killmail_events ke
                        WHERE ke.victim_character_id IN ({placeholders})
                        UNION ALL
                        SELECT ka.character_id, ke2.killmail_time AS km_time
                        FROM killmail_attackers ka
                        JOIN killmail_events ke2 ON ke2.killmail_id = ka.killmail_id
                        WHERE ka.character_id IN ({placeholders})
                    ) AS combined
                    GROUP BY character_id
                ) AS latest""",
            tuple(sub_batch) + tuple(sub_batch),
        )
        for row in rows:
            tiers[int(row["character_id"])] = row["tier"]

    # Default to cold for any IDs not found in killmail data.
    for cid in character_ids:
        if cid not in tiers:
            tiers[cid] = "cold"

    return tiers


def _upsert_affiliations(
    db: SupplyCoreDb,
    esi_results: list[dict[str, Any]],
) -> tuple[int, int, list[int]]:
    """Upsert affiliation results and detect alliance changes.

    Returns (rows_written, alliance_changes, changed_character_ids).
    """
    rows_written = 0
    alliance_changes = 0
    changed_ids: list[int] = []

    # Collect all character IDs from the ESI response for bulk tier computation.
    all_char_ids = [int(r["character_id"]) for r in esi_results if r.get("character_id")]
    tiers = _bulk_compute_refresh_tiers(db, all_char_ids)

    for entry in esi_results:
        character_id = int(entry.get("character_id") or 0)
        if character_id <= 0:
            continue

        corporation_id = int(entry.get("corporation_id") or 0) or None
        alliance_id = int(entry.get("alliance_id") or 0) or None
        faction_id = int(entry.get("faction_id") or 0) or None
        refresh_tier = tiers.get(character_id, "cold")

        # Fetch previous affiliation for change detection.
        prev = db.fetch_one(
            """SELECT corporation_id, alliance_id
               FROM character_current_affiliation
               WHERE character_id = %s""",
            (character_id,),
        )
        prev_corporation_id = int(prev["corporation_id"]) if prev and prev.get("corporation_id") else None
        prev_alliance_id = int(prev["alliance_id"]) if prev and prev.get("alliance_id") else None

        alliance_changed = (prev is not None) and (prev_alliance_id != alliance_id)

        needs_history_refresh = 1 if alliance_changed else 0

        db.execute(
            """INSERT INTO character_current_affiliation
                   (character_id, corporation_id, alliance_id, faction_id,
                    prev_corporation_id, prev_alliance_id,
                    refresh_tier, needs_history_refresh, fetched_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP())
               ON DUPLICATE KEY UPDATE
                   prev_corporation_id = corporation_id,
                   prev_alliance_id = alliance_id,
                   corporation_id = VALUES(corporation_id),
                   alliance_id = VALUES(alliance_id),
                   faction_id = VALUES(faction_id),
                   refresh_tier = VALUES(refresh_tier),
                   needs_history_refresh = CASE
                       WHEN alliance_id IS NULL AND VALUES(alliance_id) IS NULL THEN 0
                       WHEN alliance_id <> VALUES(alliance_id) THEN 1
                       WHEN alliance_id IS NULL AND VALUES(alliance_id) IS NOT NULL THEN 1
                       WHEN alliance_id IS NOT NULL AND VALUES(alliance_id) IS NULL THEN 1
                       ELSE needs_history_refresh
                   END,
                   fetched_at = UTC_TIMESTAMP()""",
            (
                character_id, corporation_id, alliance_id, faction_id,
                prev_corporation_id, prev_alliance_id,
                refresh_tier, needs_history_refresh,
            ),
        )
        rows_written += 1

        if alliance_changed:
            alliance_changes += 1
            changed_ids.append(character_id)

    return rows_written, alliance_changes, changed_ids


def _flag_changed_characters(db: SupplyCoreDb, character_ids: list[int]) -> int:
    """Re-queue characters whose alliance changed for history re-fetch."""
    if not character_ids:
        return 0

    updated = 0
    for cid in character_ids:
        updated += db.execute(
            """UPDATE esi_character_queue
               SET fetch_status = 'pending',
                   last_queue_reason = 'affiliation_changed'
               WHERE character_id = %s""",
            (cid,),
        )
    return updated


def run_esi_affiliation_sync(db: SupplyCoreDb, raw_config: dict | None = None) -> dict:
    """Bulk-fetch current corp/alliance affiliation for all known characters."""
    started = time.perf_counter()

    # 1. Gather stale character IDs.
    character_ids = _fetch_stale_character_ids(db)

    if not character_ids:
        return JobResult.success(
            job_key="esi_affiliation_sync",
            summary="No characters need affiliation refresh.",
            rows_processed=0,
            rows_written=0,
            duration_ms=int((time.perf_counter() - started) * 1000),
        ).to_dict()

    # 2. Batch into groups of 1000 and POST to ESI.
    total_written = 0
    total_alliance_changes = 0
    total_requeued = 0
    total_errors = 0
    batches_completed = 0
    all_changed_ids: list[int] = []

    for batch_start in range(0, len(character_ids), BATCH_SIZE):
        batch = character_ids[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(character_ids) + BATCH_SIZE - 1) // BATCH_SIZE

        logger.info(
            "Affiliation batch %d/%d: posting %d character IDs",
            batch_num, total_batches, len(batch),
        )

        # 3. POST to ESI.
        esi_results = _post_affiliation_batch(batch)
        if esi_results is None:
            total_errors += len(batch)
            logger.error("Batch %d/%d failed — skipping", batch_num, total_batches)
            continue

        # 4. Upsert results and detect changes.
        written, changes, changed_ids = _upsert_affiliations(db, esi_results)
        total_written += written
        total_alliance_changes += changes
        all_changed_ids.extend(changed_ids)
        batches_completed += 1

    # 5. Re-queue characters whose alliance changed.
    if all_changed_ids:
        total_requeued = _flag_changed_characters(db, all_changed_ids)
        logger.info(
            "Flagged %d characters for history re-fetch due to alliance change",
            total_requeued,
        )

    duration_ms = int((time.perf_counter() - started) * 1000)

    summary = (
        f"Processed {len(character_ids)} characters in {batches_completed} batches: "
        f"{total_written} upserted, {total_alliance_changes} alliance changes detected, "
        f"{total_requeued} re-queued ({total_errors} errors)."
    )
    logger.info(summary)

    return JobResult.success(
        job_key="esi_affiliation_sync",
        summary=summary,
        rows_processed=len(character_ids),
        rows_written=total_written,
        rows_failed=total_errors,
        batches_completed=batches_completed,
        duration_ms=duration_ms,
        meta={
            "total_ids": len(character_ids),
            "batches_completed": batches_completed,
            "alliance_changes": total_alliance_changes,
            "requeued": total_requeued,
            "errors": total_errors,
        },
    ).to_dict()
