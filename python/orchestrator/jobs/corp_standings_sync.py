"""Corporation Standings Sync — ESI /corporations/{id}/standings.

Fetches NPC standings (agents, NPC corps, factions) for each tracked
corporation via ESI.  The standings data is stored in ``corp_standings``
and used by:
  - Theater analysis for side classification (standings-aware inference)
  - Alliance overview for betrayal detection (standing vs dynamic behavior)

Requires scope: ``esi-corporations.read_standings.v1``

This route is part of the ESI rate-limit group ``corp-member``
(300 tokens / 15 min).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job

log = logging.getLogger(__name__)

JOB_KEY = "corp_standings_sync"

# ESI rate-limit group for this endpoint.
_ESI_GROUP = "corp-member"


def _processor(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    from ..esi_gateway import build_gateway

    redis_cfg = dict((raw_config or {}).get("redis") or {})
    gateway = build_gateway(db=db, redis_config=redis_cfg) if redis_cfg.get("enabled") else None

    access_token = db.fetch_latest_esi_access_token()
    warnings: list[str] = []

    if not access_token:
        warnings.append("No active ESI OAuth token found; corporation standings were not fetched.")
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": warnings,
            "summary": "Skipped — no ESI OAuth token available.",
        }

    # Load tracked corporations — these are the user's own corps.
    tracked_corps = db.fetch_all(
        "SELECT corporation_id FROM killmail_tracked_corporations WHERE is_active = 1"
    )
    corp_ids = [int(r["corporation_id"]) for r in tracked_corps if int(r.get("corporation_id") or 0) > 0]

    if not corp_ids:
        warnings.append("No tracked corporations configured. Add your corporation in Settings → Killmail Intelligence.")
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": warnings,
            "summary": "Skipped — no tracked corporations configured.",
        }

    # Ensure the table exists (idempotent).
    db.execute("""
        CREATE TABLE IF NOT EXISTS corp_standings (
            id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            corporation_id  BIGINT UNSIGNED NOT NULL,
            from_id         BIGINT UNSIGNED NOT NULL,
            from_type       ENUM('agent', 'npc_corp', 'faction') NOT NULL,
            standing        DOUBLE NOT NULL,
            fetched_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_corp_from (corporation_id, from_id, from_type),
            KEY idx_corp_type (corporation_id, from_type),
            KEY idx_from_id (from_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    rows_processed = 0
    rows_written = 0
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    for corp_id in corp_ids:
        try:
            standings = _fetch_standings(gateway, corp_id, access_token)
        except Exception as exc:
            warnings.append(f"Failed to fetch standings for corporation {corp_id}: {exc}")
            continue

        rows_processed += len(standings)

        if not standings:
            log.info("Corporation %d returned 0 standings entries.", corp_id)
            continue

        written = _upsert_standings(db, corp_id, standings, now_sql)
        rows_written += written

        # Update sync state for this corporation.
        db.execute(
            """
            INSERT INTO sync_state (dataset_key, sync_mode, status, last_success_at, last_row_count)
            VALUES (%s, 'full', 'success', UTC_TIMESTAMP(), %s)
            ON DUPLICATE KEY UPDATE
                status = 'success',
                last_success_at = UTC_TIMESTAMP(),
                last_row_count = VALUES(last_row_count),
                updated_at = UTC_TIMESTAMP()
            """,
            [f"corp_standings.{corp_id}", written],
        )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Synced standings for {len(corp_ids)} corporation(s) ({rows_written} upserts).",
        "meta": {"corporation_ids": corp_ids},
    }


def _fetch_standings(gateway, corp_id: int, access_token: str) -> list[dict[str, Any]]:
    """Fetch all pages of standings for a corporation."""
    if gateway is None:
        log.warning("No ESI gateway available; skipping standings fetch for %d.", corp_id)
        return []

    all_standings: list[dict[str, Any]] = []
    path = f"/latest/corporations/{corp_id}/standings/"

    responses = gateway.get_paginated(
        path,
        access_token=access_token,
        group=_ESI_GROUP,
        route_template="/corporations/{corporation_id}/standings/",
        identity=f"corp:{corp_id}",
        max_pages=10,
    )

    for resp in responses:
        if resp.status_code == 304:
            continue
        if resp.status_code != 200:
            log.warning("ESI standings for corp %d returned %d", corp_id, resp.status_code)
            continue
        body = resp.body
        if isinstance(body, list):
            all_standings.extend(body)

    return all_standings


def _upsert_standings(
    db: SupplyCoreDb,
    corp_id: int,
    standings: list[dict[str, Any]],
    fetched_at: str,
) -> int:
    """Upsert standings into corp_standings table. Returns rows written."""
    written = 0

    for entry in standings:
        from_id = int(entry.get("from_id") or 0)
        from_type = str(entry.get("from_type") or "").strip()
        standing = float(entry.get("standing") or 0)

        if from_id <= 0 or from_type not in ("agent", "npc_corp", "faction"):
            continue

        db.execute(
            """
            INSERT INTO corp_standings (corporation_id, from_id, from_type, standing, fetched_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                standing = VALUES(standing),
                fetched_at = VALUES(fetched_at),
                updated_at = CURRENT_TIMESTAMP
            """,
            [corp_id, from_id, from_type, standing, fetched_at],
        )
        written += 1

    return written


def run_corp_standings_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key=JOB_KEY,
        phase="A",
        objective="corporation standings",
        processor=lambda d: _processor(d, raw_config),
    )
