"""Corporation Standings & Contacts Sync.

Fetches two ESI endpoints for each tracked corporation:

1. ``/corporations/{id}/standings`` — NPC standings (agents, NPC corps, factions).
   Scope: ``esi-corporations.read_standings.v1``

2. ``/corporations/{id}/contacts/`` — Player contacts (characters, corps, alliances).
   Scope: ``esi-corporations.read_contacts.v1``

The contacts data is the primary source for determining which player alliances
and corporations are friendly or hostile from the in-game diplomatic perspective.
The standings data supplements this with NPC faction alignment.

Both routes are part of the ESI rate-limit group ``corp-member``
(300 tokens / 15 min).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job

log = logging.getLogger(__name__)

JOB_KEY = "corp_standings_sync"

# ESI rate-limit group for both endpoints.
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

    # Fall back to the ESI token's character → corporation when no tracked corps exist
    if not corp_ids:
        token_row = db.fetch_one(
            "SELECT character_id FROM esi_oauth_tokens ORDER BY updated_at DESC, id DESC LIMIT 1"
        )
        character_id = int((token_row or {}).get("character_id") or 0)
        if character_id > 0 and gateway is not None:
            try:
                resp = gateway.get(
                    f"/latest/characters/{character_id}/",
                    access_token=None,
                    group="esi-characters",
                    route_template="/characters/{character_id}/",
                    identity=f"char:{character_id}",
                )
                if resp.status_code == 200 and isinstance(resp.body, dict):
                    derived_corp = int(resp.body.get("corporation_id") or 0)
                    if derived_corp > 0:
                        corp_ids = [derived_corp]
                        log.info("No tracked corporations; derived corp %d from ESI character %d.", derived_corp, character_id)
            except Exception as exc:
                warnings.append(f"Failed to derive corporation from ESI character {character_id}: {exc}")

    if not corp_ids:
        warnings.append("No tracked corporations configured and could not derive corporation from ESI token. Add your corporation in Settings → Killmail Intelligence.")
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": warnings,
            "summary": "Skipped — no tracked corporations configured.",
        }

    _ensure_tables(db)

    rows_processed = 0
    rows_written = 0
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    for corp_id in corp_ids:
        # ── NPC standings ────────────────────────────────────────────────
        try:
            standings = _fetch_standings(gateway, corp_id, access_token)
            rows_processed += len(standings)
            if standings:
                written = _upsert_standings(db, corp_id, standings, now_sql)
                rows_written += written
                log.info("Corp %d: %d NPC standings upserted.", corp_id, written)
            else:
                log.info("Corp %d: 0 NPC standings entries.", corp_id)
        except Exception as exc:
            warnings.append(f"Failed to fetch NPC standings for corporation {corp_id}: {exc}")

        # ── Player contacts ──────────────────────────────────────────────
        try:
            contacts, contacts_error = _fetch_contacts(gateway, corp_id, access_token)
            rows_processed += len(contacts)
            if contacts:
                written = _upsert_contacts(db, corp_id, contacts, now_sql)
                rows_written += written
                log.info("Corp %d: %d player contacts upserted.", corp_id, written)
            else:
                log.info("Corp %d: 0 player contacts entries.", corp_id)
            if contacts_error:
                warnings.append(contacts_error)

            # Queue contact entity IDs for background name resolution
            _queue_contact_names(db, contacts)
        except Exception as exc:
            warnings.append(f"Failed to fetch contacts for corporation {corp_id}: {exc}")

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
            [f"corp_standings.{corp_id}", rows_written],
        )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Synced standings + contacts for {len(corp_ids)} corporation(s) ({rows_written} upserts).",
        "meta": {"corporation_ids": corp_ids},
    }


# ── Table creation ────────────────────────────────────────────────────────────

def _ensure_tables(db: SupplyCoreDb) -> None:
    """Ensure both corp_standings and corp_contacts tables exist (idempotent)."""
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
    db.execute("""
        CREATE TABLE IF NOT EXISTS corp_contacts (
            id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            corporation_id  BIGINT UNSIGNED NOT NULL,
            contact_id      BIGINT UNSIGNED NOT NULL,
            contact_type    ENUM('character', 'corporation', 'alliance', 'faction') NOT NULL,
            standing        DOUBLE NOT NULL,
            label_ids       JSON DEFAULT NULL,
            source          ENUM('esi', 'manual') NOT NULL DEFAULT 'esi',
            fetched_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_corp_contact (corporation_id, contact_id, contact_type),
            KEY idx_corp_type (corporation_id, contact_type),
            KEY idx_contact_id (contact_id),
            KEY idx_standing (corporation_id, standing),
            KEY idx_source (source)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

    # Ensure source column exists on tables created before the migration.
    try:
        db.execute("""
            ALTER TABLE corp_contacts
                ADD COLUMN source ENUM('esi', 'manual') NOT NULL DEFAULT 'esi' AFTER label_ids,
                ADD KEY idx_source (source)
        """)
    except Exception:
        pass  # Column already exists


# ── NPC standings fetcher ─────────────────────────────────────────────────────

def _fetch_standings(gateway, corp_id: int, access_token: str) -> list[dict[str, Any]]:
    """Fetch all pages of NPC standings for a corporation."""
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
    """Upsert NPC standings into corp_standings table. Returns rows written."""
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


# ── Player contacts fetcher ───────────────────────────────────────────────────

def _fetch_contacts(gateway, corp_id: int, access_token: str) -> tuple[list[dict[str, Any]], str | None]:
    """Fetch all pages of player contacts for a corporation.

    ESI endpoint: GET /corporations/{corporation_id}/contacts/
    Returns: (contacts_list, optional_error_message)
    """
    if gateway is None:
        log.warning("No ESI gateway available; skipping contacts fetch for %d.", corp_id)
        return [], None

    all_contacts: list[dict[str, Any]] = []
    error_msg: str | None = None
    path = f"/latest/corporations/{corp_id}/contacts/"

    responses = gateway.get_paginated(
        path,
        access_token=access_token,
        group=_ESI_GROUP,
        route_template="/corporations/{corporation_id}/contacts/",
        identity=f"corp:{corp_id}",
        max_pages=20,
    )

    for resp in responses:
        if resp.status_code == 304:
            continue
        if resp.status_code in (401, 403):
            error_msg = (
                f"ESI contacts for corporation {corp_id} returned {resp.status_code} — "
                f"token expired or missing scope esi-corporations.read_contacts.v1. "
                f"Re-authenticate with updated scopes in Settings → ESI Auth."
            )
            log.warning("%s", error_msg)
            break
        if resp.status_code != 200:
            log.warning("ESI contacts for corp %d returned %d", corp_id, resp.status_code)
            continue
        body = resp.body
        if isinstance(body, list):
            all_contacts.extend(body)

    return all_contacts, error_msg


def _queue_contact_names(db: SupplyCoreDb, contacts: list[dict[str, Any]]) -> None:
    """Queue contact entity IDs for background name resolution."""
    ids_by_type: dict[str, list[int]] = {}
    resolvable_types = ("character", "corporation", "alliance")
    for entry in contacts:
        contact_id = int(entry.get("contact_id") or 0)
        contact_type = str(entry.get("contact_type") or "").strip()
        if contact_id > 0 and contact_type in resolvable_types:
            ids_by_type.setdefault(contact_type, []).append(contact_id)

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    for entity_type, entity_ids in ids_by_type.items():
        unique_ids = sorted(set(entity_ids))
        for eid in unique_ids:
            db.execute(
                """
                INSERT INTO entity_metadata_cache (entity_type, entity_id, source_system, resolution_status, last_requested_at)
                VALUES (%s, %s, 'queue', 'pending', %s)
                ON DUPLICATE KEY UPDATE
                    last_requested_at = VALUES(last_requested_at)
                """,
                [entity_type, eid, now],
            )
    if ids_by_type:
        total = sum(len(v) for v in ids_by_type.values())
        log.info("Queued %d contact entities for name resolution.", total)


def _upsert_contacts(
    db: SupplyCoreDb,
    corp_id: int,
    contacts: list[dict[str, Any]],
    fetched_at: str,
) -> int:
    """Upsert player contacts into corp_contacts table. Returns rows written."""
    written = 0
    valid_types = ("character", "corporation", "alliance", "faction")

    for entry in contacts:
        contact_id = int(entry.get("contact_id") or 0)
        contact_type = str(entry.get("contact_type") or "").strip()
        standing = float(entry.get("standing") or 0)
        label_ids = entry.get("label_ids")

        if contact_id <= 0 or contact_type not in valid_types:
            continue

        # Serialize label_ids as JSON if present.
        label_json = json.dumps(label_ids) if label_ids else None

        db.execute(
            """
            INSERT INTO corp_contacts (corporation_id, contact_id, contact_type, standing, label_ids, source, fetched_at)
            VALUES (%s, %s, %s, %s, %s, 'esi', %s)
            ON DUPLICATE KEY UPDATE
                standing = IF(source = 'manual', standing, VALUES(standing)),
                label_ids = IF(source = 'manual', label_ids, VALUES(label_ids)),
                fetched_at = IF(source = 'manual', fetched_at, VALUES(fetched_at)),
                updated_at = CURRENT_TIMESTAMP
            """,
            [corp_id, contact_id, contact_type, standing, label_json, fetched_at],
        )
        written += 1

    return written


def run_corp_standings_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key=JOB_KEY,
        phase="A",
        objective="corporation standings & contacts",
        processor=lambda d: _processor(d, raw_config),
    )
