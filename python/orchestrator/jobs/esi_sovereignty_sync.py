"""ESI Sovereignty Sync — three jobs at different cadences.

Public ESI endpoints (no auth required):
- ``GET /sovereignty/campaigns/``  — active entosis fights (~5s cache)
- ``GET /sovereignty/structures/`` — sov structures + ADM + vuln windows (~120s cache)
- ``GET /sovereignty/map/``        — system ownership (~3600s cache)

Jobs:
- ``sovereignty_campaigns_sync``   — 60s interval
- ``sovereignty_structures_sync``  — 180s interval
- ``sovereignty_map_sync``         — 1800s interval
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job

log = logging.getLogger(__name__)

_ESI_GROUP = "esi-sovereignty"

# ── Module-level cache for structure role lookups (per-process lifetime) ──────

_role_cache: dict[int, str] = {}


# ── Shared helpers ───────────────────────────────────────────────────────────

def _ensure_tables(db: SupplyCoreDb) -> None:
    """Ensure all sovereignty tables exist (idempotent)."""
    db.execute("""
        CREATE TABLE IF NOT EXISTS ref_sov_structure_roles (
            structure_type_id   INT UNSIGNED PRIMARY KEY,
            structure_role      VARCHAR(32) NOT NULL,
            is_sov_structure    TINYINT(1) NOT NULL DEFAULT 0,
            notes               VARCHAR(255) DEFAULT NULL,
            created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sovereignty_map (
            system_id           BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            alliance_id         BIGINT UNSIGNED DEFAULT NULL,
            corporation_id      BIGINT UNSIGNED DEFAULT NULL,
            faction_id          BIGINT UNSIGNED DEFAULT NULL,
            owner_entity_id     BIGINT UNSIGNED DEFAULT NULL,
            owner_entity_type   ENUM('alliance','corporation','faction') DEFAULT NULL,
            fetched_at          DATETIME NOT NULL,
            created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_alliance (alliance_id),
            KEY idx_faction (faction_id),
            KEY idx_owner (owner_entity_id, owner_entity_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sovereignty_map_history (
            id                          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            system_id                   BIGINT UNSIGNED NOT NULL,
            previous_alliance_id        BIGINT UNSIGNED DEFAULT NULL,
            new_alliance_id             BIGINT UNSIGNED DEFAULT NULL,
            previous_corporation_id     BIGINT UNSIGNED DEFAULT NULL,
            new_corporation_id          BIGINT UNSIGNED DEFAULT NULL,
            previous_faction_id         BIGINT UNSIGNED DEFAULT NULL,
            new_faction_id              BIGINT UNSIGNED DEFAULT NULL,
            previous_owner_entity_id    BIGINT UNSIGNED DEFAULT NULL,
            previous_owner_entity_type  ENUM('alliance','corporation','faction') DEFAULT NULL,
            new_owner_entity_id         BIGINT UNSIGNED DEFAULT NULL,
            new_owner_entity_type       ENUM('alliance','corporation','faction') DEFAULT NULL,
            changed_at                  DATETIME NOT NULL,
            created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_system (system_id, changed_at),
            KEY idx_new_alliance (new_alliance_id),
            KEY idx_previous_alliance (previous_alliance_id),
            KEY idx_new_owner (new_owner_entity_id, new_owner_entity_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sovereignty_structures (
            structure_id                    BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            alliance_id                     BIGINT UNSIGNED NOT NULL,
            solar_system_id                 BIGINT UNSIGNED NOT NULL,
            structure_type_id               INT UNSIGNED NOT NULL,
            structure_role                  VARCHAR(32) NOT NULL DEFAULT 'unknown',
            vulnerability_occupancy_level   DECIMAL(4,1) DEFAULT NULL,
            vulnerable_start_time           DATETIME DEFAULT NULL,
            vulnerable_end_time             DATETIME DEFAULT NULL,
            fetched_at                      DATETIME NOT NULL,
            created_at                      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at                      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_alliance (alliance_id),
            KEY idx_system (solar_system_id),
            KEY idx_type (structure_type_id),
            KEY idx_role (structure_role)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sovereignty_structures_history (
            id                      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            structure_id            BIGINT UNSIGNED NOT NULL,
            alliance_id             BIGINT UNSIGNED NOT NULL,
            solar_system_id         BIGINT UNSIGNED NOT NULL,
            structure_type_id       INT UNSIGNED NOT NULL,
            structure_role          VARCHAR(32) NOT NULL,
            event_type              ENUM('appeared','disappeared','adm_changed','owner_changed','vuln_changed') NOT NULL,
            previous_adm            DECIMAL(4,1) DEFAULT NULL,
            new_adm                 DECIMAL(4,1) DEFAULT NULL,
            previous_alliance_id    BIGINT UNSIGNED DEFAULT NULL,
            new_alliance_id         BIGINT UNSIGNED DEFAULT NULL,
            details_json            JSON DEFAULT NULL,
            recorded_at             DATETIME NOT NULL,
            created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            KEY idx_structure (structure_id, recorded_at),
            KEY idx_system (solar_system_id, recorded_at),
            KEY idx_alliance (alliance_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sovereignty_campaigns (
            campaign_id         INT UNSIGNED NOT NULL PRIMARY KEY,
            event_type          VARCHAR(32) NOT NULL,
            solar_system_id     BIGINT UNSIGNED NOT NULL,
            constellation_id    BIGINT UNSIGNED NOT NULL,
            structure_id        BIGINT UNSIGNED NOT NULL,
            defender_id         BIGINT UNSIGNED NOT NULL,
            attackers_score     DECIMAL(4,2) NOT NULL DEFAULT 0.00,
            defender_score      DECIMAL(4,2) NOT NULL DEFAULT 0.00,
            start_time          DATETIME NOT NULL,
            first_seen_at       DATETIME NOT NULL,
            last_seen_at        DATETIME NOT NULL,
            is_active           TINYINT(1) NOT NULL DEFAULT 1,
            KEY idx_active (is_active, solar_system_id),
            KEY idx_defender (defender_id),
            KEY idx_system (solar_system_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sovereignty_campaigns_history (
            id                      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            campaign_id             INT UNSIGNED NOT NULL,
            event_type              VARCHAR(32) NOT NULL,
            solar_system_id         BIGINT UNSIGNED NOT NULL,
            constellation_id        BIGINT UNSIGNED NOT NULL,
            structure_id            BIGINT UNSIGNED NOT NULL,
            defender_id             BIGINT UNSIGNED NOT NULL,
            final_attackers_score   DECIMAL(4,2) NOT NULL,
            final_defender_score    DECIMAL(4,2) NOT NULL,
            outcome                 ENUM('defended','captured','unknown') NOT NULL DEFAULT 'unknown',
            start_time              DATETIME NOT NULL,
            ended_at                DATETIME NOT NULL,
            created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_campaign (campaign_id),
            KEY idx_system (solar_system_id, ended_at),
            KEY idx_defender (defender_id),
            KEY idx_outcome (outcome)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS sovereignty_alerts (
            id                  BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            alert_type          ENUM('campaign_friendly','campaign_hostile','sov_lost','sov_gained','low_adm','vuln_window') NOT NULL,
            solar_system_id     BIGINT UNSIGNED DEFAULT NULL,
            alliance_id         BIGINT UNSIGNED DEFAULT NULL,
            structure_id        BIGINT UNSIGNED DEFAULT NULL,
            campaign_id         INT UNSIGNED DEFAULT NULL,
            severity            ENUM('critical','warning','info') NOT NULL,
            title               VARCHAR(255) NOT NULL,
            details_json        JSON DEFAULT NULL,
            status              ENUM('active','stale','resolved') NOT NULL DEFAULT 'active',
            detected_at         DATETIME NOT NULL,
            last_seen_at        DATETIME NOT NULL,
            resolved_at         DATETIME DEFAULT NULL,
            created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_status_severity (status, severity, detected_at),
            KEY idx_system (solar_system_id),
            KEY idx_campaign (campaign_id),
            KEY idx_alliance (alliance_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)
    # Seed default structure role mappings.
    db.execute("""
        INSERT IGNORE INTO ref_sov_structure_roles (structure_type_id, structure_role, is_sov_structure, notes) VALUES
            (32458, 'legacy_ihub', 1, 'Infrastructure Hub'),
            (32226, 'legacy_tcu',  1, 'Territorial Claim Unit')
    """)


def _lookup_structure_role(db: SupplyCoreDb, structure_type_id: int) -> str:
    """DB-driven structure role lookup with per-process caching."""
    if structure_type_id in _role_cache:
        return _role_cache[structure_type_id]

    row = db.fetch_one(
        "SELECT structure_role FROM ref_sov_structure_roles WHERE structure_type_id = %s",
        (structure_type_id,),
    )
    if row:
        role = str(row["structure_role"])
    else:
        role = "other"
        db.execute(
            """INSERT IGNORE INTO ref_sov_structure_roles
               (structure_type_id, structure_role, is_sov_structure, notes)
               VALUES (%s, 'other', 0, NULL)""",
            (structure_type_id,),
        )

    _role_cache[structure_type_id] = role
    return role


def _resolve_owner_entity(
    alliance_id: int | None,
    corporation_id: int | None,
    faction_id: int | None,
) -> tuple[int | None, str | None]:
    """Ownership priority: alliance > corporation > faction."""
    if alliance_id and alliance_id > 0:
        return alliance_id, "alliance"
    if corporation_id and corporation_id > 0:
        return corporation_id, "corporation"
    if faction_id and faction_id > 0:
        return faction_id, "faction"
    return None, None


def _queue_sov_entity_names(db: SupplyCoreDb, entity_ids: set[int]) -> None:
    """Queue alliance/corp/faction IDs for background name resolution."""
    if not entity_ids:
        return
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    for eid in sorted(entity_ids):
        db.execute(
            """INSERT INTO entity_metadata_cache
               (entity_type, entity_id, source_system, resolution_status, last_requested_at)
               VALUES ('alliance', %s, 'queue', 'pending', %s)
               ON DUPLICATE KEY UPDATE last_requested_at = VALUES(last_requested_at)""",
            [eid, now],
        )
    log.info("Queued %d sovereignty entity IDs for name resolution.", len(entity_ids))


def _build_gateway(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None):
    """Build ESI gateway from config (reused across all three jobs)."""
    from ..esi_gateway import build_gateway

    redis_cfg = dict((raw_config or {}).get("redis") or {})
    if redis_cfg.get("enabled"):
        return build_gateway(db=db, redis_config=redis_cfg)
    return None


def _esi_get(gateway, path: str):
    """Fetch a public ESI endpoint. Returns body or None."""
    if gateway is None:
        from ..esi_client import EsiClient
        client = EsiClient()
        resp = client.get(path)
        if resp.status_code == 200:
            return resp.body
        log.warning("ESI %s returned %d (no gateway)", path, resp.status_code)
        return None

    resp = gateway.get(
        path,
        access_token=None,
        group=_ESI_GROUP,
        route_template=path,
        identity="sovereignty",
    )
    if resp.status_code == 304:
        return resp.body  # cached
    if resp.status_code == 200:
        return resp.body
    log.warning("ESI %s returned %d", path, resp.status_code)
    return None


# ── Job A: Campaigns Sync (60s) ─────────────────────────────────────────────

def _campaigns_processor(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    gateway = _build_gateway(db, raw_config)
    _ensure_tables(db)

    warnings: list[str] = []
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    rows_processed = 0
    rows_written = 0

    body = _esi_get(gateway, "/latest/sovereignty/campaigns/")
    if body is None:
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": ["ESI /sovereignty/campaigns/ returned no data."],
            "summary": "Skipped — no campaign data from ESI.",
        }

    if not isinstance(body, list):
        warnings.append(f"Unexpected response type: {type(body).__name__}")
        return {"rows_processed": 0, "rows_written": 0, "warnings": warnings, "summary": "Skipped — bad response."}

    rows_processed = len(body)
    active_campaign_ids: set[int] = set()
    entity_ids: set[int] = set()

    for entry in body:
        campaign_id = int(entry.get("campaign_id") or 0)
        if campaign_id <= 0:
            continue

        active_campaign_ids.add(campaign_id)
        defender_id = int(entry.get("defender_id") or 0)
        if defender_id > 0:
            entity_ids.add(defender_id)

        event_type = str(entry.get("event_type") or "unknown")[:32]
        solar_system_id = int(entry.get("solar_system_id") or 0)
        constellation_id = int(entry.get("constellation_id") or 0)
        structure_id = int(entry.get("structure_id") or 0)
        attackers_score = float(entry.get("attackers_score") or 0)
        defender_score = float(entry.get("defender_score") or 0)
        start_time = str(entry.get("start_time") or now_sql).replace("T", " ").replace("Z", "")[:19]

        db.execute(
            """INSERT INTO sovereignty_campaigns
               (campaign_id, event_type, solar_system_id, constellation_id, structure_id,
                defender_id, attackers_score, defender_score, start_time, first_seen_at, last_seen_at, is_active)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
               ON DUPLICATE KEY UPDATE
                   attackers_score = VALUES(attackers_score),
                   defender_score = VALUES(defender_score),
                   last_seen_at = VALUES(last_seen_at),
                   is_active = 1""",
            [campaign_id, event_type, solar_system_id, constellation_id, structure_id,
             defender_id, attackers_score, defender_score, start_time, now_sql, now_sql],
        )
        rows_written += 1

    # Mark campaigns no longer in API response as inactive and archive them.
    previously_active = db.fetch_all(
        "SELECT campaign_id, event_type, solar_system_id, constellation_id, structure_id, "
        "defender_id, attackers_score, defender_score, start_time, first_seen_at "
        "FROM sovereignty_campaigns WHERE is_active = 1"
    )
    newly_ended = 0
    for row in previously_active:
        cid = int(row["campaign_id"])
        if cid in active_campaign_ids:
            continue
        # Campaign disappeared from API — mark inactive.
        db.execute("UPDATE sovereignty_campaigns SET is_active = 0 WHERE campaign_id = %s", (cid,))
        # Archive to history with outcome=unknown (delayed resolution).
        db.execute(
            """INSERT INTO sovereignty_campaigns_history
               (campaign_id, event_type, solar_system_id, constellation_id, structure_id,
                defender_id, final_attackers_score, final_defender_score, outcome, start_time, ended_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'unknown', %s, %s)
               ON DUPLICATE KEY UPDATE
                   final_attackers_score = VALUES(final_attackers_score),
                   final_defender_score = VALUES(final_defender_score),
                   ended_at = VALUES(ended_at)""",
            [cid, row["event_type"], row["solar_system_id"], row["constellation_id"],
             row["structure_id"], row["defender_id"], float(row["attackers_score"]),
             float(row["defender_score"]), str(row["start_time"])[:19], now_sql],
        )
        newly_ended += 1

    # Delayed outcome resolution for campaigns ended >60 minutes ago.
    _resolve_campaign_outcomes(db, now_sql)

    _queue_sov_entity_names(db, entity_ids)

    db.execute(
        """INSERT INTO sync_state (dataset_key, sync_mode, status, last_success_at, last_row_count)
           VALUES ('sovereignty.campaigns', 'full', 'success', UTC_TIMESTAMP(), %s)
           ON DUPLICATE KEY UPDATE status='success', last_success_at=UTC_TIMESTAMP(),
               last_row_count=VALUES(last_row_count), updated_at=UTC_TIMESTAMP()""",
        [rows_written],
    )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Campaigns: {len(active_campaign_ids)} active, {newly_ended} ended.",
        "meta": {"active_campaigns": len(active_campaign_ids), "newly_ended": newly_ended},
    }


def _resolve_campaign_outcomes(db: SupplyCoreDb, now_sql: str) -> None:
    """Resolve unknown campaign outcomes using post-state evidence.

    Only resolves campaigns ended >60 minutes ago to allow map data
    to converge (2 map sync cycles at 30-minute intervals).
    """
    unknown_rows = db.fetch_all(
        """SELECT id, campaign_id, solar_system_id, structure_id, defender_id,
                  start_time, ended_at
           FROM sovereignty_campaigns_history
           WHERE outcome = 'unknown'
             AND ended_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 60 MINUTE)"""
    )
    for row in unknown_rows:
        system_id = int(row["solar_system_id"])
        structure_id = int(row["structure_id"])
        defender_id = int(row["defender_id"])
        start_time = row["start_time"]

        outcome = "defended"  # default if no change detected

        # 1. Structure evidence (highest confidence).
        struct_row = db.fetch_one(
            "SELECT alliance_id FROM sovereignty_structures WHERE structure_id = %s",
            (structure_id,),
        )
        if struct_row is None:
            # Structure disappeared → captured.
            outcome = "captured"
        elif int(struct_row.get("alliance_id") or 0) != defender_id:
            # Structure owner changed → captured.
            outcome = "captured"
        else:
            # 2. Map evidence.
            map_row = db.fetch_one(
                "SELECT alliance_id FROM sovereignty_map WHERE system_id = %s",
                (system_id,),
            )
            if map_row and int(map_row.get("alliance_id") or 0) != defender_id:
                outcome = "captured"

        db.execute(
            "UPDATE sovereignty_campaigns_history SET outcome = %s WHERE id = %s",
            [outcome, row["id"]],
        )
        if outcome != "defended":
            log.info("Campaign %d outcome resolved: %s", int(row["campaign_id"]), outcome)


# ── Job B: Structures Sync (180s) ───────────────────────────────────────────

def _structures_processor(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    gateway = _build_gateway(db, raw_config)
    _ensure_tables(db)

    warnings: list[str] = []
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    rows_processed = 0
    rows_written = 0

    body = _esi_get(gateway, "/latest/sovereignty/structures/")
    if body is None:
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": ["ESI /sovereignty/structures/ returned no data."],
            "summary": "Skipped — no structures data from ESI.",
        }

    if not isinstance(body, list):
        warnings.append(f"Unexpected response type: {type(body).__name__}")
        return {"rows_processed": 0, "rows_written": 0, "warnings": warnings, "summary": "Skipped — bad response."}

    rows_processed = len(body)

    # Load current snapshot for change detection.
    existing_rows = db.fetch_all(
        "SELECT structure_id, alliance_id, solar_system_id, structure_type_id, structure_role, "
        "vulnerability_occupancy_level, vulnerable_start_time, vulnerable_end_time "
        "FROM sovereignty_structures"
    )
    existing: dict[int, dict] = {int(r["structure_id"]): r for r in existing_rows}
    seen_structure_ids: set[int] = set()
    entity_ids: set[int] = set()
    history_events = 0

    for entry in body:
        structure_id = int(entry.get("structure_id") or 0)
        if structure_id <= 0:
            continue

        seen_structure_ids.add(structure_id)
        alliance_id = int(entry.get("alliance_id") or 0)
        solar_system_id = int(entry.get("solar_system_id") or 0)
        structure_type_id = int(entry.get("structure_type_id") or 0)
        adm = entry.get("vulnerability_occupancy_level")
        adm_val = float(adm) if adm is not None else None
        vuln_start = entry.get("vulnerable_start_time")
        vuln_end = entry.get("vulnerable_end_time")
        vuln_start_sql = str(vuln_start).replace("T", " ").replace("Z", "")[:19] if vuln_start else None
        vuln_end_sql = str(vuln_end).replace("T", " ").replace("Z", "")[:19] if vuln_end else None

        if alliance_id > 0:
            entity_ids.add(alliance_id)

        role = _lookup_structure_role(db, structure_type_id)

        # Detect changes vs existing snapshot.
        prev = existing.get(structure_id)
        if prev is None:
            # New structure appeared.
            db.execute(
                """INSERT INTO sovereignty_structures_history
                   (structure_id, alliance_id, solar_system_id, structure_type_id, structure_role,
                    event_type, new_adm, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, 'appeared', %s, %s)""",
                [structure_id, alliance_id, solar_system_id, structure_type_id, role, adm_val, now_sql],
            )
            history_events += 1
        else:
            prev_alliance = int(prev.get("alliance_id") or 0)
            prev_adm = float(prev["vulnerability_occupancy_level"]) if prev.get("vulnerability_occupancy_level") is not None else None
            prev_vuln_start = str(prev.get("vulnerable_start_time") or "")[:19] if prev.get("vulnerable_start_time") else None
            prev_vuln_end = str(prev.get("vulnerable_end_time") or "")[:19] if prev.get("vulnerable_end_time") else None

            if prev_alliance != alliance_id:
                db.execute(
                    """INSERT INTO sovereignty_structures_history
                       (structure_id, alliance_id, solar_system_id, structure_type_id, structure_role,
                        event_type, previous_alliance_id, new_alliance_id, recorded_at)
                       VALUES (%s, %s, %s, %s, %s, 'owner_changed', %s, %s, %s)""",
                    [structure_id, alliance_id, solar_system_id, structure_type_id, role,
                     prev_alliance, alliance_id, now_sql],
                )
                history_events += 1

            if prev_adm is not None and adm_val is not None and abs(prev_adm - adm_val) >= 0.1:
                db.execute(
                    """INSERT INTO sovereignty_structures_history
                       (structure_id, alliance_id, solar_system_id, structure_type_id, structure_role,
                        event_type, previous_adm, new_adm, recorded_at)
                       VALUES (%s, %s, %s, %s, %s, 'adm_changed', %s, %s, %s)""",
                    [structure_id, alliance_id, solar_system_id, structure_type_id, role,
                     prev_adm, adm_val, now_sql],
                )
                history_events += 1

            if prev_vuln_start != vuln_start_sql or prev_vuln_end != vuln_end_sql:
                db.execute(
                    """INSERT INTO sovereignty_structures_history
                       (structure_id, alliance_id, solar_system_id, structure_type_id, structure_role,
                        event_type, details_json, recorded_at)
                       VALUES (%s, %s, %s, %s, %s, 'vuln_changed', %s, %s)""",
                    [structure_id, alliance_id, solar_system_id, structure_type_id, role,
                     json.dumps({"prev_start": prev_vuln_start, "prev_end": prev_vuln_end,
                                 "new_start": vuln_start_sql, "new_end": vuln_end_sql}),
                     now_sql],
                )
                history_events += 1

        # Upsert structure.
        db.execute(
            """INSERT INTO sovereignty_structures
               (structure_id, alliance_id, solar_system_id, structure_type_id, structure_role,
                vulnerability_occupancy_level, vulnerable_start_time, vulnerable_end_time, fetched_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   alliance_id = VALUES(alliance_id),
                   solar_system_id = VALUES(solar_system_id),
                   structure_type_id = VALUES(structure_type_id),
                   structure_role = VALUES(structure_role),
                   vulnerability_occupancy_level = VALUES(vulnerability_occupancy_level),
                   vulnerable_start_time = VALUES(vulnerable_start_time),
                   vulnerable_end_time = VALUES(vulnerable_end_time),
                   fetched_at = VALUES(fetched_at)""",
            [structure_id, alliance_id, solar_system_id, structure_type_id, role,
             adm_val, vuln_start_sql, vuln_end_sql, now_sql],
        )
        rows_written += 1

    # Detect disappeared structures.
    for sid, prev in existing.items():
        if sid not in seen_structure_ids:
            db.execute(
                """INSERT INTO sovereignty_structures_history
                   (structure_id, alliance_id, solar_system_id, structure_type_id, structure_role,
                    event_type, previous_adm, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, 'disappeared', %s, %s)""",
                [sid, int(prev.get("alliance_id") or 0), int(prev.get("solar_system_id") or 0),
                 int(prev.get("structure_type_id") or 0), str(prev.get("structure_role") or "unknown"),
                 float(prev["vulnerability_occupancy_level"]) if prev.get("vulnerability_occupancy_level") is not None else None,
                 now_sql],
            )
            db.execute("DELETE FROM sovereignty_structures WHERE structure_id = %s", (sid,))
            history_events += 1

    _queue_sov_entity_names(db, entity_ids)

    db.execute(
        """INSERT INTO sync_state (dataset_key, sync_mode, status, last_success_at, last_row_count)
           VALUES ('sovereignty.structures', 'full', 'success', UTC_TIMESTAMP(), %s)
           ON DUPLICATE KEY UPDATE status='success', last_success_at=UTC_TIMESTAMP(),
               last_row_count=VALUES(last_row_count), updated_at=UTC_TIMESTAMP()""",
        [rows_written],
    )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Structures: {rows_written} upserted, {history_events} history events.",
        "meta": {"structures_count": rows_written, "history_events": history_events},
    }


# ── Job C: Map Sync (1800s) ─────────────────────────────────────────────────

def _map_processor(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    gateway = _build_gateway(db, raw_config)
    _ensure_tables(db)

    warnings: list[str] = []
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    rows_processed = 0
    rows_written = 0

    body = _esi_get(gateway, "/latest/sovereignty/map/")
    if body is None:
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": ["ESI /sovereignty/map/ returned no data."],
            "summary": "Skipped — no map data from ESI.",
        }

    if not isinstance(body, list):
        warnings.append(f"Unexpected response type: {type(body).__name__}")
        return {"rows_processed": 0, "rows_written": 0, "warnings": warnings, "summary": "Skipped — bad response."}

    rows_processed = len(body)

    # Load current snapshot for change detection.
    existing_rows = db.fetch_all(
        "SELECT system_id, alliance_id, corporation_id, faction_id, "
        "owner_entity_id, owner_entity_type FROM sovereignty_map"
    )
    existing: dict[int, dict] = {int(r["system_id"]): r for r in existing_rows}
    entity_ids: set[int] = set()
    ownership_changes = 0

    for entry in body:
        system_id = int(entry.get("system_id") or 0)
        if system_id <= 0:
            continue

        alliance_id = int(entry.get("alliance_id") or 0) or None
        corporation_id = int(entry.get("corporation_id") or 0) or None
        faction_id = int(entry.get("faction_id") or 0) or None
        owner_id, owner_type = _resolve_owner_entity(alliance_id, corporation_id, faction_id)

        if alliance_id:
            entity_ids.add(alliance_id)

        # Check for ownership change.
        prev = existing.get(system_id)
        if prev is not None:
            prev_owner_id = int(prev.get("owner_entity_id") or 0) or None
            prev_owner_type = prev.get("owner_entity_type")
            if prev_owner_id != owner_id or prev_owner_type != owner_type:
                db.execute(
                    """INSERT INTO sovereignty_map_history
                       (system_id, previous_alliance_id, new_alliance_id,
                        previous_corporation_id, new_corporation_id,
                        previous_faction_id, new_faction_id,
                        previous_owner_entity_id, previous_owner_entity_type,
                        new_owner_entity_id, new_owner_entity_type, changed_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    [system_id,
                     int(prev.get("alliance_id") or 0) or None, alliance_id,
                     int(prev.get("corporation_id") or 0) or None, corporation_id,
                     int(prev.get("faction_id") or 0) or None, faction_id,
                     prev_owner_id, prev_owner_type,
                     owner_id, owner_type, now_sql],
                )
                ownership_changes += 1

        # Upsert map entry.
        db.execute(
            """INSERT INTO sovereignty_map
               (system_id, alliance_id, corporation_id, faction_id,
                owner_entity_id, owner_entity_type, fetched_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   alliance_id = VALUES(alliance_id),
                   corporation_id = VALUES(corporation_id),
                   faction_id = VALUES(faction_id),
                   owner_entity_id = VALUES(owner_entity_id),
                   owner_entity_type = VALUES(owner_entity_type),
                   fetched_at = VALUES(fetched_at)""",
            [system_id, alliance_id, corporation_id, faction_id,
             owner_id, owner_type, now_sql],
        )
        rows_written += 1

    _queue_sov_entity_names(db, entity_ids)

    db.execute(
        """INSERT INTO sync_state (dataset_key, sync_mode, status, last_success_at, last_row_count)
           VALUES ('sovereignty.map', 'full', 'success', UTC_TIMESTAMP(), %s)
           ON DUPLICATE KEY UPDATE status='success', last_success_at=UTC_TIMESTAMP(),
               last_row_count=VALUES(last_row_count), updated_at=UTC_TIMESTAMP()""",
        [rows_written],
    )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Map: {rows_written} systems upserted, {ownership_changes} ownership changes.",
        "meta": {"systems_count": rows_written, "ownership_changes": ownership_changes},
    }


# ── Entry points ─────────────────────────────────────────────────────────────

def run_sovereignty_campaigns_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="sovereignty_campaigns_sync",
        phase="A",
        objective="sovereignty campaigns",
        processor=lambda d: _campaigns_processor(d, raw_config),
    )


def run_sovereignty_structures_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="sovereignty_structures_sync",
        phase="A",
        objective="sovereignty structures",
        processor=lambda d: _structures_processor(d, raw_config),
    )


def run_sovereignty_map_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key="sovereignty_map_sync",
        phase="A",
        objective="sovereignty map",
        processor=lambda d: _map_processor(d, raw_config),
    )
