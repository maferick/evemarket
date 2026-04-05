"""Opposition Daily Snapshot computation — daily killmail activity per opponent alliance.

Captures per-day activity metrics for each opponent alliance: kills, losses,
ISK destroyed/lost, active pilots, geography, ship usage, posture, relationships,
theater participation, notable kills, and threat corridor presence.

These snapshots feed the AI opposition intelligence briefing system, providing
both today's data and historical context for trend detection.

Runs once daily after killmail sync completes.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import finish_job_run, start_job_run
else:
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import finish_job_run, start_job_run

TOP_K = 5
TOP_K_NOTABLE = 3


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _load_opponent_alliance_ids(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Load opponent alliance IDs from corp_contacts (negative standing)."""
    rows = db.fetch_all(
        """
        SELECT cc.contact_id AS alliance_id,
               COALESCE(emc.entity_name, CONCAT('Alliance #', cc.contact_id)) AS alliance_name
        FROM corp_contacts cc
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'alliance' AND emc.entity_id = cc.contact_id
        WHERE cc.contact_type = 'alliance' AND cc.standing < 0
        ORDER BY cc.contact_id ASC
        """,
    )
    return rows


def _load_daily_activity_bulk(db: SupplyCoreDb, alliance_ids: list[int], target_date: str) -> dict[int, dict[str, Any]]:
    """Load daily kill/loss/ISK/pilot counts for all opponent alliances in bulk.

    Counts kills (alliance members as attackers) and losses (alliance members as victims)
    for the target date.
    """
    if not alliance_ids:
        return {}

    placeholders = ",".join(["%s"] * len(alliance_ids))

    # Kills: killmails where alliance members were attackers
    kills_rows = db.fetch_all(
        f"""
        SELECT ka.alliance_id,
               COUNT(DISTINCT ka.sequence_id) AS kills,
               COUNT(DISTINCT ka.character_id) AS active_pilots,
               COALESCE(SUM(ke.zkb_total_value), 0) AS isk_destroyed
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        WHERE ka.alliance_id IN ({placeholders})
          AND DATE(ke.killmail_time) = %s
          AND ke.zkb_npc = 0
        GROUP BY ka.alliance_id
        """,
        (*alliance_ids, target_date),
    )

    # Losses: killmails where alliance members were victims
    losses_rows = db.fetch_all(
        f"""
        SELECT ke.victim_alliance_id AS alliance_id,
               COUNT(*) AS losses,
               COALESCE(SUM(ke.zkb_total_value), 0) AS isk_lost
        FROM killmail_events ke
        WHERE ke.victim_alliance_id IN ({placeholders})
          AND DATE(ke.killmail_time) = %s
          AND ke.zkb_npc = 0
        GROUP BY ke.victim_alliance_id
        """,
        (*alliance_ids, target_date),
    )

    result: dict[int, dict[str, Any]] = {}
    for aid in alliance_ids:
        result[aid] = {"kills": 0, "losses": 0, "isk_destroyed": 0, "isk_lost": 0, "active_pilots": 0}

    for row in kills_rows:
        aid = int(row["alliance_id"])
        result[aid]["kills"] = int(row["kills"])
        result[aid]["active_pilots"] = int(row["active_pilots"])
        result[aid]["isk_destroyed"] = float(row["isk_destroyed"])

    for row in losses_rows:
        aid = int(row["alliance_id"])
        result[aid]["losses"] = int(row["losses"])
        result[aid]["isk_lost"] = float(row["isk_lost"])

    return result


def _load_daily_geography_bulk(db: SupplyCoreDb, alliance_ids: list[int], target_date: str) -> dict[int, dict[str, Any]]:
    """Load top systems and regions per opponent alliance for the target date."""
    if not alliance_ids:
        return {}

    placeholders = ",".join(["%s"] * len(alliance_ids))

    # Top systems (attacker-side activity)
    system_rows = db.fetch_all(
        f"""
        SELECT ka.alliance_id, ke.solar_system_id AS system_id,
               COALESCE(emc.entity_name, CONCAT('System #', ke.solar_system_id)) AS system_name,
               COUNT(DISTINCT ka.sequence_id) AS killmail_count
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'solar_system' AND emc.entity_id = ke.solar_system_id
        WHERE ka.alliance_id IN ({placeholders})
          AND DATE(ke.killmail_time) = %s
          AND ke.zkb_npc = 0
          AND ke.solar_system_id IS NOT NULL
        GROUP BY ka.alliance_id, ke.solar_system_id
        ORDER BY ka.alliance_id, killmail_count DESC
        """,
        (*alliance_ids, target_date),
    )

    # Top regions
    region_rows = db.fetch_all(
        f"""
        SELECT ka.alliance_id, ke.region_id,
               COALESCE(emc.entity_name, CONCAT('Region #', ke.region_id)) AS region_name,
               COUNT(DISTINCT ka.sequence_id) AS killmail_count
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'region' AND emc.entity_id = ke.region_id
        WHERE ka.alliance_id IN ({placeholders})
          AND DATE(ke.killmail_time) = %s
          AND ke.zkb_npc = 0
          AND ke.region_id IS NOT NULL
        GROUP BY ka.alliance_id, ke.region_id
        ORDER BY ka.alliance_id, killmail_count DESC
        """,
        (*alliance_ids, target_date),
    )

    result: dict[int, dict[str, Any]] = {aid: {"systems": [], "regions": []} for aid in alliance_ids}

    # Group and limit to top K per alliance
    for row in system_rows:
        aid = int(row["alliance_id"])
        if len(result[aid]["systems"]) < TOP_K:
            result[aid]["systems"].append({
                "system_id": int(row["system_id"]),
                "system_name": row["system_name"],
                "killmail_count": int(row["killmail_count"]),
            })

    for row in region_rows:
        aid = int(row["alliance_id"])
        if len(result[aid]["regions"]) < 3:
            result[aid]["regions"].append({
                "region_id": int(row["region_id"]),
                "region_name": row["region_name"],
                "killmail_count": int(row["killmail_count"]),
            })

    return result


def _load_daily_ship_usage_bulk(db: SupplyCoreDb, alliance_ids: list[int], target_date: str) -> dict[int, dict[str, Any]]:
    """Load top ship classes and types used by each opponent alliance on the target date."""
    if not alliance_ids:
        return {}

    placeholders = ",".join(["%s"] * len(alliance_ids))

    # Ship types with group names
    ship_rows = db.fetch_all(
        f"""
        SELECT ka.alliance_id, ka.ship_type_id AS type_id,
               COALESCE(emc.entity_name, CONCAT('Type #', ka.ship_type_id)) AS name,
               COUNT(*) AS count
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'type' AND emc.entity_id = ka.ship_type_id
        WHERE ka.alliance_id IN ({placeholders})
          AND DATE(ke.killmail_time) = %s
          AND ke.zkb_npc = 0
          AND ka.ship_type_id IS NOT NULL AND ka.ship_type_id > 0
        GROUP BY ka.alliance_id, ka.ship_type_id
        ORDER BY ka.alliance_id, count DESC
        """,
        (*alliance_ids, target_date),
    )

    result: dict[int, dict[str, Any]] = {aid: {"ship_types": [], "ship_classes": []} for aid in alliance_ids}

    # Group ship types per alliance (top K)
    for row in ship_rows:
        aid = int(row["alliance_id"])
        if len(result[aid]["ship_types"]) < TOP_K:
            result[aid]["ship_types"].append({
                "type_id": int(row["type_id"]),
                "name": row["name"],
                "count": int(row["count"]),
            })

    return result


def _load_dossier_context_bulk(db: SupplyCoreDb, alliance_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Load posture, engagement_rate, and trend_summary from alliance_dossiers."""
    if not alliance_ids:
        return {}

    placeholders = ",".join(["%s"] * len(alliance_ids))
    rows = db.fetch_all(
        f"""
        SELECT alliance_id, posture, avg_engagement_rate AS engagement_rate,
               trend_summary_json
        FROM alliance_dossiers
        WHERE alliance_id IN ({placeholders})
        """,
        tuple(alliance_ids),
    )

    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        aid = int(row["alliance_id"])
        trend_json = row.get("trend_summary_json")
        trend = None
        if trend_json:
            try:
                trend = json.loads(trend_json) if isinstance(trend_json, str) else trend_json
            except (json.JSONDecodeError, TypeError):
                trend = None
        result[aid] = {
            "posture": row.get("posture"),
            "engagement_rate": float(row["engagement_rate"]) if row.get("engagement_rate") else None,
            "trend_summary": trend,
        }

    return result


def _load_relationship_context_bulk(db: SupplyCoreDb, alliance_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Load allies and hostiles from alliance_relationships for each opponent."""
    if not alliance_ids:
        return {}

    placeholders = ",".join(["%s"] * len(alliance_ids))

    rows = db.fetch_all(
        f"""
        SELECT ar.source_alliance_id, ar.target_alliance_id, ar.relationship_type,
               ar.weight_30d, ar.shared_killmails,
               COALESCE(emc.entity_name, CONCAT('Alliance #', ar.target_alliance_id)) AS target_name
        FROM alliance_relationships ar
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'alliance' AND emc.entity_id = ar.target_alliance_id
        WHERE ar.source_alliance_id IN ({placeholders})
          AND ar.weight_30d > 0.1
        ORDER BY ar.source_alliance_id, ar.weight_30d DESC
        """,
        tuple(alliance_ids),
    )

    result: dict[int, dict[str, Any]] = {aid: {"allies": [], "hostiles": []} for aid in alliance_ids}

    for row in rows:
        aid = int(row["source_alliance_id"])
        entry = {
            "alliance_id": int(row["target_alliance_id"]),
            "alliance_name": row["target_name"],
            "weight_30d": float(row["weight_30d"]),
        }
        rel_type = row["relationship_type"]
        bucket = "allies" if rel_type == "allied" else "hostiles"
        if len(result[aid][bucket]) < 3:
            result[aid][bucket].append(entry)

    return result


def _load_daily_theaters_bulk(db: SupplyCoreDb, alliance_ids: list[int], target_date: str) -> dict[int, list[dict[str, Any]]]:
    """Load theaters that opponent alliances participated in on the target date."""
    if not alliance_ids:
        return {}

    placeholders = ",".join(["%s"] * len(alliance_ids))

    rows = db.fetch_all(
        f"""
        SELECT tas.alliance_id, t.theater_id, t.primary_system_id,
               COALESCE(emc.entity_name, CONCAT('System #', t.primary_system_id)) AS system_name,
               t.battle_count, t.total_kills, t.ai_headline, t.ai_verdict,
               tas.total_kills AS alliance_kills, tas.total_losses AS alliance_losses,
               COALESCE(tas.total_isk_killed, 0) AS alliance_isk_killed,
               COALESCE(tas.total_isk_lost, 0) AS alliance_isk_lost
        FROM theater_alliance_summary tas
        INNER JOIN theaters t ON t.theater_id = tas.theater_id
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'solar_system' AND emc.entity_id = t.primary_system_id
        WHERE tas.alliance_id IN ({placeholders})
          AND DATE(t.start_time) = %s
        ORDER BY tas.alliance_id, t.total_kills DESC
        """,
        (*alliance_ids, target_date),
    )

    result: dict[int, list[dict[str, Any]]] = {aid: [] for aid in alliance_ids}

    for row in rows:
        aid = int(row["alliance_id"])
        if len(result[aid]) < 3:
            result[aid].append({
                "theater_id": row["theater_id"],
                "system_name": row.get("system_name"),
                "battle_count": int(row.get("battle_count") or 0),
                "total_kills": int(row.get("total_kills") or 0),
                "alliance_kills": int(row.get("alliance_kills") or 0),
                "alliance_losses": int(row.get("alliance_losses") or 0),
                "verdict": row.get("ai_verdict"),
            })

    return result


def _load_daily_notable_kills_bulk(db: SupplyCoreDb, alliance_ids: list[int], target_date: str) -> dict[int, list[dict[str, Any]]]:
    """Load top kills/losses by ISK value for each opponent alliance on the target date."""
    if not alliance_ids:
        return {}

    placeholders = ",".join(["%s"] * len(alliance_ids))

    # Top kills (alliance members as attackers on high-value kills)
    kill_rows = db.fetch_all(
        f"""
        SELECT ka.alliance_id, ke.killmail_id,
               COALESCE(emc_ship.entity_name, CONCAT('Type #', ke.victim_ship_type_id)) AS ship_name,
               COALESCE(emc_vic.entity_name, 'Unknown') AS victim_name,
               ke.zkb_total_value AS isk_value, 'kill' AS event_type
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        LEFT JOIN entity_metadata_cache emc_ship
             ON emc_ship.entity_type = 'type' AND emc_ship.entity_id = ke.victim_ship_type_id
        LEFT JOIN entity_metadata_cache emc_vic
             ON emc_vic.entity_type = 'character' AND emc_vic.entity_id = ke.victim_character_id
        WHERE ka.alliance_id IN ({placeholders})
          AND DATE(ke.killmail_time) = %s
          AND ke.zkb_npc = 0
          AND ke.zkb_total_value >= 50000000
        GROUP BY ka.alliance_id, ke.killmail_id
        ORDER BY ka.alliance_id, ke.zkb_total_value DESC
        """,
        (*alliance_ids, target_date),
    )

    # Top losses (alliance members as victims)
    loss_rows = db.fetch_all(
        f"""
        SELECT ke.victim_alliance_id AS alliance_id, ke.killmail_id,
               COALESCE(emc_ship.entity_name, CONCAT('Type #', ke.victim_ship_type_id)) AS ship_name,
               COALESCE(emc_vic.entity_name, 'Unknown') AS victim_name,
               ke.zkb_total_value AS isk_value, 'loss' AS event_type
        FROM killmail_events ke
        LEFT JOIN entity_metadata_cache emc_ship
             ON emc_ship.entity_type = 'type' AND emc_ship.entity_id = ke.victim_ship_type_id
        LEFT JOIN entity_metadata_cache emc_vic
             ON emc_vic.entity_type = 'character' AND emc_vic.entity_id = ke.victim_character_id
        WHERE ke.victim_alliance_id IN ({placeholders})
          AND DATE(ke.killmail_time) = %s
          AND ke.zkb_npc = 0
          AND ke.zkb_total_value >= 50000000
        ORDER BY ke.victim_alliance_id, ke.zkb_total_value DESC
        """,
        (*alliance_ids, target_date),
    )

    result: dict[int, list[dict[str, Any]]] = {aid: [] for aid in alliance_ids}

    for row in kill_rows:
        aid = int(row["alliance_id"])
        if len(result[aid]) < TOP_K_NOTABLE:
            result[aid].append({
                "killmail_id": int(row["killmail_id"]),
                "ship_name": row["ship_name"],
                "victim_name": row["victim_name"],
                "isk_value": float(row["isk_value"]),
                "type": "kill",
            })

    for row in loss_rows:
        aid = int(row["alliance_id"])
        if len(result[aid]) < TOP_K_NOTABLE * 2:
            result[aid].append({
                "killmail_id": int(row["killmail_id"]),
                "ship_name": row["ship_name"],
                "victim_name": row["victim_name"],
                "isk_value": float(row["isk_value"]),
                "type": "loss",
            })

    # Sort each alliance's notable kills by ISK value descending and limit
    for aid in result:
        result[aid] = sorted(result[aid], key=lambda x: x["isk_value"], reverse=True)[:TOP_K_NOTABLE]

    return result


def _load_threat_corridor_presence(db: SupplyCoreDb, alliance_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """Load active threat corridors that involve opponent alliances."""
    if not alliance_ids:
        return {}

    rows = db.fetch_all(
        """
        SELECT corridor_id, hostile_alliance_ids_json, corridor_score,
               system_ids_json, corridor_length
        FROM threat_corridors
        WHERE is_active = 1
        ORDER BY corridor_score DESC
        LIMIT 20
        """,
    )

    result: dict[int, list[dict[str, Any]]] = {aid: [] for aid in alliance_ids}
    aid_set = set(alliance_ids)

    for row in rows:
        hostile_ids_raw = row.get("hostile_alliance_ids_json")
        if not hostile_ids_raw:
            continue
        try:
            hostile_ids = json.loads(hostile_ids_raw) if isinstance(hostile_ids_raw, str) else hostile_ids_raw
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(hostile_ids, list):
            continue

        corridor_entry = {
            "corridor_id": int(row["corridor_id"]),
            "corridor_score": float(row.get("corridor_score") or 0),
            "corridor_length": int(row.get("corridor_length") or 0),
        }

        for hid in hostile_ids:
            hid_int = int(hid)
            if hid_int in aid_set and len(result[hid_int]) < 3:
                result[hid_int].append(corridor_entry)

    return result


def _upsert_snapshots(db: SupplyCoreDb, snapshots: list[dict[str, Any]]) -> int:
    """UPSERT snapshots into opposition_daily_snapshots."""
    if not snapshots:
        return 0

    sql = """
    INSERT INTO opposition_daily_snapshots (
        snapshot_date, alliance_id, alliance_name,
        kills, losses, isk_destroyed, isk_lost, active_pilots,
        active_systems_json, active_regions_json,
        ship_classes_json, ship_types_json,
        posture, engagement_rate,
        allies_json, enemies_json,
        theaters_json, notable_kills_json,
        threat_corridors_json, trend_summary_json,
        computed_at
    ) VALUES (
        %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s, %s,
        %s
    ) ON DUPLICATE KEY UPDATE
        alliance_name = VALUES(alliance_name),
        kills = VALUES(kills), losses = VALUES(losses),
        isk_destroyed = VALUES(isk_destroyed), isk_lost = VALUES(isk_lost),
        active_pilots = VALUES(active_pilots),
        active_systems_json = VALUES(active_systems_json),
        active_regions_json = VALUES(active_regions_json),
        ship_classes_json = VALUES(ship_classes_json),
        ship_types_json = VALUES(ship_types_json),
        posture = VALUES(posture), engagement_rate = VALUES(engagement_rate),
        allies_json = VALUES(allies_json), enemies_json = VALUES(enemies_json),
        theaters_json = VALUES(theaters_json),
        notable_kills_json = VALUES(notable_kills_json),
        threat_corridors_json = VALUES(threat_corridors_json),
        trend_summary_json = VALUES(trend_summary_json),
        computed_at = VALUES(computed_at)
    """

    rows_written = 0
    batch_size = 50
    for i in range(0, len(snapshots), batch_size):
        chunk = snapshots[i: i + batch_size]
        params = [
            (
                s["snapshot_date"], s["alliance_id"], s["alliance_name"],
                s["kills"], s["losses"], s["isk_destroyed"], s["isk_lost"], s["active_pilots"],
                json_dumps_safe(s.get("active_systems")),
                json_dumps_safe(s.get("active_regions")),
                json_dumps_safe(s.get("ship_classes")),
                json_dumps_safe(s.get("ship_types")),
                s.get("posture"), s.get("engagement_rate"),
                json_dumps_safe(s.get("allies")),
                json_dumps_safe(s.get("enemies")),
                json_dumps_safe(s.get("theaters")),
                json_dumps_safe(s.get("notable_kills")),
                json_dumps_safe(s.get("threat_corridors")),
                json_dumps_safe(s.get("trend_summary")),
                s["computed_at"],
            )
            for s in chunk
        ]
        with db.transaction() as (_, cursor):
            cursor.executemany(sql, params)
            rows_written += len(chunk)

    return rows_written


def _processor(db: SupplyCoreDb) -> dict[str, Any]:
    target_date = datetime.now(UTC).strftime("%Y-%m-%d")
    computed_at = _now_sql()

    # 1. Load opponent alliance IDs
    opponents = _load_opponent_alliance_ids(db)
    if not opponents:
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": ["No opponent alliances configured in corp_contacts."],
            "summary": "No opponent alliances found — skipping opposition daily snapshot.",
        }

    alliance_ids = [int(o["alliance_id"]) for o in opponents]
    name_map = {int(o["alliance_id"]): o["alliance_name"] for o in opponents}
    logger.info("Computing opposition daily snapshots for %d alliances on %s", len(alliance_ids), target_date)

    # 2. Bulk-load all data sources
    activity = _load_daily_activity_bulk(db, alliance_ids, target_date)
    geography = _load_daily_geography_bulk(db, alliance_ids, target_date)
    ships = _load_daily_ship_usage_bulk(db, alliance_ids, target_date)
    dossiers = _load_dossier_context_bulk(db, alliance_ids)
    relationships = _load_relationship_context_bulk(db, alliance_ids)
    theaters = _load_daily_theaters_bulk(db, alliance_ids, target_date)
    notable_kills = _load_daily_notable_kills_bulk(db, alliance_ids, target_date)
    corridors = _load_threat_corridor_presence(db, alliance_ids)

    # 3. Assemble snapshots
    snapshots: list[dict[str, Any]] = []
    for aid in alliance_ids:
        act = activity.get(aid, {})
        geo = geography.get(aid, {})
        ship = ships.get(aid, {})
        dos = dossiers.get(aid, {})
        rel = relationships.get(aid, {})

        snapshots.append({
            "snapshot_date": target_date,
            "alliance_id": aid,
            "alliance_name": name_map.get(aid, f"Alliance #{aid}"),
            "kills": act.get("kills", 0),
            "losses": act.get("losses", 0),
            "isk_destroyed": act.get("isk_destroyed", 0),
            "isk_lost": act.get("isk_lost", 0),
            "active_pilots": act.get("active_pilots", 0),
            "active_systems": geo.get("systems", []),
            "active_regions": geo.get("regions", []),
            "ship_classes": ship.get("ship_classes", []),
            "ship_types": ship.get("ship_types", []),
            "posture": dos.get("posture"),
            "engagement_rate": dos.get("engagement_rate"),
            "allies": rel.get("allies", []),
            "enemies": rel.get("hostiles", []),
            "theaters": theaters.get(aid, []),
            "notable_kills": notable_kills.get(aid, []),
            "threat_corridors": corridors.get(aid, []),
            "trend_summary": dos.get("trend_summary"),
            "computed_at": computed_at,
        })

    # 4. Persist
    rows_written = _upsert_snapshots(db, snapshots)

    # 5. Update intelligence snapshot status
    active_count = sum(1 for s in snapshots if s["kills"] > 0 or s["losses"] > 0)
    db.upsert_intelligence_snapshot(
        snapshot_key="opposition_daily_snapshots_status",
        payload_json=json_dumps_safe({
            "snapshot_date": target_date,
            "alliance_count": len(alliance_ids),
            "active_alliances": active_count,
            "total_kills": sum(s["kills"] for s in snapshots),
            "total_losses": sum(s["losses"] for s in snapshots),
        }),
        metadata_json=json_dumps_safe({"source": "opposition_daily_snapshots", "job": "compute_opposition_daily_snapshots"}),
        expires_seconds=86400,
    )

    return {
        "rows_processed": len(alliance_ids),
        "rows_written": rows_written,
        "summary": f"Computed {rows_written} opposition daily snapshots ({active_count} active) for {target_date}.",
        "meta": {"snapshot_date": target_date, "alliance_count": len(alliance_ids), "active_alliances": active_count},
    }


def run_compute_opposition_daily_snapshots(db: SupplyCoreDb) -> dict[str, Any]:
    """Entry point for the opposition daily snapshot job."""
    from .sync_runtime import run_sync_phase_job
    return run_sync_phase_job(
        db,
        job_key="compute_opposition_daily_snapshots",
        phase="C",
        objective="opposition daily snapshots",
        processor=_processor,
    )
