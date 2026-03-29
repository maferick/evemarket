"""Alliance Dossier computation — graph-powered intelligence briefs per alliance.

Queries Neo4j for alliance relationship patterns (co-presence, engagement,
geographic spread) and MariaDB for aggregate combat statistics. Results are
materialized to the ``alliance_dossiers`` table for PHP consumption.
"""

from __future__ import annotations

import hashlib
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    package_root = str(Path(__file__).resolve().parents[2])
    if package_root not in sys.path:
        sys.path.insert(0, package_root)
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
else:
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run

BATCH_SIZE = 200
RECENT_DAYS = 30
TOP_K = 10


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _load_alliances_with_battles(db: SupplyCoreDb, min_battles: int = 2) -> list[dict[str, Any]]:
    """Load alliances that have participated in at least min_battles battles."""
    return db.fetch_all(
        """
        SELECT bp.alliance_id,
               COALESCE(emc.entity_name, CONCAT('Alliance #', bp.alliance_id)) AS alliance_name,
               COUNT(DISTINCT bp.battle_id) AS total_battles,
               COUNT(DISTINCT CASE WHEN br.started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                     THEN bp.battle_id END) AS recent_battles,
               MIN(br.started_at) AS first_seen_at,
               MAX(br.started_at) AS last_seen_at
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'alliance' AND emc.entity_id = bp.alliance_id
        WHERE bp.alliance_id IS NOT NULL AND bp.alliance_id > 0
        GROUP BY bp.alliance_id
        HAVING total_battles >= %s
        ORDER BY recent_battles DESC, total_battles DESC
        """,
        (RECENT_DAYS, min_battles),
    )


def _load_geographic_summary(db: SupplyCoreDb, alliance_id: int) -> dict[str, Any]:
    """Load geographic concentration from MariaDB battle data."""
    regions = db.fetch_all(
        """
        SELECT br.system_id, rs.system_name, rs.region_id, rr.region_name,
               rs.constellation_id, rc.constellation_name,
               COUNT(DISTINCT bp.battle_id) AS battle_count
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
        LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
        LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
        WHERE bp.alliance_id = %s
        GROUP BY br.system_id
        ORDER BY battle_count DESC
        """,
        (alliance_id,),
    )

    region_totals: dict[int, dict] = {}
    system_totals: list[dict] = []
    for r in regions:
        rid = int(r.get("region_id") or 0)
        if rid > 0:
            if rid not in region_totals:
                region_totals[rid] = {"region_id": rid, "region_name": r.get("region_name", ""), "battle_count": 0}
            region_totals[rid]["battle_count"] += int(r.get("battle_count") or 0)
        system_totals.append({
            "system_id": int(r.get("system_id") or 0),
            "system_name": r.get("system_name", ""),
            "region_name": r.get("region_name", ""),
            "battle_count": int(r.get("battle_count") or 0),
        })

    top_regions = sorted(region_totals.values(), key=lambda x: x["battle_count"], reverse=True)[:TOP_K]
    top_systems = system_totals[:TOP_K]
    primary_region = top_regions[0] if top_regions else None
    primary_system = top_systems[0] if top_systems else None

    return {
        "top_regions": top_regions,
        "top_systems": top_systems,
        "primary_region_id": primary_region["region_id"] if primary_region else None,
        "primary_system_id": primary_system["system_id"] if primary_system else None,
    }


def _load_ship_summary(db: SupplyCoreDb, alliance_id: int) -> dict[str, Any]:
    """Load ship class and type preferences."""
    ships = db.fetch_all(
        """
        SELECT bp.ship_type_id,
               COALESCE(rit.type_name, CONCAT('Type #', bp.ship_type_id)) AS ship_name,
               bp.is_logi, bp.is_command, bp.is_capital,
               COUNT(*) AS usage_count
        FROM battle_participants bp
        LEFT JOIN ref_item_types rit ON rit.type_id = bp.ship_type_id
        WHERE bp.alliance_id = %s
          AND bp.ship_type_id IS NOT NULL AND bp.ship_type_id > 0
        GROUP BY bp.ship_type_id, bp.is_logi, bp.is_command, bp.is_capital
        ORDER BY usage_count DESC
        LIMIT 30
        """,
        (alliance_id,),
    )

    ship_types: list[dict] = []
    class_totals: dict[str, int] = defaultdict(int)
    for s in ships:
        # Derive fleet role from boolean flags
        if int(s.get("is_logi") or 0):
            fn = "logistics"
        elif int(s.get("is_command") or 0):
            fn = "command"
        elif int(s.get("is_capital") or 0):
            fn = "capital"
        else:
            fn = "dps"
        class_totals[fn] += int(s.get("usage_count") or 0)
        ship_types.append({
            "type_id": int(s.get("ship_type_id") or 0),
            "name": s.get("ship_name", ""),
            "fleet_function": fn,
            "count": int(s.get("usage_count") or 0),
        })

    top_classes = [{"class": k, "count": v} for k, v in sorted(class_totals.items(), key=lambda x: x[1], reverse=True)][:TOP_K]

    return {
        "top_ship_types": ship_types[:TOP_K],
        "top_ship_classes": top_classes,
    }


def _load_behavior_metrics(db: SupplyCoreDb, alliance_id: int) -> dict[str, Any]:
    """Load engagement rate, token participation, overperformance from battle actor features."""
    row = db.fetch_one(
        """
        SELECT AVG(baf.centrality_score) AS avg_centrality,
               AVG(baf.visibility_score) AS avg_visibility,
               SUM(CASE WHEN baf.participated_in_high_sustain = 1 THEN 1 ELSE 0 END) AS high_sustain_count,
               SUM(CASE WHEN baf.participated_in_low_sustain = 1 THEN 1 ELSE 0 END) AS low_sustain_count,
               COUNT(*) AS total_appearances
        FROM battle_actor_features baf
        INNER JOIN battle_participants bp
             ON bp.battle_id = baf.battle_id AND bp.character_id = baf.character_id
        WHERE bp.alliance_id = %s
        """,
        (alliance_id,),
    )

    total = int(row.get("total_appearances") or 0) if row else 0
    high_sustain = int(row.get("high_sustain_count") or 0) if row else 0
    low_sustain = int(row.get("low_sustain_count") or 0) if row else 0

    if total > 0:
        commitment_ratio = high_sustain / total
        token_ratio = low_sustain / total
    else:
        commitment_ratio = 0.0
        token_ratio = 0.0

    # Determine posture
    if commitment_ratio >= 0.5:
        posture = "committed"
    elif token_ratio >= 0.4:
        posture = "opportunistic"
    elif total < 5:
        posture = "infrequent"
    else:
        posture = "balanced"

    return {
        "avg_engagement_rate": round(commitment_ratio, 4),
        "avg_token_participation": round(token_ratio, 4),
        "posture": posture,
        "total_appearances": total,
        "high_sustain_count": high_sustain,
        "low_sustain_count": low_sustain,
    }


def _query_co_presence_neo4j(neo4j_client: Any, alliance_id: int) -> list[dict]:
    """Query Neo4j for alliances that frequently co-appear in the same battles."""
    if neo4j_client is None:
        return []
    try:
        rows = neo4j_client.query(
            """
            MATCH (a:Alliance {alliance_id: $aid})<-[:MEMBER_OF_ALLIANCE]-(c:Character)
                  -[:PARTICIPATED_IN]->(b:Battle)<-[:PARTICIPATED_IN]-(c2:Character)
                  -[:MEMBER_OF_ALLIANCE]->(a2:Alliance)
            WHERE a2.alliance_id <> $aid
            WITH a2.alliance_id AS co_alliance_id, COUNT(DISTINCT b) AS co_battles,
                 COUNT(DISTINCT c2) AS co_pilots
            WHERE co_battles >= 2
            RETURN co_alliance_id, co_battles, co_pilots
            ORDER BY co_battles DESC
            LIMIT 15
            """,
            {"aid": alliance_id},
        )
        return [{"alliance_id": int(r["co_alliance_id"]), "co_battles": int(r["co_battles"]),
                 "co_pilots": int(r["co_pilots"])} for r in rows]
    except Exception:
        return []


def _query_enemies_neo4j(neo4j_client: Any, alliance_id: int) -> list[dict]:
    """Query Neo4j for alliances most often on the opposing side."""
    if neo4j_client is None:
        return []
    try:
        rows = neo4j_client.query(
            """
            MATCH (a:Alliance {alliance_id: $aid})<-[:MEMBER_OF_ALLIANCE]-(c:Character)
                  -[:ON_SIDE]->(my_side:BattleSide)<-[:HAS_SIDE]-(b:Battle)
                  -[:HAS_SIDE]->(opp_side:BattleSide)-[:REPRESENTED_BY_ALLIANCE]->(enemy:Alliance)
            WHERE my_side.side_key <> opp_side.side_key
              AND enemy.alliance_id <> $aid
            WITH enemy.alliance_id AS enemy_id, COUNT(DISTINCT b) AS engagements
            WHERE engagements >= 2
            RETURN enemy_id, engagements
            ORDER BY engagements DESC
            LIMIT 15
            """,
            {"aid": alliance_id},
        )
        return [{"alliance_id": int(r["enemy_id"]), "engagements": int(r["engagements"])} for r in rows]
    except Exception:
        return []


def _compute_trend(db: SupplyCoreDb, alliance_id: int) -> dict[str, Any]:
    """Compute recent vs historical activity trend."""
    row = db.fetch_one(
        """
        SELECT
            COUNT(DISTINCT CASE WHEN br.started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                  THEN bp.battle_id END) AS battles_7d,
            COUNT(DISTINCT CASE WHEN br.started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
                  AND br.started_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                  THEN bp.battle_id END) AS battles_8_30d,
            COUNT(DISTINCT CASE WHEN br.started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
                  AND br.started_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
                  THEN bp.battle_id END) AS battles_31_90d
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        WHERE bp.alliance_id = %s
        """,
        (alliance_id,),
    )

    b7 = int(row.get("battles_7d") or 0) if row else 0
    b8_30 = int(row.get("battles_8_30d") or 0) if row else 0
    b31_90 = int(row.get("battles_31_90d") or 0) if row else 0

    # Normalize to weekly rate
    weekly_recent = b7
    weekly_mid = b8_30 / max(1, 23 / 7)  # ~3.3 weeks
    weekly_old = b31_90 / max(1, 60 / 7)  # ~8.6 weeks

    if weekly_recent > weekly_mid * 1.5:
        trend = "rising"
    elif weekly_recent < weekly_mid * 0.5 and weekly_mid > 0:
        trend = "declining"
    else:
        trend = "stable"

    return {
        "battles_7d": b7,
        "battles_8_30d": b8_30,
        "battles_31_90d": b31_90,
        "activity_trend": trend,
    }


def _resolve_alliance_names(db: SupplyCoreDb, alliance_ids: list[int]) -> dict[int, str]:
    """Bulk-resolve alliance names from entity_metadata_cache."""
    if not alliance_ids:
        return {}
    placeholders = ",".join(["%s"] * len(alliance_ids))
    rows = db.fetch_all(
        f"SELECT entity_id, entity_name FROM entity_metadata_cache "
        f"WHERE entity_type = 'alliance' AND entity_id IN ({placeholders})",
        tuple(alliance_ids),
    )
    return {int(r["entity_id"]): str(r["entity_name"]) for r in rows if r.get("entity_name")}


def _flush_dossiers(db: SupplyCoreDb, dossiers: list[dict[str, Any]]) -> int:
    """Write dossier rows to MariaDB."""
    if not dossiers:
        return 0
    rows_written = 0
    for batch_start in range(0, len(dossiers), BATCH_SIZE):
        chunk = dossiers[batch_start:batch_start + BATCH_SIZE]
        with db.transaction() as (_, cursor):
            for d in chunk:
                cursor.execute(
                    """
                    INSERT INTO alliance_dossiers (
                        alliance_id, alliance_name, total_battles, recent_battles,
                        first_seen_at, last_seen_at, primary_region_id, primary_system_id,
                        avg_engagement_rate, avg_token_participation, avg_overperformance,
                        posture, top_co_present_json, top_enemies_json,
                        top_regions_json, top_systems_json, top_ship_classes_json,
                        top_ship_types_json, behavior_summary_json, trend_summary_json,
                        computed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        alliance_name=VALUES(alliance_name), total_battles=VALUES(total_battles),
                        recent_battles=VALUES(recent_battles), first_seen_at=VALUES(first_seen_at),
                        last_seen_at=VALUES(last_seen_at), primary_region_id=VALUES(primary_region_id),
                        primary_system_id=VALUES(primary_system_id),
                        avg_engagement_rate=VALUES(avg_engagement_rate),
                        avg_token_participation=VALUES(avg_token_participation),
                        avg_overperformance=VALUES(avg_overperformance),
                        posture=VALUES(posture),
                        top_co_present_json=VALUES(top_co_present_json),
                        top_enemies_json=VALUES(top_enemies_json),
                        top_regions_json=VALUES(top_regions_json),
                        top_systems_json=VALUES(top_systems_json),
                        top_ship_classes_json=VALUES(top_ship_classes_json),
                        top_ship_types_json=VALUES(top_ship_types_json),
                        behavior_summary_json=VALUES(behavior_summary_json),
                        trend_summary_json=VALUES(trend_summary_json),
                        computed_at=VALUES(computed_at)
                    """,
                    (
                        d["alliance_id"], d["alliance_name"], d["total_battles"], d["recent_battles"],
                        d["first_seen_at"], d["last_seen_at"], d["primary_region_id"], d["primary_system_id"],
                        d["avg_engagement_rate"], d["avg_token_participation"], d.get("avg_overperformance"),
                        d["posture"], d["top_co_present_json"], d["top_enemies_json"],
                        d["top_regions_json"], d["top_systems_json"], d["top_ship_classes_json"],
                        d["top_ship_types_json"], d["behavior_summary_json"], d["trend_summary_json"],
                        d["computed_at"],
                    ),
                )
                rows_written += max(0, int(cursor.rowcount or 0))
    return rows_written


def run_compute_alliance_dossiers(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    neo4j_raw: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute alliance dossiers and persist to MariaDB."""
    lock_key = "compute_alliance_dossiers"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=1800)
    if owner is None:
        return JobResult.skipped(job_key=lock_key, reason="lock-not-acquired").to_dict()

    job = start_job_run(db, lock_key)
    started = datetime.now(UTC)
    computed_at = _now_sql()
    rows_processed = 0
    rows_written = 0

    try:
        # Initialize Neo4j client if available
        neo4j_client = None
        try:
            from ..neo4j import Neo4jClient, Neo4jConfig
            config = Neo4jConfig.from_runtime(neo4j_raw or {})
            if config.enabled:
                neo4j_client = Neo4jClient(config)
        except Exception:
            neo4j_client = None

        alliances = _load_alliances_with_battles(db, min_battles=2)
        rows_processed = len(alliances)

        if not alliances:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
            return JobResult.success(job_key=lock_key, summary="No alliances with battles found.",
                                    rows_processed=0, rows_written=0, duration_ms=0).to_dict()

        # Collect all alliance IDs for name resolution
        all_ally_ids: set[int] = set()
        dossiers: list[dict[str, Any]] = []

        for a in alliances:
            aid = int(a["alliance_id"])
            geo = _load_geographic_summary(db, aid)
            ships = _load_ship_summary(db, aid)
            behavior = _load_behavior_metrics(db, aid)
            trend = _compute_trend(db, aid)

            co_present = _query_co_presence_neo4j(neo4j_client, aid)
            enemies = _query_enemies_neo4j(neo4j_client, aid)

            for cp in co_present:
                all_ally_ids.add(cp["alliance_id"])
            for en in enemies:
                all_ally_ids.add(en["alliance_id"])

            dossiers.append({
                "alliance_id": aid,
                "alliance_name": a.get("alliance_name", ""),
                "total_battles": int(a.get("total_battles") or 0),
                "recent_battles": int(a.get("recent_battles") or 0),
                "first_seen_at": a.get("first_seen_at"),
                "last_seen_at": a.get("last_seen_at"),
                "primary_region_id": geo["primary_region_id"],
                "primary_system_id": geo["primary_system_id"],
                "avg_engagement_rate": behavior["avg_engagement_rate"],
                "avg_token_participation": behavior["avg_token_participation"],
                "avg_overperformance": None,
                "posture": behavior["posture"],
                "top_co_present_json": json_dumps_safe(co_present),
                "top_enemies_json": json_dumps_safe(enemies),
                "top_regions_json": json_dumps_safe(geo["top_regions"]),
                "top_systems_json": json_dumps_safe(geo["top_systems"]),
                "top_ship_classes_json": json_dumps_safe(ships["top_ship_classes"]),
                "top_ship_types_json": json_dumps_safe(ships["top_ship_types"]),
                "behavior_summary_json": json_dumps_safe(behavior),
                "trend_summary_json": json_dumps_safe(trend),
                "computed_at": computed_at,
            })

        # Resolve names for co-present and enemy alliances, enrich JSON
        name_map = _resolve_alliance_names(db, list(all_ally_ids))
        for d in dossiers:
            co_list = json.loads(d["top_co_present_json"]) if d["top_co_present_json"] else []
            for item in co_list:
                item["name"] = name_map.get(item["alliance_id"], f"Alliance #{item['alliance_id']}")
            d["top_co_present_json"] = json_dumps_safe(co_list)

            en_list = json.loads(d["top_enemies_json"]) if d["top_enemies_json"] else []
            for item in en_list:
                item["name"] = name_map.get(item["alliance_id"], f"Alliance #{item['alliance_id']}")
            d["top_enemies_json"] = json_dumps_safe(en_list)

        if not dry_run:
            rows_written = _flush_dossiers(db, dossiers)

        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written,
                       meta={"dossier_count": len(dossiers)})
        return JobResult.success(
            job_key=lock_key,
            summary=f"Computed {len(dossiers)} alliance dossiers.",
            rows_processed=rows_processed,
            rows_written=rows_written,
            duration_ms=duration_ms,
        ).to_dict()

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise
    finally:
        release_job_lock(db, lock_key, owner)
