"""Alliance Dossier computation — killmail-powered intelligence briefs per alliance.

Derives alliance intelligence from ALL killmails (including ``mail_type='untracked'``)
rather than only clustered battles.  This gives visibility into small-gang, blops,
gate camps, and structure bashes that fall below the battle clustering threshold
(20+ participants).  Battle counts are retained as supplementary metrics.

Queries MariaDB ``killmail_events`` / ``killmail_attackers`` for geography, fleet
composition, behavior, and trends.  Co-presence and enemy data comes from the
pre-computed ``alliance_relationships`` table (built by ``compute_alliance_relationships``
from all killmails), with Neo4j and SQL fallbacks.

Payload Contract (JSON columns stored in alliance_dossiers)
-----------------------------------------------------------

**top_co_present_json** — alliances co-occurring on the same side::

    [{"alliance_id": int, "alliance_name": str, "shared_battles": int,
      "shared_pilots": int, "source": "relationship_graph"|"neo4j"|"sql"}]

**top_enemies_json** — alliances fought against on opposing sides::

    [{"alliance_id": int, "alliance_name": str, "engagements": int,
      "source": "relationship_graph"|"neo4j"|"sql"}]

**top_regions_json**::

    [{"region_id": int, "region_name": str, "killmail_count": int}]

**top_systems_json**::

    [{"system_id": int, "system_name": str, "region_name": str, "killmail_count": int}]

**top_ship_classes_json**::

    [{"class": str, "count": int}]

**top_ship_types_json**::

    [{"type_id": int, "name": str, "fleet_function": str, "count": int}]

**behavior_summary_json**::

    {"kills_per_week": float, "avg_gang_size": float, "solo_ratio": float,
     "total_kills": int, "total_losses": int, "kill_loss_ratio": float,
     "posture": str, "active_pilots": int}

    Posture values: "aggressive", "opportunistic", "infrequent", "balanced"

**trend_summary_json**::

    {"killmails_7d": int, "killmails_8_30d": int, "killmails_31_90d": int,
     "isk_destroyed_7d": float, "isk_destroyed_8_30d": float,
     "isk_destroyed_31_90d": float,
     "activity_trend": "rising"|"declining"|"stable"}
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.config import resolve_app_root  # noqa: F401
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import finish_job_run, start_job_run
else:
    from ..config import resolve_app_root  # noqa: F401
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import finish_job_run, start_job_run

BATCH_SIZE = 200
RECENT_DAYS = 30
TOP_K = 10


def _dossier_log(runtime: dict[str, Any] | None, event: str, payload: dict[str, Any]) -> None:
    log_path = str(((runtime or {}).get("log_file") or "")).strip()
    if log_path == "":
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_dumps_safe(record) + "\n")


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _load_alliances_with_activity(db: SupplyCoreDb, min_killmails: int = 5) -> list[dict[str, Any]]:
    """Load alliances with killmail activity from attacker data.

    An alliance qualifies if its members appear as attackers on at least
    ``min_killmails`` distinct killmails.  This captures all combat activity,
    not just clustered battles.
    """
    return db.fetch_all(
        """
        SELECT ka.alliance_id,
               COALESCE(emc.entity_name, CONCAT('Alliance #', ka.alliance_id)) AS alliance_name,
               COUNT(DISTINCT ka.sequence_id) AS total_killmails,
               COUNT(DISTINCT CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                     THEN ka.sequence_id END) AS recent_killmails,
               COUNT(DISTINCT CASE WHEN ke.battle_id IS NOT NULL THEN ke.battle_id END) AS total_battles,
               COUNT(DISTINCT CASE WHEN ke.battle_id IS NOT NULL
                     AND ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                     THEN ke.battle_id END) AS recent_battles,
               COALESCE(SUM(ke.zkb_total_value), 0) AS total_isk_destroyed,
               COALESCE(SUM(CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                     THEN ke.zkb_total_value ELSE 0 END), 0) AS recent_isk_destroyed,
               COUNT(DISTINCT ka.character_id) AS active_pilots,
               COUNT(DISTINCT CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                     THEN ka.character_id END) AS recent_active_pilots,
               MIN(ke.killmail_time) AS first_seen_at,
               MAX(ke.killmail_time) AS last_seen_at
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = 'alliance' AND emc.entity_id = ka.alliance_id
        WHERE ka.alliance_id IS NOT NULL AND ka.alliance_id > 0
          AND ke.zkb_npc = 0
        GROUP BY ka.alliance_id
        HAVING total_killmails >= %s
        ORDER BY recent_killmails DESC, total_killmails DESC
        """,
        (RECENT_DAYS, RECENT_DAYS, RECENT_DAYS, RECENT_DAYS, min_killmails),
    )


def _load_geographic_summary(db: SupplyCoreDb, alliance_id: int) -> dict[str, Any]:
    """Load geographic concentration from all killmail data.

    Counts killmails where the alliance's members participated as attackers,
    grouped by solar system.  This captures small-gang activity that never
    clusters into a battle.
    """
    rows = db.fetch_all(
        """
        SELECT ke.solar_system_id AS system_id,
               rs.system_name, rs.region_id, rr.region_name,
               COUNT(DISTINCT ka.sequence_id) AS killmail_count
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        LEFT JOIN ref_systems rs ON rs.system_id = ke.solar_system_id
        LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
        WHERE ka.alliance_id = %s
          AND ke.zkb_npc = 0
          AND ke.solar_system_id IS NOT NULL
        GROUP BY ke.solar_system_id
        ORDER BY killmail_count DESC
        LIMIT 50
        """,
        (alliance_id,),
    )

    region_totals: dict[int, dict] = {}
    system_totals: list[dict] = []
    for r in rows:
        rid = int(r.get("region_id") or 0)
        if rid > 0:
            if rid not in region_totals:
                region_totals[rid] = {"region_id": rid, "region_name": r.get("region_name", ""), "killmail_count": 0}
            region_totals[rid]["killmail_count"] += int(r.get("killmail_count") or 0)
        system_totals.append({
            "system_id": int(r.get("system_id") or 0),
            "system_name": r.get("system_name", ""),
            "region_name": r.get("region_name", ""),
            "killmail_count": int(r.get("killmail_count") or 0),
        })

    top_regions = sorted(region_totals.values(), key=lambda x: x["killmail_count"], reverse=True)[:TOP_K]
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
    """Load ship class and type preferences from all killmail attacker data.

    Uses ``killmail_attackers.ship_type_id`` which captures the ship each
    pilot was flying on every killmail — much richer than battle_participants
    which only records one ship per battle.

    Fleet function is derived from ``ref_item_types`` group metadata when
    available, falling back to a simple "dps" classification.
    """
    ships = db.fetch_all(
        """
        SELECT ka.ship_type_id,
               COALESCE(rit.type_name, CONCAT('Type #', ka.ship_type_id)) AS ship_name,
               COALESCE(rit.group_name, '') AS group_name,
               COALESCE(rit.market_group_name, '') AS market_group_name,
               COUNT(*) AS usage_count
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        LEFT JOIN ref_item_types rit ON rit.type_id = ka.ship_type_id
        WHERE ka.alliance_id = %s
          AND ka.ship_type_id IS NOT NULL AND ka.ship_type_id > 0
          AND ke.zkb_npc = 0
        GROUP BY ka.ship_type_id
        ORDER BY usage_count DESC
        LIMIT 30
        """,
        (alliance_id,),
    )

    ship_types: list[dict] = []
    class_totals: dict[str, int] = defaultdict(int)
    for s in ships:
        fn = _classify_fleet_function(
            s.get("group_name", ""),
            s.get("market_group_name", ""),
        )
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


# Keywords used for fleet function classification from group/market group names
_LOGI_KEYWORDS = {"logistics", "remote armor", "remote shield", "remote repair", "fax", "force auxiliary"}
_COMMAND_KEYWORDS = {"command", "strategic cruiser", "command destroyer", "command ship"}
_CAPITAL_KEYWORDS = {"capital", "dreadnought", "carrier", "supercarrier", "titan", "force auxiliary"}


def _classify_fleet_function(group_name: str, market_group_name: str) -> str:
    """Classify a ship into a fleet function based on group metadata."""
    combined = (group_name + " " + market_group_name).lower()
    if any(kw in combined for kw in _LOGI_KEYWORDS):
        return "logistics"
    if any(kw in combined for kw in _CAPITAL_KEYWORDS):
        return "capital"
    if any(kw in combined for kw in _COMMAND_KEYWORDS):
        return "command"
    return "dps"


def _load_behavior_metrics(db: SupplyCoreDb, alliance_id: int) -> dict[str, Any]:
    """Load behavioral profile from all killmail data.

    Metrics:
    - kills_per_week: average weekly kill rate over the last 90 days
    - avg_gang_size: average number of co-attackers on their kills
    - solo_ratio: fraction of kills with exactly 1 attacker
    - total_kills: killmails where alliance members were attackers
    - total_losses: killmails where alliance members were victims
    - kill_loss_ratio: kills / max(losses, 1)
    - posture: classified from activity patterns
    - active_pilots: distinct characters on killmails (last 90d)
    """
    row = db.fetch_one(
        """
        SELECT COUNT(DISTINCT ka.sequence_id) AS total_kills,
               COUNT(DISTINCT ka.character_id) AS active_pilots,
               MIN(ke.killmail_time) AS earliest_kill,
               MAX(ke.killmail_time) AS latest_kill
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        WHERE ka.alliance_id = %s
          AND ke.zkb_npc = 0
          AND ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
        """,
        (alliance_id,),
    )

    total_kills = int(row.get("total_kills") or 0) if row else 0
    active_pilots = int(row.get("active_pilots") or 0) if row else 0

    # Calculate kills per week over 90 day window
    kills_per_week = round(total_kills / max(1, 90 / 7), 2)

    # Average gang size: how many attackers per killmail on average
    gang_row = db.fetch_one(
        """
        SELECT AVG(attacker_count) AS avg_gang_size,
               SUM(CASE WHEN attacker_count = 1 THEN 1 ELSE 0 END) AS solo_kills,
               COUNT(*) AS total_counted
        FROM (
            SELECT ka.sequence_id, COUNT(*) AS attacker_count
            FROM killmail_attackers ka
            INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
            WHERE ka.alliance_id = %s
              AND ke.zkb_npc = 0
              AND ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
            GROUP BY ka.sequence_id
        ) sub
        """,
        (alliance_id,),
    )

    avg_gang_size = round(float(gang_row.get("avg_gang_size") or 0), 1) if gang_row else 0.0
    solo_kills = int(gang_row.get("solo_kills") or 0) if gang_row else 0
    total_counted = int(gang_row.get("total_counted") or 0) if gang_row else 0
    solo_ratio = round(solo_kills / max(total_counted, 1), 4)

    # Count losses (alliance members as victims)
    loss_row = db.fetch_one(
        """
        SELECT COUNT(*) AS total_losses
        FROM killmail_events
        WHERE victim_alliance_id = %s
          AND zkb_npc = 0
          AND killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
        """,
        (alliance_id,),
    )
    total_losses = int(loss_row.get("total_losses") or 0) if loss_row else 0
    kl_ratio = round(total_kills / max(total_losses, 1), 2)

    # Determine posture from activity patterns
    if kills_per_week >= 50 and avg_gang_size >= 10:
        posture = "aggressive"
    elif kills_per_week >= 10 and avg_gang_size < 8:
        posture = "opportunistic"
    elif total_kills < 10:
        posture = "infrequent"
    else:
        posture = "balanced"

    return {
        "kills_per_week": kills_per_week,
        "avg_gang_size": avg_gang_size,
        "solo_ratio": solo_ratio,
        "total_kills": total_kills,
        "total_losses": total_losses,
        "kill_loss_ratio": kl_ratio,
        "posture": posture,
        "active_pilots": active_pilots,
    }


def _query_co_presence_from_relationship_graph(db: SupplyCoreDb, alliance_id: int) -> list[dict]:
    """Primary source: read pre-computed allied edges from alliance_relationships.

    This table is built from ALL killmails (including untracked) by
    compute_alliance_relationships, giving much richer data than battle-only queries.
    """
    rows = db.fetch_all(
        """
        SELECT target_alliance_id AS co_alliance_id,
               shared_killmails AS shared_battles,
               shared_pilots,
               confidence
        FROM alliance_relationships
        WHERE source_alliance_id = %s
          AND relationship_type = 'allied'
          AND confidence >= 0.15
        ORDER BY confidence DESC, shared_killmails DESC
        LIMIT 15
        """,
        (alliance_id,),
    )
    return [{"alliance_id": int(r["co_alliance_id"]),
             "shared_battles": int(r["shared_battles"]),
             "shared_pilots": int(r.get("shared_pilots") or 0),
             "confidence": float(r.get("confidence") or 0),
             "source": "relationship_graph"} for r in rows]


def _query_enemies_from_relationship_graph(db: SupplyCoreDb, alliance_id: int) -> list[dict]:
    """Primary source: read pre-computed hostile edges from alliance_relationships."""
    rows = db.fetch_all(
        """
        SELECT target_alliance_id AS enemy_id,
               shared_killmails AS engagements,
               confidence
        FROM alliance_relationships
        WHERE source_alliance_id = %s
          AND relationship_type = 'hostile'
          AND confidence >= 0.15
        ORDER BY confidence DESC, shared_killmails DESC
        LIMIT 15
        """,
        (alliance_id,),
    )
    return [{"alliance_id": int(r["enemy_id"]),
             "engagements": int(r["engagements"]),
             "confidence": float(r.get("confidence") or 0),
             "source": "relationship_graph"} for r in rows]


def _query_co_presence_sql(db: SupplyCoreDb, alliance_id: int) -> list[dict]:
    """SQL fallback: find alliances fighting on the same side via killmail co-attacker data.

    Two alliances are co-present (same side) when their members appear as
    co-attackers on the same killmails.  Uses ALL killmails, not just
    battle-linked ones.
    """
    rows = db.fetch_all(
        """
        SELECT ka2.alliance_id AS co_alliance_id,
               COUNT(DISTINCT ka1.sequence_id) AS shared_battles,
               COUNT(DISTINCT ka2.character_id) AS shared_pilots
        FROM killmail_attackers ka1
        INNER JOIN killmail_attackers ka2
             ON ka2.sequence_id = ka1.sequence_id
            AND ka2.alliance_id <> ka1.alliance_id
        INNER JOIN killmail_events ke
             ON ke.sequence_id = ka1.sequence_id
            AND ke.zkb_npc = 0
        WHERE ka1.alliance_id = %s
          AND ka2.alliance_id IS NOT NULL AND ka2.alliance_id > 0
        GROUP BY ka2.alliance_id
        HAVING shared_battles >= 2
        ORDER BY shared_battles DESC
        LIMIT 15
        """,
        (alliance_id,),
    )
    return [{"alliance_id": int(r["co_alliance_id"]), "shared_battles": int(r["shared_battles"]),
             "shared_pilots": int(r["shared_pilots"]), "source": "sql"} for r in rows]


def _query_enemies_sql(db: SupplyCoreDb, alliance_id: int) -> list[dict]:
    """SQL fallback: find alliances on opposing sides via killmail attacker/victim data.

    An alliance is an enemy when our members attack their members (they are
    victims) or their members attack ours.  Uses ALL killmails.
    """
    rows = db.fetch_all(
        """
        SELECT enemy_id, COUNT(DISTINCT killmail_id) AS engagements
        FROM (
            SELECT ke.victim_alliance_id AS enemy_id, ke.sequence_id AS killmail_id
            FROM killmail_attackers ka
            INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
            WHERE ka.alliance_id = %s
              AND ke.victim_alliance_id IS NOT NULL AND ke.victim_alliance_id > 0
              AND ke.victim_alliance_id <> %s
              AND ke.zkb_npc = 0
            UNION
            SELECT ka.alliance_id AS enemy_id, ke.sequence_id AS killmail_id
            FROM killmail_events ke
            INNER JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
            WHERE ke.victim_alliance_id = %s
              AND ka.alliance_id IS NOT NULL AND ka.alliance_id > 0
              AND ka.alliance_id <> %s
              AND ke.zkb_npc = 0
        ) AS combined
        GROUP BY enemy_id
        HAVING engagements >= 2
        ORDER BY engagements DESC
        LIMIT 15
        """,
        (alliance_id, alliance_id, alliance_id, alliance_id),
    )
    return [{"alliance_id": int(r["enemy_id"]), "engagements": int(r["engagements"]),
             "source": "sql"} for r in rows]


def _query_co_presence_neo4j(neo4j_client: Any, alliance_id: int) -> list[dict]:
    """Query Neo4j for alliances whose members fight on the same side.

    Uses killmail co-attacker relationships: two alliances are co-present
    when their members appear as co-attackers (both ``ATTACKED_ON``) on the
    same ``Killmail`` node.  Traverses the CURRENT_CORP → PART_OF chain to
    resolve alliance membership, as MEMBER_OF_ALLIANCE edges are sparse.

    Returns canonical contract: ``{alliance_id, shared_battles, shared_pilots, source}``.
    Note: ``shared_battles`` counts distinct shared killmails (not battle rollups).
    """
    if neo4j_client is None:
        return []
    try:
        rows = neo4j_client.query(
            """
            MATCH (a:Alliance {alliance_id: $aid})
                  <-[:PART_OF]-(:Corporation)
                  <-[:CURRENT_CORP]-(c:Character)
                  -[:ATTACKED_ON]->(k:Killmail)
                  <-[:ATTACKED_ON]-(c2:Character)
                  -[:CURRENT_CORP]->(:Corporation)
                  -[:PART_OF]->(a2:Alliance)
            WHERE a2.alliance_id <> $aid
            WITH a2.alliance_id AS co_alliance_id,
                 COUNT(DISTINCT k) AS shared_battles,
                 COUNT(DISTINCT c2) AS shared_pilots
            WHERE shared_battles >= 2
            RETURN co_alliance_id, shared_battles, shared_pilots
            ORDER BY shared_battles DESC
            LIMIT 15
            """,
            {"aid": alliance_id},
        )
        return [{"alliance_id": int(r["co_alliance_id"]),
                 "shared_battles": int(r["shared_battles"]),
                 "shared_pilots": int(r["shared_pilots"]),
                 "source": "neo4j"} for r in rows]
    except Exception:
        return []


def _query_enemies_neo4j(neo4j_client: Any, alliance_id: int) -> list[dict]:
    """Query Neo4j for alliances most often on the opposing side.

    Uses killmail attacker/victim relationships.  An alliance is an enemy
    when our members attack their members (they are victims on a ``Killmail``
    we ``ATTACKED_ON``) or vice-versa.  Both directions are combined via
    ``CALL {} UNION ALL`` and counted as distinct killmails.

    Traverses CURRENT_CORP → PART_OF chain for alliance membership since
    MEMBER_OF_ALLIANCE edges are sparse.

    Returns canonical contract: ``{alliance_id, engagements, source}``.
    """
    if neo4j_client is None:
        return []
    try:
        rows = neo4j_client.query(
            """
            CALL {
                MATCH (a:Alliance {alliance_id: $aid})
                      <-[:PART_OF]-(:Corporation)
                      <-[:CURRENT_CORP]-(c:Character)
                      -[:ATTACKED_ON]->(k:Killmail)
                      <-[:VICTIM_OF]-(v:Character)
                      -[:CURRENT_CORP]->(:Corporation)
                      -[:PART_OF]->(e:Alliance)
                WHERE e.alliance_id <> $aid
                RETURN e.alliance_id AS enemy_id, k.killmail_id AS kid
                UNION ALL
                MATCH (a:Alliance {alliance_id: $aid})
                      <-[:PART_OF]-(:Corporation)
                      <-[:CURRENT_CORP]-(c:Character)
                      -[:VICTIM_OF]->(k:Killmail)
                      <-[:ATTACKED_ON]-(att:Character)
                      -[:CURRENT_CORP]->(:Corporation)
                      -[:PART_OF]->(e:Alliance)
                WHERE e.alliance_id <> $aid
                RETURN e.alliance_id AS enemy_id, k.killmail_id AS kid
            }
            WITH enemy_id, COUNT(DISTINCT kid) AS engagements
            WHERE engagements >= 2
            RETURN enemy_id, engagements
            ORDER BY engagements DESC
            LIMIT 15
            """,
            {"aid": alliance_id},
        )
        return [{"alliance_id": int(r["enemy_id"]), "engagements": int(r["engagements"]),
                 "source": "neo4j"} for r in rows]
    except Exception:
        return []


def _compute_trend(db: SupplyCoreDb, alliance_id: int) -> dict[str, Any]:
    """Compute recent vs historical activity trend from all killmail data.

    Uses killmail counts and ISK destroyed in rolling windows for a much
    more granular signal than battle-only trends.
    """
    row = db.fetch_one(
        """
        SELECT
            COUNT(DISTINCT CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                  THEN ka.sequence_id END) AS killmails_7d,
            COUNT(DISTINCT CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
                  AND ke.killmail_time < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                  THEN ka.sequence_id END) AS killmails_8_30d,
            COUNT(DISTINCT CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
                  AND ke.killmail_time < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
                  THEN ka.sequence_id END) AS killmails_31_90d,
            COALESCE(SUM(CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                  THEN ke.zkb_total_value ELSE 0 END), 0) AS isk_destroyed_7d,
            COALESCE(SUM(CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
                  AND ke.killmail_time < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                  THEN ke.zkb_total_value ELSE 0 END), 0) AS isk_destroyed_8_30d,
            COALESCE(SUM(CASE WHEN ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
                  AND ke.killmail_time < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
                  THEN ke.zkb_total_value ELSE 0 END), 0) AS isk_destroyed_31_90d
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        WHERE ka.alliance_id = %s
          AND ke.zkb_npc = 0
        """,
        (alliance_id,),
    )

    k7 = int(row.get("killmails_7d") or 0) if row else 0
    k8_30 = int(row.get("killmails_8_30d") or 0) if row else 0
    k31_90 = int(row.get("killmails_31_90d") or 0) if row else 0
    isk_7d = round(float(row.get("isk_destroyed_7d") or 0), 2) if row else 0.0
    isk_8_30d = round(float(row.get("isk_destroyed_8_30d") or 0), 2) if row else 0.0
    isk_31_90d = round(float(row.get("isk_destroyed_31_90d") or 0), 2) if row else 0.0

    # Normalize to weekly rate for trend detection
    weekly_recent = k7
    weekly_mid = k8_30 / max(1, 23 / 7)  # ~3.3 weeks
    weekly_old = k31_90 / max(1, 60 / 7)  # ~8.6 weeks

    if weekly_recent > weekly_mid * 1.5:
        trend = "rising"
    elif weekly_recent < weekly_mid * 0.5 and weekly_mid > 0:
        trend = "declining"
    else:
        trend = "stable"

    return {
        "killmails_7d": k7,
        "killmails_8_30d": k8_30,
        "killmails_31_90d": k31_90,
        "isk_destroyed_7d": isk_7d,
        "isk_destroyed_8_30d": isk_8_30d,
        "isk_destroyed_31_90d": isk_31_90d,
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
                        total_killmails, recent_killmails,
                        total_isk_destroyed, recent_isk_destroyed,
                        active_pilots, recent_active_pilots,
                        first_seen_at, last_seen_at, primary_region_id, primary_system_id,
                        avg_engagement_rate, avg_token_participation, avg_overperformance,
                        posture, top_co_present_json, top_enemies_json,
                        top_regions_json, top_systems_json, top_ship_classes_json,
                        top_ship_types_json, behavior_summary_json, trend_summary_json,
                        computed_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        alliance_name=VALUES(alliance_name), total_battles=VALUES(total_battles),
                        recent_battles=VALUES(recent_battles),
                        total_killmails=VALUES(total_killmails),
                        recent_killmails=VALUES(recent_killmails),
                        total_isk_destroyed=VALUES(total_isk_destroyed),
                        recent_isk_destroyed=VALUES(recent_isk_destroyed),
                        active_pilots=VALUES(active_pilots),
                        recent_active_pilots=VALUES(recent_active_pilots),
                        first_seen_at=VALUES(first_seen_at),
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
                        d["alliance_id"], d["alliance_name"],
                        d["total_battles"], d["recent_battles"],
                        d["total_killmails"], d["recent_killmails"],
                        d["total_isk_destroyed"], d["recent_isk_destroyed"],
                        d["active_pilots"], d["recent_active_pilots"],
                        d["first_seen_at"], d["last_seen_at"],
                        d["primary_region_id"], d["primary_system_id"],
                        d["avg_engagement_rate"], d["avg_token_participation"],
                        d.get("avg_overperformance"),
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
    job_key = "compute_alliance_dossiers"
    job = start_job_run(db, job_key)
    started = datetime.now(UTC)
    computed_at = _now_sql()
    rows_processed = 0
    rows_written = 0

    _dossier_log(runtime, "alliance_dossiers.job.started", {"dry_run": dry_run, "computed_at": computed_at})

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

        alliances = _load_alliances_with_activity(db, min_killmails=5)
        rows_processed = len(alliances)

        if not alliances:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
            return JobResult.success(job_key=job_key, summary="No alliances with killmail activity found.",
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

            # Prefer pre-computed relationship graph (built from ALL killmails),
            # then Neo4j, then raw SQL as final fallback.
            co_present = _query_co_presence_from_relationship_graph(db, aid)
            if not co_present:
                co_present = _query_co_presence_neo4j(neo4j_client, aid)
            if not co_present:
                co_present = _query_co_presence_sql(db, aid)
                if co_present:
                    logger.info("alliance %d: co-presence from SQL fallback (%d results)", aid, len(co_present))
            enemies = _query_enemies_from_relationship_graph(db, aid)
            if not enemies:
                enemies = _query_enemies_neo4j(neo4j_client, aid)
            if not enemies:
                enemies = _query_enemies_sql(db, aid)
                if enemies:
                    logger.info("alliance %d: enemies from SQL fallback (%d results)", aid, len(enemies))

            # Deduplicate: if an alliance appears in both co-present and
            # enemies, keep it only in the list where the count is higher.
            co_map = {cp["alliance_id"]: cp.get("shared_battles") or cp.get("count", 0) for cp in co_present}
            en_map = {en["alliance_id"]: en.get("engagements") or en.get("count", 0) for en in enemies}
            overlap = set(co_map) & set(en_map)
            if overlap:
                logger.info("alliance %d: %d overlapping alliances in co-present/enemies, deduplicating", aid, len(overlap))
                for oid in overlap:
                    if en_map[oid] >= co_map[oid]:
                        co_present = [cp for cp in co_present if cp["alliance_id"] != oid]
                    else:
                        enemies = [en for en in enemies if en["alliance_id"] != oid]

            for cp in co_present:
                all_ally_ids.add(cp["alliance_id"])
            for en in enemies:
                all_ally_ids.add(en["alliance_id"])

            dossiers.append({
                "alliance_id": aid,
                "alliance_name": a.get("alliance_name", ""),
                "total_battles": int(a.get("total_battles") or 0),
                "recent_battles": int(a.get("recent_battles") or 0),
                "total_killmails": int(a.get("total_killmails") or 0),
                "recent_killmails": int(a.get("recent_killmails") or 0),
                "total_isk_destroyed": float(a.get("total_isk_destroyed") or 0),
                "recent_isk_destroyed": float(a.get("recent_isk_destroyed") or 0),
                "active_pilots": int(a.get("active_pilots") or 0),
                "recent_active_pilots": int(a.get("recent_active_pilots") or 0),
                "first_seen_at": a.get("first_seen_at"),
                "last_seen_at": a.get("last_seen_at"),
                "primary_region_id": geo["primary_region_id"],
                "primary_system_id": geo["primary_system_id"],
                "avg_engagement_rate": behavior.get("kills_per_week", 0),
                "avg_token_participation": behavior.get("solo_ratio", 0),
                "avg_overperformance": behavior.get("kill_loss_ratio"),
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
        unresolved_ids: list[int] = []
        for d in dossiers:
            co_list = json.loads(d["top_co_present_json"]) if d["top_co_present_json"] else []
            for item in co_list:
                resolved = name_map.get(item["alliance_id"])
                item["alliance_name"] = resolved or f"Alliance #{item['alliance_id']}"
                if not resolved:
                    unresolved_ids.append(item["alliance_id"])
            d["top_co_present_json"] = json_dumps_safe(co_list)

            en_list = json.loads(d["top_enemies_json"]) if d["top_enemies_json"] else []
            for item in en_list:
                resolved = name_map.get(item["alliance_id"])
                item["alliance_name"] = resolved or f"Alliance #{item['alliance_id']}"
                if not resolved:
                    unresolved_ids.append(item["alliance_id"])
            d["top_enemies_json"] = json_dumps_safe(en_list)

        if unresolved_ids:
            unique_unresolved = sorted(set(unresolved_ids))
            logger.warning(
                "Could not resolve names for %d alliance IDs (first 10: %s). "
                "These will display as 'Alliance #ID'. Check entity_metadata_cache population.",
                len(unique_unresolved), unique_unresolved[:10],
            )

        if not dry_run:
            rows_written = _flush_dossiers(db, dossiers)

        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written,
                       meta={"dossier_count": len(dossiers)})
        result = JobResult.success(
            job_key=job_key,
            summary=f"Computed {len(dossiers)} alliance dossiers.",
            rows_processed=rows_processed,
            rows_written=rows_written,
            duration_ms=duration_ms,
        ).to_dict()
        _dossier_log(runtime, "alliance_dossiers.job.success", result)
        return result

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _dossier_log(runtime, "alliance_dossiers.job.failed", {"status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise
