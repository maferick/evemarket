"""Compute Bloom entry-point labels on the Neo4j intelligence graph.

Bloom perspectives work best when analysts start from a small set of
curated anchor nodes rather than scanning the entire graph.  This job
maintains four "smart entry point" labels on top of the canonical
Character / Battle / System / Alliance nodes:

    :HotBattle          — recent, high-intensity engagements
    :HighRiskPilot      — pilots above the suspicion threshold
    :StrategicSystem    — systems with sustained recent battle density
    :HotAlliance        — alliances with recent engagement volume

The labels are additive — they never replace canonical labels — and they
are refreshed incrementally on every run: nodes that meet the criteria
are tagged, nodes that no longer qualify are untagged.  All writes are
bounded by LIMIT clauses so this job never produces unbounded Neo4j
transactions.

All thresholds are runtime-overridable via the ``neo4j`` runtime section
so operators can tune them from the PHP control plane without touching
code:

    bloom_hot_battle_window_days
    bloom_hot_battle_min_participants
    bloom_high_risk_pilot_min_score
    bloom_strategic_system_window_days
    bloom_strategic_system_min_battles
    bloom_hot_alliance_window_days
    bloom_hot_alliance_min_engagements
    bloom_entry_point_max_tags_per_label
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

logger = logging.getLogger(__name__)

JOB_KEY = "compute_bloom_entry_points"

# ── Default thresholds (overridable via runtime) ─────────────────────────
DEFAULT_HOT_BATTLE_WINDOW_DAYS = 7
DEFAULT_HOT_BATTLE_MIN_PARTICIPANTS = 25

DEFAULT_HIGH_RISK_PILOT_MIN_SCORE = 0.65

DEFAULT_STRATEGIC_SYSTEM_WINDOW_DAYS = 14
DEFAULT_STRATEGIC_SYSTEM_MIN_BATTLES = 10

DEFAULT_HOT_ALLIANCE_WINDOW_DAYS = 7
DEFAULT_HOT_ALLIANCE_MIN_ENGAGEMENTS = 15

# Hard cap per label to protect Bloom from run-away result sets.
DEFAULT_MAX_TAGS_PER_LABEL = 500

# How many rows per tier to surface in the PHP dashboard.
# (The Neo4j labels themselves still honour max_tags_per_label — this
# only bounds the dashboard read-side projection in MariaDB.)
DASHBOARD_TOP_N = 10

# Per-query timeout — these are all small aggregations so 60s is ample.
_QUERY_TIMEOUT_SECONDS = 60


def _runtime_int(runtime: dict[str, Any], key: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(runtime.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _runtime_float(runtime: dict[str, Any], key: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(runtime.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _refresh_hot_battles(
    client: Neo4jClient,
    *,
    window_days: int,
    min_participants: int,
    max_tags: int,
) -> dict[str, int]:
    """Tag recent, high-intensity battles as :HotBattle."""
    # Untag battles that no longer qualify: either too old or below
    # participant threshold.  Bounded by LIMIT so the transaction stays
    # small even on a big prune.
    untagged = client.query(
        """
        MATCH (b:HotBattle)
        WHERE b.started_at IS NULL
           OR b.started_at < toString(datetime() - duration({days: $window}))
           OR COALESCE(b.participant_count, 0) < $min_participants
        WITH b LIMIT $cap
        REMOVE b:HotBattle
        REMOVE b.bloom_hot_score
        REMOVE b.bloom_tagged_at
        RETURN count(b) AS removed
        """,
        parameters={"window": window_days, "min_participants": min_participants, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    removed = int((untagged[0] if untagged else {}).get("removed") or 0)

    # Tag new qualifiers.  bloom_hot_score is participant_count scaled
    # by recency — newer battles score higher at equal size.
    tagged = client.query(
        """
        MATCH (b:Battle)
        WHERE b.started_at IS NOT NULL
          AND b.started_at >= toString(datetime() - duration({days: $window}))
          AND COALESCE(b.participant_count, 0) >= $min_participants
        WITH b, COALESCE(b.participant_count, 0) AS pcount
        ORDER BY pcount DESC
        LIMIT $cap
        SET b:HotBattle,
            b.bloom_hot_score = pcount,
            b.bloom_tagged_at = toString(datetime())
        RETURN count(b) AS added
        """,
        parameters={"window": window_days, "min_participants": min_participants, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    added = int((tagged[0] if tagged else {}).get("added") or 0)

    return {"added": added, "removed": removed}


def _refresh_high_risk_pilots(
    client: Neo4jClient,
    *,
    min_score: float,
    max_tags: int,
) -> dict[str, int]:
    """Tag pilots whose suspicion score exceeds the threshold."""
    untagged = client.query(
        """
        MATCH (p:HighRiskPilot)
        WHERE COALESCE(p.suspicion_score_recent, p.suspicion_score, 0) < $min_score
        WITH p LIMIT $cap
        REMOVE p:HighRiskPilot
        REMOVE p.bloom_tagged_at
        RETURN count(p) AS removed
        """,
        parameters={"min_score": min_score, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    removed = int((untagged[0] if untagged else {}).get("removed") or 0)

    tagged = client.query(
        """
        MATCH (p:Character)
        WHERE COALESCE(p.suspicion_score_recent, p.suspicion_score, 0) >= $min_score
        WITH p, COALESCE(p.suspicion_score_recent, p.suspicion_score, 0) AS score
        ORDER BY score DESC
        LIMIT $cap
        SET p:HighRiskPilot,
            p.bloom_tagged_at = toString(datetime())
        RETURN count(p) AS added
        """,
        parameters={"min_score": min_score, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    added = int((tagged[0] if tagged else {}).get("added") or 0)

    return {"added": added, "removed": removed}


def _refresh_strategic_systems(
    client: Neo4jClient,
    *,
    window_days: int,
    min_battles: int,
    max_tags: int,
) -> dict[str, int]:
    """Tag systems that hosted ``min_battles`` or more battles in the window."""
    # Untag systems whose count drops below threshold.
    untagged = client.query(
        """
        MATCH (s:StrategicSystem)
        WITH s, COUNT {
            (s)<-[:IN_SYSTEM|LOCATED_IN]-(b:Battle)
            WHERE b.started_at >= toString(datetime() - duration({days: $window}))
        } AS recent_count
        WHERE recent_count < $min_battles
        WITH s LIMIT $cap
        REMOVE s:StrategicSystem
        REMOVE s.bloom_recent_battle_count
        REMOVE s.bloom_tagged_at
        RETURN count(s) AS removed
        """,
        parameters={"window": window_days, "min_battles": min_battles, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    removed = int((untagged[0] if untagged else {}).get("removed") or 0)

    # Tag systems meeting the threshold.  We drive the match off Battle
    # first to avoid scanning every System in the universe.
    tagged = client.query(
        """
        MATCH (b:Battle)
        WHERE b.started_at IS NOT NULL
          AND b.started_at >= toString(datetime() - duration({days: $window}))
        MATCH (b)-[:IN_SYSTEM|LOCATED_IN]->(s:System)
        WITH s, count(DISTINCT b) AS recent_count
        WHERE recent_count >= $min_battles
        ORDER BY recent_count DESC
        LIMIT $cap
        SET s:StrategicSystem,
            s.bloom_recent_battle_count = recent_count,
            s.bloom_tagged_at = toString(datetime())
        RETURN count(s) AS added
        """,
        parameters={"window": window_days, "min_battles": min_battles, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    added = int((tagged[0] if tagged else {}).get("added") or 0)

    return {"added": added, "removed": removed}


def _refresh_hot_alliances(
    client: Neo4jClient,
    *,
    window_days: int,
    min_engagements: int,
    max_tags: int,
) -> dict[str, int]:
    """Tag alliances with recent engagement volume."""
    # COUNT { } subqueries on this Neo4j version only accept a single
    # pattern + WHERE, not nested MATCH clauses.  Use an OPTIONAL MATCH
    # aggregation instead — this also matches the tag path semantics,
    # which counts DISTINCT recent battles rather than path rows.
    untagged = client.query(
        """
        MATCH (a:HotAlliance)
        OPTIONAL MATCH (a)<-[:MEMBER_OF_ALLIANCE]-(:Character)-[:PARTICIPATED_IN]->(b:Battle)
        WHERE b.started_at >= toString(datetime() - duration({days: $window}))
        WITH a, count(DISTINCT b) AS recent_count
        WHERE recent_count < $min_engagements
        WITH a LIMIT $cap
        REMOVE a:HotAlliance
        REMOVE a.bloom_recent_engagement_count
        REMOVE a.bloom_tagged_at
        RETURN count(a) AS removed
        """,
        parameters={"window": window_days, "min_engagements": min_engagements, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    removed = int((untagged[0] if untagged else {}).get("removed") or 0)

    # Drive off recent battles to bound the search.
    tagged = client.query(
        """
        MATCH (b:Battle)
        WHERE b.started_at >= toString(datetime() - duration({days: $window}))
        MATCH (p:Character)-[:PARTICIPATED_IN]->(b)
        MATCH (p)-[:MEMBER_OF_ALLIANCE]->(a:Alliance)
        WITH a, count(DISTINCT b) AS recent_count
        WHERE recent_count >= $min_engagements
        ORDER BY recent_count DESC
        LIMIT $cap
        SET a:HotAlliance,
            a.bloom_recent_engagement_count = recent_count,
            a.bloom_tagged_at = toString(datetime())
        RETURN count(a) AS added
        """,
        parameters={"window": window_days, "min_engagements": min_engagements, "cap": max_tags},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    added = int((tagged[0] if tagged else {}).get("added") or 0)

    return {"added": added, "removed": removed}


# ── Dashboard read-side projection ───────────────────────────────────────
#
# Neo4j Community Edition has no Bloom UI, so operators need the four
# tiers surfaced in the PHP dashboard instead.  After the labels have
# been refreshed in Neo4j, we re-query the top DASHBOARD_TOP_N of each
# tier and write them to the `bloom_entry_points_materialized` MariaDB
# table so `public/index.php` can render them with a single SQL read.


def _query_top_hot_battles(client: Neo4jClient, *, top_n: int) -> list[dict[str, Any]]:
    rows = client.query(
        """
        MATCH (b:HotBattle)
        OPTIONAL MATCH (b)-[:IN_SYSTEM|LOCATED_IN]->(s:System)
        WITH b, s
        ORDER BY COALESCE(b.bloom_hot_score, b.participant_count, 0) DESC
        LIMIT $top_n
        RETURN b.battle_id          AS battle_id,
               toString(b.started_at) AS started_at,
               b.participant_count  AS participant_count,
               b.bloom_hot_score    AS bloom_hot_score,
               s.system_id          AS system_id,
               s.name               AS system_name
        """,
        parameters={"top_n": top_n},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    output: list[dict[str, Any]] = []
    for row in rows:
        battle_id = row.get("battle_id")
        if battle_id is None:
            continue
        system_name = row.get("system_name") or ""
        started_at = row.get("started_at") or ""
        if system_name and started_at:
            display_name = f"{system_name} @ {started_at}"
        elif system_name:
            display_name = system_name
        else:
            display_name = f"Battle {battle_id}"
        output.append({
            "entity_ref_type": "battle_id",
            "entity_ref_id": int(battle_id),
            "entity_name": display_name,
            "score": float(row.get("bloom_hot_score") or row.get("participant_count") or 0),
            "detail": {
                "started_at": started_at,
                "participant_count": row.get("participant_count"),
                "system_id": row.get("system_id"),
                "system_name": system_name or None,
            },
        })
    return output


def _query_top_high_risk_pilots(client: Neo4jClient, *, top_n: int) -> list[dict[str, Any]]:
    rows = client.query(
        """
        MATCH (p:HighRiskPilot)
        OPTIONAL MATCH (p)-[:MEMBER_OF_ALLIANCE]->(a:Alliance)
        WITH p, a,
             COALESCE(p.suspicion_score_recent, p.suspicion_score, 0) AS score
        ORDER BY score DESC
        LIMIT $top_n
        RETURN p.character_id           AS character_id,
               p.name                   AS character_name,
               score                    AS score,
               p.suspicion_score        AS suspicion_score,
               p.suspicion_score_recent AS suspicion_score_recent,
               a.alliance_id            AS alliance_id,
               a.name                   AS alliance_name
        """,
        parameters={"top_n": top_n},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    output: list[dict[str, Any]] = []
    for row in rows:
        character_id = row.get("character_id")
        if character_id is None:
            continue
        output.append({
            "entity_ref_type": "character_id",
            "entity_ref_id": int(character_id),
            "entity_name": row.get("character_name") or f"Pilot {character_id}",
            "score": float(row.get("score") or 0),
            "detail": {
                "suspicion_score": row.get("suspicion_score"),
                "suspicion_score_recent": row.get("suspicion_score_recent"),
                "alliance_id": row.get("alliance_id"),
                "alliance_name": row.get("alliance_name"),
            },
        })
    return output


def _query_top_strategic_systems(client: Neo4jClient, *, top_n: int) -> list[dict[str, Any]]:
    rows = client.query(
        """
        MATCH (s:StrategicSystem)
        OPTIONAL MATCH (s)-[:IN_CONSTELLATION]->(:Constellation)-[:IN_REGION]->(r:Region)
        WITH s, r
        ORDER BY COALESCE(s.bloom_recent_battle_count, 0) DESC
        LIMIT $top_n
        RETURN s.system_id                  AS system_id,
               s.name                       AS system_name,
               s.security                   AS security,
               s.bloom_recent_battle_count  AS recent_battle_count,
               r.region_id                  AS region_id,
               r.name                       AS region_name
        """,
        parameters={"top_n": top_n},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    output: list[dict[str, Any]] = []
    for row in rows:
        system_id = row.get("system_id")
        if system_id is None:
            continue
        output.append({
            "entity_ref_type": "system_id",
            "entity_ref_id": int(system_id),
            "entity_name": row.get("system_name") or f"System {system_id}",
            "score": float(row.get("recent_battle_count") or 0),
            "detail": {
                "security": row.get("security"),
                "recent_battle_count": row.get("recent_battle_count"),
                "region_id": row.get("region_id"),
                "region_name": row.get("region_name"),
            },
        })
    return output


def _query_top_hot_alliances(client: Neo4jClient, *, top_n: int) -> list[dict[str, Any]]:
    rows = client.query(
        """
        MATCH (a:HotAlliance)
        WITH a
        ORDER BY COALESCE(a.bloom_recent_engagement_count, 0) DESC
        LIMIT $top_n
        RETURN a.alliance_id                   AS alliance_id,
               a.name                          AS alliance_name,
               a.bloom_recent_engagement_count AS recent_engagement_count
        """,
        parameters={"top_n": top_n},
        timeout_seconds=_QUERY_TIMEOUT_SECONDS,
    )
    output: list[dict[str, Any]] = []
    for row in rows:
        alliance_id = row.get("alliance_id")
        if alliance_id is None:
            continue
        output.append({
            "entity_ref_type": "alliance_id",
            "entity_ref_id": int(alliance_id),
            "entity_name": row.get("alliance_name") or f"Alliance {alliance_id}",
            "score": float(row.get("recent_engagement_count") or 0),
            "detail": {
                "recent_engagement_count": row.get("recent_engagement_count"),
            },
        })
    return output


def _materialize_tier(
    db: SupplyCoreDb,
    *,
    tier: str,
    rows: list[dict[str, Any]],
    refreshed_at: str,
) -> int:
    """Atomically replace a tier's rows in `bloom_entry_points_materialized`."""
    with db.transaction() as (_, cursor):
        cursor.execute(
            "DELETE FROM bloom_entry_points_materialized WHERE tier = %s",
            (tier,),
        )
        for rank, row in enumerate(rows, start=1):
            cursor.execute(
                """
                INSERT INTO bloom_entry_points_materialized (
                    tier, rank_in_tier, entity_ref_type, entity_ref_id,
                    entity_name, score, detail_json, refreshed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    tier,
                    rank,
                    row["entity_ref_type"],
                    row["entity_ref_id"],
                    row.get("entity_name"),
                    row.get("score"),
                    json_dumps_safe(row.get("detail") or {}),
                    refreshed_at,
                ),
            )
    return len(rows)


def run_compute_bloom_entry_points(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Refresh Bloom smart entry-point labels on the Neo4j graph and
    materialize the top-N of each tier into MariaDB for the dashboard.

    Parameters
    ----------
    db
        SupplyCore DB handle, used for writing the read-side projection
        to `bloom_entry_points_materialized`.
    neo4j_raw
        Raw Neo4j runtime config section.
    """
    started = time.perf_counter()
    runtime = neo4j_raw or {}
    config = Neo4jConfig.from_runtime(runtime)

    if not config.enabled:
        return JobResult.skipped(job_key=JOB_KEY, reason="neo4j disabled").to_dict()

    hot_battle_window = _runtime_int(
        runtime, "bloom_hot_battle_window_days", DEFAULT_HOT_BATTLE_WINDOW_DAYS
    )
    hot_battle_min_participants = _runtime_int(
        runtime, "bloom_hot_battle_min_participants", DEFAULT_HOT_BATTLE_MIN_PARTICIPANTS
    )
    high_risk_min_score = _runtime_float(
        runtime, "bloom_high_risk_pilot_min_score", DEFAULT_HIGH_RISK_PILOT_MIN_SCORE
    )
    strategic_window = _runtime_int(
        runtime, "bloom_strategic_system_window_days", DEFAULT_STRATEGIC_SYSTEM_WINDOW_DAYS
    )
    strategic_min_battles = _runtime_int(
        runtime, "bloom_strategic_system_min_battles", DEFAULT_STRATEGIC_SYSTEM_MIN_BATTLES
    )
    hot_alliance_window = _runtime_int(
        runtime, "bloom_hot_alliance_window_days", DEFAULT_HOT_ALLIANCE_WINDOW_DAYS
    )
    hot_alliance_min_engagements = _runtime_int(
        runtime, "bloom_hot_alliance_min_engagements", DEFAULT_HOT_ALLIANCE_MIN_ENGAGEMENTS
    )
    max_tags_per_label = _runtime_int(
        runtime, "bloom_entry_point_max_tags_per_label", DEFAULT_MAX_TAGS_PER_LABEL, minimum=10
    )

    client = Neo4jClient(config)

    try:
        hot_battles = _refresh_hot_battles(
            client,
            window_days=hot_battle_window,
            min_participants=hot_battle_min_participants,
            max_tags=max_tags_per_label,
        )
        high_risk_pilots = _refresh_high_risk_pilots(
            client,
            min_score=high_risk_min_score,
            max_tags=max_tags_per_label,
        )
        strategic_systems = _refresh_strategic_systems(
            client,
            window_days=strategic_window,
            min_battles=strategic_min_battles,
            max_tags=max_tags_per_label,
        )
        hot_alliances = _refresh_hot_alliances(
            client,
            window_days=hot_alliance_window,
            min_engagements=hot_alliance_min_engagements,
            max_tags=max_tags_per_label,
        )

        # Dashboard read-side projection — pull top-N of each tier and
        # write them to MariaDB so the PHP dashboard can render the
        # tiers with a single fast SQL read.
        top_hot_battles = _query_top_hot_battles(client, top_n=DASHBOARD_TOP_N)
        top_high_risk_pilots = _query_top_high_risk_pilots(client, top_n=DASHBOARD_TOP_N)
        top_strategic_systems = _query_top_strategic_systems(client, top_n=DASHBOARD_TOP_N)
        top_hot_alliances = _query_top_hot_alliances(client, top_n=DASHBOARD_TOP_N)
    except Neo4jError as error:
        duration_ms = int((time.perf_counter() - started) * 1000)
        logger.warning("compute_bloom_entry_points failed: %s", error)
        return JobResult.failed(
            job_key=JOB_KEY,
            error=error,
            duration_ms=duration_ms,
        ).to_dict()

    refreshed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    materialized_counts = {
        "HotBattle": _materialize_tier(db, tier="HotBattle", rows=top_hot_battles, refreshed_at=refreshed_at),
        "HighRiskPilot": _materialize_tier(db, tier="HighRiskPilot", rows=top_high_risk_pilots, refreshed_at=refreshed_at),
        "StrategicSystem": _materialize_tier(db, tier="StrategicSystem", rows=top_strategic_systems, refreshed_at=refreshed_at),
        "HotAlliance": _materialize_tier(db, tier="HotAlliance", rows=top_hot_alliances, refreshed_at=refreshed_at),
    }
    materialized_total = sum(materialized_counts.values())

    duration_ms = int((time.perf_counter() - started) * 1000)

    added_total = (
        hot_battles["added"]
        + high_risk_pilots["added"]
        + strategic_systems["added"]
        + hot_alliances["added"]
    )
    removed_total = (
        hot_battles["removed"]
        + high_risk_pilots["removed"]
        + strategic_systems["removed"]
        + hot_alliances["removed"]
    )

    summary = (
        f"Bloom entry points refreshed: +{added_total} tagged, -{removed_total} untagged "
        f"(HotBattle +{hot_battles['added']}/-{hot_battles['removed']}, "
        f"HighRiskPilot +{high_risk_pilots['added']}/-{high_risk_pilots['removed']}, "
        f"StrategicSystem +{strategic_systems['added']}/-{strategic_systems['removed']}, "
        f"HotAlliance +{hot_alliances['added']}/-{hot_alliances['removed']}); "
        f"dashboard top-{DASHBOARD_TOP_N} materialized: {materialized_total} rows."
    )

    return JobResult.success(
        job_key=JOB_KEY,
        summary=summary,
        rows_processed=added_total + removed_total,
        rows_written=added_total + materialized_total,
        duration_ms=duration_ms,
        meta={
            "execution_mode": "python",
            "thresholds": {
                "hot_battle_window_days": hot_battle_window,
                "hot_battle_min_participants": hot_battle_min_participants,
                "high_risk_pilot_min_score": high_risk_min_score,
                "strategic_system_window_days": strategic_window,
                "strategic_system_min_battles": strategic_min_battles,
                "hot_alliance_window_days": hot_alliance_window,
                "hot_alliance_min_engagements": hot_alliance_min_engagements,
                "max_tags_per_label": max_tags_per_label,
                "dashboard_top_n": DASHBOARD_TOP_N,
            },
            "hot_battles": hot_battles,
            "high_risk_pilots": high_risk_pilots,
            "strategic_systems": strategic_systems,
            "hot_alliances": hot_alliances,
            "materialized_counts": materialized_counts,
        },
    ).to_dict()
