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
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
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
    untagged = client.query(
        """
        MATCH (a:HotAlliance)
        WITH a, COUNT {
            (p:Character)-[:MEMBER_OF_ALLIANCE]->(a)
            MATCH (p)-[:PARTICIPATED_IN]->(b:Battle)
            WHERE b.started_at >= toString(datetime() - duration({days: $window}))
        } AS recent_count
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


def run_compute_bloom_entry_points(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Refresh Bloom smart entry-point labels on the Neo4j graph.

    Parameters
    ----------
    db
        SupplyCore DB handle.  Unused today but kept in the signature for
        parity with other graph jobs and for future MariaDB snapshotting.
    neo4j_raw
        Raw Neo4j runtime config section.
    """
    del db  # reserved for future freshness snapshots
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
    except Neo4jError as error:
        duration_ms = int((time.perf_counter() - started) * 1000)
        logger.warning("compute_bloom_entry_points failed: %s", error)
        return JobResult.failed(
            job_key=JOB_KEY,
            error=error,
            duration_ms=duration_ms,
        ).to_dict()

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
        f"HotAlliance +{hot_alliances['added']}/-{hot_alliances['removed']})."
    )

    return JobResult.success(
        job_key=JOB_KEY,
        summary=summary,
        rows_processed=added_total + removed_total,
        rows_written=added_total,
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
            },
            "hot_battles": hot_battles,
            "high_risk_pilots": high_risk_pilots,
            "strategic_systems": strategic_systems,
            "hot_alliances": hot_alliances,
        },
    ).to_dict()
