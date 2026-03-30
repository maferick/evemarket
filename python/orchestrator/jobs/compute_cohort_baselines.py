from __future__ import annotations

import bisect
import math
import statistics
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..neo4j import Neo4jClient, Neo4jConfig

# ---------------------------------------------------------------------------
# Feature keys sourced from character_battle_intelligence + graph + baselines.
# These are the per-character metrics we compute cohort baselines for.
# ---------------------------------------------------------------------------
COHORT_FEATURE_KEYS: list[str] = [
    "high_sustain_frequency",
    "cross_side_rate",
    "enemy_efficiency_uplift",
    "anomaly_delta_score",
    "co_occurrence_density",
    "engagement_avoidance_score",
    "bridge_score",
    "pagerank_score",
    "role_weight",
]

# Minimum members for a cohort to produce meaningful baselines.
MIN_COHORT_SIZE = 5


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    if d <= 0:
        return default
    return n / d


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


def _mad(values: list[float]) -> float:
    """Median Absolute Deviation."""
    if len(values) < 2:
        return 0.0
    med = statistics.median(values)
    return float(statistics.median([abs(v - med) for v in values]))


def _stddev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(max(0.0, variance))


# ---------------------------------------------------------------------------
# Cohort assignment rules
# ---------------------------------------------------------------------------

def _assign_cohorts(
    row: dict[str, Any],
    first_seen_map: dict[int, datetime | None],
    own_alliance_id: int | None,
    now: datetime,
) -> list[str]:
    """Return a list of cohort_key strings this character belongs to.

    Rules:
      - combat_active:   eligible_battle_count >= 5
      - low_activity:    eligible_battle_count < 5 and total_battle_count > 0
      - logistics:       role_weight > 0 and is_logi dominant
      - capital:         is_capital dominant
      - newly_observed:  first battle within last 30 days
      - alliance_local:  character's current alliance matches the configured own alliance
    """
    cid = int(row.get("character_id") or 0)
    eligible = int(row.get("eligible_battle_count") or 0)
    total = int(row.get("total_battle_count") or 0)
    role_weight = float(row.get("role_weight") or 0.0)
    is_logi_rate = float(row.get("is_logi_rate") or 0.0)
    is_capital_rate = float(row.get("is_capital_rate") or 0.0)
    alliance_id = int(row.get("alliance_id") or 0)

    cohorts: list[str] = []

    # Combat-active: enough eligible battles to produce stable statistics
    if eligible >= 5:
        cohorts.append("combat_active")

    # Low-activity: has participated but not enough for full scoring
    if eligible < 5 and total > 0:
        cohorts.append("low_activity")

    # Logistics-heavy: predominantly flies logi across observed battles
    if is_logi_rate >= 0.4 and eligible >= 3:
        cohorts.append("logistics")

    # Capital-heavy: predominantly flies capitals
    if is_capital_rate >= 0.3 and eligible >= 3:
        cohorts.append("capital")

    # Newly observed: first seen within last 30 days
    first_seen = first_seen_map.get(cid)
    if first_seen is not None and (now - first_seen).days <= 30:
        cohorts.append("newly_observed")

    # Alliance-local: currently in the configured own alliance
    if own_alliance_id and alliance_id > 0 and alliance_id == own_alliance_id:
        cohorts.append("alliance_local")

    return cohorts


# ---------------------------------------------------------------------------
# Main job entry point
# ---------------------------------------------------------------------------

def run_compute_cohort_baselines(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    neo4j_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    computed_at = _now_sql()
    now = datetime.now(UTC)
    runtime = runtime or {}

    neo4j_cfg = Neo4jConfig.from_runtime(neo4j_config or {})
    neo4j = Neo4jClient(neo4j_cfg) if neo4j_cfg.enabled else None

    # Resolve own alliance_id from app_settings if available.
    own_alliance_row = db.fetch_one(
        "SELECT setting_value FROM app_settings WHERE setting_key = 'own_alliance_id' LIMIT 1"
    )
    own_alliance_id = int(own_alliance_row.get("setting_value") or 0) if own_alliance_row else 0

    # ── Step 1: Load character feature vectors ────────────────────────────
    rows = db.fetch_all(
        """
        SELECT
            cbi.character_id,
            cbi.total_battle_count,
            cbi.eligible_battle_count,
            cbi.high_sustain_frequency,
            cbi.cross_side_rate,
            cbi.enemy_efficiency_uplift,
            cbi.role_weight,
            COALESCE(cbb.anomaly_delta_score, 0) AS anomaly_delta_score,
            COALESCE(cgi.co_occurrence_density, 0) AS co_occurrence_density,
            COALESCE(cgi.engagement_avoidance_score, 0) AS engagement_avoidance_score,
            COALESCE(cgi.bridge_score, 0) AS bridge_score,
            COALESCE(cgi.pagerank_score, 0) AS pagerank_score,
            COALESCE(bp_agg.is_logi_rate, 0) AS is_logi_rate,
            COALESCE(bp_agg.is_capital_rate, 0) AS is_capital_rate,
            COALESCE(bp_agg.alliance_id, 0) AS alliance_id
        FROM character_battle_intelligence cbi
        LEFT JOIN character_behavioral_baselines cbb ON cbb.character_id = cbi.character_id
        LEFT JOIN character_graph_intelligence cgi ON cgi.character_id = cbi.character_id
        LEFT JOIN (
            SELECT
                character_id,
                AVG(is_logi) AS is_logi_rate,
                AVG(is_capital) AS is_capital_rate,
                MAX(alliance_id) AS alliance_id
            FROM battle_participants
            GROUP BY character_id
        ) bp_agg ON bp_agg.character_id = cbi.character_id
        """
    )

    # First-seen date per character (earliest battle_rollup started_at).
    first_seen_rows = db.fetch_all(
        """
        SELECT baf.character_id, MIN(br.started_at) AS first_seen
        FROM battle_actor_features baf
        INNER JOIN battle_rollups br ON br.battle_id = baf.battle_id
        WHERE baf.character_id > 0
        GROUP BY baf.character_id
        """
    )
    first_seen_map: dict[int, datetime | None] = {}
    for fsr in first_seen_rows:
        cid = int(fsr.get("character_id") or 0)
        raw = fsr.get("first_seen")
        if cid > 0 and raw is not None:
            if isinstance(raw, datetime):
                first_seen_map[cid] = raw.replace(tzinfo=UTC) if raw.tzinfo is None else raw
            else:
                try:
                    first_seen_map[cid] = datetime.fromisoformat(str(raw)).replace(tzinfo=UTC)
                except (ValueError, TypeError):
                    pass

    # ── Step 2: Assign cohorts ────────────────────────────────────────────
    # character_id → list of cohort_key
    character_cohorts: dict[int, list[str]] = {}
    # cohort_key → list of feature dicts
    cohort_members: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        cid = int(row.get("character_id") or 0)
        if cid <= 0:
            continue
        cohorts = _assign_cohorts(row, first_seen_map, own_alliance_id, now)
        character_cohorts[cid] = cohorts
        for cohort_key in cohorts:
            cohort_members.setdefault(cohort_key, []).append(row)

    # ── Step 3: Compute cohort feature baselines ──────────────────────────
    baseline_payload: list[tuple[Any, ...]] = []
    # Store per-cohort stats for scoring lookups: (cohort_key, feature_key) → stats
    cohort_stats: dict[tuple[str, str], dict[str, float]] = {}

    for cohort_key, members in cohort_members.items():
        if len(members) < MIN_COHORT_SIZE:
            continue
        for feature_key in COHORT_FEATURE_KEYS:
            values = [float(m.get(feature_key) or 0.0) for m in members]
            if not values:
                continue
            mean = sum(values) / len(values)
            sd = _stddev(values)
            med = _median(values)
            mad_val = _mad(values)

            stats = {
                "mean": mean,
                "stddev": sd,
                "median": med,
                "mad": mad_val,
                "sample_count": len(values),
            }
            cohort_stats[(cohort_key, feature_key)] = stats
            baseline_payload.append((
                cohort_key,
                feature_key,
                "all_time",
                mean,
                sd,
                med,
                mad_val,
                len(values),
                computed_at,
            ))

    # ── Step 4: Write cohort membership ───────────────────────────────────
    membership_payload: list[tuple[Any, ...]] = []
    for cid, cohorts in character_cohorts.items():
        for cohort_key in cohorts:
            membership_payload.append((cid, cohort_key, computed_at, computed_at))

    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM character_cohort_membership")
        if membership_payload:
            cursor.executemany(
                """
                INSERT INTO character_cohort_membership (
                    character_id, cohort_key, valid_from, computed_at
                ) VALUES (%s, %s, %s, %s)
                """,
                membership_payload,
            )

        cursor.execute("DELETE FROM cohort_feature_baselines")
        if baseline_payload:
            cursor.executemany(
                """
                INSERT INTO cohort_feature_baselines (
                    cohort_key, feature_key, window_label,
                    mean, stddev, median, mad, sample_count, computed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                baseline_payload,
            )

    # ── Step 5: Compute per-character cohort-relative scores ──────────────
    # For each character pick the most specific cohort they belong to.
    # Priority: alliance_local > logistics > capital > combat_active > newly_observed > low_activity
    COHORT_PRIORITY = {
        "alliance_local": 6,
        "logistics": 5,
        "capital": 4,
        "combat_active": 3,
        "newly_observed": 2,
        "low_activity": 1,
    }

    # Collect suspicion_score per character for cohort percentile calculation.
    suspicion_rows = db.fetch_all(
        "SELECT character_id, suspicion_score FROM character_suspicion_scores"
    )
    suspicion_by_id: dict[int, float] = {
        int(r["character_id"]): float(r.get("suspicion_score") or 0.0) for r in suspicion_rows
    }

    # Group suspicion scores by cohort for percentile computation.
    cohort_score_lists: dict[str, list[float]] = {}
    for cid, cohorts in character_cohorts.items():
        score = suspicion_by_id.get(cid, 0.0)
        for ck in cohorts:
            cohort_score_lists.setdefault(ck, []).append(score)
    for ck in cohort_score_lists:
        cohort_score_lists[ck].sort()

    update_payload: list[tuple[Any, ...]] = []
    for cid, cohorts in character_cohorts.items():
        if not cohorts:
            continue
        # Pick primary cohort by priority
        primary = max(cohorts, key=lambda c: COHORT_PRIORITY.get(c, 0))
        score = suspicion_by_id.get(cid, 0.0)

        # Cohort z-score: (score - cohort_mean) / cohort_stddev
        stats = cohort_stats.get((primary, "high_sustain_frequency"))
        cohort_mean = stats["mean"] if stats else 0.0
        cohort_sd = stats["stddev"] if stats else 0.0
        # Use the suspicion_score mean/sd from the cohort instead
        cohort_scores = cohort_score_lists.get(primary, [])
        if len(cohort_scores) >= MIN_COHORT_SIZE:
            c_mean = sum(cohort_scores) / len(cohort_scores)
            c_sd = _stddev(cohort_scores)
            c_med = _median(cohort_scores)
            c_mad = _mad(cohort_scores)

            z_score = _safe_div(score - c_mean, c_sd, 0.0) if c_sd > 0 else 0.0
            mad_dev = _safe_div(score - c_med, max(c_mad * 1.4826, 1e-9), 0.0) if c_mad > 0 else 0.0
            percentile = _safe_div(
                float(bisect.bisect_right(cohort_scores, score)),
                float(len(cohort_scores)),
                0.0,
            )
        else:
            z_score = 0.0
            mad_dev = 0.0
            percentile = 0.0

        update_payload.append((z_score, mad_dev, percentile, cid))

    # Ensure columns exist (safe migration for running before migration is applied).
    for col_name, col_def, after_col in (
        ("cohort_z_score", "DECIMAL(12,6) NOT NULL DEFAULT 0.000000", "percentile_rank"),
        ("cohort_mad_deviation", "DECIMAL(12,6) NOT NULL DEFAULT 0.000000", "cohort_z_score"),
        ("cohort_percentile", "DECIMAL(10,6) NOT NULL DEFAULT 0.000000", "cohort_mad_deviation"),
    ):
        exists = db.fetch_one(
            """
            SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'character_suspicion_scores'
              AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (col_name,),
        )
        if not exists:
            db.execute(
                f"ALTER TABLE character_suspicion_scores ADD COLUMN {col_name} {col_def} AFTER {after_col}"
            )

    if update_payload:
        with db.transaction() as (_, cursor):
            cursor.executemany(
                """
                UPDATE character_suspicion_scores
                SET cohort_z_score = %s,
                    cohort_mad_deviation = %s,
                    cohort_percentile = %s
                WHERE character_id = %s
                """,
                update_payload,
            )

    # ── Step 6: Sync cohort data to Neo4j ─────────────────────────────────
    neo4j_rows_synced = 0
    if neo4j:
        NEO4J_BATCH = 500
        NEO4J_TIMEOUT = 60

        # 6a. Create/update Cohort nodes with baseline statistics.
        cohort_node_rows: list[dict[str, Any]] = []
        for cohort_key, members in cohort_members.items():
            cohort_node_rows.append({
                "cohort_key": cohort_key,
                "member_count": len(members),
                "computed_at": computed_at,
            })
        if cohort_node_rows:
            for i in range(0, len(cohort_node_rows), NEO4J_BATCH):
                neo4j.query(
                    """
                    UNWIND $rows AS row
                    MERGE (co:Cohort {cohort_key: row.cohort_key})
                    SET co.member_count = toInteger(row.member_count),
                        co.computed_at = row.computed_at
                    """,
                    {"rows": cohort_node_rows[i:i + NEO4J_BATCH]},
                    timeout_seconds=NEO4J_TIMEOUT,
                )
            neo4j_rows_synced += len(cohort_node_rows)

        # 6b. Create BELONGS_TO_COHORT relationships (character → cohort).
        membership_neo4j_rows: list[dict[str, Any]] = []
        for cid, cohorts in character_cohorts.items():
            for cohort_key in cohorts:
                membership_neo4j_rows.append({
                    "character_id": cid,
                    "cohort_key": cohort_key,
                    "computed_at": computed_at,
                })
        if membership_neo4j_rows:
            # Clear stale relationships first.
            neo4j.query(
                "MATCH (:Character)-[r:BELONGS_TO_COHORT]->(:Cohort) WHERE r.computed_at <> $computed_at DELETE r",
                {"computed_at": computed_at},
                timeout_seconds=NEO4J_TIMEOUT,
            )
            for i in range(0, len(membership_neo4j_rows), NEO4J_BATCH):
                neo4j.query(
                    """
                    UNWIND $rows AS row
                    MERGE (c:Character {character_id: row.character_id})
                    MERGE (co:Cohort {cohort_key: row.cohort_key})
                    MERGE (c)-[r:BELONGS_TO_COHORT]->(co)
                    SET r.computed_at = row.computed_at
                    """,
                    {"rows": membership_neo4j_rows[i:i + NEO4J_BATCH]},
                    timeout_seconds=NEO4J_TIMEOUT,
                )
            neo4j_rows_synced += len(membership_neo4j_rows)

        # 6c. Set cohort-relative scores on Character nodes.
        score_neo4j_rows: list[dict[str, Any]] = []
        for z_score, mad_dev, percentile, cid in update_payload:
            primary = max(
                character_cohorts.get(cid, ["combat_active"]),
                key=lambda c: COHORT_PRIORITY.get(c, 0),
            )
            score_neo4j_rows.append({
                "character_id": cid,
                "cohort_z_score": float(z_score),
                "cohort_mad_deviation": float(mad_dev),
                "cohort_percentile": float(percentile),
                "primary_cohort": primary,
            })
        if score_neo4j_rows:
            for i in range(0, len(score_neo4j_rows), NEO4J_BATCH):
                neo4j.query(
                    """
                    UNWIND $rows AS row
                    MATCH (c:Character {character_id: row.character_id})
                    SET c.cohort_z_score = toFloat(row.cohort_z_score),
                        c.cohort_mad_deviation = toFloat(row.cohort_mad_deviation),
                        c.cohort_percentile = toFloat(row.cohort_percentile),
                        c.primary_cohort = row.primary_cohort
                    """,
                    {"rows": score_neo4j_rows[i:i + NEO4J_BATCH]},
                    timeout_seconds=NEO4J_TIMEOUT,
                )
            neo4j_rows_synced += len(score_neo4j_rows)

        # 6d. Set feature baselines on Cohort nodes, one Cypher per feature key
        #     to avoid APOC dependency for dynamic property names.
        for feature_key in COHORT_FEATURE_KEYS:
            feature_rows: list[dict[str, Any]] = []
            for cohort_key in cohort_members:
                s = cohort_stats.get((cohort_key, feature_key))
                if s is None:
                    continue
                feature_rows.append({
                    "cohort_key": cohort_key,
                    "mean": float(s["mean"]),
                    "stddev": float(s["stddev"]),
                    "median": float(s["median"]),
                    "mad": float(s["mad"]),
                })
            if not feature_rows:
                continue
            # Property names are static per query — safe to interpolate the
            # feature_key since it comes from COHORT_FEATURE_KEYS constant.
            cypher = f"""
                UNWIND $rows AS row
                MATCH (co:Cohort {{cohort_key: row.cohort_key}})
                SET co.{feature_key}_mean = toFloat(row.mean),
                    co.{feature_key}_stddev = toFloat(row.stddev),
                    co.{feature_key}_median = toFloat(row.median),
                    co.{feature_key}_mad = toFloat(row.mad)
            """
            for i in range(0, len(feature_rows), NEO4J_BATCH):
                neo4j.query(
                    cypher,
                    {"rows": feature_rows[i:i + NEO4J_BATCH]},
                    timeout_seconds=NEO4J_TIMEOUT,
                )
            neo4j_rows_synced += len(feature_rows)

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key="compute_cohort_baselines",
        summary=(
            f"Assigned {len(membership_payload)} cohort memberships across "
            f"{len(cohort_members)} cohorts for {len(character_cohorts)} characters. "
            f"Computed {len(baseline_payload)} feature baselines. "
            f"Updated {len(update_payload)} cohort-relative scores."
        ),
        rows_processed=len(rows),
        rows_written=len(membership_payload) + len(baseline_payload) + len(update_payload),
        duration_ms=duration_ms,
        meta={
            "computed_at": computed_at,
            "cohort_sizes": {k: len(v) for k, v in cohort_members.items()},
            "features_computed": len(baseline_payload),
            "neo4j_rows_synced": neo4j_rows_synced,
        },
    ).to_dict()
