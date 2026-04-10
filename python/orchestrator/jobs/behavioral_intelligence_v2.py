from __future__ import annotations

import bisect
import math
import statistics
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

MIN_SAMPLE_COUNT = 5

SUSPICION_V2_WEIGHTS: dict[str, float] = {
    "high_sustain_frequency": 0.12,
    "cross_side_rate": 0.09,
    "enemy_efficiency_uplift": 0.12,
    "anomaly_delta_score": 0.10,
    "baseline_adjusted_uplift": 0.10,
    "co_occurrence_density": 0.09,
    "engagement_avoidance": 0.10,
    "anomalous_co_occurrence_density": 0.07,
    "cross_side_cluster_score": 0.06,
    "anomalous_neighbor_density": 0.05,
    "bridge_score": 0.04,
    "pagerank_score": 0.03,
    "role_weight": 0.03,
}


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    if d <= 0:
        return default
    return n / d


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _bounded(value: float, scale: float = 1.0) -> float:
    if scale <= 0:
        scale = 1.0
    return max(0.0, min(1.0, value / scale))


def _bv2_cohort_normalize(evidence_rows: list[dict[str, Any]]) -> None:
    """Enrich evidence rows in-place with cohort statistics."""
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence_rows:
        if row.get("evidence_value") is not None:
            by_key[row["evidence_key"]].append(row)

    for key, rows in by_key.items():
        values = [float(r["evidence_value"]) for r in rows]
        n = len(values)
        if n == 0:
            continue

        mean = statistics.mean(values)
        std = statistics.pstdev(values) if n > 1 else 0.0
        median = statistics.median(values)
        diffs = [abs(v - median) for v in values]
        mad = statistics.median(diffs) if diffs else 0.0
        sorted_vals = sorted(values)

        for row in rows:
            raw = float(row["evidence_value"])
            dev = raw - mean
            row["expected_value"] = round(mean, 6)
            row["deviation_value"] = round(dev, 6)
            row["z_score"] = round(dev / std, 6) if std > 0 else 0.0
            row["mad_score"] = round((raw - median) / (mad * 1.4826), 6) if mad > 0 else 0.0
            row["cohort_percentile"] = round(
                bisect.bisect_right(sorted_vals, raw) / max(1, n), 6
            )
            if n >= 10:
                row["confidence_flag"] = "high"
            elif n >= 5:
                row["confidence_flag"] = "medium"
            else:
                row["confidence_flag"] = "low"


def _ensure_character_suspicion_scores_schema(db: SupplyCoreDb) -> None:
    column_defs: list[tuple[str, str, str]] = [
        ("suspicion_score_recent", "DECIMAL(12,6) NOT NULL DEFAULT 0.000000", "suspicion_score"),
        ("suspicion_score_all_time", "DECIMAL(12,6) NOT NULL DEFAULT 0.000000", "suspicion_score_recent"),
        ("suspicion_momentum", "DECIMAL(12,6) NOT NULL DEFAULT 0.000000", "suspicion_score_all_time"),
        ("percentile_rank", "DECIMAL(10,6) NOT NULL DEFAULT 0.000000", "suspicion_momentum"),
        ("support_evidence_count", "INT UNSIGNED NOT NULL DEFAULT 0", "supporting_battle_count"),
        ("community_id", "INT NOT NULL DEFAULT 0", "support_evidence_count"),
        ("top_supporting_battles_json", "LONGTEXT NOT NULL", "community_id"),
        ("top_graph_neighbors_json", "LONGTEXT NOT NULL", "top_supporting_battles_json"),
        ("explanation_json", "LONGTEXT NOT NULL", "top_graph_neighbors_json"),
    ]
    for column_name, column_def, after_column in column_defs:
        row = db.fetch_one(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'character_suspicion_scores'
              AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (column_name,),
        )
        if row:
            continue
        db.execute(
            f"ALTER TABLE character_suspicion_scores ADD COLUMN {column_name} {column_def} AFTER {after_column}"
        )

    for index_name, index_cols in (
        ("idx_character_suspicion_scores_recent", "suspicion_score_recent, suspicion_momentum"),
        ("idx_character_suspicion_scores_computed", "computed_at"),
    ):
        index_row = db.fetch_one(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'character_suspicion_scores'
              AND INDEX_NAME = %s
            LIMIT 1
            """,
            (index_name,),
        )
        if index_row:
            continue
        db.execute(
            f"ALTER TABLE character_suspicion_scores ADD KEY {index_name} ({index_cols})"
        )


def run_compute_behavioral_baselines(db: SupplyCoreDb, runtime: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    computed_at = _now_sql()
    rows = db.fetch_all(
        """
        SELECT
            baf.character_id,
            baf.battle_id,
            baf.side_key,
            baf.is_logi,
            baf.is_command,
            baf.is_capital,
            baf.participated_in_high_sustain,
            baf.participated_in_low_sustain,
            COALESCE(bsm.z_efficiency_score, 0) AS z_efficiency_score,
            COALESCE(ba.anomaly_class, 'normal') AS anomaly_class,
            br.eligible_for_suspicion,
            br.started_at
        FROM battle_actor_features baf
        INNER JOIN battle_rollups br ON br.battle_id = baf.battle_id
        LEFT JOIN battle_side_metrics bsm ON bsm.battle_id = baf.battle_id AND bsm.side_key = baf.side_key
        LEFT JOIN battle_anomalies ba ON ba.battle_id = baf.battle_id AND ba.side_key = baf.side_key
        WHERE baf.character_id > 0
        """
    )

    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        cid = int(row.get("character_id") or 0)
        if cid <= 0:
            continue
        grouped.setdefault(cid, []).append(row)

    payload: list[tuple[Any, ...]] = []
    for cid, items in grouped.items():
        normal_rows = [r for r in items if str(r.get("anomaly_class") or "normal") == "normal"]
        eligible_rows = [r for r in items if int(r.get("eligible_for_suspicion") or 0) == 1]
        high = sum(1 for r in eligible_rows if int(r.get("participated_in_high_sustain") or 0) == 1)
        low = sum(1 for r in eligible_rows if int(r.get("participated_in_low_sustain") or 0) == 1)

        normal_battle_frequency = _safe_div(float(len(normal_rows)), float(max(len(items), 1)), 0.0)
        normal_co_occurrence_density = _safe_div(float(len({str(r.get('battle_id')) for r in normal_rows})), float(max(len(items), 1)), 0.0)
        low_sustain_participation_frequency = _safe_div(float(low), float(max(len(eligible_rows), 1)), 0.0)
        expected_enemy_efficiency = _safe_div(sum(float(r.get("z_efficiency_score") or 0.0) for r in normal_rows), float(max(len(normal_rows), 1)), 0.0)
        role_adjusted_baseline = min(
            1.0,
            _safe_div(
                float(sum(int(r.get("is_logi") or 0) * 2 + int(r.get("is_command") or 0) * 2 + int(r.get("is_capital") or 0) * 3 for r in normal_rows)),
                float(max(len(normal_rows) * 3, 1)),
                0.0,
            ),
        )

        anomaly_delta_score = _safe_div(float(high - low), float(max(len(eligible_rows), 1)), 0.0)
        payload.append(
            (
                cid,
                normal_battle_frequency,
                normal_co_occurrence_density,
                low_sustain_participation_frequency,
                expected_enemy_efficiency,
                role_adjusted_baseline,
                anomaly_delta_score,
                computed_at,
            )
        )

    # Upsert current batch and delete stale rows not in this computation.
    current_ids: set[int] = set()
    BATCH_SIZE = 500
    with db.transaction() as (_, cursor):
        for i in range(0, len(payload), BATCH_SIZE):
            batch = payload[i : i + BATCH_SIZE]
            cursor.executemany(
                """
                INSERT INTO character_behavioral_baselines (
                    character_id,
                    normal_battle_frequency,
                    normal_co_occurrence_density,
                    low_sustain_participation_frequency,
                    expected_enemy_efficiency,
                    role_adjusted_baseline,
                    anomaly_delta_score,
                    computed_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    normal_battle_frequency = VALUES(normal_battle_frequency),
                    normal_co_occurrence_density = VALUES(normal_co_occurrence_density),
                    low_sustain_participation_frequency = VALUES(low_sustain_participation_frequency),
                    expected_enemy_efficiency = VALUES(expected_enemy_efficiency),
                    role_adjusted_baseline = VALUES(role_adjusted_baseline),
                    anomaly_delta_score = VALUES(anomaly_delta_score),
                    computed_at = VALUES(computed_at)
                """,
                batch,
            )
            for row in batch:
                current_ids.add(int(row[0]))

        # Remove rows for characters no longer in the computed set.
        if current_ids:
            placeholders = ",".join(["%s"] * len(current_ids))
            cursor.execute(
                f"DELETE FROM character_behavioral_baselines WHERE character_id NOT IN ({placeholders})",
                tuple(current_ids),
            )
        elif not payload:
            cursor.execute("DELETE FROM character_behavioral_baselines")

    return JobResult.success(
        job_key="compute_behavioral_baselines",
        summary=f"Computed behavioral baselines for {len(payload)} characters from {len(rows)} actor features.",
        rows_processed=len(rows),
        rows_written=len(payload),
        duration_ms=int((time.perf_counter() - started) * 1000),
        meta={"computed_at": computed_at},
    ).to_dict()


def _fetch_neighbors_from_neo4j(
    neo4j: Neo4jClient, character_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    """Use the pre-computed CO_OCCURS_WITH graph edges for top neighbors."""
    CYPHER_CHUNK = 5000
    result: dict[int, list[dict[str, Any]]] = {}
    for offset in range(0, len(character_ids), CYPHER_CHUNK):
        chunk = character_ids[offset : offset + CYPHER_CHUNK]
        rows = neo4j.query(
            """
            UNWIND $ids AS cid
            MATCH (c:Character {character_id: cid})-[r:CO_OCCURS_WITH]-(other:Character)
            WITH c.character_id AS character_id,
                 other.character_id AS other_character_id,
                 r.occurrence_count AS shared_battle_count,
                 COALESCE(r.high_sustain_battle_count, 0) AS high_sustain_count
            ORDER BY character_id, shared_battle_count DESC
            WITH character_id, collect({
                other_character_id: other_character_id,
                shared_battle_count: shared_battle_count,
                high_sustain_count: high_sustain_count
            })[..5] AS top5
            UNWIND top5 AS t
            RETURN character_id,
                   t.other_character_id AS other_character_id,
                   t.shared_battle_count AS shared_battle_count,
                   t.high_sustain_count AS high_sustain_count
            """,
            {"ids": chunk},
        )
        for row in rows:
            cid = int(row.get("character_id") or 0)
            result.setdefault(cid, []).append(
                {
                    "character_id": int(row.get("other_character_id") or 0),
                    "weight": float(row.get("shared_battle_count") or 0.0),
                    "high_sustain_battle_count": int(row.get("high_sustain_count") or 0),
                }
            )
    return result


def run_compute_suspicion_scores_v2(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    neo4j_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    computed_at = _now_sql()
    _ensure_character_suspicion_scores_schema(db)

    rows = db.fetch_all(
        """
        SELECT
            cbi.character_id,
            cbi.eligible_battle_count,
            cbi.high_sustain_frequency,
            cbi.low_sustain_frequency,
            cbi.cross_side_rate,
            cbi.enemy_efficiency_uplift,
            cbi.role_weight,
            COALESCE(cgi.co_occurrence_density, 0) AS co_occurrence_density,
            COALESCE(cgi.anomalous_co_occurrence_density, 0) AS anomalous_co_occurrence_density,
            COALESCE(cgi.cross_side_cluster_score, 0) AS cross_side_cluster_score,
            COALESCE(cgi.anomalous_neighbor_density, 0) AS anomalous_neighbor_density,
            COALESCE(cgi.bridge_score, 0) AS bridge_score,
            COALESCE(cgi.pagerank_score, 0) AS pagerank_score,
            COALESCE(cgi.engagement_avoidance_score, 0) AS engagement_avoidance_score,
            COALESCE(cgi.community_id, 0) AS community_id,
            COALESCE(cbb.normal_battle_frequency, 0) AS normal_battle_frequency,
            COALESCE(cbb.normal_co_occurrence_density, 0) AS normal_co_occurrence_density,
            COALESCE(cbb.low_sustain_participation_frequency, 0) AS low_sustain_participation_frequency,
            COALESCE(cbb.expected_enemy_efficiency, 0) AS expected_enemy_efficiency,
            COALESCE(cbb.role_adjusted_baseline, 0) AS role_adjusted_baseline,
            COALESCE(cbb.anomaly_delta_score, 0) AS anomaly_delta_score
        FROM character_battle_intelligence cbi
        LEFT JOIN character_graph_intelligence cgi ON cgi.character_id = cbi.character_id
        LEFT JOIN character_behavioral_baselines cbb ON cbb.character_id = cbi.character_id
        """
    )

    # recency signal from last 30d battles
    recent = db.fetch_all(
        """
        SELECT
            baf.character_id,
            AVG(CASE WHEN ba.anomaly_class = 'high_sustain' THEN 1 ELSE 0 END) AS recent_high_sustain,
            AVG(CASE WHEN ba.anomaly_class = 'normal' THEN 1 ELSE 0 END) AS recent_normal,
            COUNT(DISTINCT baf.battle_id) AS recent_battle_count
        FROM battle_actor_features baf
        INNER JOIN battle_rollups br ON br.battle_id = baf.battle_id
        LEFT JOIN battle_anomalies ba ON ba.battle_id = baf.battle_id AND ba.side_key = baf.side_key
        WHERE br.started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
        GROUP BY baf.character_id
        """
    )
    recent_map = {int(row["character_id"]): row for row in recent}

    # ── Top graph neighbors + top supporting battles: parallel chunked ──
    # Each chunk gets its own DB connection (db.fetch_all opens/closes one),
    # so we can safely run chunks across threads.
    all_character_ids = [int(r["character_id"]) for r in rows]
    NEIGHBOR_CHUNK = 2000
    MAX_WORKERS = 4

    # Try Neo4j for neighbor data — the CO_OCCURS_WITH edges are pre-computed
    # and much faster to traverse than a self-join in MySQL.
    neo4j_cfg = Neo4jConfig.from_runtime(neo4j_config or {})
    use_neo4j_neighbors = neo4j_cfg.enabled

    def _fetch_neighbor_chunk(chunk_ids: list[int]) -> list[dict[str, Any]]:
        placeholders = ",".join(["%s"] * len(chunk_ids))
        return db.fetch_all(
            f"""
            SELECT
                x.character_id,
                x.other_character_id,
                x.shared_battle_count,
                x.high_sustain_count
            FROM (
                SELECT
                    baf1.character_id,
                    baf2.character_id AS other_character_id,
                    COUNT(DISTINCT baf1.battle_id) AS shared_battle_count,
                    SUM(CASE WHEN baf1.participated_in_high_sustain = 1 OR baf2.participated_in_high_sustain = 1 THEN 1 ELSE 0 END) AS high_sustain_count,
                    ROW_NUMBER() OVER (PARTITION BY baf1.character_id ORDER BY COUNT(DISTINCT baf1.battle_id) DESC) AS rn
                FROM battle_actor_features baf1
                INNER JOIN battle_actor_features baf2 ON baf2.battle_id = baf1.battle_id AND baf2.character_id <> baf1.character_id
                WHERE baf1.character_id IN ({placeholders})
                GROUP BY baf1.character_id, baf2.character_id
            ) x
            WHERE x.rn <= 5
            """,
            tuple(chunk_ids),
        )

    def _fetch_battles_chunk(chunk_ids: list[int]) -> list[dict[str, Any]]:
        placeholders = ",".join(["%s"] * len(chunk_ids))
        return db.fetch_all(
            f"""
            SELECT tb.character_id, tb.battle_id, tb.side_key, tb.z_efficiency_score
            FROM (
                SELECT
                    baf.character_id,
                    baf.battle_id,
                    baf.side_key,
                    COALESCE(bsm.z_efficiency_score, 0) AS z_efficiency_score,
                    ROW_NUMBER() OVER (PARTITION BY baf.character_id ORDER BY ABS(COALESCE(bsm.z_efficiency_score, 0)) DESC) AS rn
                FROM battle_actor_features baf
                LEFT JOIN battle_side_metrics bsm ON bsm.battle_id = baf.battle_id AND bsm.side_key = baf.side_key
                WHERE baf.character_id IN ({placeholders})
            ) tb
            WHERE tb.rn <= 5
            """,
            tuple(chunk_ids),
        )

    chunks = [
        all_character_ids[i : i + NEIGHBOR_CHUNK]
        for i in range(0, len(all_character_ids), NEIGHBOR_CHUNK)
    ]

    top_battles_rows: list[dict[str, Any]] = []

    if use_neo4j_neighbors:
        # Fast path: read pre-computed CO_OCCURS_WITH from the graph.
        neo4j = Neo4jClient(neo4j_cfg)
        top_neighbors = _fetch_neighbors_from_neo4j(neo4j, all_character_ids)
        # Only need top-battles from MySQL — run those in parallel.
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            for result_rows in pool.map(_fetch_battles_chunk, chunks):
                top_battles_rows.extend(result_rows)
    else:
        # Fallback: parallel MySQL self-join for neighbors + top battles.
        support_rows: list[dict[str, Any]] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            neighbor_futures = {pool.submit(_fetch_neighbor_chunk, c): "neighbor" for c in chunks}
            battle_futures = {pool.submit(_fetch_battles_chunk, c): "battle" for c in chunks}
            all_futures = {**neighbor_futures, **battle_futures}
            for future in as_completed(all_futures):
                result_rows = future.result()
                if all_futures[future] == "neighbor":
                    support_rows.extend(result_rows)
                else:
                    top_battles_rows.extend(result_rows)

        top_neighbors: dict[int, list[dict[str, Any]]] = {}
        for row in support_rows:
            cid = int(row.get("character_id") or 0)
            top_neighbors.setdefault(cid, []).append(
                {
                    "character_id": int(row.get("other_character_id") or 0),
                    "weight": float(row.get("shared_battle_count") or 0.0),
                    "high_sustain_battle_count": int(row.get("high_sustain_count") or 0),
                }
            )

    top_battles_map: dict[int, list[dict[str, Any]]] = {}
    for tb_row in top_battles_rows:
        cid_tb = int(tb_row.get("character_id") or 0)
        top_battles_map.setdefault(cid_tb, []).append(
            {
                "battle_id": tb_row.get("battle_id"),
                "side_key": tb_row.get("side_key"),
                "z_efficiency_score": float(tb_row.get("z_efficiency_score") or 0.0),
            }
        )

    score_payload: list[tuple[Any, ...]] = []
    bv2_evidence_rows: list[dict[str, Any]] = []
    for row in rows:
        cid = int(row.get("character_id") or 0)
        eligible = int(row.get("eligible_battle_count") or 0)
        if cid <= 0:
            continue

        high = float(row.get("high_sustain_frequency") or 0.0)
        low = float(row.get("low_sustain_frequency") or 0.0)
        cross = float(row.get("cross_side_rate") or 0.0)
        uplift = float(row.get("enemy_efficiency_uplift") or 0.0)
        role = float(row.get("role_weight") or 0.0)

        anomaly_delta = float(row.get("anomaly_delta_score") or 0.0)
        baseline_adjusted_uplift = uplift - float(row.get("expected_enemy_efficiency") or 0.0)
        normal_behavior_score = max(0.0, min(1.0, 1.0 - abs(float(row.get("normal_battle_frequency") or 0.0) - high)))

        engagement_avoidance = float(row.get("engagement_avoidance_score") or 0.0)

        components = {
            "high_sustain_frequency": SUSPICION_V2_WEIGHTS["high_sustain_frequency"] * high,
            "cross_side_rate": SUSPICION_V2_WEIGHTS["cross_side_rate"] * cross,
            "enemy_efficiency_uplift": SUSPICION_V2_WEIGHTS["enemy_efficiency_uplift"] * _bounded(uplift + 1.0, 2.0),
            "anomaly_delta_score": SUSPICION_V2_WEIGHTS["anomaly_delta_score"] * _bounded(anomaly_delta + 1.0, 2.0),
            "baseline_adjusted_uplift": SUSPICION_V2_WEIGHTS["baseline_adjusted_uplift"] * _bounded(baseline_adjusted_uplift + 1.0, 2.0),
            "co_occurrence_density": SUSPICION_V2_WEIGHTS["co_occurrence_density"] * _bounded(float(row.get("co_occurrence_density") or 0.0), 10.0),
            "engagement_avoidance": SUSPICION_V2_WEIGHTS["engagement_avoidance"] * _bounded(engagement_avoidance, 1.0),
            "anomalous_co_occurrence_density": SUSPICION_V2_WEIGHTS["anomalous_co_occurrence_density"] * _bounded(float(row.get("anomalous_co_occurrence_density") or 0.0), 10.0),
            "cross_side_cluster_score": SUSPICION_V2_WEIGHTS["cross_side_cluster_score"] * _bounded(float(row.get("cross_side_cluster_score") or 0.0), 5.0),
            "anomalous_neighbor_density": SUSPICION_V2_WEIGHTS["anomalous_neighbor_density"] * _bounded(float(row.get("anomalous_neighbor_density") or 0.0), 3.0),
            "bridge_score": SUSPICION_V2_WEIGHTS["bridge_score"] * _bounded(float(row.get("bridge_score") or 0.0), 10.0),
            "pagerank_score": SUSPICION_V2_WEIGHTS["pagerank_score"] * _bounded(float(row.get("pagerank_score") or 0.0), 50.0),
            "role_weight": SUSPICION_V2_WEIGHTS["role_weight"] * role,
        }

        all_time_score = max(0.0, min(1.0, sum(components.values()))) if eligible >= MIN_SAMPLE_COUNT else 0.0
        recent_row = recent_map.get(cid, {})
        recent_support = float(recent_row.get("recent_high_sustain") or 0.0)
        recent_score = max(0.0, min(1.0, all_time_score * 0.6 + recent_support * 0.4))
        momentum = recent_score - all_time_score

        # ── Plain-language evidence flags ──────────────────────────────
        evidence_flags: list[str] = []
        if high >= 0.3:
            evidence_flags.append("Frequently participates in high-sustain (anomalous) battles")
        if cross >= 0.15:
            evidence_flags.append("Switches sides at an elevated rate")
        if engagement_avoidance >= 0.4:
            evidence_flags.append("Encounters a specific alliance repeatedly but avoids engaging them")
        if uplift >= 0.3:
            evidence_flags.append("Enemy efficiency is unusually high in their battles")
        if baseline_adjusted_uplift >= 0.25:
            evidence_flags.append("Performance significantly exceeds behavioral baseline")
        if float(row.get("co_occurrence_density") or 0.0) >= 5.0:
            evidence_flags.append("Repeatedly appears alongside the same group of characters")
        if float(row.get("bridge_score") or 0.0) >= 5.0:
            evidence_flags.append("Acts as a bridge between distinct player clusters")

        explanation = {
            "formula": "weighted_v2_components",
            "weights": SUSPICION_V2_WEIGHTS,
            "components": components,
            "evidence_flags": evidence_flags,
            "normal_behavior_score": normal_behavior_score,
            "anomaly_delta_score": anomaly_delta,
            "baseline_adjusted_uplift": baseline_adjusted_uplift,
            "engagement_avoidance_raw": engagement_avoidance,
            "minimum_sample_count": MIN_SAMPLE_COUNT,
        }

        evidence_count = len(top_neighbors.get(cid, [])) + int(recent_row.get("recent_battle_count") or 0)
        top_supporting_battles = top_battles_map.get(cid, [])

        score_payload.append(
            (
                cid,
                recent_score,
                all_time_score,
                momentum,
                all_time_score,
                high,
                low,
                cross,
                uplift,
                role,
                eligible,
                evidence_count,
                int(row.get("community_id") or 0),
                json_dumps_safe(top_supporting_battles),
                json_dumps_safe(top_neighbors.get(cid, [])),
                json_dumps_safe(explanation),
                computed_at,
            )
        )

        # Emit per-component evidence rows for the normalized output layer.
        for comp_key, comp_val in components.items():
            bv2_evidence_rows.append({
                "character_id": cid,
                "evidence_key": f"bv2_{comp_key}",
                "window_label": "all_time",
                "evidence_value": round(float(comp_val), 6),
                "evidence_text": next(
                    (flag for flag in evidence_flags if comp_key.replace("_", " ") in flag.lower()),
                    f"{comp_key} weighted component = {comp_val:.4f}",
                ),
                "evidence_payload_json": json_dumps_safe({
                    "component": comp_key,
                    "weight": SUSPICION_V2_WEIGHTS.get(comp_key, 0.0),
                    "weighted_value": round(float(comp_val), 6),
                    "all_time_score": round(all_time_score, 6),
                }),
            })
        bv2_evidence_rows.append({
            "character_id": cid,
            "evidence_key": "bv2_suspicion_score",
            "window_label": "all_time",
            "evidence_value": round(all_time_score, 6),
            "evidence_text": f"behavioral suspicion score {all_time_score:.4f}",
            "evidence_payload_json": json_dumps_safe({"all_time": round(all_time_score, 6), "recent": round(recent_score, 6), "momentum": round(momentum, 6)}),
        })
        if recent_score != all_time_score:
            bv2_evidence_rows.append({
                "character_id": cid,
                "evidence_key": "bv2_suspicion_score",
                "window_label": "recent",
                "evidence_value": round(recent_score, 6),
                "evidence_text": f"recent behavioral suspicion score {recent_score:.4f} (momentum {momentum:+.4f})",
                "evidence_payload_json": json_dumps_safe({"all_time": round(all_time_score, 6), "recent": round(recent_score, 6), "momentum": round(momentum, 6)}),
            })

    sorted_scores = sorted(float(r[4]) for r in score_payload)
    total = float(max(len(sorted_scores), 1))

    # Pre-compute percentile ranks using bisect (O(n log n) total).
    insert_tuples = []
    for row in score_payload:
        score = float(row[4])
        percentile = _safe_div(float(bisect.bisect_right(sorted_scores, score)), total, 0.0)
        insert_tuples.append(
            (
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                float(percentile),
                float(row[5]),
                float(row[6]),
                float(row[7]),
                float(row[8]),
                float(row[9]),
                int(row[10]),
                int(row[11]),
                int(row[12]),
                row[13],
                row[14],
                row[15],
                row[16],
            )
        )

    # ── Cohort-normalize the behavioral evidence rows ────────────
    _bv2_cohort_normalize(bv2_evidence_rows)

    # ── Persist in short, retryable transactions ──────────────────
    # Previously this whole block ran inside one long ``db.transaction()``
    # that held row locks across hundreds of thousands of INSERT/UPDATE
    # statements (and a giant ``NOT IN`` DELETE) — that regularly tripped
    # the MariaDB 50s lock-wait timeout (issue #898).
    #
    # We now split the writes into short batches, each in its own
    # ``run_in_transaction`` call which automatically retries on deadlock
    # (1213) and lock-wait timeout (1205). Batches are sorted by their
    # primary key so concurrent runs always acquire locks in the same
    # order, eliminating cross-job deadlocks.

    BATCH_SIZE = 500

    # Sort by character_id for consistent lock acquisition order across runs.
    insert_tuples.sort(key=lambda r: int(r[0]))
    suspicion_ids: set[int] = {int(row[0]) for row in insert_tuples}

    insert_sql = """
        INSERT INTO character_suspicion_scores (
            character_id,
            suspicion_score_recent,
            suspicion_score_all_time,
            suspicion_momentum,
            suspicion_score,
            percentile_rank,
            high_sustain_frequency,
            low_sustain_frequency,
            cross_side_rate,
            enemy_efficiency_uplift,
            role_weight,
            supporting_battle_count,
            support_evidence_count,
            community_id,
            top_supporting_battles_json,
            top_graph_neighbors_json,
            explanation_json,
            computed_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            suspicion_score_recent = VALUES(suspicion_score_recent),
            suspicion_score_all_time = VALUES(suspicion_score_all_time),
            suspicion_momentum = VALUES(suspicion_momentum),
            suspicion_score = VALUES(suspicion_score),
            percentile_rank = VALUES(percentile_rank),
            high_sustain_frequency = VALUES(high_sustain_frequency),
            low_sustain_frequency = VALUES(low_sustain_frequency),
            cross_side_rate = VALUES(cross_side_rate),
            enemy_efficiency_uplift = VALUES(enemy_efficiency_uplift),
            role_weight = VALUES(role_weight),
            supporting_battle_count = VALUES(supporting_battle_count),
            support_evidence_count = VALUES(support_evidence_count),
            community_id = VALUES(community_id),
            top_supporting_battles_json = VALUES(top_supporting_battles_json),
            top_graph_neighbors_json = VALUES(top_graph_neighbors_json),
            explanation_json = VALUES(explanation_json),
            computed_at = VALUES(computed_at)
        """

    for i in range(0, len(insert_tuples), BATCH_SIZE):
        batch = insert_tuples[i : i + BATCH_SIZE]

        def _write_scores(_conn, cursor, batch=batch):
            cursor.executemany(insert_sql, batch)

        db.run_in_transaction(_write_scores)

    # Remove stale suspicion scores not in the current computation.
    # We avoid a single giant ``NOT IN`` DELETE (which can touch tens of
    # thousands of rows in one statement and hold locks for too long).
    # Instead we look up the stale ids in a read-only query, then delete
    # them in small batches, each in its own short retryable transaction.
    if not insert_tuples:
        def _wipe_scores(_conn, cursor):
            cursor.execute("DELETE FROM character_suspicion_scores")

        db.run_in_transaction(_wipe_scores)
    elif suspicion_ids:
        existing_rows = db.fetch_all(
            "SELECT character_id FROM character_suspicion_scores"
        )
        stale_ids = sorted(
            int(r["character_id"]) for r in existing_rows
            if int(r["character_id"]) not in suspicion_ids
        )
        DELETE_BATCH = 500
        for i in range(0, len(stale_ids), DELETE_BATCH):
            stale_batch = stale_ids[i : i + DELETE_BATCH]

            def _delete_stale(_conn, cursor, stale_batch=stale_batch):
                placeholders = ",".join(["%s"] * len(stale_batch))
                cursor.execute(
                    f"DELETE FROM character_suspicion_scores WHERE character_id IN ({placeholders})",
                    tuple(stale_batch),
                )

            db.run_in_transaction(_delete_stale)

    # ── Persist behavioral evidence rows in batched executemany calls ──
    # Previously this loop ran row-by-row ``cursor.execute`` inside the
    # same long transaction as the scores above — 10-20k individual
    # statements was the worst offender for lock hold time.
    if bv2_evidence_rows:
        evidence_sql = """
            INSERT INTO character_counterintel_evidence (
                character_id, evidence_key, window_label,
                evidence_value, expected_value, deviation_value,
                z_score, mad_score, cohort_percentile, confidence_flag,
                evidence_text, evidence_payload_json, computed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                evidence_value = VALUES(evidence_value),
                expected_value = VALUES(expected_value),
                deviation_value = VALUES(deviation_value),
                z_score = VALUES(z_score),
                mad_score = VALUES(mad_score),
                cohort_percentile = VALUES(cohort_percentile),
                confidence_flag = VALUES(confidence_flag),
                evidence_text = VALUES(evidence_text),
                evidence_payload_json = VALUES(evidence_payload_json),
                computed_at = VALUES(computed_at)
            """

        evidence_tuples = [
            (
                int(ev["character_id"]),
                str(ev["evidence_key"]),
                str(ev.get("window_label", "all_time")),
                ev["evidence_value"],
                ev.get("expected_value"),
                ev.get("deviation_value"),
                ev.get("z_score"),
                ev.get("mad_score"),
                ev.get("cohort_percentile"),
                ev.get("confidence_flag", "low"),
                ev["evidence_text"],
                ev["evidence_payload_json"],
                computed_at,
            )
            for ev in bv2_evidence_rows
        ]

        # Sort by (character_id, evidence_key, window_label) — the composite
        # PK of character_counterintel_evidence — to ensure consistent lock
        # ordering across concurrent workers.
        evidence_tuples.sort(key=lambda r: (r[0], r[1], r[2]))

        for i in range(0, len(evidence_tuples), BATCH_SIZE):
            batch = evidence_tuples[i : i + BATCH_SIZE]

            def _write_evidence(_conn, cursor, batch=batch):
                cursor.executemany(evidence_sql, batch)

            db.run_in_transaction(_write_evidence)

    return JobResult.success(
        job_key="compute_suspicion_scores_v2",
        summary=f"Scored {len(score_payload)} characters from {len(rows)} behavioral baselines.",
        rows_processed=len(rows),
        rows_written=len(score_payload),
        duration_ms=int((time.perf_counter() - started) * 1000),
        meta={"computed_at": computed_at},
    ).to_dict()
