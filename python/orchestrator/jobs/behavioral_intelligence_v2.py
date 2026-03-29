from __future__ import annotations

import math
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe

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

    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM character_behavioral_baselines")
        if payload:
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
                """,
                payload,
            )

    return JobResult.success(
        job_key="compute_behavioral_baselines",
        summary=f"Computed behavioral baselines for {len(payload)} characters from {len(rows)} actor features.",
        rows_processed=len(rows),
        rows_written=len(payload),
        duration_ms=int((time.perf_counter() - started) * 1000),
        meta={"computed_at": computed_at},
    ).to_dict()


def run_compute_suspicion_scores_v2(db: SupplyCoreDb, runtime: dict[str, Any] | None = None) -> dict[str, Any]:
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

    support_rows = db.fetch_all(
        """
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
            INNER JOIN character_battle_intelligence cbi_filter ON cbi_filter.character_id = baf1.character_id
            INNER JOIN battle_actor_features baf2 ON baf2.battle_id = baf1.battle_id AND baf2.character_id <> baf1.character_id
            GROUP BY baf1.character_id, baf2.character_id
        ) x
        WHERE x.rn <= 5
        """
    )
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

    # Batch-fetch top 5 supporting battles per character (replaces N+1 per-character queries)
    top_battles_rows = db.fetch_all(
        """
        SELECT tb.character_id, tb.battle_id, tb.side_key, tb.z_efficiency_score
        FROM (
            SELECT
                baf.character_id,
                baf.battle_id,
                baf.side_key,
                COALESCE(bsm.z_efficiency_score, 0) AS z_efficiency_score,
                ROW_NUMBER() OVER (PARTITION BY baf.character_id ORDER BY ABS(COALESCE(bsm.z_efficiency_score, 0)) DESC) AS rn
            FROM battle_actor_features baf
            INNER JOIN character_battle_intelligence cbi_filter ON cbi_filter.character_id = baf.character_id
            LEFT JOIN battle_side_metrics bsm ON bsm.battle_id = baf.battle_id AND bsm.side_key = baf.side_key
        ) tb
        WHERE tb.rn <= 5
        """
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

    sorted_scores = sorted((float(r[4]) for r in score_payload))
    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM character_suspicion_scores")
        for row in score_payload:
            score = float(row[4])
            percentile = _safe_div(float(sum(1 for x in sorted_scores if x <= score)), float(max(len(sorted_scores), 1)), 0.0)
            cursor.execute(
                """
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
                """,
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
                ),
            )

    return JobResult.success(
        job_key="compute_suspicion_scores_v2",
        summary=f"Scored {len(score_payload)} characters from {len(rows)} behavioral baselines.",
        rows_processed=len(rows),
        rows_written=len(score_payload),
        duration_ms=int((time.perf_counter() - started) * 1000),
        meta={"computed_at": computed_at},
    ).to_dict()
