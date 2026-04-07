"""CIP Signal Emitter — adapts existing pipeline outputs into unified signals.

Reads from the legacy intelligence tables and emits typed, versioned signals
into ``character_intelligence_signals``.  This is a bridge layer: existing
pipelines continue to write to their own tables, and this job harvests those
outputs into the signal registry.

Phase 1 adapts four source pipelines:
  1. character_suspicion_scores  → behavioral signals
  2. character_graph_intelligence → graph signals
  3. character_behavioral_scores → behavioral signals (Lane 2)
  4. character_counterintel_evidence → temporal + relational signals
  5. character_movement_footprints → movement signals
  6. character_copresence_signals → relational signals
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from .cip_signal_definitions import SIGNAL_DEF_MAP, compute_signal_confidence

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _clamp(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Signal upsert helper
# ---------------------------------------------------------------------------

def _upsert_signals(db: SupplyCoreDb, signals: list[dict[str, Any]]) -> int:
    """Batch-upsert signals into ``character_intelligence_signals``.

    Each signal dict must have:
      character_id, signal_type, window_label, signal_value, confidence,
      signal_version, source_pipeline, computed_at, detail_json (optional)
    """
    if not signals:
        return 0

    sql = """
        INSERT INTO character_intelligence_signals (
            character_id, signal_type, window_label,
            signal_value, confidence, signal_version, source_pipeline,
            computed_at, first_seen_at, last_reinforced_at, reinforcement_count,
            detail_json
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s)
        ON DUPLICATE KEY UPDATE
            signal_value        = VALUES(signal_value),
            confidence          = VALUES(confidence),
            signal_version      = VALUES(signal_version),
            source_pipeline     = VALUES(source_pipeline),
            computed_at         = VALUES(computed_at),
            last_reinforced_at  = VALUES(last_reinforced_at),
            reinforcement_count = reinforcement_count + 1,
            detail_json         = VALUES(detail_json)
    """
    written = 0
    for i in range(0, len(signals), BATCH_SIZE):
        batch = signals[i:i + BATCH_SIZE]
        for sig in batch:
            computed = sig["computed_at"]
            db.execute(sql, [
                sig["character_id"], sig["signal_type"], sig.get("window_label", "all_time"),
                sig["signal_value"], sig.get("confidence", 1.0),
                sig.get("signal_version", "v1"), sig.get("source_pipeline", ""),
                computed, computed, computed,
                sig.get("detail_json"),
            ])
            written += 1
    return written


# ---------------------------------------------------------------------------
# Adapter 1: character_suspicion_scores → behavioral signals
# ---------------------------------------------------------------------------

def _emit_suspicion_signals(db: SupplyCoreDb) -> int:
    """Emit behavioral signals from character_suspicion_scores."""
    rows = db.fetch_all("""
        SELECT character_id, suspicion_score, suspicion_score_recent,
               high_sustain_frequency, cross_side_rate,
               enemy_efficiency_uplift, role_weight,
               percentile_rank, cohort_z_score,
               supporting_battle_count, computed_at
        FROM character_suspicion_scores
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
    """)
    if not rows:
        return 0

    signals: list[dict[str, Any]] = []
    for row in rows:
        cid = row["character_id"]
        computed = row["computed_at"]
        battle_count = int(row.get("supporting_battle_count") or 0)
        # Use standardized confidence rubric
        computed_dt = computed if isinstance(computed, datetime) else datetime.fromisoformat(str(computed))
        age_days = max(0.0, (datetime.now(UTC) - computed_dt.replace(tzinfo=UTC)).total_seconds() / 86400.0) if computed_dt.tzinfo is None else max(0.0, (datetime.now(UTC) - computed_dt).total_seconds() / 86400.0)
        has_cohort = row.get("cohort_z_score") is not None
        base_confidence = compute_signal_confidence(
            sample_count=battle_count, age_days=age_days,
            cohort_grounded=has_cohort, source_complete=True,
        )

        signals.append({
            "character_id": cid,
            "signal_type": "suspicion_score",
            "signal_value": float(row.get("suspicion_score") or 0),
            "confidence": base_confidence,
            "signal_version": "v2",
            "source_pipeline": "compute_suspicion_scores_v2",
            "computed_at": computed,
            "detail_json": json_dumps_safe({
                "percentile_rank": float(row.get("percentile_rank") or 0),
                "cohort_z_score": float(row.get("cohort_z_score") or 0),
                "recent_score": float(row.get("suspicion_score_recent") or 0),
                "battle_count": battle_count,
            }),
        })
        if row.get("high_sustain_frequency") is not None:
            signals.append({
                "character_id": cid,
                "signal_type": "high_sustain_frequency",
                "signal_value": float(row["high_sustain_frequency"]),
                "confidence": base_confidence,
                "signal_version": "v2",
                "source_pipeline": "compute_suspicion_scores_v2",
                "computed_at": computed,
            })
        if row.get("cross_side_rate") is not None:
            signals.append({
                "character_id": cid,
                "signal_type": "cross_side_rate",
                "signal_value": float(row["cross_side_rate"]),
                "confidence": base_confidence,
                "signal_version": "v2",
                "source_pipeline": "compute_suspicion_scores_v2",
                "computed_at": computed,
            })
        if row.get("enemy_efficiency_uplift") is not None:
            signals.append({
                "character_id": cid,
                "signal_type": "enemy_efficiency_uplift",
                "signal_value": float(row["enemy_efficiency_uplift"]),
                "confidence": base_confidence,
                "signal_version": "v2",
                "source_pipeline": "compute_suspicion_scores_v2",
                "computed_at": computed,
            })

    return _upsert_signals(db, signals)


# ---------------------------------------------------------------------------
# Adapter 2: character_graph_intelligence → graph signals
# ---------------------------------------------------------------------------

def _emit_graph_signals(db: SupplyCoreDb) -> int:
    """Emit graph signals from character_graph_intelligence."""
    rows = db.fetch_all("""
        SELECT character_id, pagerank_score, bridge_score,
               co_occurrence_density, anomalous_co_occurrence_density,
               cross_side_cluster_score, neighbor_anomaly_score,
               engagement_avoidance_score, computed_at
        FROM character_graph_intelligence
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
    """)
    if not rows:
        return 0

    signals: list[dict[str, Any]] = []
    for row in rows:
        cid = row["character_id"]
        computed = row["computed_at"]
        # Graph signals: high sample sufficiency (graph covers all characters),
        # always cohort-grounded (relative to graph population)
        computed_dt = computed if isinstance(computed, datetime) else datetime.fromisoformat(str(computed))
        age_days = max(0.0, (datetime.now(UTC) - (computed_dt.replace(tzinfo=UTC) if computed_dt.tzinfo is None else computed_dt)).total_seconds() / 86400.0)
        conf = compute_signal_confidence(
            sample_count=20, age_days=age_days,
            cohort_grounded=True, source_complete=True,
        )

        mapping = [
            ("pagerank_score", "pagerank_score"),
            ("bridge_score", "bridge_score"),
            ("co_occurrence_density", "co_occurrence_density"),
            ("cross_side_cluster_score", "cross_side_cluster_score"),
            ("neighbor_anomaly_score", "neighbor_anomaly_score"),
            ("engagement_avoidance", "engagement_avoidance_score"),
        ]
        for signal_type, col in mapping:
            val = row.get(col)
            if val is not None:
                signals.append({
                    "character_id": cid,
                    "signal_type": signal_type,
                    "signal_value": float(val),
                    "confidence": conf,
                    "signal_version": "v1",
                    "source_pipeline": "graph_community_detection_sync",
                    "computed_at": computed,
                })

    return _upsert_signals(db, signals)


# ---------------------------------------------------------------------------
# Adapter 3: character_behavioral_scores → behavioral signals (Lane 2)
# ---------------------------------------------------------------------------

def _emit_behavioral_scoring_signals(db: SupplyCoreDb) -> int:
    """Emit Lane 2 behavioral risk signal from character_behavioral_scores."""
    rows = db.fetch_all("""
        SELECT character_id, behavioral_risk_score, percentile_rank,
               confidence_tier, total_kill_count,
               fleet_absence_ratio, post_engagement_continuation_rate,
               kill_concentration_score, geographic_concentration_score,
               temporal_regularity_score, companion_consistency_score,
               cross_side_small_rate, asymmetry_preference,
               computed_at
        FROM character_behavioral_scores
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
    """)
    if not rows:
        return 0

    signals: list[dict[str, Any]] = []
    for row in rows:
        cid = row["character_id"]
        computed = row["computed_at"]
        kill_count = int(row.get("total_kill_count") or 0)
        computed_dt = computed if isinstance(computed, datetime) else datetime.fromisoformat(str(computed))
        age_days = max(0.0, (datetime.now(UTC) - (computed_dt.replace(tzinfo=UTC) if computed_dt.tzinfo is None else computed_dt)).total_seconds() / 86400.0)
        conf = compute_signal_confidence(
            sample_count=kill_count, age_days=age_days,
            cohort_grounded=False, source_complete=True,
        )

        signals.append({
            "character_id": cid,
            "signal_type": "behavioral_risk_score",
            "signal_value": float(row.get("behavioral_risk_score") or 0),
            "confidence": conf,
            "signal_version": "v1",
            "source_pipeline": "compute_behavioral_scoring",
            "computed_at": computed,
            "detail_json": json_dumps_safe({
                "percentile_rank": float(row.get("percentile_rank") or 0),
                "total_kill_count": int(row.get("total_kill_count") or 0),
                "fleet_absence_ratio": float(row.get("fleet_absence_ratio") or 0),
                "kill_concentration_score": float(row.get("kill_concentration_score") or 0),
                "geographic_concentration_score": float(row.get("geographic_concentration_score") or 0),
                "companion_consistency_score": float(row.get("companion_consistency_score") or 0),
            }),
        })

    return _upsert_signals(db, signals)


# ---------------------------------------------------------------------------
# Adapter 4: character_counterintel_evidence → temporal + relational signals
# ---------------------------------------------------------------------------

# Map evidence_keys to CIP signal types
_EVIDENCE_TO_SIGNAL: dict[str, str] = {
    "active_hour_shift": "active_hour_shift",
    "weekday_profile_shift": "weekday_profile_shift",
    "cadence_burstiness": "cadence_burstiness",
    "reactivation_after_dormancy": "reactivation_after_dormancy",
    "pre_op_join": "pre_op_join",
    "historical_alliance_overlap": "alliance_overlap_risk",
}

def _emit_counterintel_signals(db: SupplyCoreDb) -> int:
    """Emit temporal and relational signals from character_counterintel_evidence."""
    evidence_keys = list(_EVIDENCE_TO_SIGNAL.keys())
    placeholders = ",".join(["%s"] * len(evidence_keys))
    rows = db.fetch_all(f"""
        SELECT character_id, evidence_key, window_label,
               evidence_value, z_score, cohort_percentile,
               confidence_flag, evidence_text, evidence_payload_json,
               computed_at
        FROM character_counterintel_evidence
        WHERE evidence_key IN ({placeholders})
          AND computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
    """, evidence_keys)
    if not rows:
        return 0

    signals: list[dict[str, Any]] = []
    for row in rows:
        ek = row["evidence_key"]
        signal_type = _EVIDENCE_TO_SIGNAL.get(ek)
        if not signal_type:
            continue

        computed_dt = row["computed_at"] if isinstance(row["computed_at"], datetime) else datetime.fromisoformat(str(row["computed_at"]))
        age_days = max(0.0, (datetime.now(UTC) - (computed_dt.replace(tzinfo=UTC) if computed_dt.tzinfo is None else computed_dt)).total_seconds() / 86400.0)
        has_cohort = row.get("cohort_percentile") is not None and float(row.get("cohort_percentile") or 0) > 0
        # Evidence rows don't carry explicit sample count; use cohort percentile
        # presence as a proxy for sufficient grounding
        conf = compute_signal_confidence(
            sample_count=8, age_days=age_days,
            cohort_grounded=has_cohort, source_complete=True,
        )
        signals.append({
            "character_id": row["character_id"],
            "signal_type": signal_type,
            "window_label": row.get("window_label", "all_time"),
            "signal_value": float(row.get("evidence_value") or 0),
            "confidence": conf,
            "signal_version": "v1",
            "source_pipeline": "temporal_behavior_detection",
            "computed_at": row["computed_at"],
            "detail_json": json_dumps_safe({
                "z_score": float(row.get("z_score") or 0),
                "cohort_percentile": float(row.get("cohort_percentile") or 0),
                "evidence_text": row.get("evidence_text", ""),
            }),
        })

    return _upsert_signals(db, signals)


# ---------------------------------------------------------------------------
# Adapter 5: character_movement_footprints → movement signals
# ---------------------------------------------------------------------------

def _emit_movement_signals(db: SupplyCoreDb) -> int:
    """Emit movement signals from character_movement_footprints."""
    rows = db.fetch_all("""
        SELECT character_id, window_label,
               footprint_expansion_score, footprint_contraction_score,
               new_area_entry_score, hostile_overlap_change_score,
               cohort_z_footprint_size, cohort_z_hostile_overlap,
               cohort_percentile_footprint,
               computed_at
        FROM character_movement_footprints
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
          AND window_label = '30d'
    """)
    if not rows:
        return 0

    signals: list[dict[str, Any]] = []
    for row in rows:
        cid = row["character_id"]
        computed = row["computed_at"]
        # Confidence from cohort percentile availability
        conf = 0.8 if row.get("cohort_percentile_footprint") is not None else 0.5

        mapping = [
            ("footprint_expansion", "footprint_expansion_score"),
            ("hostile_overlap_change", "hostile_overlap_change_score"),
            ("new_area_entry", "new_area_entry_score"),
        ]
        for signal_type, col in mapping:
            val = row.get(col)
            if val is not None and float(val) > 0.0:
                signals.append({
                    "character_id": cid,
                    "signal_type": signal_type,
                    "window_label": "30d",
                    "signal_value": float(val),
                    "confidence": conf,
                    "signal_version": "v1",
                    "source_pipeline": "character_movement_footprints",
                    "computed_at": computed,
                    "detail_json": json_dumps_safe({
                        "cohort_z_footprint_size": float(row.get("cohort_z_footprint_size") or 0),
                        "cohort_z_hostile_overlap": float(row.get("cohort_z_hostile_overlap") or 0),
                    }),
                })

    return _upsert_signals(db, signals)


# ---------------------------------------------------------------------------
# Adapter 6: character_copresence_signals → relational signals
# ---------------------------------------------------------------------------

def _emit_copresence_signals(db: SupplyCoreDb) -> int:
    """Emit relational signals from character_copresence_signals."""
    rows = db.fetch_all("""
        SELECT character_id, window_label,
               out_of_cluster_ratio, out_of_cluster_ratio_delta,
               pair_frequency_delta, cohort_percentile,
               total_edge_weight, unique_associates,
               computed_at
        FROM character_copresence_signals
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
          AND window_label = '30d'
    """)
    if not rows:
        return 0

    signals: list[dict[str, Any]] = []
    for row in rows:
        cid = row["character_id"]
        computed = row["computed_at"]
        pct = float(row.get("cohort_percentile") or 0)
        # High cohort percentile → anomalous co-presence
        if pct >= 0.7:
            conf = _clamp(pct)
            signals.append({
                "character_id": cid,
                "signal_type": "copresence_anomaly",
                "window_label": "30d",
                "signal_value": pct,
                "confidence": conf,
                "signal_version": "v1",
                "source_pipeline": "compute_copresence_edges",
                "computed_at": computed,
                "detail_json": json_dumps_safe({
                    "out_of_cluster_ratio": float(row.get("out_of_cluster_ratio") or 0),
                    "pair_frequency_delta": float(row.get("pair_frequency_delta") or 0),
                    "unique_associates": int(row.get("unique_associates") or 0),
                }),
            })

    return _upsert_signals(db, signals)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_cip_signal_emitter(db: SupplyCoreDb) -> JobResult:
    """Run all signal emitter adapters and populate character_intelligence_signals."""
    job = start_job_run(db, "cip_signal_emitter")
    t0 = time.monotonic()
    total_written = 0
    warnings: list[str] = []

    adapters = [
        ("suspicion_scores", _emit_suspicion_signals),
        ("graph_intelligence", _emit_graph_signals),
        ("behavioral_scoring", _emit_behavioral_scoring_signals),
        ("counterintel_evidence", _emit_counterintel_signals),
        ("movement_footprints", _emit_movement_signals),
        ("copresence_signals", _emit_copresence_signals),
    ]

    for name, adapter_fn in adapters:
        try:
            count = adapter_fn(db)
            total_written += count
            logger.info("cip_signal_emitter: %s emitted %d signals", name, count)
        except Exception as exc:
            msg = f"Adapter {name} failed: {exc}"
            logger.warning(msg, exc_info=True)
            warnings.append(msg)

    elapsed = int((time.monotonic() - t0) * 1000)
    finish_job_run(db, job, status="success", rows_processed=total_written, rows_written=total_written)

    return JobResult(
        status="success",
        summary=f"Emitted {total_written} signals from {len(adapters)} adapters",
        started_at="", finished_at="",
        duration_ms=elapsed, rows_seen=total_written,
        rows_processed=total_written, rows_written=total_written,
        rows_skipped=0, rows_failed=0, batches_completed=len(adapters),
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=warnings, meta={},
    )
