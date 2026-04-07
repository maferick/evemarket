"""Intelligence signal type definitions for the CIP fusion engine.

Each entry defines a signal type that pipelines can emit into the
``character_intelligence_signals`` table.  The fusion engine uses these
definitions to apply decay, weighting, and domain classification.

This module is the single source of truth for signal metadata.  Adding a
new signal type is a two-step process:
  1. Add a definition here.
  2. Emit the signal from the relevant pipeline via ``emit_signals()``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run


@dataclass(frozen=True, slots=True)
class SignalDefinition:
    signal_type: str
    signal_domain: str          # behavioral, graph, temporal, movement, relational
    display_name: str
    description: str
    decay_type: str             # none, linear, exponential, step
    half_life_days: int
    cost_class: str             # low, medium, high, very_high
    tactical_eligible: bool
    current_version: str
    weight_default: float
    # Normalization: how raw source values become fusion-ready [0,1] values
    normalization: str          # bounded_0_1, binary, percentile, zscore_capped, piecewise
    norm_params: dict[str, float] | None = None  # e.g. {"cap": 3.0} for zscore_capped


# ---------------------------------------------------------------------------
# Confidence rubric — shared contract for all adapters
# ---------------------------------------------------------------------------
# Every adapter must compute confidence against this rubric.  The four axes
# are evaluated independently and the minimum is taken as the final confidence.
#
# 1. Sample sufficiency  — enough underlying observations?
#      <3 events → 0.2,  3-5 → 0.5,  6-10 → 0.75,  >10 → 0.9
# 2. Recency             — how fresh is the source data?
#      <7d → 1.0,  7-30d → 0.8,  30-60d → 0.5,  >60d → 0.3
# 3. Cohort grounding    — was the value compared against a peer group?
#      yes → 1.0,  partial → 0.7,  no → 0.5
# 4. Source completeness — did the pipeline have all its inputs?
#      full → 1.0,  degraded → 0.6
#
# Adapters use the helper below to standardize confidence computation.

def compute_signal_confidence(
    sample_count: int,
    age_days: float = 0.0,
    cohort_grounded: bool = True,
    source_complete: bool = True,
) -> float:
    """Compute confidence per the shared CIP rubric.

    Returns a value in [0, 1] representing how much the fused score should
    trust this signal measurement.
    """
    # Sample sufficiency
    if sample_count < 3:
        sample_conf = 0.20
    elif sample_count <= 5:
        sample_conf = 0.50
    elif sample_count <= 10:
        sample_conf = 0.75
    else:
        sample_conf = 0.90

    # Recency
    if age_days < 7:
        recency_conf = 1.0
    elif age_days < 30:
        recency_conf = 0.8
    elif age_days < 60:
        recency_conf = 0.5
    else:
        recency_conf = 0.3

    # Cohort grounding
    cohort_conf = 1.0 if cohort_grounded else 0.5

    # Source completeness
    completeness_conf = 1.0 if source_complete else 0.6

    return min(sample_conf, recency_conf, cohort_conf, completeness_conf)


# ---------------------------------------------------------------------------
# Canonical signal definitions
# ---------------------------------------------------------------------------

SIGNAL_DEFINITIONS: list[SignalDefinition] = [
    # ── Behavioral domain (from suspicion_scores_v2 + behavioral_scoring) ──
    SignalDefinition(
        signal_type="suspicion_score",
        signal_domain="behavioral",
        display_name="Suspicion Score",
        description="Blended suspicion score from battle behavior, graph metrics, and cohort normalization",
        decay_type="exponential", half_life_days=30,
        cost_class="high", tactical_eligible=False,
        current_version="v2", weight_default=0.20,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="high_sustain_frequency",
        signal_domain="behavioral",
        display_name="High Sustain Frequency",
        description="Frequency of appearing in battles where the enemy side overperformed",
        decay_type="exponential", half_life_days=30,
        cost_class="high", tactical_eligible=False,
        current_version="v2", weight_default=0.12,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="cross_side_rate",
        signal_domain="behavioral",
        display_name="Cross-Side Rate",
        description="Rate of appearing on opposing sides in different battles with the same actors",
        decay_type="exponential", half_life_days=30,
        cost_class="high", tactical_eligible=False,
        current_version="v2", weight_default=0.10,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="enemy_efficiency_uplift",
        signal_domain="behavioral",
        display_name="Enemy Efficiency Uplift",
        description="How much enemy efficiency increases when this character is present",
        decay_type="exponential", half_life_days=30,
        cost_class="high", tactical_eligible=False,
        current_version="v2", weight_default=0.15,
        normalization="zscore_capped", norm_params={"cap": 3.0},
    ),
    SignalDefinition(
        signal_type="behavioral_risk_score",
        signal_domain="behavioral",
        display_name="Behavioral Risk Score",
        description="Small-engagement behavioral risk scoring (Lane 2: 1-19 participant battles)",
        decay_type="exponential", half_life_days=21,
        cost_class="high", tactical_eligible=False,
        current_version="v1", weight_default=0.12,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="engagement_avoidance",
        signal_domain="behavioral",
        display_name="Engagement Avoidance",
        description="Degree to which a character avoids direct engagement while maintaining presence",
        decay_type="exponential", half_life_days=30,
        cost_class="high", tactical_eligible=False,
        current_version="v1", weight_default=0.08,
        normalization="bounded_0_1", norm_params=None,
    ),

    # ── Graph domain (from graph_community_detection, graph_intelligence) ──
    SignalDefinition(
        signal_type="pagerank_score",
        signal_domain="graph",
        display_name="PageRank Score",
        description="PageRank centrality in the character co-occurrence graph",
        decay_type="exponential", half_life_days=14,
        cost_class="very_high", tactical_eligible=False,
        current_version="v1", weight_default=0.06,
        normalization="percentile", norm_params=None,
    ),
    SignalDefinition(
        signal_type="bridge_score",
        signal_domain="graph",
        display_name="Bridge Score",
        description="Betweenness centrality — characters bridging distinct communities",
        decay_type="exponential", half_life_days=14,
        cost_class="very_high", tactical_eligible=False,
        current_version="v1", weight_default=0.10,
        normalization="piecewise", norm_params={"low": 0.0, "mid": 2.0, "high": 10.0},
    ),
    SignalDefinition(
        signal_type="co_occurrence_density",
        signal_domain="graph",
        display_name="Co-occurrence Density",
        description="Density of co-occurrence edges in the character's local neighborhood",
        decay_type="exponential", half_life_days=14,
        cost_class="high", tactical_eligible=False,
        current_version="v1", weight_default=0.08,
        normalization="zscore_capped", norm_params={"cap": 3.0},
    ),
    SignalDefinition(
        signal_type="cross_side_cluster_score",
        signal_domain="graph",
        display_name="Cross-Side Cluster Score",
        description="Score indicating connections spanning hostile and friendly clusters",
        decay_type="exponential", half_life_days=14,
        cost_class="high", tactical_eligible=False,
        current_version="v1", weight_default=0.06,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="neighbor_anomaly_score",
        signal_domain="graph",
        display_name="Neighbor Anomaly Score",
        description="Anomaly density among graph neighbors (guilt by association)",
        decay_type="exponential", half_life_days=14,
        cost_class="high", tactical_eligible=False,
        current_version="v1", weight_default=0.05,
        normalization="bounded_0_1", norm_params=None,
    ),

    # ── Temporal domain (from temporal_behavior_detection) ──
    SignalDefinition(
        signal_type="active_hour_shift",
        signal_domain="temporal",
        display_name="Active Hour Shift",
        description="Jensen-Shannon divergence in hour-of-day activity distribution",
        decay_type="exponential", half_life_days=14,
        cost_class="medium", tactical_eligible=True,
        current_version="v1", weight_default=0.06,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="weekday_profile_shift",
        signal_domain="temporal",
        display_name="Weekday Profile Shift",
        description="Jensen-Shannon divergence in weekday activity distribution",
        decay_type="exponential", half_life_days=14,
        cost_class="medium", tactical_eligible=True,
        current_version="v1", weight_default=0.04,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="cadence_burstiness",
        signal_domain="temporal",
        display_name="Cadence Burstiness",
        description="Irregularity in inter-event timing (normalized burstiness index)",
        decay_type="exponential", half_life_days=14,
        cost_class="medium", tactical_eligible=True,
        current_version="v1", weight_default=0.04,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="reactivation_after_dormancy",
        signal_domain="temporal",
        display_name="Reactivation After Dormancy",
        description="Activity spike after prolonged inactivity (>30 days dormant)",
        decay_type="step", half_life_days=30,
        cost_class="medium", tactical_eligible=True,
        current_version="v1", weight_default=0.06,
        normalization="binary", norm_params=None,
    ),

    # ── Movement domain (from character_movement_footprints) ──
    SignalDefinition(
        signal_type="footprint_expansion",
        signal_domain="movement",
        display_name="Footprint Expansion",
        description="Rapid expansion of geographic footprint into new regions",
        decay_type="exponential", half_life_days=14,
        cost_class="medium", tactical_eligible=True,
        current_version="v1", weight_default=0.05,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="hostile_overlap_change",
        signal_domain="movement",
        display_name="Hostile Overlap Change",
        description="Change in geographic overlap with hostile alliance operating areas",
        decay_type="exponential", half_life_days=14,
        cost_class="medium", tactical_eligible=True,
        current_version="v1", weight_default=0.06,
        normalization="bounded_0_1", norm_params=None,
    ),
    SignalDefinition(
        signal_type="new_area_entry",
        signal_domain="movement",
        display_name="New Area Entry",
        description="Character operating in systems not previously frequented",
        decay_type="exponential", half_life_days=14,
        cost_class="medium", tactical_eligible=True,
        current_version="v1", weight_default=0.04,
        normalization="bounded_0_1", norm_params=None,
    ),

    # ── Relational domain (from copresence_edges, pre_op_join, etc.) ──
    SignalDefinition(
        signal_type="copresence_anomaly",
        signal_domain="relational",
        display_name="Co-presence Anomaly",
        description="Anomalous co-presence patterns deviating from cohort baseline",
        decay_type="exponential", half_life_days=21,
        cost_class="high", tactical_eligible=False,
        current_version="v1", weight_default=0.08,
        normalization="percentile", norm_params=None,
    ),
    SignalDefinition(
        signal_type="pre_op_join",
        signal_domain="relational",
        display_name="Pre-Op Join",
        description="Joined corp/alliance shortly before a significant battle",
        decay_type="step", half_life_days=30,
        cost_class="low", tactical_eligible=True,
        current_version="v1", weight_default=0.10,
        normalization="binary", norm_params=None,
    ),
    SignalDefinition(
        signal_type="alliance_overlap_risk",
        signal_domain="relational",
        display_name="Alliance Overlap Risk",
        description="Historical alliance membership overlap with hostile entities",
        decay_type="linear", half_life_days=90,
        cost_class="medium", tactical_eligible=False,
        current_version="v1", weight_default=0.05,
        normalization="bounded_0_1", norm_params=None,
    ),
]

# Quick lookup by signal_type
SIGNAL_DEF_MAP: dict[str, SignalDefinition] = {d.signal_type: d for d in SIGNAL_DEFINITIONS}

# All known signal domains (used for coverage calculation)
ALL_SIGNAL_DOMAINS: tuple[str, ...] = ("behavioral", "graph", "temporal", "movement", "relational")


# ---------------------------------------------------------------------------
# Seed / sync definitions to the database
# ---------------------------------------------------------------------------

def run_seed_signal_definitions(db: SupplyCoreDb) -> JobResult:
    """Upsert all signal definitions into ``intelligence_signal_definitions``."""
    job = start_job_run(db, "seed_signal_definitions")
    try:
        sql = """
            INSERT INTO intelligence_signal_definitions (
                signal_type, signal_domain, display_name, description,
                decay_type, half_life_days, cost_class, tactical_eligible,
                current_version, weight_default,
                normalization_type, normalization_params_json
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                signal_domain              = VALUES(signal_domain),
                display_name               = VALUES(display_name),
                description                = VALUES(description),
                decay_type                 = VALUES(decay_type),
                half_life_days             = VALUES(half_life_days),
                cost_class                 = VALUES(cost_class),
                tactical_eligible          = VALUES(tactical_eligible),
                current_version            = VALUES(current_version),
                weight_default             = VALUES(weight_default),
                normalization_type         = VALUES(normalization_type),
                normalization_params_json  = VALUES(normalization_params_json)
        """
        rows_written = 0
        for defn in SIGNAL_DEFINITIONS:
            import json as _json
            norm_json = _json.dumps(defn.norm_params) if defn.norm_params else None
            db.execute(sql, [
                defn.signal_type, defn.signal_domain, defn.display_name,
                defn.description, defn.decay_type, defn.half_life_days,
                defn.cost_class, 1 if defn.tactical_eligible else 0,
                defn.current_version, defn.weight_default,
                defn.normalization, norm_json,
            ])
            rows_written += 1

        finish_job_run(db, job, status="success", rows_processed=len(SIGNAL_DEFINITIONS), rows_written=rows_written)
        return JobResult(
            status="success",
            summary=f"Seeded {rows_written} signal definitions",
            started_at="", finished_at="",
            duration_ms=0, rows_seen=len(SIGNAL_DEFINITIONS),
            rows_processed=len(SIGNAL_DEFINITIONS), rows_written=rows_written,
            rows_skipped=0, rows_failed=0, batches_completed=1,
            checkpoint_before=None, checkpoint_after=None,
            has_more=False, error_text=None, warnings=[], meta={},
        )
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=0, rows_written=0, meta={"error": str(exc)})
        raise
