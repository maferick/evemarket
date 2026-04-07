"""Compound signal definitions for the CIP system.

Compound signals detect meaningful co-occurrences of simple signals that,
individually, may be unremarkable but together indicate a specific operational
pattern.  They are evaluated against a character's current signal set and
profile state.

Design constraints:
  - Compounds are *observational* first: they are materialized, surfaced,
    and evented on, but do NOT feed into the core risk_score until validated.
  - Each compound is a simple boolean/weighted intersection — no temporal
    sequencing, no absence conditions, no rule DSL.
  - Evidence is always inspectable: the contributing signals are recorded.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class CompoundDefinition:
    compound_type: str
    display_name: str
    description: str
    # Compound family — groups compounds by analyst consumption purpose:
    #   infiltration:    patterns suggesting an entity was planted
    #   coordination:    patterns suggesting coordinated hostile activity
    #   prioritization:  patterns that amplify review urgency (convergent evidence)
    #   trust:           patterns that validate or challenge profile trust surface
    compound_family: str
    # Which simple signals must be present AND above their min_value
    # to activate this compound.  {"signal_type": min_value}
    required_signals: dict[str, float]
    # Optional: additional profile-level conditions (evaluated against
    # character_intelligence_profiles columns).
    # {"column_name": {"op": ">=", "value": 0.5}}
    profile_conditions: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Scoring
    base_weight: float = 0.10
    severity_default: str = "medium"  # used when this compound triggers an event
    # Normalization: how the compound score is computed from contributing signals
    # "min"  = min(signal_values) — conservative, only as strong as weakest link
    # "mean" = mean(signal_values) — balanced
    # "max"  = max(signal_values) — aggressive, driven by strongest signal
    score_mode: str = "mean"
    # Confidence derivation mode:
    #   "min_signal"  = minimum confidence across contributing signals
    #   "weighted"    = confidence weighted by signal weight
    confidence_mode: str = "min_signal"
    # Metadata
    tactical_eligible: bool = False
    enabled: bool = True
    version: str = "v1"


# ---------------------------------------------------------------------------
# Phase 4 compound definitions
# ---------------------------------------------------------------------------

COMPOUND_DEFINITIONS: list[CompoundDefinition] = [
    # ── 1. Elevated suspicion + bridge position ───────────────────────
    # A character who is both highly suspicious AND bridges distinct
    # communities is a strong candidate for an intelligence operative.
    CompoundDefinition(
        compound_type="elevated_suspicion_bridge",
        compound_family="infiltration",
        display_name="Suspicious Bridge Node",
        description=(
            "Character has elevated suspicion score AND high betweenness "
            "centrality (bridging distinct communities). This combination "
            "suggests an intelligence operative connecting separate groups."
        ),
        required_signals={
            "suspicion_score": 0.30,
            "bridge_score": 0.25,
        },
        base_weight=0.15,
        severity_default="high",
        score_mode="min",
        confidence_mode="min_signal",
    ),

    # ── 2. Pre-op join + hostile overlap increase ─────────────────────
    # A character who joined shortly before a battle AND whose geographic
    # footprint now overlaps with hostile territory.
    CompoundDefinition(
        compound_type="pre_op_infiltration",
        compound_family="infiltration",
        display_name="Pre-Op Infiltration Pattern",
        description=(
            "Character joined corp/alliance shortly before a significant "
            "engagement AND shows increased geographic overlap with hostile "
            "areas. Classic infiltration pattern."
        ),
        required_signals={
            "pre_op_join": 0.50,  # binary signal, 0.5 = active
            "hostile_overlap_change": 0.20,
        },
        base_weight=0.15,
        severity_default="high",
        score_mode="min",
        confidence_mode="min_signal",
    ),

    # ── 3. Multi-domain + top percentile ──────────────────────────────
    # A character who is already in a high risk percentile AND has signals
    # firing across many domains — convergent evidence from independent sources.
    CompoundDefinition(
        compound_type="multi_domain_top_percentile",
        compound_family="prioritization",
        display_name="Convergent Multi-Domain Threat",
        description=(
            "Character is in a high risk percentile with active signals "
            "across 4+ independent domains. Convergent evidence from "
            "behavioral, graph, temporal, movement, and relational analysis."
        ),
        required_signals={
            "suspicion_score": 0.15,
            "bridge_score": 0.10,
            "active_hour_shift": 0.10,
            "footprint_expansion": 0.10,
        },
        profile_conditions={
            "risk_percentile": {"op": ">=", "value": 0.90},
        },
        base_weight=0.12,
        severity_default="high",
        score_mode="mean",
        confidence_mode="min_signal",
    ),

    # ── 4. Co-presence anomaly + alliance overlap risk ────────────────
    # Anomalous co-presence patterns combined with historical ties to
    # hostile alliances — suggests a coordinated actor.
    CompoundDefinition(
        compound_type="copresence_alliance_risk",
        compound_family="coordination",
        display_name="Coordinated Hostile Contact",
        description=(
            "Anomalous co-presence patterns deviating from cohort baseline "
            "combined with historical alliance overlap with hostile entities. "
            "Suggests coordinated intelligence activity."
        ),
        required_signals={
            "copresence_anomaly": 0.25,
            "alliance_overlap_risk": 0.20,
        },
        base_weight=0.10,
        severity_default="medium",
        score_mode="min",
        confidence_mode="min_signal",
    ),

    # ── 5. Behavioral surge + rank jump ───────────────────────────────
    # A character whose behavioral risk score is elevated AND who just
    # jumped significantly in the overall rankings — rapid escalation
    # driven by behavioral patterns.
    CompoundDefinition(
        compound_type="behavioral_surge_rank_jump",
        compound_family="prioritization",
        display_name="Behavioral Surge with Rank Jump",
        description=(
            "Behavioral risk score is elevated AND the character recently "
            "jumped significantly in overall risk rankings. Indicates rapid "
            "escalation driven by small-engagement behavior patterns."
        ),
        required_signals={
            "behavioral_risk_score": 0.30,
            "suspicion_score": 0.20,
        },
        profile_conditions={
            "risk_delta_24h": {"op": ">=", "value": 0.05},
        },
        base_weight=0.10,
        severity_default="medium",
        score_mode="mean",
        confidence_mode="min_signal",
    ),
]

# Quick lookups
COMPOUND_DEF_MAP: dict[str, CompoundDefinition] = {
    c.compound_type: c for c in COMPOUND_DEFINITIONS
}
ENABLED_COMPOUNDS: list[CompoundDefinition] = [
    c for c in COMPOUND_DEFINITIONS if c.enabled
]
