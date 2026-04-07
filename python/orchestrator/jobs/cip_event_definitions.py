"""Intelligence event type definitions and detection rules.

Each event type defines:
  - What condition triggers it (a SQL-expressible or Python-evaluable check)
  - What severity it starts at
  - How impact score is computed
  - How it escalates on repeated detection
  - When it auto-resolves (condition no longer met)

This is the Phase 2 starting point: simple threshold-based rules evaluated
against CIP profiles.  No temporal sequencing or compound signals yet —
those come in Phase 4.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class EventTypeDefinition:
    event_type: str
    entity_type: str            # character, alliance, theater
    display_name: str
    description: str
    base_severity: str          # critical, high, medium, low, info
    # Impact scoring weights (how the impact_score is computed)
    # impact = sum(factor_weight * factor_value) clamped to [0,1]
    impact_factors: dict[str, float]
    # Escalation: severity upgrades after N consecutive detections
    escalation_thresholds: dict[int, str]  # count → new severity
    # Auto-resolve: event is resolved when condition is no longer true
    auto_resolve: bool


# ---------------------------------------------------------------------------
# Character event types
# ---------------------------------------------------------------------------

CHARACTER_EVENT_TYPES: list[EventTypeDefinition] = [
    # ── Risk rank / percentile events ──────────────────────────────────
    EventTypeDefinition(
        event_type="risk_rank_entry_top50",
        entity_type="character",
        display_name="Entered Top 50 Risk",
        description="Character entered the top 50 risk-ranked profiles",
        base_severity="high",
        impact_factors={
            "rank_position": 0.4,       # lower rank → higher impact
            "risk_score": 0.3,
            "effective_coverage": 0.3,   # high coverage = trustworthy signal
        },
        escalation_thresholds={3: "critical"},
        auto_resolve=True,
    ),
    EventTypeDefinition(
        event_type="risk_rank_entry_top200",
        entity_type="character",
        display_name="Entered Top 200 Risk",
        description="Character entered the top 200 risk-ranked profiles",
        base_severity="medium",
        impact_factors={
            "rank_position": 0.3,
            "risk_score": 0.3,
            "effective_coverage": 0.4,
        },
        escalation_thresholds={5: "high"},
        auto_resolve=True,
    ),
    EventTypeDefinition(
        event_type="percentile_escalation",
        entity_type="character",
        display_name="Risk Percentile Escalation",
        description="Character crossed into a higher risk percentile bucket (e.g. top 5% → top 1%)",
        base_severity="high",
        impact_factors={
            "percentile_jump": 0.5,     # magnitude of bucket change
            "risk_score": 0.3,
            "confidence": 0.2,
        },
        escalation_thresholds={3: "critical"},
        auto_resolve=True,
    ),

    # ── Score movement events ──────────────────────────────────────────
    EventTypeDefinition(
        event_type="risk_score_surge",
        entity_type="character",
        display_name="Risk Score Surge",
        description="Risk score increased significantly in 24 hours",
        base_severity="medium",
        impact_factors={
            "delta_magnitude": 0.4,
            "risk_score": 0.3,
            "new_signals_24h": 0.3,
        },
        escalation_thresholds={3: "high", 7: "critical"},
        auto_resolve=True,
    ),
    EventTypeDefinition(
        event_type="rank_jump",
        entity_type="character",
        display_name="Significant Rank Jump",
        description="Character jumped significantly in risk rankings",
        base_severity="medium",
        impact_factors={
            "rank_jump_magnitude": 0.5,
            "risk_score": 0.3,
            "effective_coverage": 0.2,
        },
        escalation_thresholds={3: "high"},
        auto_resolve=True,
    ),

    # ── Signal events ──────────────────────────────────────────────────
    EventTypeDefinition(
        event_type="new_high_weight_signal",
        entity_type="character",
        display_name="New High-Weight Signal",
        description="A new signal with significant weight appeared for this character",
        base_severity="medium",
        impact_factors={
            "signal_weight": 0.4,
            "signal_value": 0.3,
            "risk_score": 0.3,
        },
        escalation_thresholds={},
        auto_resolve=False,
    ),
    EventTypeDefinition(
        event_type="multi_domain_activation",
        entity_type="character",
        display_name="Multi-Domain Activation",
        description="Character now has active signals across 4+ domains simultaneously",
        base_severity="high",
        impact_factors={
            "domain_count": 0.4,
            "risk_score": 0.3,
            "effective_coverage": 0.3,
        },
        escalation_thresholds={3: "critical"},
        auto_resolve=True,
    ),

    # ── Trust surface events ───────────────────────────────────────────
    EventTypeDefinition(
        event_type="freshness_degradation",
        entity_type="character",
        display_name="Profile Freshness Degradation",
        description="Profile freshness dropped below trust threshold — signals are going stale",
        base_severity="info",
        impact_factors={
            "freshness_drop": 0.5,
            "risk_score": 0.3,
            "signal_count": 0.2,
        },
        escalation_thresholds={},
        auto_resolve=True,
    ),
    EventTypeDefinition(
        event_type="coverage_expansion",
        entity_type="character",
        display_name="Coverage Expansion",
        description="Effective coverage materially increased — more signal domains contributing",
        base_severity="info",
        impact_factors={
            "coverage_increase": 0.5,
            "risk_score": 0.3,
            "confidence": 0.2,
        },
        escalation_thresholds={},
        auto_resolve=False,
    ),
]

# Quick lookup
EVENT_TYPE_MAP: dict[str, EventTypeDefinition] = {e.event_type: e for e in CHARACTER_EVENT_TYPES}

# Percentile bucket boundaries (for bucket change detection)
PERCENTILE_BUCKETS: list[tuple[float, str]] = [
    (0.99, "top_1pct"),
    (0.95, "top_5pct"),
    (0.90, "top_10pct"),
    (0.75, "top_25pct"),
    (0.50, "top_50pct"),
    (0.00, "below_50pct"),
]


def percentile_bucket(pct: float | None) -> str:
    """Return the bucket label for a given percentile value."""
    if pct is None:
        return "unknown"
    for threshold, label in PERCENTILE_BUCKETS:
        if pct >= threshold:
            return label
    return "below_50pct"
