"""Display-tier classification for pipeline jobs (T1–T6).

These tiers are the same functional grouping shown in the log-viewer UI and
in the scheduler's tier overview (Data Ingestion, Resolution, Graph,
Intelligence, Analytics, Maintenance).  They are **distinct** from the
topologically-derived execution tiers that ``scheduling_graph.py`` computes
from the DAG: those are used to enforce dependency ordering, while these
are used to classify jobs by functional role for dispatcher fairness.

The authoritative tier assignment lives in PHP (``src/functions.php`` →
``automation_runtime_job_tier``).  This module mirrors it.  If the two
ever drift, ``validate_display_tier_parity`` can be used at startup to
surface the discrepancy.

Typical use:

    from orchestrator.display_tiers import get_display_tier, parse_tier_slots

    tier = get_display_tier("market_hub_current_sync")   # -> 1
    slots = parse_tier_slots("1:2,3:2")                  # -> {1: 2, 3: 2}
"""

from __future__ import annotations

from typing import Iterable

# Default tier for any job not explicitly classified.  Matches the PHP
# fallback (`$tiers[$jobKey] ?? 5`).
DEFAULT_DISPLAY_TIER: int = 5

# Display-tier labels (for log output).
DISPLAY_TIER_LABELS: dict[int, str] = {
    1: "Tier 1 — Data Ingestion",
    2: "Tier 2 — Resolution",
    3: "Tier 3 — Graph",
    4: "Tier 4 — Intelligence",
    5: "Tier 5 — Analytics",
    6: "Tier 6 — Maintenance",
}

# Authoritative tier mapping, mirrored from ``automation_runtime_job_tier``
# in src/functions.php.  Keep in sync when adding new jobs on either side.
DISPLAY_TIERS: dict[str, int] = {
    # ── Tier 1: Data Ingestion ──────────────────────────────────────────
    "market_hub_current_sync": 1,
    "market_hub_historical_sync": 1,
    "market_hub_local_history_sync": 1,
    "alliance_current_sync": 1,
    "alliance_historical_sync": 1,
    "current_state_refresh_sync": 1,
    "esi_character_queue_sync": 1,
    "esi_affiliation_sync": 1,
    "character_killmail_sync": 1,
    "evewho_enrichment_sync": 1,
    "evewho_alliance_member_sync": 1,
    "tracked_alliance_member_sync": 1,
    "corp_standings_sync": 1,
    "sovereignty_campaigns_sync": 1,
    "sovereignty_structures_sync": 1,
    "sovereignty_map_sync": 1,
    "jump_bridge_sync": 1,

    # ── Tier 2: Resolution & Enrichment ─────────────────────────────────
    "entity_metadata_resolve_sync": 2,
    "esi_alliance_history_sync": 2,
    "compute_signals": 2,
    "deal_alerts_sync": 2,

    # ── Tier 3: Graph & Computation ─────────────────────────────────────
    "compute_graph_sync": 3,
    "compute_graph_insights": 3,
    "compute_graph_derived_relationships": 3,
    "compute_graph_sync_killmail_entities": 3,
    "compute_graph_sync_killmail_edges": 3,
    "compute_graph_sync_doctrine_dependency": 3,
    "compute_graph_sync_battle_intelligence": 3,
    "compute_graph_prune": 3,
    "compute_graph_topology_metrics": 3,
    "graph_data_quality_check": 3,
    "graph_temporal_metrics_sync": 3,
    "graph_typed_interactions_sync": 3,
    "graph_community_detection_sync": 3,
    "graph_motif_detection_sync": 3,
    "graph_evidence_paths_sync": 3,
    "graph_analyst_recalibration": 3,
    "graph_model_audit": 3,
    "graph_query_plan_validation": 3,
    "neo4j_ml_exploration": 3,
    "graph_universe_sync": 3,
    "compute_copresence_edges": 3,

    # ── Tier 4: Intelligence ────────────────────────────────────────────
    "compute_suspicion_scores_v2": 4,
    "compute_alliance_dossiers": 4,
    "compute_threat_corridors": 4,
    "compute_counterintel_pipeline": 4,
    "intelligence_pipeline": 4,
    "compute_battle_rollups": 4,
    "compute_behavioral_scoring": 4,
    "compute_sovereignty_alerts": 4,
    "compute_alliance_relationships": 4,
    "staging_system_detection": 4,
    "theater_clustering": 4,
    "theater_analysis": 4,
    "theater_graph_integration": 4,
    "theater_suspicion": 4,
    "temporal_behavior_detection": 4,
    "compute_character_feature_windows": 4,
    "shell_corp_detection": 4,
    "pre_op_join_detection": 4,
    "compute_cohort_baselines": 4,

    # ── Tier 5: Analytics & Output ──────────────────────────────────────
    "dashboard_summary_sync": 5,
    "activity_priority_summary_sync": 5,
    "analytics_bucket_1h_sync": 5,
    "analytics_bucket_1d_sync": 5,
    "rebuild_ai_briefings": 5,
    "forecasting_ai_sync": 5,
    "market_comparison_summary_sync": 5,
    "loss_demand_summary_sync": 5,
    "compute_economic_warfare": 5,
    "discord_webhook_filter": 5,

    # ── Tier 6: Maintenance ─────────────────────────────────────────────
    "cache_expiry_cleanup_sync": 6,
    "killmail_zkb_repair": 6,
    "log_to_issues": 6,
}

VALID_DISPLAY_TIERS: frozenset[int] = frozenset(DISPLAY_TIER_LABELS.keys())


def get_display_tier(job_key: str) -> int:
    """Return the display tier (1–6) for *job_key*.

    Jobs not present in ``DISPLAY_TIERS`` fall through to
    ``DEFAULT_DISPLAY_TIER`` (5 = analytics), matching the PHP side.
    """
    return DISPLAY_TIERS.get(job_key, DEFAULT_DISPLAY_TIER)


def parse_tier_slots(spec: str, max_parallel: int) -> dict[int, int]:
    """Parse a ``--tier-slots`` CLI spec into a ``{tier: slots}`` dict.

    Format: comma-separated ``tier:slots`` pairs, e.g. ``"1:2,3:2"`` means
    reserve at least 2 slots each for tiers 1 and 3.

    Validation:
      - Empty string returns ``{}`` (feature disabled).
      - Tier must be 1–6 (a known display tier).
      - Slot count must be a non-negative integer.
      - Sum of reservations must not exceed *max_parallel* (otherwise there
        would be no overflow pool for non-reserved tiers).

    Raises ``ValueError`` on any format or bounds violation.
    """
    if not spec or not spec.strip():
        return {}

    result: dict[int, int] = {}
    for chunk in spec.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if ":" not in chunk:
            raise ValueError(
                f"--tier-slots: invalid entry {chunk!r}, expected 'tier:slots'"
            )
        tier_str, _, slots_str = chunk.partition(":")
        try:
            tier = int(tier_str.strip())
            slots = int(slots_str.strip())
        except ValueError as exc:
            raise ValueError(
                f"--tier-slots: entry {chunk!r} must have integer tier and slot count"
            ) from exc
        if tier not in VALID_DISPLAY_TIERS:
            raise ValueError(
                f"--tier-slots: unknown tier {tier} (valid: {sorted(VALID_DISPLAY_TIERS)})"
            )
        if slots < 0:
            raise ValueError(
                f"--tier-slots: tier {tier} has negative slot count {slots}"
            )
        if tier in result:
            raise ValueError(
                f"--tier-slots: tier {tier} specified more than once"
            )
        result[tier] = slots

    total_reserved = sum(result.values())
    if total_reserved > max_parallel:
        raise ValueError(
            f"--tier-slots: total reserved slots {total_reserved} exceeds "
            f"--max-parallel {max_parallel}; leave at least 1 slot for the "
            f"shared overflow pool"
        )

    return result


def tier_capacity_allows(
    job_key: str,
    in_flight_by_tier: dict[int, int],
    in_flight_total: int,
    max_parallel: int,
    reserved_slots: dict[int, int],
) -> bool:
    """Return ``True`` if dispatching *job_key* respects tier reservations.

    Semantics (dead-simple "reserved + shared pool" model):

    * Each reserved tier T has a guaranteed allocation of ``reserved_slots[T]``
      slots.  Its first ``reserved_slots[T]`` in-flight jobs come out of its
      own reservation and do not touch the shared pool.
    * Any additional jobs from a reserved tier (beyond its reservation) use
      the shared pool.
    * Jobs from non-reserved tiers always use the shared pool.
    * Shared pool size = ``max_parallel - sum(reserved_slots.values())``.
    * A dispatch is allowed iff total in-flight is under ``max_parallel``
      and the job either fits its tier's reservation or the shared pool
      has free capacity.

    This guarantees: a reserved tier always has room for its reservation
    when it has ready work, even if the shared pool is saturated.  In
    exchange, non-reserved (and over-reserved) tiers are capped at the
    shared pool size, so a flood from one tier cannot starve the others.
    """
    if in_flight_total >= max_parallel:
        return False

    if not reserved_slots:
        # Feature disabled — behave exactly like the old scheduler.
        return True

    tier = get_display_tier(job_key)
    tier_reserved = reserved_slots.get(tier, 0)
    tier_in_flight = in_flight_by_tier.get(tier, 0)

    if tier_in_flight < tier_reserved:
        # Dispatch comes out of this tier's reservation — always allowed.
        return True

    # Dispatch comes out of the shared pool.
    shared_pool_used = 0
    for t, count in in_flight_by_tier.items():
        r = reserved_slots.get(t, 0)
        if count > r:
            shared_pool_used += count - r
    shared_pool_size = max_parallel - sum(reserved_slots.values())
    return shared_pool_used < shared_pool_size


def validate_display_tier_parity(
    php_tier_map: dict[str, int],
) -> list[str]:
    """Return a list of jobs whose PHP display tier disagrees with Python.

    Intended for a CI / startup audit check; accepts the PHP-side mapping
    (however it's loaded — JSON export, bridge call, etc.) and flags any
    divergence from ``DISPLAY_TIERS``.  An empty list means the two are
    in sync.  Jobs present in one side but not the other are also flagged.
    """
    issues: list[str] = []
    py_keys = set(DISPLAY_TIERS.keys())
    php_keys = set(php_tier_map.keys())

    for key in sorted(py_keys & php_keys):
        py = DISPLAY_TIERS[key]
        php = php_tier_map[key]
        if py != php:
            issues.append(f"{key}: python tier {py} != php tier {php}")

    for key in sorted(py_keys - php_keys):
        issues.append(f"{key}: present in python but not in php")
    for key in sorted(php_keys - py_keys):
        issues.append(f"{key}: present in php but not in python")

    return issues


def describe_tier_slots(
    reserved_slots: dict[int, int],
    max_parallel: int,
) -> str:
    """Render a human-readable summary of an active reservation config."""
    if not reserved_slots:
        return f"tier reservations: disabled (shared pool = {max_parallel})"
    parts: list[str] = []
    for tier in sorted(reserved_slots.keys()):
        label = DISPLAY_TIER_LABELS.get(tier, f"Tier {tier}")
        parts.append(f"T{tier} reserved={reserved_slots[tier]} ({label})")
    shared = max_parallel - sum(reserved_slots.values())
    parts.append(f"shared pool={shared}")
    return "tier reservations: " + ", ".join(parts)


def _iter_known_tiers() -> Iterable[int]:
    """Iterate through display tiers in ascending order (for stable logs)."""
    return sorted(VALID_DISPLAY_TIERS)
