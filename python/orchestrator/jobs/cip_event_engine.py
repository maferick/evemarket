"""CIP Event Engine — detects meaningful profile changes and manages event lifecycle.

Two-phase operation:
  1. **Delta detection**: scan CIP profiles for changes that exceed thresholds,
     produce candidate events with impact scores.
  2. **Event lifecycle**: upsert events (dedup on entity+type+subtype), escalate
     repeated detections, auto-resolve events whose conditions are no longer met.

Events are persistent intelligence objects, not notifications.  The same
condition re-firing updates the existing event rather than creating a duplicate.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from .cip_event_definitions import (
    EVENT_TYPE_MAP,
    PERCENTILE_BUCKETS,
    EventTypeDefinition,
    percentile_bucket,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds — tuneable constants for detection rules
# ---------------------------------------------------------------------------

# Risk score surge: minimum 24h delta to trigger
RISK_SURGE_DELTA_THRESHOLD = 0.08

# Rank jump: minimum rank improvement to trigger
RANK_JUMP_THRESHOLD = 20

# Top-N rank entry thresholds
TOP_50_RANK = 50
TOP_200_RANK = 200

# New signal: minimum weight to be considered "high weight"
HIGH_WEIGHT_SIGNAL_THRESHOLD = 0.08

# Freshness: below this, profile is considered degrading
FRESHNESS_DEGRADATION_THRESHOLD = 0.40

# Coverage expansion: minimum increase to trigger
COVERAGE_EXPANSION_THRESHOLD = 0.15

# Multi-domain: minimum domains with active signals
MULTI_DOMAIN_THRESHOLD = 4


def _now() -> datetime:
    return datetime.now(UTC)


def _now_sql() -> str:
    return _now().strftime("%Y-%m-%d %H:%M:%S")


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _dedup_key(entity_type: str, entity_id: int, event_type: str, subtype: str = "") -> str:
    return f"{entity_type}:{entity_id}:{event_type}:{subtype}"


# ---------------------------------------------------------------------------
# Impact score computation
# ---------------------------------------------------------------------------

def _compute_impact(defn: EventTypeDefinition, factors: dict[str, float]) -> float:
    """Compute impact score from event definition weights and actual factor values."""
    score = 0.0
    for factor_name, weight in defn.impact_factors.items():
        score += weight * _clamp(factors.get(factor_name, 0.0))
    return _clamp(score)


# ---------------------------------------------------------------------------
# Delta detection — scan profiles for triggerable conditions
# ---------------------------------------------------------------------------

def _detect_character_events(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Scan character_intelligence_profiles for event-worthy changes.

    Returns a list of candidate event dicts ready for upsert.
    """
    now = _now()
    now_str = _now_sql()
    candidates: list[dict[str, Any]] = []

    # Load profiles that were recently computed (within last 2 hours)
    profiles = db.fetch_all("""
        SELECT character_id, risk_score, risk_rank, risk_rank_previous,
               risk_percentile, confidence, freshness,
               signal_coverage, effective_coverage, signal_count,
               behavioral_score, graph_score, temporal_score,
               movement_score, relational_score,
               risk_score_previous_run, risk_delta_24h, risk_delta_7d,
               new_signals_24h, domain_detail_json, computed_at
        FROM character_intelligence_profiles
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)
          AND signal_count > 0
    """)

    if not profiles:
        return candidates

    # Load previous percentile buckets from yesterday's history
    yesterday_buckets: dict[int, str] = {}
    history_rows = db.fetch_all("""
        SELECT h.character_id, h.risk_percentile
        FROM character_intelligence_profile_history h
        WHERE h.snapshot_date = DATE(DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY))
    """)
    for hr in history_rows:
        yesterday_buckets[int(hr["character_id"])] = percentile_bucket(
            float(hr["risk_percentile"]) if hr.get("risk_percentile") is not None else None
        )

    # Load yesterday's effective_coverage for coverage expansion detection
    yesterday_coverage: dict[int, float] = {}
    coverage_rows = db.fetch_all("""
        SELECT cip.character_id, cip.risk_score_previous_run
        FROM character_intelligence_profiles cip
        WHERE cip.computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)
    """)
    # We need yesterday's coverage from history — but history doesn't store it yet.
    # Use risk_score_previous_run as a proxy for "something changed".
    # For coverage, we compare current effective_coverage against the profile's
    # previous state.  Since we don't have yesterday's coverage in history,
    # we detect coverage expansion when effective_coverage > 0.6 and signal_count
    # increased recently (new_signals_24h > 0).

    # Load recent high-weight signals (emitted in last 24h)
    recent_signals = db.fetch_all("""
        SELECT cis.character_id, cis.signal_type, cis.signal_value,
               cis.confidence, cis.first_seen_at, cis.last_reinforced_at,
               cis.reinforcement_count
        FROM character_intelligence_signals cis
        WHERE cis.first_seen_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
    """)
    # Group by character
    new_signals_by_char: dict[int, list[dict]] = defaultdict(list)
    for sig in recent_signals:
        new_signals_by_char[int(sig["character_id"])].append(sig)

    for prof in profiles:
        cid = int(prof["character_id"])
        risk = float(prof.get("risk_score") or 0)
        rank = prof.get("risk_rank")
        rank_prev = prof.get("risk_rank_previous")
        pct = float(prof["risk_percentile"]) if prof.get("risk_percentile") is not None else None
        conf = float(prof.get("confidence") or 0)
        fresh = float(prof.get("freshness") or 0)
        eff_cov = float(prof.get("effective_coverage") or 0)
        sig_count = int(prof.get("signal_count") or 0)
        delta_24h = float(prof.get("risk_delta_24h") or 0)
        new_24h = int(prof.get("new_signals_24h") or 0)

        # Count active domains
        domain_count = sum(1 for d in ["behavioral_score", "graph_score", "temporal_score",
                                        "movement_score", "relational_score"]
                          if float(prof.get(d) or 0) > 0)

        profile_snapshot = {
            "risk_score": risk,
            "risk_rank": rank,
            "risk_percentile": pct,
            "confidence": conf,
            "freshness": fresh,
            "effective_coverage": eff_cov,
        }

        # ── Detection: Top 50 rank entry ──
        if rank is not None and rank <= TOP_50_RANK:
            if rank_prev is None or rank_prev > TOP_50_RANK:
                defn = EVENT_TYPE_MAP["risk_rank_entry_top50"]
                impact = _compute_impact(defn, {
                    "rank_position": _clamp(1.0 - (rank / TOP_50_RANK)),
                    "risk_score": risk,
                    "effective_coverage": eff_cov,
                })
                candidates.append(_make_candidate(
                    defn, cid, impact,
                    title=f"Entered top {TOP_50_RANK} risk (rank #{rank})",
                    detail={"rank": rank, "previous_rank": rank_prev, **profile_snapshot},
                    profile=prof, now_str=now_str,
                ))

        # ── Detection: Top 200 rank entry ──
        elif rank is not None and rank <= TOP_200_RANK:
            if rank_prev is None or rank_prev > TOP_200_RANK:
                defn = EVENT_TYPE_MAP["risk_rank_entry_top200"]
                impact = _compute_impact(defn, {
                    "rank_position": _clamp(1.0 - (rank / TOP_200_RANK)),
                    "risk_score": risk,
                    "effective_coverage": eff_cov,
                })
                candidates.append(_make_candidate(
                    defn, cid, impact,
                    title=f"Entered top {TOP_200_RANK} risk (rank #{rank})",
                    detail={"rank": rank, "previous_rank": rank_prev, **profile_snapshot},
                    profile=prof, now_str=now_str,
                ))

        # ── Detection: Percentile bucket escalation ──
        if pct is not None:
            current_bucket = percentile_bucket(pct)
            prev_bucket = yesterday_buckets.get(cid, "unknown")
            if prev_bucket != "unknown" and current_bucket != prev_bucket:
                # Check if it's actually an escalation (moved to a higher bucket)
                bucket_order = [b[1] for b in PERCENTILE_BUCKETS]
                curr_idx = bucket_order.index(current_bucket) if current_bucket in bucket_order else 99
                prev_idx = bucket_order.index(prev_bucket) if prev_bucket in bucket_order else 99
                if curr_idx < prev_idx:  # Lower index = higher risk bucket
                    defn = EVENT_TYPE_MAP["percentile_escalation"]
                    jump = (prev_idx - curr_idx) / len(bucket_order)
                    impact = _compute_impact(defn, {
                        "percentile_jump": jump,
                        "risk_score": risk,
                        "confidence": conf,
                    })
                    candidates.append(_make_candidate(
                        defn, cid, impact,
                        title=f"Percentile escalation: {prev_bucket} → {current_bucket}",
                        detail={"previous_bucket": prev_bucket, "current_bucket": current_bucket,
                                "percentile": pct, **profile_snapshot},
                        profile=prof, now_str=now_str,
                    ))

        # ── Detection: Risk score surge ──
        if delta_24h >= RISK_SURGE_DELTA_THRESHOLD:
            defn = EVENT_TYPE_MAP["risk_score_surge"]
            impact = _compute_impact(defn, {
                "delta_magnitude": _clamp(delta_24h / 0.3),  # 0.3 delta = max impact
                "risk_score": risk,
                "new_signals_24h": _clamp(new_24h / 5.0),
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Risk score surged +{delta_24h:.3f} in 24h (now {risk:.3f})",
                detail={"delta_24h": delta_24h, "new_signals_24h": new_24h, **profile_snapshot},
                profile=prof, now_str=now_str,
            ))

        # ── Detection: Rank jump ──
        if rank is not None and rank_prev is not None:
            rank_improvement = rank_prev - rank  # positive = moved up
            if rank_improvement >= RANK_JUMP_THRESHOLD:
                defn = EVENT_TYPE_MAP["rank_jump"]
                impact = _compute_impact(defn, {
                    "rank_jump_magnitude": _clamp(rank_improvement / 100.0),
                    "risk_score": risk,
                    "effective_coverage": eff_cov,
                })
                candidates.append(_make_candidate(
                    defn, cid, impact,
                    title=f"Jumped {rank_improvement} positions (#{rank_prev} → #{rank})",
                    detail={"rank": rank, "previous_rank": rank_prev,
                            "jump": rank_improvement, **profile_snapshot},
                    profile=prof, now_str=now_str,
                ))

        # ── Detection: New high-weight signal ──
        char_new_signals = new_signals_by_char.get(cid, [])
        for sig in char_new_signals:
            from .cip_signal_definitions import SIGNAL_DEF_MAP
            sig_defn = SIGNAL_DEF_MAP.get(sig["signal_type"])
            if sig_defn and sig_defn.weight_default >= HIGH_WEIGHT_SIGNAL_THRESHOLD:
                # Only first-time signals (reinforcement_count = 1 means brand new)
                if int(sig.get("reinforcement_count") or 1) <= 1:
                    defn = EVENT_TYPE_MAP["new_high_weight_signal"]
                    impact = _compute_impact(defn, {
                        "signal_weight": _clamp(sig_defn.weight_default / 0.20),
                        "signal_value": _clamp(float(sig.get("signal_value") or 0)),
                        "risk_score": risk,
                    })
                    candidates.append(_make_candidate(
                        defn, cid, impact,
                        title=f"New signal: {sig_defn.display_name} ({sig['signal_type']})",
                        detail={"signal_type": sig["signal_type"],
                                "signal_value": float(sig.get("signal_value") or 0),
                                "signal_weight": sig_defn.weight_default,
                                **profile_snapshot},
                        profile=prof, now_str=now_str,
                        subtype=sig["signal_type"],
                    ))

        # ── Detection: Multi-domain activation ──
        if domain_count >= MULTI_DOMAIN_THRESHOLD:
            defn = EVENT_TYPE_MAP["multi_domain_activation"]
            impact = _compute_impact(defn, {
                "domain_count": _clamp(domain_count / 5.0),
                "risk_score": risk,
                "effective_coverage": eff_cov,
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Active across {domain_count} signal domains",
                detail={"domain_count": domain_count, **profile_snapshot},
                profile=prof, now_str=now_str,
            ))

        # ── Detection: Freshness degradation ──
        if fresh < FRESHNESS_DEGRADATION_THRESHOLD and sig_count > 3:
            defn = EVENT_TYPE_MAP["freshness_degradation"]
            impact = _compute_impact(defn, {
                "freshness_drop": _clamp(1.0 - fresh),
                "risk_score": risk,
                "signal_count": _clamp(sig_count / 15.0),
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Profile freshness degraded to {fresh:.2f}",
                detail={"freshness": fresh, "signal_count": sig_count, **profile_snapshot},
                profile=prof, now_str=now_str,
            ))

        # ── Detection: Coverage expansion ──
        if eff_cov > 0.6 and new_24h >= 2:
            defn = EVENT_TYPE_MAP["coverage_expansion"]
            impact = _compute_impact(defn, {
                "coverage_increase": _clamp(eff_cov),
                "risk_score": risk,
                "confidence": conf,
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Coverage expanded to {eff_cov:.0%} with {new_24h} new signals",
                detail={"effective_coverage": eff_cov, "new_signals_24h": new_24h,
                        **profile_snapshot},
                profile=prof, now_str=now_str,
            ))

    return candidates


def _make_candidate(
    defn: EventTypeDefinition,
    entity_id: int,
    impact: float,
    title: str,
    detail: dict[str, Any],
    profile: dict[str, Any],
    now_str: str,
    subtype: str = "",
) -> dict[str, Any]:
    """Build an event candidate dict."""
    return {
        "entity_type": defn.entity_type,
        "entity_id": entity_id,
        "event_type": defn.event_type,
        "event_subtype": subtype,
        "severity": defn.base_severity,
        "impact_score": round(impact, 4),
        "title": title,
        "detail_json": json_dumps_safe(detail),
        "dedup_key": _dedup_key(defn.entity_type, entity_id, defn.event_type, subtype),
        "now_str": now_str,
        "risk_score_at_event": float(profile.get("risk_score") or 0),
        "risk_rank_at_event": profile.get("risk_rank"),
        "risk_percentile_at_event": float(profile["risk_percentile"]) if profile.get("risk_percentile") is not None else None,
        "auto_resolve": defn.auto_resolve,
        "escalation_thresholds": defn.escalation_thresholds,
    }


# ---------------------------------------------------------------------------
# Event lifecycle management
# ---------------------------------------------------------------------------

def _upsert_events(db: SupplyCoreDb, candidates: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Upsert detected events with deduplication and escalation.

    Returns (created, updated, resolved) counts.
    """
    if not candidates:
        return 0, 0, 0

    created = 0
    updated = 0

    # Collect all dedup_keys for batch lookup
    dedup_keys = [c["dedup_key"] for c in candidates]
    placeholders = ",".join(["%s"] * len(dedup_keys))
    existing_rows = db.fetch_all(f"""
        SELECT id, dedup_key, state, severity, escalation_count
        FROM intelligence_events
        WHERE dedup_key IN ({placeholders})
    """, dedup_keys)
    existing_map: dict[str, dict] = {r["dedup_key"]: r for r in existing_rows}

    for cand in candidates:
        dk = cand["dedup_key"]
        existing = existing_map.get(dk)

        if existing is None:
            # New event — insert
            db.execute("""
                INSERT INTO intelligence_events (
                    entity_type, entity_id, event_type, event_subtype,
                    state, severity, impact_score,
                    title, detail_json, dedup_key,
                    first_detected_at, last_updated_at,
                    escalation_count,
                    risk_score_at_event, risk_rank_at_event, risk_percentile_at_event
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s)
            """, [
                cand["entity_type"], cand["entity_id"],
                cand["event_type"], cand["event_subtype"],
                "active", cand["severity"], cand["impact_score"],
                cand["title"], cand["detail_json"], dk,
                cand["now_str"], cand["now_str"],
                cand["risk_score_at_event"], cand["risk_rank_at_event"],
                cand["risk_percentile_at_event"],
            ])
            created += 1
        else:
            # Existing event — update with escalation logic
            new_count = int(existing["escalation_count"] or 0) + 1
            current_severity = existing["severity"]
            new_severity = current_severity

            # Check escalation thresholds
            thresholds = cand.get("escalation_thresholds", {})
            for threshold_count, upgraded_severity in sorted(thresholds.items()):
                if new_count >= threshold_count:
                    new_severity = upgraded_severity

            severity_changed = new_severity != current_severity

            # Re-activate if it was resolved/expired
            new_state = "active" if existing["state"] in ("resolved", "expired") else existing["state"]

            db.execute("""
                UPDATE intelligence_events
                SET state            = %s,
                    severity         = %s,
                    previous_severity = CASE WHEN %s != severity THEN severity ELSE previous_severity END,
                    impact_score     = %s,
                    title            = %s,
                    detail_json      = %s,
                    last_updated_at  = %s,
                    escalation_count = %s,
                    risk_score_at_event     = %s,
                    risk_rank_at_event      = %s,
                    risk_percentile_at_event = %s,
                    resolved_at      = NULL
                WHERE id = %s
            """, [
                new_state, new_severity, new_severity,
                cand["impact_score"], cand["title"], cand["detail_json"],
                cand["now_str"], new_count,
                cand["risk_score_at_event"], cand["risk_rank_at_event"],
                cand["risk_percentile_at_event"],
                existing["id"],
            ])

            # Log state/severity transitions
            if severity_changed or new_state != existing["state"]:
                db.execute("""
                    INSERT INTO intelligence_event_history (
                        event_id, previous_state, new_state,
                        previous_severity, new_severity,
                        changed_by, reason
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, [
                    existing["id"],
                    existing["state"], new_state,
                    current_severity, new_severity,
                    "cip_event_engine",
                    f"Re-detected (count={new_count})" + (f", escalated to {new_severity}" if severity_changed else ""),
                ])

            updated += 1

    # Auto-resolve: find active events whose conditions are no longer met
    resolved = _auto_resolve_events(db, candidates)

    return created, updated, resolved


def _auto_resolve_events(db: SupplyCoreDb, current_candidates: list[dict[str, Any]]) -> int:
    """Resolve active events whose conditions are no longer true.

    An auto-resolvable event is resolved if it was NOT re-detected in the
    current scan (i.e., its dedup_key is not in the candidate set).
    """
    now_str = _now_sql()
    active_dedup_keys = {c["dedup_key"] for c in current_candidates}

    # Get all active auto-resolvable event types
    auto_resolve_types = [et.event_type for et in EVENT_TYPE_MAP.values() if et.auto_resolve]
    if not auto_resolve_types:
        return 0

    placeholders = ",".join(["%s"] * len(auto_resolve_types))
    active_events = db.fetch_all(f"""
        SELECT id, dedup_key, state, severity, event_type
        FROM intelligence_events
        WHERE state = 'active'
          AND event_type IN ({placeholders})
          AND last_updated_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR)
    """, auto_resolve_types)

    resolved = 0
    for evt in active_events:
        if evt["dedup_key"] not in active_dedup_keys:
            db.execute("""
                UPDATE intelligence_events
                SET state = 'resolved', resolved_at = %s, last_updated_at = %s
                WHERE id = %s AND state = 'active'
            """, [now_str, now_str, evt["id"]])

            db.execute("""
                INSERT INTO intelligence_event_history (
                    event_id, previous_state, new_state,
                    previous_severity, new_severity,
                    changed_by, reason
                ) VALUES (%s, 'active', 'resolved', %s, %s, 'cip_event_engine', 'Condition no longer detected')
            """, [evt["id"], evt["severity"], evt["severity"]])

            resolved += 1

    return resolved


# ---------------------------------------------------------------------------
# Expire old events (housekeeping)
# ---------------------------------------------------------------------------

def _expire_stale_events(db: SupplyCoreDb) -> int:
    """Expire events that have been resolved for over 30 days or
    active but not updated for over 14 days.
    """
    now_str = _now_sql()

    # Expire resolved events older than 30 days
    result1 = db.execute("""
        UPDATE intelligence_events
        SET state = 'expired', last_updated_at = %s
        WHERE state = 'resolved'
          AND resolved_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
    """, [now_str])

    # Expire active events not updated in 14 days (stale detections)
    result2 = db.execute("""
        UPDATE intelligence_events
        SET state = 'expired', resolved_at = %s, last_updated_at = %s
        WHERE state = 'active'
          AND last_updated_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 14 DAY)
    """, [now_str, now_str])

    count1 = result1 if isinstance(result1, int) else 0
    count2 = result2 if isinstance(result2, int) else 0
    return count1 + count2


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_cip_event_engine(db: SupplyCoreDb) -> JobResult:
    """Detect profile changes, manage event lifecycle, and surface intelligence."""
    job = start_job_run(db, "cip_event_engine")
    t0 = time.monotonic()

    # 1. Delta detection
    logger.info("cip_event_engine: scanning for events...")
    candidates = _detect_character_events(db)
    logger.info("cip_event_engine: detected %d candidate events", len(candidates))

    # 2. Event lifecycle management
    logger.info("cip_event_engine: upserting events...")
    created, updated, resolved = _upsert_events(db, candidates)
    logger.info("cip_event_engine: created=%d, updated=%d, resolved=%d", created, updated, resolved)

    # 3. Housekeeping
    expired = _expire_stale_events(db)
    if expired:
        logger.info("cip_event_engine: expired %d stale events", expired)

    elapsed = int((time.monotonic() - t0) * 1000)
    total_actions = created + updated + resolved + expired

    finish_job_run(db, job, status="success",
                   rows_processed=len(candidates), rows_written=total_actions,
                   meta={"created": created, "updated": updated,
                         "resolved": resolved, "expired": expired})

    return JobResult(
        status="success",
        summary=f"Events: {created} created, {updated} updated, {resolved} resolved, {expired} expired",
        started_at=job.started_at, finished_at=job.finished_at,
        duration_ms=elapsed, rows_seen=len(candidates),
        rows_processed=len(candidates), rows_written=total_actions,
        rows_skipped=0, rows_failed=0, batches_completed=1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={"created": created, "updated": updated,
              "resolved": resolved, "expired": expired},
    )
