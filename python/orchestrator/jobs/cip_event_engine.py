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

import json
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
from .cip_calibration import load_calibrated_thresholds, priority_band

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds — fallback constants, overridden by calibration when available
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

def _compute_impact(defn: EventTypeDefinition, factors: dict[str, float]) -> tuple[float, dict[str, float]]:
    """Compute impact score from event definition weights and actual factor values.

    Returns (total_score, decomposition) where decomposition maps factor names
    to their individual contributions.
    """
    decomposition: dict[str, float] = {}
    score = 0.0
    for factor_name, weight in defn.impact_factors.items():
        contribution = weight * _clamp(factors.get(factor_name, 0.0))
        decomposition[factor_name] = round(contribution, 4)
        score += contribution
    return _clamp(score), decomposition


# ---------------------------------------------------------------------------
# Delta detection — scan profiles for triggerable conditions
# ---------------------------------------------------------------------------

def _detect_character_events(db: SupplyCoreDb, cal: dict | None = None) -> list[dict[str, Any]]:
    """Scan character_intelligence_profiles for event-worthy changes.

    Uses calibrated thresholds from `cal` when available, falling back
    to module-level constants.

    Returns a list of candidate event dicts ready for upsert.
    """
    now = _now()
    now_str = _now_sql()
    candidates: list[dict[str, Any]] = []

    # Use calibrated thresholds if available
    surge_threshold = cal["surge_delta"] if cal else RISK_SURGE_DELTA_THRESHOLD
    rank_jump_threshold = cal["rank_jump"] if cal else RANK_JUMP_THRESHOLD
    freshness_threshold = cal["freshness_floor"] if cal else FRESHNESS_DEGRADATION_THRESHOLD

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
                impact, decomp = _compute_impact(defn, {
                    "rank_position": _clamp(1.0 - (rank / TOP_50_RANK)),
                    "risk_score": risk,
                    "effective_coverage": eff_cov,
                })
                candidates.append(_make_candidate(
                    defn, cid, impact,
                    title=f"Entered top {TOP_50_RANK} risk (rank #{rank})",
                    detail={"rank": rank, "previous_rank": rank_prev, **profile_snapshot},
                    profile=prof, now_str=now_str,
                    impact_decomposition=decomp,
                    threshold_info={"threshold": f"rank <= {TOP_50_RANK}", "actual": rank,
                                    "margin": TOP_50_RANK - rank, "hysteresis": defn.hysteresis_margin},
                ))

        # ── Detection: Top 200 rank entry ──
        elif rank is not None and rank <= TOP_200_RANK:
            if rank_prev is None or rank_prev > TOP_200_RANK:
                defn = EVENT_TYPE_MAP["risk_rank_entry_top200"]
                impact, decomp = _compute_impact(defn, {
                    "rank_position": _clamp(1.0 - (rank / TOP_200_RANK)),
                    "risk_score": risk,
                    "effective_coverage": eff_cov,
                })
                candidates.append(_make_candidate(
                    defn, cid, impact,
                    title=f"Entered top {TOP_200_RANK} risk (rank #{rank})",
                    detail={"rank": rank, "previous_rank": rank_prev, **profile_snapshot},
                    profile=prof, now_str=now_str,
                    impact_decomposition=decomp,
                    threshold_info={"threshold": f"rank <= {TOP_200_RANK}", "actual": rank,
                                    "margin": TOP_200_RANK - rank, "hysteresis": defn.hysteresis_margin},
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
                    impact, decomp = _compute_impact(defn, {
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
                        impact_decomposition=decomp,
                        threshold_info={"threshold": f"bucket escalation", "previous_bucket": prev_bucket,
                                        "current_bucket": current_bucket, "hysteresis": defn.hysteresis_margin},
                    ))

        # ── Detection: Risk score surge ──
        if delta_24h >= surge_threshold:
            defn = EVENT_TYPE_MAP["risk_score_surge"]
            impact, decomp = _compute_impact(defn, {
                "delta_magnitude": _clamp(delta_24h / 0.3),
                "risk_score": risk,
                "new_signals_24h": _clamp(new_24h / 5.0),
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Risk score surged +{delta_24h:.3f} in 24h (now {risk:.3f})",
                detail={"delta_24h": delta_24h, "new_signals_24h": new_24h, **profile_snapshot},
                profile=prof, now_str=now_str,
                impact_decomposition=decomp,
                threshold_info={"threshold": f"delta_24h >= {surge_threshold:.4f} (calibrated)",
                                "actual": round(delta_24h, 4),
                                "margin": round(delta_24h - surge_threshold, 4)},
            ))

        # ── Detection: Rank jump ──
        if rank is not None and rank_prev is not None:
            rank_improvement = rank_prev - rank  # positive = moved up
            if rank_improvement >= rank_jump_threshold:
                defn = EVENT_TYPE_MAP["rank_jump"]
                impact, decomp = _compute_impact(defn, {
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
                    impact_decomposition=decomp,
                    threshold_info={"threshold": f"rank_improvement >= {rank_jump_threshold} (calibrated)",
                                    "actual": rank_improvement,
                                    "margin": rank_improvement - rank_jump_threshold},
                ))

        # ── Detection: New high-weight signal ──
        # Gated: signal must also have minimum confidence and the profile
        # must have reasonable freshness.  This prevents noisy events from
        # low-confidence signals or stale profiles.
        char_new_signals = new_signals_by_char.get(cid, [])
        for sig in char_new_signals:
            from .cip_signal_definitions import SIGNAL_DEF_MAP
            sig_defn = SIGNAL_DEF_MAP.get(sig["signal_type"])
            if sig_defn and sig_defn.weight_default >= HIGH_WEIGHT_SIGNAL_THRESHOLD:
                sig_confidence = float(sig.get("confidence") or 0)
                # Gates: minimum signal confidence + minimum profile freshness
                if sig_confidence < 0.4 or fresh < 0.3:
                    continue
                # Only first-time signals (reinforcement_count = 1 means brand new)
                if int(sig.get("reinforcement_count") or 1) <= 1:
                    defn = EVENT_TYPE_MAP["new_high_weight_signal"]
                    impact, decomp = _compute_impact(defn, {
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
                                "signal_confidence": sig_confidence,
                                **profile_snapshot},
                        profile=prof, now_str=now_str,
                        subtype=sig["signal_type"],
                        impact_decomposition=decomp,
                        threshold_info={"threshold": f"weight >= {HIGH_WEIGHT_SIGNAL_THRESHOLD}",
                                        "actual": sig_defn.weight_default,
                                        "gates": f"confidence >= 0.4 (actual: {sig_confidence:.2f}), freshness >= 0.3 (actual: {fresh:.2f})"},
                    ))

        # ── Detection: Multi-domain activation ──
        if domain_count >= MULTI_DOMAIN_THRESHOLD:
            defn = EVENT_TYPE_MAP["multi_domain_activation"]
            impact, decomp = _compute_impact(defn, {
                "domain_count": _clamp(domain_count / 5.0),
                "risk_score": risk,
                "effective_coverage": eff_cov,
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Active across {domain_count} signal domains",
                detail={"domain_count": domain_count, **profile_snapshot},
                profile=prof, now_str=now_str,
                impact_decomposition=decomp,
                threshold_info={"threshold": f"domains >= {MULTI_DOMAIN_THRESHOLD}",
                                "actual": domain_count, "margin": domain_count - MULTI_DOMAIN_THRESHOLD},
            ))

        # ── Detection: Freshness degradation ──
        if fresh < freshness_threshold and sig_count > 3:
            defn = EVENT_TYPE_MAP["freshness_degradation"]
            impact, decomp = _compute_impact(defn, {
                "freshness_drop": _clamp(1.0 - fresh),
                "risk_score": risk,
                "signal_count": _clamp(sig_count / 15.0),
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Profile freshness degraded to {fresh:.2f}",
                detail={"freshness": fresh, "signal_count": sig_count, **profile_snapshot},
                profile=prof, now_str=now_str,
                impact_decomposition=decomp,
                threshold_info={"threshold": f"freshness < {freshness_threshold:.2f} (calibrated)",
                                "actual": round(fresh, 4),
                                "margin": round(freshness_threshold - fresh, 4),
                                "hysteresis": defn.hysteresis_margin},
            ))

        # ── Detection: Coverage expansion ──
        if eff_cov > 0.6 and new_24h >= 2:
            defn = EVENT_TYPE_MAP["coverage_expansion"]
            impact, decomp = _compute_impact(defn, {
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
                impact_decomposition=decomp,
                threshold_info={"threshold": "coverage > 0.6 AND new_signals >= 2",
                                "actual_coverage": round(eff_cov, 4), "actual_new_signals": new_24h},
            ))

    return candidates


def _detect_compound_events(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Detect events from newly activated or strengthened compound signals.

    Scans character_intelligence_compound_signals for:
      1. Newly materialized compounds (first_detected_at within last 24h)
      2. Significantly strengthened compounds (score increase)
    """
    from .cip_compound_definitions import COMPOUND_DEF_MAP

    now_str = _now_sql()
    candidates: list[dict[str, Any]] = []

    # Load recently evaluated compound signals
    compounds = db.fetch_all("""
        SELECT cics.character_id, cics.compound_type, cics.score,
               cics.confidence, cics.evidence_json, cics.first_detected_at,
               cics.last_evaluated_at
        FROM character_intelligence_compound_signals cics
        WHERE cics.last_evaluated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)
    """)

    if not compounds:
        return candidates

    # Load profiles for these characters
    char_ids = list({int(c["character_id"]) for c in compounds})
    placeholders = ",".join(["%s"] * len(char_ids))
    profiles = db.fetch_all(f"""
        SELECT character_id, risk_score, risk_rank, risk_percentile,
               confidence, freshness, effective_coverage
        FROM character_intelligence_profiles
        WHERE character_id IN ({placeholders})
    """, char_ids)
    profiles_by_id = {int(p["character_id"]): p for p in profiles}

    # Load existing compound events to detect strengthening
    existing_compound_events = db.fetch_all("""
        SELECT dedup_key, impact_score
        FROM intelligence_events
        WHERE event_type IN ('compound_signal_activated', 'compound_signal_strengthened')
          AND state IN ('active', 'acknowledged', 'suppressed')
    """)
    existing_impact: dict[str, float] = {
        r["dedup_key"]: float(r["impact_score"]) for r in existing_compound_events
    }

    for comp in compounds:
        cid = int(comp["character_id"])
        ctype = comp["compound_type"]
        score = float(comp.get("score") or 0)
        conf = float(comp.get("confidence") or 0)
        first_detected = str(comp.get("first_detected_at") or "")

        defn_compound = COMPOUND_DEF_MAP.get(ctype)
        if defn_compound is None:
            continue

        prof = profiles_by_id.get(cid)
        if prof is None:
            continue

        risk = float(prof.get("risk_score") or 0)
        profile_snapshot = {
            "risk_score": risk,
            "risk_rank": prof.get("risk_rank"),
            "risk_percentile": float(prof["risk_percentile"]) if prof.get("risk_percentile") is not None else None,
            "confidence": float(prof.get("confidence") or 0),
            "freshness": float(prof.get("freshness") or 0),
            "effective_coverage": float(prof.get("effective_coverage") or 0),
        }

        # Parse evidence
        try:
            evidence_list = json.loads(comp.get("evidence_json") or "[]")
        except (json.JSONDecodeError, TypeError):
            evidence_list = []

        # Check if this is a NEW compound (first detected in last 24h)
        is_new = False
        if first_detected:
            from datetime import datetime as dt
            try:
                fd = dt.strptime(first_detected[:19], "%Y-%m-%d %H:%M:%S")
                age_hours = (datetime.now(UTC).replace(tzinfo=None) - fd).total_seconds() / 3600
                is_new = age_hours <= 24
            except (ValueError, TypeError):
                pass

        if is_new:
            defn = EVENT_TYPE_MAP["compound_signal_activated"]
            impact, decomp = _compute_impact(defn, {
                "compound_score": _clamp(score),
                "compound_confidence": _clamp(conf),
                "risk_score": risk,
            })
            candidates.append(_make_candidate(
                defn, cid, impact,
                title=f"Compound: {defn_compound.display_name} (score {score:.3f})",
                detail={
                    "compound_type": ctype,
                    "compound_score": score,
                    "compound_confidence": conf,
                    "contributing_signals": evidence_list,
                    **profile_snapshot,
                },
                profile=prof, now_str=now_str,
                subtype=ctype,
                impact_decomposition=decomp,
                threshold_info={
                    "type": "compound_activation",
                    "required_signals": dict(defn_compound.required_signals),
                    "score_mode": defn_compound.score_mode,
                },
            ))
        else:
            # Check for significant strengthening
            dedup = _dedup_key("character", cid, "compound_signal_activated", ctype)
            prev_impact = existing_impact.get(dedup, 0.0)

            # Also check the strengthened dedup key
            dedup_str = _dedup_key("character", cid, "compound_signal_strengthened", ctype)
            prev_str_impact = existing_impact.get(dedup_str, 0.0)

            defn = EVENT_TYPE_MAP["compound_signal_strengthened"]
            impact, decomp = _compute_impact(defn, {
                "score_increase": _clamp(score * 0.5),  # conservative
                "compound_score": _clamp(score),
                "risk_score": risk,
            })

            # Only fire if impact exceeds previous by meaningful margin
            if impact > max(prev_impact, prev_str_impact) * 1.1 + 0.05:
                candidates.append(_make_candidate(
                    defn, cid, impact,
                    title=f"Compound strengthened: {defn_compound.display_name} (score {score:.3f})",
                    detail={
                        "compound_type": ctype,
                        "compound_score": score,
                        "compound_confidence": conf,
                        "contributing_signals": evidence_list,
                        **profile_snapshot,
                    },
                    profile=prof, now_str=now_str,
                    subtype=ctype,
                    impact_decomposition=decomp,
                    threshold_info={
                        "type": "compound_strengthening",
                        "previous_impact": round(max(prev_impact, prev_str_impact), 4),
                        "current_impact": round(impact, 4),
                    },
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
    impact_decomposition: dict[str, float] | None = None,
    threshold_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an event candidate dict."""
    # Enrich detail_json with impact decomposition and threshold metadata
    if impact_decomposition:
        detail["_impact_decomposition"] = impact_decomposition
    if threshold_info:
        detail["_threshold_info"] = threshold_info
    return {
        "entity_type": defn.entity_type,
        "entity_id": entity_id,
        "event_type": defn.event_type,
        "event_family": defn.event_family,
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
        SELECT id, dedup_key, state, severity, escalation_count, impact_score
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
                    entity_type, entity_id, event_type, event_family, event_subtype,
                    state, severity, impact_score,
                    title, detail_json, dedup_key,
                    first_detected_at, last_updated_at,
                    escalation_count,
                    risk_score_at_event, risk_rank_at_event, risk_percentile_at_event
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, %s, %s, %s)
            """, [
                cand["entity_type"], cand["entity_id"],
                cand["event_type"], cand.get("event_family", "threat"), cand["event_subtype"],
                "active", cand["severity"], cand["impact_score"],
                cand["title"], cand["detail_json"], dk,
                cand["now_str"], cand["now_str"],
                cand["risk_score_at_event"], cand["risk_rank_at_event"],
                cand["risk_percentile_at_event"],
            ])
            created += 1
        else:
            # Existing event — update with escalation logic.
            # Escalation requires WORSENING, not just persistence.
            # We check if the current impact_score exceeds the previous one,
            # indicating the situation is getting worse, not merely continuing.
            new_count = int(existing["escalation_count"] or 0) + 1
            current_severity = existing["severity"]
            new_severity = current_severity

            # Only escalate if impact is increasing (worsening condition)
            prev_impact = float(existing.get("impact_score") or 0)
            is_worsening = cand["impact_score"] > prev_impact

            thresholds = cand.get("escalation_thresholds", {})
            if is_worsening:
                for threshold_count, upgraded_severity in sorted(thresholds.items()):
                    if new_count >= threshold_count:
                        new_severity = upgraded_severity

            severity_changed = new_severity != current_severity

            # Re-activate if it was resolved/expired.
            # Suppressed events only reactivate if materially worsened.
            if existing["state"] == "suppressed":
                if is_worsening:
                    new_state = "active"  # Material worsening overrides suppression
                else:
                    new_state = "suppressed"  # Stay suppressed
            elif existing["state"] in ("resolved", "expired"):
                new_state = "active"
            else:
                new_state = existing["state"]

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


def _auto_resolve_events(
    db: SupplyCoreDb,
    current_candidates: list[dict[str, Any]],
) -> int:
    """Resolve active events whose conditions are no longer true.

    An auto-resolvable event is resolved if it was NOT re-detected in the
    current scan (i.e., its dedup_key is not in the candidate set).

    For events with hysteresis_margin > 0, the character must be *past* the
    boundary + margin before we resolve.  This prevents create→resolve→create
    churn when a character oscillates near a threshold.
    """
    now_str = _now_sql()
    active_dedup_keys = {c["dedup_key"] for c in current_candidates}

    # Get all active auto-resolvable event types
    auto_resolve_types = [et.event_type for et in EVENT_TYPE_MAP.values() if et.auto_resolve]
    if not auto_resolve_types:
        return 0

    placeholders = ",".join(["%s"] * len(auto_resolve_types))
    active_events = db.fetch_all(f"""
        SELECT ie.id, ie.dedup_key, ie.state, ie.severity, ie.event_type,
               ie.entity_id, ie.entity_type
        FROM intelligence_events ie
        WHERE ie.state = 'active'
          AND ie.event_type IN ({placeholders})
          AND ie.last_updated_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR)
    """, auto_resolve_types)

    # Pre-load profiles for hysteresis checks
    char_ids = [int(e["entity_id"]) for e in active_events if e["entity_type"] == "character"]
    profiles_by_id: dict[int, dict] = {}
    if char_ids:
        char_placeholders = ",".join(["%s"] * len(char_ids))
        prof_rows = db.fetch_all(f"""
            SELECT character_id, risk_rank, risk_percentile, freshness
            FROM character_intelligence_profiles
            WHERE character_id IN ({char_placeholders})
        """, char_ids)
        for pr in prof_rows:
            profiles_by_id[int(pr["character_id"])] = pr

    resolved = 0
    for evt in active_events:
        if evt["dedup_key"] in active_dedup_keys:
            continue  # Still detected, don't resolve

        # Check hysteresis
        defn = EVENT_TYPE_MAP.get(evt["event_type"])
        if defn and defn.hysteresis_margin > 0 and evt["entity_type"] == "character":
            prof = profiles_by_id.get(int(evt["entity_id"]))
            if prof and not _passes_hysteresis(defn, prof):
                continue  # Too close to threshold, skip resolution

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


def _passes_hysteresis(defn: EventTypeDefinition, prof: dict[str, Any]) -> bool:
    """Check if a character has moved far enough past a threshold to resolve.

    Returns True if the event SHOULD be resolved (past hysteresis margin).
    Returns False if the character is still too close to the boundary.
    """
    margin = defn.hysteresis_margin
    et = defn.event_type

    if et in ("risk_rank_entry_top50", "risk_rank_entry_top200"):
        # Rank-based: hysteresis_margin is absolute rank positions
        rank = prof.get("risk_rank")
        if rank is None:
            return True  # No rank = not in list = safe to resolve
        threshold = TOP_50_RANK if et == "risk_rank_entry_top50" else TOP_200_RANK
        return rank > (threshold + margin)

    if et == "percentile_escalation":
        # Percentile: margin is fractional (e.g. 0.02)
        pct = float(prof["risk_percentile"]) if prof.get("risk_percentile") is not None else 0.0
        # Event fired when pct was high; resolve when it drops below bucket - margin
        # Since we don't know which bucket triggered, just check if pct dropped
        # meaningfully (below 90th percentile - margin as a safe proxy)
        return pct < (0.90 - margin)

    if et == "freshness_degradation":
        # Freshness: margin means must recover to threshold + margin
        fresh = float(prof.get("freshness") or 0)
        return fresh >= (FRESHNESS_DEGRADATION_THRESHOLD + margin)

    # Default: no hysteresis check, safe to resolve
    return True


# ---------------------------------------------------------------------------
# Expire old events (housekeeping)
# ---------------------------------------------------------------------------

def _unsuppress_expired(db: SupplyCoreDb) -> int:
    """Reactivate suppressed events whose suppression period has expired."""
    now_str = _now_sql()
    result = db.execute("""
        UPDATE intelligence_events
        SET state = 'active', suppressed_until = NULL, last_updated_at = %s
        WHERE state = 'suppressed'
          AND suppressed_until IS NOT NULL
          AND suppressed_until <= UTC_TIMESTAMP()
    """, [now_str])
    return result if isinstance(result, int) else 0


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

    # 0. Load calibrated thresholds (self-leveling from population stats)
    cal = load_calibrated_thresholds(db)
    logger.info(
        "cip_event_engine: calibration loaded — surge=%.4f, rank_jump=%d, freshness=%.2f",
        cal["surge_delta"], cal["rank_jump"], cal["freshness_floor"],
    )

    # 1. Delta detection (simple signals) — with calibrated thresholds
    logger.info("cip_event_engine: scanning for events...")
    candidates = _detect_character_events(db, cal)
    logger.info("cip_event_engine: detected %d simple event candidates", len(candidates))

    # 1b. Compound signal detection
    compound_candidates = _detect_compound_events(db)
    logger.info("cip_event_engine: detected %d compound event candidates", len(compound_candidates))
    candidates.extend(compound_candidates)

    # 2. Event lifecycle management
    logger.info("cip_event_engine: upserting events...")
    created, updated, resolved = _upsert_events(db, candidates)
    logger.info("cip_event_engine: created=%d, updated=%d, resolved=%d", created, updated, resolved)

    # 3. Unsuppress expired suppressions
    unsuppressed = _unsuppress_expired(db)
    if unsuppressed:
        logger.info("cip_event_engine: unsuppressed %d events", unsuppressed)

    # 4. Housekeeping
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
        started_at="", finished_at="",
        duration_ms=elapsed, rows_seen=len(candidates),
        rows_processed=len(candidates), rows_written=total_actions,
        rows_skipped=0, rows_failed=0, batches_completed=1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={"created": created, "updated": updated,
              "resolved": resolved, "expired": expired},
    )
