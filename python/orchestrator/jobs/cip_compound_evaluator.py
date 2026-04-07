"""CIP Compound Signal Evaluator — Phase 4.

Evaluates compound signal definitions against each character's active
simple signals and profile state.  Materializes results into
character_intelligence_compound_signals.

Two-pass approach:
  1. Load all characters with recently-computed profiles.
  2. For each character, check every enabled compound definition:
     - Are all required signals present and above their thresholds?
     - Do all profile conditions hold?
     - If yes: compute compound score, record evidence, upsert.
     - If no:  delete any existing materialization (compound no longer active).
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, UTC
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from .cip_compound_definitions import ENABLED_COMPOUNDS, CompoundDefinition

logger = logging.getLogger(__name__)


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _evaluate_profile_conditions(
    conditions: dict[str, dict[str, Any]],
    profile: dict[str, Any],
) -> bool:
    """Check if a profile satisfies all compound-level conditions."""
    for col, rule in conditions.items():
        val = profile.get(col)
        if val is None:
            return False
        val = float(val)
        op = rule.get("op", ">=")
        threshold = float(rule.get("value", 0))
        if op == ">=" and val < threshold:
            return False
        elif op == ">" and val <= threshold:
            return False
        elif op == "<=" and val > threshold:
            return False
        elif op == "<" and val >= threshold:
            return False
        elif op == "==" and val != threshold:
            return False
    return True


def _compute_compound_score(
    defn: CompoundDefinition,
    signal_values: list[float],
) -> float:
    """Compute the compound score from contributing signal values."""
    if not signal_values:
        return 0.0
    if defn.score_mode == "min":
        return min(signal_values)
    elif defn.score_mode == "max":
        return max(signal_values)
    else:  # mean
        return sum(signal_values) / len(signal_values)


def _check_temporal_conditions(
    defn: CompoundDefinition,
    cid: int,
    char_signals: dict[str, dict[str, float]],
    db: SupplyCoreDb,
    history_cache: dict[int, list[dict]],
    signal_ages_cache: dict[str, float],
) -> bool:
    """Check temporal conditions for a compound definition.

    Returns True if ALL temporal conditions are satisfied.
    """
    if not defn.temporal_conditions:
        return True

    for tc in defn.temporal_conditions:
        check = tc.get("check", "")
        params = tc.get("params", {})

        if check == "signal_age_range":
            sig_type = params.get("signal_type", "")
            min_days = float(params.get("min_days", 0))
            max_days = float(params.get("max_days", 9999))
            cache_key = f"{cid}:{sig_type}"
            if cache_key not in signal_ages_cache:
                return False  # Signal age not available
            age_days = signal_ages_cache[cache_key]
            if age_days < min_days or age_days > max_days:
                return False

        elif check == "consecutive_rank_improvement":
            min_consecutive = int(params.get("min_consecutive", 3))
            history = history_cache.get(cid, [])
            if len(history) < min_consecutive + 1:
                return False
            # History is ordered by snapshot_date DESC
            consecutive = 0
            for i in range(len(history) - 1):
                current_rank = history[i].get("risk_rank")
                prev_rank = history[i + 1].get("risk_rank")
                if current_rank is not None and prev_rank is not None:
                    if int(current_rank) < int(prev_rank):
                        consecutive += 1
                    else:
                        break
                else:
                    break
            if consecutive < min_consecutive:
                return False

    return True


def run_cip_compound_evaluator(db: SupplyCoreDb) -> JobResult:
    """Evaluate all enabled compound definitions against recent profiles."""
    job = start_job_run(db, "cip_compound_evaluator")
    t0 = time.monotonic()
    now_str = _now_sql()

    # Load profiles computed within last 2 hours
    profiles = db.fetch_all("""
        SELECT character_id, risk_score, risk_rank, risk_percentile,
               confidence, freshness, effective_coverage,
               risk_delta_24h, risk_delta_7d,
               behavioral_score, graph_score, temporal_score,
               movement_score, relational_score
        FROM character_intelligence_profiles
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)
          AND signal_count > 0
    """)

    if not profiles:
        finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
        return JobResult(
            status="success", summary="No recent profiles to evaluate",
            started_at="", finished_at="",
            duration_ms=0, rows_seen=0, rows_processed=0, rows_written=0,
            rows_skipped=0, rows_failed=0, batches_completed=0,
            checkpoint_before=None, checkpoint_after=None,
            has_more=False, error_text=None, warnings=[], meta={},
        )

    char_ids = [int(p["character_id"]) for p in profiles]
    profiles_by_id = {int(p["character_id"]): p for p in profiles}

    # Load all active signals for these characters (including first_seen_at for temporal checks)
    placeholders = ",".join(["%s"] * len(char_ids))
    signal_rows = db.fetch_all(f"""
        SELECT character_id, signal_type, signal_value, confidence, first_seen_at
        FROM character_intelligence_signals
        WHERE character_id IN ({placeholders})
    """, char_ids)

    # Group signals by character: {char_id: {signal_type: {value, confidence}}}
    signals_by_char: dict[int, dict[str, dict[str, float]]] = defaultdict(dict)
    signal_ages_cache: dict[str, float] = {}  # "char_id:signal_type" -> age in days
    now_utc = datetime.now(UTC)
    for sr in signal_rows:
        cid = int(sr["character_id"])
        signals_by_char[cid][sr["signal_type"]] = {
            "value": float(sr.get("signal_value") or 0),
            "confidence": float(sr.get("confidence") or 0),
        }
        # Compute signal age for temporal conditions
        if sr.get("first_seen_at"):
            try:
                fs = sr["first_seen_at"]
                if isinstance(fs, str):
                    from datetime import datetime as dt
                    fs = dt.strptime(fs[:19], "%Y-%m-%d %H:%M:%S")
                age_days = (now_utc.replace(tzinfo=None) - fs.replace(tzinfo=None) if hasattr(fs, 'replace') else now_utc.replace(tzinfo=None) - fs).total_seconds() / 86400
                signal_ages_cache[f"{cid}:{sr['signal_type']}"] = max(0.0, age_days)
            except (ValueError, TypeError, AttributeError):
                pass

    # Load profile history for temporal checks (last 7 snapshots per character)
    # Only load if any compound has temporal_conditions
    has_temporal = any(defn.temporal_conditions for defn in ENABLED_COMPOUNDS)
    history_cache: dict[int, list[dict]] = defaultdict(list)
    if has_temporal:
        history_rows = db.fetch_all(f"""
            SELECT character_id, snapshot_date, risk_rank
            FROM character_intelligence_profile_history
            WHERE character_id IN ({placeholders})
            ORDER BY character_id, snapshot_date DESC
        """, char_ids)
        for hr in history_rows:
            history_cache[int(hr["character_id"])].append(hr)

    # Load existing compound signals for these characters (for first_detected_at preservation)
    existing_rows = db.fetch_all(f"""
        SELECT character_id, compound_type, first_detected_at
        FROM character_intelligence_compound_signals
        WHERE character_id IN ({placeholders})
    """, char_ids)
    existing_map: dict[str, str] = {}  # "char_id:compound_type" -> first_detected_at
    for er in existing_rows:
        key = f"{er['character_id']}:{er['compound_type']}"
        existing_map[key] = str(er["first_detected_at"])

    created = 0
    updated = 0
    deactivated = 0

    for cid in char_ids:
        char_signals = signals_by_char.get(cid, {})
        profile = profiles_by_id[cid]

        for defn in ENABLED_COMPOUNDS:
            # Check all required signals are present and above thresholds
            matched_signals: list[dict[str, Any]] = []
            all_met = True

            for sig_type, min_val in defn.required_signals.items():
                sig = char_signals.get(sig_type)
                if sig is None or sig["value"] < min_val:
                    all_met = False
                    break
                matched_signals.append({
                    "signal_type": sig_type,
                    "value": round(sig["value"], 6),
                    "confidence": round(sig["confidence"], 4),
                    "min_required": min_val,
                })

            # Check profile-level conditions
            if all_met and defn.profile_conditions:
                all_met = _evaluate_profile_conditions(defn.profile_conditions, profile)

            # Check temporal conditions
            if all_met and defn.temporal_conditions:
                all_met = _check_temporal_conditions(
                    defn, cid, char_signals, db, history_cache, signal_ages_cache,
                )

            existing_key = f"{cid}:{defn.compound_type}"

            if all_met:
                # Compound is active — compute and upsert
                signal_values = [s["value"] for s in matched_signals]
                signal_confs = [s["confidence"] for s in matched_signals]
                score = _compute_compound_score(defn, signal_values)
                conf = min(signal_confs) if signal_confs else 0.0

                # Build enriched evidence with confidence derivation
                evidence_payload = {
                    "signals": matched_signals,
                    "compound_family": defn.compound_family,
                    "score_mode": defn.score_mode,
                    "confidence_derivation": {
                        "mode": defn.confidence_mode,
                        "per_signal": {s["signal_type"]: s["confidence"] for s in matched_signals},
                        "result": round(conf, 4),
                        "weakest_signal": min(matched_signals, key=lambda s: s["confidence"])["signal_type"] if matched_signals else None,
                    },
                }
                if defn.profile_conditions:
                    evidence_payload["profile_conditions_met"] = {
                        col: round(float(profile.get(col) or 0), 4)
                        for col in defn.profile_conditions
                    }
                evidence = json.dumps(evidence_payload)
                first_detected = existing_map.get(existing_key, now_str)

                db.execute("""
                    INSERT INTO character_intelligence_compound_signals
                        (character_id, compound_type, score, confidence,
                         evidence_json, version, first_detected_at, last_evaluated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        score             = VALUES(score),
                        confidence        = VALUES(confidence),
                        evidence_json     = VALUES(evidence_json),
                        version           = VALUES(version),
                        last_evaluated_at = VALUES(last_evaluated_at)
                """, [
                    cid, defn.compound_type, round(score, 6), round(conf, 4),
                    evidence, defn.version, first_detected, now_str,
                ])

                if existing_key in existing_map:
                    updated += 1
                else:
                    created += 1
            else:
                # Compound not active — remove if it existed
                if existing_key in existing_map:
                    db.execute("""
                        DELETE FROM character_intelligence_compound_signals
                        WHERE character_id = %s AND compound_type = %s
                    """, [cid, defn.compound_type])
                    deactivated += 1

    elapsed = int((time.monotonic() - t0) * 1000)
    total = created + updated + deactivated

    logger.info(
        "cip_compound_evaluator: %d characters × %d compounds → "
        "%d created, %d updated, %d deactivated",
        len(char_ids), len(ENABLED_COMPOUNDS), created, updated, deactivated,
    )

    finish_job_run(db, job, status="success",
                   rows_processed=len(char_ids), rows_written=total,
                   meta={"created": created, "updated": updated,
                         "deactivated": deactivated})

    return JobResult(
        status="success",
        summary=f"Compounds: {created} created, {updated} updated, {deactivated} deactivated",
        started_at="", finished_at="",
        duration_ms=elapsed, rows_seen=len(char_ids),
        rows_processed=len(char_ids), rows_written=total,
        rows_skipped=0, rows_failed=0, batches_completed=1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={"created": created, "updated": updated, "deactivated": deactivated},
    )
