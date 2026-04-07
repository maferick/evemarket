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
            started_at=job.started_at, finished_at=job.finished_at,
            duration_ms=0, rows_seen=0, rows_processed=0, rows_written=0,
            rows_skipped=0, rows_failed=0, batches_completed=0,
            checkpoint_before=None, checkpoint_after=None,
            has_more=False, error_text=None, warnings=[], meta={},
        )

    char_ids = [int(p["character_id"]) for p in profiles]
    profiles_by_id = {int(p["character_id"]): p for p in profiles}

    # Load all active signals for these characters
    placeholders = ",".join(["%s"] * len(char_ids))
    signal_rows = db.fetch_all(f"""
        SELECT character_id, signal_type, signal_value, confidence
        FROM character_intelligence_signals
        WHERE character_id IN ({placeholders})
    """, char_ids)

    # Group signals by character: {char_id: {signal_type: {value, confidence}}}
    signals_by_char: dict[int, dict[str, dict[str, float]]] = defaultdict(dict)
    for sr in signal_rows:
        cid = int(sr["character_id"])
        signals_by_char[cid][sr["signal_type"]] = {
            "value": float(sr.get("signal_value") or 0),
            "confidence": float(sr.get("confidence") or 0),
        }

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

            existing_key = f"{cid}:{defn.compound_type}"

            if all_met:
                # Compound is active — compute and upsert
                signal_values = [s["value"] for s in matched_signals]
                signal_confs = [s["confidence"] for s in matched_signals]
                score = _compute_compound_score(defn, signal_values)
                conf = min(signal_confs) if signal_confs else 0.0
                evidence = json.dumps(matched_signals)
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

    db.commit()

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
        started_at=job.started_at, finished_at=job.finished_at,
        duration_ms=elapsed, rows_seen=len(char_ids),
        rows_processed=len(char_ids), rows_written=total,
        rows_skipped=0, rows_failed=0, batches_completed=1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={"created": created, "updated": updated, "deactivated": deactivated},
    )
