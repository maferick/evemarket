"""CIP Fusion Engine — computes Character Intelligence Profiles.

Reads from ``character_intelligence_signals``, applies decay and weighting
per signal definition, and produces a fused profile per character in
``character_intelligence_profiles``.

The fusion engine owns:
  - Signal normalization (clamp to [0,1] using definition-aware scaling)
  - Decay application (exponential/linear/step based on signal age)
  - Trust surface computation (confidence, freshness, coverage)
  - Domain sub-score aggregation
  - Delta tracking (vs previous profile)
  - Daily history snapshots
"""

from __future__ import annotations

import logging
import math
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from .cip_signal_definitions import (
    ALL_SIGNAL_DOMAINS,
    SIGNAL_DEF_MAP,
    SignalDefinition,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


def _now() -> datetime:
    return datetime.now(UTC)


def _now_sql() -> str:
    return _now().strftime("%Y-%m-%d %H:%M:%S")


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


# ---------------------------------------------------------------------------
# Decay functions
# ---------------------------------------------------------------------------

def _compute_decay(defn: SignalDefinition, age_days: float) -> float:
    """Return a decay multiplier in [0, 1] for a signal of the given age.

    Decay types:
      - none:        always 1.0
      - exponential: 0.5^(age / half_life)
      - linear:      max(0, 1 - age / (2 * half_life))
      - step:        1.0 if age <= half_life else 0.0
    """
    if defn.decay_type == "none" or defn.half_life_days <= 0:
        return 1.0
    hl = float(defn.half_life_days)
    if defn.decay_type == "exponential":
        return math.pow(0.5, age_days / hl)
    if defn.decay_type == "linear":
        return max(0.0, 1.0 - age_days / (2.0 * hl))
    if defn.decay_type == "step":
        return 1.0 if age_days <= hl else 0.0
    # Fallback: exponential
    return math.pow(0.5, age_days / hl)


# ---------------------------------------------------------------------------
# Fusion logic
# ---------------------------------------------------------------------------

def _fuse_character(
    character_id: int,
    signals: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any] | None:
    """Compute a fused CIP for one character from their active signals.

    Returns a dict ready for upsert, or None if no usable signals.
    """
    if not signals:
        return None

    # Accumulate weighted contributions per domain
    domain_scores: dict[str, float] = defaultdict(float)
    domain_weights: dict[str, float] = defaultdict(float)
    domain_signal_counts: dict[str, int] = defaultdict(int)
    domain_freshness_sum: dict[str, float] = defaultdict(float)
    domain_confidence_sum: dict[str, float] = defaultdict(float)

    total_weighted_score = 0.0
    total_weight = 0.0
    total_confidence_weighted = 0.0
    total_freshness_weighted = 0.0
    active_signal_count = 0
    top_signals: list[dict[str, Any]] = []
    domains_with_data: set[str] = set()

    for sig in signals:
        signal_type = sig["signal_type"]
        defn = SIGNAL_DEF_MAP.get(signal_type)
        if defn is None:
            continue

        # Compute age from last_reinforced_at (preferred) or computed_at
        reinforced = sig.get("last_reinforced_at") or sig["computed_at"]
        if isinstance(reinforced, str):
            reinforced = datetime.fromisoformat(reinforced)
        if reinforced.tzinfo is None:
            reinforced = reinforced.replace(tzinfo=UTC)
        age_days = max(0.0, (now - reinforced).total_seconds() / 86400.0)

        decay = _compute_decay(defn, age_days)
        if decay < 0.01:
            # Signal has fully decayed — skip
            continue

        raw_value = float(sig.get("signal_value") or 0)
        signal_confidence = float(sig.get("confidence") or 1.0)
        weight = defn.weight_default

        # Effective contribution: value × decay × confidence × weight
        effective_value = _clamp(raw_value) * decay * signal_confidence
        weighted_contribution = effective_value * weight

        domain = defn.signal_domain
        domain_scores[domain] += weighted_contribution
        domain_weights[domain] += weight
        domain_signal_counts[domain] += 1
        domain_freshness_sum[domain] += decay
        domain_confidence_sum[domain] += signal_confidence
        domains_with_data.add(domain)

        total_weighted_score += weighted_contribution
        total_weight += weight
        total_confidence_weighted += signal_confidence * weight
        total_freshness_weighted += decay * weight
        active_signal_count += 1

        top_signals.append({
            "signal_type": signal_type,
            "domain": domain,
            "raw_value": round(raw_value, 6),
            "decay": round(decay, 4),
            "confidence": round(signal_confidence, 4),
            "weight": round(weight, 4),
            "contribution": round(weighted_contribution, 6),
        })

    if active_signal_count == 0:
        return None

    # Final risk score (normalized by total weight)
    risk_score = _clamp(total_weighted_score / total_weight) if total_weight > 0 else 0.0

    # Trust surface
    confidence = _clamp(total_confidence_weighted / total_weight) if total_weight > 0 else 0.0
    freshness = _clamp(total_freshness_weighted / total_weight) if total_weight > 0 else 0.0
    coverage = len(domains_with_data) / len(ALL_SIGNAL_DOMAINS)

    # Domain sub-scores (normalized per domain)
    def _domain_score(d: str) -> float:
        if domain_weights[d] <= 0:
            return 0.0
        return _clamp(domain_scores[d] / domain_weights[d])

    # Sort top signals by contribution descending, keep top 10
    top_signals.sort(key=lambda s: s["contribution"], reverse=True)
    top_signals = top_signals[:10]

    # Domain detail
    domain_detail = {}
    for d in ALL_SIGNAL_DOMAINS:
        count = domain_signal_counts[d]
        if count > 0:
            domain_detail[d] = {
                "score": round(_domain_score(d), 6),
                "signal_count": count,
                "freshness": round(domain_freshness_sum[d] / count, 4),
                "confidence": round(domain_confidence_sum[d] / count, 4),
            }
        else:
            domain_detail[d] = {
                "score": 0.0,
                "signal_count": 0,
                "freshness": 0.0,
                "confidence": 0.0,
            }

    return {
        "character_id": character_id,
        "risk_score": round(risk_score, 6),
        "confidence": round(confidence, 6),
        "freshness": round(freshness, 6),
        "signal_coverage": round(coverage, 6),
        "signal_count": active_signal_count,
        "behavioral_score": round(_domain_score("behavioral"), 6),
        "graph_score": round(_domain_score("graph"), 6),
        "temporal_score": round(_domain_score("temporal"), 6),
        "movement_score": round(_domain_score("movement"), 6),
        "relational_score": round(_domain_score("relational"), 6),
        "top_signals_json": json_dumps_safe(top_signals),
        "domain_detail_json": json_dumps_safe(domain_detail),
    }


# ---------------------------------------------------------------------------
# Profile upsert
# ---------------------------------------------------------------------------

_UPSERT_SQL = """
    INSERT INTO character_intelligence_profiles (
        character_id, risk_score, risk_score_previous,
        confidence, freshness, signal_coverage, signal_count,
        behavioral_score, graph_score, temporal_score,
        movement_score, relational_score,
        risk_delta_24h, risk_delta_7d, new_signals_24h,
        top_signals_json, domain_detail_json,
        computed_at, previous_computed_at
    ) VALUES (
        %s, %s, 0,
        %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        0, 0, 0,
        %s, %s,
        %s, NULL
    )
    ON DUPLICATE KEY UPDATE
        risk_score_previous  = risk_score,
        risk_score           = VALUES(risk_score),
        confidence           = VALUES(confidence),
        freshness            = VALUES(freshness),
        signal_coverage      = VALUES(signal_coverage),
        signal_count         = VALUES(signal_count),
        behavioral_score     = VALUES(behavioral_score),
        graph_score          = VALUES(graph_score),
        temporal_score       = VALUES(temporal_score),
        movement_score       = VALUES(movement_score),
        relational_score     = VALUES(relational_score),
        top_signals_json     = VALUES(top_signals_json),
        domain_detail_json   = VALUES(domain_detail_json),
        previous_computed_at = computed_at,
        computed_at          = VALUES(computed_at)
"""


def _upsert_profiles(db: SupplyCoreDb, profiles: list[dict[str, Any]], now_str: str) -> int:
    """Batch-upsert fused profiles."""
    written = 0
    for prof in profiles:
        db.execute(_UPSERT_SQL, [
            prof["character_id"], prof["risk_score"],
            prof["confidence"], prof["freshness"],
            prof["signal_coverage"], prof["signal_count"],
            prof["behavioral_score"], prof["graph_score"],
            prof["temporal_score"], prof["movement_score"],
            prof["relational_score"],
            prof["top_signals_json"], prof["domain_detail_json"],
            now_str,
        ])
        written += 1
    return written


# ---------------------------------------------------------------------------
# Delta computation (post-upsert)
# ---------------------------------------------------------------------------

def _compute_deltas(db: SupplyCoreDb) -> None:
    """Update delta fields by comparing current vs previous scores."""
    # risk_delta_24h: difference from previous computation
    db.execute("""
        UPDATE character_intelligence_profiles
        SET risk_delta_24h = risk_score - risk_score_previous
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)
    """)

    # risk_delta_7d: compare against history from 7 days ago
    db.execute("""
        UPDATE character_intelligence_profiles cip
        INNER JOIN character_intelligence_profile_history h
            ON h.character_id = cip.character_id
            AND h.snapshot_date = DATE(DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY))
        SET cip.risk_delta_7d = cip.risk_score - h.risk_score
        WHERE cip.computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)
    """)

    # new_signals_24h: count signals emitted/reinforced in last 24h
    db.execute("""
        UPDATE character_intelligence_profiles cip
        INNER JOIN (
            SELECT character_id, COUNT(*) AS cnt
            FROM character_intelligence_signals
            WHERE last_reinforced_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
            GROUP BY character_id
        ) s ON s.character_id = cip.character_id
        SET cip.new_signals_24h = s.cnt
        WHERE cip.computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)
    """)


# ---------------------------------------------------------------------------
# Rank computation
# ---------------------------------------------------------------------------

def _compute_ranks(db: SupplyCoreDb) -> None:
    """Assign ordinal risk ranks (1 = highest risk)."""
    # Save previous ranks
    db.execute("""
        UPDATE character_intelligence_profiles
        SET risk_rank_previous = risk_rank
        WHERE risk_rank IS NOT NULL
    """)

    # Assign new ranks using a variable-based approach (MariaDB compatible)
    db.execute("""
        UPDATE character_intelligence_profiles cip
        INNER JOIN (
            SELECT character_id,
                   RANK() OVER (ORDER BY risk_score DESC) AS new_rank
            FROM character_intelligence_profiles
            WHERE signal_count > 0
        ) ranked ON ranked.character_id = cip.character_id
        SET cip.risk_rank = ranked.new_rank
    """)


# ---------------------------------------------------------------------------
# Daily history snapshot
# ---------------------------------------------------------------------------

def _snapshot_history(db: SupplyCoreDb) -> int:
    """Write today's profile snapshot to the history table."""
    today = _now().strftime("%Y-%m-%d")
    result = db.execute("""
        INSERT INTO character_intelligence_profile_history (
            character_id, snapshot_date,
            risk_score, confidence, freshness, signal_coverage, signal_count,
            risk_rank,
            behavioral_score, graph_score, temporal_score,
            movement_score, relational_score
        )
        SELECT character_id, %s,
               risk_score, confidence, freshness, signal_coverage, signal_count,
               risk_rank,
               behavioral_score, graph_score, temporal_score,
               movement_score, relational_score
        FROM character_intelligence_profiles
        WHERE signal_count > 0
        ON DUPLICATE KEY UPDATE
            risk_score       = VALUES(risk_score),
            confidence       = VALUES(confidence),
            freshness        = VALUES(freshness),
            signal_coverage  = VALUES(signal_coverage),
            signal_count     = VALUES(signal_count),
            risk_rank        = VALUES(risk_rank),
            behavioral_score = VALUES(behavioral_score),
            graph_score      = VALUES(graph_score),
            temporal_score   = VALUES(temporal_score),
            movement_score   = VALUES(movement_score),
            relational_score = VALUES(relational_score)
    """, [today])
    return result if isinstance(result, int) else 0


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_cip_fusion(db: SupplyCoreDb) -> JobResult:
    """Fuse all character intelligence signals into unified profiles."""
    job = start_job_run(db, "cip_fusion")
    t0 = time.monotonic()
    now = _now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # 1. Load all active signals grouped by character
    logger.info("cip_fusion: loading signals...")
    all_signals = db.fetch_all("""
        SELECT character_id, signal_type, window_label,
               signal_value, confidence, signal_version,
               source_pipeline, computed_at,
               first_seen_at, last_reinforced_at, reinforcement_count
        FROM character_intelligence_signals
        WHERE computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 180 DAY)
    """)

    if not all_signals:
        finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
        return JobResult(
            status="success", summary="No signals to fuse",
            started_at=job.started_at, finished_at=job.finished_at,
            duration_ms=0, rows_seen=0, rows_processed=0, rows_written=0,
            rows_skipped=0, rows_failed=0, batches_completed=0,
            checkpoint_before=None, checkpoint_after=None,
            has_more=False, error_text=None, warnings=[], meta={},
        )

    # Group by character
    by_character: dict[int, list[dict]] = defaultdict(list)
    for row in all_signals:
        by_character[int(row["character_id"])].append(row)

    logger.info("cip_fusion: fusing %d characters from %d signals...", len(by_character), len(all_signals))

    # 2. Fuse each character
    profiles: list[dict[str, Any]] = []
    for cid, sigs in by_character.items():
        profile = _fuse_character(cid, sigs, now)
        if profile is not None:
            profiles.append(profile)

    # 3. Upsert profiles in batches
    total_written = 0
    for i in range(0, len(profiles), BATCH_SIZE):
        batch = profiles[i:i + BATCH_SIZE]
        total_written += _upsert_profiles(db, batch, now_str)

    logger.info("cip_fusion: wrote %d profiles", total_written)

    # 4. Compute deltas and ranks
    logger.info("cip_fusion: computing deltas...")
    _compute_deltas(db)

    logger.info("cip_fusion: computing ranks...")
    _compute_ranks(db)

    # 5. Daily history snapshot
    logger.info("cip_fusion: snapshotting history...")
    history_count = _snapshot_history(db)

    elapsed = int((time.monotonic() - t0) * 1000)
    finish_job_run(db, job, status="success",
                   rows_processed=len(all_signals), rows_written=total_written,
                   meta={"history_snapshots": history_count})

    return JobResult(
        status="success",
        summary=f"Fused {total_written} profiles from {len(all_signals)} signals across {len(by_character)} characters",
        started_at=job.started_at, finished_at=job.finished_at,
        duration_ms=elapsed, rows_seen=len(all_signals),
        rows_processed=len(all_signals), rows_written=total_written,
        rows_skipped=0, rows_failed=0,
        batches_completed=(len(profiles) // BATCH_SIZE) + 1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={"characters_fused": len(profiles), "history_snapshots": history_count},
    )
