"""CIP Calibration Layer — Phase 5.

Observes the population of profiled characters and event production,
then computes calibrated thresholds and priority bands.  This is NOT
machine learning — it is percentile-based self-leveling.

The calibration job runs daily and writes a snapshot.  The event engine
reads the latest snapshot at startup to replace hardcoded thresholds.

Calibration targets:
  - Surge delta threshold → p90 of delta_24h distribution
  - Rank jump threshold → based on rank volatility (IQR of rank changes)
  - Freshness floor → p10 of freshness distribution
  - Priority bands → risk_score percentile boundaries (p99/p95/p75/p50)
  - Noise ratio → events_created_24h / total_characters (target < 0.10)
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, UTC

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fallback defaults (used when not enough data for calibration)
# ---------------------------------------------------------------------------
DEFAULT_SURGE_DELTA = 0.08
DEFAULT_RANK_JUMP = 20
DEFAULT_FRESHNESS_FLOOR = 0.40
MIN_POPULATION_FOR_CALIBRATION = 50


def run_cip_calibration(db: SupplyCoreDb) -> JobResult:
    """Compute calibrated thresholds from population statistics."""
    job = start_job_run(db, "cip_calibration")
    t0 = time.monotonic()
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    # Check if calibration is frozen via admin override
    frozen_row = db.fetch_one(
        "SELECT setting_value FROM app_settings WHERE setting_key = 'cip_calibration_frozen'"
    )
    if frozen_row and frozen_row["setting_value"] == "1":
        logger.info("cip_calibration: FROZEN by admin override, skipping")
        finish_job_run(db, job, status="success", rows_processed=0, rows_written=0,
                       meta={"frozen": True})
        elapsed = int((time.monotonic() - t0) * 1000)
        return JobResult(
            status="success", summary="Calibration frozen by admin override",
            started_at="", finished_at="",
            duration_ms=elapsed, rows_seen=0, rows_processed=0, rows_written=0,
            rows_skipped=0, rows_failed=0, batches_completed=0,
            checkpoint_before=None, checkpoint_after=None,
            has_more=False, error_text=None, warnings=["Calibration frozen by admin"], meta={"frozen": True},
        )

    # ── Population stats ─────────────────────────────────────────────
    pop = db.fetch_one("""
        SELECT COUNT(*) AS total,
               COUNT(CASE WHEN signal_count > 0 THEN 1 END) AS with_signals
        FROM character_intelligence_profiles
    """)
    total_profiled = int(pop["total"]) if pop else 0
    with_signals = int(pop["with_signals"]) if pop else 0

    # Event production
    ev_stats = db.fetch_one("""
        SELECT
            SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) AS active_count,
            SUM(CASE WHEN first_detected_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) AS created_24h,
            SUM(CASE WHEN state = 'resolved' AND resolved_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) AS resolved_24h,
            SUM(CASE WHEN state = 'suppressed' THEN 1 ELSE 0 END) AS suppressed_24h
        FROM intelligence_events
    """)
    active_events = int(ev_stats["active_count"] or 0) if ev_stats else 0
    created_24h = int(ev_stats["created_24h"] or 0) if ev_stats else 0
    resolved_24h = int(ev_stats["resolved_24h"] or 0) if ev_stats else 0
    suppressed_24h = int(ev_stats["suppressed_24h"] or 0) if ev_stats else 0

    chars_with_events = 0
    cwe_row = db.fetch_one("""
        SELECT COUNT(DISTINCT entity_id) AS cnt
        FROM intelligence_events
        WHERE entity_type = 'character' AND state IN ('active', 'acknowledged')
    """)
    if cwe_row:
        chars_with_events = int(cwe_row["cnt"] or 0)

    # ── Risk score percentiles ────────────────────────────────────────
    # Use approximate percentiles via LIMIT/OFFSET
    risk_pcts = {"p50": 0.0, "p75": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0}
    if total_profiled >= MIN_POPULATION_FOR_CALIBRATION:
        for label, pct in [("p50", 0.50), ("p75", 0.75), ("p90", 0.90), ("p95", 0.95), ("p99", 0.99)]:
            offset = max(0, int(total_profiled * pct) - 1)
            row = db.fetch_one("""
                SELECT risk_score
                FROM character_intelligence_profiles
                ORDER BY risk_score ASC
                LIMIT 1 OFFSET %s
            """, [offset])
            if row:
                risk_pcts[label] = float(row["risk_score"])

    # ── Delta distribution ────────────────────────────────────────────
    delta_pcts = {"p75": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0}
    delta_pop = db.fetch_one("""
        SELECT COUNT(*) AS cnt
        FROM character_intelligence_profiles
        WHERE risk_delta_24h > 0
    """)
    delta_count = int(delta_pop["cnt"] or 0) if delta_pop else 0
    if delta_count >= 20:
        for label, pct in [("p75", 0.75), ("p90", 0.90), ("p95", 0.95), ("p99", 0.99)]:
            offset = max(0, int(delta_count * pct) - 1)
            row = db.fetch_one("""
                SELECT risk_delta_24h
                FROM character_intelligence_profiles
                WHERE risk_delta_24h > 0
                ORDER BY risk_delta_24h ASC
                LIMIT 1 OFFSET %s
            """, [offset])
            if row:
                delta_pcts[label] = float(row["risk_delta_24h"])

    # ── Freshness distribution ────────────────────────────────────────
    fresh_p10 = DEFAULT_FRESHNESS_FLOOR
    if with_signals >= MIN_POPULATION_FOR_CALIBRATION:
        offset = max(0, int(with_signals * 0.10) - 1)
        row = db.fetch_one("""
            SELECT freshness
            FROM character_intelligence_profiles
            WHERE signal_count > 0
            ORDER BY freshness ASC
            LIMIT 1 OFFSET %s
        """, [offset])
        if row and float(row["freshness"]) > 0:
            fresh_p10 = float(row["freshness"])

    # ── Rank volatility (for rank jump threshold) ─────────────────────
    calibrated_rank_jump = DEFAULT_RANK_JUMP
    if total_profiled >= MIN_POPULATION_FOR_CALIBRATION:
        rank_vol = db.fetch_one("""
            SELECT
                COALESCE(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ABS(CAST(risk_rank AS SIGNED) - CAST(risk_rank_previous AS SIGNED))), 20) AS p90_jump
            FROM character_intelligence_profiles
            WHERE risk_rank IS NOT NULL AND risk_rank_previous IS NOT NULL
              AND risk_rank != risk_rank_previous
        """)
        if rank_vol and rank_vol.get("p90_jump") is not None:
            calibrated_rank_jump = max(5, int(float(rank_vol["p90_jump"])))
        else:
            # Fallback: simple percentile via LIMIT/OFFSET
            movers = db.fetch_one("""
                SELECT COUNT(*) AS cnt
                FROM character_intelligence_profiles
                WHERE risk_rank IS NOT NULL AND risk_rank_previous IS NOT NULL
                  AND risk_rank < risk_rank_previous
            """)
            mover_count = int(movers["cnt"] or 0) if movers else 0
            if mover_count >= 20:
                offset = max(0, int(mover_count * 0.90) - 1)
                row = db.fetch_one("""
                    SELECT (risk_rank_previous - risk_rank) AS jump
                    FROM character_intelligence_profiles
                    WHERE risk_rank IS NOT NULL AND risk_rank_previous IS NOT NULL
                      AND risk_rank < risk_rank_previous
                    ORDER BY (risk_rank_previous - risk_rank) ASC
                    LIMIT 1 OFFSET %s
                """, [offset])
                if row:
                    calibrated_rank_jump = max(5, int(float(row["jump"])))

    # ── Compute calibrated thresholds ─────────────────────────────────
    calibrated_surge = delta_pcts["p90"] if delta_pcts["p90"] > 0.01 else DEFAULT_SURGE_DELTA

    # Clamp to sane ranges
    calibrated_surge = max(0.02, min(0.30, calibrated_surge))
    calibrated_rank_jump = max(5, min(100, calibrated_rank_jump))
    calibrated_freshness = max(0.15, min(0.70, fresh_p10))

    # ── Apply admin overrides (if set) ────────────────────────────────
    overrides = db.fetch_all(
        "SELECT setting_key, setting_value FROM app_settings "
        "WHERE setting_key IN ('cip_override_surge_delta', 'cip_override_rank_jump', 'cip_override_freshness_floor')"
    )
    for ov in overrides:
        val = (ov.get("setting_value") or "").strip()
        if val == "" or val.lower() == "auto":
            continue
        try:
            if ov["setting_key"] == "cip_override_surge_delta":
                calibrated_surge = max(0.01, min(0.50, float(val)))
                logger.info("cip_calibration: surge_delta overridden to %.4f", calibrated_surge)
            elif ov["setting_key"] == "cip_override_rank_jump":
                calibrated_rank_jump = max(3, min(200, int(float(val))))
                logger.info("cip_calibration: rank_jump overridden to %d", calibrated_rank_jump)
            elif ov["setting_key"] == "cip_override_freshness_floor":
                calibrated_freshness = max(0.05, min(0.90, float(val)))
                logger.info("cip_calibration: freshness_floor overridden to %.2f", calibrated_freshness)
        except (ValueError, TypeError):
            pass

    # ── Priority bands ────────────────────────────────────────────────
    band_critical = risk_pcts["p99"]
    band_high = risk_pcts["p95"]
    band_moderate = risk_pcts["p75"]
    band_low = risk_pcts["p50"]

    # ── Noise metrics ─────────────────────────────────────────────────
    noise_ratio = (created_24h / total_profiled) if total_profiled > 0 else 0.0
    suppression_rate = (suppressed_24h / max(1, created_24h))

    # ── Write snapshot ────────────────────────────────────────────────
    db.execute("""
        INSERT INTO intelligence_calibration_snapshots
            (snapshot_date,
             total_profiled_characters, characters_with_events,
             active_events_count, events_created_24h, events_resolved_24h,
             events_suppressed_24h,
             risk_p50, risk_p75, risk_p90, risk_p95, risk_p99,
             delta_p75, delta_p90, delta_p95, delta_p99,
             calibrated_surge_delta, calibrated_rank_jump,
             calibrated_freshness_floor,
             event_noise_ratio, suppression_rate,
             band_critical_floor, band_high_floor,
             band_moderate_floor, band_low_floor)
        VALUES (%s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_profiled_characters  = VALUES(total_profiled_characters),
            characters_with_events     = VALUES(characters_with_events),
            active_events_count        = VALUES(active_events_count),
            events_created_24h         = VALUES(events_created_24h),
            events_resolved_24h        = VALUES(events_resolved_24h),
            events_suppressed_24h      = VALUES(events_suppressed_24h),
            risk_p50 = VALUES(risk_p50), risk_p75 = VALUES(risk_p75),
            risk_p90 = VALUES(risk_p90), risk_p95 = VALUES(risk_p95),
            risk_p99 = VALUES(risk_p99),
            delta_p75 = VALUES(delta_p75), delta_p90 = VALUES(delta_p90),
            delta_p95 = VALUES(delta_p95), delta_p99 = VALUES(delta_p99),
            calibrated_surge_delta     = VALUES(calibrated_surge_delta),
            calibrated_rank_jump       = VALUES(calibrated_rank_jump),
            calibrated_freshness_floor = VALUES(calibrated_freshness_floor),
            event_noise_ratio          = VALUES(event_noise_ratio),
            suppression_rate           = VALUES(suppression_rate),
            band_critical_floor        = VALUES(band_critical_floor),
            band_high_floor            = VALUES(band_high_floor),
            band_moderate_floor        = VALUES(band_moderate_floor),
            band_low_floor             = VALUES(band_low_floor)
    """, [
        today,
        total_profiled, chars_with_events,
        active_events, created_24h, resolved_24h, suppressed_24h,
        risk_pcts["p50"], risk_pcts["p75"], risk_pcts["p90"],
        risk_pcts["p95"], risk_pcts["p99"],
        delta_pcts["p75"], delta_pcts["p90"], delta_pcts["p95"], delta_pcts["p99"],
        round(calibrated_surge, 6), calibrated_rank_jump,
        round(calibrated_freshness, 4),
        round(noise_ratio, 4), round(suppression_rate, 4),
        band_critical, band_high, band_moderate, band_low,
    ])
    elapsed = int((time.monotonic() - t0) * 1000)
    logger.info(
        "cip_calibration: pop=%d, events=%d created, noise=%.3f | "
        "surge=%.4f, rank_jump=%d, freshness=%.2f | "
        "bands: critical=%.4f, high=%.4f, moderate=%.4f",
        total_profiled, created_24h, noise_ratio,
        calibrated_surge, calibrated_rank_jump, calibrated_freshness,
        band_critical, band_high, band_moderate,
    )

    finish_job_run(db, job, status="success", rows_processed=total_profiled, rows_written=1,
                   meta={
                       "calibrated_surge_delta": round(calibrated_surge, 6),
                       "calibrated_rank_jump": calibrated_rank_jump,
                       "calibrated_freshness_floor": round(calibrated_freshness, 4),
                       "noise_ratio": round(noise_ratio, 4),
                   })

    return JobResult(
        status="success",
        summary=f"Calibration: surge={calibrated_surge:.4f}, rank_jump={calibrated_rank_jump}, freshness={calibrated_freshness:.2f}, noise={noise_ratio:.3f}",
        started_at="", finished_at="",
        duration_ms=elapsed, rows_seen=total_profiled,
        rows_processed=total_profiled, rows_written=1,
        rows_skipped=0, rows_failed=0, batches_completed=1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={
            "calibrated_surge_delta": round(calibrated_surge, 6),
            "calibrated_rank_jump": calibrated_rank_jump,
            "calibrated_freshness_floor": round(calibrated_freshness, 4),
            "noise_ratio": round(noise_ratio, 4),
            "bands": {
                "critical": round(band_critical, 6),
                "high": round(band_high, 6),
                "moderate": round(band_moderate, 6),
                "low": round(band_low, 6),
            },
        },
    )


def load_calibrated_thresholds(db: SupplyCoreDb) -> dict:
    """Load the latest calibration snapshot for use by the event engine.

    Returns a dict of thresholds, or defaults if no calibration exists yet.
    """
    row = db.fetch_one("""
        SELECT calibrated_surge_delta, calibrated_rank_jump,
               calibrated_freshness_floor,
               band_critical_floor, band_high_floor,
               band_moderate_floor, band_low_floor
        FROM intelligence_calibration_snapshots
        ORDER BY snapshot_date DESC
        LIMIT 1
    """)
    if row is None:
        return {
            "surge_delta": DEFAULT_SURGE_DELTA,
            "rank_jump": DEFAULT_RANK_JUMP,
            "freshness_floor": DEFAULT_FRESHNESS_FLOOR,
            "band_critical": 0.0,
            "band_high": 0.0,
            "band_moderate": 0.0,
            "band_low": 0.0,
        }
    return {
        "surge_delta": float(row["calibrated_surge_delta"]),
        "rank_jump": int(row["calibrated_rank_jump"]),
        "freshness_floor": float(row["calibrated_freshness_floor"]),
        "band_critical": float(row["band_critical_floor"]),
        "band_high": float(row["band_high_floor"]),
        "band_moderate": float(row["band_moderate_floor"]),
        "band_low": float(row["band_low_floor"]),
    }


def priority_band(risk_score: float, thresholds: dict) -> str:
    """Assign a priority band based on calibrated thresholds."""
    if thresholds["band_critical"] > 0 and risk_score >= thresholds["band_critical"]:
        return "critical"
    if thresholds["band_high"] > 0 and risk_score >= thresholds["band_high"]:
        return "high"
    if thresholds["band_moderate"] > 0 and risk_score >= thresholds["band_moderate"]:
        return "moderate"
    if thresholds["band_low"] > 0 and risk_score >= thresholds["band_low"]:
        return "low"
    return "noise"
