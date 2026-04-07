"""CIP Compound Analytics — Phase 4.5.

Produces daily per-compound metrics snapshots for validation and tuning:
  - Activation counts, new/deactivated, avg score/confidence
  - Event metrics (created, strengthened)
  - Analyst interaction metrics (acknowledged, resolved, suppressed, noted)
  - Overlap with simple threat events

These snapshots answer: "are compounds useful, noisy, or redundant?"
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, UTC

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from .cip_compound_definitions import COMPOUND_DEF_MAP, ENABLED_COMPOUNDS

logger = logging.getLogger(__name__)


def run_cip_compound_analytics(db: SupplyCoreDb) -> JobResult:
    """Generate a daily analytics snapshot for each compound type."""
    job = start_job_run(db, "cip_compound_analytics")
    t0 = time.monotonic()
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    rows_written = 0

    for defn in ENABLED_COMPOUNDS:
        ctype = defn.compound_type

        # Active count + avg score/confidence
        stats = db.fetch_one("""
            SELECT COUNT(*) AS active_count,
                   COALESCE(AVG(score), 0) AS avg_score,
                   COALESCE(AVG(confidence), 0) AS avg_confidence
            FROM character_intelligence_compound_signals
            WHERE compound_type = %s
        """, [ctype])
        active_count = int(stats["active_count"]) if stats else 0
        avg_score = float(stats["avg_score"]) if stats else 0.0
        avg_confidence = float(stats["avg_confidence"]) if stats else 0.0

        # New activations (first_detected_at in last 24h)
        new_row = db.fetch_one("""
            SELECT COUNT(*) AS cnt
            FROM character_intelligence_compound_signals
            WHERE compound_type = %s
              AND first_detected_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
        """, [ctype])
        new_activations = int(new_row["cnt"]) if new_row else 0

        # Median age (hours) of active compounds
        median_row = db.fetch_one("""
            SELECT COALESCE(
                TIMESTAMPDIFF(HOUR, first_detected_at, UTC_TIMESTAMP()), 0
            ) AS median_age
            FROM character_intelligence_compound_signals
            WHERE compound_type = %s
            ORDER BY first_detected_at
            LIMIT 1 OFFSET %s
        """, [ctype, max(0, active_count // 2)])
        median_age = int(median_row["median_age"]) if median_row else 0

        # Events created/strengthened in last 24h
        ev_created = db.fetch_one("""
            SELECT COUNT(*) AS cnt
            FROM intelligence_events
            WHERE event_type = 'compound_signal_activated'
              AND event_subtype = %s
              AND first_detected_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
        """, [ctype])
        events_created = int(ev_created["cnt"]) if ev_created else 0

        ev_strengthened = db.fetch_one("""
            SELECT COUNT(*) AS cnt
            FROM intelligence_events
            WHERE event_type = 'compound_signal_strengthened'
              AND event_subtype = %s
              AND last_updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
        """, [ctype])
        events_strengthened = int(ev_strengthened["cnt"]) if ev_strengthened else 0

        # Analyst interaction on compound events (last 24h)
        interaction = db.fetch_one("""
            SELECT
                SUM(CASE WHEN ieh.new_state = 'acknowledged' THEN 1 ELSE 0 END) AS ack_count,
                SUM(CASE WHEN ieh.new_state = 'resolved' THEN 1 ELSE 0 END) AS res_count,
                SUM(CASE WHEN ieh.new_state = 'suppressed' THEN 1 ELSE 0 END) AS sup_count
            FROM intelligence_event_history ieh
            JOIN intelligence_events ie ON ie.id = ieh.event_id
            WHERE ie.event_type IN ('compound_signal_activated', 'compound_signal_strengthened')
              AND ie.event_subtype = %s
              AND ieh.created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
              AND ieh.changed_by != 'cip_event_engine'
        """, [ctype])
        ack_count = int(interaction["ack_count"] or 0) if interaction else 0
        res_count = int(interaction["res_count"] or 0) if interaction else 0
        sup_count = int(interaction["sup_count"] or 0) if interaction else 0

        # Notes on compound events
        noted = db.fetch_one("""
            SELECT COUNT(*) AS cnt
            FROM intelligence_event_notes ien
            JOIN intelligence_events ie ON ie.id = ien.event_id
            WHERE ie.event_type IN ('compound_signal_activated', 'compound_signal_strengthened')
              AND ie.event_subtype = %s
              AND ien.created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
        """, [ctype])
        noted_count = int(noted["cnt"]) if noted else 0

        # Overlap with simple threat events
        overlap = db.fetch_one("""
            SELECT COUNT(DISTINCT cics.character_id) AS cnt
            FROM character_intelligence_compound_signals cics
            JOIN intelligence_events ie
                ON ie.entity_type = 'character'
                AND ie.entity_id = cics.character_id
                AND ie.state IN ('active', 'acknowledged')
                AND ie.event_family = 'threat'
                AND ie.event_type NOT IN ('compound_signal_activated', 'compound_signal_strengthened')
            WHERE cics.compound_type = %s
        """, [ctype])
        overlap_count = int(overlap["cnt"]) if overlap else 0

        # Upsert snapshot
        db.execute("""
            INSERT INTO compound_analytics_snapshots
                (snapshot_date, compound_type, compound_family,
                 active_count, new_activations, deactivations,
                 avg_score, avg_confidence, median_age_hours,
                 events_created, events_strengthened,
                 acknowledged_count, resolved_count, suppressed_count, noted_count,
                 overlap_with_simple_events)
            VALUES (%s, %s, %s, %s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                compound_family            = VALUES(compound_family),
                active_count               = VALUES(active_count),
                new_activations            = VALUES(new_activations),
                avg_score                  = VALUES(avg_score),
                avg_confidence             = VALUES(avg_confidence),
                median_age_hours           = VALUES(median_age_hours),
                events_created             = VALUES(events_created),
                events_strengthened        = VALUES(events_strengthened),
                acknowledged_count         = VALUES(acknowledged_count),
                resolved_count             = VALUES(resolved_count),
                suppressed_count           = VALUES(suppressed_count),
                noted_count                = VALUES(noted_count),
                overlap_with_simple_events = VALUES(overlap_with_simple_events)
        """, [
            today, ctype, defn.compound_family,
            active_count, new_activations,
            round(avg_score, 6), round(avg_confidence, 4), median_age,
            events_created, events_strengthened,
            ack_count, res_count, sup_count, noted_count,
            overlap_count,
        ])
        rows_written += 1

    db.commit()

    elapsed = int((time.monotonic() - t0) * 1000)
    logger.info("cip_compound_analytics: wrote %d compound snapshots for %s", rows_written, today)

    finish_job_run(db, job, status="success",
                   rows_processed=len(ENABLED_COMPOUNDS), rows_written=rows_written)

    return JobResult(
        status="success",
        summary=f"Compound analytics: {rows_written} snapshots for {today}",
        started_at="", finished_at="",
        duration_ms=elapsed, rows_seen=len(ENABLED_COMPOUNDS),
        rows_processed=len(ENABLED_COMPOUNDS), rows_written=rows_written,
        rows_skipped=0, rows_failed=0, batches_completed=1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={"snapshot_date": today},
    )
