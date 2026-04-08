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
from collections import defaultdict
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

    # -- Grouped aggregates (one query per metric, all compound types at once) --

    # Active count + avg score/confidence per compound type
    stats_rows = db.fetch_all("""
        SELECT compound_type,
               COUNT(*) AS active_count,
               COALESCE(AVG(score), 0) AS avg_score,
               COALESCE(AVG(confidence), 0) AS avg_confidence
        FROM character_intelligence_compound_signals
        GROUP BY compound_type
    """)
    stats_map: dict[str, dict] = {r["compound_type"]: r for r in stats_rows}

    # New activations (first_detected_at in last 24h)
    new_rows = db.fetch_all("""
        SELECT compound_type, COUNT(*) AS cnt
        FROM character_intelligence_compound_signals
        WHERE first_detected_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY compound_type
    """)
    new_map: dict[str, int] = {r["compound_type"]: int(r["cnt"]) for r in new_rows}

    # Approximate median age using PERCENTILE_CONT (MariaDB 10.3.3+)
    # Falls back to offset-based per compound only if window func unavailable.
    median_rows = db.fetch_all("""
        SELECT compound_type,
               COALESCE(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP
                   (ORDER BY TIMESTAMPDIFF(HOUR, first_detected_at, UTC_TIMESTAMP()))
                   OVER (PARTITION BY compound_type) AS SIGNED), 0) AS median_age
        FROM character_intelligence_compound_signals
        GROUP BY compound_type
    """)
    median_map: dict[str, int] = {r["compound_type"]: int(r["median_age"]) for r in median_rows}

    # Events created/strengthened in last 24h
    ev_rows = db.fetch_all("""
        SELECT event_subtype,
               SUM(CASE WHEN event_type = 'compound_signal_activated'
                        AND first_detected_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
                   THEN 1 ELSE 0 END) AS created,
               SUM(CASE WHEN event_type = 'compound_signal_strengthened'
                        AND last_updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
                   THEN 1 ELSE 0 END) AS strengthened
        FROM intelligence_events
        WHERE event_type IN ('compound_signal_activated', 'compound_signal_strengthened')
        GROUP BY event_subtype
    """)
    ev_map: dict[str, dict] = {r["event_subtype"]: r for r in ev_rows}

    # Analyst interactions on compound events (last 24h)
    interaction_rows = db.fetch_all("""
        SELECT ie.event_subtype,
               SUM(CASE WHEN ieh.new_state = 'acknowledged' THEN 1 ELSE 0 END) AS ack_count,
               SUM(CASE WHEN ieh.new_state = 'resolved' THEN 1 ELSE 0 END) AS res_count,
               SUM(CASE WHEN ieh.new_state = 'suppressed' THEN 1 ELSE 0 END) AS sup_count
        FROM intelligence_event_history ieh
        JOIN intelligence_events ie ON ie.id = ieh.event_id
        WHERE ie.event_type IN ('compound_signal_activated', 'compound_signal_strengthened')
          AND ieh.created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
          AND ieh.changed_by != 'cip_event_engine'
        GROUP BY ie.event_subtype
    """)
    interaction_map: dict[str, dict] = {r["event_subtype"]: r for r in interaction_rows}

    # Notes on compound events (last 24h)
    note_rows = db.fetch_all("""
        SELECT ie.event_subtype, COUNT(*) AS cnt
        FROM intelligence_event_notes ien
        JOIN intelligence_events ie ON ie.id = ien.event_id
        WHERE ie.event_type IN ('compound_signal_activated', 'compound_signal_strengthened')
          AND ien.created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY ie.event_subtype
    """)
    note_map: dict[str, int] = {r["event_subtype"]: int(r["cnt"]) for r in note_rows}

    # Overlap with simple threat events
    overlap_rows = db.fetch_all("""
        SELECT cics.compound_type, COUNT(DISTINCT cics.character_id) AS cnt
        FROM character_intelligence_compound_signals cics
        JOIN intelligence_events ie
            ON ie.entity_type = 'character'
            AND ie.entity_id = cics.character_id
            AND ie.state IN ('active', 'acknowledged')
            AND ie.event_family = 'threat'
            AND ie.event_type NOT IN ('compound_signal_activated', 'compound_signal_strengthened')
        GROUP BY cics.compound_type
    """)
    overlap_map: dict[str, int] = {r["compound_type"]: int(r["cnt"]) for r in overlap_rows}

    # -- Assemble and batch-write snapshots --
    upsert_params: list[list] = []

    for defn in ENABLED_COMPOUNDS:
        ctype = defn.compound_type
        st = stats_map.get(ctype, {})
        active_count = int(st.get("active_count", 0))
        avg_score = float(st.get("avg_score", 0))
        avg_confidence = float(st.get("avg_confidence", 0))
        new_activations = new_map.get(ctype, 0)
        median_age = median_map.get(ctype, 0)

        ev = ev_map.get(ctype, {})
        events_created = int(ev.get("created", 0))
        events_strengthened = int(ev.get("strengthened", 0))

        ia = interaction_map.get(ctype, {})
        ack_count = int(ia.get("ack_count", 0))
        res_count = int(ia.get("res_count", 0))
        sup_count = int(ia.get("sup_count", 0))
        noted_count = note_map.get(ctype, 0)
        overlap_count = overlap_map.get(ctype, 0)

        upsert_params.append([
            today, ctype, defn.compound_family,
            active_count, new_activations,
            round(avg_score, 6), round(avg_confidence, 4), median_age,
            events_created, events_strengthened,
            ack_count, res_count, sup_count, noted_count,
            overlap_count,
        ])

    if upsert_params:
        db.execute_many("""
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
        """, upsert_params)

    rows_written = len(upsert_params)
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
