"""CIP Event Digest Generator — Phase 3.

Produces periodic summary digests of intelligence event activity:
  - How many events were created, escalated, resolved, expired
  - Top new/escalated/resolved events by impact
  - Breakdown by family (threat vs profile_quality)

Digests are written to `intelligence_event_digests` for the analyst surface
to display as a "what changed since last time" banner.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, UTC

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run

logger = logging.getLogger(__name__)


def run_cip_event_digest(db: SupplyCoreDb) -> JobResult:
    """Generate a daily event digest covering the last 24 hours."""
    job = start_job_run(db, "cip_event_digest")
    t0 = time.monotonic()

    now = datetime.now(UTC)
    period_end = now.strftime("%Y-%m-%d %H:%M:%S")
    period_start = (now - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")

    logger.info("Generating event digest for %s — %s", period_start, period_end)

    # ── New events (created in this window) ─────────────────────────
    new_count_row = db.fetch_one(
        """SELECT COUNT(*) AS cnt
           FROM intelligence_events
           WHERE first_detected_at >= %s AND first_detected_at < %s""",
        [period_start, period_end],
    )
    new_count = int(new_count_row["cnt"]) if new_count_row else 0

    top_new_rows = db.fetch_all(
        """SELECT id, event_type, event_family, severity, impact_score,
                  title, entity_type, entity_id
           FROM intelligence_events
           WHERE first_detected_at >= %s AND first_detected_at < %s
           ORDER BY impact_score DESC
           LIMIT 5""",
        [period_start, period_end],
    )

    # ── Escalated events (escalation_count increased in this window) ─
    escalated_count_row = db.fetch_one(
        """SELECT COUNT(*) AS cnt
           FROM intelligence_events
           WHERE last_updated_at >= %s AND last_updated_at < %s
             AND escalation_count > 1
             AND state = 'active'""",
        [period_start, period_end],
    )
    escalated_count = int(escalated_count_row["cnt"]) if escalated_count_row else 0

    top_escalated_rows = db.fetch_all(
        """SELECT id, event_type, event_family, severity, previous_severity,
                  impact_score, title, escalation_count, entity_type, entity_id
           FROM intelligence_events
           WHERE last_updated_at >= %s AND last_updated_at < %s
             AND escalation_count > 1
             AND state = 'active'
           ORDER BY impact_score DESC
           LIMIT 5""",
        [period_start, period_end],
    )

    # ── Resolved events ──────────────────────────────────────────────
    resolved_count_row = db.fetch_one(
        """SELECT COUNT(*) AS cnt
           FROM intelligence_events
           WHERE resolved_at >= %s AND resolved_at < %s""",
        [period_start, period_end],
    )
    resolved_count = int(resolved_count_row["cnt"]) if resolved_count_row else 0

    top_resolved_rows = db.fetch_all(
        """SELECT id, event_type, event_family, severity, impact_score,
                  title, entity_type, entity_id
           FROM intelligence_events
           WHERE resolved_at >= %s AND resolved_at < %s
           ORDER BY impact_score DESC
           LIMIT 5""",
        [period_start, period_end],
    )

    # ── Expired events ───────────────────────────────────────────────
    expired_row = db.fetch_one(
        """SELECT COUNT(*) AS cnt
           FROM intelligence_event_history
           WHERE new_state = 'expired'
             AND created_at >= %s AND created_at < %s""",
        [period_start, period_end],
    )
    expired_count = int(expired_row["cnt"]) if expired_row else 0

    # ── Active breakdown by family ────────────────────────────────────
    breakdown_rows = db.fetch_all(
        """SELECT event_family, severity, COUNT(*) AS cnt
           FROM intelligence_events
           WHERE state = 'active'
           GROUP BY event_family, severity"""
    )

    threat_active = 0
    threat_critical = 0
    quality_active = 0
    for row in breakdown_rows:
        cnt = int(row["cnt"])
        if row["event_family"] == "threat":
            threat_active += cnt
            if row["severity"] == "critical":
                threat_critical += cnt
        elif row["event_family"] == "profile_quality":
            quality_active += cnt

    # ── Build top-N JSON summaries ────────────────────────────────────

    def _event_summary(ev: dict) -> dict:
        return {
            "id": int(ev["id"]),
            "event_type": ev["event_type"],
            "title": ev.get("title", ""),
            "severity": ev.get("severity", ""),
            "impact_score": float(ev.get("impact_score", 0)),
            "entity_type": ev.get("entity_type", ""),
            "entity_id": int(ev.get("entity_id", 0)),
        }

    top_new = [_event_summary(e) for e in top_new_rows]
    top_escalated = [_event_summary(e) for e in top_escalated_rows]
    top_resolved = [_event_summary(e) for e in top_resolved_rows]

    # ── Insert digest ─────────────────────────────────────────────────
    db.execute(
        """INSERT INTO intelligence_event_digests
               (digest_type, period_start, period_end,
                new_events, escalated_events, resolved_events, expired_events,
                threat_active, threat_critical, quality_active,
                top_new_json, top_escalated_json, top_resolved_json,
                generated_by)
           VALUES ('daily', %s, %s,
                   %s, %s, %s, %s,
                   %s, %s, %s,
                   %s, %s, %s,
                   'cip_event_digest')""",
        [
            period_start, period_end,
            new_count, escalated_count, resolved_count, expired_count,
            threat_active, threat_critical, quality_active,
            json.dumps(top_new) if top_new else None,
            json.dumps(top_escalated) if top_escalated else None,
            json.dumps(top_resolved) if top_resolved else None,
        ],
    )

    elapsed = int((time.monotonic() - t0) * 1000)

    logger.info(
        "Digest generated: %d new, %d escalated, %d resolved, %d expired | "
        "%d active threats (%d critical), %d active quality",
        new_count, escalated_count, resolved_count, expired_count,
        threat_active, threat_critical, quality_active,
    )

    finish_job_run(db, job, status="success",
                   rows_processed=new_count + escalated_count + resolved_count,
                   rows_written=1,
                   meta={"new": new_count, "escalated": escalated_count,
                         "resolved": resolved_count, "expired": expired_count})

    return JobResult(
        status="success",
        summary=f"Digest: {new_count} new, {escalated_count} escalated, {resolved_count} resolved, {expired_count} expired",
        started_at="", finished_at="",
        duration_ms=elapsed,
        rows_seen=new_count + escalated_count + resolved_count,
        rows_processed=new_count + escalated_count + resolved_count,
        rows_written=1,
        rows_skipped=0, rows_failed=0, batches_completed=1,
        checkpoint_before=None, checkpoint_after=None,
        has_more=False, error_text=None, warnings=[],
        meta={"new": new_count, "escalated": escalated_count,
              "resolved": resolved_count, "expired": expired_count},
    )
