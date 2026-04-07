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
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def run_cip_event_digest(db) -> dict:
    """Generate a daily event digest covering the last 24 hours."""
    now = datetime.utcnow()
    period_end = now
    period_start = now - timedelta(hours=24)

    logger.info("Generating event digest for %s — %s", period_start, period_end)

    cur = db.cursor()

    # ── New events (created in this window) ─────────────────────────
    cur.execute(
        """SELECT id, event_type, event_family, severity, impact_score,
                  title, entity_type, entity_id
           FROM intelligence_events
           WHERE first_detected_at >= %s AND first_detected_at < %s
           ORDER BY impact_score DESC""",
        (period_start, period_end),
    )
    new_events = cur.fetchall()
    new_count = len(new_events)

    # ── Escalated events (escalation_count increased in this window) ─
    cur.execute(
        """SELECT id, event_type, event_family, severity, previous_severity,
                  impact_score, title, escalation_count, entity_type, entity_id
           FROM intelligence_events
           WHERE last_updated_at >= %s AND last_updated_at < %s
             AND escalation_count > 1
             AND state = 'active'
           ORDER BY impact_score DESC""",
        (period_start, period_end),
    )
    escalated_events = cur.fetchall()
    escalated_count = len(escalated_events)

    # ── Resolved events ──────────────────────────────────────────────
    cur.execute(
        """SELECT id, event_type, event_family, severity, impact_score,
                  title, entity_type, entity_id
           FROM intelligence_events
           WHERE resolved_at >= %s AND resolved_at < %s
           ORDER BY impact_score DESC""",
        (period_start, period_end),
    )
    resolved_events = cur.fetchall()
    resolved_count = len(resolved_events)

    # ── Expired events ───────────────────────────────────────────────
    cur.execute(
        """SELECT COUNT(*) AS cnt
           FROM intelligence_event_history
           WHERE new_state = 'expired'
             AND created_at >= %s AND created_at < %s""",
        (period_start, period_end),
    )
    expired_row = cur.fetchone()
    expired_count = int(expired_row["cnt"]) if expired_row else 0

    # ── Active breakdown by family ────────────────────────────────────
    cur.execute(
        """SELECT event_family, severity, COUNT(*) AS cnt
           FROM intelligence_events
           WHERE state = 'active'
           GROUP BY event_family, severity"""
    )
    breakdown_rows = cur.fetchall()

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
    top_n = 5

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

    top_new = [_event_summary(e) for e in new_events[:top_n]]
    top_escalated = [_event_summary(e) for e in escalated_events[:top_n]]
    top_resolved = [_event_summary(e) for e in resolved_events[:top_n]]

    # ── Insert digest ─────────────────────────────────────────────────
    cur.execute(
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
        (
            period_start, period_end,
            new_count, escalated_count, resolved_count, expired_count,
            threat_active, threat_critical, quality_active,
            json.dumps(top_new) if top_new else None,
            json.dumps(top_escalated) if top_escalated else None,
            json.dumps(top_resolved) if top_resolved else None,
        ),
    )
    db.commit()

    logger.info(
        "Digest generated: %d new, %d escalated, %d resolved, %d expired | "
        "%d active threats (%d critical), %d active quality",
        new_count, escalated_count, resolved_count, expired_count,
        threat_active, threat_critical, quality_active,
    )

    return {
        "new": new_count,
        "escalated": escalated_count,
        "resolved": resolved_count,
        "expired": expired_count,
        "threat_active": threat_active,
        "threat_critical": threat_critical,
        "quality_active": quality_active,
    }
