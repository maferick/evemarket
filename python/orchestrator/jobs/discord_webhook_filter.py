# ── Job Wiring Checklist ─────────────────────────────────────────────────────
# New Python job?  See python/orchestrator/jobs/__init__.py for the full
# checklist (11 registration points).  In THIS file you need:
#   - Implement run_discord_webhook_filter(db)
# ─────────────────────────────────────────────────────────────────────────────
from __future__ import annotations

import hashlib
import json
import logging
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from ..db import SupplyCoreDb

logger = logging.getLogger(__name__)

# Discord rate limits: 30 messages per 60 seconds per webhook.
# We cap per-run sends well below that and add inter-message delay.
_MAX_MESSAGES_PER_RUN = 8
_INTER_MESSAGE_DELAY = 1.5  # seconds between webhook calls

# ── Colour palette (decimal, not hex — Discord embed spec) ──────────
_CLR_RED = 15548997       # #ED4245 — failures / action required
_CLR_AMBER = 16312092     # #FEE75C — warnings / watch items
_CLR_GREEN = 5763719      # #57F287 — positive / deal alerts
_CLR_BLUE = 5793266       # #5865F2 — info / status updates
_CLR_PURPLE = 10181046    # #9B59B6 — sovereignty / strategic

# ── Ensure tracker table ────────────────────────────────────────────
_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS discord_webhook_sent (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fingerprint     CHAR(64)       NOT NULL,
    event_type      VARCHAR(60)    NOT NULL,
    event_summary   VARCHAR(500)   NOT NULL,
    sent_at         DATETIME       NOT NULL,
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_fingerprint (fingerprint),
    KEY idx_dws_event_type (event_type, sent_at),
    KEY idx_dws_sent_at (sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def _fingerprint(event_type: str, identity: str) -> str:
    return hashlib.sha256(f"{event_type}::{identity}".encode()).hexdigest()


def _already_sent(db: SupplyCoreDb, fp: str) -> bool:
    row = db.fetch_one(
        "SELECT id FROM discord_webhook_sent WHERE fingerprint = %s LIMIT 1",
        (fp,),
    )
    return row is not None


def _mark_sent(db: SupplyCoreDb, fp: str, event_type: str, summary: str) -> None:
    db.execute(
        "INSERT IGNORE INTO discord_webhook_sent "
        "(fingerprint, event_type, event_summary, sent_at) "
        "VALUES (%s, %s, %s, UTC_TIMESTAMP())",
        (fp, event_type, summary[:500]),
    )


# ── Discord webhook helpers ────────────────────────────────────────
def _send_webhook(webhook_url: str, payload: dict[str, Any]) -> bool:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "SupplyCore-Discord/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        return True
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:300]
        if exc.code == 429:
            logger.warning("Discord rate-limited (429), stopping sends. Body: %s", body)
            raise  # Caller should stop sending
        logger.warning("Discord webhook error %d: %s", exc.code, body)
        return False
    except Exception as exc:
        logger.warning("Discord webhook send failed: %s", exc)
        return False


# ── Event collectors ────────────────────────────────────────────────

def _collect_job_failures(db: SupplyCoreDb, lookback_hours: int) -> list[dict[str, Any]]:
    """Collect recent critical job failures from sync_runs."""
    rows = db.fetch_all(
        "SELECT dataset_key, error_message, started_at, finished_at "
        "FROM sync_runs "
        "WHERE run_status = 'failed' "
        "  AND started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s HOUR) "
        "ORDER BY started_at DESC "
        "LIMIT 20",
        (lookback_hours,),
    )
    events: list[dict[str, Any]] = []
    for r in rows:
        job_key = str(r.get("dataset_key") or "unknown")
        error = str(r.get("error_message") or "")[:200]
        ts = r.get("started_at", datetime.now(timezone.utc))
        fp = _fingerprint("job_failure", f"{job_key}:{ts}")
        events.append({
            "type": "job_failure",
            "fingerprint": fp,
            "job_key": job_key,
            "error": error,
            "timestamp": ts,
            "embed": {
                "title": f"\u274c Job Failure: {job_key}",
                "description": f"```\n{error[:300]}\n```" if error else "No error message captured.",
                "color": _CLR_RED,
                "fields": [
                    {"name": "Job", "value": f"`{job_key}`", "inline": True},
                    {"name": "Time", "value": str(ts)[:19], "inline": True},
                ],
                "footer": {"text": "SupplyCore \u00b7 Job Failure Alert"},
                "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            },
        })
    return events


def _collect_deal_alerts(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Collect critical and very_strong deal alerts not yet sent."""
    rows = db.fetch_all(
        "SELECT da.alert_key, da.item_type_id, da.source_name, da.severity, "
        "       da.current_price, da.normal_price, da.percent_of_normal, "
        "       da.anomaly_score, da.quantity_available, da.listing_count, "
        "       da.observed_at, "
        "       COALESCE(t.typeName, CONCAT('Type #', da.item_type_id)) AS item_name "
        "FROM market_deal_alerts_current da "
        "LEFT JOIN ref_types t ON t.typeID = da.item_type_id "
        "WHERE da.severity IN ('critical', 'very_strong') "
        "  AND da.status = 'active' "
        "  AND da.observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 4 HOUR) "
        "ORDER BY da.severity_rank DESC, da.anomaly_score DESC "
        "LIMIT 10",
    )
    events: list[dict[str, Any]] = []
    for r in rows:
        alert_key = str(r.get("alert_key") or "")
        item_name = str(r.get("item_name") or "Unknown Item")
        severity = str(r.get("severity") or "watch")
        source = str(r.get("source_name") or "Unknown")
        current = float(r.get("current_price") or 0)
        normal = float(r.get("normal_price") or 0)
        pct = float(r.get("percent_of_normal") or 0)
        anomaly = float(r.get("anomaly_score") or 0)
        qty = int(r.get("quantity_available") or 0)
        listings = int(r.get("listing_count") or 0)
        ts = r.get("observed_at", datetime.now(timezone.utc))

        fp = _fingerprint("deal_alert", alert_key)

        severity_icon = "\U0001f534" if severity == "critical" else "\U0001f7e0"
        discount_pct = round((1 - pct) * 100, 1) if pct < 1 else round((pct - 1) * 100, 1)
        direction = "below" if pct < 1 else "above"

        events.append({
            "type": "deal_alert",
            "fingerprint": fp,
            "summary": f"{item_name} at {source}",
            "embed": {
                "title": f"{severity_icon} Deal Alert: {item_name}",
                "description": f"**{discount_pct}% {direction} normal** at {source}",
                "color": _CLR_GREEN if pct < 1 else _CLR_AMBER,
                "fields": [
                    {"name": "Current Price", "value": f"{current:,.0f} ISK", "inline": True},
                    {"name": "Normal Price", "value": f"{normal:,.0f} ISK", "inline": True},
                    {"name": "Anomaly Score", "value": f"{anomaly:.1f}", "inline": True},
                    {"name": "Available", "value": f"{qty:,} units ({listings} listings)", "inline": True},
                    {"name": "Severity", "value": severity.upper(), "inline": True},
                ],
                "footer": {"text": f"SupplyCore \u00b7 Market Intelligence \u00b7 {source}"},
                "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            },
        })
    return events


def _collect_sovereignty_alerts(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Collect recent sovereignty alerts worth notifying about."""
    # Check if the table exists first
    try:
        rows = db.fetch_all(
            "SELECT alert_id, alert_type, system_name, region_name, "
            "       alliance_name, severity, headline, detail_text, created_at "
            "FROM sovereignty_alerts "
            "WHERE severity IN ('critical', 'high') "
            "  AND created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR) "
            "ORDER BY created_at DESC "
            "LIMIT 5",
        )
    except Exception:
        return []

    events: list[dict[str, Any]] = []
    for r in rows:
        alert_id = str(r.get("alert_id") or "")
        alert_type = str(r.get("alert_type") or "unknown")
        system_name = str(r.get("system_name") or "Unknown System")
        region = str(r.get("region_name") or "")
        alliance = str(r.get("alliance_name") or "")
        severity = str(r.get("severity") or "high")
        headline = str(r.get("headline") or alert_type)
        detail = str(r.get("detail_text") or "")
        ts = r.get("created_at", datetime.now(timezone.utc))

        fp = _fingerprint("sov_alert", f"{alert_id}:{ts}")

        sev_icon = "\U0001f6a8" if severity == "critical" else "\u26a0\ufe0f"
        location = f"{system_name} ({region})" if region else system_name

        events.append({
            "type": "sov_alert",
            "fingerprint": fp,
            "summary": headline,
            "embed": {
                "title": f"{sev_icon} Sovereignty: {headline}",
                "description": detail[:400] if detail else None,
                "color": _CLR_PURPLE,
                "fields": [
                    {"name": "System", "value": location, "inline": True},
                    {"name": "Alliance", "value": alliance or "N/A", "inline": True},
                    {"name": "Type", "value": alert_type.replace("_", " ").title(), "inline": True},
                ],
                "footer": {"text": "SupplyCore \u00b7 Sovereignty Intelligence"},
                "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            },
        })
    return events


def _collect_high_value_battles(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Collect recent large battles with high ISK destroyed."""
    try:
        rows = db.fetch_all(
            "SELECT br.battle_id, br.total_isk_destroyed, br.total_participants, "
            "       br.duration_seconds, br.start_time, br.end_time, "
            "       COALESCE(rs.solarSystemName, CONCAT('System #', br.system_id)) AS system_name, "
            "       COALESCE(rr.regionName, '') AS region_name "
            "FROM battle_rollups br "
            "LEFT JOIN ref_systems rs ON rs.system_id = br.system_id "
            "LEFT JOIN ref_regions rr ON rr.regionID = rs.region_id "
            "WHERE br.total_isk_destroyed >= 1000000000 "
            "  AND br.start_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 6 HOUR) "
            "ORDER BY br.total_isk_destroyed DESC "
            "LIMIT 5",
        )
    except Exception:
        return []

    events: list[dict[str, Any]] = []
    for r in rows:
        battle_id = str(r.get("battle_id") or "")
        isk = float(r.get("total_isk_destroyed") or 0)
        participants = int(r.get("total_participants") or 0)
        duration_s = int(r.get("duration_seconds") or 0)
        system = str(r.get("system_name") or "Unknown")
        region = str(r.get("region_name") or "")
        ts = r.get("start_time", datetime.now(timezone.utc))

        fp = _fingerprint("battle", battle_id)

        # Format ISK nicely
        if isk >= 1_000_000_000:
            isk_str = f"{isk / 1_000_000_000:.1f}B ISK"
        else:
            isk_str = f"{isk / 1_000_000:.0f}M ISK"

        # Duration
        if duration_s > 3600:
            dur_str = f"{duration_s // 3600}h {(duration_s % 3600) // 60}m"
        elif duration_s > 60:
            dur_str = f"{duration_s // 60}m"
        else:
            dur_str = f"{duration_s}s"

        location = f"{system} ({region})" if region else system

        # Scale the color based on ISK
        color = _CLR_RED if isk >= 10_000_000_000 else _CLR_AMBER

        events.append({
            "type": "battle",
            "fingerprint": fp,
            "summary": f"{isk_str} battle in {system}",
            "embed": {
                "title": f"\u2694\ufe0f Battle Report: {location}",
                "description": f"**{isk_str}** destroyed in a {dur_str} engagement",
                "color": color,
                "fields": [
                    {"name": "ISK Destroyed", "value": isk_str, "inline": True},
                    {"name": "Participants", "value": str(participants), "inline": True},
                    {"name": "Duration", "value": dur_str, "inline": True},
                    {"name": "Location", "value": location, "inline": True},
                ],
                "footer": {"text": "SupplyCore \u00b7 Battle Intelligence"},
                "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            },
        })
    return events


def _collect_system_health(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Generate a periodic system health summary if enough failures accumulate."""
    try:
        row = db.fetch_one(
            "SELECT "
            "  COUNT(*) AS total_runs, "
            "  SUM(CASE WHEN run_status = 'failed' THEN 1 ELSE 0 END) AS failed, "
            "  SUM(CASE WHEN run_status = 'success' THEN 1 ELSE 0 END) AS succeeded "
            "FROM sync_runs "
            "WHERE started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR)"
        )
    except Exception:
        return []

    if not row:
        return []

    total = int(row.get("total_runs") or 0)
    failed = int(row.get("failed") or 0)
    succeeded = int(row.get("succeeded") or 0)

    # Only report if there are meaningful failures (>20% failure rate)
    if total < 5 or failed == 0 or (failed / total) < 0.2:
        return []

    # Use hour-bucket fingerprint so we report at most once per hour
    hour_key = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
    fp = _fingerprint("health_summary", hour_key)
    fail_pct = round(failed / total * 100, 1)

    return [{
        "type": "health_summary",
        "fingerprint": fp,
        "summary": f"System health: {fail_pct}% failure rate",
        "embed": {
            "title": "\U0001f3e5 System Health Alert",
            "description": f"**{fail_pct}%** job failure rate in the last hour",
            "color": _CLR_RED if fail_pct > 50 else _CLR_AMBER,
            "fields": [
                {"name": "Total Runs", "value": str(total), "inline": True},
                {"name": "Succeeded", "value": str(succeeded), "inline": True},
                {"name": "Failed", "value": str(failed), "inline": True},
            ],
            "footer": {"text": "SupplyCore \u00b7 System Health"},
        },
    }]


def _collect_fishy_users(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Top 3 suspicious characters from friendly alliances, once per day."""
    # Day-bucket fingerprint so we only report once per calendar day (UTC).
    day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fp = _fingerprint("fishy_users", day_key)

    if _already_sent(db, fp):
        return []

    # Friendly alliance IDs: corp_contacts where standing > 0 and contact_type = 'alliance'
    try:
        rows = db.fetch_all(
            "SELECT cip.character_id, cip.risk_score, cip.risk_percentile, "
            "       cip.confidence, cip.behavioral_score, cip.graph_score, "
            "       cip.temporal_score, cip.risk_delta_24h, cip.signal_count, "
            "       cip.top_signals_json, "
            "       css.alliance_id, "
            "       COALESCE(emc_c.entity_name, CONCAT('Character #', cip.character_id)) AS char_name, "
            "       COALESCE(emc_a.entity_name, CONCAT('Alliance #', css.alliance_id)) AS alliance_name "
            "FROM character_intelligence_profiles cip "
            "JOIN character_suspicion_signals css ON css.character_id = cip.character_id "
            "JOIN corp_contacts cc ON cc.contact_id = css.alliance_id "
            "     AND cc.contact_type = 'alliance' AND cc.standing > 0 "
            "LEFT JOIN entity_metadata_cache emc_c "
            "     ON emc_c.entity_type = 'character' AND emc_c.entity_id = cip.character_id "
            "LEFT JOIN entity_metadata_cache emc_a "
            "     ON emc_a.entity_type = 'alliance' AND emc_a.entity_id = css.alliance_id "
            "WHERE cip.risk_score > 0 AND cip.confidence > 0.1 "
            "ORDER BY cip.risk_score DESC "
            "LIMIT 3",
        )
    except Exception:
        return []

    if not rows:
        return []

    # Build embed fields for each character
    fields: list[dict[str, Any]] = []
    for i, r in enumerate(rows, 1):
        char_name = str(r.get("char_name") or "Unknown")
        alliance_name = str(r.get("alliance_name") or "Unknown")
        risk = float(r.get("risk_score") or 0)
        pctl = float(r.get("risk_percentile") or 0) * 100
        confidence = float(r.get("confidence") or 0)
        delta_24h = float(r.get("risk_delta_24h") or 0)
        signals = int(r.get("signal_count") or 0)
        behavioral = float(r.get("behavioral_score") or 0)
        graph = float(r.get("graph_score") or 0)
        temporal = float(r.get("temporal_score") or 0)

        # Trend arrow
        if delta_24h > 0.01:
            trend = f" (+{delta_24h:.2f})"
        elif delta_24h < -0.01:
            trend = f" ({delta_24h:.2f})"
        else:
            trend = ""

        fields.append({
            "name": f"#{i} — {char_name}",
            "value": (
                f"**Alliance:** {alliance_name}\n"
                f"**Risk:** {risk:.3f} (P{pctl:.0f}){trend}\n"
                f"**Confidence:** {confidence:.2f} | **Signals:** {signals}\n"
                f"Behavioral `{behavioral:.2f}` · Graph `{graph:.2f}` · Temporal `{temporal:.2f}`"
            ),
            "inline": False,
        })

    return [{
        "type": "fishy_users",
        "fingerprint": fp,
        "summary": f"Top {len(rows)} fishy users for {day_key}",
        "embed": {
            "title": f"\U0001f440 Daily Fishy Users — {day_key}",
            "description": (
                f"Top {len(rows)} highest-risk characters in friendly alliances. "
                "Review these for potential counterintel concerns."
            ),
            "color": _CLR_AMBER,
            "fields": fields,
            "footer": {"text": "SupplyCore · Counterintelligence · Daily Digest"},
        },
    }]


# ── Main worker ─────────────────────────────────────────────────────

def run_discord_webhook_filter(db: SupplyCoreDb) -> dict[str, object]:
    """Scan for interesting events and send curated Discord webhook messages.

    Returns a standard job result dict.
    """
    webhook_url = db.fetch_app_setting("discord_webhook_url", "").strip()

    if not webhook_url:
        return {
            "status": "skipped",
            "rows_processed": 0,
            "rows_written": 0,
            "summary": "discord_webhook_url not configured in app_settings. "
                       "Set it in Settings > Integrations to enable.",
            "warnings": ["discord_webhook_url not configured"],
        }

    # Ensure tracker table.
    db.execute(_ENSURE_TABLE_SQL)

    # ── Collect events from all sources ──────────────────────────────
    all_events: list[dict[str, Any]] = []
    collector_stats: dict[str, int] = {}

    collectors = [
        ("job_failures", lambda: _collect_job_failures(db, lookback_hours=4)),
        ("deal_alerts", lambda: _collect_deal_alerts(db)),
        ("sov_alerts", lambda: _collect_sovereignty_alerts(db)),
        ("battles", lambda: _collect_high_value_battles(db)),
        ("health", lambda: _collect_system_health(db)),
        ("fishy_users", lambda: _collect_fishy_users(db)),
    ]

    for name, collector in collectors:
        try:
            events = collector()
            collector_stats[name] = len(events)
            all_events.extend(events)
        except Exception as exc:
            logger.warning("Collector %s failed: %s", name, exc)
            collector_stats[name] = 0

    logger.info(
        "Collected events: %s",
        ", ".join(f"{k}={v}" for k, v in collector_stats.items()),
    )

    if not all_events:
        return {
            "status": "success",
            "rows_processed": 0,
            "rows_written": 0,
            "summary": "No interesting events to report.",
            "meta": {"collector_stats": collector_stats},
        }

    # ── Filter out already-sent events ───────────────────────────────
    unsent: list[dict[str, Any]] = []
    for event in all_events:
        if not _already_sent(db, event["fingerprint"]):
            unsent.append(event)

    if not unsent:
        return {
            "status": "success",
            "rows_processed": len(all_events),
            "rows_written": 0,
            "summary": f"Found {len(all_events)} event(s) but all already sent.",
            "meta": {"collector_stats": collector_stats, "total_found": len(all_events)},
        }

    # ── Prioritize: failures > sov > battles > deals > health ────────
    priority_order = ["health_summary", "job_failure", "sov_alert", "battle", "fishy_users", "deal_alert"]
    unsent.sort(key=lambda e: priority_order.index(e["type"]) if e["type"] in priority_order else 99)

    # ── Batch embeds (up to 10 per message, max _MAX_MESSAGES_PER_RUN)
    # Group same-type events into single messages where possible.
    messages_to_send: list[dict[str, Any]] = []
    current_embeds: list[dict[str, Any]] = []
    current_type: str | None = None
    current_events: list[dict[str, Any]] = []

    for event in unsent:
        if current_type is not None and (event["type"] != current_type or len(current_embeds) >= 10):
            messages_to_send.append({
                "username": "SupplyCore Intel",
                "embeds": current_embeds[:10],
                "_events": current_events,
            })
            current_embeds = []
            current_events = []

        current_type = event["type"]
        embed = event["embed"]
        # Strip None description
        if embed.get("description") is None:
            embed.pop("description", None)
        current_embeds.append(embed)
        current_events.append(event)

    if current_embeds:
        messages_to_send.append({
            "username": "SupplyCore Intel",
            "embeds": current_embeds[:10],
            "_events": current_events,
        })

    # ── Send messages (capped) ───────────────────────────────────────
    sent_count = 0
    events_sent = 0
    warnings: list[str] = []
    rate_limited = False

    for msg in messages_to_send[:_MAX_MESSAGES_PER_RUN]:
        if rate_limited:
            break

        events_in_msg = msg.pop("_events")
        try:
            ok = _send_webhook(webhook_url, msg)
        except urllib.error.HTTPError:
            rate_limited = True
            warnings.append("Hit Discord rate limit — stopped sending.")
            break

        if ok:
            sent_count += 1
            for event in events_in_msg:
                _mark_sent(
                    db,
                    event["fingerprint"],
                    event["type"],
                    event.get("summary", event.get("embed", {}).get("title", ""))[:500],
                )
                events_sent += 1
        else:
            warnings.append(f"Failed to send {len(events_in_msg)} event(s) of type {events_in_msg[0]['type']}.")

        if sent_count < len(messages_to_send[:_MAX_MESSAGES_PER_RUN]):
            time.sleep(_INTER_MESSAGE_DELAY)

    return {
        "status": "success",
        "rows_processed": len(all_events),
        "rows_written": events_sent,
        "summary": (
            f"Sent {sent_count} message(s) with {events_sent} event(s) to Discord. "
            f"Sources: {', '.join(f'{k}={v}' for k, v in collector_stats.items())}."
        ),
        "warnings": warnings,
        "meta": {
            "messages_sent": sent_count,
            "events_sent": events_sent,
            "events_found": len(all_events),
            "events_new": len(unsent),
            "rate_limited": rate_limited,
            "collector_stats": collector_stats,
        },
    }
