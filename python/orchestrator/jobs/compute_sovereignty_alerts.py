"""Sovereignty Alert Computation.

Cross-references sovereignty data with diplomatic standings to generate
operational alerts:

- Friendly sov under attack (critical)
- Hostile sov under attack (info)
- Sov ownership lost/gained (critical/info)
- Low ADM warnings (warning)
- Upcoming vulnerability windows (warning)

Alert lifecycle: active → stale → resolved.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job

log = logging.getLogger(__name__)

JOB_KEY = "compute_sovereignty_alerts"

# Configurable thresholds.
_LOW_ADM_THRESHOLD = 3.0
_VULN_WINDOW_HOURS = 2
_STALE_MINUTES = 5  # 2× the 120s job interval


def _load_standing_ids(db: SupplyCoreDb) -> dict[str, set[int]]:
    """Load friendly/hostile alliance IDs from corp_contacts.

    Mirrors the logic from theater_analysis._load_side_configuration_ids
    but imported as a standalone to avoid coupling to the theater module.
    """
    friendly: set[int] = set()
    hostile: set[int] = set()

    try:
        rows = db.fetch_all(
            """SELECT contact_id, contact_type, standing
               FROM corp_contacts
               WHERE contact_type = 'alliance' AND standing != 0"""
        )
        for row in rows:
            contact_id = int(row.get("contact_id") or 0)
            standing = float(row.get("standing") or 0)
            if contact_id <= 0:
                continue
            if standing > 0:
                friendly.add(contact_id)
            elif standing < 0:
                hostile.add(contact_id)
    except Exception as exc:
        log.warning("Failed to load standings: %s", exc)

    return {"friendly": friendly, "hostile": hostile}


def _upsert_alert(
    db: SupplyCoreDb,
    *,
    alert_type: str,
    severity: str,
    title: str,
    solar_system_id: int | None = None,
    alliance_id: int | None = None,
    structure_id: int | None = None,
    campaign_id: int | None = None,
    details: dict | None = None,
    now_sql: str,
) -> None:
    """Upsert an alert: update last_seen_at if active, else create new."""
    details_json = json.dumps(details) if details else None

    # Try to find an existing active alert of the same type for the same target.
    where_parts = ["alert_type = %s", "status IN ('active','stale')"]
    where_params: list = [alert_type]

    if campaign_id is not None:
        where_parts.append("campaign_id = %s")
        where_params.append(campaign_id)
    elif structure_id is not None:
        where_parts.append("structure_id = %s")
        where_params.append(structure_id)
    elif solar_system_id is not None:
        where_parts.append("solar_system_id = %s")
        where_params.append(solar_system_id)

    existing = db.fetch_one(
        f"SELECT id FROM sovereignty_alerts WHERE {' AND '.join(where_parts)} LIMIT 1",
        where_params,
    )

    if existing:
        db.execute(
            "UPDATE sovereignty_alerts SET last_seen_at = %s, status = 'active', "
            "severity = %s, title = %s, details_json = %s WHERE id = %s",
            [now_sql, severity, title, details_json, existing["id"]],
        )
    else:
        db.execute(
            """INSERT INTO sovereignty_alerts
               (alert_type, solar_system_id, alliance_id, structure_id, campaign_id,
                severity, title, details_json, status, detected_at, last_seen_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'active', %s, %s)""",
            [alert_type, solar_system_id, alliance_id, structure_id, campaign_id,
             severity, title, details_json, now_sql, now_sql],
        )


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    standings = _load_standing_ids(db)
    friendly = standings["friendly"]
    hostile = standings["hostile"]
    warnings: list[str] = []
    alerts_created = 0

    if not friendly and not hostile:
        warnings.append(
            "No standings data available — run corp_standings_sync first. "
            "Sovereignty alerts require diplomatic standings to classify friendly/hostile."
        )
        return {
            "rows_processed": 0,
            "rows_written": 0,
            "warnings": warnings,
            "summary": "Skipped — no standings configured.",
        }

    # ── 1. Campaign alerts ───────────────────────────────────────────────
    active_campaigns = db.fetch_all(
        "SELECT campaign_id, event_type, solar_system_id, defender_id, "
        "attackers_score, defender_score FROM sovereignty_campaigns WHERE is_active = 1"
    )

    for c in active_campaigns:
        defender_id = int(c["defender_id"])
        campaign_id = int(c["campaign_id"])
        system_id = int(c["solar_system_id"])

        # Resolve system name for title.
        sys_row = db.fetch_one(
            "SELECT system_name FROM ref_systems WHERE system_id = %s", (system_id,)
        )
        sys_name = str(sys_row["system_name"]) if sys_row else f"System {system_id}"

        if defender_id in friendly:
            _upsert_alert(
                db,
                alert_type="campaign_friendly",
                severity="critical",
                title=f"Friendly sov under attack in {sys_name}",
                solar_system_id=system_id,
                alliance_id=defender_id,
                campaign_id=campaign_id,
                details={
                    "event_type": c["event_type"],
                    "attackers_score": float(c["attackers_score"]),
                    "defender_score": float(c["defender_score"]),
                },
                now_sql=now_sql,
            )
            alerts_created += 1
        elif defender_id in hostile:
            _upsert_alert(
                db,
                alert_type="campaign_hostile",
                severity="info",
                title=f"Hostile sov under attack in {sys_name}",
                solar_system_id=system_id,
                alliance_id=defender_id,
                campaign_id=campaign_id,
                details={
                    "event_type": c["event_type"],
                    "attackers_score": float(c["attackers_score"]),
                    "defender_score": float(c["defender_score"]),
                },
                now_sql=now_sql,
            )
            alerts_created += 1

    # Resolve campaign alerts where campaign is no longer active.
    db.execute(
        """UPDATE sovereignty_alerts
           SET status = 'resolved', resolved_at = %s
           WHERE alert_type IN ('campaign_friendly', 'campaign_hostile')
             AND status = 'active'
             AND campaign_id IS NOT NULL
             AND campaign_id NOT IN (
                 SELECT campaign_id FROM sovereignty_campaigns WHERE is_active = 1
             )""",
        [now_sql],
    )

    # ── 2. Ownership change alerts ───────────────────────────────────────
    recent_changes = db.fetch_all(
        """SELECT system_id, previous_alliance_id, new_alliance_id,
                  previous_owner_entity_id, new_owner_entity_id
           FROM sovereignty_map_history
           WHERE changed_at > DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 HOUR)"""
    )

    for ch in recent_changes:
        system_id = int(ch["system_id"])
        prev_ally = int(ch.get("previous_alliance_id") or ch.get("previous_owner_entity_id") or 0)
        new_ally = int(ch.get("new_alliance_id") or ch.get("new_owner_entity_id") or 0)

        sys_row = db.fetch_one(
            "SELECT system_name FROM ref_systems WHERE system_id = %s", (system_id,)
        )
        sys_name = str(sys_row["system_name"]) if sys_row else f"System {system_id}"

        if prev_ally in friendly and new_ally not in friendly:
            _upsert_alert(
                db,
                alert_type="sov_lost",
                severity="critical",
                title=f"Sovereignty lost in {sys_name}",
                solar_system_id=system_id,
                alliance_id=prev_ally,
                details={"previous_owner": prev_ally, "new_owner": new_ally},
                now_sql=now_sql,
            )
            alerts_created += 1

        if new_ally in friendly and prev_ally not in friendly:
            _upsert_alert(
                db,
                alert_type="sov_gained",
                severity="info",
                title=f"Sovereignty gained in {sys_name}",
                solar_system_id=system_id,
                alliance_id=new_ally,
                details={"previous_owner": prev_ally, "new_owner": new_ally},
                now_sql=now_sql,
            )
            alerts_created += 1

    # ── 3. Low ADM warnings ──────────────────────────────────────────────
    if friendly:
        placeholders = ",".join(["%s"] * len(friendly))
        low_adm_structures = db.fetch_all(
            f"""SELECT structure_id, alliance_id, solar_system_id,
                       vulnerability_occupancy_level
                FROM sovereignty_structures
                WHERE alliance_id IN ({placeholders})
                  AND vulnerability_occupancy_level IS NOT NULL
                  AND vulnerability_occupancy_level < %s""",
            [*friendly, _LOW_ADM_THRESHOLD],
        )

        for s in low_adm_structures:
            system_id = int(s["solar_system_id"])
            sys_row = db.fetch_one(
                "SELECT system_name FROM ref_systems WHERE system_id = %s", (system_id,)
            )
            sys_name = str(sys_row["system_name"]) if sys_row else f"System {system_id}"
            adm = float(s["vulnerability_occupancy_level"])

            _upsert_alert(
                db,
                alert_type="low_adm",
                severity="warning",
                title=f"Low ADM ({adm:.1f}) in {sys_name}",
                solar_system_id=system_id,
                alliance_id=int(s["alliance_id"]),
                structure_id=int(s["structure_id"]),
                details={"adm": adm, "threshold": _LOW_ADM_THRESHOLD},
                now_sql=now_sql,
            )
            alerts_created += 1

        # Resolve low ADM alerts where ADM has recovered.
        db.execute(
            f"""UPDATE sovereignty_alerts sa
                SET sa.status = 'resolved', sa.resolved_at = %s
                WHERE sa.alert_type = 'low_adm'
                  AND sa.status = 'active'
                  AND sa.structure_id IS NOT NULL
                  AND sa.structure_id NOT IN (
                      SELECT ss.structure_id FROM sovereignty_structures ss
                      WHERE ss.alliance_id IN ({placeholders})
                        AND ss.vulnerability_occupancy_level IS NOT NULL
                        AND ss.vulnerability_occupancy_level < %s
                  )""",
            [now_sql, *friendly, _LOW_ADM_THRESHOLD],
        )

    # ── 4. Vulnerability window warnings ─────────────────────────────────
    if friendly:
        placeholders = ",".join(["%s"] * len(friendly))
        vuln_soon = db.fetch_all(
            f"""SELECT structure_id, alliance_id, solar_system_id,
                       vulnerable_start_time, vulnerable_end_time
                FROM sovereignty_structures
                WHERE alliance_id IN ({placeholders})
                  AND vulnerable_start_time IS NOT NULL
                  AND vulnerable_start_time <= DATE_ADD(UTC_TIMESTAMP(), INTERVAL %s HOUR)
                  AND vulnerable_end_time >= UTC_TIMESTAMP()""",
            [*friendly, _VULN_WINDOW_HOURS],
        )

        for s in vuln_soon:
            system_id = int(s["solar_system_id"])
            sys_row = db.fetch_one(
                "SELECT system_name FROM ref_systems WHERE system_id = %s", (system_id,)
            )
            sys_name = str(sys_row["system_name"]) if sys_row else f"System {system_id}"

            _upsert_alert(
                db,
                alert_type="vuln_window",
                severity="warning",
                title=f"Vulnerability window active/upcoming in {sys_name}",
                solar_system_id=system_id,
                alliance_id=int(s["alliance_id"]),
                structure_id=int(s["structure_id"]),
                details={
                    "vulnerable_start": str(s["vulnerable_start_time"])[:19],
                    "vulnerable_end": str(s["vulnerable_end_time"])[:19],
                },
                now_sql=now_sql,
            )
            alerts_created += 1

        # Resolve vuln window alerts where window has passed.
        db.execute(
            """UPDATE sovereignty_alerts
               SET status = 'resolved', resolved_at = %s
               WHERE alert_type = 'vuln_window'
                 AND status = 'active'
                 AND structure_id IS NOT NULL
                 AND structure_id NOT IN (
                     SELECT ss.structure_id FROM sovereignty_structures ss
                     WHERE ss.vulnerable_end_time >= UTC_TIMESTAMP()
                 )""",
            [now_sql],
        )

    # ── 5. Mark stale alerts ─────────────────────────────────────────────
    db.execute(
        """UPDATE sovereignty_alerts
           SET status = 'stale'
           WHERE status = 'active'
             AND last_seen_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)""",
        [_STALE_MINUTES],
    )

    db.execute(
        """INSERT INTO sync_state (dataset_key, sync_mode, status, last_success_at, last_row_count)
           VALUES ('sovereignty.alerts', 'full', 'success', UTC_TIMESTAMP(), %s)
           ON DUPLICATE KEY UPDATE status='success', last_success_at=UTC_TIMESTAMP(),
               last_row_count=VALUES(last_row_count), updated_at=UTC_TIMESTAMP()""",
        [alerts_created],
    )

    return {
        "rows_processed": len(active_campaigns) + len(recent_changes),
        "rows_written": alerts_created,
        "warnings": warnings,
        "summary": f"Generated {alerts_created} sovereignty alerts.",
        "meta": {"alerts_created": alerts_created, "friendly_count": len(friendly), "hostile_count": len(hostile)},
    }


def run_compute_sovereignty_alerts(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(
        db,
        job_key=JOB_KEY,
        phase="A",
        objective="sovereignty alerts",
        processor=_processor,
    )
