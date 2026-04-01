"""Pre-op join detection — flag characters who joined shortly before significant ops.

Detects the pattern: a character joins a corp/alliance, and within a short window
a significant battle occurs involving that org. This is a classic infiltration signal.

Uses:
  * ``character_alliance_history`` — corp/alliance join dates from ESI
  * ``battle_rollups`` + ``battle_participants`` — significant battles
  * Neo4j ``MEMBER_OF`` relationships with ``from`` dates

Output: rows in ``character_counterintel_evidence`` with evidence_key = 'pre_op_join'.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Max days between join date and battle to flag as suspicious
PRE_OP_WINDOW_DAYS = 14
# Minimum battle size to consider significant
MIN_BATTLE_PARTICIPANTS = 20
# Lookback for recent battles
LOOKBACK_DAYS = 30
BATCH_SIZE = 500


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def run_pre_op_join_detection(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Detect characters who joined an org shortly before a significant battle."""
    lock_key = "pre_op_join_detection"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()

    try:
        cutoff = (datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

        # Find characters whose alliance join date is recent and who participated
        # in a significant battle within PRE_OP_WINDOW_DAYS of joining.
        #
        # character_alliance_history has: character_id, alliance_id, started_at, ended_at
        # battle_participants has: battle_id, character_id, alliance_id
        # battle_rollups has: battle_id, started_at, participant_count

        suspects = db.fetch_all(
            """
            SELECT DISTINCT
                cah.character_id,
                cah.alliance_id,
                cah.started_at AS join_date,
                br.battle_id,
                br.started_at AS battle_date,
                br.participant_count,
                DATEDIFF(br.started_at, cah.started_at) AS days_after_join
            FROM character_alliance_history cah
            INNER JOIN battle_participants bp
                ON bp.character_id = cah.character_id
                AND bp.alliance_id = cah.alliance_id
            INNER JOIN battle_rollups br
                ON br.battle_id = bp.battle_id
            WHERE cah.started_at >= %s
              AND br.participant_count >= %s
              AND br.started_at >= cah.started_at
              AND DATEDIFF(br.started_at, cah.started_at) <= %s
            ORDER BY days_after_join ASC
            LIMIT 2000
            """,
            (cutoff, MIN_BATTLE_PARTICIPANTS, PRE_OP_WINDOW_DAYS),
        )

        if not suspects:
            finish_job_run(db, job, status="success",
                           rows_processed=0, rows_written=0)
            return {"status": "success", "rows_processed": 0, "rows_written": 0,
                    "duration_ms": int((time.perf_counter() - started) * 1000)}

        rows_processed = len(suspects)

        # Group by character — take the most suspicious (shortest days_after_join)
        by_char: dict[int, dict] = {}
        for s in suspects:
            cid = int(s["character_id"])
            days = int(s.get("days_after_join") or 0)
            if cid not in by_char or days < by_char[cid].get("days_after_join", 999):
                by_char[cid] = s

        # Write evidence entries
        insert_rows = []
        for cid, s in by_char.items():
            days_after = int(s.get("days_after_join") or 0)
            participant_count = int(s.get("participant_count") or 0)

            # Higher signal for shorter join-to-battle gap
            if days_after <= 3:
                confidence = "high"
                z_score = 3.0
            elif days_after <= 7:
                confidence = "medium"
                z_score = 2.0
            else:
                confidence = "low"
                z_score = 1.0

            evidence_text = (
                f"Joined alliance {s.get('alliance_id')} on {s.get('join_date')} "
                f"and participated in battle ({participant_count} pilots) "
                f"{days_after} days later"
            )

            payload = {
                "alliance_id": int(s.get("alliance_id") or 0),
                "join_date": str(s.get("join_date", "")),
                "battle_id": str(s.get("battle_id", "")),
                "battle_date": str(s.get("battle_date", "")),
                "days_after_join": days_after,
                "battle_participant_count": participant_count,
            }

            insert_rows.append((
                cid, "pre_op_join", "all_time",
                float(days_after),  # evidence_value = days after join
                float(PRE_OP_WINDOW_DAYS),  # expected_value = window
                float(PRE_OP_WINDOW_DAYS - days_after),  # deviation
                z_score, z_score,  # z_score, mad_score
                None,  # cohort_percentile
                confidence,
                evidence_text,
                json_dumps_safe(payload),
                computed_at,
                # ON DUPLICATE KEY UPDATE values:
                float(days_after), float(PRE_OP_WINDOW_DAYS),
                float(PRE_OP_WINDOW_DAYS - days_after),
                z_score, z_score, None, confidence,
                evidence_text, json_dumps_safe(payload), computed_at,
            ))

        if insert_rows:
            db.execute_many(
                """
                INSERT INTO character_counterintel_evidence
                    (character_id, evidence_key, window_label,
                     evidence_value, expected_value, deviation_value,
                     z_score, mad_score, cohort_percentile,
                     confidence_flag, evidence_text, evidence_payload_json,
                     computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    evidence_value = %s, expected_value = %s,
                    deviation_value = %s, z_score = %s, mad_score = %s,
                    cohort_percentile = %s, confidence_flag = %s,
                    evidence_text = %s, evidence_payload_json = %s,
                    computed_at = %s
                """,
                insert_rows,
            )
            rows_written = len(insert_rows)

        finish_job_run(db, job, status="success",
                       rows_processed=rows_processed, rows_written=rows_written)
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_written": rows_written,
            "suspects_flagged": rows_written,
            "duration_ms": int((time.perf_counter() - started) * 1000),
        }
    except Exception as exc:
        finish_job_run(db, job, status="failed",
                       rows_processed=rows_processed, rows_written=rows_written,
                       error_text=str(exc))
        return {"status": "failed", "error_text": str(exc),
                "rows_processed": rows_processed, "rows_written": rows_written}
