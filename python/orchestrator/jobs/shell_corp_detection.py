"""Shell corp detection — flag corporations with suspicious structural patterns.

A shell corp is typically:
  * Young (recently created)
  * Low member count or rapid turnover
  * Very few killmails relative to members
  * Acts as a pass-through (members join then leave quickly)
  * May connect hostile and friendly networks

Uses data from:
  * ``evewho_alliance_member_sync`` / ``tracked_alliance_member_sync`` for membership data
  * ``killmail_events`` / ``killmail_attackers`` for combat activity
  * Neo4j ``MEMBER_OF`` relationships for historical membership with from/to dates

Output: ``shell_corp_indicators`` with per-corp shell score and flags.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE = 500

# Shell score weights
W_AGE = 0.15           # young corps score higher
W_TURNOVER = 0.25      # high turnover = suspicious
W_ACTIVITY = 0.20      # low kill activity per member
W_TENURE = 0.20        # short average member tenure
W_SIZE = 0.10          # very small corps
W_MEMBER_RATIO = 0.10  # unique members >> current members = churn

# Thresholds
YOUNG_CORP_DAYS = 90
SHORT_TENURE_DAYS = 30
MIN_CORPS_TO_PROCESS = 1


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _shell_score(
    age_days: int,
    turnover_ratio: float,
    killmail_count: int,
    member_count: int,
    avg_tenure: int,
    unique_members: int,
) -> tuple[float, list[str]]:
    """Compute shell score (0-1) and list of flag reasons."""
    score = 0.0
    flags: list[str] = []

    # Age factor: newer corps are more suspicious
    if age_days <= YOUNG_CORP_DAYS:
        age_score = 1.0 - (age_days / YOUNG_CORP_DAYS)
        score += W_AGE * age_score
        if age_days <= 30:
            flags.append("very_young_corp")
        else:
            flags.append("young_corp")
    elif age_days <= 180:
        score += W_AGE * 0.3

    # Turnover: high ratio means lots of join/leave
    if turnover_ratio > 2.0:
        score += W_TURNOVER * min(1.0, turnover_ratio / 5.0)
        flags.append("high_turnover")
    elif turnover_ratio > 1.0:
        score += W_TURNOVER * 0.4
        flags.append("moderate_turnover")

    # Activity: killmails per member
    if member_count > 0:
        kills_per_member = killmail_count / member_count
        if kills_per_member < 0.5:
            score += W_ACTIVITY * (1.0 - min(1.0, kills_per_member * 2))
            flags.append("low_combat_activity")
    else:
        score += W_ACTIVITY
        flags.append("empty_corp")

    # Tenure: short average membership duration
    if avg_tenure > 0 and avg_tenure <= SHORT_TENURE_DAYS:
        score += W_TENURE * (1.0 - avg_tenure / SHORT_TENURE_DAYS)
        flags.append("short_member_tenure")
    elif avg_tenure == 0 and unique_members > 0:
        score += W_TENURE * 0.5

    # Size: very small
    if 0 < member_count <= 5:
        score += W_SIZE
        flags.append("micro_corp")
    elif member_count <= 15:
        score += W_SIZE * 0.5

    # Churn ratio: unique members seen >> current members
    if member_count > 0 and unique_members > member_count * 2:
        churn = unique_members / member_count
        score += W_MEMBER_RATIO * min(1.0, churn / 10.0)
        flags.append("high_churn_ratio")

    return (round(min(1.0, score), 4), flags)


def run_shell_corp_detection(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Detect shell corps among all known corporations in Neo4j."""
    lock_key = "shell_corp_detection"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()

    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    client = Neo4jClient(config) if config.enabled else None

    try:
        # Get corporations from Neo4j with membership data
        corp_data = []
        if client:
            corp_data = client.query(
                """
                MATCH (corp:Corporation)<-[:CURRENT_CORP]-(c:Character)
                OPTIONAL MATCH (corp)-[:PART_OF]->(a:Alliance)
                WITH corp, a,
                     count(DISTINCT c) AS member_count
                RETURN corp.corporation_id AS corporation_id,
                       a.alliance_id AS alliance_id,
                       member_count
                ORDER BY member_count ASC
                """
            )

        if not corp_data:
            finish_job_run(db, job, status="success",
                           rows_processed=0, rows_written=0)
            return {"status": "success", "rows_processed": 0, "rows_written": 0,
                    "duration_ms": int((time.perf_counter() - started) * 1000)}

        # Get historical membership stats from Neo4j (unique members, avg tenure)
        history_data = {}
        if client:
            hist_rows = client.query(
                """
                MATCH (c:Character)-[m:MEMBER_OF]->(corp:Corporation)
                WITH corp.corporation_id AS corp_id,
                     count(DISTINCT c) AS unique_members,
                     avg(CASE
                         WHEN m.to IS NOT NULL AND m.from IS NOT NULL
                         THEN duration.inDays(m.from, m.to).days
                         ELSE 0
                     END) AS avg_tenure_days
                RETURN corp_id, unique_members, toInteger(avg_tenure_days) AS avg_tenure_days
                """
            )
            for r in hist_rows:
                history_data[int(r["corp_id"])] = {
                    "unique_members": int(r.get("unique_members") or 0),
                    "avg_tenure_days": int(r.get("avg_tenure_days") or 0),
                }

        # Get killmail counts per corp (last 90 days)
        corp_ids = [int(r["corporation_id"]) for r in corp_data if r.get("corporation_id")]
        kill_counts: dict[int, int] = {}
        if corp_ids:
            for i in range(0, len(corp_ids), BATCH_SIZE):
                batch = corp_ids[i:i + BATCH_SIZE]
                placeholders = ",".join(["%s"] * len(batch))
                rows = db.fetch_all(
                    f"""
                    SELECT ka.corporation_id, COUNT(DISTINCT ka.sequence_id) AS km_count
                    FROM killmail_attackers ka
                    INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
                    WHERE ka.corporation_id IN ({placeholders})
                      AND ke.effective_killmail_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 90 DAY)
                    GROUP BY ka.corporation_id
                    """,
                    batch,
                )
                for r in rows:
                    kill_counts[int(r["corporation_id"])] = int(r.get("km_count") or 0)

        # Compute shell scores
        insert_rows = []
        for corp in corp_data:
            cid = int(corp["corporation_id"])
            aid = int(corp.get("alliance_id") or 0) if corp.get("alliance_id") else None
            member_count = int(corp.get("member_count") or 0)

            hist = history_data.get(cid, {})
            unique_members = hist.get("unique_members", 0)
            avg_tenure = hist.get("avg_tenure_days", 0)
            km_count = kill_counts.get(cid, 0)

            # Estimate corp age from earliest MEMBER_OF relationship (rough proxy)
            # For now, use a default if we can't determine age
            age_days = 365  # default assumption; would need ESI corp creation date

            # Turnover ratio: unique members seen / current members
            turnover = unique_members / max(member_count, 1) if unique_members > 0 else 0.0

            shell_s, flags = _shell_score(
                age_days=age_days,
                turnover_ratio=turnover,
                killmail_count=km_count,
                member_count=member_count,
                avg_tenure=avg_tenure,
                unique_members=unique_members,
            )

            # Only store corps with non-trivial shell score
            if shell_s >= 0.15 or flags:
                insert_rows.append((
                    cid, aid, member_count, age_days,
                    round(turnover, 4), km_count, unique_members, avg_tenure,
                    shell_s, json_dumps_safe(flags), computed_at,
                ))

        rows_processed = len(corp_data)

        if insert_rows:
            db.execute_many(
                """
                INSERT INTO shell_corp_indicators
                    (corporation_id, alliance_id, member_count, age_days,
                     turnover_ratio_90d, killmail_count_90d, unique_members_90d,
                     avg_member_tenure_days, shell_score, flags_json, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    alliance_id = VALUES(alliance_id), member_count = VALUES(member_count),
                    age_days = VALUES(age_days), turnover_ratio_90d = VALUES(turnover_ratio_90d),
                    killmail_count_90d = VALUES(killmail_count_90d),
                    unique_members_90d = VALUES(unique_members_90d),
                    avg_member_tenure_days = VALUES(avg_member_tenure_days),
                    shell_score = VALUES(shell_score), flags_json = VALUES(flags_json),
                    computed_at = VALUES(computed_at)
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
            "duration_ms": int((time.perf_counter() - started) * 1000),
        }
    except Exception as exc:
        finish_job_run(db, job, status="failed",
                       rows_processed=rows_processed, rows_written=rows_written,
                       error_text=str(exc))
        return {"status": "failed", "error_text": str(exc),
                "rows_processed": rows_processed, "rows_written": rows_written}
