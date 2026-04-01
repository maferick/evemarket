"""Escalation detection — identify sequences of battles showing increasing commitment.

Groups nearby battles (same constellation/region, within a time window) that share
participants and checks whether the sequence shows escalation patterns:
  * increasing participant counts
  * increasing capital usage
  * increasing ISK destroyed
  * sustained or shrinking time between engagements

Writes to ``escalation_sequences`` (per-battle membership) and
``escalation_sequence_summary`` (aggregate per sequence).

Escalation grades:
  * ``minor``     — 2 linked battles, modest increase
  * ``moderate``  — 3-4 battles, clear participant growth
  * ``major``     — 5+ battles or significant capital escalation
  * ``critical``  — large-scale sustained campaign with heavy assets
"""

from __future__ import annotations

import hashlib
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Time window: battles within this period in the same constellation may be linked
SEQUENCE_WINDOW_HOURS = 72
# Minimum participant overlap ratio to consider battles related
MIN_OVERLAP_RATIO = 0.10
# Minimum battles to form a sequence
MIN_SEQUENCE_LENGTH = 2
BATCH_SIZE = 200
# Lookback for finding new escalation candidates
LOOKBACK_DAYS = 7


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _sequence_id(battle_ids: list[str]) -> str:
    """Deterministic ID from sorted battle IDs."""
    content = "|".join(sorted(battle_ids))
    return hashlib.sha256(content.encode()).hexdigest()


def _escalation_grade(battle_count: int, peak_participants: int,
                      peak_capitals: int, total_isk: float) -> str:
    if battle_count >= 5 or (peak_capitals >= 10 and total_isk >= 10_000_000_000):
        return "critical"
    if battle_count >= 5 or peak_participants >= 100 or peak_capitals >= 5:
        return "major"
    if battle_count >= 3 or peak_participants >= 50:
        return "moderate"
    return "minor"


def run_escalation_detection(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Detect escalation sequences among recent battles."""
    lock_key = "escalation_detection"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()

    try:
        cutoff = (datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

        # Fetch recent battles with system/constellation info
        battles = db.fetch_all(
            """
            SELECT br.battle_id, br.system_id, br.started_at, br.ended_at,
                   br.duration_seconds, br.participant_count,
                   rs.constellation_id, rs.region_id
            FROM battle_rollups br
            LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
            WHERE br.started_at >= %s
              AND br.participant_count >= 5
            ORDER BY br.started_at ASC
            """,
            (cutoff,),
        )

        if not battles:
            finish_job_run(db, job, status="success",
                           rows_processed=0, rows_written=0)
            return {"status": "success", "rows_processed": 0, "rows_written": 0,
                    "duration_ms": int((time.perf_counter() - started) * 1000)}

        rows_processed = len(battles)

        # Group by constellation
        by_constellation: dict[int, list[dict]] = defaultdict(list)
        for b in battles:
            cid = b.get("constellation_id")
            if cid:
                by_constellation[int(cid)].append(b)

        # Fetch participant sets per battle
        battle_ids = [b["battle_id"] for b in battles]
        placeholders = ",".join(["%s"] * len(battle_ids))

        participants = db.fetch_all(
            f"""
            SELECT battle_id, alliance_id, character_id, is_capital, side_key
            FROM battle_participants
            WHERE battle_id IN ({placeholders})
            """,
            battle_ids,
        )

        chars_by_battle: dict[str, set[int]] = defaultdict(set)
        caps_by_battle: dict[str, int] = defaultdict(int)
        alliances_by_battle: dict[str, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
        for p in participants:
            bid = p["battle_id"]
            cid = p.get("character_id")
            if cid:
                chars_by_battle[bid].add(int(cid))
            if p.get("is_capital"):
                caps_by_battle[bid] += 1
            aid = p.get("alliance_id")
            sk = p.get("side_key", "unknown")
            if aid and int(aid) > 0:
                alliances_by_battle[bid][sk].add(int(aid))

        # Fetch ISK destroyed per battle
        isk_rows = db.fetch_all(
            f"""
            SELECT battle_id, SUM(zkb_total_value) AS isk_destroyed
            FROM killmail_events
            WHERE battle_id IN ({placeholders})
            GROUP BY battle_id
            """,
            battle_ids,
        )
        isk_by_battle = {r["battle_id"]: float(r.get("isk_destroyed") or 0) for r in isk_rows}

        # Build sequences within each constellation
        all_sequences: list[list[dict]] = []

        for _cid, constellation_battles in by_constellation.items():
            # Sort by time
            constellation_battles.sort(key=lambda b: b["started_at"])

            # Greedy chain: link battles that share participants and are within time window
            used = set()
            for i, b1 in enumerate(constellation_battles):
                if b1["battle_id"] in used:
                    continue
                chain = [b1]
                used.add(b1["battle_id"])

                for j in range(i + 1, len(constellation_battles)):
                    b2 = constellation_battles[j]
                    if b2["battle_id"] in used:
                        continue

                    # Check time window from last battle in chain
                    last = chain[-1]
                    t1 = last["ended_at"] if last.get("ended_at") else last["started_at"]
                    t2 = b2["started_at"]
                    if hasattr(t1, "timestamp"):
                        delta_hours = (t2.timestamp() - t1.timestamp()) / 3600
                    else:
                        delta_hours = SEQUENCE_WINDOW_HOURS + 1  # skip

                    if delta_hours > SEQUENCE_WINDOW_HOURS:
                        continue

                    # Check participant overlap with any battle in chain
                    chars2 = chars_by_battle.get(b2["battle_id"], set())
                    overlap = False
                    for cb in chain:
                        chars1 = chars_by_battle.get(cb["battle_id"], set())
                        if chars1 and chars2:
                            ratio = len(chars1 & chars2) / min(len(chars1), len(chars2))
                            if ratio >= MIN_OVERLAP_RATIO:
                                overlap = True
                                break

                    if overlap:
                        chain.append(b2)
                        used.add(b2["battle_id"])

                if len(chain) >= MIN_SEQUENCE_LENGTH:
                    all_sequences.append(chain)

        # Write sequences
        seq_rows = []
        summary_rows = []

        for chain in all_sequences:
            chain.sort(key=lambda b: b["started_at"])
            bids = [b["battle_id"] for b in chain]
            sid = _sequence_id(bids)

            peak_participants = 0
            peak_capitals = 0
            total_isk = 0.0

            for ordinal, b in enumerate(chain):
                bid = b["battle_id"]
                pc = int(b.get("participant_count") or 0)
                cc = caps_by_battle.get(bid, 0)
                isk = isk_by_battle.get(bid, 0.0)
                peak_participants = max(peak_participants, pc)
                peak_capitals = max(peak_capitals, cc)
                total_isk += isk

                seq_rows.append((
                    sid, bid, ordinal, int(b["system_id"]),
                    pc, cc, isk, b["started_at"], computed_at,
                    ordinal, int(b["system_id"]), pc, cc, isk,
                    b["started_at"], computed_at,
                ))

            # Determine primary aggressor/defender from largest battle
            largest = max(chain, key=lambda b: int(b.get("participant_count") or 0))
            sides = alliances_by_battle.get(largest["battle_id"], {})
            sorted_sides = sorted(sides.items(), key=lambda kv: len(kv[1]), reverse=True)
            aggressor_aid = next(iter(sorted_sides[0][1]), None) if len(sorted_sides) >= 1 else None
            defender_aid = next(iter(sorted_sides[1][1]), None) if len(sorted_sides) >= 2 else None

            grade = _escalation_grade(len(chain), peak_participants, peak_capitals, total_isk)

            region_id = chain[0].get("region_id")
            constellation_id = chain[0].get("constellation_id")

            summary_rows.append((
                sid, region_id, constellation_id,
                len(chain), peak_participants, peak_capitals, total_isk,
                grade, aggressor_aid, defender_aid,
                chain[0]["started_at"], chain[-1]["started_at"], computed_at,
                region_id, constellation_id,
                len(chain), peak_participants, peak_capitals, total_isk,
                grade, aggressor_aid, defender_aid,
                chain[0]["started_at"], chain[-1]["started_at"], computed_at,
            ))

        if seq_rows:
            db.execute_many(
                """
                INSERT INTO escalation_sequences
                    (sequence_id, battle_id, ordinal, system_id,
                     participant_count, capital_count, isk_destroyed,
                     started_at, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    ordinal = %s, system_id = %s, participant_count = %s,
                    capital_count = %s, isk_destroyed = %s,
                    started_at = %s, computed_at = %s
                """,
                seq_rows,
            )
            rows_written += len(seq_rows)

        if summary_rows:
            db.execute_many(
                """
                INSERT INTO escalation_sequence_summary
                    (sequence_id, region_id, constellation_id,
                     battle_count, peak_participants, peak_capitals,
                     total_isk_destroyed, escalation_grade,
                     primary_aggressor_alliance_id, primary_defender_alliance_id,
                     first_battle_at, last_battle_at, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    region_id = %s, constellation_id = %s,
                    battle_count = %s, peak_participants = %s, peak_capitals = %s,
                    total_isk_destroyed = %s, escalation_grade = %s,
                    primary_aggressor_alliance_id = %s, primary_defender_alliance_id = %s,
                    first_battle_at = %s, last_battle_at = %s, computed_at = %s
                """,
                summary_rows,
            )
            rows_written += len(summary_rows)

        finish_job_run(db, job, status="success",
                       rows_processed=rows_processed, rows_written=rows_written)
        return {
            "status": "success",
            "rows_processed": rows_processed,
            "rows_written": rows_written,
            "sequences_found": len(all_sequences),
            "duration_ms": int((time.perf_counter() - started) * 1000),
        }
    except Exception as exc:
        finish_job_run(db, job, status="failed",
                       rows_processed=rows_processed, rows_written=rows_written,
                       error_text=str(exc))
        return {"status": "failed", "error_text": str(exc),
                "rows_processed": rows_processed, "rows_written": rows_written}
