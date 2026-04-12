"""Staging system detection — identify systems where forces concentrate before operations.

A staging system is characterised by:
  * Multiple pilots from the same alliance appearing in a system
  * That system is 1-3 jumps from recent battle locations
  * Pilots appear there before showing up in battles
  * Recurring pattern across multiple fights

Neo4j is **required** for multi-hop gate distance calculations — the
universe graph is the source of truth. When Neo4j is unavailable the
job exits with ``JobResult.skipped`` rather than running a 1-hop-only
SQL approximation.

Uses MariaDB for battle locations, participant data, and killmail timestamps.

Output: ``staging_system_candidates`` per (system_id, alliance_id).
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOOKBACK_DAYS = 14
MAX_GATE_DISTANCE = 3       # max jumps from battle to count as staging
MIN_PILOTS_FOR_STAGING = 3  # at least 3 pilots seen
MIN_BATTLES_NEARBY = 2      # at least 2 battles within range
BATCH_SIZE = 200


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def run_staging_system_detection(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Detect candidate staging systems based on pre-battle pilot concentration."""
    lock_key = "staging_system_detection"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()

    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        # Neo4j is the source of truth for the universe graph; without it
        # we cannot compute multi-hop gate distances correctly. The old
        # SQL fallback (1-hop only via `ref_stargates`) silently produced
        # wrong answers — it is explicitly not a supported failure mode.
        finish_job_run(
            db, job, status="skipped",
            rows_processed=0, rows_written=0,
            error_text="Neo4j disabled",
        )
        return JobResult.skipped(
            job_key=lock_key,
            reason="Neo4j is disabled; staging_system_detection requires the universe graph for multi-hop gate distances.",
        ).to_dict()
    client = Neo4jClient(config)

    try:
        cutoff = (datetime.now(UTC) - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

        # 1. Get recent battle systems
        battle_systems = db.fetch_all(
            """
            SELECT DISTINCT br.system_id, br.battle_id, br.started_at
            FROM battle_rollups br
            WHERE br.started_at >= %s
              AND br.participant_count >= 10
            """,
            (cutoff,),
        )

        if not battle_systems:
            finish_job_run(db, job, status="success",
                           rows_processed=0, rows_written=0)
            return {"status": "success", "rows_processed": 0, "rows_written": 0,
                    "duration_ms": int((time.perf_counter() - started) * 1000)}

        battle_sys_ids = list({int(r["system_id"]) for r in battle_systems})
        rows_processed = len(battle_systems)

        # 2. Find nearby systems (within MAX_GATE_DISTANCE jumps) via Neo4j.
        # Batched UNWIND + CALL subquery: one round-trip for all battle
        # systems (chunked to bound per-call memory), replacing the old
        # per-system query loop which issued N queries for N battle
        # systems. The CALL subquery gives us per-row aggregation so
        # each sysId gets its own `collect(DISTINCT ...)` result.
        nearby_systems: dict[int, set[int]] = defaultdict(set)  # battle_sys -> set of nearby sys
        NEARBY_CHUNK = 100

        for i in range(0, len(battle_sys_ids), NEARBY_CHUNK):
            chunk = battle_sys_ids[i:i + NEARBY_CHUNK]
            rows = client.query(
                f"""
                UNWIND $sysIds AS sysId
                CALL {{
                    WITH sysId
                    MATCH (s:System {{system_id: sysId}})-[:CONNECTS_TO|JUMP_BRIDGE*1..{MAX_GATE_DISTANCE}]-(n:System)
                    RETURN collect(DISTINCT n.system_id) AS nearby_ids
                }}
                RETURN sysId, nearby_ids
                """,
                {"sysIds": chunk},
            )
            for r in rows:
                sid = int(r["sysId"])
                for nid in r.get("nearby_ids") or []:
                    nearby_systems[sid].add(int(nid))

        # 3. Build reverse map: system -> list of battle systems it's near
        system_to_battles: dict[int, list[dict]] = defaultdict(list)
        for bs in battle_systems:
            bsid = int(bs["system_id"])
            for nearby_sid in nearby_systems.get(bsid, set()):
                system_to_battles[nearby_sid].append(bs)
            # Also include the battle system itself
            system_to_battles[bsid].append(bs)

        # 4. Get killmail participation per system per alliance (who was seen where)
        # Use killmails occurring in nearby systems to find pre-battle presence
        candidate_sys_ids = list(system_to_battles.keys())
        if not candidate_sys_ids:
            finish_job_run(db, job, status="success",
                           rows_processed=rows_processed, rows_written=0)
            return {"status": "success", "rows_processed": rows_processed, "rows_written": 0,
                    "duration_ms": int((time.perf_counter() - started) * 1000)}

        # Get all killmail activity in candidate systems
        # Group by system + alliance to find pilot concentrations
        sys_alliance_pilots: dict[tuple[int, int], set[int]] = defaultdict(set)

        for i in range(0, len(candidate_sys_ids), BATCH_SIZE):
            batch = candidate_sys_ids[i:i + BATCH_SIZE]
            ph = ",".join(["%s"] * len(batch))
            rows = db.fetch_all(
                f"""
                SELECT ke.solar_system_id, ka.alliance_id, ka.character_id
                FROM killmail_events ke
                INNER JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
                WHERE ke.solar_system_id IN ({ph})
                  AND ke.effective_killmail_at >= %s
                  AND ka.alliance_id IS NOT NULL AND ka.alliance_id > 0
                  AND ka.character_id IS NOT NULL AND ka.character_id > 0
                """,
                batch + [cutoff],
            )
            for r in rows:
                key = (int(r["solar_system_id"]), int(r["alliance_id"]))
                sys_alliance_pilots[key].add(int(r["character_id"]))

        # 5. Score candidate staging systems
        insert_rows = []
        for (sys_id, alliance_id), pilots in sys_alliance_pilots.items():
            if len(pilots) < MIN_PILOTS_FOR_STAGING:
                continue

            nearby_battles = system_to_battles.get(sys_id, [])
            if len(nearby_battles) < MIN_BATTLES_NEARBY:
                continue

            # Compute average jump distance to nearby battles
            total_dist = 0
            battle_count = 0
            for bs in nearby_battles:
                bsid = int(bs["system_id"])
                if bsid == sys_id:
                    total_dist += 0
                elif sys_id in nearby_systems.get(bsid, set()):
                    total_dist += 1  # rough estimate; actual distance needs BFS
                else:
                    total_dist += 2
                battle_count += 1

            avg_jump = total_dist / max(battle_count, 1)

            # Staging score: more pilots + more nearby battles + closer = higher
            pilot_factor = min(1.0, len(pilots) / 20.0)
            battle_factor = min(1.0, len(nearby_battles) / 10.0)
            distance_factor = 1.0 - (avg_jump / (MAX_GATE_DISTANCE + 1))

            staging_score = round(
                0.4 * pilot_factor + 0.35 * battle_factor + 0.25 * distance_factor,
                4
            )

            if staging_score >= 0.15:
                features = {
                    "unique_pilots": len(pilots),
                    "nearby_battle_count": len(nearby_battles),
                    "avg_jump_distance": round(avg_jump, 2),
                    "pilot_factor": round(pilot_factor, 4),
                    "battle_factor": round(battle_factor, 4),
                    "distance_factor": round(distance_factor, 4),
                }
                insert_rows.append((
                    sys_id, alliance_id, staging_score,
                    len(pilots), len(nearby_battles),
                    round(avg_jump, 2), 0,  # pre_battle_appearances placeholder
                    json_dumps_safe(features), computed_at,
                ))

        if insert_rows:
            db.execute_many(
                """
                INSERT INTO staging_system_candidates
                    (system_id, alliance_id, staging_score,
                     unique_pilots_7d, battles_within_2j, avg_jump_to_battle,
                     pre_battle_appearances, features_json, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    staging_score = VALUES(staging_score),
                    unique_pilots_7d = VALUES(unique_pilots_7d),
                    battles_within_2j = VALUES(battles_within_2j),
                    avg_jump_to_battle = VALUES(avg_jump_to_battle),
                    pre_battle_appearances = VALUES(pre_battle_appearances),
                    features_json = VALUES(features_json),
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
            "candidates_found": rows_written,
            "duration_ms": int((time.perf_counter() - started) * 1000),
        }
    except Exception as exc:
        finish_job_run(db, job, status="failed",
                       rows_processed=rows_processed, rows_written=rows_written,
                       error_text=str(exc))
        return {"status": "failed", "error_text": str(exc),
                "rows_processed": rows_processed, "rows_written": rows_written}
