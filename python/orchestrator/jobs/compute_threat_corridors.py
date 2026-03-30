"""Threat Corridor computation — identifies repeated hostile movement paths.

Uses Neo4j system connectivity graph and MariaDB battle data to find
corridors where hostile alliances repeatedly operate through connected
systems. Materializes results to ``threat_corridors`` and
``threat_corridor_systems`` tables.
"""

from __future__ import annotations

import hashlib
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    package_root = str(Path(__file__).resolve().parents[2])
    if package_root not in sys.path:
        sys.path.insert(0, package_root)
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
else:
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run

RECENT_DAYS = 30
MIN_CORRIDOR_BATTLES = 3
MAX_CORRIDOR_LENGTH = 5
BATCH_SIZE = 500


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _corridor_hash(system_ids: list[int]) -> str:
    """Deterministic hash for a corridor (ordered system list)."""
    canonical = "|".join(str(s) for s in sorted(system_ids))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _load_system_battle_activity(db: SupplyCoreDb) -> dict[int, dict[str, Any]]:
    """Load per-system battle activity with hostile alliance presence."""
    rows = db.fetch_all(
        """
        SELECT br.system_id,
               rs.system_name,
               rs.region_id,
               bp.alliance_id,
               COUNT(DISTINCT br.battle_id) AS battle_count,
               COUNT(DISTINCT CASE WHEN br.started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                     THEN br.battle_id END) AS recent_battle_count,
               MIN(br.started_at) AS first_activity,
               MAX(br.started_at) AS last_activity,
               SUM(COALESCE(br.participant_count, 0)) AS total_participants
        FROM battle_rollups br
        INNER JOIN battle_participants bp ON bp.battle_id = br.battle_id
        LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
        WHERE bp.alliance_id IS NOT NULL AND bp.alliance_id > 0
          AND br.system_id IS NOT NULL AND br.system_id > 0
        GROUP BY br.system_id, bp.alliance_id
        ORDER BY battle_count DESC
        """,
        (RECENT_DAYS,),
    )

    systems: dict[int, dict[str, Any]] = {}
    for r in rows:
        sid = int(r["system_id"])
        aid = int(r["alliance_id"])
        if sid not in systems:
            systems[sid] = {
                "system_id": sid,
                "system_name": r.get("system_name", ""),
                "region_id": int(r.get("region_id") or 0),
                "alliances": {},
                "total_battles": 0,
                "recent_battles": 0,
                "first_activity": r.get("first_activity"),
                "last_activity": r.get("last_activity"),
            }
        bc = int(r.get("battle_count") or 0)
        rbc = int(r.get("recent_battle_count") or 0)
        systems[sid]["alliances"][aid] = {
            "battle_count": bc,
            "recent_battle_count": rbc,
        }
        systems[sid]["total_battles"] += bc
        systems[sid]["recent_battles"] += rbc
        if r.get("last_activity") and (systems[sid]["last_activity"] is None
                                        or r["last_activity"] > systems[sid]["last_activity"]):
            systems[sid]["last_activity"] = r["last_activity"]

    return systems


def _find_connected_corridors_neo4j(
    neo4j_client: Any,
    active_system_ids: list[int],
    max_length: int = MAX_CORRIDOR_LENGTH,
) -> list[list[int]]:
    """Use Neo4j to find connected chains of active battle systems."""
    if neo4j_client is None or not active_system_ids:
        return []

    try:
        rows = neo4j_client.query(
            """
            UNWIND $system_ids AS start_id
            MATCH (s1:System {system_id: start_id})
            MATCH path = (s1)-[:CONNECTS_TO*1..""" + str(max_length - 1) + """]-(s2:System)
            WHERE s2.system_id IN $system_ids
              AND s1.system_id < s2.system_id
              AND ALL(n IN nodes(path) WHERE n.system_id IN $system_ids)
            WITH [n IN nodes(path) | n.system_id] AS corridor_systems,
                 length(path) + 1 AS corridor_length
            WHERE corridor_length >= 2 AND corridor_length <= $max_len
            RETURN DISTINCT corridor_systems, corridor_length
            ORDER BY corridor_length DESC
            LIMIT 500
            """,
            {"system_ids": active_system_ids, "max_len": max_length},
            timeout_seconds=90,
        )
        return [list(r["corridor_systems"]) for r in rows]
    except Exception:
        return []


def _find_connected_corridors_sql(db: SupplyCoreDb, active_system_ids: set[int]) -> list[list[int]]:
    """Fallback: find 2-system corridors using stargate adjacency from MariaDB."""
    if not active_system_ids:
        return []

    placeholders = ",".join(["%s"] * len(active_system_ids))
    rows = db.fetch_all(
        f"""
        SELECT sg.system_id AS sys_a, sg.dest_system_id AS sys_b
        FROM ref_stargates sg
        WHERE sg.system_id IN ({placeholders})
          AND sg.dest_system_id IN ({placeholders})
          AND sg.system_id < sg.dest_system_id
        """,
        tuple(active_system_ids) + tuple(active_system_ids),
    )

    return [[int(r["sys_a"]), int(r["sys_b"])] for r in rows]


def _find_connected_corridors_constellation(
    db: SupplyCoreDb,
    active_system_ids: set[int],
    system_data: dict[int, dict[str, Any]],
) -> list[list[int]]:
    """Fallback: group co-constellation active systems into corridors.

    When stargate adjacency data is unavailable, systems sharing a
    constellation are treated as connected.  Within each constellation
    systems are sorted by battle count (descending) and capped at
    MAX_CORRIDOR_LENGTH to form a single corridor.
    """
    if not active_system_ids:
        return []

    constellation_groups: dict[int, list[int]] = defaultdict(list)
    placeholders = ",".join(["%s"] * len(active_system_ids))
    rows = db.fetch_all(
        f"SELECT system_id, constellation_id FROM ref_systems WHERE system_id IN ({placeholders})",
        tuple(active_system_ids),
    )
    for r in rows:
        constellation_groups[int(r["constellation_id"])].append(int(r["system_id"]))

    corridors: list[list[int]] = []
    for _cid, sids in constellation_groups.items():
        if len(sids) < 2:
            continue
        # Sort by battle activity descending, cap length
        sids.sort(key=lambda s: system_data.get(s, {}).get("total_battles", 0), reverse=True)
        corridors.append(sids[:MAX_CORRIDOR_LENGTH])

    return corridors


def _score_corridor(
    corridor_systems: list[int],
    system_data: dict[int, dict[str, Any]],
    tracked_alliance_ids: set[int],
) -> dict[str, Any]:
    """Score a corridor based on hostile activity, recency, and concentration."""
    total_battles = 0
    recent_battles = 0
    total_isk = 0.0
    hostile_alliances: dict[int, int] = defaultdict(int)
    first_activity = None
    last_activity = None
    system_battle_counts: list[int] = []

    for sid in corridor_systems:
        sdata = system_data.get(sid)
        if not sdata:
            continue
        total_battles += sdata["total_battles"]
        recent_battles += sdata["recent_battles"]
        system_battle_counts.append(sdata["total_battles"])

        for aid, adata in sdata["alliances"].items():
            if aid not in tracked_alliance_ids:
                hostile_alliances[aid] += adata["battle_count"]

        sa = sdata.get("first_activity")
        la = sdata.get("last_activity")
        if sa and (first_activity is None or sa < first_activity):
            first_activity = sa
        if la and (last_activity is None or la > last_activity):
            last_activity = la

    # Corridor score components:
    # - battle density (battles per system)
    # - recency weight (recent battles count more)
    # - hostile concentration (more hostile alliances = higher threat)
    # - connectivity (longer corridors with consistent activity score higher)
    length = len(corridor_systems)
    avg_battles = total_battles / max(1, length)
    recency_factor = 1.0 + (recent_battles / max(1, total_battles)) * 2.0
    hostile_count = len(hostile_alliances)
    hostile_factor = min(3.0, 1.0 + hostile_count * 0.3)

    corridor_score = round(avg_battles * recency_factor * hostile_factor * (1 + length * 0.1), 4)

    # Identify choke points (systems appearing in many battles relative to corridor avg)
    choke_threshold = avg_battles * 1.5 if avg_battles > 0 else float("inf")

    # Dominant hostile
    dominant_hostile = max(hostile_alliances.items(), key=lambda x: x[1]) if hostile_alliances else (None, 0)

    system_names = []
    for sid in corridor_systems:
        sdata = system_data.get(sid)
        system_names.append(sdata["system_name"] if sdata else f"System #{sid}")

    return {
        "system_ids": corridor_systems,
        "system_names": system_names,
        "corridor_length": length,
        "total_battles": total_battles,
        "recent_battles": recent_battles,
        "total_isk": total_isk,
        "corridor_score": corridor_score,
        "hostile_alliance_ids": list(hostile_alliances.keys())[:10],
        "first_activity": first_activity,
        "last_activity": last_activity,
        "region_id": system_data[corridor_systems[0]]["region_id"] if corridor_systems[0] in system_data else None,
        "choke_systems": [
            sid for sid in corridor_systems
            if system_data.get(sid, {}).get("total_battles", 0) >= choke_threshold
        ],
        "system_battle_counts": {sid: system_data.get(sid, {}).get("total_battles", 0) for sid in corridor_systems},
    }


def _flush_corridors(db: SupplyCoreDb, corridors: list[dict[str, Any]], computed_at: str) -> int:
    """Write corridor data to MariaDB."""
    if not corridors:
        return 0

    rows_written = 0
    with db.transaction() as (_, cursor):
        # Clear old data
        cursor.execute("DELETE FROM threat_corridor_systems")
        cursor.execute("DELETE FROM threat_corridors")

        for c in corridors:
            chash = _corridor_hash(c["system_ids"])
            cursor.execute(
                """
                INSERT INTO threat_corridors (
                    corridor_hash, system_ids_json, system_names_json, region_id,
                    corridor_length, hostile_alliance_ids_json, battle_count,
                    recent_battle_count, total_isk_destroyed, corridor_score,
                    first_activity_at, last_activity_at, is_active, computed_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    chash,
                    json_dumps_safe(c["system_ids"]),
                    json_dumps_safe(c["system_names"]),
                    c["region_id"],
                    c["corridor_length"],
                    json_dumps_safe(c["hostile_alliance_ids"]),
                    c["total_battles"],
                    c["recent_battles"],
                    c["total_isk"],
                    c["corridor_score"],
                    c["first_activity"],
                    c["last_activity"],
                    1 if c["recent_battles"] > 0 else 0,
                    computed_at,
                ),
            )
            corridor_id = cursor.lastrowid
            rows_written += 1

            choke_set = set(c.get("choke_systems", []))
            for pos, sid in enumerate(c["system_ids"]):
                cursor.execute(
                    """
                    INSERT INTO threat_corridor_systems (corridor_id, system_id, position_in_corridor,
                           system_battle_count, is_choke_point)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (corridor_id, sid, pos, c["system_battle_counts"].get(sid, 0), 1 if sid in choke_set else 0),
                )
                rows_written += 1

    return rows_written


def _flush_system_threat_scores(db: SupplyCoreDb, system_data: dict[int, dict[str, Any]],
                                 tracked_alliance_ids: set[int], computed_at: str) -> int:
    """Compute and write system threat scores."""
    rows_written = 0
    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM system_threat_scores")

        for sid, sdata in system_data.items():
            hostile_alliances = {
                aid: adata["battle_count"]
                for aid, adata in sdata["alliances"].items()
                if aid not in tracked_alliance_ids
            }
            unique_hostile = len(hostile_alliances)
            dominant = max(hostile_alliances.items(), key=lambda x: x[1]) if hostile_alliances else (None, 0)

            # Hotspot score: battles * recency * hostile diversity
            recency = 1.0 + (sdata["recent_battles"] / max(1, sdata["total_battles"])) * 2.0
            hotspot = round(sdata["total_battles"] * recency * (1 + unique_hostile * 0.2), 4)

            if hotspot >= 20:
                level = "critical"
            elif hotspot >= 10:
                level = "high"
            elif hotspot >= 4:
                level = "medium"
            else:
                level = "low"

            cursor.execute(
                """
                INSERT INTO system_threat_scores (
                    system_id, battle_count, recent_battle_count, total_kills,
                    total_isk_destroyed, unique_hostile_alliances, avg_anomaly_score,
                    threat_level, hotspot_score, dominant_hostile_alliance_id,
                    dominant_hostile_name, last_battle_at, computed_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    sid, sdata["total_battles"], sdata["recent_battles"], 0, 0,
                    unique_hostile, None, level, hotspot,
                    dominant[0], None, sdata.get("last_activity"), computed_at,
                ),
            )
            rows_written += 1

    return rows_written


def run_compute_threat_corridors(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    neo4j_raw: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute threat corridors and system threat scores."""
    lock_key = "compute_threat_corridors"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=900)
    if owner is None:
        return JobResult.skipped(job_key=lock_key, reason="lock-not-acquired").to_dict()

    job = start_job_run(db, lock_key)
    started = datetime.now(UTC)
    computed_at = _now_sql()
    rows_processed = 0
    rows_written = 0

    try:
        # Load tracked alliances to distinguish hostile from friendly
        tracked_rows = db.fetch_all("SELECT alliance_id FROM killmail_tracked_alliances WHERE is_active = 1")
        tracked_alliance_ids = {int(r["alliance_id"]) for r in tracked_rows if int(r.get("alliance_id") or 0) > 0}

        # Load system battle activity
        system_data = _load_system_battle_activity(db)
        rows_processed = len(system_data)

        if not system_data:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
            return JobResult.success(job_key=lock_key, summary="No system activity found.",
                                    rows_processed=0, rows_written=0, duration_ms=0).to_dict()

        # Find active systems (at least 2 battles)
        active_systems = {sid for sid, sd in system_data.items() if sd["total_battles"] >= 2}

        # Initialize Neo4j
        neo4j_client = None
        try:
            from ..neo4j import Neo4jClient, Neo4jConfig
            config = Neo4jConfig.from_runtime(neo4j_raw or {})
            if config.enabled:
                neo4j_client = Neo4jClient(config)
        except Exception:
            neo4j_client = None

        # Find connected corridors (Neo4j → stargate SQL → constellation fallback)
        raw_corridors: list[list[int]] = []
        corridor_source = "none"
        if neo4j_client is not None:
            raw_corridors = _find_connected_corridors_neo4j(neo4j_client, list(active_systems))
            if raw_corridors:
                corridor_source = "neo4j"
        if not raw_corridors:
            raw_corridors = _find_connected_corridors_sql(db, active_systems)
            if raw_corridors:
                corridor_source = "stargate_sql"
        if not raw_corridors:
            raw_corridors = _find_connected_corridors_constellation(db, active_systems, system_data)
            if raw_corridors:
                corridor_source = "constellation"

        # Score and filter corridors
        scored: list[dict[str, Any]] = []
        seen_hashes: set[str] = set()

        for corridor_systems in raw_corridors:
            if len(corridor_systems) < 2:
                continue
            ch = _corridor_hash(corridor_systems)
            if ch in seen_hashes:
                continue
            seen_hashes.add(ch)

            result = _score_corridor(corridor_systems, system_data, tracked_alliance_ids)
            if result["total_battles"] >= MIN_CORRIDOR_BATTLES:
                scored.append(result)

        scored.sort(key=lambda x: x["corridor_score"], reverse=True)
        scored = scored[:200]  # Keep top 200

        if not dry_run:
            rows_written += _flush_corridors(db, scored, computed_at)
            rows_written += _flush_system_threat_scores(db, system_data, tracked_alliance_ids, computed_at)

        duration_ms = int((datetime.now(UTC) - started).total_seconds() * 1000)
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written,
                       meta={"corridor_count": len(scored), "systems_scored": len(system_data),
                             "active_system_count": len(active_systems), "corridor_source": corridor_source,
                             "raw_corridors_found": len(raw_corridors)})
        return JobResult.success(
            job_key=lock_key,
            summary=f"Found {len(scored)} threat corridors across {len(system_data)} active systems.",
            rows_processed=rows_processed,
            rows_written=rows_written,
            duration_ms=duration_ms,
        ).to_dict()

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise
    finally:
        release_job_lock(db, lock_key, owner)
