from __future__ import annotations

import logging
import time
import traceback
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError


_TRIANGLE_BATCH_SIZE = 100
_TRIANGLE_LIMIT = 500
_TRIANGLE_ENRICH_CHUNK = 50


def _detect_triangles(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect triangle motifs: 3 characters mutually co-occurring.

    Splits detection into two phases to avoid cartesian explosion:
    1. Find triangles (cheap pattern match, no OPTIONAL MATCH)
    2. Enrich with battle data in a separate query
    """
    # Get character_id boundaries for nodes that have outgoing CO_OCCURS_WITH.
    bounds = client.query(
        """
        MATCH (a:Character)-[:CO_OCCURS_WITH]->()
        RETURN min(a.character_id) AS lo, max(a.character_id) AS hi
        """,
        timeout_seconds=15,
    )
    if not bounds or bounds[0].get("lo") is None:
        return []

    lo, hi = int(bounds[0]["lo"]), int(bounds[0]["hi"])

    # Phase 1: Find triangles.
    # CO_OCCURS_WITH edges are always directed from lower character_id to
    # higher, so we use directed patterns to avoid the combinatorial explosion
    # of undirected cycle matching.  The third edge (a)->(c) is checked as a
    # WHERE-EXISTS filter, which Neo4j can evaluate cheaply.
    find_query = """
        MATCH (a:Character)-[:CO_OCCURS_WITH]->(b:Character)-[:CO_OCCURS_WITH]->(c:Character)
        WHERE a.character_id >= $lo AND a.character_id < $hi
          AND a.character_id < b.character_id
          AND b.character_id < c.character_id
          AND EXISTS { (a)-[:CO_OCCURS_WITH]->(c) }
        RETURN
            [a.character_id, b.character_id, c.character_id] AS member_ids,
            toFloat(
                (COALESCE(a.suspicion_score, 0) + COALESCE(b.suspicion_score, 0) + COALESCE(c.suspicion_score, 0)) / 3.0
            ) AS suspicion_relevance
        LIMIT $batch_limit
    """

    raw_triangles: list[dict[str, Any]] = []
    cursor = lo
    while cursor <= hi and len(raw_triangles) < _TRIANGLE_LIMIT:
        remaining = _TRIANGLE_LIMIT - len(raw_triangles)
        rows = client.query(
            find_query,
            parameters={"lo": cursor, "hi": cursor + _TRIANGLE_BATCH_SIZE, "batch_limit": remaining},
            timeout_seconds=30,
        )
        raw_triangles.extend(rows)
        cursor += _TRIANGLE_BATCH_SIZE

    raw_triangles = raw_triangles[:_TRIANGLE_LIMIT]
    if not raw_triangles:
        return []

    # Phase 2: Enrich with battle data.
    # Collect the first member of each triangle and look up battles in bulk.
    enrich_query = """
        UNWIND $char_ids AS cid
        MATCH (a:Character {character_id: cid})-[:ON_SIDE]->(:BattleSide)<-[:HAS_SIDE]-(battle:Battle)
        RETURN cid, collect(DISTINCT battle.battle_id)[..10] AS battle_ids
    """

    battle_map: dict[int, list] = {}
    all_first_ids = [t["member_ids"][0] for t in raw_triangles]
    for chunk_start in range(0, len(all_first_ids), _TRIANGLE_ENRICH_CHUNK):
        chunk_ids = all_first_ids[chunk_start:chunk_start + _TRIANGLE_ENRICH_CHUNK]
        rows = client.query(
            enrich_query,
            parameters={"char_ids": chunk_ids},
            timeout_seconds=15,
        )
        for row in rows:
            battle_map[row["cid"]] = row["battle_ids"]

    # Assemble final results.
    results: list[dict[str, Any]] = []
    for t in raw_triangles:
        results.append({
            "motif_type": "triangle",
            "member_ids": t["member_ids"],
            "battle_ids": battle_map.get(t["member_ids"][0], []),
            "occurrence_count": 1,
            "suspicion_relevance": t["suspicion_relevance"],
        })
    return results


def _detect_stars(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect star motifs: 1 hub connected to 4+ others who don't connect to each other.

    Uses directed CO_OCCURS_WITH (always low->high character_id) to halve
    intermediate results.  Caps leaf collection at 30 per hub to bound the
    UNWIND fan-out, and uses OPTIONAL MATCH + count instead of nested list
    comprehension pattern matching.
    """
    return client.query(
        """
        MATCH (hub:Character)-[:CO_OCCURS_WITH]->(out:Character)
        WITH hub, collect(out) AS out_leaves
        OPTIONAL MATCH (in_leaf:Character)-[:CO_OCCURS_WITH]->(hub)
        WITH hub, out_leaves, collect(in_leaf) AS in_leaves
        WITH hub, (out_leaves + in_leaves)[..30] AS leaves
        WHERE size(leaves) >= 4
        UNWIND leaves AS l1
        OPTIONAL MATCH (l1)-[ie:CO_OCCURS_WITH]-(l2)
        WHERE l2 IN leaves AND l1 <> l2
        WITH hub, l1, count(ie) AS inter_edges
        WHERE inter_edges = 0
        WITH hub, collect(l1) AS isolated_leaves
        WHERE size(isolated_leaves) >= 4
        WITH hub, isolated_leaves[..8] AS top_leaves
        RETURN
            'star' AS motif_type,
            [hub.character_id] + [l IN top_leaves | l.character_id] AS member_ids,
            [] AS battle_ids,
            1 AS occurrence_count,
            toFloat(COALESCE(hub.suspicion_score, 0)) AS suspicion_relevance
        LIMIT 200
        """,
        timeout_seconds=30,
    )


def _detect_chains(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect chain motifs: A→B→C→D with no shortcuts (intelligence relay pattern).

    Uses directed CO_OCCURS_WITH edges (low→high character_id) to halve
    intermediate results vs undirected matching.  The NOT EXISTS filters
    verify no shortcut edges exist between non-adjacent chain members.
    """
    return client.query(
        """
        MATCH (a:Character)-[:CO_OCCURS_WITH]->(b:Character)-[:CO_OCCURS_WITH]->(c:Character)-[:CO_OCCURS_WITH]->(d:Character)
        WHERE a.character_id < b.character_id
          AND b.character_id < c.character_id
          AND c.character_id < d.character_id
          AND NOT EXISTS { (a)-[:CO_OCCURS_WITH]->(c) }
          AND NOT EXISTS { (a)-[:CO_OCCURS_WITH]->(d) }
          AND NOT EXISTS { (b)-[:CO_OCCURS_WITH]->(d) }
        RETURN
            'chain' AS motif_type,
            [a.character_id, b.character_id, c.character_id, d.character_id] AS member_ids,
            [] AS battle_ids,
            1 AS occurrence_count,
            toFloat(
                (COALESCE(a.suspicion_score, 0) + COALESCE(b.suspicion_score, 0) +
                 COALESCE(c.suspicion_score, 0) + COALESCE(d.suspicion_score, 0)) / 4.0
            ) AS suspicion_relevance
        LIMIT 300
        """,
        timeout_seconds=30,
    )


def _detect_fleet_cores(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect fleet core motifs: 5+ characters all in SAME_FLEET with each other."""
    return client.query(
        """
        MATCH (a:Character)-[:SAME_FLEET]-(b:Character)-[:SAME_FLEET]-(c:Character)-[:SAME_FLEET]-(a)
        WHERE a.character_id < b.character_id AND b.character_id < c.character_id
        WITH a, b, c
        OPTIONAL MATCH (d:Character)
        WHERE (d)-[:SAME_FLEET]-(a) AND (d)-[:SAME_FLEET]-(b) AND (d)-[:SAME_FLEET]-(c)
          AND d.character_id > c.character_id
        WITH a, b, c, collect(d)[..5] AS extras
        WHERE size(extras) >= 2
        WITH [a.character_id, b.character_id, c.character_id]
             + [e IN extras | e.character_id] AS member_ids,
             (COALESCE(a.suspicion_score, 0) + COALESCE(b.suspicion_score, 0) + COALESCE(c.suspicion_score, 0)) / 3.0 AS avg_suspicion
        RETURN
            'fleet_core' AS motif_type,
            member_ids,
            [] AS battle_ids,
            1 AS occurrence_count,
            toFloat(avg_suspicion) AS suspicion_relevance
        LIMIT 200
        """,
        timeout_seconds=30,
    )


def _detect_rotating_scouts(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect rotating scout: character alternates sides across battles."""
    return client.query(
        """
        MATCH (c:Character)-[cr:CROSSED_SIDES]->(c)
        WHERE COALESCE(cr.side_transition_count, 0) >= 3
        RETURN
            'rotating_scout' AS motif_type,
            [c.character_id] AS member_ids,
            [] AS battle_ids,
            toInteger(cr.side_transition_count) AS occurrence_count,
            toFloat(COALESCE(c.suspicion_score, 0) * 1.5) AS suspicion_relevance
        ORDER BY cr.side_transition_count DESC
        LIMIT 200
        """,
        timeout_seconds=15,
    )


def run_graph_motif_detection_sync(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_motif_detection_sync"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})

    logger.info(
        "Starting %s — neo4j enabled=%s uri=%s database=%s",
        job_name,
        config.enabled,
        getattr(config, "uri", "?"),
        getattr(config, "database", "?"),
    )

    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Run all detectors
    all_motifs: list[dict[str, Any]] = []
    detectors = [
        ("triangle", _detect_triangles),
        ("star", _detect_stars),
        ("chain", _detect_chains),
        ("fleet_core", _detect_fleet_cores),
        ("rotating_scout", _detect_rotating_scouts),
    ]

    counts_by_type: dict[str, int] = {}
    failed_detectors: list[str] = []
    detector_errors: dict[str, str] = {}
    for motif_name, detector_fn in detectors:
        detector_start = time.perf_counter()
        try:
            results = detector_fn(client)
            detector_ms = int((time.perf_counter() - detector_start) * 1000)
            all_motifs.extend(results)
            counts_by_type[motif_name] = len(results)
            logger.info(
                "Detector %s OK: %d results in %dms",
                motif_name, len(results), detector_ms,
            )
        except (Neo4jError, OSError) as exc:
            detector_ms = int((time.perf_counter() - detector_start) * 1000)
            tb = traceback.format_exc()
            logger.error(
                "Detector %s FAILED after %dms: %s\n%s",
                motif_name, detector_ms, exc, tb,
            )
            counts_by_type[motif_name] = 0
            failed_detectors.append(motif_name)
            detector_errors[motif_name] = f"{type(exc).__name__}: {exc}\n{tb}"
        except Exception as exc:
            detector_ms = int((time.perf_counter() - detector_start) * 1000)
            tb = traceback.format_exc()
            logger.error(
                "Detector %s UNEXPECTED FAILURE after %dms: %s\n%s",
                motif_name, detector_ms, exc, tb,
            )
            counts_by_type[motif_name] = 0
            failed_detectors.append(motif_name)
            detector_errors[motif_name] = f"{type(exc).__name__}: {exc}\n{tb}"

    if failed_detectors:
        duration_ms = int((time.perf_counter() - started) * 1000)
        error_detail = "; ".join(
            f"{name}: {detector_errors.get(name, '?')[:300]}"
            for name in failed_detectors
        )
        logger.error(
            "Job %s failing — detectors failed: %s — detail: %s",
            job_name, ", ".join(failed_detectors), error_detail,
        )
        return JobResult.failed(
            job_key=job_name,
            error=f"Detectors failed: {', '.join(failed_detectors)}",
            duration_ms=duration_ms,
            meta={
                "counts": counts_by_type,
                "failed": failed_detectors,
                "errors": {k: v[:500] for k, v in detector_errors.items()},
            },
        ).to_dict()

    if not all_motifs:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return JobResult.success(
            job_key=job_name,
            summary="No motifs detected.",
            rows_processed=0,
            rows_written=0,
            rows_seen=0,
            duration_ms=duration_ms,
            meta={"counts": counts_by_type},
        ).to_dict()

    # Truncate and re-insert using retry-aware transactions to avoid deadlocks.
    db.run_in_transaction(lambda _conn, cur: cur.execute("DELETE FROM graph_motif_detections"))

    batch_size = 500
    total_written = 0
    for offset in range(0, len(all_motifs), batch_size):
        chunk = all_motifs[offset:offset + batch_size]
        values = []
        params: list[Any] = []
        for m in chunk:
            member_ids = m.get("member_ids") or []
            battle_ids = m.get("battle_ids") or []
            values.append("(%s, %s, %s, %s, %s, %s, %s, %s)")
            params.extend([
                str(m.get("motif_type") or "unknown"),
                json_dumps_safe(member_ids),
                json_dumps_safe(battle_ids) if battle_ids else None,
                int(m.get("occurrence_count") or 1),
                min(1.0, max(0.0, float(m.get("suspicion_relevance") or 0.0))),
                computed_at,
                computed_at,
                computed_at,
            ])
        if values:
            sql = (
                "INSERT INTO graph_motif_detections "
                "(motif_type, member_ids_json, battle_ids_json, occurrence_count, "
                "suspicion_relevance, first_seen_at, last_seen_at, computed_at) "
                "VALUES " + ", ".join(values)
            )
            sql_params = tuple(params)
            db.run_in_transaction(lambda _conn, cur: cur.execute(sql, sql_params))
            total_written += len(values)

    db.upsert_intelligence_snapshot(
        snapshot_key="graph_motif_detection_state",
        payload_json=json_dumps_safe({
            "total_motifs": total_written,
            "counts_by_type": counts_by_type,
        }),
        metadata_json=json_dumps_safe({"source": "neo4j", "reason": "scheduler:python"}),
        expires_seconds=14400,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=f"Motif detection: {total_written} motifs found ({', '.join(f'{k}={v}' for k, v in counts_by_type.items())}).",
        rows_processed=total_written,
        rows_written=total_written,
        rows_seen=total_written,
        duration_ms=duration_ms,
        meta={"counts": counts_by_type},
    ).to_dict()
