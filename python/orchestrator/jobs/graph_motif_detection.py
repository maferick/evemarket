from __future__ import annotations

import logging
import time
import traceback
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

logger = logging.getLogger(__name__)


_TRIANGLE_BATCH_SIZE = 50
_TRIANGLE_LIMIT = 500
_TRIANGLE_ENRICH_CHUNK = 50
# Skip characters with very high CO_OCCURS_WITH degree when enumerating
# triangles — their fan-out is quadratic in neighbors and will blow query
# memory / runtime on Neo4j (issue #902).  Super-hub characters will still
# appear in triangles via their lower-degree neighbours.
_TRIANGLE_MAX_A_DEGREE = 100
_TRIANGLE_QUERY_TIMEOUT = 60
# Skip hubs with extreme CO_OCCURS_WITH degree in the star detector — the
# original query did ``collect(out) + collect(in)`` which materialized every
# neighbour in memory, OOM-ing on super-hubs.
_STAR_MAX_HUB_DEGREE = 150
_STAR_QUERY_TIMEOUT = 60


def _detect_triangles(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect triangle motifs: 3 characters mutually co-occurring.

    Streams triangles in small character_id windows so the planner never
    has to hold more than ``_TRIANGLE_BATCH_SIZE`` source characters'
    worth of path expansions in memory at once.  Super-hub source
    characters (degree > ``_TRIANGLE_MAX_A_DEGREE``) are skipped to bound
    the per-batch work — they would otherwise produce O(degree^2) path
    expansions and time out (issue #902).
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

    # Combined find + enrich query: uses CALL {} subquery to look up
    # battles for the first triangle member inline, avoiding a separate
    # enrichment pass with chunked Neo4j round-trips.  The ``degree``
    # subquery pre-filters super-hubs whose expansion would be quadratic.
    find_query = """
        MATCH (a:Character)
        WHERE a.character_id >= $lo AND a.character_id < $hi
        CALL {
            WITH a
            MATCH (a)-[r:CO_OCCURS_WITH]->(:Character)
            RETURN count(r) AS deg
        }
        WITH a WHERE deg > 0 AND deg <= $max_degree
        MATCH (a)-[:CO_OCCURS_WITH]->(b:Character)-[:CO_OCCURS_WITH]->(c:Character)
        WHERE a.character_id < b.character_id
          AND b.character_id < c.character_id
          AND EXISTS { (a)-[:CO_OCCURS_WITH]->(c) }
        WITH a, b, c LIMIT $batch_limit
        CALL {
            WITH a
            OPTIONAL MATCH (a)-[:ON_SIDE]->(:BattleSide)<-[:HAS_SIDE]-(battle:Battle)
            RETURN collect(DISTINCT battle.battle_id)[..10] AS battle_ids
        }
        RETURN
            [a.character_id, b.character_id, c.character_id] AS member_ids,
            battle_ids,
            1 AS occurrence_count,
            toFloat(
                (COALESCE(a.suspicion_score, 0) + COALESCE(b.suspicion_score, 0) + COALESCE(c.suspicion_score, 0)) / 3.0
            ) AS suspicion_relevance
    """

    raw_triangles: list[dict[str, Any]] = []
    cursor = lo
    while cursor <= hi and len(raw_triangles) < _TRIANGLE_LIMIT:
        remaining = _TRIANGLE_LIMIT - len(raw_triangles)
        try:
            rows = client.query(
                find_query,
                parameters={
                    "lo": cursor,
                    "hi": cursor + _TRIANGLE_BATCH_SIZE,
                    "batch_limit": remaining,
                    "max_degree": _TRIANGLE_MAX_A_DEGREE,
                },
                timeout_seconds=_TRIANGLE_QUERY_TIMEOUT,
            )
            raw_triangles.extend(rows)
        except Neo4jError as exc:
            # A single slow window should not kill the whole detector — log
            # and advance the cursor so the remaining windows still run.
            logger.warning(
                "Triangle window [%d, %d) failed, skipping: %s",
                cursor, cursor + _TRIANGLE_BATCH_SIZE, exc,
            )
        cursor += _TRIANGLE_BATCH_SIZE

    raw_triangles = raw_triangles[:_TRIANGLE_LIMIT]

    return [
        {
            "motif_type": "triangle",
            "member_ids": t["member_ids"],
            "battle_ids": t.get("battle_ids") or [],
            "occurrence_count": int(t.get("occurrence_count") or 1),
            "suspicion_relevance": t["suspicion_relevance"],
        }
        for t in raw_triangles
    ]


def _detect_stars(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect star motifs: 1 hub connected to 4+ others who don't connect to each other.

    Filters out super-hubs (degree > ``_STAR_MAX_HUB_DEGREE``) **before**
    materialising neighbour collections, which previously blew the query
    memory pool for pop characters with thousands of co-occurrences (issue
    #902).  Only examines bidirectional neighbours inline and caps the
    leaf list to 30 to keep UNWIND fan-out bounded.
    """
    return client.query(
        """
        MATCH (hub:Character)
        CALL {
            WITH hub
            MATCH (hub)-[r:CO_OCCURS_WITH]-(:Character)
            RETURN count(r) AS hub_degree
        }
        WITH hub, hub_degree
        WHERE hub_degree >= 4 AND hub_degree <= $max_hub_degree
        MATCH (hub)-[:CO_OCCURS_WITH]-(leaf:Character)
        WITH hub, collect(DISTINCT leaf)[..30] AS leaves
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
        parameters={"max_hub_degree": _STAR_MAX_HUB_DEGREE},
        timeout_seconds=_STAR_QUERY_TIMEOUT,
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
        error_detail = "; ".join(
            f"{name}: {detector_errors.get(name, '?')[:300]}"
            for name in failed_detectors
        )
        if len(failed_detectors) == len(detectors):
            # ALL detectors failed — nothing to persist
            duration_ms = int((time.perf_counter() - started) * 1000)
            logger.error(
                "Job %s failing — ALL detectors failed: %s — detail: %s",
                job_name, ", ".join(failed_detectors), error_detail,
            )
            return JobResult.failed(
                job_key=job_name,
                error=f"All detectors failed: {', '.join(failed_detectors)}",
                duration_ms=duration_ms,
                meta={
                    "counts": counts_by_type,
                    "failed": failed_detectors,
                    "errors": {k: v[:500] for k, v in detector_errors.items()},
                },
            ).to_dict()
        # Some detectors failed but others succeeded — persist what we have
        logger.warning(
            "Job %s partial failure — detectors failed: %s (continuing with %d results) — detail: %s",
            job_name, ", ".join(failed_detectors), len(all_motifs), error_detail,
        )

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
    summary = f"Motif detection: {total_written} motifs found ({', '.join(f'{k}={v}' for k, v in counts_by_type.items())})."
    meta: dict[str, Any] = {"counts": counts_by_type}
    if failed_detectors:
        summary += f" Partial failure: {', '.join(failed_detectors)} skipped."
        meta["failed"] = failed_detectors
        meta["errors"] = {k: v[:500] for k, v in detector_errors.items()}
    return JobResult.success(
        job_key=job_name,
        summary=summary,
        rows_processed=total_written,
        rows_written=total_written,
        rows_seen=total_written,
        duration_ms=duration_ms,
        meta=meta,
    ).to_dict()
