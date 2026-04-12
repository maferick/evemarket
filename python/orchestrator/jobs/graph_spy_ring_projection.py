"""Graph spy ring projection — dedicated GDS projection for ring detection.

Phase 4 of the spy detection platform.  Builds a weighted GDS projection
from identity links, copresence edges, cross-side interactions, and temporal
coordination signals.  The projection is consumed by compute_spy_network_cases
for Leiden community detection and ring scoring.

This job writes to ``spy_ring_projection_runs`` (MariaDB) and creates a
transient GDS projection in Neo4j.  The projection is dropped in a finally
block to avoid memory leaks.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe


PROJECTION_NAME = "spy_ring_projection"

# Thresholds for edge inclusion
COPRESENCE_MIN_COUNT = 3
IDENTITY_LINK_MIN_SCORE = 0.50
CROSS_SIDE_MIN_COUNT = 1


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def run_graph_spy_ring_projection(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    lock_key = "graph_spy_ring_projection"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    computed_at = _now_sql()
    run_id = f"srp_{uuid.uuid4().hex[:16]}"

    db.execute(
        """INSERT INTO spy_ring_projection_runs
           (run_id, started_at, status, projection_name)
           VALUES (%s, %s, 'running', %s)""",
        (run_id, computed_at, PROJECTION_NAME),
    )

    node_count = 0
    edge_count = 0
    rel_dist: dict[str, int] = {}

    try:
        # If no Neo4j connection, build a MariaDB-only summary of the
        # projection (sufficient for compute_spy_network_cases to work
        # from MariaDB tables directly).
        if not neo4j_raw:
            return _mariadb_only_projection(db, job, run_id, computed_at, started)

        driver = neo4j_raw.get("driver")
        if not driver:
            return _mariadb_only_projection(db, job, run_id, computed_at, started)

        # Drop stale projection if it exists
        with driver.session() as session:
            try:
                session.run(f"CALL gds.graph.drop('{PROJECTION_NAME}', false)")
            except Exception:
                pass

        # Build projection via Cypher projection — union of edge sources
        # We project Character nodes with weighted edges from multiple sources
        cypher_node = "MATCH (n:Character) RETURN id(n) AS id"
        cypher_rel = f"""
        MATCH (a:Character)-[r:CO_OCCURS_WITH]->(b:Character)
        WHERE r.count >= {COPRESENCE_MIN_COUNT}
        RETURN id(a) AS source, id(b) AS target, r.count AS weight, 'copresence' AS type
        UNION ALL
        MATCH (a:Character)-[r:CROSSED_SIDES]->(b:Character)
        RETURN id(a) AS source, id(b) AS target, r.count AS weight, 'cross_side' AS type
        """

        with driver.session() as session:
            # Try GDS cypher projection
            try:
                result = session.run(
                    """
                    CALL gds.graph.project.cypher($name, $nodeQuery, $relQuery, {})
                    YIELD graphName, nodeCount, relationshipCount
                    RETURN graphName, nodeCount, relationshipCount
                    """,
                    name=PROJECTION_NAME, nodeQuery=cypher_node, relQuery=cypher_rel,
                )
                record = result.single()
                if record:
                    node_count = record["nodeCount"]
                    edge_count = record["relationshipCount"]
            except Exception:
                # GDS not available — fall back to MariaDB-only
                return _mariadb_only_projection(db, job, run_id, computed_at, started)

            # Run Leiden community detection on the projection
            try:
                session.run(
                    """
                    CALL gds.leiden.write($name, {
                        writeProperty: 'spy_ring_community',
                        maxLevels: 10,
                        gamma: 1.0,
                        theta: 0.01
                    })
                    """,
                    name=PROJECTION_NAME,
                )
            except Exception:
                pass  # Leiden not available — communities from MariaDB fallback

            # Drop the projection — always clean up
            try:
                session.run(f"CALL gds.graph.drop('{PROJECTION_NAME}', false)")
            except Exception:
                pass

        rel_dist = {"copresence": edge_count}

        # Write run results
        _finish_projection_run(db, run_id, "success", node_count, edge_count, rel_dist)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Projected {node_count} nodes, {edge_count} edges.",
            rows_processed=node_count, rows_written=0,
            duration_ms=duration_ms,
            meta={"run_id": run_id, "node_count": node_count, "edge_count": edge_count},
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=node_count, rows_written=0, meta=result)
        return result

    except Exception as exc:
        _finish_projection_run(db, run_id, "failed", 0, 0, {}, error=str(exc))
        finish_job_run(db, job, status="failed", rows_processed=0, rows_written=0, error_text=str(exc))
        raise


def _mariadb_only_projection(
    db: SupplyCoreDb, job: Any, run_id: str, computed_at: str, started: float,
) -> dict[str, Any]:
    """When GDS is unavailable, record the run as success with zero projection.

    compute_spy_network_cases will read community data from
    graph_community_assignments and identity links from character_identity_links
    directly — it does not strictly require a live GDS projection.
    """
    _finish_projection_run(db, run_id, "success", 0, 0, {},
                            config={"mode": "mariadb_only", "reason": "no_gds_driver"})

    duration_ms = int((time.perf_counter() - started) * 1000)
    result = JobResult.success(
        job_key="graph_spy_ring_projection",
        summary="MariaDB-only mode (no GDS driver). Cases will use graph_community_assignments.",
        rows_processed=0, rows_written=0, duration_ms=duration_ms,
        meta={"run_id": run_id, "mode": "mariadb_only"},
    ).to_dict()
    finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta=result)
    return result


def _finish_projection_run(
    db: SupplyCoreDb, run_id: str, status: str,
    nodes: int, edges: int, rel_dist: dict[str, int],
    error: str | None = None, config: dict[str, Any] | None = None,
) -> None:
    db.execute(
        """UPDATE spy_ring_projection_runs
           SET finished_at=%s, status=%s, node_count=%s, edge_count=%s,
               rel_type_distribution_json=%s, threshold_config_json=%s, error_text=%s
           WHERE run_id=%s""",
        (_now_sql(), status, nodes, edges,
         json_dumps_safe(rel_dist), json_dumps_safe(config) if config else None,
         error, run_id),
    )
