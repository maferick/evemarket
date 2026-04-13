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

import logging
import time
import uuid
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

logger = logging.getLogger(__name__)

PROJECTION_NAME = "spy_ring_projection"

# Thresholds for edge inclusion
COPRESENCE_MIN_COUNT = 3
IDENTITY_LINK_MIN_SCORE = 0.50
CROSS_SIDE_MIN_COUNT = 1


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _has_gds(client: Neo4jClient) -> bool:
    """Check if the Neo4j Graph Data Science plugin is available."""
    try:
        result = client.query("RETURN gds.version() AS v")
        return len(result) > 0
    except Exception:
        return False


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
        # Build Neo4j client from runtime config
        config = Neo4jConfig.from_runtime(neo4j_raw or {})
        if not config.enabled:
            return _mariadb_only_projection(db, job, run_id, computed_at, started,
                                            reason="neo4j_disabled")

        client = Neo4jClient(config)

        # Check GDS availability
        if not _has_gds(client):
            return _mariadb_only_projection(db, job, run_id, computed_at, started,
                                            reason="no_gds_plugin")

        # Drop stale projection if it exists
        try:
            client.query(f"CALL gds.graph.drop('{PROJECTION_NAME}', false)")
        except Exception:
            pass

        # Build projection via the GDS 2.4+ Cypher projection function.
        #
        # Leiden requires the projection to be UNDIRECTED.  The legacy
        # ``gds.graph.project.cypher`` procedure (also removed in GDS 2.4+)
        # always produced DIRECTED relationships and rejected an
        # ``orientation`` configuration option, which caused
        # ``CALL gds.leiden.write`` to fail with::
        #
        #     Leiden requires relationship projections to be UNDIRECTED.
        #     Selected relationships `[copresence]` are not all undirected.
        #
        # The function-style ``gds.graph.project(...)`` accepts an
        # ``undirectedRelationshipTypes`` option that marks the projected
        # relationship types as undirected without double-counting edges
        # from a symmetric Cypher pattern.  We feed it a UNION ALL of the
        # edge sources so a single projection contains both copresence and
        # cross-side interactions, each tagged with its own relationship
        # type.
        try:
            rows = client.query(
                f"""
                CALL {{
                    MATCH (a:Character)-[r:CO_OCCURS_WITH]->(b:Character)
                    WHERE r.occurrence_count >= {COPRESENCE_MIN_COUNT}
                    RETURN a AS source, b AS target,
                           toFloat(COALESCE(r.weight, 1.0)) AS weight,
                           'copresence' AS rel_type
                    UNION ALL
                    MATCH (a:Character)-[r:DIRECT_COMBAT]->(b:Character)
                    WHERE r.count >= {CROSS_SIDE_MIN_COUNT}
                    RETURN a AS source, b AS target,
                           toFloat(COALESCE(r.count, 1.0)) AS weight,
                           'cross_side' AS rel_type
                }}
                WITH gds.graph.project(
                    $name,
                    source,
                    target,
                    {{
                        relationshipType: rel_type,
                        relationshipProperties: {{ weight: weight }}
                    }},
                    {{ undirectedRelationshipTypes: ['*'] }}
                ) AS g
                RETURN g.graphName AS graphName,
                       g.nodeCount AS nodeCount,
                       g.relationshipCount AS relationshipCount
                """,
                {"name": PROJECTION_NAME},
                timeout_seconds=180,
            )
            if rows:
                node_count = int(rows[0].get("nodeCount") or 0)
                edge_count = int(rows[0].get("relationshipCount") or 0)
        except Exception as exc:
            logger.warning("GDS cypher projection failed: %s — falling back to MariaDB-only", exc)
            return _mariadb_only_projection(db, job, run_id, computed_at, started,
                                            reason="gds_projection_failed")

        # Empty projections cause downstream Leiden / drop calls to fail
        # with GraphNotFoundException — clean up and fall back early.
        if edge_count == 0:
            try:
                client.query(f"CALL gds.graph.drop('{PROJECTION_NAME}', false)")
            except Exception:
                pass
            return _mariadb_only_projection(db, job, run_id, computed_at, started,
                                            reason="empty_projection")

        # Run Leiden community detection on the projection
        try:
            client.query(
                f"""
                CALL gds.leiden.write('{PROJECTION_NAME}', {{
                    writeProperty: 'spy_ring_community',
                    maxLevels: 10,
                    gamma: 1.0,
                    theta: 0.01
                }})
                """,
                timeout_seconds=120,
            )
        except Exception as exc:
            logger.warning("Leiden community detection failed: %s — communities from MariaDB fallback", exc)

        # Capture per-type relationship distribution before dropping the
        # projection so the run log records what was actually built rather
        # than the previous hard-coded ``{"copresence": edge_count}``.
        try:
            count_rows = client.query(
                f"""
                CALL gds.graph.relationships.stream('{PROJECTION_NAME}')
                YIELD relationshipType
                RETURN relationshipType AS rel_type, count(*) AS cnt
                """,
            )
            for row in count_rows:
                rt = row.get("rel_type")
                if rt is not None:
                    rel_dist[str(rt)] = int(row.get("cnt") or 0)
        except Exception:
            pass

        # Drop the projection — always clean up
        try:
            client.query(f"CALL gds.graph.drop('{PROJECTION_NAME}', false)")
        except Exception:
            pass

        if not rel_dist:
            rel_dist = {"all": edge_count}

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
    reason: str = "no_gds_driver",
) -> dict[str, Any]:
    """When GDS is unavailable, record the run as success with zero projection.

    compute_spy_network_cases will read community data from
    graph_community_assignments and identity links from character_identity_links
    directly — it does not strictly require a live GDS projection.
    """
    _finish_projection_run(db, run_id, "success", 0, 0, {},
                            config={"mode": "mariadb_only", "reason": reason})

    duration_ms = int((time.perf_counter() - started) * 1000)
    result = JobResult.success(
        job_key="graph_spy_ring_projection",
        summary=f"MariaDB-only mode ({reason}). Cases will use graph_community_assignments.",
        rows_processed=0, rows_written=0, duration_ms=duration_ms,
        meta={"run_id": run_id, "mode": "mariadb_only", "reason": reason},
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
