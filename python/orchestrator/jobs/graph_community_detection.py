from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

LABEL_PROPAGATION_ROUNDS = 8
PAGERANK_ITERATIONS = 3
BETWEENNESS_SAMPLE_RATE = 0.01
BETWEENNESS_MAX_PATH_LENGTH = 5
BRIDGE_COMMUNITY_THRESHOLD = 2


def _run_label_propagation(client: Neo4jClient, rounds: int) -> None:
    """Cypher-native label propagation community detection."""
    # Initialize: each character is its own community
    client.query("MATCH (c:Character) SET c.community_label = c.character_id")

    # Iterate: adopt the most common neighbor label weighted by edge count
    for _ in range(rounds):
        client.query(
            """
            MATCH (c:Character)-[r:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(n:Character)
            WHERE n.community_label IS NOT NULL
            WITH c, n.community_label AS neighbor_label,
                 count(*) + sum(COALESCE(r.weight, COALESCE(r.count, 1))) AS weight
            ORDER BY weight DESC
            WITH c, collect(neighbor_label)[0] AS dominant_label
            SET c.community_label = dominant_label
            """
        )


def _run_pagerank_approximation(client: Neo4jClient, iterations: int) -> None:
    """3-iteration power-method PageRank approximation in Cypher."""
    # Initialize all nodes to 1.0
    client.query("MATCH (c:Character) SET c.pr = 1.0")

    for _ in range(iterations):
        client.query(
            """
            MATCH (c:Character)
            OPTIONAL MATCH (c)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(n:Character)
            WITH c, collect(n) AS neighbors
            WITH c, neighbors,
                 CASE WHEN size(neighbors) > 0
                      THEN reduce(s = 0.0, n IN neighbors |
                          s + COALESCE(n.pr, 1.0) / CASE WHEN size([(n)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-() | 1]) > 0
                                                         THEN size([(n)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-() | 1])
                                                         ELSE 1 END)
                      ELSE 0.0 END AS incoming
            SET c.pr = 0.15 + 0.85 * incoming
            """
        )


def _compute_betweenness_approximation(client: Neo4jClient) -> None:
    """Approximate betweenness centrality via shortest-path sampling."""
    # Initialize
    client.query("MATCH (c:Character) SET c.betweenness_approx = 0")

    # Sample random pairs and count path traversals
    client.query(
        """
        MATCH (a:Character), (b:Character)
        WHERE rand() < $sample_rate AND a.character_id <> b.character_id
        WITH a, b LIMIT 200
        MATCH p = shortestPath((a)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET*..5]-(b))
        UNWIND nodes(p) AS intermediate
        WITH intermediate WHERE intermediate:Character
        WITH intermediate, count(*) AS path_count
        SET intermediate.betweenness_approx = COALESCE(intermediate.betweenness_approx, 0) + path_count
        """,
        {"sample_rate": BETWEENNESS_SAMPLE_RATE},
    )


def _export_community_assignments(client: Neo4jClient, db: SupplyCoreDb, computed_at: str) -> int:
    """Export community assignments, centrality scores to MariaDB."""
    rows = client.query(
        """
        MATCH (c:Character)
        WHERE c.community_label IS NOT NULL
        OPTIONAL MATCH (c)-[r:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(:Character)
        WITH c, count(r) AS degree
        RETURN
            toInteger(c.character_id) AS character_id,
            toInteger(c.community_label) AS community_id,
            toFloat(COALESCE(c.pr, 0.0)) AS pagerank_score,
            toFloat(COALESCE(c.betweenness_approx, 0.0)) AS betweenness_centrality,
            toInteger(degree) AS degree_centrality
        """
    )

    if not rows:
        return 0

    # Compute community sizes
    community_sizes: dict[int, int] = {}
    for r in rows:
        cid = int(r.get("community_id") or 0)
        community_sizes[cid] = community_sizes.get(cid, 0) + 1

    # Detect bridge nodes: characters whose neighbors span multiple communities
    bridge_set: set[int] = set()
    bridge_rows = client.query(
        """
        MATCH (c:Character)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(n:Character)
        WHERE c.community_label IS NOT NULL AND n.community_label IS NOT NULL
        WITH c, collect(DISTINCT n.community_label) AS neighbor_communities
        WHERE size(neighbor_communities) >= $threshold
        RETURN toInteger(c.character_id) AS character_id
        """,
        {"threshold": BRIDGE_COMMUNITY_THRESHOLD},
    )
    for br in bridge_rows:
        cid = int(br.get("character_id") or 0)
        if cid > 0:
            bridge_set.add(cid)

    # Truncate and insert
    db.execute("DELETE FROM graph_community_assignments")

    batch_size = 500
    total_written = 0
    for offset in range(0, len(rows), batch_size):
        chunk = rows[offset:offset + batch_size]
        values = []
        params: list[Any] = []
        for r in chunk:
            char_id = int(r.get("character_id") or 0)
            comm_id = int(r.get("community_id") or 0)
            if char_id <= 0:
                continue
            comm_size = community_sizes.get(comm_id, 0)
            membership = min(1.0, comm_size / max(1, len(rows))) if comm_size > 1 else 1.0
            is_bridge = 1 if char_id in bridge_set else 0
            values.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            params.extend([
                char_id,
                comm_id,
                comm_size,
                round(membership, 4),
                is_bridge,
                float(r.get("betweenness_centrality") or 0.0),
                float(r.get("pagerank_score") or 0.0),
                int(r.get("degree_centrality") or 0),
                computed_at,
            ])
        if values:
            db.execute(
                "INSERT INTO graph_community_assignments "
                "(character_id, community_id, community_size, membership_score, is_bridge, "
                "betweenness_centrality, pagerank_score, degree_centrality, computed_at) "
                "VALUES " + ", ".join(values),
                tuple(params),
            )
            total_written += len(values)

    return total_written


def run_graph_community_detection_sync(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_community_detection_sync"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})

    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Run algorithms in Neo4j
    _run_label_propagation(client, LABEL_PROPAGATION_ROUNDS)
    _run_pagerank_approximation(client, PAGERANK_ITERATIONS)
    _compute_betweenness_approximation(client)

    # Export to MariaDB
    rows_written = _export_community_assignments(client, db, computed_at)

    # Count distinct communities
    community_count_row = db.fetch_scalar(
        "SELECT COUNT(DISTINCT community_id) FROM graph_community_assignments"
    )

    bridge_count = db.fetch_scalar(
        "SELECT COUNT(*) FROM graph_community_assignments WHERE is_bridge = 1"
    )

    snapshot_payload = {
        "total_characters": rows_written,
        "distinct_communities": int(community_count_row or 0),
        "bridge_characters": int(bridge_count or 0),
        "label_propagation_rounds": LABEL_PROPAGATION_ROUNDS,
        "pagerank_iterations": PAGERANK_ITERATIONS,
    }
    db.upsert_intelligence_snapshot(
        snapshot_key="graph_community_detection_state",
        payload_json=json_dumps_safe(snapshot_payload),
        metadata_json=json_dumps_safe({"source": "neo4j+graph_community_assignments", "reason": "scheduler:python"}),
        expires_seconds=7200,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=f"Community detection: {rows_written} characters assigned to {community_count_row or 0} communities, {bridge_count or 0} bridges.",
        rows_processed=rows_written,
        rows_written=rows_written,
        rows_seen=rows_written,
        duration_ms=duration_ms,
        meta={
            "communities": int(community_count_row or 0),
            "bridges": int(bridge_count or 0),
        },
    ).to_dict()
