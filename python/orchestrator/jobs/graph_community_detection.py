from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

logger = logging.getLogger(__name__)

LABEL_PROPAGATION_ROUNDS = 8
# Cypher fallback uses 3 iterations because each iteration is a full
# pattern-match round-trip — anything higher is impractically slow.
# Native GDS PageRank is C++-optimized and converges quickly, so we
# match neo4j_ml_exploration.py's 40-iteration / tolerance settings.
PAGERANK_ITERATIONS = 3
GDS_PAGERANK_MAX_ITERATIONS = 40
GDS_PAGERANK_TOLERANCE = 0.0001
BETWEENNESS_SAMPLE_RATE = 0.01
BETWEENNESS_MAX_PATH_LENGTH = 5
BRIDGE_COMMUNITY_THRESHOLD = 2

GDS_GRAPH_NAME = "character_community"
_COMMUNITY_REL_TYPES = ["CO_OCCURS_WITH", "DIRECT_COMBAT", "ASSISTED_KILL", "SAME_FLEET"]
_COMMUNITY_REL_CYPHER = "CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET"


# ---------------------------------------------------------------------------
#  GDS availability + projection
# ---------------------------------------------------------------------------

def _has_gds(client: Neo4jClient) -> bool:
    """Check if the Neo4j Graph Data Science plugin is available."""
    try:
        result = client.query("RETURN gds.version() AS v")
        return len(result) > 0
    except Exception:
        return False


def _ensure_gds_projection(client: Neo4jClient) -> bool:
    """Create the GDS in-memory graph projection for character community analysis.

    Only includes relationship types that actually exist in the database to
    avoid GDS ``IllegalArgumentException`` when some types are absent.
    """
    try:
        try:
            client.query(f"CALL gds.graph.drop('{GDS_GRAPH_NAME}', false)")
        except Exception:
            pass

        # Filter to relationship types that exist in the database
        present_types = []
        for rt in _COMMUNITY_REL_TYPES:
            try:
                rows = client.query(f"MATCH ()-[r:{rt}]->() RETURN r LIMIT 1")
                if rows:
                    present_types.append(rt)
            except Exception:
                pass

        if not present_types:
            logger.warning("GDS projection skipped: no community relationship types exist")
            return False

        rel_projection = ", ".join(
            f"{rt}: {{orientation: 'UNDIRECTED'}}" for rt in present_types
        )
        client.query(
            f"""
            CALL gds.graph.project(
                '{GDS_GRAPH_NAME}',
                'Character',
                {{{rel_projection}}}
            )
            """,
            timeout_seconds=120,
        )
        return True
    except Exception as exc:
        logger.warning("GDS projection failed for community detection: %s", exc)
        return False


def _drop_gds_projection(client: Neo4jClient) -> None:
    """Drop the GDS projection if it exists."""
    try:
        client.query(f"CALL gds.graph.drop('{GDS_GRAPH_NAME}', false)")
    except Exception:
        pass


# ---------------------------------------------------------------------------
#  GDS-based algorithms
# ---------------------------------------------------------------------------

def _gds_community_detection(client: Neo4jClient) -> str:
    """Run community detection and write community_label back to nodes.

    Tries Leiden first (best modularity, hierarchical), then falls back
    to Louvain, then to label propagation.  Mirrors the cascade used in
    ``neo4j_ml_exploration.py`` so the two community-detection paths
    agree on algorithm preference instead of one running LPA-only and
    the other Leiden-first.

    The projection is built with ``orientation: 'UNDIRECTED'`` in
    ``_ensure_gds_projection``, which is what Leiden requires.

    Returns the name of the algorithm that actually wrote the property.
    """
    # Leiden — preferred when available
    try:
        client.query(
            f"""
            CALL gds.leiden.write('{GDS_GRAPH_NAME}', {{
                writeProperty: 'community_label',
                maxLevels: 10,
                gamma: 1.0,
                theta: 0.01
            }})
            """,
            timeout_seconds=180,
        )
        return "leiden"
    except Exception as exc:
        logger.info("GDS Leiden unavailable (%s) — trying Louvain", exc)

    # Louvain — solid fallback, available in every modern GDS
    try:
        client.query(
            f"""
            CALL gds.louvain.write('{GDS_GRAPH_NAME}', {{
                writeProperty: 'community_label'
            }})
            """,
            timeout_seconds=180,
        )
        return "louvain"
    except Exception as exc:
        logger.info("GDS Louvain unavailable (%s) — falling back to label propagation", exc)

    # Label propagation — last-resort GDS algorithm
    client.query(
        f"""
        CALL gds.labelPropagation.write('{GDS_GRAPH_NAME}', {{
            writeProperty: 'community_label',
            maxIterations: {LABEL_PROPAGATION_ROUNDS}
        }})
        """,
        timeout_seconds=120,
    )
    return "label_propagation"


def _gds_pagerank(client: Neo4jClient) -> None:
    """Run GDS PageRank and write pr score back to nodes.

    Native GDS PageRank is C++-optimized — using the 3-iteration cap
    designed for the slow Cypher fallback writes essentially
    un-converged scores.  Match the 40-iteration / tolerance settings
    used in ``neo4j_ml_exploration.py`` so PageRank actually converges.
    """
    client.query(
        f"""
        CALL gds.pageRank.write('{GDS_GRAPH_NAME}', {{
            writeProperty: 'pr',
            maxIterations: {GDS_PAGERANK_MAX_ITERATIONS},
            tolerance: {GDS_PAGERANK_TOLERANCE},
            dampingFactor: 0.85
        }})
        """,
        timeout_seconds=180,
    )


def _gds_betweenness(client: Neo4jClient) -> None:
    """Run GDS betweenness centrality and write score back to nodes."""
    client.query(
        f"""
        CALL gds.betweenness.write('{GDS_GRAPH_NAME}', {{
            writeProperty: 'betweenness_approx',
            samplingSize: 200
        }})
        """,
        timeout_seconds=300,
    )


# ---------------------------------------------------------------------------
#  Cypher-native fallback algorithms (used when GDS is unavailable)
# ---------------------------------------------------------------------------

def _run_label_propagation(client: Neo4jClient, rounds: int) -> None:
    """Cypher-native label propagation community detection."""
    client.query("MATCH (c:Character) SET c.community_label = c.character_id")

    for _ in range(rounds):
        client.query(
            f"""
            MATCH (c:Character)-[r:{_COMMUNITY_REL_CYPHER}]-(n:Character)
            WHERE n.community_label IS NOT NULL
            WITH c, n.community_label AS neighbor_label,
                 count(*) + sum(COALESCE(r.weight, COALESCE(r.count, 1))) AS weight
            ORDER BY weight DESC
            WITH c, collect(neighbor_label)[0] AS dominant_label
            SET c.community_label = dominant_label
            """
        )


def _run_pagerank_approximation(client: Neo4jClient, iterations: int) -> None:
    """3-iteration power-method PageRank approximation in Cypher.

    Uses COUNT {{ }} subquery syntax (Cypher 5.0+) instead of deprecated
    size([(pattern)]) for neighbor degree computation.
    """
    client.query("MATCH (c:Character) SET c.pr = 1.0")

    for _ in range(iterations):
        client.query(
            f"""
            MATCH (c:Character)
            OPTIONAL MATCH (c)-[:{_COMMUNITY_REL_CYPHER}]-(n:Character)
            WITH c, collect(n) AS neighbors
            WITH c, neighbors,
                 CASE WHEN size(neighbors) > 0
                      THEN reduce(s = 0.0, n IN neighbors |
                          s + COALESCE(n.pr, 1.0) / CASE WHEN COUNT {{ (n)-[:{_COMMUNITY_REL_CYPHER}]-() }} > 0
                                                         THEN COUNT {{ (n)-[:{_COMMUNITY_REL_CYPHER}]-() }}
                                                         ELSE 1 END)
                      ELSE 0.0 END AS incoming
            SET c.pr = 0.15 + 0.85 * incoming
            """
        )


def _compute_betweenness_approximation(client: Neo4jClient) -> None:
    """Approximate betweenness centrality via shortest-path sampling."""
    client.query("MATCH (c:Character) SET c.betweenness_approx = 0")

    client.query(
        f"""
        MATCH (a:Character), (b:Character)
        WHERE rand() < $sample_rate AND a.character_id <> b.character_id
        WITH a, b LIMIT 200
        MATCH p = shortestPath((a)-[:{_COMMUNITY_REL_CYPHER}*..5]-(b))
        UNWIND nodes(p) AS intermediate
        WITH intermediate WHERE intermediate:Character
        WITH intermediate, count(*) AS path_count
        SET intermediate.betweenness_approx = COALESCE(intermediate.betweenness_approx, 0) + path_count
        """,
        {"sample_rate": BETWEENNESS_SAMPLE_RATE},
    )


NEO4J_EXPORT_CHUNK = 2000


def _export_community_assignments(client: Neo4jClient, db: SupplyCoreDb, computed_at: str) -> int:
    """Export community assignments, centrality scores to MariaDB.

    Previously this function issued a single Cypher statement that did:

        MATCH (c:Character) WHERE c.community_label IS NOT NULL
        WITH c.community_label AS comm_label, collect(c) AS members, ...
        UNWIND members AS c
        CALL { ... degree ... }
        CALL { ... neighbor_communities ... }
        RETURN ...

    The ``collect(c)`` materialized every character node into Neo4j's
    transaction memory at once, and the per-character subqueries on top of
    that regularly blew the page-cache / transaction memory pool, producing
    the ``MemoryPoolOutOfMemoryError`` tracked in issue #900.

    This rewrite streams the export in three memory-bounded phases:

    1. Aggregate community sizes with a pure ``count()`` query — the result
       is one row per community, which is tiny even for large graphs.
    2. Page character centrality / neighbor_communities in chunks of
       ``NEO4J_EXPORT_CHUNK`` rows, ordered by ``character_id``.
       Each page is a short-lived query whose output fits easily in memory.
    3. Merge community_size / membership in Python and bulk-insert into
       MariaDB in small batches.
    """
    # Phase 1 — community sizes (tiny aggregation result, no node collection).
    community_rows = client.query(
        """
        MATCH (c:Character)
        WHERE c.community_label IS NOT NULL
        RETURN toInteger(c.community_label) AS community_id,
               count(*) AS community_size
        """,
        timeout_seconds=120,
    )
    if not community_rows:
        return 0

    community_sizes: dict[int, int] = {
        int(r.get("community_id") or 0): int(r.get("community_size") or 0)
        for r in community_rows
    }
    total_characters = sum(community_sizes.values())
    if total_characters <= 0:
        return 0

    # Phase 2 — stream character data paginated by character_id.  Each page
    # returns at most ``NEO4J_EXPORT_CHUNK`` characters with their degree
    # and neighbor_community count.  SKIP/LIMIT keeps the working set
    # bounded regardless of total graph size.
    character_rows: list[dict[str, Any]] = []
    skip = 0
    while True:
        page = client.query(
            """
            MATCH (c:Character)
            WHERE c.community_label IS NOT NULL
            WITH c ORDER BY c.character_id ASC SKIP $skip LIMIT $limit
            CALL {
                WITH c
                OPTIONAL MATCH (c)-[r:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(:Character)
                RETURN count(r) AS degree
            }
            CALL {
                WITH c
                OPTIONAL MATCH (c)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(n:Character)
                WHERE n.community_label IS NOT NULL
                RETURN count(DISTINCT n.community_label) AS neighbor_communities
            }
            RETURN
                c.character_id AS character_id,
                toInteger(c.community_label) AS community_id,
                toFloat(COALESCE(c.pr, 0.0)) AS pagerank_score,
                toFloat(COALESCE(c.betweenness_approx, 0.0)) AS betweenness_centrality,
                toInteger(degree) AS degree_centrality,
                CASE WHEN neighbor_communities >= $bridge_threshold THEN 1 ELSE 0 END AS is_bridge
            """,
            {
                "skip": skip,
                "limit": NEO4J_EXPORT_CHUNK,
                "bridge_threshold": BRIDGE_COMMUNITY_THRESHOLD,
            },
            timeout_seconds=180,
        )
        if not page:
            break
        character_rows.extend(page)
        if len(page) < NEO4J_EXPORT_CHUNK:
            break
        skip += NEO4J_EXPORT_CHUNK

    if not character_rows:
        return 0

    # Phase 3 — bulk-insert into MariaDB.
    db.execute("DELETE FROM graph_community_assignments")

    batch_size = 500
    total_written = 0
    for offset in range(0, len(character_rows), batch_size):
        chunk = character_rows[offset:offset + batch_size]
        values = []
        params: list[Any] = []
        for r in chunk:
            char_id = int(r.get("character_id") or 0)
            comm_id = int(r.get("community_id") or 0)
            if char_id <= 0:
                continue
            comm_size = community_sizes.get(comm_id, 0)
            membership = min(1.0, comm_size / max(1, total_characters)) if comm_size > 1 else 1.0
            is_bridge = int(r.get("is_bridge") or 0)
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

    # Prefer GDS algorithms when available — faster and more memory-predictable
    # than Cypher-native iterative loops.
    use_gds = _has_gds(client)
    analytics_path = "cypher_fallback"

    if use_gds:
        use_gds = _ensure_gds_projection(client)

    community_algo = "label_propagation"
    if use_gds:
        analytics_path = "gds"
        logger.info("Community detection: using GDS algorithms")
        community_algo = _gds_community_detection(client)
        logger.info("Community detection: chose %s", community_algo)
        _gds_pagerank(client)
        _gds_betweenness(client)
        _drop_gds_projection(client)
    else:
        logger.info("Community detection: falling back to Cypher-native algorithms")
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
        "pagerank_iterations": (
            GDS_PAGERANK_MAX_ITERATIONS if analytics_path == "gds" else PAGERANK_ITERATIONS
        ),
        "community_algo": community_algo,
        "analytics_path": analytics_path,
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
        summary=(
            f"Community detection ({analytics_path}/{community_algo}): "
            f"{rows_written} characters assigned to "
            f"{community_count_row or 0} communities, {bridge_count or 0} bridges."
        ),
        rows_processed=rows_written,
        rows_written=rows_written,
        rows_seen=rows_written,
        duration_ms=duration_ms,
        meta={
            "communities": int(community_count_row or 0),
            "bridges": int(bridge_count or 0),
            "analytics_path": analytics_path,
            "community_algo": community_algo,
        },
    ).to_dict()
