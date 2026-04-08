"""Neo4j GDS ML Exploration — Phase 1: Core Modernization.

Replaces hand-rolled Cypher approximations (3-iteration PageRank, sampled
betweenness, label propagation) with proper GDS-native algorithms.  Introduces
time-window discipline (30d / 90d / lifetime projections) and a versioned
feature store so graph-derived outputs are governed, not ad-hoc.

Phase 1 scope:
  - Named graph projections with time-window filtering
  - GDS-native PageRank, Betweenness, Degree centrality
  - Leiden community detection (falls back to Louvain, then LPA)
  - HITS hub/authority scoring
  - K-Core decomposition
  - Articulation point detection
  - Versioned writeback to graph_ml_features table

Future phases (not implemented here):
  - Phase 2: FastRP embeddings + similarity/anomaly
  - Phase 3: Link prediction (topological baseline -> supervised)
  - Phase 4: Node classification (roles, battle types, demand regression)

Results are materialized to ``graph_ml_features`` and ``graph_ml_run_log``
in MariaDB.  Neo4j is never queried in the rendering hot path.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig


# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

FEATURE_VERSION = "v1"
BATCH_SIZE = 500

# Time windows: each produces a separate graph projection and feature row.
# "recent" = operational signal, "medium" = trend stability, "long" = structural.
TIME_WINDOWS: list[dict[str, Any]] = [
    {"key": "30d",       "days": 30,   "label": "recent operational"},
    {"key": "90d",       "days": 90,   "label": "trend stability"},
    {"key": "lifetime",  "days": None, "label": "structural lifetime"},
]

# Relationship types used for character combat graph projections.
CHARACTER_REL_TYPES = [
    "CO_OCCURS_WITH",
    "DIRECT_COMBAT",
    "ASSISTED_KILL",
    "SAME_FLEET",
]

CHARACTER_REL_TYPES_STR = "|".join(CHARACTER_REL_TYPES)


# ---------------------------------------------------------------------------
#  GDS availability + projection helpers
# ---------------------------------------------------------------------------

def _has_gds(client: Neo4jClient) -> bool:
    """Check if the Neo4j Graph Data Science plugin is available."""
    try:
        result = client.query("RETURN gds.version() AS v")
        return len(result) > 0
    except Exception:
        return False


def _drop_projection(client: Neo4jClient, name: str) -> None:
    """Silently drop a GDS projection if it exists."""
    try:
        client.query(f"CALL gds.graph.drop('{name}', false)")
    except Exception:
        pass


def _projection_name(window_key: str) -> str:
    """Deterministic projection name for a time window."""
    return f"character_combat_{window_key}"


def _create_projection(client: Neo4jClient, window: dict[str, Any]) -> bool:
    """Create a GDS in-memory projection for a given time window.

    Filters relationships by last_seen/updated_at/last_at within the window.
    Lifetime window includes all relationships (no date filter).
    """
    name = _projection_name(window["key"])
    _drop_projection(client, name)

    days = window["days"]

    if days is not None:
        # Time-filtered projection via Cypher projection
        # Filter on the most common timestamp properties across rel types
        client.query(
            f"""
            CALL gds.graph.project.cypher(
                '{name}',
                'MATCH (c:Character) RETURN id(c) AS id',
                'MATCH (a:Character)-[r:{CHARACTER_REL_TYPES_STR}]-(b:Character)
                 WHERE COALESCE(r.last_seen, COALESCE(r.last_at, COALESCE(r.updated_at, datetime())))
                       >= datetime() - duration({{days: {days}}})
                 RETURN id(a) AS source, id(b) AS target,
                        COALESCE(r.weight, COALESCE(r.count, 1.0)) AS weight'
            )
            """,
            timeout_seconds=180,
        )
    else:
        # Lifetime: native projection, all relationships, undirected
        rel_config_parts = []
        for rt in CHARACTER_REL_TYPES:
            rel_config_parts.append(
                f"{rt}: {{orientation: 'UNDIRECTED', properties: 'weight'}}"
            )
        rel_config = "{" + ", ".join(rel_config_parts) + "}"

        client.query(
            f"""
            CALL gds.graph.project(
                '{name}',
                'Character',
                {rel_config},
                {{relationshipProperties: 'weight'}}
            )
            """,
            timeout_seconds=180,
        )

    return True


# ---------------------------------------------------------------------------
#  GDS algorithm runners
# ---------------------------------------------------------------------------

def _gds_pagerank(client: Neo4jClient, proj: str) -> dict[int, float]:
    """Full convergent PageRank via GDS (not a 3-iteration approximation)."""
    rows = client.query(
        f"""
        CALL gds.pageRank.stream('{proj}', {{
            maxIterations: 40,
            dampingFactor: 0.85,
            tolerance: 0.0001
        }})
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        RETURN node.character_id AS character_id, score
        """,
        timeout_seconds=300,
    )
    return {int(r["character_id"]): float(r["score"]) for r in rows if r.get("character_id")}


def _gds_betweenness(client: Neo4jClient, proj: str) -> dict[int, float]:
    """Proper betweenness centrality via GDS (not sampled path counting)."""
    rows = client.query(
        f"""
        CALL gds.betweenness.stream('{proj}')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        RETURN node.character_id AS character_id, score
        """,
        timeout_seconds=300,
    )
    return {int(r["character_id"]): float(r["score"]) for r in rows if r.get("character_id")}


def _gds_degree(client: Neo4jClient, proj: str) -> dict[int, int]:
    """Degree centrality via GDS."""
    rows = client.query(
        f"""
        CALL gds.degree.stream('{proj}')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        RETURN node.character_id AS character_id, toInteger(score) AS score
        """,
        timeout_seconds=120,
    )
    return {int(r["character_id"]): int(r["score"]) for r in rows if r.get("character_id")}


def _gds_leiden(client: Neo4jClient, proj: str) -> dict[int, int] | None:
    """Leiden community detection — better than Louvain for quality, hierarchical."""
    try:
        rows = client.query(
            f"""
            CALL gds.leiden.stream('{proj}', {{
                maxLevels: 10,
                gamma: 1.0,
                theta: 0.01
            }})
            YIELD nodeId, communityId
            WITH gds.util.asNode(nodeId) AS node, communityId
            RETURN node.character_id AS character_id, communityId AS community_id
            """,
            timeout_seconds=300,
        )
        return {int(r["character_id"]): int(r["community_id"]) for r in rows if r.get("character_id")}
    except Exception:
        return None


def _gds_louvain(client: Neo4jClient, proj: str) -> dict[int, int] | None:
    """Louvain community detection — fallback if Leiden unavailable."""
    try:
        rows = client.query(
            f"""
            CALL gds.louvain.stream('{proj}')
            YIELD nodeId, communityId
            WITH gds.util.asNode(nodeId) AS node, communityId
            RETURN node.character_id AS character_id, communityId AS community_id
            """,
            timeout_seconds=300,
        )
        return {int(r["character_id"]): int(r["community_id"]) for r in rows if r.get("character_id")}
    except Exception:
        return None


def _gds_hits(client: Neo4jClient, proj: str) -> dict[int, tuple[float, float]] | None:
    """HITS hub/authority — identifies FCs (hubs) vs line members (authorities)."""
    try:
        rows = client.query(
            f"""
            CALL gds.hits.stream('{proj}', {{hitsIterations: 20}})
            YIELD nodeId, values
            WITH gds.util.asNode(nodeId) AS node, values
            RETURN node.character_id AS character_id,
                   values.hub AS hub, values.auth AS auth
            """,
            timeout_seconds=180,
        )
        return {
            int(r["character_id"]): (float(r.get("hub", 0)), float(r.get("auth", 0)))
            for r in rows if r.get("character_id")
        }
    except Exception:
        return None


def _gds_kcore(client: Neo4jClient, proj: str) -> dict[int, int] | None:
    """K-Core decomposition — find densely connected cores of clusters."""
    try:
        rows = client.query(
            f"""
            CALL gds.kcore.stream('{proj}')
            YIELD nodeId, coreValue
            WITH gds.util.asNode(nodeId) AS node, coreValue
            RETURN node.character_id AS character_id, coreValue AS kcore
            """,
            timeout_seconds=180,
        )
        return {int(r["character_id"]): int(r["kcore"]) for r in rows if r.get("character_id")}
    except Exception:
        return None


def _gds_articulation_points(client: Neo4jClient, proj: str) -> set[int] | None:
    """Articulation points — characters whose removal splits the graph."""
    try:
        rows = client.query(
            f"""
            CALL gds.articulationPoints.stream('{proj}')
            YIELD nodeId
            WITH gds.util.asNode(nodeId) AS node
            RETURN node.character_id AS character_id
            """,
            timeout_seconds=180,
        )
        return {int(r["character_id"]) for r in rows if r.get("character_id")}
    except Exception:
        return None


# ---------------------------------------------------------------------------
#  Fallback: Cypher-only algorithms (when GDS unavailable)
# ---------------------------------------------------------------------------

def _fallback_pagerank(client: Neo4jClient, iterations: int = 5) -> dict[int, float]:
    """Power-method PageRank in Cypher (improved from 3 to 5 iterations)."""
    client.query("MATCH (c:Character) SET c._pr_tmp = 1.0")
    for _ in range(iterations):
        client.query(
            f"""
            MATCH (c:Character)
            OPTIONAL MATCH (c)-[:{CHARACTER_REL_TYPES_STR}]-(n:Character)
            WITH c, collect(n) AS neighbors
            WITH c, neighbors,
                 CASE WHEN size(neighbors) > 0
                      THEN reduce(s = 0.0, n IN neighbors |
                          s + COALESCE(n._pr_tmp, 1.0) /
                          CASE WHEN size([(n)-[:{CHARACTER_REL_TYPES_STR}]-() | 1]) > 0
                               THEN size([(n)-[:{CHARACTER_REL_TYPES_STR}]-() | 1])
                               ELSE 1 END)
                      ELSE 0.0 END AS incoming
            SET c._pr_tmp = 0.15 + 0.85 * incoming
            """
        )
    rows = client.query(
        "MATCH (c:Character) WHERE c._pr_tmp IS NOT NULL "
        "RETURN c.character_id AS character_id, c._pr_tmp AS score"
    )
    # Clean up temp property
    client.query("MATCH (c:Character) REMOVE c._pr_tmp")
    return {int(r["character_id"]): float(r["score"]) for r in rows if r.get("character_id")}


def _fallback_degree(client: Neo4jClient) -> dict[int, int]:
    """Degree centrality via Cypher count."""
    rows = client.query(
        f"""
        MATCH (c:Character)
        OPTIONAL MATCH (c)-[r:{CHARACTER_REL_TYPES_STR}]-(:Character)
        WITH c, count(r) AS deg
        RETURN c.character_id AS character_id, deg AS score
        """
    )
    return {int(r["character_id"]): int(r["score"]) for r in rows if r.get("character_id")}


def _fallback_community_lpa(client: Neo4jClient, rounds: int = 8) -> dict[int, int]:
    """Label propagation community detection in Cypher."""
    client.query("MATCH (c:Character) SET c._comm_tmp = c.character_id")
    for _ in range(rounds):
        client.query(
            f"""
            MATCH (c:Character)-[r:{CHARACTER_REL_TYPES_STR}]-(n:Character)
            WHERE n._comm_tmp IS NOT NULL
            WITH c, n._comm_tmp AS neighbor_label,
                 count(*) + sum(COALESCE(r.weight, COALESCE(r.count, 1))) AS weight
            ORDER BY weight DESC
            WITH c, collect(neighbor_label)[0] AS dominant_label
            SET c._comm_tmp = dominant_label
            """
        )
    rows = client.query(
        "MATCH (c:Character) WHERE c._comm_tmp IS NOT NULL "
        "RETURN c.character_id AS character_id, toInteger(c._comm_tmp) AS community_id"
    )
    client.query("MATCH (c:Character) REMOVE c._comm_tmp")
    return {int(r["character_id"]): int(r["community_id"]) for r in rows if r.get("character_id")}


# ---------------------------------------------------------------------------
#  Feature assembly + writeback
# ---------------------------------------------------------------------------

def _run_window(
    client: Neo4jClient,
    db: SupplyCoreDb,
    window: dict[str, Any],
    run_id: str,
    computed_at: str,
    use_gds: bool,
) -> dict[str, Any]:
    """Run all Phase 1 algorithms for a single time window and write features."""
    wkey = window["key"]
    proj = _projection_name(wkey)
    algo_report: dict[str, str] = {}

    # --- Create projection ---
    if use_gds:
        try:
            _create_projection(client, window)
            algo_report["projection"] = "ok"
        except Exception as exc:
            algo_report["projection"] = f"failed: {exc}"
            use_gds = False  # fall back for this window

    # --- PageRank ---
    if use_gds:
        pagerank = _gds_pagerank(client, proj)
        algo_report["pagerank"] = f"gds ({len(pagerank)} nodes)"
    else:
        pagerank = _fallback_pagerank(client)
        algo_report["pagerank"] = f"fallback ({len(pagerank)} nodes)"

    # --- Betweenness ---
    if use_gds:
        betweenness = _gds_betweenness(client, proj)
        algo_report["betweenness"] = f"gds ({len(betweenness)} nodes)"
    else:
        betweenness = {}
        algo_report["betweenness"] = "skipped (no GDS)"

    # --- Degree ---
    if use_gds:
        degree = _gds_degree(client, proj)
        algo_report["degree"] = f"gds ({len(degree)} nodes)"
    else:
        degree = _fallback_degree(client)
        algo_report["degree"] = f"fallback ({len(degree)} nodes)"

    # --- Community detection: Leiden > Louvain > LPA ---
    communities: dict[int, int] = {}
    community_algo = "none"
    if use_gds:
        communities_result = _gds_leiden(client, proj)
        if communities_result is not None:
            communities = communities_result
            community_algo = "leiden"
        else:
            communities_result = _gds_louvain(client, proj)
            if communities_result is not None:
                communities = communities_result
                community_algo = "louvain"
    if not communities:
        communities = _fallback_community_lpa(client)
        community_algo = "lpa"
    algo_report["community"] = f"{community_algo} ({len(communities)} nodes)"

    # --- HITS (GDS only, advisory) ---
    hits: dict[int, tuple[float, float]] = {}
    if use_gds:
        hits_result = _gds_hits(client, proj)
        if hits_result is not None:
            hits = hits_result
            algo_report["hits"] = f"gds ({len(hits)} nodes)"
        else:
            algo_report["hits"] = "unavailable"
    else:
        algo_report["hits"] = "skipped (no GDS)"

    # --- K-Core (GDS only) ---
    kcore: dict[int, int] = {}
    if use_gds:
        kcore_result = _gds_kcore(client, proj)
        if kcore_result is not None:
            kcore = kcore_result
            algo_report["kcore"] = f"gds ({len(kcore)} nodes)"
        else:
            algo_report["kcore"] = "unavailable"
    else:
        algo_report["kcore"] = "skipped (no GDS)"

    # --- Articulation Points (GDS only) ---
    artic_points: set[int] = set()
    if use_gds:
        artic_result = _gds_articulation_points(client, proj)
        if artic_result is not None:
            artic_points = artic_result
            algo_report["articulation_points"] = f"gds ({len(artic_points)} nodes)"
        else:
            algo_report["articulation_points"] = "unavailable"
    else:
        algo_report["articulation_points"] = "skipped (no GDS)"

    # --- Clean up projection ---
    if use_gds:
        _drop_projection(client, proj)

    # --- Collect all character IDs ---
    all_char_ids = set(pagerank.keys()) | set(degree.keys()) | set(communities.keys())

    # --- Delete old rows for this window+version, then batch insert ---
    db.execute(
        "DELETE FROM graph_ml_features WHERE time_window = %s AND feature_version = %s",
        (wkey, FEATURE_VERSION),
    )

    rows_written = 0
    char_list = sorted(all_char_ids)
    for offset in range(0, len(char_list), BATCH_SIZE):
        chunk = char_list[offset : offset + BATCH_SIZE]
        values = []
        params: list[Any] = []
        for cid in chunk:
            hub_score, auth_score = hits.get(cid, (0.0, 0.0))
            values.append("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
            params.extend([
                cid,
                wkey,
                FEATURE_VERSION,
                proj,
                round(pagerank.get(cid, 0.0), 8),
                round(betweenness.get(cid, 0.0), 8),
                degree.get(cid, 0),
                round(hub_score, 8),
                round(auth_score, 8),
                kcore.get(cid, 0),
                1 if cid in artic_points else 0,
                communities.get(cid, 0),
                community_algo,
                computed_at,
                run_id,
            ])
        if values:
            db.execute(
                "INSERT INTO graph_ml_features "
                "(character_id, time_window, feature_version, projection_source, "
                "pagerank_score, betweenness_score, degree_centrality, "
                "hits_hub_score, hits_auth_score, kcore_level, is_articulation_point, "
                "community_id, community_algo, computed_at, run_id) "
                "VALUES " + ", ".join(values),
                tuple(params),
            )
            rows_written += len(chunk)

    return {
        "window": wkey,
        "characters": len(all_char_ids),
        "rows_written": rows_written,
        "community_algo": community_algo,
        "communities": len(set(communities.values())) if communities else 0,
        "articulation_points": len(artic_points),
        "kcore_max": max(kcore.values()) if kcore else 0,
        "algorithms": algo_report,
    }


# ---------------------------------------------------------------------------
#  Main entry point
# ---------------------------------------------------------------------------

def run_neo4j_ml_exploration(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Phase 1: GDS core modernization with time-window discipline."""
    started = time.perf_counter()
    job_name = "neo4j_ml_exploration"
    run_id = f"gds_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    use_gds = _has_gds(client)
    analytics_path = "gds" if use_gds else "fallback"

    # Log run start
    db.execute(
        "INSERT INTO graph_ml_run_log "
        "(run_id, started_at, status, time_window, feature_version, "
        "gds_available, analytics_path, config_json) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (run_id, computed_at, "running", "all", FEATURE_VERSION,
         1 if use_gds else 0, analytics_path,
         json_dumps_safe({"time_windows": [w["key"] for w in TIME_WINDOWS]})),
    )

    total_features = 0
    total_characters = 0
    window_results: list[dict[str, Any]] = []
    errors: list[str] = []

    for window in TIME_WINDOWS:
        try:
            result = _run_window(client, db, window, run_id, computed_at, use_gds)
            window_results.append(result)
            total_features += result["rows_written"]
            total_characters = max(total_characters, result["characters"])
        except Exception as exc:
            errors.append(f"{window['key']}: {exc}")
            window_results.append({"window": window["key"], "error": str(exc)})

    # Log run completion
    finished_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    status = "success" if not errors else "failed"
    db.execute(
        "UPDATE graph_ml_run_log SET finished_at = %s, status = %s, "
        "characters_processed = %s, features_written = %s, error_text = %s "
        "WHERE run_id = %s",
        (finished_at, status, total_characters, total_features,
         "; ".join(errors) if errors else None, run_id),
    )

    # Intelligence snapshot for dashboard visibility
    db.upsert_intelligence_snapshot(
        snapshot_key="neo4j_ml_exploration_state",
        payload_json=json_dumps_safe({
            "run_id": run_id,
            "analytics_path": analytics_path,
            "total_characters": total_characters,
            "total_features": total_features,
            "windows": window_results,
            "errors": errors,
        }),
        metadata_json=json_dumps_safe({
            "source": "neo4j+graph_ml_features",
            "reason": "scheduler:python",
            "phase": "1_core_modernization",
        }),
        expires_seconds=7200,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)

    summary_parts = [
        f"Phase 1 GDS exploration: {total_characters} characters,",
        f"{total_features} features across {len(TIME_WINDOWS)} windows.",
        f"Path: {analytics_path}.",
    ]
    if errors:
        summary_parts.append(f"Errors: {len(errors)}.")

    return JobResult.success(
        job_key=job_name,
        summary=" ".join(summary_parts),
        rows_processed=total_characters,
        rows_written=total_features,
        duration_ms=duration_ms,
        meta={
            "run_id": run_id,
            "analytics_path": analytics_path,
            "feature_version": FEATURE_VERSION,
            "windows": window_results,
            "errors": errors,
        },
    ).to_dict()
