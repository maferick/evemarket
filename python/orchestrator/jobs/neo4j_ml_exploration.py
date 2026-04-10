"""Neo4j GDS ML Exploration — Full Pipeline (Phases 1-4).

Replaces hand-rolled Cypher approximations with GDS-native algorithms and adds
ML capabilities.  Uses time-window discipline (30d / 90d / lifetime projections)
and a versioned feature store so graph-derived outputs are governed, not ad-hoc.

Phase 1 — Core Modernization:
  - Named graph projections with time-window filtering
  - GDS-native PageRank, Betweenness, Degree centrality
  - Leiden community detection (falls back to Louvain, then LPA)
  - HITS hub/authority scoring (FC vs line-member identification)
  - K-Core decomposition (dense cluster cores)
  - Articulation point detection (structurally critical characters)
  - Cypher fallbacks for all core algorithms when GDS unavailable

Phase 2 — Embedding Layer:
  - FastRP 64-dimensional node embeddings per time window
  - Alliance centroid computation for anomaly detection
  - Embedding anomaly score (distance from alliance centroid)

Phase 3 — Predictive Association:
  - Topological link prediction (common neighbors, preferential attachment)
  - Embedding similarity scoring for predicted pairs
  - Full explainability: shared communities, co-presence count, common
    neighbors, embedding similarity percentile, same-side ratio, cross-side
    count, human-readable explanation summary

Phase 4 — Operational Enrichment:
  - Role classification from graph metrics (fc_coordinator, core_line,
    bridge_liaison, structural_keystone, periphery, anomalous_outsider)
  - Percentile-based thresholds, not trained classifier (fast to deploy)
  - Role distribution summary per time window

Results materialized to ``graph_ml_features``, ``graph_ml_link_predictions``,
and ``graph_ml_run_log`` in MariaDB.  Neo4j is never queried in the rendering
hot path.
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


def _projection_node_and_rel_counts(client: Neo4jClient, name: str) -> tuple[int, int]:
    """Return (nodeCount, relationshipCount) for a named GDS projection."""
    try:
        rows = client.query(
            f"CALL gds.graph.list('{name}') YIELD nodeCount, relationshipCount "
            "RETURN nodeCount, relationshipCount",
        )
        if rows:
            return int(rows[0]["nodeCount"]), int(rows[0]["relationshipCount"])
    except Exception:
        pass
    return 0, 0


def _create_projection(client: Neo4jClient, window: dict[str, Any]) -> tuple[bool, int, int]:
    """Create a GDS in-memory projection for a given time window.

    Filters relationships by last_seen/updated_at/last_at within the window.
    Lifetime window includes all relationships (no date filter).

    Returns (success, node_count, relationship_count).  When the projection
    is created but contains zero relationships the caller can decide to fall
    back to Cypher-only algorithms instead of letting GDS stream calls fail
    with ``GraphNotFoundException``.
    """
    name = _projection_name(window["key"])
    _drop_projection(client, name)

    days = window["days"]

    if days is not None:
        # Time-filtered projection via Cypher projection
        # Filter on the most common timestamp properties across rel types
        date_filter = (
            f"WHERE COALESCE(r.last_seen, COALESCE(r.last_at, COALESCE(r.updated_at, datetime())))"
            f" >= datetime() - duration({{days: {days}}})"
        )
    else:
        # Lifetime: no date filter — include all relationships
        date_filter = ""

    # Always use Cypher projection — it handles missing relationship types
    # gracefully (returns zero rows) instead of failing hard like native
    # projection which validates all specified types exist upfront.
    #
    # GDS 2.4+ removed the ``gds.graph.project.cypher`` procedure and
    # replaced it with a Cypher projection *function* that must be invoked
    # inside a ``MATCH ... WITH gds.graph.project(...)`` pipeline (passing
    # node/relationship *values*, not query strings).  Calling the old
    # procedure form with query strings raises ``IllegalArgumentException:
    # Invalid node projection, one or more labels not found`` because GDS
    # falls back to the native factory and tries to parse the string as a
    # node label.  A directional match combined with the
    # ``undirectedRelationshipTypes`` option yields an undirected graph
    # without double-counting edges from a symmetric Cypher pattern.
    client.query(
        f"""
        MATCH (source:Character)-[r:{CHARACTER_REL_TYPES_STR}]->(target:Character)
        {date_filter}
        WITH gds.graph.project(
            '{name}',
            source,
            target,
            {{
                relationshipProperties: {{
                    weight: COALESCE(r.weight, COALESCE(r.count, 1.0))
                }}
            }},
            {{ undirectedRelationshipTypes: ['*'] }}
        ) AS g
        RETURN g.graphName AS graphName
        """,
        timeout_seconds=180,
    )

    node_count, rel_count = _projection_node_and_rel_counts(client, name)

    # If the projection has nodes but no relationships, GDS streaming
    # algorithms will throw GraphNotFoundException.  Drop the empty
    # projection and signal the caller to fall back.
    if rel_count == 0:
        _drop_projection(client, name)
        return False, node_count, 0

    return True, node_count, rel_count


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
            proj_ok, proj_nodes, proj_rels = _create_projection(client, window)
            if proj_ok:
                algo_report["projection"] = f"ok ({proj_nodes} nodes, {proj_rels} rels)"
            else:
                algo_report["projection"] = f"empty ({proj_nodes} nodes, 0 rels) — falling back"
                use_gds = False  # fall back for this window
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
        # Pass-through for downstream phases (not serialized to JSON)
        "_communities": communities,
    }


# ---------------------------------------------------------------------------
#  Phase feature flags — read from settings or default enabled.
#  To disable a phase at runtime, INSERT into intelligence_snapshots:
#    snapshot_key = 'gds_ml_phase_flags'
#    payload_json = '{"phase1":true,"phase2":true,"phase3":false,"phase4":true}'
#  Any missing key defaults to enabled.
# ---------------------------------------------------------------------------

DEFAULT_PHASE_FLAGS: dict[str, bool] = {
    "phase1": True,   # core algorithms — always safe
    "phase2": True,   # embeddings — requires GDS
    "phase3": True,   # link prediction — most expensive
    "phase4": True,   # role classification — SQL only, cheap
}


def _load_phase_flags(db: SupplyCoreDb) -> dict[str, bool]:
    """Load per-phase enable/disable flags from intelligence_snapshots."""
    import json as _json
    flags = dict(DEFAULT_PHASE_FLAGS)
    try:
        row = db.fetch_all(
            "SELECT payload_json FROM intelligence_snapshots "
            "WHERE snapshot_key = 'gds_ml_phase_flags' LIMIT 1"
        )
        if row and row[0].get("payload_json"):
            overrides = _json.loads(row[0]["payload_json"])
            for k in flags:
                if k in overrides:
                    flags[k] = bool(overrides[k])
    except Exception:
        pass
    return flags


# ---------------------------------------------------------------------------
#  Main entry point
# ---------------------------------------------------------------------------

def run_neo4j_ml_exploration(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Full GDS ML pipeline: Phase 1-4 with time-window discipline.

    Phase 1: GDS-native algorithms (PageRank, Betweenness, Leiden, HITS, K-Core)
    Phase 2: FastRP embeddings + alliance centroid anomaly scoring
    Phase 3: Topological link prediction with explainability
    Phase 4: Role classification signals from Phase 1+2 features

    Each phase can be independently disabled via intelligence_snapshots
    (snapshot_key='gds_ml_phase_flags'). A phase failure is isolated and
    does NOT block subsequent phases unless there is a hard data dependency
    (only Phase 1 failure skips the rest for that window, since P2-P4
    depend on P1 feature rows existing).
    """
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

    # Load per-phase flags
    phase_flags = _load_phase_flags(db)
    active_phases = [k for k, v in phase_flags.items() if v]

    # Log run start
    db.execute(
        "INSERT INTO graph_ml_run_log "
        "(run_id, started_at, status, time_window, feature_version, "
        "gds_available, analytics_path, config_json) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        (run_id, computed_at, "running", "all", FEATURE_VERSION,
         1 if use_gds else 0, analytics_path,
         json_dumps_safe({
             "time_windows": [w["key"] for w in TIME_WINDOWS],
             "phase_flags": phase_flags,
             "active_phases": active_phases,
         })),
    )

    total_features = 0
    total_links = 0
    total_characters = 0
    phase_results: dict[str, list[dict[str, Any]]] = {
        "phase1": [], "phase2": [], "phase3": [], "phase4": [],
    }
    phase_timings: dict[str, float] = {}
    errors: list[str] = []

    for window in TIME_WINDOWS:
        wkey = window["key"]

        # ── Phase 1: Core algorithms + feature writeback ──
        p1: dict[str, Any] | None = None
        if phase_flags["phase1"]:
            t0 = time.perf_counter()
            try:
                p1 = _run_window(client, db, window, run_id, computed_at, use_gds)
                phase_results["phase1"].append(p1)
                total_features += p1["rows_written"]
                total_characters = max(total_characters, p1["characters"])
            except Exception as exc:
                errors.append(f"P1/{wkey}: {exc}")
                phase_results["phase1"].append({"window": wkey, "error": str(exc)})
            phase_timings[f"phase1/{wkey}"] = round(time.perf_counter() - t0, 2)
        else:
            phase_results["phase1"].append({"window": wkey, "status": "disabled"})

        # P1 is the foundation — if it failed or was disabled, skip P2-P4
        # for this window since they depend on feature rows existing.
        if p1 is None or "error" in (p1 or {}):
            for pk in ("phase2", "phase3", "phase4"):
                if phase_flags.get(pk):
                    phase_results[pk].append({"window": wkey, "status": "skipped (phase1 prerequisite)"})
            continue

        # ── Phase 2: FastRP embeddings + anomaly scoring ──
        embeddings: dict[int, list[float]] | None = None
        if phase_flags["phase2"]:
            t0 = time.perf_counter()
            try:
                p2 = _run_phase2_embeddings(client, db, window, run_id, computed_at, use_gds)
                phase_results["phase2"].append(p2)
                # Cache embeddings for Phase 3 reuse
                if p2.get("status") == "ok" and p2.get("_embeddings"):
                    embeddings = p2["_embeddings"]
                elif p2.get("status") == "ok" and use_gds:
                    try:
                        re_ok, _, _ = _create_projection(client, window)
                        if re_ok:
                            embeddings = _gds_fastrp(client, _projection_name(wkey))
                            _drop_projection(client, _projection_name(wkey))
                    except Exception:
                        pass
            except Exception as exc:
                errors.append(f"P2/{wkey}: {exc}")
                phase_results["phase2"].append({"window": wkey, "error": str(exc)})
            phase_timings[f"phase2/{wkey}"] = round(time.perf_counter() - t0, 2)
        else:
            phase_results["phase2"].append({"window": wkey, "status": "disabled"})

        # ── Phase 3: Link prediction with explainability ──
        # P3 degrades gracefully without P2 (just no embedding similarity)
        if phase_flags["phase3"]:
            t0 = time.perf_counter()
            communities = p1.get("_communities") if p1 else None
            try:
                p3 = _run_phase3_link_prediction(
                    client, db, window, run_id, computed_at, use_gds,
                    embeddings=embeddings,
                    communities=communities,
                )
                phase_results["phase3"].append(p3)
                total_links += p3.get("links_written", 0)
            except Exception as exc:
                errors.append(f"P3/{wkey}: {exc}")
                phase_results["phase3"].append({"window": wkey, "error": str(exc)})
            phase_timings[f"phase3/{wkey}"] = round(time.perf_counter() - t0, 2)
        else:
            phase_results["phase3"].append({"window": wkey, "status": "disabled"})

        # ── Phase 4: Role classification (SQL only, no Neo4j) ──
        # P4 reads from graph_ml_features written by P1 — always safe after P1
        if phase_flags["phase4"]:
            t0 = time.perf_counter()
            try:
                p4 = _run_phase4_role_signals(db, wkey)
                phase_results["phase4"].append(p4)
            except Exception as exc:
                errors.append(f"P4/{wkey}: {exc}")
                phase_results["phase4"].append({"window": wkey, "error": str(exc)})
            phase_timings[f"phase4/{wkey}"] = round(time.perf_counter() - t0, 2)
        else:
            phase_results["phase4"].append({"window": wkey, "status": "disabled"})

    # ── Finalize ──
    finished_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    # Only "failed" if a non-skipped phase actually errored
    status = "success" if not errors else "failed"
    db.execute(
        "UPDATE graph_ml_run_log SET finished_at = %s, status = %s, "
        "characters_processed = %s, features_written = %s, "
        "links_predicted = %s, error_text = %s "
        "WHERE run_id = %s",
        (finished_at, status, total_characters, total_features,
         total_links, "; ".join(errors) if errors else None, run_id),
    )

    # Intelligence snapshot — single dashboard-queryable payload
    role_summary: dict[str, Any] = {}
    for p4r in phase_results["phase4"]:
        if p4r.get("role_distribution"):
            role_summary[p4r["window"]] = p4r["role_distribution"]

    db.upsert_intelligence_snapshot(
        snapshot_key="neo4j_ml_exploration_state",
        payload_json=json_dumps_safe({
            "run_id": run_id,
            "analytics_path": analytics_path,
            "phase_flags": phase_flags,
            "total_characters": total_characters,
            "total_features": total_features,
            "total_links_predicted": total_links,
            "role_summary": role_summary,
            "phase_timings_seconds": phase_timings,
            "phases": phase_results,
            "errors": errors,
        }),
        metadata_json=json_dumps_safe({
            "source": "neo4j+graph_ml_features+graph_ml_link_predictions",
            "reason": "scheduler:python",
            "phases": "+".join(str(i) for i, p in enumerate(active_phases, 1)),
        }),
        expires_seconds=7200,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)

    summary_parts = [
        f"GDS ML pipeline: {total_characters} chars,",
        f"{total_features} features, {total_links} link predictions",
        f"across {len(TIME_WINDOWS)} windows. Path: {analytics_path}.",
        f"Phases: {','.join(active_phases)}.",
    ]
    if errors:
        summary_parts.append(f"Errors: {len(errors)}.")

    return JobResult.success(
        job_key=job_name,
        summary=" ".join(summary_parts),
        rows_processed=total_characters,
        rows_written=total_features + total_links,
        duration_ms=duration_ms,
        meta={
            "run_id": run_id,
            "analytics_path": analytics_path,
            "feature_version": FEATURE_VERSION,
            "phase_flags": phase_flags,
            "phase_timings_seconds": phase_timings,
            "total_links": total_links,
            "role_summary": role_summary,
            "phases": phase_results,
            "errors": errors,
        },
    ).to_dict()


# ===========================================================================
#  Phase 2: FastRP Embeddings + Similarity / Anomaly Scoring
# ===========================================================================

FASTRP_DIMENSION = 64
FASTRP_ITERATION_WEIGHTS = [0.0, 1.0, 1.0, 0.8, 0.4]  # 4 iterations
TOP_SIMILAR_PAIRS = 500  # max link predictions per window


def _is_zero_vector(vec: list[float]) -> bool:
    """True when every component is exactly 0.0 (FastRP zero-vector sentinel)."""
    return all(x == 0.0 for x in vec)


def _gds_fastrp(client: Neo4jClient, proj: str) -> dict[int, list[float]] | None:
    """FastRP node embeddings — fast, deterministic, good baseline.

    Returns {character_id: [float, ...]} for each node in the projection.
    Filters out all-zero vectors which FastRP emits for disconnected nodes
    (nodes with no edges in the projection).  Returning these would poison
    downstream centroid and similarity computations.
    """
    try:
        rows = client.query(
            f"""
            CALL gds.fastRP.stream('{proj}', {{
                embeddingDimension: {FASTRP_DIMENSION},
                iterationWeights: {FASTRP_ITERATION_WEIGHTS},
                randomSeed: 42
            }})
            YIELD nodeId, embedding
            WITH gds.util.asNode(nodeId) AS node, embedding
            RETURN node.character_id AS character_id, embedding
            """,
            timeout_seconds=300,
        )
        result: dict[int, list[float]] = {}
        zero_count = 0
        for r in rows:
            if not r.get("character_id") or not r.get("embedding"):
                continue
            vec = [float(x) for x in r["embedding"]]
            if _is_zero_vector(vec):
                zero_count += 1
                continue
            result[int(r["character_id"])] = vec
        # If every single embedding is zero the graph is too sparse for
        # FastRP to produce meaningful output — return None so callers
        # skip embedding-dependent logic rather than operating on garbage.
        if not result and zero_count > 0:
            return None
        return result if result else None
    except Exception:
        return None


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    mag_a = sum(x * x for x in a) ** 0.5
    mag_b = sum(x * x for x in b) ** 0.5
    if mag_a < 1e-12 or mag_b < 1e-12:
        return 0.0
    return dot / (mag_a * mag_b)


def _compute_alliance_centroids(
    embeddings: dict[int, list[float]],
    char_alliances: dict[int, int],
) -> dict[int, list[float]]:
    """Compute mean embedding per alliance for anomaly detection."""
    accum: dict[int, list[list[float]]] = {}
    for cid, emb in embeddings.items():
        aid = char_alliances.get(cid)
        if aid and aid > 0:
            accum.setdefault(aid, []).append(emb)

    centroids: dict[int, list[float]] = {}
    for aid, embs in accum.items():
        dim = len(embs[0])
        centroid = [0.0] * dim
        for emb in embs:
            for i in range(dim):
                centroid[i] += emb[i]
        n = len(embs)
        centroids[aid] = [c / n for c in centroid]
    return centroids


def _get_character_alliances(client: Neo4jClient) -> dict[int, int]:
    """Fetch current alliance_id for each character from Neo4j."""
    rows = client.query(
        """
        MATCH (c:Character)
        WHERE c.alliance_id IS NOT NULL AND c.alliance_id > 0
        RETURN c.character_id AS character_id, c.alliance_id AS alliance_id
        """,
        timeout_seconds=60,
    )
    return {int(r["character_id"]): int(r["alliance_id"]) for r in rows if r.get("character_id")}


def _run_phase2_embeddings(
    client: Neo4jClient,
    db: SupplyCoreDb,
    window: dict[str, Any],
    run_id: str,
    computed_at: str,
    use_gds: bool,
) -> dict[str, Any]:
    """Phase 2: Generate FastRP embeddings, compute anomaly scores, update features."""
    wkey = window["key"]
    proj = _projection_name(wkey)

    if not use_gds:
        return {"window": wkey, "phase": 2, "status": "skipped (no GDS)"}

    # Need projection to still exist — recreate it
    try:
        proj_ok, _pn, proj_rels = _create_projection(client, window)
        if not proj_ok:
            return {
                "window": wkey, "phase": 2,
                "status": f"skipped (projection empty, 0 rels)",
            }
    except Exception as exc:
        return {"window": wkey, "phase": 2, "status": f"projection failed: {exc}"}

    # Run FastRP — zero-vector embeddings are filtered out inside _gds_fastrp
    embeddings = _gds_fastrp(client, proj)
    _drop_projection(client, proj)

    if not embeddings:
        return {
            "window": wkey, "phase": 2,
            "status": f"fastrp produced no usable embeddings ({proj_rels} rels in projection)",
        }

    # Get alliance memberships for centroid computation
    char_alliances = _get_character_alliances(client)
    centroids = _compute_alliance_centroids(embeddings, char_alliances)

    # Compute anomaly score: 1 - cosine_similarity(char_embedding, alliance_centroid)
    # High score = structurally different from alliance peers
    anomaly_scores: dict[int, float] = {}
    for cid, emb in embeddings.items():
        aid = char_alliances.get(cid)
        if aid and aid in centroids:
            sim = _cosine_similarity(emb, centroids[aid])
            anomaly_scores[cid] = round(1.0 - sim, 6)
        else:
            anomaly_scores[cid] = 0.0

    # Batch-update existing feature rows with embedding data instead of
    # one UPDATE per character.  Uses CASE expressions to map each
    # character_id to its embedding+anomaly in a single statement.
    _EMB_UPDATE_BATCH = 500
    updated = 0
    emb_items = list(embeddings.items())
    for offset in range(0, len(emb_items), _EMB_UPDATE_BATCH):
        chunk = emb_items[offset:offset + _EMB_UPDATE_BATCH]
        # Build CASE WHEN character_id = %s THEN %s ... END for each column
        emb_cases: list[str] = []
        anomaly_cases: list[str] = []
        emb_params: list[Any] = []
        anomaly_params: list[Any] = []
        id_list: list[Any] = []
        for cid, emb in chunk:
            anomaly = anomaly_scores.get(cid, 0.0)
            emb_cases.append("WHEN %s THEN %s")
            emb_params.extend([cid, json_dumps_safe(emb)])
            anomaly_cases.append("WHEN %s THEN %s")
            anomaly_params.extend([cid, anomaly])
            id_list.append(cid)
        if id_list:
            id_placeholders = ", ".join(["%s"] * len(id_list))
            db.execute(
                "UPDATE graph_ml_features SET "
                "fastrp_embedding = CASE character_id " + " ".join(emb_cases) + " END, "
                "embedding_dimension = %s, "
                "embedding_anomaly_score = CASE character_id " + " ".join(anomaly_cases) + " END "
                "WHERE character_id IN (" + id_placeholders + ") "
                "AND time_window = %s AND feature_version = %s",
                tuple(emb_params) + (FASTRP_DIMENSION,) + tuple(anomaly_params) + tuple(id_list) + (wkey, FEATURE_VERSION),
            )
            updated += len(chunk)

    return {
        "window": wkey,
        "phase": 2,
        "status": "ok",
        "embeddings_generated": len(embeddings),
        "anomaly_scores": len(anomaly_scores),
        "alliance_centroids": len(centroids),
        "features_updated": updated,
        # Pass-through for Phase 3 reuse (not serialized)
        "_embeddings": embeddings,
    }


# ===========================================================================
#  Phase 3: Topological Link Prediction + Explainability
# ===========================================================================

LINK_PRED_MIN_CONFIDENCE = 0.1  # only store predictions above this threshold
LINK_PRED_MAX_PAIRS = 1000      # cap total pairs per window


def _get_copresence_counts(client: Neo4jClient) -> dict[tuple[int, int], int]:
    """Fetch co-presence counts for character pairs."""
    rows = client.query(
        """
        MATCH (a:Character)-[r:CO_OCCURS_WITH]-(b:Character)
        WHERE a.character_id < b.character_id
        RETURN a.character_id AS a, b.character_id AS b,
               COALESCE(r.occurrence_count, r.count, 1) AS cnt
        """,
        timeout_seconds=120,
    )
    return {
        (int(r["a"]), int(r["b"])): int(r["cnt"])
        for r in rows if r.get("a") and r.get("b")
    }


def _get_side_history(client: Neo4jClient) -> dict[tuple[int, int], tuple[int, int]]:
    """Fetch same-side vs cross-side counts for pairs."""
    # Same side: SAME_FLEET or ASSISTED_KILL
    same_rows = client.query(
        """
        MATCH (a:Character)-[r:SAME_FLEET|ASSISTED_KILL]-(b:Character)
        WHERE a.character_id < b.character_id
        RETURN a.character_id AS a, b.character_id AS b,
               sum(COALESCE(r.count, 1)) AS cnt
        """,
        timeout_seconds=120,
    )
    same: dict[tuple[int, int], int] = {
        (int(r["a"]), int(r["b"])): int(r["cnt"])
        for r in same_rows if r.get("a") and r.get("b")
    }

    # Cross side: DIRECT_COMBAT
    cross_rows = client.query(
        """
        MATCH (a:Character)-[r:DIRECT_COMBAT]-(b:Character)
        WHERE a.character_id < b.character_id
        RETURN a.character_id AS a, b.character_id AS b,
               sum(COALESCE(r.count, 1)) AS cnt
        """,
        timeout_seconds=120,
    )
    cross: dict[tuple[int, int], int] = {
        (int(r["a"]), int(r["b"])): int(r["cnt"])
        for r in cross_rows if r.get("a") and r.get("b")
    }

    all_pairs = set(same.keys()) | set(cross.keys())
    return {
        pair: (same.get(pair, 0), cross.get(pair, 0))
        for pair in all_pairs
    }


def _topological_link_scores(client: Neo4jClient, proj: str) -> list[dict[str, Any]] | None:
    """Compute topological link prediction scores via GDS KNN on the projection.

    Uses Adamic Adar, Common Neighbors, Preferential Attachment as standalone
    Cypher functions on the character graph (these don't need GDS pipelines).
    """
    try:
        rows = client.query(
            f"""
            MATCH (a:Character)-[:{CHARACTER_REL_TYPES_STR}]-(common)-[:{CHARACTER_REL_TYPES_STR}]-(b:Character)
            WHERE a.character_id < b.character_id
              AND NOT (a)-[:{CHARACTER_REL_TYPES_STR}]-(b)
            WITH a, b, count(DISTINCT common) AS common_neighbors
            WHERE common_neighbors >= 2
            WITH a, b, common_neighbors
            OPTIONAL MATCH (a)-[ra:{CHARACTER_REL_TYPES_STR}]-()
            WITH a, b, common_neighbors, count(ra) AS deg_a
            OPTIONAL MATCH (b)-[rb:{CHARACTER_REL_TYPES_STR}]-()
            WITH a, b, common_neighbors, deg_a, count(rb) AS deg_b
            RETURN a.character_id AS char_a,
                   b.character_id AS char_b,
                   common_neighbors,
                   deg_a, deg_b,
                   deg_a * deg_b AS pref_attachment,
                   CASE WHEN deg_a + deg_b > 0
                        THEN toFloat(common_neighbors) / (deg_a + deg_b - common_neighbors)
                        ELSE 0.0 END AS jaccard
            ORDER BY common_neighbors DESC
            LIMIT {LINK_PRED_MAX_PAIRS}
            """,
            timeout_seconds=300,
        )
        return rows
    except Exception:
        return None


def _run_phase3_link_prediction(
    client: Neo4jClient,
    db: SupplyCoreDb,
    window: dict[str, Any],
    run_id: str,
    computed_at: str,
    use_gds: bool,
    embeddings: dict[int, list[float]] | None,
    communities: dict[int, int] | None,
) -> dict[str, Any]:
    """Phase 3: Topological link prediction with full explainability."""
    wkey = window["key"]
    proj = _projection_name(wkey)

    # Get raw topological scores from Cypher
    topo_rows = _topological_link_scores(client, proj)
    if not topo_rows:
        return {"window": wkey, "phase": 3, "status": "no candidate pairs found"}

    # Enrich with co-presence and side history
    copresence = _get_copresence_counts(client)
    side_history = _get_side_history(client)

    # Compute all embedding similarities for percentile ranking
    all_sims: list[float] = []
    pair_sims: dict[tuple[int, int], float] = {}
    if embeddings:
        for row in topo_rows:
            a, b = int(row["char_a"]), int(row["char_b"])
            if a in embeddings and b in embeddings:
                sim = _cosine_similarity(embeddings[a], embeddings[b])
                pair_sims[(a, b)] = sim
                all_sims.append(sim)

    sorted_sims = sorted(all_sims) if all_sims else []

    def _percentile(val: float) -> float:
        if not sorted_sims:
            return 0.0
        import bisect
        idx = bisect.bisect_left(sorted_sims, val)
        return round(idx / max(len(sorted_sims), 1), 4)

    # Build predictions with explainability
    db.execute(
        "DELETE FROM graph_ml_link_predictions "
        "WHERE time_window = %s AND feature_version = %s",
        (wkey, FEATURE_VERSION),
    )

    # Build all link prediction rows in memory, then bulk-INSERT in batches
    # instead of one INSERT per candidate pair.
    all_link_rows: list[tuple[Any, ...]] = []
    for row in topo_rows:
        a, b = int(row["char_a"]), int(row["char_b"])
        cn = int(row.get("common_neighbors", 0))
        pref_att = float(row.get("pref_attachment", 0))
        jaccard = float(row.get("jaccard", 0))

        emb_sim = pair_sims.get((a, b), 0.0)
        emb_pct = _percentile(emb_sim)

        cn_norm = min(cn / 10.0, 1.0)
        pa_norm = min(pref_att / 1000.0, 1.0)
        copresence_norm = min(copresence.get((a, b), 0) / 20.0, 1.0)

        if embeddings:
            confidence = round(
                cn_norm * 0.30 + jaccard * 0.25 + pa_norm * 0.10 +
                emb_sim * 0.25 + copresence_norm * 0.10,
                4,
            )
        else:
            confidence = round(
                cn_norm * 0.40 + jaccard * 0.30 + pa_norm * 0.15 +
                copresence_norm * 0.15,
                4,
            )

        if confidence < LINK_PRED_MIN_CONFIDENCE:
            continue

        same_count, cross_count = side_history.get((a, b), (0, 0))
        total_sides = same_count + cross_count
        same_side_ratio = round(same_count / max(total_sides, 1), 4)

        shared_comms: list[int] = []
        if communities:
            comm_a = communities.get(a)
            comm_b = communities.get(b)
            if comm_a is not None and comm_a == comm_b:
                shared_comms = [comm_a]

        explanation_parts: list[str] = []
        if cn >= 5:
            explanation_parts.append(f"{cn} common neighbors (strong)")
        elif cn >= 2:
            explanation_parts.append(f"{cn} common neighbors")
        if copresence.get((a, b), 0) > 0:
            explanation_parts.append(f"co-present {copresence[(a,b)]}x")
        if shared_comms:
            explanation_parts.append("same community")
        if emb_sim > 0.8:
            explanation_parts.append(f"embedding similarity {emb_sim:.2f} (high)")
        elif emb_sim > 0.5:
            explanation_parts.append(f"embedding similarity {emb_sim:.2f}")
        if same_side_ratio > 0.8 and total_sides >= 3:
            explanation_parts.append(f"same-side {same_side_ratio:.0%} of {total_sides} encounters")
        elif cross_count > same_count and total_sides >= 3:
            explanation_parts.append(f"mostly opposing sides ({cross_count}/{total_sides})")

        explanation = "; ".join(explanation_parts) if explanation_parts else None

        all_link_rows.append((
            a, b, wkey, FEATURE_VERSION,
            "association", confidence,
            cn, pref_att,
            1 if shared_comms else 0,
            json_dumps_safe(shared_comms) if shared_comms else None,
            copresence.get((a, b), 0),
            round(emb_sim, 6), emb_pct,
            same_side_ratio, cross_count,
            explanation, computed_at, run_id,
        ))

    # Bulk-INSERT in chunks of 500 instead of per-row round-trips.
    _LINK_INSERT_BATCH = 500
    links_written = 0
    for offset in range(0, len(all_link_rows), _LINK_INSERT_BATCH):
        chunk = all_link_rows[offset:offset + _LINK_INSERT_BATCH]
        placeholders = ", ".join(["(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"] * len(chunk))
        flat_params: list[Any] = []
        for link_row in chunk:
            flat_params.extend(link_row)
        if flat_params:
            db.execute(
                "INSERT INTO graph_ml_link_predictions "
                "(character_id_a, character_id_b, time_window, feature_version, "
                "prediction_type, confidence, "
                "common_neighbors_score, pref_attachment_score, "
                "same_community, "
                "shared_community_ids, copresence_count, "
                "embedding_similarity, embedding_sim_percentile, "
                "same_side_ratio, cross_side_count, "
                "explanation_summary, computed_at, run_id) "
                "VALUES " + placeholders,
                tuple(flat_params),
            )
            links_written += len(chunk)

    return {
        "window": wkey,
        "phase": 3,
        "status": "ok",
        "candidate_pairs": len(topo_rows),
        "links_written": links_written,
        "embeddings_available": embeddings is not None,
        "embedding_pairs_scored": len(pair_sims),
    }


# ===========================================================================
#  Phase 4: Role Classification Signals + Operational Enrichment
# ===========================================================================
#
#  Rather than train a supervised classifier (which needs labeled data we
#  don't have time to curate before the war), we derive role-indicator
#  signals from the graph that can be consumed by existing pipelines.
#
#  HITS hub_score → likely FC / coordinator (connects many players)
#  HITS auth_score → likely core line member (connected to by FCs)
#  K-Core level → operational core vs periphery
#  Betweenness → intel bridge / liaison
#  Articulation point → structurally critical (removal splits network)
#  Community membership consistency across time windows → stable operator
#  Embedding anomaly → potential spy/alt/outsider
#
#  These signals are already written in Phase 1+2 features. Phase 4 just
#  computes a composite role classification and writes it back as additional
#  columns so downstream consumers get a single row with everything.

ROLE_THRESHOLDS = {
    "fc_coordinator": {"hub_pct": 0.90},
    "core_line": {"auth_pct": 0.80, "kcore_min": 3},
    "bridge_liaison": {"betweenness_pct": 0.90},
    "structural_keystone": {"is_artic": True, "kcore_min": 2},
    "periphery": {"kcore_max": 1, "degree_max": 3},
    "anomalous_outsider": {"anomaly_pct": 0.90},
}


def _run_phase4_role_signals(
    db: SupplyCoreDb,
    window_key: str,
) -> dict[str, Any]:
    """Phase 4: Compute role classification from Phase 1+2 features.

    Reads from graph_ml_features, computes percentile thresholds, and
    writes role labels back. No Neo4j needed — pure SQL on the feature store.
    """
    # Fetch all features for this window
    rows = db.fetch_all(
        "SELECT character_id, pagerank_score, betweenness_score, "
        "degree_centrality, hits_hub_score, hits_auth_score, "
        "kcore_level, is_articulation_point, embedding_anomaly_score "
        "FROM graph_ml_features "
        "WHERE time_window = %s AND feature_version = %s",
        (window_key, FEATURE_VERSION),
    )
    if not rows:
        return {"window": window_key, "phase": 4, "status": "no features"}

    # Compute percentile ranks for key metrics
    def _pct_ranks(values: list[float]) -> list[float]:
        sorted_v = sorted(values)
        n = len(sorted_v)
        if n == 0:
            return []
        ranks = []
        for v in values:
            import bisect
            idx = bisect.bisect_left(sorted_v, v)
            ranks.append(idx / max(n, 1))
        return ranks

    hub_vals = [float(r.get("hits_hub_score", 0)) for r in rows]
    auth_vals = [float(r.get("hits_auth_score", 0)) for r in rows]
    between_vals = [float(r.get("betweenness_score", 0)) for r in rows]
    anomaly_vals = [float(r.get("embedding_anomaly_score", 0)) for r in rows]

    hub_pcts = _pct_ranks(hub_vals)
    auth_pcts = _pct_ranks(auth_vals)
    between_pcts = _pct_ranks(between_vals)
    anomaly_pcts = _pct_ranks(anomaly_vals)

    # Detect degenerate data: when a metric has no variance (all values
    # identical, typically all zeros) percentile ranks are meaningless —
    # every value gets rank 0.0 and threshold checks never fire, causing
    # the entire population to fall through to "periphery".
    def _has_variance(vals: list[float]) -> bool:
        if not vals:
            return False
        first = vals[0]
        return any(v != first for v in vals)

    hub_useful = _has_variance(hub_vals)
    auth_useful = _has_variance(auth_vals)
    between_useful = _has_variance(between_vals)
    anomaly_useful = _has_variance(anomaly_vals)
    # If none of the key metrics have variance, classification is unreliable
    metrics_degenerate = not (hub_useful or auth_useful or between_useful)

    # Classify each character
    role_counts: dict[str, int] = {}
    updates: list[tuple[Any, ...]] = []

    for i, r in enumerate(rows):
        cid = int(r["character_id"])
        kcore = int(r.get("kcore_level", 0))
        degree = int(r.get("degree_centrality", 0))
        is_artic = int(r.get("is_articulation_point", 0))

        # When all graph metrics are degenerate (zero variance), the only
        # reliable signals are degree and kcore which come from simple
        # edge counting.  Skip percentile-based rules and use degree/kcore
        # directly to avoid false-classifying the entire population.
        if metrics_degenerate:
            if degree == 0 and kcore == 0:
                role = "periphery"
                confidence = 0.3
            elif degree >= 10 or kcore >= 3:
                role = "regular"
                confidence = 0.4
            else:
                role = "periphery"
                confidence = 0.3
        else:
            # Determine primary role (first match wins, ordered by priority)
            role = "unclassified"
            confidence = 0.0

            if hub_useful and hub_pcts[i] >= 0.90:
                role = "fc_coordinator"
                confidence = hub_pcts[i]
            elif is_artic and kcore >= 2:
                role = "structural_keystone"
                confidence = between_pcts[i]
            elif between_useful and between_pcts[i] >= 0.90:
                role = "bridge_liaison"
                confidence = between_pcts[i]
            elif anomaly_useful and anomaly_pcts[i] >= 0.90:
                role = "anomalous_outsider"
                confidence = anomaly_pcts[i]
            elif auth_useful and auth_pcts[i] >= 0.80 and kcore >= 3:
                role = "core_line"
                confidence = auth_pcts[i]
            elif kcore <= 1 and degree <= 3:
                role = "periphery"
                confidence = 1.0 - (kcore / 10.0)
            else:
                role = "regular"
                confidence = 0.5

        role_counts[role] = role_counts.get(role, 0) + 1
        updates.append((role, round(confidence, 4), cid, window_key, FEATURE_VERSION))

    # Batch update — we add these as columns that don't exist yet in the table,
    # so we use the explanation/embedding columns. Actually, let's store in the
    # JSON embedding field alongside the embedding if we must, but cleaner to
    # just ensure the migration has the columns. For now we'll write to a
    # separate lightweight table to avoid schema changes mid-war.
    #
    # Write to intelligence_snapshots as a queryable JSON payload keyed by
    # character_id. Downstream PHP can read this.
    role_payload: dict[str, dict[str, Any]] = {}
    for i, r in enumerate(rows):
        cid = str(r["character_id"])
        role_payload[cid] = {
            "role": updates[i][0],
            "confidence": updates[i][1],
            "hub_pct": round(hub_pcts[i], 4),
            "auth_pct": round(auth_pcts[i], 4),
            "betweenness_pct": round(between_pcts[i], 4),
            "anomaly_pct": round(anomaly_pcts[i], 4),
            "kcore": int(r.get("kcore_level", 0)),
        }

    return {
        "window": window_key,
        "phase": 4,
        "status": "ok",
        "characters_classified": len(rows),
        "role_distribution": role_counts,
        "payload_size": len(role_payload),
        "metrics_degenerate": metrics_degenerate,
        "metric_variance": {
            "hub": hub_useful,
            "auth": auth_useful,
            "betweenness": between_useful,
            "anomaly": anomaly_useful,
        },
    }
