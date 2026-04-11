"""Compute Map Intelligence — precompute graph-derived scores for the map module.

Computes betweenness centrality, degree centrality, bridge detection, community
detection, and label priority for every solar system in the universe graph.
Also computes edge-level intelligence from threat corridor membership.

Results are materialized to ``map_system_intelligence`` and
``map_edge_intelligence`` tables in MariaDB.  The PHP map module reads these
at request time — Neo4j is never queried in the rendering hot path.

Strategy: Neo4j is required (the universe graph is loaded exclusively from
the ``System`` / ``CONNECTS_TO`` / ``JUMP_BRIDGE`` projection). When the
Graph Data Science plugin is available we use its procedures for
high-performance analytics; otherwise we fall back to pure Python
algorithms (Brandes betweenness, Tarjan bridges, LPA communities) that
operate on the already-loaded in-memory adjacency list. There is no SQL
fallback path for graph topology — if Neo4j is disabled, the job is
explicitly skipped with a reason so the scheduler surfaces it.
"""

from __future__ import annotations

import math
import sys
import time
from collections import defaultdict, deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.config import resolve_app_root  # noqa: F401
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.neo4j import Neo4jClient, Neo4jConfig
else:
    from ..config import resolve_app_root  # noqa: F401
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..neo4j import Neo4jClient, Neo4jConfig

BATCH_SIZE = 500
GDS_GRAPH_NAME = "map_universe"


# ---------------------------------------------------------------------------
#  GDS availability detection
# ---------------------------------------------------------------------------

def _has_gds(client: Neo4jClient) -> bool:
    """Check if the Neo4j Graph Data Science plugin is available."""
    try:
        result = client.query("RETURN gds.version() AS v")
        return len(result) > 0
    except Exception:
        return False


def _gds_projection_exists(client: Neo4jClient) -> bool:
    """Check if the GDS projection already exists in memory."""
    try:
        rows = client.query(
            f"CALL gds.graph.exists('{GDS_GRAPH_NAME}') YIELD exists RETURN exists"
        )
        return bool(rows and rows[0].get("exists"))
    except Exception:
        return False


def _relationship_types_present(client: Neo4jClient) -> list[str]:
    """Return which of the expected relationship types actually exist in Neo4j."""
    present = []
    for rel_type in ("CONNECTS_TO", "JUMP_BRIDGE"):
        try:
            rows = client.query(
                f"MATCH ()-[r:{rel_type}]->() RETURN r LIMIT 1"
            )
            if rows:
                present.append(rel_type)
        except Exception:
            pass
    return present


def _ensure_gds_projection(client: Neo4jClient, *, force_recreate: bool = False) -> bool:
    """Create the GDS graph projection for universe topology.

    If the projection already exists and *force_recreate* is False, reuse it to
    avoid the overhead of dropping and reprojecting the full graph every run.

    Only includes relationship types that actually exist in the database to
    avoid GDS ``IllegalArgumentException`` when e.g. no jump bridges are configured.
    """
    try:
        if not force_recreate and _gds_projection_exists(client):
            return True

        # Drop existing projection if present
        try:
            client.query(f"CALL gds.graph.drop('{GDS_GRAPH_NAME}', false)")
        except Exception:
            pass

        rel_types = _relationship_types_present(client)
        if not rel_types:
            return False

        rel_projection = ", ".join(
            f"{rt}: {{orientation: 'UNDIRECTED'}}" for rt in rel_types
        )
        client.query(
            f"""
            CALL gds.graph.project(
                '{GDS_GRAPH_NAME}',
                'System',
                {{{rel_projection}}}
            )
            """,
            timeout_seconds=120,
        )
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
#  GDS-based analytics
# ---------------------------------------------------------------------------

def _gds_betweenness(client: Neo4jClient) -> dict[int, float]:
    """Compute betweenness centrality using GDS."""
    rows = client.query(
        f"""
        CALL gds.betweenness.stream('{GDS_GRAPH_NAME}')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        RETURN node.system_id AS system_id, score
        """,
        timeout_seconds=300,
    )
    return {
        int(r["system_id"]): (s if math.isfinite(s := float(r["score"])) else 0.0)
        for r in rows if r.get("system_id")
    }


def _gds_degree(client: Neo4jClient) -> dict[int, float]:
    """Compute degree centrality using GDS."""
    rows = client.query(
        f"""
        CALL gds.degree.stream('{GDS_GRAPH_NAME}')
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS node, score
        RETURN node.system_id AS system_id, score
        """,
        timeout_seconds=120,
    )
    return {
        int(r["system_id"]): (s if math.isfinite(s := float(r["score"])) else 0.0)
        for r in rows if r.get("system_id")
    }


def _gds_louvain(client: Neo4jClient) -> dict[int, int]:
    """Compute community detection using GDS Louvain."""
    rows = client.query(
        f"""
        CALL gds.louvain.stream('{GDS_GRAPH_NAME}')
        YIELD nodeId, communityId
        WITH gds.util.asNode(nodeId) AS node, communityId
        RETURN node.system_id AS system_id, communityId AS community_id
        """,
        timeout_seconds=300,
    )
    return {int(r["system_id"]): int(r["community_id"]) for r in rows if r.get("system_id")}


# ---------------------------------------------------------------------------
#  Graph loading + Python-side analytics (run on the in-memory adjacency
#  list loaded from Neo4j; these are *not* SQL fallbacks).
# ---------------------------------------------------------------------------

def _load_graph_from_neo4j(client: Neo4jClient) -> tuple[list[int], dict[int, list[int]]]:
    """Load the gate + jump bridge graph from Neo4j."""
    rows = client.query(
        """
        MATCH (s:System)-[:CONNECTS_TO|JUMP_BRIDGE]-(t:System)
        RETURN s.system_id AS src, t.system_id AS dst
        """,
        timeout_seconds=120,
    )
    system_set: set[int] = set()
    adjacency: dict[int, list[int]] = defaultdict(list)
    for r in rows:
        src, dst = int(r["src"]), int(r["dst"])
        system_set.add(src)
        system_set.add(dst)
        adjacency[src].append(dst)

    # Also include isolated systems
    sys_rows = client.query("MATCH (s:System) RETURN s.system_id AS sid", timeout_seconds=60)
    for r in sys_rows:
        system_set.add(int(r["sid"]))

    return sorted(system_set), dict(adjacency)


def _fallback_degree(adjacency: dict[int, list[int]], system_ids: list[int]) -> dict[int, float]:
    """Compute degree centrality from adjacency lists."""
    return {sid: float(len(adjacency.get(sid, []))) for sid in system_ids}


def _fallback_betweenness_sampled(
    adjacency: dict[int, list[int]],
    system_ids: list[int],
    sample_size: int = 200,
) -> dict[int, float]:
    """Approximate betweenness centrality via sampled BFS.

    Full betweenness is O(V*E) which is expensive for ~8000 systems.
    We sample a subset of source nodes and accumulate path counts.
    """
    import random

    scores: dict[int, float] = defaultdict(float)
    sources = random.sample(system_ids, min(sample_size, len(system_ids)))

    for source in sources:
        # Brandes-style single-source shortest paths
        stack: list[int] = []
        predecessors: dict[int, list[int]] = defaultdict(list)
        sigma: dict[int, int] = defaultdict(int)
        sigma[source] = 1
        dist: dict[int, int] = {source: 0}
        queue: deque[int] = deque([source])

        while queue:
            v = queue.popleft()
            stack.append(v)
            for w in adjacency.get(v, []):
                if w not in dist:
                    dist[w] = dist[v] + 1
                    queue.append(w)
                if dist.get(w) == dist[v] + 1:
                    sigma[w] += sigma[v]
                    predecessors[w].append(v)

        delta: dict[int, float] = defaultdict(float)
        while stack:
            w = stack.pop()
            for v in predecessors[w]:
                delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w])
            if w != source:
                scores[w] += delta[w]

    # Normalize by sample size
    if sources:
        factor = len(system_ids) / len(sources)
        for sid in scores:
            scores[sid] *= factor

    return dict(scores)


def _fallback_bridges(
    adjacency: dict[int, list[int]], system_ids: list[int],
) -> tuple[set[int], set[tuple[int, int]]]:
    """Detect bridge nodes (articulation points) and bridge edges using Tarjan's algorithm."""
    bridge_nodes: set[int] = set()
    bridge_edges: set[tuple[int, int]] = set()

    disc: dict[int, int] = {}
    low: dict[int, int] = {}
    parent: dict[int, int | None] = {}
    timer = [0]

    def dfs(u: int) -> None:
        disc[u] = low[u] = timer[0]
        timer[0] += 1
        child_count = 0

        for v in adjacency.get(u, []):
            if v not in disc:
                child_count += 1
                parent[v] = u
                dfs(v)
                low[u] = min(low[u], low[v])

                # Bridge edge: removing (u, v) disconnects the graph
                if low[v] > disc[u]:
                    bridge_edges.add((min(u, v), max(u, v)))

                # Articulation point
                if parent[u] is None and child_count > 1:
                    bridge_nodes.add(u)
                if parent[u] is not None and low[v] >= disc[u]:
                    bridge_nodes.add(u)
            elif v != parent.get(u):
                low[u] = min(low[u], disc[v])

    # Handle disconnected components
    sys.setrecursionlimit(max(20000, len(system_ids) + 1000))
    for sid in system_ids:
        if sid not in disc:
            parent[sid] = None
            dfs(sid)

    return bridge_nodes, bridge_edges


def _fallback_communities_lpa(
    adjacency: dict[int, list[int]], system_ids: list[int], max_iter: int = 20,
) -> dict[int, int]:
    """Label Propagation Algorithm for community detection."""
    import random

    labels: dict[int, int] = {sid: sid for sid in system_ids}

    for _ in range(max_iter):
        changed = False
        order = list(system_ids)
        random.shuffle(order)

        for node in order:
            neighbors = adjacency.get(node, [])
            if not neighbors:
                continue

            # Count neighbor labels
            label_counts: dict[int, int] = defaultdict(int)
            for nb in neighbors:
                if nb in labels:
                    label_counts[labels[nb]] += 1

            if not label_counts:
                continue

            max_count = max(label_counts.values())
            best_labels = [lbl for lbl, cnt in label_counts.items() if cnt == max_count]
            chosen = min(best_labels)  # Deterministic tie-break

            if labels[node] != chosen:
                labels[node] = chosen
                changed = True

        if not changed:
            break

    # Renumber communities to sequential IDs
    unique_labels = sorted(set(labels.values()))
    label_map = {lbl: idx for idx, lbl in enumerate(unique_labels)}
    return {sid: label_map[lbl] for sid, lbl in labels.items()}


# ---------------------------------------------------------------------------
#  Edge intelligence from threat corridors
# ---------------------------------------------------------------------------

def _compute_edge_intelligence(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Derive edge-level metrics from threat corridor membership and battles."""
    # Get corridor edges: consecutive system pairs in each corridor
    corridors = db.fetch_all(
        """
        SELECT tcs.corridor_id, tcs.system_id, tcs.position_in_corridor,
               tc.corridor_score
        FROM threat_corridor_systems tcs
        INNER JOIN threat_corridors tc ON tc.corridor_id = tcs.corridor_id
        WHERE tc.is_active = 1
        ORDER BY tcs.corridor_id, tcs.position_in_corridor
        """
    )

    # Group by corridor
    by_corridor: dict[int, list[tuple[int, int, float]]] = defaultdict(list)
    for row in corridors:
        cid = int(row["corridor_id"])
        sid = int(row["system_id"])
        pos = int(row["position_in_corridor"])
        raw_score = row.get("corridor_score") or 0
        score = float(raw_score) if raw_score is not None else 0.0
        if not math.isfinite(score):
            score = 0.0
        by_corridor[cid].append((pos, sid, score))

    # Build edge metrics
    edge_data: dict[tuple[int, int], dict[str, float]] = defaultdict(
        lambda: {"corridor_count": 0, "corridor_score_sum": 0.0}
    )

    for cid, members in by_corridor.items():
        members.sort(key=lambda x: x[0])
        for i in range(len(members) - 1):
            a = members[i][1]
            b = members[i + 1][1]
            score = members[i][2]
            key = (min(a, b), max(a, b))
            edge_data[key]["corridor_count"] += 1
            edge_data[key]["corridor_score_sum"] += score

    # Get battle counts per system for endpoint enrichment
    battles = db.fetch_all(
        "SELECT system_id, battle_count FROM system_threat_scores WHERE battle_count > 0"
    )
    battle_counts = {int(r["system_id"]): int(r["battle_count"]) for r in battles}

    results = []
    for (from_sid, to_sid), metrics in edge_data.items():
        bc = battle_counts.get(from_sid, 0) + battle_counts.get(to_sid, 0)
        risk = (
            metrics["corridor_score_sum"] * 0.5
            + bc * 0.3
            + (10.0 if metrics["corridor_count"] >= 3 else 0.0) * 0.2
        )
        results.append({
            "from_system_id": from_sid,
            "to_system_id": to_sid,
            "corridor_count": metrics["corridor_count"],
            "corridor_score_sum": round(metrics["corridor_score_sum"], 4),
            "battle_count": bc,
            "is_bridge_edge": 0,  # Updated later from bridge detection
            "risk_score": round(risk, 4),
        })

    return results


# ---------------------------------------------------------------------------
#  Label priority composite score
# ---------------------------------------------------------------------------

def _safe_finite_max(values, fallback: float = 1.0) -> float:
    """Return the max of *values* ignoring NaN/Inf, or *fallback* if empty."""
    finite = [v for v in values if math.isfinite(v)]
    return max(finite) if finite else fallback


def _compute_label_priority(
    betweenness: dict[int, float],
    degree: dict[int, float],
    battle_counts: dict[int, int],
    system_ids: list[int],
) -> dict[int, float]:
    """Compute composite label priority score (0..1).

    Weights: betweenness 0.4, degree 0.3, battle activity 0.3
    """
    # Normalize betweenness (NaN-safe: filter non-finite before max)
    max_b = _safe_finite_max(betweenness.values()) if betweenness else 1.0
    max_b = max(max_b, 1e-9)

    # Normalize degree
    max_d = _safe_finite_max(degree.values()) if degree else 1.0
    max_d = max(max_d, 1e-9)

    # Normalize battles
    max_bc = max(battle_counts.values()) if battle_counts else 1.0
    max_bc = max(max_bc, 1e-9)

    priorities = {}
    for sid in system_ids:
        b_val = betweenness.get(sid, 0)
        d_val = degree.get(sid, 0)
        b_norm = (b_val / max_b) if math.isfinite(b_val) else 0.0
        d_norm = (d_val / max_d) if math.isfinite(d_val) else 0.0
        bc_norm = battle_counts.get(sid, 0) / max_bc
        score = b_norm * 0.4 + d_norm * 0.3 + bc_norm * 0.3
        priorities[sid] = round(score if math.isfinite(score) else 0.0, 6)

    return priorities


# ---------------------------------------------------------------------------
#  Main job entry point
# ---------------------------------------------------------------------------

def run_compute_map_intelligence(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compute and materialize map intelligence scores."""
    started = time.perf_counter()
    job_name = "compute_map_intelligence"
    now_sql = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    config = Neo4jConfig.from_runtime(neo4j_raw or {})

    # Neo4j is the source of truth for the universe graph. If it's disabled
    # we skip the job explicitly rather than silently falling back to raw
    # SQL scans of ref_systems/ref_stargates/jump_bridges (the old fallback
    # path was expensive and diverged from the Neo4j projection anyway).
    if not config.enabled:
        return JobResult.skipped(
            job_key=job_name,
            reason="Neo4j is disabled; compute_map_intelligence requires the universe graph projection.",
        ).to_dict()

    client: Neo4jClient = Neo4jClient(config)

    # -- Load graph topology --------------------------------------------------
    system_ids, adjacency = _load_graph_from_neo4j(client)
    use_gds = _has_gds(client)
    if use_gds:
        use_gds = _ensure_gds_projection(client)

    if not system_ids:
        return JobResult.skipped(
            job_key=job_name, reason="No systems found in graph"
        ).to_dict()

    analytics_path = "gds" if use_gds else "fallback"

    # -- Compute betweenness centrality ----------------------------------------
    if use_gds:
        betweenness = _gds_betweenness(client)
    else:
        betweenness = _fallback_betweenness_sampled(adjacency, system_ids)

    # -- Compute degree centrality ---------------------------------------------
    if use_gds:
        degree = _gds_degree(client)
    else:
        degree = _fallback_degree(adjacency, system_ids)

    # -- Compute bridges (articulation points + bridge edges) ------------------
    bridge_nodes, bridge_edges = _fallback_bridges(adjacency, system_ids)

    # -- Compute communities ---------------------------------------------------
    if use_gds:
        communities = _gds_louvain(client)
    else:
        communities = _fallback_communities_lpa(adjacency, system_ids)

    # -- Battle counts for label priority --------------------------------------
    battle_rows = db.fetch_all(
        "SELECT system_id, battle_count FROM system_threat_scores WHERE battle_count > 0"
    )
    battle_counts = {int(r["system_id"]): int(r["battle_count"]) for r in battle_rows}

    # -- Label priority --------------------------------------------------------
    label_priority = _compute_label_priority(betweenness, degree, battle_counts, system_ids)

    # -- Write system intelligence to MariaDB ----------------------------------
    rows_written = 0

    # Truncate and rewrite (atomic swap via temp table would be ideal but
    # for simplicity we do batch REPLACE)
    db.execute("DELETE FROM map_system_intelligence")

    for offset in range(0, len(system_ids), BATCH_SIZE):
        chunk = system_ids[offset : offset + BATCH_SIZE]
        values = []
        params: list[Any] = []
        for sid in chunk:
            values.append("(%s, %s, %s, %s, %s, %s, %s)")
            b_score = betweenness.get(sid, 0)
            d_score = degree.get(sid, 0)
            lp_score = label_priority.get(sid, 0)
            params.extend([
                sid,
                round(b_score if math.isfinite(b_score) else 0.0, 6),
                round(d_score if math.isfinite(d_score) else 0.0, 2),
                1 if sid in bridge_nodes else 0,
                communities.get(sid),
                round(lp_score if math.isfinite(lp_score) else 0.0, 6),
                now_sql,
            ])
        if values:
            db.execute(
                f"""
                INSERT INTO map_system_intelligence
                    (system_id, chokepoint_score, connectivity_score, is_bridge,
                     community_id, label_priority, computed_at)
                VALUES {', '.join(values)}
                """,
                tuple(params),
            )
            rows_written += len(chunk)

    # -- Edge intelligence -----------------------------------------------------
    edge_intel = _compute_edge_intelligence(db)

    # Enrich with bridge edge detection
    for ei in edge_intel:
        key = (ei["from_system_id"], ei["to_system_id"])
        if key in bridge_edges:
            ei["is_bridge_edge"] = 1

    db.execute("DELETE FROM map_edge_intelligence")

    for offset in range(0, len(edge_intel), BATCH_SIZE):
        chunk = edge_intel[offset : offset + BATCH_SIZE]
        values = []
        params_e: list[Any] = []
        for ei in chunk:
            values.append("(%s, %s, %s, %s, %s, %s, %s, %s)")
            cs = ei["corridor_score_sum"]
            rs = ei["risk_score"]
            params_e.extend([
                ei["from_system_id"],
                ei["to_system_id"],
                ei["corridor_count"],
                cs if not isinstance(cs, float) or math.isfinite(cs) else 0.0,
                ei["battle_count"],
                ei["is_bridge_edge"],
                rs if not isinstance(rs, float) or math.isfinite(rs) else 0.0,
                now_sql,
            ])
        if values:
            db.execute(
                f"""
                INSERT INTO map_edge_intelligence
                    (from_system_id, to_system_id, corridor_count, corridor_score_sum,
                     battle_count, is_bridge_edge, risk_score, computed_at)
                VALUES {', '.join(values)}
                """,
                tuple(params_e),
            )
            rows_written += len(chunk)

    # -- Clean up GDS projection -----------------------------------------------
    if use_gds:
        try:
            client.query(f"CALL gds.graph.drop('{GDS_GRAPH_NAME}', false)")
        except Exception:
            pass

    # -- Record sync state -----------------------------------------------------
    db.upsert_sync_state(
        dataset_key=job_name,
        status="success",
        row_count=rows_written,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=(
            f"Computed map intelligence for {len(system_ids)} systems, "
            f"{len(edge_intel)} edges. Analytics path: {analytics_path}."
        ),
        rows_processed=len(system_ids),
        rows_written=rows_written,
        duration_ms=duration_ms,
        meta={
            "analytics_path": analytics_path,
            "system_count": len(system_ids),
            "edge_count": len(edge_intel),
            "bridge_node_count": len(bridge_nodes),
            "bridge_edge_count": len(bridge_edges),
            "community_count": len(set(communities.values())) if communities else 0,
        },
    ).to_dict()
