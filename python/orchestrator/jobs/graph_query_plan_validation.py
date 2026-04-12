"""Graph Query Plan Validation — PROFILE/EXPLAIN pass for top Cypher queries.

Runs the most performance-critical Cypher queries through EXPLAIN (or PROFILE
when safe) to detect plan anti-patterns: cartesian products, eager operators,
label scans without index usage, and cardinality blowups.

Results are persisted to ``graph_query_plan_diagnostics`` intelligence snapshot
so operators can review them without running the pass manually.

This job is non-destructive: EXPLAIN queries are never executed, and PROFILE
queries use read-only statements with tight LIMIT guards.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, QueryPlanMetrics


# ---------------------------------------------------------------------------
#  Query inventory: the hot queries we want to validate
# ---------------------------------------------------------------------------

# Each entry: (label, cypher_statement, parameters, mode)
# mode = "explain" (no execution) or "profile" (executes with LIMIT guard)
_QUERY_INVENTORY: list[tuple[str, str, dict[str, Any], str]] = [
    # -- graph_community_detection --
    (
        "community:label_propagation_round",
        """
        MATCH (c:Character)-[r:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(n:Character)
        WHERE n.community_label IS NOT NULL
        WITH c, n.community_label AS neighbor_label,
             count(*) + sum(COALESCE(r.weight, COALESCE(r.count, 1))) AS weight
        ORDER BY weight DESC
        WITH c, collect(neighbor_label)[0] AS dominant_label
        SET c.community_label = dominant_label
        """,
        {},
        "explain",
    ),
    (
        "community:pagerank_iteration",
        """
        MATCH (c:Character)
        OPTIONAL MATCH (c)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(n:Character)
        WITH c, collect(n) AS neighbors
        WITH c, neighbors,
             CASE WHEN size(neighbors) > 0
                  THEN reduce(s = 0.0, n IN neighbors |
                      s + COALESCE(n.pr, 1.0) / CASE WHEN COUNT { (n)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-() } > 0
                                                     THEN COUNT { (n)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-() }
                                                     ELSE 1 END)
                  ELSE 0.0 END AS incoming
        SET c.pr = 0.15 + 0.85 * incoming
        """,
        {},
        "explain",
    ),
    # Switched from profile→explain: shortestPath traversals are expensive
    # and contributed to the 1020s tier timeout on compute-graph.
    (
        "community:betweenness_sampling",
        """
        MATCH (a:Character), (b:Character)
        WHERE rand() < 0.001 AND a.character_id <> b.character_id
        WITH a, b LIMIT 20
        MATCH p = shortestPath((a)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET*..5]-(b))
        UNWIND nodes(p) AS intermediate
        WITH intermediate WHERE intermediate:Character
        RETURN intermediate.character_id AS cid, count(*) AS path_count
        LIMIT 100
        """,
        {},
        "explain",
    ),
    # -- graph_pipeline: topology metrics --
    (
        "topology:character_intelligence",
        """
        MATCH (c:Character)
        OPTIONAL MATCH (c)-[co:CO_OCCURS_WITH]-(:Character)
        OPTIONAL MATCH (c)-[cross:CROSSED_SIDES]->(c)
        OPTIONAL MATCH (c)-[anom:ASSOCIATED_WITH_ANOMALY]->(:Battle)
        WITH c,
             count(DISTINCT co) AS degree_count,
             avg(COALESCE(co.weight, 0.0)) AS avg_co_weight,
             max(COALESCE(cross.side_transition_count, 0)) AS side_transitions,
             avg(COALESCE(anom.avg_z_score, 0.0)) AS anomalous_neighbor_density
        RETURN c.character_id AS character_id,
               degree_count, avg_co_weight, side_transitions, anomalous_neighbor_density
        LIMIT 100
        """,
        {},
        "profile",
    ),
    # Switched from profile→explain: multi-hop traversal is expensive.
    (
        "topology:engagement_avoidance",
        """
        MATCH (c:Character)-[:ON_SIDE]->(my_side:BattleSide)<-[:HAS_SIDE]-(b:Battle)-[:HAS_SIDE]->(opp_side:BattleSide)
        WHERE my_side.side_key <> opp_side.side_key
        MATCH (opp_side)-[:REPRESENTED_BY_ALLIANCE]->(a:Alliance)
        WITH c, a.alliance_id AS opp_alliance,
             collect(DISTINCT b) AS battles,
             avg(CASE WHEN my_side.anomaly_class = 'high_sustain' THEN 1.0 ELSE 0.0 END) AS hs_rate
        WHERE size(battles) >= 2
        RETURN c.character_id AS cid, opp_alliance, size(battles) AS enc, hs_rate
        LIMIT 100
        """,
        {},
        "explain",
    ),
    # -- graph_pipeline: derived relationships --
    (
        "derived:co_occurrence_build",
        """
        MATCH (c1:Character {character_id: 0})
        MATCH (c1)-[:PARTICIPATED_IN]->(b:Battle)<-[:PARTICIPATED_IN]-(c2:Character)
        WHERE c1.character_id < c2.character_id
        OPTIONAL MATCH (c1)-[:ON_SIDE]->(s1:BattleSide)<-[:HAS_SIDE]-(b)
        OPTIONAL MATCH (c2)-[:ON_SIDE]->(s2:BattleSide)<-[:HAS_SIDE]-(b)
        WITH c1, c2,
             count(DISTINCT b) AS occurrence_count,
             count(DISTINCT CASE WHEN s1.anomaly_class = 'high_sustain' OR s2.anomaly_class = 'high_sustain' THEN b END) AS hs_count
        RETURN c1.character_id AS c1, c2.character_id AS c2, occurrence_count, hs_count
        LIMIT 100
        """,
        {},
        "explain",
    ),
    # -- graph_pipeline: battle actor metrics --
    # Switched from profile→explain: full Battle×BattleSide×Character scan.
    (
        "topology:battle_actor_metrics",
        """
        MATCH (b:Battle)-[:HAS_SIDE]->(s:BattleSide)<-[:ON_SIDE]-(c:Character)
        OPTIONAL MATCH (c)-[co:CO_OCCURS_WITH]-(:Character)
        WITH b, s,
             count(DISTINCT c) AS participant_count,
             avg(COALESCE(co.weight, 0.0)) AS co_density
        RETURN b.battle_id AS battle_id, s.side_key AS side_key,
               participant_count, co_density
        LIMIT 100
        """,
        {},
        "explain",
    ),
    # -- graph_temporal_metrics --
    # Switched from profile→explain: multi-hop traversal with OPTIONAL MATCHes.
    (
        "temporal:windowed_metrics",
        """
        MATCH (c:Character)-[:ON_SIDE]->(:BattleSide)<-[:HAS_SIDE]-(b:Battle)
        WHERE b.started_at >= '2024-01-01T00:00:00Z'
        OPTIONAL MATCH (c)-[:ATTACKED_ON]->(k:Killmail)-[:PART_OF_BATTLE]->(b)
        OPTIONAL MATCH (c)-[:VICTIM_OF]->(v:Killmail)-[:PART_OF_BATTLE]->(b)
        OPTIONAL MATCH (c)-[co:CO_OCCURS_WITH]-(:Character)
        WITH c,
             count(DISTINCT b) AS battles_present,
             count(DISTINCT k) AS kills_total,
             count(DISTINCT v) AS losses_total
        WHERE battles_present > 0
        RETURN c.character_id AS cid, battles_present, kills_total, losses_total
        LIMIT 100
        """,
        {},
        "explain",
    ),
    # -- graph_pipeline: graph health snapshot --
    (
        "health:character_degree_stats",
        """
        MATCH (c:Character)
        WITH c, COUNT { (c)--() } AS deg
        RETURN avg(toFloat(deg)) AS avg_degree, max(toInteger(deg)) AS max_degree
        """,
        {},
        "profile",
    ),
    # -- graph_pipeline: critical edge ratio --
    (
        "insights:critical_edge_ratio",
        """
        MATCH (f:Fit)-[:CONTAINS]->(i:Item)
        WITH i, count(DISTINCT f) AS total_fits
        OPTIONAL MATCH (f2:Fit)-[:USES_CRITICAL_ITEM]->(i)
        WITH i, total_fits, count(DISTINCT f2) AS critical_fits
        RETURN toInteger(i.type_id) AS type_id, total_fits, critical_fits
        LIMIT 100
        """,
        {},
        "explain",
    ),
    # -- graph_pipeline: fit overlap --
    (
        "insights:fit_overlap",
        """
        MATCH (f1:Fit)-[r:SHARES_ITEM_WITH]->(f2:Fit)
        RETURN toInteger(f1.fit_id) AS fit_id,
               toInteger(f2.fit_id) AS other_fit_id,
               toInteger(r.shared_item_count) AS shared_item_count,
               toFloat(r.overlap_score) AS overlap_score
        LIMIT 100
        """,
        {},
        "explain",
    ),
    # -- community detection: bridge detection --
    (
        "community:bridge_detection",
        """
        MATCH (c:Character)-[:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(n:Character)
        WHERE c.community_label IS NOT NULL AND n.community_label IS NOT NULL
        WITH c, collect(DISTINCT n.community_label) AS neighbor_communities
        WHERE size(neighbor_communities) >= 2
        RETURN c.character_id AS character_id
        """,
        {},
        "explain",
    ),
    # -- community detection: export --
    (
        "community:export_assignments",
        """
        MATCH (c:Character)
        WHERE c.community_label IS NOT NULL
        OPTIONAL MATCH (c)-[r:CO_OCCURS_WITH|DIRECT_COMBAT|ASSISTED_KILL|SAME_FLEET]-(:Character)
        WITH c, count(r) AS degree
        RETURN c.character_id AS character_id,
               toInteger(c.community_label) AS community_id,
               toFloat(COALESCE(c.pr, 0.0)) AS pagerank_score,
               toFloat(COALESCE(c.betweenness_approx, 0.0)) AS betweenness_centrality,
               toInteger(degree) AS degree_centrality
        """,
        {},
        "explain",
    ),
]


# ---------------------------------------------------------------------------
#  Plan analysis and recommendations
# ---------------------------------------------------------------------------

def _generate_recommendations(metrics: QueryPlanMetrics, label: str) -> list[str]:
    """Generate actionable recommendations from plan metrics."""
    recs: list[str] = []

    if metrics.has_cartesian_product:
        recs.append(
            f"[{label}] CartesianProduct detected: ensure MATCH clauses share "
            f"bound variables or add connecting patterns to avoid cross-joins."
        )

    if metrics.has_all_nodes_scan:
        recs.append(
            f"[{label}] AllNodesScan: add a label to MATCH clauses so the "
            f"planner can use label-indexed lookups."
        )

    if metrics.has_eager:
        recs.append(
            f"[{label}] Eager operator: query forces full materialization "
            f"before proceeding. Consider splitting into staged queries or "
            f"using WITH clauses to control pipeline flow."
        )

    if metrics.db_hits > 100_000:
        recs.append(
            f"[{label}] High db hits ({metrics.db_hits:,}): review index "
            f"coverage and relationship traversal patterns. Consider adding "
            f"composite indexes or constraining the traversal window."
        )

    if metrics.has_node_by_label_scan and metrics.db_hits > 10_000:
        recs.append(
            f"[{label}] NodeByLabelScan with high db hits: property lookup "
            f"after label scan is expensive. Add an index on the filtered property."
        )

    if metrics.estimated_rows > 0 and metrics.rows > 0:
        ratio = metrics.rows / metrics.estimated_rows
        if ratio > 10:
            recs.append(
                f"[{label}] Cardinality blowup ({ratio:.1f}x): planner estimated "
                f"{metrics.estimated_rows:.0f} rows but got {metrics.rows:,}. "
                f"Run ANALYZE on the database or add cardinality hints."
            )

    return recs


# ---------------------------------------------------------------------------
#  Main job entry point
# ---------------------------------------------------------------------------

def run_graph_query_plan_validation(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run EXPLAIN/PROFILE pass on hot Cypher queries and report plan quality."""
    started = time.perf_counter()
    job_name = "graph_query_plan_validation"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})

    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    diagnostics: list[dict[str, Any]] = []
    all_recommendations: list[str] = []
    critical_count = 0
    warning_count = 0
    ok_count = 0
    errors: list[dict[str, str]] = []

    for label, statement, parameters, mode in _QUERY_INVENTORY:
        try:
            if mode == "profile":
                plan_metrics = client.profile(statement, parameters, timeout_seconds=30)
            else:
                plan_metrics = client.explain(statement, parameters, timeout_seconds=30)

            severity = plan_metrics.severity()
            if severity == "critical":
                critical_count += 1
            elif severity == "warning":
                warning_count += 1
            else:
                ok_count += 1

            recs = _generate_recommendations(plan_metrics, label)
            all_recommendations.extend(recs)

            diagnostics.append({
                "label": label,
                "mode": mode,
                **plan_metrics.to_dict(),
                "recommendations": recs,
            })

        except Exception as exc:
            errors.append({"label": label, "error": f"{type(exc).__name__}: {exc}"})
            diagnostics.append({
                "label": label,
                "mode": mode,
                "error": str(exc)[:300],
                "severity": "error",
            })

    # Sort diagnostics: critical first, then warning, then ok
    severity_order = {"critical": 0, "error": 1, "warning": 2, "ok": 3}
    diagnostics.sort(key=lambda d: severity_order.get(d.get("severity", "ok"), 4))

    snapshot_payload = {
        "computed_at": computed_at,
        "total_queries": len(_QUERY_INVENTORY),
        "critical": critical_count,
        "warning": warning_count,
        "ok": ok_count,
        "errors": len(errors),
        "recommendations": all_recommendations,
        "diagnostics": diagnostics,
    }

    db.upsert_intelligence_snapshot(
        snapshot_key="graph_query_plan_diagnostics",
        payload_json=json_dumps_safe(snapshot_payload),
        metadata_json=json_dumps_safe({
            "source": "neo4j+query_plan_validation",
            "reason": "scheduler:python",
        }),
        expires_seconds=14400,  # 4 hours
    )

    duration_ms = int((time.perf_counter() - started) * 1000)

    summary_parts = [
        f"Validated {len(_QUERY_INVENTORY)} queries:",
        f"{critical_count} critical" if critical_count else None,
        f"{warning_count} warnings" if warning_count else None,
        f"{ok_count} ok" if ok_count else None,
        f"{len(errors)} errors" if errors else None,
        f"{len(all_recommendations)} recommendations" if all_recommendations else None,
    ]
    summary = " ".join(p for p in summary_parts if p)

    return JobResult.success(
        job_key=job_name,
        summary=summary,
        rows_processed=len(_QUERY_INVENTORY),
        rows_written=len(diagnostics),
        rows_seen=len(_QUERY_INVENTORY),
        duration_ms=duration_ms,
        meta={
            "critical": critical_count,
            "warning": warning_count,
            "ok": ok_count,
            "errors": len(errors),
            "top_recommendations": all_recommendations[:10],
        },
    ).to_dict()
