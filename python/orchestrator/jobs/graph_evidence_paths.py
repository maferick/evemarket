from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

SUSPICION_THRESHOLD = 0.5
MAX_PATH_LENGTH = 4
TOP_PATHS_PER_CHARACTER = 3

# Edge type weights for path scoring (lower = stronger evidence)
EDGE_WEIGHTS: dict[str, float] = {
    "SHARED_ALLIANCE_WITH": 0.90,
    "CO_OCCURS_WITH": 0.80,
    "DIRECT_COMBAT": 0.70,
    "ASSISTED_KILL": 0.75,
    "SAME_FLEET": 0.85,
    "CROSSED_SIDES": 0.60,
    "ASSOCIATED_WITH_ANOMALY": 0.65,
    "ON_SIDE": 0.95,
    "ATTACKED_ON": 0.70,
    "VICTIM_OF": 0.70,
}


def _build_path_description(nodes_data: list[dict[str, Any]], edges_data: list[dict[str, Any]]) -> str:
    """Generate a human-readable evidence path description."""
    parts: list[str] = []
    for i, node in enumerate(nodes_data):
        node_type = str(node.get("type") or "Unknown")
        node_name = str(node.get("name") or node.get("id") or "?")
        if i == 0:
            parts.append(f"{node_name}")
        else:
            edge_type = edges_data[i - 1]["type"] if i - 1 < len(edges_data) else "?"
            readable_edge = edge_type.replace("_", " ").lower()
            parts.append(f" \u2192 [{readable_edge}] \u2192 {node_name}")
            if node_type == "Character" and node.get("flagged"):
                parts.append(" (flagged)")
    return "".join(parts)


def run_graph_evidence_paths_sync(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_evidence_paths_sync"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})

    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Get flagged characters
    flagged = client.query(
        """
        MATCH (c:Character)
        WHERE COALESCE(c.suspicion_score, 0) > $threshold
        RETURN toInteger(c.character_id) AS character_id,
               COALESCE(c.name, 'Character #' + toString(c.character_id)) AS name,
               toFloat(c.suspicion_score) AS suspicion_score
        ORDER BY c.suspicion_score DESC
        LIMIT 200
        """,
        {"threshold": SUSPICION_THRESHOLD},
    )

    if not flagged:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return JobResult.success(
            job_key=job_name,
            summary="No flagged characters above threshold.",
            rows_processed=0, rows_written=0, rows_seen=0,
            duration_ms=duration_ms,
            meta={"threshold": SUSPICION_THRESHOLD},
        ).to_dict()

    all_paths: list[tuple[Any, ...]] = []

    for char_row in flagged:
        char_id = int(char_row.get("character_id") or 0)
        if char_id <= 0:
            continue

        # Find shortest paths to other suspicious or notable entities
        path_rows = client.query(
            """
            MATCH (c:Character {character_id: $char_id})
            MATCH p = (c)-[*1..4]-(target)
            WHERE (target:Character OR target:Alliance OR target:Battle)
              AND target <> c
              AND (target:Alliance OR target:Battle OR COALESCE(target.suspicion_score, 0) > 0.3)
            WITH c, p, target,
                 relationships(p) AS rels,
                 nodes(p) AS path_nodes,
                 length(p) AS path_length
            WITH c, p, target, rels, path_nodes, path_length,
                 reduce(score = 1.0, r IN rels |
                     score * CASE type(r)
                         WHEN 'SHARED_ALLIANCE_WITH' THEN 0.90
                         WHEN 'CO_OCCURS_WITH' THEN 0.80
                         WHEN 'DIRECT_COMBAT' THEN 0.70
                         WHEN 'ASSISTED_KILL' THEN 0.75
                         WHEN 'SAME_FLEET' THEN 0.85
                         WHEN 'CROSSED_SIDES' THEN 0.60
                         WHEN 'ASSOCIATED_WITH_ANOMALY' THEN 0.65
                         WHEN 'ON_SIDE' THEN 0.95
                         WHEN 'ATTACKED_ON' THEN 0.70
                         WHEN 'VICTIM_OF' THEN 0.70
                         ELSE 0.90 END
                 ) AS path_score
            ORDER BY path_score ASC
            LIMIT $top_n
            RETURN
                [n IN path_nodes | {
                    type: CASE
                        WHEN n:Character THEN 'Character'
                        WHEN n:Alliance THEN 'Alliance'
                        WHEN n:Battle THEN 'Battle'
                        WHEN n:Corporation THEN 'Corporation'
                        ELSE 'Unknown' END,
                    id: COALESCE(n.character_id, n.alliance_id, n.battle_id, n.corporation_id, id(n)),
                    name: COALESCE(n.name, n.character_name, n.alliance_name, toString(COALESCE(n.character_id, n.alliance_id, n.battle_id, ''))),
                    flagged: CASE WHEN n:Character AND COALESCE(n.suspicion_score, 0) > 0.5 THEN true ELSE false END
                }] AS path_nodes_data,
                [r IN rels | {
                    type: type(r),
                    properties: {weight: COALESCE(r.weight, r.count, 1)}
                }] AS path_edges_data,
                toFloat(path_score) AS path_score
            """,
            {"char_id": char_id, "top_n": TOP_PATHS_PER_CHARACTER},
        )

        for rank, pr in enumerate(path_rows, start=1):
            nodes_data = pr.get("path_nodes_data") or []
            edges_data = pr.get("path_edges_data") or []
            path_score = float(pr.get("path_score") or 0.0)
            description = _build_path_description(nodes_data, edges_data)

            all_paths.append((
                char_id,
                rank,
                description,
                json_dumps_safe(nodes_data),
                json_dumps_safe(edges_data),
                round(1.0 - path_score, 6),  # Invert: lower raw = stronger evidence = higher score
                computed_at,
            ))

    # Truncate and insert
    db.execute("DELETE FROM character_evidence_paths")

    batch_size = 500
    total_written = 0
    for offset in range(0, len(all_paths), batch_size):
        chunk = all_paths[offset:offset + batch_size]
        placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s)"] * len(chunk))
        flat_params: list[Any] = []
        for row in chunk:
            flat_params.extend(row)
        if flat_params:
            db.execute(
                "INSERT INTO character_evidence_paths "
                "(character_id, path_rank, path_description, path_nodes_json, "
                "path_edges_json, path_score, computed_at) "
                "VALUES " + placeholders,
                tuple(flat_params),
            )
            total_written += len(chunk)

    characters_with_paths = len(set(p[0] for p in all_paths))

    db.upsert_intelligence_snapshot(
        snapshot_key="graph_evidence_paths_state",
        payload_json=json_dumps_safe({
            "total_paths": total_written,
            "characters_with_paths": characters_with_paths,
            "suspicion_threshold": SUSPICION_THRESHOLD,
        }),
        metadata_json=json_dumps_safe({"source": "neo4j", "reason": "scheduler:python"}),
        expires_seconds=7200,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=f"Evidence paths: {total_written} paths for {characters_with_paths} flagged characters.",
        rows_processed=len(flagged),
        rows_written=total_written,
        rows_seen=len(flagged),
        duration_ms=duration_ms,
        meta={"characters_with_paths": characters_with_paths},
    ).to_dict()
