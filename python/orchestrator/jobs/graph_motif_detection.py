from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig


def _detect_triangles(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect triangle motifs: 3 characters mutually co-occurring."""
    return client.query(
        """
        MATCH (a:Character)-[:CO_OCCURS_WITH]-(b:Character)-[:CO_OCCURS_WITH]-(c:Character)-[:CO_OCCURS_WITH]-(a)
        WHERE a.character_id < b.character_id AND b.character_id < c.character_id
        WITH a, b, c
        OPTIONAL MATCH (a)-[:ON_SIDE]->(:BattleSide)<-[:HAS_SIDE]-(battle:Battle)
        WITH a, b, c, collect(DISTINCT battle.battle_id)[..10] AS battle_ids
        RETURN
            'triangle' AS motif_type,
            [toInteger(a.character_id), toInteger(b.character_id), toInteger(c.character_id)] AS member_ids,
            battle_ids,
            1 AS occurrence_count,
            toFloat(
                (COALESCE(a.suspicion_score, 0) + COALESCE(b.suspicion_score, 0) + COALESCE(c.suspicion_score, 0)) / 3.0
            ) AS suspicion_relevance
        LIMIT 500
        """
    )


def _detect_stars(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect star motifs: 1 hub connected to 4+ others who don't connect to each other."""
    return client.query(
        """
        MATCH (hub:Character)-[:CO_OCCURS_WITH]-(leaf:Character)
        WITH hub, collect(leaf) AS leaves
        WHERE size(leaves) >= 4
        WITH hub, leaves,
             [l1 IN leaves WHERE NONE(l2 IN leaves WHERE l1 <> l2 AND (l1)-[:CO_OCCURS_WITH]-(l2))] AS isolated_leaves
        WHERE size(isolated_leaves) >= 4
        WITH hub, isolated_leaves[..8] AS top_leaves
        RETURN
            'star' AS motif_type,
            [toInteger(hub.character_id)] + [l IN top_leaves | toInteger(l.character_id)] AS member_ids,
            [] AS battle_ids,
            1 AS occurrence_count,
            toFloat(COALESCE(hub.suspicion_score, 0)) AS suspicion_relevance
        LIMIT 200
        """
    )


def _detect_chains(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect chain motifs: A→B→C→D with no shortcuts (intelligence relay pattern)."""
    return client.query(
        """
        MATCH (a:Character)-[:CO_OCCURS_WITH]-(b:Character)-[:CO_OCCURS_WITH]-(c:Character)-[:CO_OCCURS_WITH]-(d:Character)
        WHERE a.character_id <> d.character_id
          AND a.character_id <> c.character_id
          AND b.character_id <> d.character_id
          AND NOT (a)-[:CO_OCCURS_WITH]-(c)
          AND NOT (a)-[:CO_OCCURS_WITH]-(d)
          AND NOT (b)-[:CO_OCCURS_WITH]-(d)
          AND a.character_id < d.character_id
        WITH a, b, c, d
        RETURN
            'chain' AS motif_type,
            [toInteger(a.character_id), toInteger(b.character_id), toInteger(c.character_id), toInteger(d.character_id)] AS member_ids,
            [] AS battle_ids,
            1 AS occurrence_count,
            toFloat(
                (COALESCE(a.suspicion_score, 0) + COALESCE(b.suspicion_score, 0) +
                 COALESCE(c.suspicion_score, 0) + COALESCE(d.suspicion_score, 0)) / 4.0
            ) AS suspicion_relevance
        LIMIT 300
        """
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
        WITH [toInteger(a.character_id), toInteger(b.character_id), toInteger(c.character_id)]
             + [e IN extras | toInteger(e.character_id)] AS member_ids,
             (COALESCE(a.suspicion_score, 0) + COALESCE(b.suspicion_score, 0) + COALESCE(c.suspicion_score, 0)) / 3.0 AS avg_suspicion
        RETURN
            'fleet_core' AS motif_type,
            member_ids,
            [] AS battle_ids,
            1 AS occurrence_count,
            toFloat(avg_suspicion) AS suspicion_relevance
        LIMIT 200
        """
    )


def _detect_rotating_scouts(client: Neo4jClient) -> list[dict[str, Any]]:
    """Detect rotating scout: character alternates sides across battles."""
    return client.query(
        """
        MATCH (c:Character)-[cr:CROSSED_SIDES]->(c)
        WHERE COALESCE(cr.side_transition_count, 0) >= 3
        RETURN
            'rotating_scout' AS motif_type,
            [toInteger(c.character_id)] AS member_ids,
            [] AS battle_ids,
            toInteger(cr.side_transition_count) AS occurrence_count,
            toFloat(COALESCE(c.suspicion_score, 0) * 1.5) AS suspicion_relevance
        ORDER BY cr.side_transition_count DESC
        LIMIT 200
        """
    )


def run_graph_motif_detection_sync(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_motif_detection_sync"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})

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
    for motif_name, detector_fn in detectors:
        try:
            results = detector_fn(client)
            all_motifs.extend(results)
            counts_by_type[motif_name] = len(results)
        except Exception:
            counts_by_type[motif_name] = 0

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

    # Truncate and re-insert
    db.execute("DELETE FROM graph_motif_detections")

    batch_size = 500
    total_written = 0
    for offset in range(0, len(all_motifs), batch_size):
        chunk = all_motifs[offset:offset + batch_size]
        values = []
        params: list[Any] = []
        for m in chunk:
            member_ids = m.get("member_ids") or []
            battle_ids = m.get("battle_ids") or []
            values.append("(%s, %s, %s, %s, %s, %s, %s)")
            params.extend([
                str(m.get("motif_type") or "unknown"),
                json_dumps_safe(member_ids),
                json_dumps_safe(battle_ids) if battle_ids else None,
                int(m.get("occurrence_count") or 1),
                min(1.0, max(0.0, float(m.get("suspicion_relevance") or 0.0))),
                computed_at,
                computed_at,
            ])
        if values:
            db.execute(
                "INSERT INTO graph_motif_detections "
                "(motif_type, member_ids_json, battle_ids_json, occurrence_count, "
                "suspicion_relevance, first_seen_at, computed_at) "
                "VALUES " + ", ".join(values),
                tuple(params),
            )
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
    return JobResult.success(
        job_key=job_name,
        summary=f"Motif detection: {total_written} motifs found ({', '.join(f'{k}={v}' for k, v in counts_by_type.items())}).",
        rows_processed=total_written,
        rows_written=total_written,
        rows_seen=total_written,
        duration_ms=duration_ms,
        meta={"counts": counts_by_type},
    ).to_dict()
