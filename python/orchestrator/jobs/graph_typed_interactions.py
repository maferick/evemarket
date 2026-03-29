from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

RELATIONSHIP_WINDOW_DAYS = 90
SAME_FLEET_MIN_CO_BATTLES = 3


def _utc_cutoff_iso(days: int) -> str:
    return (datetime.now(UTC) - timedelta(days=days)).isoformat()


def _create_typed_relationships(client: Neo4jClient, window_days: int) -> dict[str, int]:
    """Create typed Neo4j relationships and return counts per type."""
    counts: dict[str, int] = {}
    cutoff = _utc_cutoff_iso(window_days)

    # Direct combat: A attacked a killmail where B was victim
    result = client.query(
        """
        MATCH (a:Character)-[:ATTACKED_ON]->(k:Killmail)<-[:VICTIM_OF]-(b:Character)
        WHERE a.character_id <> b.character_id
          AND k.occurred_at >= $cutoff
        WITH a, b, count(DISTINCT k) AS cnt, max(k.occurred_at) AS last_at
        MERGE (a)-[r:DIRECT_COMBAT]->(b)
        SET r.count = cnt, r.last_at = last_at, r.updated_at = datetime()
        RETURN count(r) AS total
        """,
        {"cutoff": cutoff},
    )
    counts["direct_combat"] = int((result[0] if result else {}).get("total") or 0)

    # Assisted kill: A and B both attacked the same killmail
    result = client.query(
        """
        MATCH (a:Character)-[:ATTACKED_ON]->(k:Killmail)<-[:ATTACKED_ON]-(b:Character)
        WHERE a.character_id < b.character_id
          AND k.occurred_at >= $cutoff
        WITH a, b, count(DISTINCT k) AS cnt, max(k.occurred_at) AS last_at
        MERGE (a)-[r:ASSISTED_KILL]->(b)
        SET r.count = cnt, r.last_at = last_at, r.updated_at = datetime()
        RETURN count(r) AS total
        """,
        {"cutoff": cutoff},
    )
    counts["assisted_kill"] = int((result[0] if result else {}).get("total") or 0)

    # Same fleet inference: A and B on same side in multiple battles
    result = client.query(
        """
        MATCH (a:Character)-[:ON_SIDE]->(s:BattleSide)<-[:ON_SIDE]-(b:Character)
        WHERE a.character_id < b.character_id
        WITH a, b, count(DISTINCT s) AS co_battles
        WHERE co_battles >= $min_co_battles
        MERGE (a)-[r:SAME_FLEET]->(b)
        SET r.count = co_battles, r.updated_at = datetime()
        RETURN count(r) AS total
        """,
        {"min_co_battles": SAME_FLEET_MIN_CO_BATTLES},
    )
    counts["same_fleet"] = int((result[0] if result else {}).get("total") or 0)

    return counts


def _export_typed_interactions(client: Neo4jClient, db: SupplyCoreDb, computed_at: str) -> int:
    """Export typed interactions from Neo4j to MariaDB."""
    interaction_types = [
        ("direct_combat", "DIRECT_COMBAT"),
        ("assisted_kill", "ASSISTED_KILL"),
        ("same_fleet", "SAME_FLEET"),
    ]

    total_written = 0
    for interaction_type, rel_type in interaction_types:
        rows = client.query(
            f"""
            MATCH (a:Character)-[r:{rel_type}]->(b:Character)
            RETURN
                a.character_id AS character_a_id,
                b.character_id AS character_b_id,
                toInteger(r.count) AS interaction_count,
                toString(r.last_at) AS last_interaction_at
            """,
        )
        if not rows:
            continue

        batch_size = 500
        for offset in range(0, len(rows), batch_size):
            chunk = rows[offset:offset + batch_size]
            values = []
            params: list[Any] = []
            for r in chunk:
                a_id = int(r.get("character_a_id") or 0)
                b_id = int(r.get("character_b_id") or 0)
                if a_id <= 0 or b_id <= 0:
                    continue
                values.append("(%s, %s, %s, %s, %s, %s)")
                last_at = str(r.get("last_interaction_at") or "")[:19] or None
                params.extend([
                    a_id,
                    b_id,
                    interaction_type,
                    int(r.get("interaction_count") or 1),
                    last_at,
                    computed_at,
                ])
            if values:
                db.execute(
                    "INSERT INTO character_typed_interactions "
                    "(character_a_id, character_b_id, interaction_type, interaction_count, "
                    "last_interaction_at, computed_at) VALUES " + ", ".join(values) + " "
                    "ON DUPLICATE KEY UPDATE "
                    "interaction_count = VALUES(interaction_count), "
                    "last_interaction_at = VALUES(last_interaction_at), "
                    "computed_at = VALUES(computed_at)",
                    tuple(params),
                )
                total_written += len(values)

    return total_written


def run_graph_typed_interactions_sync(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_typed_interactions_sync"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})

    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    # Create typed relationships in Neo4j
    neo4j_counts = _create_typed_relationships(client, RELATIONSHIP_WINDOW_DAYS)

    # Export to MariaDB
    rows_written = _export_typed_interactions(client, db, computed_at)

    total_neo4j_rels = sum(neo4j_counts.values())

    db.upsert_intelligence_snapshot(
        snapshot_key="graph_typed_interactions_state",
        payload_json=json_dumps_safe({
            "neo4j_relationship_counts": neo4j_counts,
            "mariadb_rows_written": rows_written,
        }),
        metadata_json=json_dumps_safe({"source": "neo4j", "reason": "scheduler:python"}),
        expires_seconds=7200,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=f"Typed interactions: {total_neo4j_rels} Neo4j rels created, {rows_written} MariaDB rows written.",
        rows_processed=total_neo4j_rels,
        rows_written=rows_written,
        rows_seen=total_neo4j_rels,
        duration_ms=duration_ms,
        meta={"neo4j_counts": neo4j_counts},
    ).to_dict()
