from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..neo4j import Neo4jClient, Neo4jConfig


def run_compute_graph_insights(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return {"status": "skipped", "reason": "neo4j disabled", "rows_processed": 0, "rows_written": 0}

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    doctrine_rows = client.query(
        """
        MATCH (d:Doctrine)-[:USES]->(f:Fit)
        OPTIONAL MATCH (f)-[:CONTAINS]->(i:Item)
        RETURN d.id AS doctrine_id, coalesce(d.name, '') AS doctrine_name, count(DISTINCT f) AS fit_count, count(DISTINCT i) AS item_count
        """
    )

    item_rows = client.query(
        """
        MATCH (d:Doctrine)-[:USES]->(f:Fit)-[:CONTAINS]->(i:Item)
        RETURN i.type_id AS type_id, count(DISTINCT d) AS doctrine_count, count(DISTINCT f) AS fit_count
        """
    )

    with db.transaction() as (_, cursor):
        for row in doctrine_rows:
            doctrine_id = int(row.get("doctrine_id") or 0)
            if doctrine_id <= 0:
                continue
            fit_count = max(0, int(row.get("fit_count") or 0))
            item_count = max(0, int(row.get("item_count") or 0))
            dependency_depth = 2 if fit_count > 0 and item_count > 0 else (1 if fit_count > 0 else 0)
            cursor.execute(
                """
                INSERT INTO doctrine_dependency_depth (doctrine_id, doctrine_name, fit_count, item_count, dependency_depth, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    doctrine_name = VALUES(doctrine_name),
                    fit_count = VALUES(fit_count),
                    item_count = VALUES(item_count),
                    dependency_depth = VALUES(dependency_depth),
                    computed_at = VALUES(computed_at)
                """,
                (doctrine_id, str(row.get("doctrine_name") or "Doctrine"), fit_count, item_count, dependency_depth, computed_at),
            )

        for row in item_rows:
            type_id = int(row.get("type_id") or 0)
            if type_id <= 0:
                continue
            doctrine_count = max(0, int(row.get("doctrine_count") or 0))
            fit_count = max(0, int(row.get("fit_count") or 0))
            dependency_score = round((doctrine_count * 0.7) + (fit_count * 0.3), 2)
            cursor.execute(
                """
                INSERT INTO item_dependency_score (type_id, doctrine_count, fit_count, dependency_score, computed_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    doctrine_count = VALUES(doctrine_count),
                    fit_count = VALUES(fit_count),
                    dependency_score = VALUES(dependency_score),
                    computed_at = VALUES(computed_at)
                """,
                (type_id, doctrine_count, fit_count, dependency_score, computed_at),
            )

    return {
        "status": "success",
        "rows_processed": len(doctrine_rows) + len(item_rows),
        "rows_written": len(doctrine_rows) + len(item_rows),
        "computed_at": computed_at,
    }
