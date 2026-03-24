from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..neo4j import Neo4jClient, Neo4jConfig


def _ensure_constraints(client: Neo4jClient) -> None:
    client.query("CREATE CONSTRAINT doctrine_id IF NOT EXISTS FOR (d:Doctrine) REQUIRE d.id IS UNIQUE")
    client.query("CREATE CONSTRAINT fit_id IF NOT EXISTS FOR (f:Fit) REQUIRE f.id IS UNIQUE")
    client.query("CREATE CONSTRAINT item_id IF NOT EXISTS FOR (i:Item) REQUIRE i.type_id IS UNIQUE")


def run_compute_graph_sync(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return {"status": "skipped", "reason": "neo4j disabled", "rows_processed": 0, "rows_written": 0}

    client = Neo4jClient(config)
    _ensure_constraints(client)

    sync_state = db.fetch_one("SELECT last_synced_at FROM graph_sync_state WHERE sync_key = %s LIMIT 1", ("doctrine_fit_item",))
    last_synced_at = str((sync_state or {}).get("last_synced_at") or "1970-01-01 00:00:00")

    rows = db.fetch_all(
        """
        SELECT
            dg.id AS doctrine_id,
            dg.group_name AS doctrine_name,
            df.id AS fit_id,
            df.fit_name,
            dfi.type_id,
            COALESCE(rit.type_name, CONCAT('Type #', dfi.type_id)) AS item_name,
            GREATEST(COALESCE(df.updated_at, '1970-01-01 00:00:00'), COALESCE(dfi.updated_at, '1970-01-01 00:00:00')) AS changed_at
        FROM doctrine_fits df
        INNER JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
        INNER JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
        INNER JOIN doctrine_fit_items dfi ON dfi.doctrine_fit_id = df.id
        LEFT JOIN ref_item_types rit ON rit.type_id = dfi.type_id
        WHERE GREATEST(COALESCE(df.updated_at, '1970-01-01 00:00:00'), COALESCE(dfi.updated_at, '1970-01-01 00:00:00')) >= %s
        ORDER BY changed_at ASC
        LIMIT 5000
        """,
        (last_synced_at,),
    )
    if not rows:
        return {"status": "success", "rows_processed": 0, "rows_written": 0, "last_synced_at": last_synced_at}

    client.query(
        """
        UNWIND $rows AS row
        MERGE (d:Doctrine {id: row.doctrine_id})
          SET d.name = row.doctrine_name
        MERGE (f:Fit {id: row.fit_id})
          SET f.name = row.fit_name
        MERGE (i:Item {type_id: row.type_id})
          SET i.name = row.item_name
        MERGE (d)-[:USES]->(f)
        MERGE (f)-[:CONTAINS]->(i)
        """,
        {"rows": rows},
    )

    latest_changed_at = max(str(row.get("changed_at") or last_synced_at) for row in rows)
    db.execute(
        """
        INSERT INTO graph_sync_state (sync_key, last_synced_at)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_synced_at = VALUES(last_synced_at), updated_at = CURRENT_TIMESTAMP
        """,
        ("doctrine_fit_item", latest_changed_at),
    )

    return {
        "status": "success",
        "rows_processed": len(rows),
        "rows_written": len(rows),
        "last_synced_at": latest_changed_at,
        "computed_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S"),
    }
