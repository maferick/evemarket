from __future__ import annotations

from datetime import UTC, datetime
import json
import time
from pathlib import Path
from typing import Any

from ..db import SupplyCoreDb
from ..neo4j import Neo4jClient, Neo4jConfig


def _write_graph_log(log_file: str, event: str, payload: dict[str, Any]) -> None:
    target = str(log_file).strip()
    if target == "":
        return

    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"event": event, "ts": datetime.now(UTC).isoformat(), **payload}, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def _query_item_dependency_rows(client: Neo4jClient) -> list[dict[str, Any]]:
    return client.query(
        """
        MATCH (d:Doctrine)-[:USES]->(f:Fit)-[:CONTAINS]->(i:Item)
        WITH i,
             count(DISTINCT d) AS doctrine_count,
             count(DISTINCT f) AS fit_count
        RETURN
            toInteger(i.type_id) AS type_id,
            toInteger(doctrine_count) AS doctrine_count,
            toInteger(fit_count) AS fit_count,
            toFloat((doctrine_count * 10) + fit_count) AS dependency_score
        ORDER BY dependency_score DESC, type_id ASC
        """
    )


def _query_doctrine_dependency_rows(client: Neo4jClient) -> list[dict[str, Any]]:
    return client.query(
        """
        MATCH (d:Doctrine)
        OPTIONAL MATCH (d)-[:USES]->(f:Fit)
        OPTIONAL MATCH (f)-[:CONTAINS]->(i:Item)
        WITH d,
             count(DISTINCT f) AS fit_count,
             count(i) AS item_count,
             count(DISTINCT i) AS unique_item_count
        RETURN
            toInteger(d.id) AS doctrine_id,
            toInteger(fit_count) AS fit_count,
            toInteger(item_count) AS item_count,
            toInteger(unique_item_count) AS unique_item_count,
            toFloat(unique_item_count + (fit_count * 5)) AS dependency_depth_score
        ORDER BY dependency_depth_score DESC, doctrine_id ASC
        """
    )


def run_compute_graph_insights(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        result = {
            "status": "skipped",
            "reason": "neo4j disabled",
            "rows_processed": 0,
            "rows_written": 0,
            "duration_ms": int((time.perf_counter() - started) * 1000),
            "errors": [],
        }
        _write_graph_log(config.log_file, "graph.job.skipped", {"job": "compute_graph_insights", **result})
        return result

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    errors: list[str] = []

    item_rows = _query_item_dependency_rows(client)
    doctrine_rows = _query_doctrine_dependency_rows(client)

    item_payload: list[tuple[int, int, int, float, str]] = []
    for row in item_rows:
        type_id = int(row.get("type_id") or 0)
        if type_id <= 0:
            continue
        doctrine_count = max(0, int(row.get("doctrine_count") or 0))
        fit_count = max(0, int(row.get("fit_count") or 0))
        dependency_score = float(row.get("dependency_score") or 0.0)
        item_payload.append((type_id, doctrine_count, fit_count, round(dependency_score, 4), computed_at))

    doctrine_payload: list[tuple[int, int, int, int, float, str]] = []
    for row in doctrine_rows:
        doctrine_id = int(row.get("doctrine_id") or 0)
        if doctrine_id <= 0:
            continue
        fit_count = max(0, int(row.get("fit_count") or 0))
        item_count = max(0, int(row.get("item_count") or 0))
        unique_item_count = max(0, int(row.get("unique_item_count") or 0))
        dependency_depth_score = float(row.get("dependency_depth_score") or 0.0)
        doctrine_payload.append((doctrine_id, fit_count, item_count, unique_item_count, round(dependency_depth_score, 4), computed_at))

    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM item_dependency_score")
        if item_payload:
            cursor.executemany(
                """
                INSERT INTO item_dependency_score (type_id, doctrine_count, fit_count, dependency_score, computed_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                item_payload,
            )

        cursor.execute("DELETE FROM doctrine_dependency_depth")
        if doctrine_payload:
            cursor.executemany(
                """
                INSERT INTO doctrine_dependency_depth (doctrine_id, fit_count, item_count, unique_item_count, dependency_depth_score, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                doctrine_payload,
            )

    rows_processed = len(item_rows) + len(doctrine_rows)
    rows_written = len(item_payload) + len(doctrine_payload)
    duration_ms = int((time.perf_counter() - started) * 1000)
    result = {
        "status": "success",
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "duration_ms": duration_ms,
        "errors": errors,
        "computed_at": computed_at,
    }
    _write_graph_log(config.log_file, "graph.insights.completed", result)
    return result
