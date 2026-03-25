from __future__ import annotations

from datetime import UTC, datetime
import json
import time
from pathlib import Path
from typing import Any

from ..db import SupplyCoreDb
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

GRAPH_SYNC_KEYS = {
    "doctrine": "graph_sync_doctrine_dependency",
    "battle": "graph_sync_battle_intelligence",
}

# Graph-density controls.
CO_OCCUR_THRESHOLD = 2
SHARED_ITEM_THRESHOLD = 2
ANOMALY_ASSOC_THRESHOLD = 1
TOP_K_CHARACTER_EDGES = 100
TOP_K_FIT_EDGES = 100
RELATIONSHIP_WINDOW_DAYS = 30
STALE_DAYS = 45


def _utc_now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _write_graph_log(log_file: str, event: str, payload: dict[str, Any]) -> None:
    target = str(log_file).strip()
    if target == "":
        return
    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json_dumps_safe({"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload})
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def _job_payload(job_name: str, started_at: float, status: str, **kwargs: Any) -> dict[str, Any]:
    payload = {
        "job_name": job_name,
        "status": status,
        "duration_ms": int((time.perf_counter() - started_at) * 1000),
        "rows_processed": int(kwargs.pop("rows_processed", 0)),
        "rows_written": int(kwargs.pop("rows_written", 0)),
        "nodes_created": int(kwargs.pop("nodes_created", 0)),
        "nodes_merged": int(kwargs.pop("nodes_merged", 0)),
        "relationships_created": int(kwargs.pop("relationships_created", 0)),
        "relationships_merged": int(kwargs.pop("relationships_merged", 0)),
        "error_text": str(kwargs.pop("error_text", "")),
        "timestamp": datetime.now(UTC).isoformat(),
    }
    payload.update(kwargs)
    return payload


def _ensure_constraints_and_indexes(client: Neo4jClient) -> None:
    client.query("CREATE CONSTRAINT character_character_id IF NOT EXISTS FOR (n:Character) REQUIRE n.character_id IS UNIQUE")
    client.query("CREATE CONSTRAINT battle_battle_id IF NOT EXISTS FOR (n:Battle) REQUIRE n.battle_id IS UNIQUE")
    client.query("CREATE CONSTRAINT doctrine_doctrine_id IF NOT EXISTS FOR (n:Doctrine) REQUIRE n.doctrine_id IS UNIQUE")
    client.query("CREATE CONSTRAINT fit_fit_id IF NOT EXISTS FOR (n:Fit) REQUIRE n.fit_id IS UNIQUE")
    client.query("CREATE CONSTRAINT item_type_id IF NOT EXISTS FOR (n:Item) REQUIRE n.type_id IS UNIQUE")
    client.query("CREATE CONSTRAINT side_side_uid IF NOT EXISTS FOR (n:BattleSide) REQUIRE n.side_uid IS UNIQUE")
    client.query("CREATE CONSTRAINT alliance_alliance_id IF NOT EXISTS FOR (n:Alliance) REQUIRE n.alliance_id IS UNIQUE")
    client.query("CREATE CONSTRAINT corp_corporation_id IF NOT EXISTS FOR (n:Corporation) REQUIRE n.corporation_id IS UNIQUE")
    client.query("CREATE CONSTRAINT ship_type_id IF NOT EXISTS FOR (n:ShipType) REQUIRE n.type_id IS UNIQUE")
    client.query("CREATE CONSTRAINT system_system_id IF NOT EXISTS FOR (n:System) REQUIRE n.system_id IS UNIQUE")

    client.query("CREATE INDEX character_lookup IF NOT EXISTS FOR (n:Character) ON (n.character_id)")
    client.query("CREATE INDEX battle_lookup IF NOT EXISTS FOR (n:Battle) ON (n.battle_id)")
    client.query("CREATE INDEX battle_started_lookup IF NOT EXISTS FOR (n:Battle) ON (n.started_at)")
    client.query("CREATE INDEX doctrine_lookup IF NOT EXISTS FOR (n:Doctrine) ON (n.doctrine_id)")
    client.query("CREATE INDEX fit_lookup IF NOT EXISTS FOR (n:Fit) ON (n.fit_id)")
    client.query("CREATE INDEX item_lookup IF NOT EXISTS FOR (n:Item) ON (n.type_id)")
    client.query("CREATE INDEX side_key_lookup IF NOT EXISTS FOR (n:BattleSide) ON (n.side_key)")


def _load_sync_cursor(db: SupplyCoreDb, sync_key: str) -> str:
    row = db.fetch_one("SELECT last_synced_at FROM graph_sync_state WHERE sync_key = %s LIMIT 1", (sync_key,))
    return str((row or {}).get("last_synced_at") or "1970-01-01 00:00:00")


def _save_sync_cursor(db: SupplyCoreDb, sync_key: str, value: str) -> None:
    db.execute(
        """
        INSERT INTO graph_sync_state (sync_key, last_synced_at)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE last_synced_at = VALUES(last_synced_at), updated_at = CURRENT_TIMESTAMP
        """,
        (sync_key, value),
    )


def _snapshot_graph_health(client: Neo4jClient) -> dict[str, Any]:
    labels = client.query("MATCH (n) UNWIND labels(n) AS label RETURN label, count(*) AS count ORDER BY count DESC")
    rel_types = client.query("MATCH ()-[r]->() RETURN type(r) AS rel_type, count(*) AS count ORDER BY count DESC")

    degrees = client.query(
        """
        MATCH (c:Character)
        WITH c, COUNT { (c)--() } AS deg
        RETURN avg(toFloat(deg)) AS avg_character_degree, max(toInteger(deg)) AS max_character_degree
        """
    )
    fit_degrees = client.query(
        """
        MATCH (f:Fit)
        WITH f, COUNT { (f)--() } AS deg
        RETURN avg(toFloat(deg)) AS avg_fit_degree, max(toInteger(deg)) AS max_fit_degree
        """
    )
    return {
        "labels": labels,
        "relationships": rel_types,
        "degree": (degrees[0] if degrees else {}),
        "fit_degree": (fit_degrees[0] if fit_degrees else {}),
    }


def run_compute_graph_sync_doctrine_dependency(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "compute_graph_sync_doctrine_dependency"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return _job_payload(job_name, started, "skipped", error_text="neo4j disabled")

    client = Neo4jClient(config)
    _ensure_constraints_and_indexes(client)

    last_synced_at = _load_sync_cursor(db, GRAPH_SYNC_KEYS["doctrine"])
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
        LIMIT 20000
        """,
        (last_synced_at,),
    )
    if not rows:
        return _job_payload(job_name, started, "success", last_synced_at=last_synced_at)

    client.query(
        """
        UNWIND $rows AS row
        MERGE (d:Doctrine {doctrine_id: toInteger(row.doctrine_id)})
          SET d.name = row.doctrine_name
        MERGE (f:Fit {fit_id: toInteger(row.fit_id)})
          SET f.name = row.fit_name
        MERGE (i:Item {type_id: toInteger(row.type_id)})
          SET i.name = row.item_name
        MERGE (d)-[:USES]->(f)
        MERGE (f)-[:CONTAINS]->(i)
        """,
        {"rows": rows},
    )

    latest_synced_at = max(str(row.get("changed_at") or last_synced_at) for row in rows)
    _save_sync_cursor(db, GRAPH_SYNC_KEYS["doctrine"], latest_synced_at)

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=len(rows),
        rows_written=len(rows),
        nodes_merged=len(rows) * 3,
        relationships_merged=len(rows) * 2,
        last_synced_at=latest_synced_at,
    )
    _write_graph_log(config.log_file, "graph.sync.doctrine.completed", result)
    return result


def run_compute_graph_sync_battle_intelligence(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "compute_graph_sync_battle_intelligence"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return _job_payload(job_name, started, "skipped", error_text="neo4j disabled")

    client = Neo4jClient(config)
    _ensure_constraints_and_indexes(client)
    last_synced_at = _load_sync_cursor(db, GRAPH_SYNC_KEYS["battle"])

    rows = db.fetch_all(
        """
        SELECT
            bp.battle_id,
            bp.character_id,
            bp.side_key,
            bp.ship_type_id,
            bp.alliance_id,
            bp.corporation_id,
            br.system_id,
            br.started_at,
            br.ended_at,
            br.participant_count,
            COALESCE(ba.anomaly_class, 'normal') AS anomaly_class,
            COALESCE(ba.z_efficiency_score, 0) AS z_efficiency_score,
            GREATEST(
                COALESCE(bp.updated_at, '1970-01-01 00:00:00'),
                COALESCE(br.updated_at, '1970-01-01 00:00:00'),
                COALESCE(ba.updated_at, '1970-01-01 00:00:00')
            ) AS changed_at
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        LEFT JOIN battle_anomalies ba ON ba.battle_id = bp.battle_id AND ba.side_key = bp.side_key
        WHERE GREATEST(
                COALESCE(bp.updated_at, '1970-01-01 00:00:00'),
                COALESCE(br.updated_at, '1970-01-01 00:00:00'),
                COALESCE(ba.updated_at, '1970-01-01 00:00:00')
            ) >= %s
        ORDER BY changed_at ASC
        LIMIT 25000
        """,
        (last_synced_at,),
    )
    if not rows:
        return _job_payload(job_name, started, "success", last_synced_at=last_synced_at)

    client.query(
        """
        UNWIND $rows AS row
        MERGE (b:Battle {battle_id: row.battle_id})
          SET b.started_at = row.started_at,
              b.ended_at = row.ended_at,
              b.participant_count = toInteger(row.participant_count)
        MERGE (sys:System {system_id: toInteger(row.system_id)})
        MERGE (b)-[:IN_SYSTEM]->(sys)

        WITH row, b
        MERGE (side:BattleSide {side_uid: row.battle_id + '|' + row.side_key})
          SET side.battle_id = row.battle_id,
              side.side_key = row.side_key,
              side.anomaly_class = row.anomaly_class,
              side.z_efficiency_score = toFloat(row.z_efficiency_score)
        MERGE (b)-[:HAS_SIDE]->(side)

        WITH row, side, b
        MERGE (c:Character {character_id: toInteger(row.character_id)})
        MERGE (c)-[:PARTICIPATED_IN]->(b)
        MERGE (c)-[:ON_SIDE]->(side)

        FOREACH(_ IN CASE WHEN toInteger(COALESCE(row.ship_type_id, 0)) > 0 THEN [1] ELSE [] END |
            MERGE (ship:ShipType {type_id: toInteger(row.ship_type_id)})
            MERGE (c)-[:FLEW]->(ship)
        )

        FOREACH(_ IN CASE WHEN toInteger(COALESCE(row.alliance_id, 0)) > 0 THEN [1] ELSE [] END |
            MERGE (a:Alliance {alliance_id: toInteger(row.alliance_id)})
            MERGE (c)-[:MEMBER_OF_ALLIANCE]->(a)
            MERGE (side)-[:REPRESENTED_BY_ALLIANCE]->(a)
        )

        FOREACH(_ IN CASE WHEN toInteger(COALESCE(row.corporation_id, 0)) > 0 THEN [1] ELSE [] END |
            MERGE (corp:Corporation {corporation_id: toInteger(row.corporation_id)})
            MERGE (c)-[:MEMBER_OF_CORPORATION]->(corp)
            MERGE (side)-[:REPRESENTED_BY_CORPORATION]->(corp)
        )
        """,
        {"rows": rows},
    )

    latest_synced_at = max(str(row.get("changed_at") or last_synced_at) for row in rows)
    _save_sync_cursor(db, GRAPH_SYNC_KEYS["battle"], latest_synced_at)
    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=len(rows),
        rows_written=len(rows),
        nodes_merged=len(rows) * 4,
        relationships_merged=len(rows) * 6,
        last_synced_at=latest_synced_at,
    )
    _write_graph_log(config.log_file, "graph.sync.battle.completed", result)
    return result


def run_compute_graph_derived_relationships(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "compute_graph_derived_relationships"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return _job_payload(job_name, started, "skipped", error_text="neo4j disabled")

    client = Neo4jClient(config)

    # clear and rebuild in bounded windows
    client.query("MATCH ()-[r:CO_OCCURS_WITH]-() DELETE r")
    client.query("MATCH ()-[r:SHARES_ITEM_WITH]-() DELETE r")
    client.query("MATCH ()-[r:ASSOCIATED_WITH_ANOMALY]-() DELETE r")
    client.query("MATCH ()-[r:CROSSED_SIDES]-() DELETE r")
    client.query("MATCH ()-[r:USES_CRITICAL_ITEM]-() DELETE r")

    client.query(
        """
        MATCH (c1:Character)-[:PARTICIPATED_IN]->(b:Battle)<-[:PARTICIPATED_IN]-(c2:Character)
        WHERE c1.character_id < c2.character_id
          AND datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z')) >= datetime() - duration({days:$window_days})
        OPTIONAL MATCH (c1)-[:ON_SIDE]->(s1:BattleSide)<-[:HAS_SIDE]-(b)
        OPTIONAL MATCH (c2)-[:ON_SIDE]->(s2:BattleSide)<-[:HAS_SIDE]-(b)
        WITH c1, c2,
             min(datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z'))) AS first_seen,
             max(datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z'))) AS last_seen,
             count(DISTINCT b) AS occurrence_count,
             count(DISTINCT CASE WHEN datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z')) >= datetime() - duration({days:7}) THEN b END) AS recent_occurrence_count,
             count(DISTINCT CASE WHEN s1.anomaly_class = 'high_sustain' OR s2.anomaly_class = 'high_sustain' THEN b END) AS high_sustain_battle_count
        WHERE occurrence_count >= $co_threshold
        MERGE (c1)-[r:CO_OCCURS_WITH]->(c2)
          SET r.first_seen = toString(first_seen),
              r.last_seen = toString(last_seen),
              r.occurrence_count = toInteger(occurrence_count),
              r.recent_occurrence_count = toInteger(recent_occurrence_count),
              r.high_sustain_battle_count = toInteger(high_sustain_battle_count),
              r.recent_weight = toFloat(recent_occurrence_count * 2 + high_sustain_battle_count),
              r.all_time_weight = toFloat(occurrence_count + high_sustain_battle_count * 2),
              r.weight = toFloat((recent_occurrence_count * 2) + occurrence_count + (high_sustain_battle_count * 2))
        """,
        {"window_days": RELATIONSHIP_WINDOW_DAYS, "co_threshold": CO_OCCUR_THRESHOLD},
    )

    # top-K co-occurrence retention per character
    client.query(
        """
        MATCH (c:Character)-[r:CO_OCCURS_WITH]->(:Character)
        WITH c, r ORDER BY r.weight DESC
        WITH c, collect(r) AS rels
        FOREACH(rel IN rels[$top_k..] | DELETE rel)
        """,
        {"top_k": TOP_K_CHARACTER_EDGES},
    )

    client.query(
        """
        MATCH (f1:Fit)-[:CONTAINS]->(i:Item)<-[:CONTAINS]-(f2:Fit)
        WHERE f1.fit_id < f2.fit_id
        WITH f1, f2, count(DISTINCT i) AS shared_item_count
        OPTIONAL MATCH (f1)-[:CONTAINS]->(i1:Item)
        WITH f1, f2, shared_item_count, count(DISTINCT i1) AS f1_item_count
        OPTIONAL MATCH (f2)-[:CONTAINS]->(i2:Item)
        WITH f1, f2, shared_item_count, f1_item_count, count(DISTINCT i2) AS f2_item_count
        WITH f1, f2, shared_item_count,
             toFloat(shared_item_count) / CASE WHEN (f1_item_count + f2_item_count - shared_item_count) = 0 THEN 1 ELSE (f1_item_count + f2_item_count - shared_item_count) END AS overlap_score
        WHERE shared_item_count >= $fit_threshold
        MERGE (f1)-[r:SHARES_ITEM_WITH]->(f2)
          SET r.shared_item_count = toInteger(shared_item_count),
              r.occurrence_count = toInteger(shared_item_count),
              r.first_seen = toString(datetime()),
              r.last_seen = toString(datetime()),
              r.recent_occurrence_count = toInteger(shared_item_count),
              r.recent_weight = toFloat(overlap_score * 2.0),
              r.all_time_weight = toFloat(overlap_score),
              r.overlap_score = toFloat(overlap_score),
              r.weight = toFloat(overlap_score * 100.0)
        """,
        {"fit_threshold": SHARED_ITEM_THRESHOLD},
    )
    client.query(
        """
        MATCH (f:Fit)-[r:SHARES_ITEM_WITH]->(:Fit)
        WITH f, r ORDER BY r.weight DESC
        WITH f, collect(r) AS rels
        FOREACH(rel IN rels[$top_k..] | DELETE rel)
        """,
        {"top_k": TOP_K_FIT_EDGES},
    )

    client.query(
        """
        MATCH (c:Character)-[:ON_SIDE]->(s:BattleSide)<-[:HAS_SIDE]-(b:Battle)
        WHERE s.anomaly_class IN ['high_sustain', 'low_sustain']
          AND datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z')) >= datetime() - duration({days:$window_days})
        WITH c, b,
             min(datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z'))) AS first_seen,
             max(datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z'))) AS last_seen,
             count(*) AS occurrence_count,
             avg(toFloat(s.z_efficiency_score)) AS avg_z_score,
             count(CASE WHEN datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z')) >= datetime() - duration({days:7}) THEN 1 END) AS recent_occurrence_count
        WHERE occurrence_count >= $anom_threshold
        MERGE (c)-[r:ASSOCIATED_WITH_ANOMALY]->(b)
          SET r.first_seen = toString(first_seen),
              r.last_seen = toString(last_seen),
              r.occurrence_count = toInteger(occurrence_count),
              r.recent_occurrence_count = toInteger(recent_occurrence_count),
              r.avg_z_score = toFloat(avg_z_score),
              r.recent_weight = toFloat(recent_occurrence_count + avg_z_score),
              r.all_time_weight = toFloat(occurrence_count + avg_z_score),
              r.count = toInteger(occurrence_count)
        """,
        {"window_days": RELATIONSHIP_WINDOW_DAYS, "anom_threshold": ANOMALY_ASSOC_THRESHOLD},
    )

    client.query(
        """
        MATCH (c:Character)-[:ON_SIDE]->(s:BattleSide)<-[:HAS_SIDE]-(b:Battle)
        WHERE datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z')) >= datetime() - duration({days:$window_days})
        WITH c,
             collect(DISTINCT s.side_key) AS side_keys,
             min(datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z'))) AS first_seen,
             max(datetime(COALESCE(b.started_at, '1970-01-01T00:00:00Z'))) AS last_seen,
             count(DISTINCT b) AS occurrence_count
        WHERE size(side_keys) > 1
        MERGE (c)-[r:CROSSED_SIDES]->(c)
          SET r.first_seen = toString(first_seen),
              r.last_seen = toString(last_seen),
              r.occurrence_count = toInteger(occurrence_count),
              r.recent_occurrence_count = toInteger(occurrence_count),
              r.side_count = size(side_keys),
              r.side_transition_count = size(side_keys) - 1,
              r.recent_weight = toFloat(size(side_keys)),
              r.all_time_weight = toFloat(size(side_keys) + occurrence_count)
        """,
        {"window_days": RELATIONSHIP_WINDOW_DAYS},
    )

    client.query(
        """
        MATCH (d:Doctrine)-[:USES]->(f:Fit)-[:CONTAINS]->(i:Item)
        WITH i, count(DISTINCT d) AS doctrine_count, count(DISTINCT f) AS fit_count
        WHERE doctrine_count >= 2 OR fit_count >= 3
        MATCH (f2:Fit)-[:CONTAINS]->(i)
        MERGE (f2)-[r:USES_CRITICAL_ITEM]->(i)
          SET r.criticality_score = toFloat(doctrine_count * 2 + fit_count),
              r.doctrine_count = toInteger(doctrine_count),
              r.fit_count = toInteger(fit_count),
              r.last_seen = toString(datetime())
        """
    )

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=1,
        rows_written=1,
        relationships_created=1,
    )
    _write_graph_log(config.log_file, "graph.derived.completed", result)
    return result


def run_compute_graph_prune(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "compute_graph_prune"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return _job_payload(job_name, started, "skipped", error_text="neo4j disabled")

    client = Neo4jClient(config)
    deleted_counts = []
    for rel_type, threshold in (
        ("CO_OCCURS_WITH", CO_OCCUR_THRESHOLD),
        ("SHARES_ITEM_WITH", SHARED_ITEM_THRESHOLD),
        ("ASSOCIATED_WITH_ANOMALY", ANOMALY_ASSOC_THRESHOLD),
    ):
        rows = client.query(
            f"""
            MATCH ()-[r:{rel_type}]-()
            WHERE COALESCE(r.occurrence_count,0) < $threshold
               OR (r.last_seen IS NOT NULL AND datetime(r.last_seen) < datetime() - duration({{days:$stale_days}}))
            WITH r LIMIT 200000
            DELETE r
            RETURN count(*) AS deleted
            """,
            {"threshold": threshold, "stale_days": STALE_DAYS},
        )
        deleted_counts.append(int((rows[0] if rows else {}).get("deleted") or 0))

    health = _snapshot_graph_health(client)
    warnings: list[str] = []
    max_character_degree = float((health.get("degree") or {}).get("max_character_degree") or 0)
    if max_character_degree > 5000:
        warnings.append("character degree is above expected threshold")

    db.execute(
        """
        INSERT INTO graph_health_snapshots (
            snapshot_ts, labels_json, relationships_json, avg_character_degree, max_character_degree,
            avg_fit_degree, max_fit_degree, notes
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            _utc_now_sql(),
            json_dumps_safe(health.get("labels") or []),
            json_dumps_safe(health.get("relationships") or []),
            float((health.get("degree") or {}).get("avg_character_degree") or 0.0),
            int((health.get("degree") or {}).get("max_character_degree") or 0),
            float((health.get("fit_degree") or {}).get("avg_fit_degree") or 0.0),
            int((health.get("fit_degree") or {}).get("max_fit_degree") or 0),
            "; ".join(warnings),
        ),
    )

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=sum(deleted_counts),
        rows_written=1,
        relationships_created=0,
        relationships_merged=0,
        deleted_relationships=sum(deleted_counts),
        warnings=warnings,
    )
    _write_graph_log(config.log_file, "graph.prune.completed", result)
    return result


def _upsert_table(db: SupplyCoreDb, table_name: str, columns: str, placeholders: str, rows: list[tuple[Any, ...]]) -> None:
    with db.transaction() as (_, cursor):
        cursor.execute(f"DELETE FROM {table_name}")
        if rows:
            cursor.executemany(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", rows)


def run_compute_graph_topology_metrics(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "compute_graph_topology_metrics"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return _job_payload(job_name, started, "skipped", error_text="neo4j disabled")

    client = Neo4jClient(config)
    computed_at = _utc_now_sql()

    character_rows = client.query(
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
        RETURN
            toInteger(c.character_id) AS character_id,
            toFloat(degree_count) AS pagerank_score,
            toFloat(avg_co_weight) AS bridge_score,
            toInteger(side_transitions) AS community_id,
            toFloat(anomalous_neighbor_density) AS anomalous_neighbor_density,
            toFloat(degree_count) AS recurrence_centrality,
            toFloat(avg_co_weight) AS co_occurrence_density,
            toFloat(avg_co_weight * (1 + side_transitions)) AS bridge_between_clusters_score,
            toFloat(side_transitions) AS cross_side_cluster_score,
            toFloat(avg_co_weight * CASE WHEN anomalous_neighbor_density > 0 THEN anomalous_neighbor_density ELSE 1 END) AS suspicious_cluster_density
        """
    )

    battle_rows = client.query(
        """
        MATCH (b:Battle)-[:HAS_SIDE]->(s:BattleSide)<-[:ON_SIDE]-(c:Character)
        OPTIONAL MATCH (c)-[co:CO_OCCURS_WITH]-(:Character)
        WITH b, s,
             count(DISTINCT c) AS participant_count,
             avg(COALESCE(co.weight, 0.0)) AS co_density,
             avg(CASE WHEN s.anomaly_class = 'high_sustain' THEN COALESCE(co.weight, 0.0) ELSE 0.0 END) AS anomaly_neighbor_density
        RETURN
            b.battle_id AS battle_id,
            s.side_key AS side_key,
            toInteger(participant_count) AS participant_count,
            toFloat(co_density) AS co_occurrence_density,
            toFloat(anomaly_neighbor_density) AS anomalous_co_occurrence_density,
            toFloat(CASE WHEN s.anomaly_class = 'high_sustain' THEN 1.0 ELSE 0.0 END) AS cross_side_cluster_score,
            toFloat(co_density * (1 + anomaly_neighbor_density)) AS bridge_score
        """
    )

    _upsert_table(
        db,
        "character_graph_intelligence",
        "character_id, co_occurrence_density, anomalous_co_occurrence_density, cross_side_cluster_score, neighbor_anomaly_score, anomalous_neighbor_density, recurrence_centrality, bridge_score, pagerank_score, community_id, suspicious_cluster_density, bridge_between_clusters_score, computed_at",
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
        [
            (
                int(r["character_id"]),
                float(r["co_occurrence_density"]),
                0.0,
                float(r["cross_side_cluster_score"]),
                float(r["anomalous_neighbor_density"]),
                float(r["anomalous_neighbor_density"]),
                float(r["recurrence_centrality"]),
                float(r["bridge_score"]),
                float(r["pagerank_score"]),
                int(r["community_id"]),
                float(r["suspicious_cluster_density"]),
                float(r["bridge_between_clusters_score"]),
                computed_at,
            )
            for r in character_rows
            if int(r.get("character_id") or 0) > 0
        ],
    )


    cluster_rollup: dict[int, dict[str, Any]] = {}
    for r in character_rows:
        cid = int(r.get("character_id") or 0)
        if cid <= 0:
            continue
        community_id = int(r.get("community_id") or 0)
        bucket = cluster_rollup.setdefault(
            community_id,
            {
                "member_count": 0,
                "suspicious_cluster_density": 0.0,
                "bridge_between_clusters_score": 0.0,
                "members": [],
            },
        )
        bucket["member_count"] += 1
        bucket["suspicious_cluster_density"] += float(r.get("suspicious_cluster_density") or 0.0)
        bucket["bridge_between_clusters_score"] += float(r.get("bridge_between_clusters_score") or 0.0)
        bucket["members"].append({"character_id": cid, "bridge_score": float(r.get("bridge_score") or 0.0)})

    cluster_rows: list[tuple[Any, ...]] = []
    membership_rows: list[tuple[Any, ...]] = []
    for community_id, bucket in cluster_rollup.items():
        members = list(bucket["members"])
        member_count = int(bucket["member_count"])
        density = float(bucket["suspicious_cluster_density"]) / max(member_count, 1)
        bridge = float(bucket["bridge_between_clusters_score"]) / max(member_count, 1)
        cluster_rows.append((
            int(community_id),
            density,
            density,
            bridge,
            member_count,
            json.dumps([], separators=(",", ":"), ensure_ascii=False),
            computed_at,
        ))
        for member in members:
            membership_rows.append((int(community_id), int(member["character_id"]), float(member["bridge_score"]), computed_at))

    _upsert_table(
        db,
        "suspicious_actor_clusters",
        "cluster_id, suspicious_cluster_density, anomalous_group_recurrence, bridge_between_clusters_score, member_count, supporting_battles_json, computed_at",
        "%s, %s, %s, %s, %s, %s, %s",
        cluster_rows,
    )

    _upsert_table(
        db,
        "suspicious_cluster_membership",
        "cluster_id, character_id, bridge_score, computed_at",
        "%s, %s, %s, %s",
        membership_rows,
    )

    _upsert_table(
        db,
        "battle_actor_graph_metrics",
        "battle_id, side_key, participant_count, co_occurrence_density, anomalous_co_occurrence_density, anomalous_neighbor_density, cross_side_cluster_score, bridge_score, computed_at",
        "%s, %s, %s, %s, %s, %s, %s, %s, %s",
        [
            (
                str(r["battle_id"]),
                str(r["side_key"]),
                int(r["participant_count"]),
                float(r["co_occurrence_density"]),
                float(r["anomalous_co_occurrence_density"]),
                float(r["anomalous_co_occurrence_density"]),
                float(r["cross_side_cluster_score"]),
                float(r["bridge_score"]),
                computed_at,
            )
            for r in battle_rows
            if str(r.get("battle_id") or "")
        ],
    )

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=len(character_rows) + len(battle_rows),
        rows_written=len(character_rows) + len(battle_rows),
    )
    _write_graph_log(config.log_file, "graph.topology.completed", result)
    return result


def run_compute_graph_insights(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "compute_graph_insights"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return _job_payload(job_name, started, "skipped", error_text="neo4j disabled")

    client = Neo4jClient(config)
    computed_at = _utc_now_sql()

    item_rows = client.query(
        """
        MATCH (d:Doctrine)-[:USES]->(f:Fit)-[:CONTAINS]->(i:Item)
        WITH i, count(DISTINCT d) AS doctrine_count, count(DISTINCT f) AS fit_count
        RETURN toInteger(i.type_id) AS type_id,
               toInteger(doctrine_count) AS doctrine_count,
               toInteger(fit_count) AS fit_count,
               toFloat((doctrine_count * 10) + fit_count) AS dependency_score
        ORDER BY dependency_score DESC, type_id ASC
        """
    )

    doctrine_rows = client.query(
        """
        MATCH (d:Doctrine)
        OPTIONAL MATCH (d)-[:USES]->(f:Fit)
        OPTIONAL MATCH (f)-[:CONTAINS]->(i:Item)
        WITH d, count(DISTINCT f) AS fit_count, count(i) AS item_count, count(DISTINCT i) AS unique_item_count
        RETURN toInteger(d.doctrine_id) AS doctrine_id,
               toInteger(fit_count) AS fit_count,
               toInteger(item_count) AS item_count,
               toInteger(unique_item_count) AS unique_item_count,
               toFloat(unique_item_count + (fit_count * 5)) AS dependency_depth_score
        ORDER BY dependency_depth_score DESC, doctrine_id ASC
        """
    )

    fit_overlap_rows = client.query(
        """
        MATCH (f1:Fit)-[r:SHARES_ITEM_WITH]->(f2:Fit)
        RETURN toInteger(f1.fit_id) AS fit_id,
               toInteger(f2.fit_id) AS other_fit_id,
               toInteger(r.shared_item_count) AS shared_item_count,
               toFloat(r.overlap_score) AS overlap_score
        ORDER BY overlap_score DESC, shared_item_count DESC
        LIMIT 50000
        """
    )

    _upsert_table(
        db,
        "item_dependency_score",
        "type_id, doctrine_count, fit_count, dependency_score, computed_at",
        "%s, %s, %s, %s, %s",
        [(int(r["type_id"]), int(r["doctrine_count"]), int(r["fit_count"]), float(r["dependency_score"]), computed_at) for r in item_rows if int(r.get("type_id") or 0) > 0],
    )

    _upsert_table(
        db,
        "doctrine_dependency_depth",
        "doctrine_id, fit_count, item_count, unique_item_count, dependency_depth_score, computed_at",
        "%s, %s, %s, %s, %s, %s",
        [
            (int(r["doctrine_id"]), int(r["fit_count"]), int(r["item_count"]), int(r["unique_item_count"]), float(r["dependency_depth_score"]), computed_at)
            for r in doctrine_rows
            if int(r.get("doctrine_id") or 0) > 0
        ],
    )

    _upsert_table(
        db,
        "fit_overlap_score",
        "fit_id, other_fit_id, shared_item_count, overlap_score, computed_at",
        "%s, %s, %s, %s, %s",
        [
            (int(r["fit_id"]), int(r["other_fit_id"]), int(r["shared_item_count"]), float(r["overlap_score"]), computed_at)
            for r in fit_overlap_rows
            if int(r.get("fit_id") or 0) > 0 and int(r.get("other_fit_id") or 0) > 0
        ],
    )

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=len(item_rows) + len(doctrine_rows) + len(fit_overlap_rows),
        rows_written=len(item_rows) + len(doctrine_rows) + len(fit_overlap_rows),
        computed_at=computed_at,
    )
    _write_graph_log(config.log_file, "graph.insights.completed", result)
    return result


def run_graph_model_audit(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return {"enabled": False, "reason": "neo4j disabled"}

    client = Neo4jClient(config)
    summary: dict[str, Any] = {"enabled": True}
    try:
        summary["labels"] = [row.get("label") for row in client.query("CALL db.labels() YIELD label RETURN label ORDER BY label")]
        summary["relationship_types"] = [
            row.get("relationshipType") for row in client.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType")
        ]
        summary["constraints"] = client.query("SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties RETURN name, type, entityType, labelsOrTypes, properties")
        summary["indexes"] = client.query("SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties RETURN name, type, entityType, labelsOrTypes, properties")
    except Neo4jError as exc:
        summary["error_text"] = str(exc)
    return summary
