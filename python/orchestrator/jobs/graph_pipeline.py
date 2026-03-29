from __future__ import annotations

from datetime import UTC, datetime, timedelta
import math
import time
from pathlib import Path
from typing import Any

import logging

from ..db import SupplyCoreDb
from ..eve_constants import FLEET_FUNCTION_BY_GROUP, SHIP_SIZE_BY_GROUP
from ..influx import InfluxClient, InfluxConfig, InfluxQueryError
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

logger = logging.getLogger(__name__)

GRAPH_BATCH_STATE_KEYS = {
    "doctrine_cursor": "graph_sync_doctrine_dependency_cursor",
    "battle_cursor": "graph_sync_battle_intelligence_cursor",
    "derived_character_cursor": "graph_derived_relationships_character_cursor",
    "derived_fit_cursor": "graph_derived_relationships_fit_cursor",
}

# Graph-density controls.
CO_OCCUR_THRESHOLD = 2
SHARED_ITEM_THRESHOLD = 2
ANOMALY_ASSOC_THRESHOLD = 1
TOP_K_CHARACTER_EDGES = 100
TOP_K_FIT_EDGES = 100
RELATIONSHIP_WINDOW_DAYS = 30
STALE_DAYS = 45
DEFAULT_BATCH_SIZE = 1000
SYNC_DATASET_GRAPH_INSIGHTS_FIT_OVERLAP = "graph_insights_fit_overlap"


def _utc_cutoff_iso(days: int) -> str:
    return (datetime.now(UTC) - timedelta(days=days)).isoformat()


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


def _coerce_batch_size(value: Any, *, fallback: int = DEFAULT_BATCH_SIZE) -> int:
    try:
        return max(100, min(5000, int(value)))
    except (TypeError, ValueError):
        return fallback


def _sync_state_get(db: SupplyCoreDb, dataset_key: str) -> dict[str, Any] | None:
    return db.fetch_one(
        "SELECT dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_error_message FROM sync_state WHERE dataset_key = %s LIMIT 1",
        (dataset_key,),
    )


def _sync_state_upsert(
    db: SupplyCoreDb,
    dataset_key: str,
    *,
    sync_mode: str,
    status: str,
    last_success_at: str | None,
    last_cursor: str | None,
    last_row_count: int,
    last_error_message: str | None,
) -> None:
    db.execute(
        """
        INSERT INTO sync_state (
            dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            sync_mode = VALUES(sync_mode),
            status = VALUES(status),
            last_success_at = VALUES(last_success_at),
            last_cursor = VALUES(last_cursor),
            last_row_count = VALUES(last_row_count),
            last_error_message = VALUES(last_error_message),
            updated_at = CURRENT_TIMESTAMP
        """,
        (dataset_key, sync_mode, status, last_success_at, last_cursor, max(0, int(last_row_count)), last_error_message),
    )


def _parse_pair_cursor(raw_cursor: str | None) -> tuple[int, int]:
    text = str(raw_cursor or "").strip()
    if text == "":
        return 0, 0
    left, _, right = text.partition(":")
    try:
        return max(0, int(left)), max(0, int(right))
    except ValueError:
        return 0, 0


def _format_pair_cursor(fit_id: int, other_fit_id: int) -> str:
    return f"{max(0, int(fit_id))}:{max(0, int(other_fit_id))}"


def _parse_int_cursor(raw_cursor: str | None) -> int:
    try:
        return max(0, int(str(raw_cursor or "").strip()))
    except ValueError:
        return 0


def _format_int_cursor(cursor: int) -> str:
    return str(max(0, int(cursor)))


def _parse_battle_participant_cursor(raw_cursor: str | None) -> tuple[str, int]:
    text = str(raw_cursor or "").strip()
    if text == "":
        return "", 0
    battle_id, separator, character_id_raw = text.partition("|")
    if separator == "" or battle_id.strip() == "":
        return "", 0
    try:
        return battle_id.strip(), max(0, int(character_id_raw))
    except ValueError:
        return "", 0


def _format_battle_participant_cursor(battle_id: str, character_id: int) -> str:
    battle_id_value = str(battle_id or "").strip()
    if battle_id_value == "":
        return ""
    return f"{battle_id_value}|{max(0, int(character_id))}"


def _emit_batch_telemetry(log_file: str, job_name: str, payload: dict[str, Any]) -> None:
    _write_graph_log(
        log_file,
        f"{job_name}.batch",
        {
            "job_name": job_name,
            "batch_start": str(payload.get("batch_start") or ""),
            "batch_end": str(payload.get("batch_end") or ""),
            "rows_processed": int(payload.get("rows_processed") or 0),
            "rows_written": int(payload.get("rows_written") or 0),
            "duration_ms": int(payload.get("duration_ms") or 0),
            "checkpoint_after": str(payload.get("checkpoint_after") or ""),
            "errors": str(payload.get("errors") or ""),
        },
    )


def _job_payload(job_name: str, started_at: float, status: str, **kwargs: Any) -> dict[str, Any]:
    duration_ms = int((time.perf_counter() - started_at) * 1000)
    rows_processed = int(kwargs.pop("rows_processed", 0))
    rows_written = int(kwargs.pop("rows_written", 0))
    error_text = str(kwargs.pop("error_text", "")) or None

    graph_meta = {
        "nodes_created": int(kwargs.pop("nodes_created", 0)),
        "nodes_merged": int(kwargs.pop("nodes_merged", 0)),
        "relationships_created": int(kwargs.pop("relationships_created", 0)),
        "relationships_merged": int(kwargs.pop("relationships_merged", 0)),
        "timestamp": datetime.now(UTC).isoformat(),
    }
    graph_meta.update(kwargs)

    if status == "skipped":
        return JobResult.skipped(
            job_key=job_name,
            reason=error_text or "skipped",
            meta=graph_meta,
        ).to_dict()

    if status == "failed":
        return JobResult.failed(
            job_key=job_name,
            error=error_text or "unknown error",
            duration_ms=duration_ms,
            meta=graph_meta,
        ).to_dict()

    return JobResult.success(
        job_key=job_name,
        summary=f"{job_name} completed successfully.",
        rows_processed=rows_processed,
        rows_written=rows_written,
        rows_seen=rows_processed,
        duration_ms=duration_ms,
        meta=graph_meta,
    ).to_dict()


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
    client.query("CREATE CONSTRAINT killmail_killmail_id IF NOT EXISTS FOR (n:Killmail) REQUIRE n.killmail_id IS UNIQUE")

    client.query("CREATE INDEX character_lookup IF NOT EXISTS FOR (n:Character) ON (n.character_id)")
    client.query("CREATE INDEX battle_lookup IF NOT EXISTS FOR (n:Battle) ON (n.battle_id)")
    client.query("CREATE INDEX battle_started_lookup IF NOT EXISTS FOR (n:Battle) ON (n.started_at)")
    client.query("CREATE INDEX doctrine_lookup IF NOT EXISTS FOR (n:Doctrine) ON (n.doctrine_id)")
    client.query("CREATE INDEX fit_lookup IF NOT EXISTS FOR (n:Fit) ON (n.fit_id)")
    client.query("CREATE INDEX item_lookup IF NOT EXISTS FOR (n:Item) ON (n.type_id)")
    client.query("CREATE INDEX side_key_lookup IF NOT EXISTS FOR (n:BattleSide) ON (n.side_key)")


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

    runtime = neo4j_raw or {}
    batch_size = _coerce_batch_size(runtime.get("sync_doctrine_batch_size") or runtime.get("batch_size"))
    max_batches = max(1, int(runtime.get("sync_doctrine_max_batches_per_run") or 6))
    cursor_row = _sync_state_get(db, GRAPH_BATCH_STATE_KEYS["doctrine_cursor"]) or {}
    cursor_id = _parse_int_cursor(cursor_row.get("last_cursor"))
    rows_processed = 0
    rows_written = 0
    batch_count = 0
    latest_checkpoint = cursor_id
    while batch_count < max_batches:
        batch_started = time.perf_counter()
        rows = db.fetch_all(
            """
            SELECT
                dfi.id AS doctrine_item_id,
                dg.id AS doctrine_id,
                dg.group_name AS doctrine_name,
                df.id AS fit_id,
                df.fit_name,
                dfi.type_id,
                COALESCE(rit.type_name, CONCAT('Type #', dfi.type_id)) AS item_name,
                GREATEST(COALESCE(df.updated_at, '1970-01-01 00:00:00'), COALESCE(dfi.updated_at, '1970-01-01 00:00:00')) AS changed_at
            FROM doctrine_fit_items dfi
            INNER JOIN doctrine_fits df ON df.id = dfi.doctrine_fit_id
            INNER JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
            INNER JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
            LEFT JOIN ref_item_types rit ON rit.type_id = dfi.type_id
            WHERE dfi.id > %s
            ORDER BY dfi.id ASC
            LIMIT %s
            """,
            (cursor_id, batch_size),
        )
        if not rows:
            cursor_id = 0
            break

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

        cursor_id = int(rows[-1].get("doctrine_item_id") or cursor_id)
        latest_checkpoint = cursor_id
        batch_count += 1
        rows_processed += len(rows)
        rows_written += len(rows)
        _sync_state_upsert(
            db,
            GRAPH_BATCH_STATE_KEYS["doctrine_cursor"],
            sync_mode="incremental",
            status="running",
            last_success_at=None,
            last_cursor=_format_int_cursor(cursor_id),
            last_row_count=rows_written,
            last_error_message=None,
        )
        _emit_batch_telemetry(
            config.log_file,
            job_name,
            {
                "batch_start": batch_count,
                "batch_end": batch_count,
                "rows_processed": len(rows),
                "rows_written": len(rows),
                "duration_ms": int((time.perf_counter() - batch_started) * 1000),
                "checkpoint_after": _format_int_cursor(cursor_id),
                "errors": "",
            },
        )

    _sync_state_upsert(
        db,
        GRAPH_BATCH_STATE_KEYS["doctrine_cursor"],
        sync_mode="incremental",
        status="success",
        last_success_at=_utc_now_sql(),
        last_cursor=_format_int_cursor(cursor_id),
        last_row_count=rows_written,
        last_error_message=None,
    )

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=rows_processed,
        rows_written=rows_written,
        nodes_merged=rows_written * 3,
        relationships_merged=rows_written * 2,
        checkpoint_cursor=_format_int_cursor(latest_checkpoint),
        batch_count=batch_count,
        batch_size=batch_size,
        has_more=cursor_id > 0,
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
    runtime = neo4j_raw or {}
    batch_size = _coerce_batch_size(runtime.get("sync_battle_batch_size") or runtime.get("batch_size"), fallback=800)
    max_batches = max(1, int(runtime.get("sync_battle_max_batches_per_run") or 6))
    cursor_row = _sync_state_get(db, GRAPH_BATCH_STATE_KEYS["battle_cursor"]) or {}
    participant_has_id = _table_has_column(db, "battle_participants", "id")
    cursor_id = _parse_int_cursor(cursor_row.get("last_cursor")) if participant_has_id else 0
    cursor_battle_id, cursor_character_id = _parse_battle_participant_cursor(cursor_row.get("last_cursor"))
    rows_processed = 0
    rows_written = 0
    batch_count = 0
    latest_checkpoint = _format_int_cursor(cursor_id) if participant_has_id else _format_battle_participant_cursor(cursor_battle_id, cursor_character_id)
    while batch_count < max_batches:
        batch_started = time.perf_counter()
        if participant_has_id:
            rows = db.fetch_all(
                """
                SELECT
                    bp.id AS participant_row_id,
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
                    rit.group_id AS ship_group_id
                FROM battle_participants bp
                INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
                LEFT JOIN battle_anomalies ba ON ba.battle_id = bp.battle_id AND ba.side_key = bp.side_key
                LEFT JOIN ref_item_types rit ON rit.type_id = bp.ship_type_id
                WHERE bp.id > %s
                ORDER BY bp.id ASC
                LIMIT %s
                """,
                (cursor_id, batch_size),
            )
        else:
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
                    rit.group_id AS ship_group_id
                FROM battle_participants bp
                INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
                LEFT JOIN battle_anomalies ba ON ba.battle_id = bp.battle_id AND ba.side_key = bp.side_key
                LEFT JOIN ref_item_types rit ON rit.type_id = bp.ship_type_id
                WHERE (bp.battle_id > %s)
                   OR (bp.battle_id = %s AND bp.character_id > %s)
                ORDER BY bp.battle_id ASC, bp.character_id ASC
                LIMIT %s
                """,
                (cursor_battle_id, cursor_battle_id, cursor_character_id, batch_size),
            )
        if not rows:
            if participant_has_id:
                cursor_id = 0
            else:
                cursor_battle_id = ""
                cursor_character_id = 0
            break

        # Enrich rows with fleet function and ship size from group_id
        for row in rows:
            gid = int(row.get("ship_group_id") or 0)
            row["fleet_function"] = FLEET_FUNCTION_BY_GROUP.get(gid, "mainline_dps")
            row["ship_size"] = SHIP_SIZE_BY_GROUP.get(gid, "medium")

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
            MERGE (c:Character {character_id: row.character_id})
            MERGE (c)-[:PARTICIPATED_IN]->(b)
            MERGE (c)-[:ON_SIDE]->(side)

            FOREACH(_ IN CASE WHEN toInteger(COALESCE(row.ship_type_id, 0)) > 0 THEN [1] ELSE [] END |
                MERGE (ship:ShipType {type_id: toInteger(row.ship_type_id)})
                  SET ship.fleet_function = row.fleet_function,
                      ship.ship_size = row.ship_size
                MERGE (c)-[:USED_SHIP]->(ship)
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
        client.query(
            """
            UNWIND $rows AS row
            WITH row
            WHERE toInteger(COALESCE(row.ship_type_id, 0)) > 0
            MATCH (c:Character {character_id: row.character_id})
            MATCH (ship:ShipType {type_id: toInteger(row.ship_type_id)})
            OPTIONAL MATCH (c)-[legacy:FLEW]->(ship)
            DELETE legacy
            """,
            {"rows": rows},
        )

        if participant_has_id:
            cursor_id = int(rows[-1].get("participant_row_id") or cursor_id)
            latest_checkpoint = _format_int_cursor(cursor_id)
        else:
            cursor_battle_id = str(rows[-1].get("battle_id") or cursor_battle_id)
            cursor_character_id = int(rows[-1].get("character_id") or cursor_character_id)
            latest_checkpoint = _format_battle_participant_cursor(cursor_battle_id, cursor_character_id)
        batch_count += 1
        rows_processed += len(rows)
        rows_written += len(rows)
        _sync_state_upsert(
            db,
            GRAPH_BATCH_STATE_KEYS["battle_cursor"],
            sync_mode="incremental",
            status="running",
            last_success_at=None,
            last_cursor=latest_checkpoint,
            last_row_count=rows_written,
            last_error_message=None,
        )
        _emit_batch_telemetry(
            config.log_file,
            job_name,
            {
                "batch_start": batch_count,
                "batch_end": batch_count,
                "rows_processed": len(rows),
                "rows_written": len(rows),
                "duration_ms": int((time.perf_counter() - batch_started) * 1000),
                "checkpoint_after": latest_checkpoint,
                "errors": "",
            },
        )

    _sync_state_upsert(
        db,
        GRAPH_BATCH_STATE_KEYS["battle_cursor"],
        sync_mode="incremental",
        status="success",
        last_success_at=_utc_now_sql(),
        last_cursor=latest_checkpoint,
        last_row_count=rows_written,
        last_error_message=None,
    )
    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=rows_processed,
        rows_written=rows_written,
        nodes_merged=rows_written * 4,
        relationships_merged=rows_written * 6,
        checkpoint_cursor=latest_checkpoint,
        batch_count=batch_count,
        batch_size=batch_size,
        has_more=(cursor_id > 0) if participant_has_id else (cursor_battle_id != ""),
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
    runtime = neo4j_raw or {}
    derived_timeout = max(15, int(runtime.get("derived_timeout_seconds") or 120))
    character_batch_size = _coerce_batch_size(runtime.get("derived_character_batch_size") or runtime.get("batch_size"), fallback=50)
    fit_batch_size = _coerce_batch_size(runtime.get("derived_fit_batch_size") or runtime.get("batch_size"), fallback=200)
    max_character_batches = max(1, int(runtime.get("derived_character_max_batches_per_run") or 10))
    max_fit_batches = max(1, int(runtime.get("derived_fit_max_batches_per_run") or 3))
    total_processed = 0
    total_written = 0
    total_relationships = 0
    character_cursor = _parse_int_cursor((_sync_state_get(db, GRAPH_BATCH_STATE_KEYS["derived_character_cursor"]) or {}).get("last_cursor"))
    fit_cursor = _parse_int_cursor((_sync_state_get(db, GRAPH_BATCH_STATE_KEYS["derived_fit_cursor"]) or {}).get("last_cursor"))

    # This job is cursor-based and resumable: each batch advances the cursor,
    # which is persisted to the database.  If a timeout or error occurs mid-batch
    # the cursor from the *previous* successful batch is already saved, so the
    # next run picks up where this one left off without losing progress.
    character_batches = 0
    while character_batches < max_character_batches:
        batch_started = time.perf_counter()
        anchor_rows = client.query(
            """
            MATCH (c:Character)
            WHERE c.character_id > $cursor
            RETURN c.character_id AS character_id
            ORDER BY character_id ASC
            LIMIT $batch_size
            """,
            {"cursor": character_cursor, "batch_size": character_batch_size},
        )
        if not anchor_rows:
            character_cursor = 0
            break
        anchor_ids = [int(row["character_id"]) for row in anchor_rows if int(row.get("character_id") or 0) > 0]
        if not anchor_ids:
            break

        cutoff = _utc_cutoff_iso(RELATIONSHIP_WINDOW_DAYS)
        recent_cutoff = _utc_cutoff_iso(7)

        try:
            client.query(
                """
                UNWIND $anchor_ids AS anchor_id
                MATCH (c1:Character {character_id: anchor_id})
                MATCH (c1)-[:PARTICIPATED_IN]->(b:Battle)<-[:PARTICIPATED_IN]-(c2:Character)
                WHERE c1.character_id < c2.character_id
                  AND b.started_at >= $cutoff
                OPTIONAL MATCH (c1)-[:ON_SIDE]->(s1:BattleSide)<-[:HAS_SIDE]-(b)
                OPTIONAL MATCH (c2)-[:ON_SIDE]->(s2:BattleSide)<-[:HAS_SIDE]-(b)
                WITH c1, c2,
                     min(b.started_at) AS first_seen,
                     max(b.started_at) AS last_seen,
                     count(DISTINCT b) AS occurrence_count,
                     count(DISTINCT CASE WHEN b.started_at >= $recent_cutoff THEN b END) AS recent_occurrence_count,
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
                {"anchor_ids": anchor_ids, "cutoff": cutoff, "recent_cutoff": recent_cutoff, "co_threshold": CO_OCCUR_THRESHOLD},
                timeout_seconds=derived_timeout,
            )
            client.query(
                """
                UNWIND $anchor_ids AS anchor_id
                MATCH (c:Character {character_id: anchor_id})-[r:CO_OCCURS_WITH]->(:Character)
                WITH c, r ORDER BY r.weight DESC
                WITH c, collect(r) AS rels
                FOREACH(rel IN rels[$top_k..] | DELETE rel)
                """,
                {"anchor_ids": anchor_ids, "top_k": TOP_K_CHARACTER_EDGES},
                timeout_seconds=derived_timeout,
            )
            client.query(
                """
                UNWIND $anchor_ids AS anchor_id
                MATCH (c:Character {character_id: anchor_id})-[r:ASSOCIATED_WITH_ANOMALY]->(:Battle)
                WHERE datetime(COALESCE(r.last_seen, '1970-01-01T00:00:00Z')) < datetime() - duration({days:$window_days})
                DELETE r
                """,
                {"anchor_ids": anchor_ids, "window_days": RELATIONSHIP_WINDOW_DAYS},
                timeout_seconds=derived_timeout,
            )
            client.query(
                """
                UNWIND $anchor_ids AS anchor_id
                MATCH (c:Character {character_id: anchor_id})
                OPTIONAL MATCH (c)-[:ON_SIDE]->(s:BattleSide)<-[:HAS_SIDE]-(b:Battle)
                WHERE b.started_at >= $cutoff
                WITH c, collect(DISTINCT s.side_key) AS side_keys,
                     min(b.started_at) AS first_seen,
                     max(b.started_at) AS last_seen,
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
                {"anchor_ids": anchor_ids, "cutoff": cutoff},
                timeout_seconds=derived_timeout,
            )
            client.query(
                """
                UNWIND $anchor_ids AS anchor_id
                MATCH (c:Character {character_id: anchor_id})
                OPTIONAL MATCH (c)-[:ON_SIDE]->(s:BattleSide)<-[:HAS_SIDE]-(b:Battle)
                WHERE b.started_at >= $cutoff
                WITH c, collect(DISTINCT s.side_key) AS side_keys
                WHERE size(side_keys) <= 1
                MATCH (c)-[r:CROSSED_SIDES]->(c)
                DELETE r
                """,
                {"anchor_ids": anchor_ids, "cutoff": cutoff},
                timeout_seconds=derived_timeout,
            )
            client.query(
                """
                UNWIND $anchor_ids AS anchor_id
                MATCH (c:Character {character_id: anchor_id})-[:ON_SIDE]->(s:BattleSide)<-[:HAS_SIDE]-(b:Battle)
                WHERE s.anomaly_class IN ['high_sustain', 'low_sustain']
                  AND b.started_at >= $cutoff
                WITH c, b,
                     min(b.started_at) AS first_seen,
                     max(b.started_at) AS last_seen,
                     count(*) AS occurrence_count,
                     avg(toFloat(s.z_efficiency_score)) AS avg_z_score,
                     count(CASE WHEN b.started_at >= $recent_cutoff THEN 1 END) AS recent_occurrence_count
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
                {"anchor_ids": anchor_ids, "cutoff": cutoff, "recent_cutoff": recent_cutoff, "anom_threshold": ANOMALY_ASSOC_THRESHOLD},
                timeout_seconds=derived_timeout,
            )
        except (Neo4jError, OSError) as exc:
            # Save cursor from last *completed* batch so the next run resumes
            # without re-processing already-finished batches.
            _sync_state_upsert(
                db,
                GRAPH_BATCH_STATE_KEYS["derived_character_cursor"],
                sync_mode="incremental",
                status="error",
                last_success_at=None,
                last_cursor=_format_int_cursor(character_cursor),
                last_row_count=total_written,
                last_error_message=str(exc)[:500],
            )
            raise

        character_cursor = max(anchor_ids)
        character_batches += 1
        total_processed += len(anchor_ids)
        total_written += len(anchor_ids)
        total_relationships += len(anchor_ids)
        _sync_state_upsert(
            db,
            GRAPH_BATCH_STATE_KEYS["derived_character_cursor"],
            sync_mode="incremental",
            status="running",
            last_success_at=None,
            last_cursor=_format_int_cursor(character_cursor),
            last_row_count=total_written,
            last_error_message=None,
        )
        _emit_batch_telemetry(
            config.log_file,
            job_name,
            {
                "batch_start": character_batches,
                "batch_end": character_batches,
                "rows_processed": len(anchor_ids),
                "rows_written": len(anchor_ids),
                "duration_ms": int((time.perf_counter() - batch_started) * 1000),
                "checkpoint_after": f"character:{_format_int_cursor(character_cursor)}",
                "errors": "",
            },
        )

    fit_batches = 0
    while fit_batches < max_fit_batches:
        batch_started = time.perf_counter()
        fit_rows = client.query(
            """
            MATCH (f:Fit)
            WHERE toInteger(f.fit_id) > $cursor
            RETURN toInteger(f.fit_id) AS fit_id
            ORDER BY fit_id ASC
            LIMIT $batch_size
            """,
            {"cursor": fit_cursor, "batch_size": fit_batch_size},
        )
        if not fit_rows:
            fit_cursor = 0
            break
        fit_ids = [int(row["fit_id"]) for row in fit_rows if int(row.get("fit_id") or 0) > 0]
        if not fit_ids:
            break

        client.query(
            """
            UNWIND $fit_ids AS anchor_fit_id
            MATCH (f1:Fit {fit_id: toInteger(anchor_fit_id)})-[:CONTAINS]->(i:Item)<-[:CONTAINS]-(f2:Fit)
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
            {"fit_ids": fit_ids, "fit_threshold": SHARED_ITEM_THRESHOLD},
        )
        client.query(
            """
            UNWIND $fit_ids AS anchor_fit_id
            MATCH (f:Fit {fit_id: toInteger(anchor_fit_id)})-[r:SHARES_ITEM_WITH]->(:Fit)
            WITH f, r ORDER BY r.weight DESC
            WITH f, collect(r) AS rels
            FOREACH(rel IN rels[$top_k..] | DELETE rel)
            """,
            {"fit_ids": fit_ids, "top_k": TOP_K_FIT_EDGES},
        )
        client.query(
            """
            UNWIND $fit_ids AS anchor_fit_id
            MATCH (f_anchor:Fit {fit_id: toInteger(anchor_fit_id)})-[:CONTAINS]->(i:Item)
            OPTIONAL MATCH (f_any:Fit)-[:CONTAINS]->(i)
            WITH f_anchor, i, count(DISTINCT f_any) AS fit_count
            OPTIONAL MATCH (d:Doctrine)-[:USES]->(:Fit)-[:CONTAINS]->(i)
            WITH f_anchor, i, fit_count, count(DISTINCT d) AS doctrine_count
            WHERE doctrine_count >= 2 OR fit_count >= 3
            MERGE (f_anchor)-[r:USES_CRITICAL_ITEM]->(i)
              SET r.criticality_score = toFloat(doctrine_count * 2 + fit_count),
                  r.doctrine_count = toInteger(doctrine_count),
                  r.fit_count = toInteger(fit_count),
                  r.last_seen = toString(datetime())
            """,
            {"fit_ids": fit_ids},
        )
        fit_cursor = max(fit_ids)
        fit_batches += 1
        total_processed += len(fit_ids)
        total_written += len(fit_ids)
        total_relationships += len(fit_ids)
        _sync_state_upsert(
            db,
            GRAPH_BATCH_STATE_KEYS["derived_fit_cursor"],
            sync_mode="incremental",
            status="running",
            last_success_at=None,
            last_cursor=_format_int_cursor(fit_cursor),
            last_row_count=total_written,
            last_error_message=None,
        )
        _emit_batch_telemetry(
            config.log_file,
            job_name,
            {
                "batch_start": character_batches + fit_batches,
                "batch_end": character_batches + fit_batches,
                "rows_processed": len(fit_ids),
                "rows_written": len(fit_ids),
                "duration_ms": int((time.perf_counter() - batch_started) * 1000),
                "checkpoint_after": f"fit:{_format_int_cursor(fit_cursor)}",
                "errors": "",
            },
        )

    _sync_state_upsert(
        db,
        GRAPH_BATCH_STATE_KEYS["derived_character_cursor"],
        sync_mode="incremental",
        status="success",
        last_success_at=_utc_now_sql(),
        last_cursor=_format_int_cursor(character_cursor),
        last_row_count=total_written,
        last_error_message=None,
    )
    _sync_state_upsert(
        db,
        GRAPH_BATCH_STATE_KEYS["derived_fit_cursor"],
        sync_mode="incremental",
        status="success",
        last_success_at=_utc_now_sql(),
        last_cursor=_format_int_cursor(fit_cursor),
        last_row_count=total_written,
        last_error_message=None,
    )

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=total_processed,
        rows_written=total_written,
        relationships_created=total_relationships,
        character_batches=character_batches,
        fit_batches=fit_batches,
        character_batch_size=character_batch_size,
        fit_batch_size=fit_batch_size,
        character_checkpoint=_format_int_cursor(character_cursor),
        fit_checkpoint=_format_int_cursor(fit_cursor),
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
    batch_size = DEFAULT_BATCH_SIZE
    with db.transaction() as (_, cursor):
        cursor.execute(f"DELETE FROM {table_name}")
    for offset in range(0, len(rows), batch_size):
        chunk = rows[offset : offset + batch_size]
        with db.transaction() as (_, cursor):
            cursor.executemany(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})", chunk)


def _ensure_doctrine_dependency_depth_schema(db: SupplyCoreDb) -> None:
    expected_columns: list[tuple[str, str, str]] = [
        ("doctrine_name", "VARCHAR(191) NOT NULL DEFAULT ''", "doctrine_id"),
        ("fit_count", "INT UNSIGNED NOT NULL DEFAULT 0", "doctrine_id"),
        ("item_count", "INT UNSIGNED NOT NULL DEFAULT 0", "fit_count"),
        ("unique_item_count", "INT UNSIGNED NOT NULL DEFAULT 0", "item_count"),
        ("dependency_depth_score", "DECIMAL(12,4) NOT NULL DEFAULT 0.0000", "unique_item_count"),
        ("computed_at", "DATETIME NOT NULL", "dependency_depth_score"),
    ]
    for column_name, definition, after_column in expected_columns:
        row = db.fetch_one(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'doctrine_dependency_depth'
              AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (column_name,),
        )
        if row:
            continue
        db.execute(
            f"ALTER TABLE doctrine_dependency_depth ADD COLUMN {column_name} {definition} AFTER {after_column}"
        )

    for index_name, index_cols in (
        ("idx_doctrine_dependency_depth_score", "dependency_depth_score, computed_at"),
        ("idx_doctrine_dependency_depth_computed", "computed_at"),
    ):
        index_row = db.fetch_one(
            """
            SELECT 1
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = 'doctrine_dependency_depth'
              AND INDEX_NAME = %s
            LIMIT 1
            """,
            (index_name,),
        )
        if index_row:
            continue
        db.execute(
            f"ALTER TABLE doctrine_dependency_depth ADD KEY {index_name} ({index_cols})"
        )


def _table_has_column(db: SupplyCoreDb, table_name: str, column_name: str) -> bool:
    row = db.fetch_one(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = %s
        LIMIT 1
        """,
        (table_name, column_name),
    )
    return row is not None


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
            c.character_id AS character_id,
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

    # ── Engagement avoidance detection ──────────────────────────────────
    # For each character, compare their high-sustain rate when facing each
    # opposing alliance.  A character who consistently has anomalous
    # (high_sustain) battles specifically against one alliance — while
    # fighting normally against all others — is flagged for avoidance.
    cutoff = _utc_cutoff_iso(RELATIONSHIP_WINDOW_DAYS)
    avoidance_rows = client.query(
        """
        MATCH (c:Character)-[:ON_SIDE]->(my_side:BattleSide)<-[:HAS_SIDE]-(b:Battle)-[:HAS_SIDE]->(opp_side:BattleSide)
        WHERE my_side.side_key <> opp_side.side_key
          AND b.started_at >= $cutoff
        MATCH (opp_side)-[:REPRESENTED_BY_ALLIANCE]->(a:Alliance)
        WITH c, a.alliance_id AS opp_alliance,
             collect(DISTINCT b) AS battles,
             avg(CASE WHEN my_side.anomaly_class = 'high_sustain' THEN 1.0 ELSE 0.0 END) AS hs_rate
        WHERE size(battles) >= 2
        WITH c,
             collect({alliance: opp_alliance, encounters: size(battles), hs_rate: hs_rate}) AS per_alliance,
             avg(hs_rate) AS overall_hs_rate,
             count(*) AS alliances_faced
        WHERE alliances_faced >= 2
        WITH c, per_alliance, overall_hs_rate, alliances_faced,
             reduce(mx = 0.0, s IN per_alliance |
                 CASE WHEN s.encounters >= 2 AND (s.hs_rate - overall_hs_rate) > mx
                      THEN s.hs_rate - overall_hs_rate ELSE mx END
             ) AS max_avoidance_delta,
             reduce(mx_enc = 0, s IN per_alliance |
                 CASE WHEN s.encounters > mx_enc THEN s.encounters ELSE mx_enc END
             ) AS max_encounters_single
        RETURN
            c.character_id AS character_id,
            toFloat(max_avoidance_delta) AS engagement_avoidance_score,
            toInteger(alliances_faced) AS alliances_encountered,
            toInteger(max_encounters_single) AS max_encounters_single_alliance
        """,
        {"cutoff": cutoff},
    )
    avoidance_map: dict[int, float] = {}
    for row in avoidance_rows:
        cid = int(row.get("character_id") or 0)
        if cid > 0:
            avoidance_map[cid] = float(row.get("engagement_avoidance_score") or 0.0)

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
        "character_id, co_occurrence_density, anomalous_co_occurrence_density, cross_side_cluster_score, neighbor_anomaly_score, anomalous_neighbor_density, recurrence_centrality, bridge_score, pagerank_score, community_id, suspicious_cluster_density, bridge_between_clusters_score, engagement_avoidance_score, computed_at",
        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
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
                avoidance_map.get(int(r["character_id"]), 0.0),
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
            json_dumps_safe([]),
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


def _compute_substitutability(db: SupplyCoreDb, item_data: dict[int, dict[str, Any]]) -> None:
    """Compute substitutability for each item based on group_id peers in the same fits."""
    type_ids = list(item_data.keys())
    if not type_ids:
        return

    # Load group_id for all tracked items
    placeholders = ",".join(["%s"] * len(type_ids))
    group_rows = db.fetch_all(
        f"SELECT type_id, group_id FROM ref_item_types WHERE type_id IN ({placeholders})",
        tuple(type_ids),
    )
    type_to_group: dict[int, int] = {int(r["type_id"]): int(r["group_id"]) for r in group_rows if r.get("group_id")}
    group_to_types: dict[int, set[int]] = {}
    for tid, gid in type_to_group.items():
        group_to_types.setdefault(gid, set()).add(tid)

    for tid, data in item_data.items():
        gid = type_to_group.get(tid)
        if gid is None:
            data["substitute_count"] = 0
            data["substitutability"] = 0.0
            continue
        peers = group_to_types.get(gid, set())
        # Substitutes are other items in the same group that are also tracked (used in fits)
        subs = peers & set(type_ids) - {tid}
        data["substitute_count"] = len(subs)
        # substitutability: 0 = irreplaceable, 1 = many alternatives
        data["substitutability"] = min(1.0, len(subs) / 5.0) if subs else 0.0


def _compute_spof(item_data: dict[int, dict[str, Any]]) -> None:
    """Flag single-point-of-failure items and compute impact scores."""
    for tid, data in item_data.items():
        dc = data.get("doctrine_count", 0)
        fc = data.get("fit_count", 0)
        sc = data.get("substitute_count", 0)
        # SPOF: used by multiple doctrines but has no/few substitutes
        spof = (sc == 0 and dc >= 2) or (sc <= 1 and dc >= 3)
        data["spof_flag"] = 1 if spof else 0
        data["spof_impact_score"] = float(dc * 10 + fc * 2) if spof else 0.0


def _compute_item_trend_metrics(
    influx_cfg: InfluxConfig | None,
    item_data: dict[int, dict[str, Any]],
) -> None:
    """Query InfluxDB for price/volume/consumption trends and populate item_data."""
    if influx_cfg is None or not influx_cfg.enabled:
        logger.info("InfluxDB disabled — skipping trend metrics")
        return

    try:
        client = InfluxClient(influx_cfg)
    except Exception as exc:
        logger.warning("Failed to create InfluxClient: %s", exc)
        return

    bucket = influx_cfg.bucket

    # --- Price 7d & 30d moving averages ---
    try:
        price_7d_rows = client.query_flux(f"""
from(bucket: "{bucket}")
  |> range(start: -8d)
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "market_hub")
  |> filter(fn: (r) => r._field == "weighted_price")
  |> group(columns: ["type_id"])
  |> mean()
  |> yield(name: "price_7d")
""")
        for row in price_7d_rows:
            tid = int(row.get("type_id") or 0)
            if tid in item_data and row.get("_value") is not None:
                item_data[tid]["price_avg_7d"] = float(row["_value"])
    except InfluxQueryError as exc:
        logger.warning("InfluxDB price 7d query failed: %s", exc)

    try:
        price_30d_rows = client.query_flux(f"""
from(bucket: "{bucket}")
  |> range(start: -31d)
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "market_hub")
  |> filter(fn: (r) => r._field == "weighted_price")
  |> group(columns: ["type_id"])
  |> mean()
  |> yield(name: "price_30d")
""")
        for row in price_30d_rows:
            tid = int(row.get("type_id") or 0)
            if tid in item_data and row.get("_value") is not None:
                item_data[tid]["price_avg_30d"] = float(row["_value"])
    except InfluxQueryError as exc:
        logger.warning("InfluxDB price 30d query failed: %s", exc)

    # --- Price volatility (stddev over 30d) ---
    try:
        vol_rows = client.query_flux(f"""
from(bucket: "{bucket}")
  |> range(start: -31d)
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "market_hub")
  |> filter(fn: (r) => r._field == "weighted_price")
  |> group(columns: ["type_id"])
  |> stddev()
  |> yield(name: "price_stddev")
""")
        for row in vol_rows:
            tid = int(row.get("type_id") or 0)
            if tid in item_data and row.get("_value") is not None:
                avg_30d = item_data[tid].get("price_avg_30d")
                if avg_30d and avg_30d > 0:
                    item_data[tid]["price_volatility"] = float(row["_value"]) / avg_30d
    except InfluxQueryError as exc:
        logger.warning("InfluxDB price volatility query failed: %s", exc)

    # --- Volume (stock) 7d & 30d ---
    try:
        stock_7d_rows = client.query_flux(f"""
from(bucket: "{bucket}")
  |> range(start: -8d)
  |> filter(fn: (r) => r._measurement == "market_item_stock")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r._field == "local_stock_units")
  |> group(columns: ["type_id"])
  |> mean()
  |> yield(name: "stock_7d")
""")
        for row in stock_7d_rows:
            tid = int(row.get("type_id") or 0)
            if tid in item_data and row.get("_value") is not None:
                item_data[tid]["volume_avg_7d"] = float(row["_value"])
    except InfluxQueryError as exc:
        logger.warning("InfluxDB stock 7d query failed: %s", exc)

    try:
        stock_30d_rows = client.query_flux(f"""
from(bucket: "{bucket}")
  |> range(start: -31d)
  |> filter(fn: (r) => r._measurement == "market_item_stock")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r._field == "local_stock_units")
  |> group(columns: ["type_id"])
  |> mean()
  |> yield(name: "stock_30d")
""")
        for row in stock_30d_rows:
            tid = int(row.get("type_id") or 0)
            if tid in item_data and row.get("_value") is not None:
                item_data[tid]["volume_avg_30d"] = float(row["_value"])
    except InfluxQueryError as exc:
        logger.warning("InfluxDB stock 30d query failed: %s", exc)

    # --- Item consumption from killmail losses (30d total) ---
    try:
        consumption_rows = client.query_flux(f"""
from(bucket: "{bucket}")
  |> range(start: -31d)
  |> filter(fn: (r) => r._measurement == "killmail_item_loss")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r._field == "quantity_lost")
  |> group(columns: ["type_id"])
  |> sum()
  |> yield(name: "consumption_30d")
""")
        for row in consumption_rows:
            tid = int(row.get("type_id") or 0)
            if tid in item_data and row.get("_value") is not None:
                item_data[tid]["consumption_30d"] = float(row["_value"])
    except InfluxQueryError as exc:
        logger.warning("InfluxDB consumption query failed: %s", exc)

    # --- Derived trend metrics ---
    for tid, data in item_data.items():
        p7 = data.get("price_avg_7d")
        p30 = data.get("price_avg_30d")
        v7 = data.get("volume_avg_7d")
        v30 = data.get("volume_avg_30d")

        # Price velocity: rate of change
        if p7 is not None and p30 is not None and p30 > 0:
            data["price_velocity"] = (p7 - p30) / p30
        # Volume velocity
        if v7 is not None and v30 is not None and v30 > 0:
            data["volume_velocity"] = (v7 - v30) / v30

        # Trend regime classification
        pv = data.get("price_velocity")
        if pv is not None:
            if abs(pv) < 0.05:
                data["trend_regime"] = "stable"
            elif pv > 0.20:
                data["trend_regime"] = "spike"
            elif pv < -0.20:
                data["trend_regime"] = "crash"
            elif pv > 0.05:
                data["trend_regime"] = "rising"
            else:
                data["trend_regime"] = "falling"

        # Composite trend score (0-100)
        abs_pv = abs(data.get("price_velocity") or 0)
        abs_vv = abs(data.get("volume_velocity") or 0)
        volatility = data.get("price_volatility") or 0
        data["trend_score"] = min(100.0, abs_pv * 40 + abs_vv * 30 + volatility * 30)

        # Data coverage for confidence
        signals_present = sum(1 for k in ("price_avg_7d", "price_avg_30d", "volume_avg_7d", "volume_avg_30d", "price_volatility") if data.get(k) is not None)
        data["coverage_ratio"] = signals_present / 5.0


def _compute_market_stress(
    db: SupplyCoreDb,
    item_data: dict[int, dict[str, Any]],
) -> None:
    """Combine current MariaDB market snapshot with InfluxDB consumption data for stress scoring."""
    market_rows = db.fetch_all("""
        SELECT hub.type_id,
            hub.best_sell_price AS hub_sell,
            hub.best_buy_price AS hub_buy,
            hub.total_sell_volume AS hub_volume,
            alliance.total_sell_volume AS alliance_volume,
            TIMESTAMPDIFF(HOUR, hub.observed_at, NOW()) AS freshness_hrs
        FROM market_order_snapshots_summary hub
        LEFT JOIN market_order_snapshots_summary alliance
            ON alliance.type_id = hub.type_id AND alliance.source_type = 'alliance_structure'
        WHERE hub.source_type = 'market_hub'
    """)

    for row in market_rows:
        tid = int(row.get("type_id") or 0)
        if tid not in item_data:
            continue

        data = item_data[tid]
        hub_sell = float(row.get("hub_sell") or 0)
        hub_buy = float(row.get("hub_buy") or 0)
        hub_volume = float(row.get("hub_volume") or 0)
        alliance_volume = float(row.get("alliance_volume") or 0)
        freshness_hrs = float(row.get("freshness_hrs") or 0)

        data["data_freshness_hrs"] = freshness_hrs

        # Market spread
        if hub_sell > 0:
            data["market_spread_pct"] = (hub_sell - hub_buy) / hub_sell if hub_buy > 0 else 1.0

        # Liquidity: sell volume / average daily volume
        vol_30d = data.get("volume_avg_30d") or 0
        if vol_30d > 0:
            data["liquidity_score"] = hub_volume / vol_30d

        # Stock days remaining: alliance stock / avg daily consumption
        consumption_30d = data.get("consumption_30d") or 0
        avg_daily_consumption = consumption_30d / 30.0 if consumption_30d > 0 else 0
        if avg_daily_consumption > 0:
            data["stock_days_remaining"] = alliance_volume / avg_daily_consumption

        # Market stress score (0-100)
        liq = data.get("liquidity_score")
        spread = data.get("market_spread_pct") or 0
        stock_days = data.get("stock_days_remaining")

        stress = 0.0
        if liq is not None:
            stress += max(0.0, 1.0 - liq) * 40
        if spread > 0:
            stress += min(30.0, spread * 30)
        if stock_days is not None:
            stress += max(0.0, 10.0 - stock_days) * 3
        data["market_stress_score"] = min(100.0, stress)


def _compute_composite_scores(item_data: dict[int, dict[str, Any]]) -> None:
    """Compute criticality_score, confidence_score, and priority_index for each item."""
    for tid, data in item_data.items():
        dep_score = data.get("dependency_score", 0)
        cer = data.get("critical_edge_ratio", 0)
        sub = data.get("substitutability", 1.0)
        spof = data.get("spof_flag", 0)

        # Criticality score (0-100)
        criticality = min(100.0,
            dep_score * 0.3
            + cer * 20
            + (1.0 - sub) * 25
            + spof * 25
        )
        data["criticality_score"] = criticality

        # Confidence: based on data coverage and freshness
        coverage = data.get("coverage_ratio", 0)
        freshness = data.get("data_freshness_hrs")
        freshness_factor = 1.0
        if freshness is not None and freshness > 0:
            freshness_factor = max(0.3, 1.0 - (freshness / 48.0))
        data["confidence_score"] = min(1.0, coverage * 0.7 + freshness_factor * 0.3)

        # Priority index (0-100)
        trend = data.get("trend_score", 0)
        stress = data.get("market_stress_score", 0)
        conf = data.get("confidence_score", 0.5)
        data["priority_index"] = min(100.0,
            criticality * 0.35
            + trend * 0.25
            + stress * 0.30
            + conf * 10 * 0.10
        )


def _sanitize_for_mysql(value: Any) -> Any:
    """Replace float NaN/Inf with None so MySQL doesn't reject them."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _persist_criticality_index(db: SupplyCoreDb, item_data: dict[int, dict[str, Any]], computed_at: str) -> int:
    """Write item_criticality_index table and assign priority_rank."""
    if not item_data:
        return 0

    # Sort by priority_index descending for rank assignment
    sorted_items = sorted(item_data.items(), key=lambda kv: kv[1].get("priority_index", 0), reverse=True)
    for rank, (tid, data) in enumerate(sorted_items, start=1):
        data["priority_rank"] = rank

    rows = []
    for tid, d in sorted_items:
        rows.append(tuple(_sanitize_for_mysql(v) for v in (
            tid,
            d.get("doctrine_count", 0),
            d.get("fit_count", 0),
            d.get("dependency_score", 0),
            d.get("critical_edge_ratio", 0),
            d.get("substitute_count", 0),
            d.get("substitutability", 1.0),
            d.get("dependency_depth", 0),
            d.get("spof_flag", 0),
            d.get("spof_impact_score", 0),
            d.get("criticality_score", 0),
            d.get("price_avg_7d"),
            d.get("price_avg_30d"),
            d.get("volume_avg_7d"),
            d.get("volume_avg_30d"),
            d.get("price_velocity"),
            d.get("volume_velocity"),
            d.get("price_volatility"),
            d.get("trend_regime", "stable"),
            d.get("trend_score", 0),
            d.get("market_spread_pct"),
            d.get("liquidity_score"),
            d.get("stock_days_remaining"),
            d.get("market_stress_score", 0),
            d.get("data_freshness_hrs"),
            d.get("coverage_ratio"),
            d.get("confidence_score", 0.5),
            d.get("priority_index", 0),
            d.get("priority_rank"),
            computed_at,
        )))

    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM item_criticality_index")
    for offset in range(0, len(rows), DEFAULT_BATCH_SIZE):
        chunk = rows[offset : offset + DEFAULT_BATCH_SIZE]
        with db.transaction() as (_, cursor):
            cursor.executemany(
                """INSERT INTO item_criticality_index (
                    type_id, doctrine_count, fit_count, dependency_score,
                    critical_edge_ratio, substitute_count, substitutability,
                    dependency_depth, spof_flag, spof_impact_score, criticality_score,
                    price_avg_7d, price_avg_30d, volume_avg_7d, volume_avg_30d,
                    price_velocity, volume_velocity, price_volatility,
                    trend_regime, trend_score,
                    market_spread_pct, liquidity_score, stock_days_remaining,
                    market_stress_score,
                    data_freshness_hrs, coverage_ratio, confidence_score,
                    priority_index, priority_rank, computed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )""",
                chunk,
            )
    return len(rows)


def run_compute_graph_insights(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    influx_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "compute_graph_insights"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return _job_payload(job_name, started, "skipped", error_text="neo4j disabled")

    client = Neo4jClient(config)
    computed_at = _utc_now_sql()
    runtime = neo4j_raw or {}
    batch_size = _coerce_batch_size(runtime.get("insights_batch_size") or runtime.get("batch_size"))

    # ── Step 1: Base item dependency scores from Neo4j ──
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
               toString(COALESCE(d.name, '')) AS doctrine_name,
               toInteger(fit_count) AS fit_count,
               toInteger(item_count) AS item_count,
               toInteger(unique_item_count) AS unique_item_count,
               toFloat(unique_item_count + (fit_count * 5)) AS dependency_depth_score
        ORDER BY dependency_depth_score DESC, doctrine_id ASC
        """
    )

    _ensure_doctrine_dependency_depth_schema(db)

    _upsert_table(
        db,
        "item_dependency_score",
        "type_id, doctrine_count, fit_count, dependency_score, computed_at",
        "%s, %s, %s, %s, %s",
        [(int(r["type_id"]), int(r["doctrine_count"]), int(r["fit_count"]), float(r["dependency_score"]), computed_at) for r in item_rows if int(r.get("type_id") or 0) > 0],
    )

    doctrine_dependency_rows = [r for r in doctrine_rows if int(r.get("doctrine_id") or 0) > 0]
    _upsert_table(
        db,
        "doctrine_dependency_depth",
        "doctrine_id, doctrine_name, fit_count, item_count, unique_item_count, dependency_depth_score, computed_at",
        "%s, %s, %s, %s, %s, %s, %s",
        [
            (
                int(r["doctrine_id"]),
                str(r.get("doctrine_name") or f"Doctrine #{int(r['doctrine_id'])}"),
                int(r["fit_count"]),
                int(r["item_count"]),
                int(r["unique_item_count"]),
                float(r["dependency_depth_score"]),
                computed_at,
            )
            for r in doctrine_dependency_rows
        ],
    )

    # ── Step 2: Critical edge ratio from Neo4j ──
    critical_edge_rows = client.query(
        """
        MATCH (f:Fit)-[:CONTAINS]->(i:Item)
        WITH i, count(DISTINCT f) AS total_fits
        OPTIONAL MATCH (f2:Fit)-[:USES_CRITICAL_ITEM]->(i)
        WITH i, total_fits, count(DISTINCT f2) AS critical_fits
        RETURN toInteger(i.type_id) AS type_id,
               toInteger(total_fits) AS total_fits,
               toInteger(critical_fits) AS critical_fits,
               toFloat(CASE WHEN total_fits > 0 THEN toFloat(critical_fits) / total_fits ELSE 0 END) AS critical_edge_ratio
        """
    )

    # Build unified item_data dict for enrichment
    item_data: dict[int, dict[str, Any]] = {}
    for r in item_rows:
        tid = int(r.get("type_id") or 0)
        if tid <= 0:
            continue
        item_data[tid] = {
            "doctrine_count": int(r["doctrine_count"]),
            "fit_count": int(r["fit_count"]),
            "dependency_score": float(r["dependency_score"]),
            "critical_edge_ratio": 0.0,
        }

    for r in critical_edge_rows:
        tid = int(r.get("type_id") or 0)
        if tid in item_data:
            item_data[tid]["critical_edge_ratio"] = float(r.get("critical_edge_ratio") or 0)

    # ── Step 3: Substitutability (MariaDB group_id lookup) ──
    _compute_substitutability(db, item_data)

    # ── Step 4: SPOF detection ──
    _compute_spof(item_data)

    # ── Step 5: InfluxDB trend metrics ──
    influx_cfg = None
    if influx_raw:
        influx_cfg = InfluxConfig.from_runtime(influx_raw)
        if influx_cfg.validate():
            influx_cfg = None
    _compute_item_trend_metrics(influx_cfg, item_data)

    # ── Step 6: Market stress ──
    _compute_market_stress(db, item_data)

    # ── Step 7: Composite scores ──
    _compute_composite_scores(item_data)

    # ── Step 8: Persist item_criticality_index ──
    criticality_written = _persist_criticality_index(db, item_data, computed_at)
    spof_count = sum(1 for d in item_data.values() if d.get("spof_flag"))

    # ── Fit overlap (existing batched logic) ──
    sync_state = _sync_state_get(db, SYNC_DATASET_GRAPH_INSIGHTS_FIT_OVERLAP) or {}
    cursor_fit_id, cursor_other_fit_id = _parse_pair_cursor(sync_state.get("last_cursor"))
    fit_rows_processed = 0
    fit_rows_written = 0
    batch_count = 0
    while True:
        batch_started = time.perf_counter()
        fit_overlap_rows = client.query(
            """
            MATCH (f1:Fit)-[r:SHARES_ITEM_WITH]->(f2:Fit)
            WHERE toInteger(f1.fit_id) > $cursor_fit_id
               OR (toInteger(f1.fit_id) = $cursor_fit_id AND toInteger(f2.fit_id) > $cursor_other_fit_id)
            RETURN toInteger(f1.fit_id) AS fit_id,
                   toInteger(f2.fit_id) AS other_fit_id,
                   toInteger(r.shared_item_count) AS shared_item_count,
                   toFloat(r.overlap_score) AS overlap_score
            ORDER BY fit_id ASC, other_fit_id ASC
            LIMIT $batch_size
            """,
            {"cursor_fit_id": cursor_fit_id, "cursor_other_fit_id": cursor_other_fit_id, "batch_size": batch_size},
        )
        if not fit_overlap_rows:
            break

        upsert_rows = [
            (int(r["fit_id"]), int(r["other_fit_id"]), int(r["shared_item_count"]), float(r["overlap_score"]), computed_at)
            for r in fit_overlap_rows
            if int(r.get("fit_id") or 0) > 0 and int(r.get("other_fit_id") or 0) > 0
        ]
        if upsert_rows:
            with db.transaction() as (_, cursor):
                cursor.executemany(
                    """
                    INSERT INTO fit_overlap_score (fit_id, other_fit_id, shared_item_count, overlap_score, computed_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        shared_item_count = VALUES(shared_item_count),
                        overlap_score = VALUES(overlap_score),
                        computed_at = VALUES(computed_at)
                    """,
                    upsert_rows,
                )

        last_row = fit_overlap_rows[-1]
        cursor_fit_id = int(last_row.get("fit_id") or cursor_fit_id)
        cursor_other_fit_id = int(last_row.get("other_fit_id") or cursor_other_fit_id)
        batch_cursor = _format_pair_cursor(cursor_fit_id, cursor_other_fit_id)
        fit_rows_processed += len(fit_overlap_rows)
        fit_rows_written += len(upsert_rows)
        batch_count += 1
        _sync_state_upsert(
            db,
            SYNC_DATASET_GRAPH_INSIGHTS_FIT_OVERLAP,
            sync_mode="incremental",
            status="running",
            last_success_at=None,
            last_cursor=batch_cursor,
            last_row_count=fit_rows_written,
            last_error_message=None,
        )
        _emit_batch_telemetry(
            config.log_file,
            job_name,
            {
                "batch_start": batch_count,
                "batch_end": batch_count,
                "rows_processed": len(fit_overlap_rows),
                "rows_written": len(upsert_rows),
                "duration_ms": int((time.perf_counter() - batch_started) * 1000),
                "checkpoint_after": batch_cursor,
                "errors": "",
            },
        )

    db.execute("DELETE FROM fit_overlap_score WHERE computed_at < %s", (computed_at,))
    _sync_state_upsert(
        db,
        SYNC_DATASET_GRAPH_INSIGHTS_FIT_OVERLAP,
        sync_mode="incremental",
        status="success",
        last_success_at=computed_at,
        last_cursor=None,
        last_row_count=fit_rows_written,
        last_error_message=None,
    )

    result = _job_payload(
        job_name,
        started,
        "success",
        rows_processed=len(item_rows) + len(doctrine_rows) + fit_rows_processed,
        rows_written=len(item_rows) + len(doctrine_rows) + fit_rows_written + criticality_written,
        computed_at=computed_at,
        fit_overlap_batches=batch_count,
        fit_overlap_batch_size=batch_size,
        criticality_items=criticality_written,
        spof_items=spof_count,
        influx_enabled=influx_cfg is not None and influx_cfg.enabled,
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


# ── Killmail entity projection ──────────────────────────────────────────────

_KILLMAIL_CURSOR_KEY = "graph_sync_killmail_entity_cursor"
_KILLMAIL_BATCH = 1000


def run_compute_graph_sync_killmail_entities(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Project killmail events into Neo4j as Killmail nodes with system/battle relationships."""
    started = time.perf_counter()
    job_name = "compute_graph_sync_killmail_entities"

    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    _ensure_constraints_and_indexes(client)

    # Read cursor
    cursor_row = db.fetch_one(
        "SELECT last_cursor FROM sync_state WHERE dataset_key = %s",
        (_KILLMAIL_CURSOR_KEY,),
    )
    last_cursor = int(cursor_row["last_cursor"]) if cursor_row and cursor_row.get("last_cursor") else 0

    rows_processed = 0
    rows_written = 0
    new_cursor = last_cursor

    while True:
        batch = db.fetch_all(
            """
            SELECT id, killmail_id, solar_system_id, battle_id,
                   effective_killmail_at, zkb_total_value AS total_value,
                   victim_ship_type_id
            FROM killmail_events
            WHERE id > %s AND battle_id IS NOT NULL
            ORDER BY id ASC
            LIMIT %s
            """,
            (new_cursor, _KILLMAIL_BATCH),
        )
        if not batch:
            break

        rows_processed += len(batch)

        # Build Neo4j rows
        neo4j_rows = []
        for row in batch:
            neo4j_rows.append({
                "killmail_id": int(row["killmail_id"]),
                "solar_system_id": int(row["solar_system_id"]),
                "battle_id": str(row["battle_id"]),
                "killed_at": str(row.get("effective_killmail_at") or ""),
                "total_value": float(row.get("total_value") or 0),
                "victim_ship_type_id": int(row.get("victim_ship_type_id") or 0),
            })

        client.query(
            """
            UNWIND $rows AS row
            MERGE (km:Killmail {killmail_id: toInteger(row.killmail_id)})
              SET km.killed_at = row.killed_at,
                  km.total_value = toFloat(row.total_value),
                  km.victim_ship_type_id = toInteger(row.victim_ship_type_id)
            WITH km, row
            MERGE (sys:System {system_id: toInteger(row.solar_system_id)})
            MERGE (km)-[:OCCURRED_IN]->(sys)
            WITH km, row
            MERGE (b:Battle {battle_id: row.battle_id})
            MERGE (km)-[:PART_OF_BATTLE]->(b)
            """,
            {"rows": neo4j_rows},
        )
        rows_written += len(neo4j_rows)
        new_cursor = int(batch[-1]["id"])

    # Update cursor
    if new_cursor > last_cursor:
        db.execute(
            """
            INSERT INTO sync_state (dataset_key, last_cursor, last_row_count, last_success_at, status)
            VALUES (%s, %s, %s, UTC_TIMESTAMP(), 'success')
            ON DUPLICATE KEY UPDATE last_cursor = VALUES(last_cursor),
                                    last_row_count = VALUES(last_row_count),
                                    last_success_at = VALUES(last_success_at),
                                    status = 'success'
            """,
            (_KILLMAIL_CURSOR_KEY, str(new_cursor), rows_written),
        )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=f"Projected {rows_written} killmails into Neo4j.",
        rows_processed=rows_processed,
        rows_written=rows_written,
        duration_ms=duration_ms,
    ).to_dict()
