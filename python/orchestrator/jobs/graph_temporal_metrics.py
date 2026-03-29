from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

WINDOWS = [
    ("7d", 7),
    ("30d", 30),
    ("90d", 90),
]

DRIFT_THRESHOLD = 0.15


def _utc_cutoff_iso(days: int) -> str:
    return (datetime.now(UTC) - timedelta(days=days)).isoformat()


def run_graph_temporal_metrics_sync(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_temporal_metrics_sync"
    config = Neo4jConfig.from_runtime(neo4j_raw or {})

    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    total_rows_written = 0

    for window_label, window_days in WINDOWS:
        cutoff = _utc_cutoff_iso(window_days)
        rows = client.query(
            """
            MATCH (c:Character)-[:ON_SIDE]->(:BattleSide)<-[:HAS_SIDE]-(b:Battle)
            WHERE b.started_at >= $cutoff
            OPTIONAL MATCH (c)-[:ATTACKED_ON]->(k:Killmail)<-[:OCCURRED_IN]-(b)
            OPTIONAL MATCH (c)-[:VICTIM_OF]->(v:Killmail)<-[:OCCURRED_IN]-(b)
            OPTIONAL MATCH (c)-[co:CO_OCCURS_WITH]-(:Character)
            WITH c,
                 count(DISTINCT b) AS battles_present,
                 count(DISTINCT k) AS kills_total,
                 count(DISTINCT v) AS losses_total,
                 COALESCE(sum(k.total_damage), 0) AS damage_total,
                 c.suspicion_score AS suspicion_score,
                 avg(COALESCE(co.weight, 0.0)) AS co_presence_density,
                 CASE WHEN count(DISTINCT b) > 0
                      THEN toFloat(count(DISTINCT k)) / count(DISTINCT b)
                      ELSE 0.0 END AS engagement_rate_avg
            WHERE battles_present > 0
            RETURN
                c.character_id AS character_id,
                toInteger(battles_present) AS battles_present,
                toInteger(kills_total) AS kills_total,
                toInteger(losses_total) AS losses_total,
                toInteger(damage_total) AS damage_total,
                toFloat(COALESCE(suspicion_score, 0.0)) AS suspicion_score,
                toFloat(co_presence_density) AS co_presence_density,
                toFloat(engagement_rate_avg) AS engagement_rate_avg
            """,
            {"cutoff": cutoff},
        )

        if not rows:
            continue

        # Batch upsert into MariaDB
        batch_size = 500
        for offset in range(0, len(rows), batch_size):
            chunk = rows[offset:offset + batch_size]
            values = []
            params: list[Any] = []
            for r in chunk:
                cid = int(r.get("character_id") or 0)
                if cid <= 0:
                    continue
                values.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                params.extend([
                    cid,
                    window_label,
                    int(r.get("battles_present") or 0),
                    int(r.get("kills_total") or 0),
                    int(r.get("losses_total") or 0),
                    int(r.get("damage_total") or 0),
                    max(-9999.999999, min(9999.999999, float(r.get("suspicion_score") or 0.0))),
                    max(-9999.999999, min(9999.999999, float(r.get("co_presence_density") or 0.0))),
                    max(-9999.999999, min(9999.999999, float(r.get("engagement_rate_avg") or 0.0))),
                    computed_at,
                ])
            if values:
                db.execute(
                    "INSERT INTO character_temporal_metrics "
                    "(character_id, window_label, battles_present, kills_total, losses_total, "
                    "damage_total, suspicion_score, co_presence_density, engagement_rate_avg, computed_at) "
                    "VALUES " + ", ".join(values) + " "
                    "ON DUPLICATE KEY UPDATE "
                    "battles_present = VALUES(battles_present), kills_total = VALUES(kills_total), "
                    "losses_total = VALUES(losses_total), damage_total = VALUES(damage_total), "
                    "suspicion_score = VALUES(suspicion_score), co_presence_density = VALUES(co_presence_density), "
                    "engagement_rate_avg = VALUES(engagement_rate_avg), computed_at = VALUES(computed_at)",
                    tuple(params),
                )
                total_rows_written += len(values)

    # Detect temporal drift: characters where 7d score diverges from 90d
    drift_rows = db.fetch_all(
        """SELECT ct7.character_id,
                  ct7.suspicion_score AS score_7d,
                  COALESCE(ct90.suspicion_score, 0) AS score_90d,
                  (ct7.suspicion_score - COALESCE(ct90.suspicion_score, 0)) AS drift
           FROM character_temporal_metrics ct7
           LEFT JOIN character_temporal_metrics ct90
               ON ct90.character_id = ct7.character_id AND ct90.window_label = '90d'
           WHERE ct7.window_label = '7d'
             AND ABS(ct7.suspicion_score - COALESCE(ct90.suspicion_score, 0)) > %s
           ORDER BY ABS(ct7.suspicion_score - COALESCE(ct90.suspicion_score, 0)) DESC
           LIMIT 100""",
        (DRIFT_THRESHOLD,),
    )

    snapshot_payload = {
        "total_rows_written": total_rows_written,
        "drift_characters": len(drift_rows or []),
        "drift_threshold": DRIFT_THRESHOLD,
        "top_drifters": [
            {
                "character_id": int(r.get("character_id") or 0),
                "score_7d": float(r.get("score_7d") or 0),
                "score_90d": float(r.get("score_90d") or 0),
                "drift": float(r.get("drift") or 0),
            }
            for r in (drift_rows or [])[:20]
        ],
    }
    db.upsert_intelligence_snapshot(
        snapshot_key="graph_temporal_metrics_state",
        payload_json=json_dumps_safe(snapshot_payload),
        metadata_json=json_dumps_safe({"source": "neo4j+character_temporal_metrics", "reason": "scheduler:python"}),
        expires_seconds=7200,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    return JobResult.success(
        job_key=job_name,
        summary=f"Temporal metrics computed for 3 windows, {total_rows_written} rows written, {len(drift_rows or [])} drift flags.",
        rows_processed=total_rows_written,
        rows_written=total_rows_written,
        rows_seen=total_rows_written,
        duration_ms=duration_ms,
        meta={"windows": ["7d", "30d", "90d"], "drift_count": len(drift_rows or [])},
    ).to_dict()
