"""Theater Intelligence — Phase 4: Graph integration.

For each theater, queries Neo4j to extract per-character graph metrics
(cluster assignment, bridge score, co-occurrence density) and computes
theater-level graph summary statistics.

Populates: ``theater_graph_summary``, ``theater_graph_participants``
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.config import resolve_app_root  # noqa: F401
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import finish_job_run, start_job_run
    from orchestrator.neo4j import Neo4jClient, Neo4jConfig
else:
    from ..config import resolve_app_root  # noqa: F401
    from ..db import SupplyCoreDb
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import finish_job_run, start_job_run
    from ..neo4j import Neo4jClient, Neo4jConfig

BATCH_SIZE = 500
BRIDGE_SCORE_THRESHOLD = 0.3
SUSPICIOUS_CLUSTER_MIN_SUSPICIOUS = 2
_DECIMAL_8_4_MAX = 9999.9999


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _theater_log(runtime: dict[str, Any] | None, event: str, payload: dict[str, Any]) -> None:
    log_path = str(((runtime or {}).get("log_file") or "")).strip()
    if log_path == "":
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_dumps_safe(record) + "\n")


# ── Data loaders ────────────────────────────────────────────────────────────

def _load_theater_character_ids(db: SupplyCoreDb, theater_id: str) -> list[int]:
    """Load character IDs for a theater from theater_participants."""
    rows = db.fetch_all(
        "SELECT character_id FROM theater_participants WHERE theater_id = %s",
        (theater_id,),
    )
    return [int(r["character_id"]) for r in rows if int(r.get("character_id") or 0) > 0]


def _load_theaters(db: SupplyCoreDb) -> list[dict[str, Any]]:
    return db.fetch_all(
        "SELECT theater_id FROM theaters ORDER BY start_time ASC"
    )


def _load_theater_sides(db: SupplyCoreDb, theater_id: str) -> dict[int, str]:
    """Load character_id → side mapping from theater_participants."""
    rows = db.fetch_all(
        "SELECT character_id, side FROM theater_participants WHERE theater_id = %s",
        (theater_id,),
    )
    return {int(r["character_id"]): str(r["side"]) for r in rows}


# ── Neo4j queries ──────────────────────────────────────────────────────────

def _query_graph_metrics(client: Neo4jClient, character_ids: list[int]) -> list[dict[str, Any]]:
    """Query Neo4j for per-character graph metrics.

    Returns community_label, bridge_score (betweenness_approx),
    co_occurrence_density (pr / pagerank), and cluster membership.
    """
    if not character_ids:
        return []

    rows = client.query(
        """
        UNWIND $char_ids AS cid
        MATCH (c:Character {character_id: cid})
        OPTIONAL MATCH (c)-[r:CO_OCCURS_WITH]-()
        WITH c, count(r) AS co_count
        RETURN
            c.character_id AS character_id,
            COALESCE(c.community_label, 0) AS cluster_id,
            COALESCE(c.betweenness_approx, 0.0) AS bridge_score,
            COALESCE(c.pr, 0.0) AS pagerank,
            co_count AS co_occurrence_count,
            CASE WHEN co_count > 0 THEN toFloat(co_count) / 10.0 ELSE 0.0 END AS co_occurrence_density
        """,
        {"char_ids": character_ids},
    )
    return rows


# ── Computation ────────────────────────────────────────────────────────────

def _compute_graph_summary(
    graph_participants: list[dict[str, Any]],
    char_sides: dict[int, str],
) -> dict[str, Any]:
    """Compute theater-level graph summary from per-character metrics."""
    if not graph_participants:
        return {
            "cluster_count": 0,
            "suspicious_cluster_count": 0,
            "bridge_character_count": 0,
            "cross_side_edge_count": 0,
            "avg_co_occurrence_density": 0.0,
        }

    clusters: dict[int, list[dict[str, Any]]] = defaultdict(list)
    bridge_count = 0
    total_density = 0.0

    for gp in graph_participants:
        cluster_id = int(gp.get("cluster_id") or 0)
        clusters[cluster_id].append(gp)
        if float(gp.get("bridge_score") or 0) >= BRIDGE_SCORE_THRESHOLD:
            bridge_count += 1
        total_density += float(gp.get("co_occurrence_density") or 0)

    # Suspicious clusters: clusters where members span both sides
    suspicious_cluster_count = 0
    cross_side_edge_count = 0
    for cluster_id, members in clusters.items():
        sides_in_cluster = set()
        for m in members:
            cid = int(m.get("character_id") or 0)
            side = char_sides.get(cid, "unknown")
            sides_in_cluster.add(side)
        if "friendly" in sides_in_cluster and "opponent" in sides_in_cluster:
            suspicious_cluster_count += 1
            cross_side_edge_count += len(members)

    avg_density = total_density / len(graph_participants) if graph_participants else 0.0

    return {
        "cluster_count": len(clusters),
        "suspicious_cluster_count": suspicious_cluster_count,
        "bridge_character_count": bridge_count,
        "cross_side_edge_count": cross_side_edge_count,
        "avg_co_occurrence_density": round(avg_density, 4),
    }


# ── DB writes ──────────────────────────────────────────────────────────────

def _flush_graph_data(
    db: SupplyCoreDb,
    theater_id: str,
    summary: dict[str, Any],
    graph_participants: list[dict[str, Any]],
    computed_at: str,
) -> int:
    def _do_flush(connection, cursor):
        written = 0
        cursor.execute("DELETE FROM theater_graph_participants WHERE theater_id = %s", (theater_id,))
        cursor.execute("DELETE FROM theater_graph_summary WHERE theater_id = %s", (theater_id,))

        # Summary
        cursor.execute(
            """
            INSERT INTO theater_graph_summary (
                theater_id, cluster_count, suspicious_cluster_count,
                bridge_character_count, cross_side_edge_count,
                avg_co_occurrence_density, computed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                theater_id, summary["cluster_count"], summary["suspicious_cluster_count"],
                summary["bridge_character_count"], summary["cross_side_edge_count"],
                summary["avg_co_occurrence_density"], computed_at,
            ),
        )
        written += 1

        # Per-character
        for chunk_start in range(0, len(graph_participants), BATCH_SIZE):
            chunk = graph_participants[chunk_start:chunk_start + BATCH_SIZE]
            cursor.executemany(
                """
                INSERT INTO theater_graph_participants (
                    theater_id, character_id, cluster_id,
                    bridge_score, co_occurrence_density, suspicious_cluster_flag
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        theater_id,
                        int(gp["character_id"]),
                        int(gp.get("cluster_id") or 0),
                        min(round(float(gp.get("bridge_score") or 0), 4), _DECIMAL_8_4_MAX),
                        min(round(float(gp.get("co_occurrence_density") or 0), 4), _DECIMAL_8_4_MAX),
                        int(gp.get("suspicious_cluster_flag") or 0),
                    )
                    for gp in chunk
                ],
            )
            written += len(chunk)
        return written

    return db.run_in_transaction(_do_flush, max_retries=3)


# ── Entry point ─────────────────────────────────────────────────────────────

def run_theater_graph_integration(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Enrich theaters with Neo4j graph metrics."""
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return JobResult.skipped(job_key="theater_graph_integration", reason="neo4j disabled").to_dict()

    job = start_job_run(db, "theater_graph_integration")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _theater_log(runtime, "theater_graph_integration.job.started", {"dry_run": dry_run})

    try:
        client = Neo4jClient(config)
        theaters = _load_theaters(db)
        rows_processed = len(theaters)

        if not theaters:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
            duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
            return JobResult.success(
                job_key="theater_graph_integration", summary="No theaters.", rows_processed=0, rows_written=0, duration_ms=duration_ms,
            ).to_dict()

        theaters_enriched = 0
        for theater in theaters:
            theater_id = str(theater["theater_id"])

            character_ids = _load_theater_character_ids(db, theater_id)
            if not character_ids:
                continue

            char_sides = _load_theater_sides(db, theater_id)

            # Query Neo4j in batches
            all_graph_rows: list[dict[str, Any]] = []
            for offset in range(0, len(character_ids), BATCH_SIZE):
                chunk = character_ids[offset:offset + BATCH_SIZE]
                graph_rows = _query_graph_metrics(client, chunk)
                all_graph_rows.extend(graph_rows)

            # Mark suspicious cluster flags
            cluster_sides: dict[int, set[str]] = defaultdict(set)
            for gp in all_graph_rows:
                cid = int(gp.get("character_id") or 0)
                cluster_id = int(gp.get("cluster_id") or 0)
                side = char_sides.get(cid, "unknown")
                cluster_sides[cluster_id].add(side)

            suspicious_clusters = set()
            for cid_val, sides in cluster_sides.items():
                if "friendly" in sides and "opponent" in sides:
                    suspicious_clusters.add(cid_val)

            for gp in all_graph_rows:
                cluster_id = int(gp.get("cluster_id") or 0)
                gp["suspicious_cluster_flag"] = 1 if cluster_id in suspicious_clusters else 0

            # Compute summary
            summary = _compute_graph_summary(all_graph_rows, char_sides)

            if not dry_run:
                written = _flush_graph_data(db, theater_id, summary, all_graph_rows, computed_at)
                rows_written += written

            theaters_enriched += 1

        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written,
                       meta={"theaters_enriched": theaters_enriched})

        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="theater_graph_integration",
            summary=f"Enriched {theaters_enriched} theaters with graph data.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={"theaters_enriched": theaters_enriched, "dry_run": dry_run},
        ).to_dict()
        _theater_log(runtime, "theater_graph_integration.job.success", result)
        return result

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _theater_log(runtime, "theater_graph_integration.job.failed", {"error": str(exc)})
        raise
