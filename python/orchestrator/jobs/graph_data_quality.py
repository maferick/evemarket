from __future__ import annotations

import time
import uuid
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

QUALITY_GATE_THRESHOLD = 0.60

# Weights for composite quality score (sum to 1.0).
# missing_alliance is low because many characters legitimately have no alliance.
WEIGHT_ORPHANS = 0.30
WEIGHT_DUPLICATES = 0.25
WEIGHT_MISSING_ALLIANCE = 0.05
WEIGHT_STALE = 0.25
WEIGHT_IDENTITY = 0.15

STALE_DAYS = 45


def _count_duplicate_relationships_batched(client: Neo4jClient, timeout: int, batch_size: int = 500) -> int:
    """Count duplicate relationships in batches to avoid cartesian-product timeouts.

    Instead of scanning every pair globally, we iterate Character nodes in
    cursor-based batches and count duplicates anchored on each batch.  The
    remaining node labels are checked in a single pass each since they are
    typically much smaller than Character.
    """
    total_dups = 0

    # -- Characters: cursor-based batches ----------------------------------
    cursor = 0
    while True:
        batch_rows = client.query(
            "MATCH (a:Character) "
            "WHERE a.character_id > $cursor "
            "RETURN a.character_id AS cid "
            "ORDER BY cid ASC LIMIT $limit",
            {"cursor": cursor, "limit": batch_size},
            timeout_seconds=timeout,
        )
        if not batch_rows:
            break
        batch_ids = [int(r["cid"]) for r in batch_rows if int(r.get("cid") or 0) > 0]
        if not batch_ids:
            break
        cursor = batch_ids[-1]

        dup_row = client.query(
            "UNWIND $ids AS aid "
            "MATCH (a:Character {character_id: aid})-[r1]->(b) "
            "MATCH (a)-[r2]->(b) "
            "WHERE type(r1) = type(r2) AND id(r1) < id(r2) "
            "RETURN count(r1) AS cnt",
            {"ids": batch_ids},
            timeout_seconds=timeout,
        )
        total_dups += int((dup_row[0] if dup_row else {}).get("cnt") or 0)

    # -- Non-Character labels: single pass each (much smaller cardinality) -
    for label in ("Battle", "Alliance", "Corporation", "ShipType", "Fit", "Doctrine"):
        dup_row = client.query(
            f"MATCH (a:{label})-[r1]->(b), (a:{label})-[r2]->(b) "
            "WHERE type(r1) = type(r2) AND id(r1) < id(r2) "
            "RETURN count(r1) AS cnt",
            timeout_seconds=timeout,
        )
        total_dups += int((dup_row[0] if dup_row else {}).get("cnt") or 0)

    return total_dups


def run_graph_data_quality_check(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_data_quality_check"
    runtime = neo4j_raw or {}
    config = Neo4jConfig.from_runtime(runtime)

    if not config.enabled:
        return JobResult.skipped(job_key=job_name, reason="neo4j disabled").to_dict()

    client = Neo4jClient(config)
    run_id = uuid.uuid4().hex[:16]
    qc_timeout = max(15, int(runtime.get("quality_check_timeout_seconds") or 60))
    dup_batch_size = max(50, int(runtime.get("quality_check_dup_batch_size") or 500))

    # ── Total characters ──────────────────────────────────────────────
    total_row = client.query("MATCH (c:Character) RETURN count(c) AS cnt", timeout_seconds=qc_timeout)
    characters_total = int((total_row[0] if total_row else {}).get("cnt") or 0)

    if characters_total == 0:
        return JobResult.skipped(job_key=job_name, reason="No characters in graph").to_dict()

    # ── Characters with at least one relationship ─────────────────────
    with_battles_row = client.query(
        "MATCH (c:Character) WHERE (c)--() RETURN count(c) AS cnt",
        timeout_seconds=qc_timeout,
    )
    characters_with_battles = int((with_battles_row[0] if with_battles_row else {}).get("cnt") or 0)

    # ── Orphan characters (no relationships at all) ───────────────────
    orphan_row = client.query(
        "MATCH (c:Character) WHERE NOT (c)--() RETURN count(c) AS cnt",
        timeout_seconds=qc_timeout,
    )
    orphan_characters = int((orphan_row[0] if orphan_row else {}).get("cnt") or 0)

    # ── Duplicate relationships (same type between same pair) ─────────
    duplicate_relationships = _count_duplicate_relationships_batched(client, qc_timeout, dup_batch_size)

    # ── Missing alliance IDs ──────────────────────────────────────────
    missing_alliance_row = client.query(
        "MATCH (c:Character) WHERE c.alliance_id IS NULL OR c.alliance_id = 0 RETURN count(c) AS cnt",
        timeout_seconds=qc_timeout,
    )
    missing_alliance_ids = int((missing_alliance_row[0] if missing_alliance_row else {}).get("cnt") or 0)

    # ── Stale nodes (computed_at older than STALE_DAYS) ───────────────
    stale_row = client.query(
        "MATCH (c:Character) WHERE c.computed_at IS NOT NULL "
        "AND c.computed_at < datetime() - duration({days: $days}) "
        "RETURN count(c) AS cnt",
        {"days": STALE_DAYS},
        timeout_seconds=qc_timeout,
    )
    stale_data_count = int((stale_row[0] if stale_row else {}).get("cnt") or 0)

    # ── Identity mismatches (null or zero character_id) ───────────────
    identity_row = client.query(
        "MATCH (c:Character) WHERE c.character_id IS NULL OR c.character_id = 0 RETURN count(c) AS cnt",
        timeout_seconds=qc_timeout,
    )
    identity_mismatches = int((identity_row[0] if identity_row else {}).get("cnt") or 0)

    # ── Compute composite quality score ───────────────────────────────
    orphan_ratio = orphan_characters / characters_total
    dup_ratio = min(1.0, duplicate_relationships / max(1, characters_total))
    missing_alliance_ratio = missing_alliance_ids / characters_total
    stale_ratio = stale_data_count / characters_total
    identity_ratio = identity_mismatches / characters_total

    quality_score = round(max(0.0, 1.0 - (
        WEIGHT_ORPHANS * orphan_ratio
        + WEIGHT_DUPLICATES * dup_ratio
        + WEIGHT_MISSING_ALLIANCE * missing_alliance_ratio
        + WEIGHT_STALE * stale_ratio
        + WEIGHT_IDENTITY * identity_ratio
    )), 4)

    gate_passed = 1 if quality_score >= QUALITY_GATE_THRESHOLD else 0

    gate_details = {
        "orphan_ratio": round(orphan_ratio, 4),
        "duplicate_ratio": round(dup_ratio, 4),
        "missing_alliance_ratio": round(missing_alliance_ratio, 4),
        "stale_ratio": round(stale_ratio, 4),
        "identity_ratio": round(identity_ratio, 4),
        "threshold": QUALITY_GATE_THRESHOLD,
    }

    # ── Export to MariaDB ─────────────────────────────────────────────
    db.execute(
        """INSERT INTO graph_data_quality_metrics (
                run_id, stage, characters_total, characters_with_battles,
                orphan_characters, duplicate_relationships, missing_alliance_ids,
                stale_data_count, identity_mismatches, quality_score, gate_passed,
                gate_details_json, computed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP())""",
        (
            run_id,
            "pre_pipeline",
            characters_total,
            characters_with_battles,
            orphan_characters,
            duplicate_relationships,
            missing_alliance_ids,
            stale_data_count,
            identity_mismatches,
            quality_score,
            gate_passed,
            json_dumps_safe(gate_details),
        ),
    )

    # Write intelligence snapshot for freshness tracking
    snapshot_payload = {
        "run_id": run_id,
        "quality_score": quality_score,
        "gate_passed": bool(gate_passed),
        "characters_total": characters_total,
        "orphan_characters": orphan_characters,
        "duplicate_relationships": duplicate_relationships,
        "missing_alliance_ids": missing_alliance_ids,
        "stale_data_count": stale_data_count,
        "identity_mismatches": identity_mismatches,
    }
    db.upsert_intelligence_snapshot(
        snapshot_key="graph_data_quality_state",
        payload_json=json_dumps_safe(snapshot_payload),
        metadata_json=json_dumps_safe({"source": "neo4j", "reason": "scheduler:python", "run_id": run_id}),
        expires_seconds=3600,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)

    if not gate_passed:
        return JobResult.failed(
            job_key=job_name,
            error=f"Quality gate failed: score {quality_score:.4f} < {QUALITY_GATE_THRESHOLD}",
            duration_ms=duration_ms,
            meta={"run_id": run_id, "quality_score": quality_score, "gate_details": gate_details},
        ).to_dict()

    return JobResult.success(
        job_key=job_name,
        summary=f"Quality gate passed: score {quality_score:.4f} ({characters_total} characters, {orphan_characters} orphans, {duplicate_relationships} dups).",
        rows_processed=characters_total,
        rows_written=1,
        rows_seen=characters_total,
        duration_ms=duration_ms,
        meta={"run_id": run_id, "quality_score": quality_score, "gate_details": gate_details},
    ).to_dict()
