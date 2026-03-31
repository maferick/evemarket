"""EveWho Neo4j Enrichment Sync — batch-enriches characters from EveWho into Neo4j.

This job processes the enrichment_queue table in bounded batches:

1. Claims a batch of pending characters (highest priority first)
2. Fetches corp history from EveWho via the adapter layer
3. Upserts Character, Corporation, Alliance nodes and MEMBER_OF relationships
   into Neo4j
4. Updates queue status and sync_state cursor

All Neo4j writes use MERGE for idempotency.  NPC corps (id < 2_000_000) are
flagged but still tracked.
"""
from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..evewho_adapter import EveWhoAdapter
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

JOB_KEY = "evewho_enrichment_sync"
DATASET_KEY = "evewho_enrichment_sync_cursor"
DEFAULT_BATCH_SIZE = 10
DEFAULT_MAX_BATCHES = 5
MAX_ATTEMPTS = 3


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _sync_state_upsert(db: SupplyCoreDb, cursor: str, status: str, row_count: int) -> None:
    db.execute(
        """
        INSERT INTO sync_state (dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_error_message)
        VALUES (%s, 'incremental', %s, UTC_TIMESTAMP(), %s, %s, NULL)
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            last_success_at = VALUES(last_success_at),
            last_cursor = VALUES(last_cursor),
            last_row_count = VALUES(last_row_count),
            last_error_message = NULL,
            updated_at = CURRENT_TIMESTAMP
        """,
        (DATASET_KEY, status, cursor, max(0, int(row_count))),
    )


def _claim_batch(db: SupplyCoreDb, batch_size: int) -> list[dict[str, Any]]:
    """Claim a batch of pending characters for processing."""
    rows = db.fetch_all(
        """
        SELECT character_id, priority
        FROM enrichment_queue
        WHERE status = 'pending' AND attempts < %s
        ORDER BY priority DESC, queued_at ASC
        LIMIT %s
        """,
        (MAX_ATTEMPTS, batch_size),
    )
    if not rows:
        return []

    ids = [int(r["character_id"]) for r in rows]
    placeholders = ",".join(["%s"] * len(ids))
    db.execute(
        f"""
        UPDATE enrichment_queue
        SET status = 'processing', attempts = attempts + 1
        WHERE character_id IN ({placeholders})
        """,
        tuple(ids),
    )
    return rows


def _mark_done(db: SupplyCoreDb, character_id: int) -> None:
    db.execute(
        "UPDATE enrichment_queue SET status = 'done', done_at = UTC_TIMESTAMP() WHERE character_id = %s",
        (character_id,),
    )


def _mark_failed(db: SupplyCoreDb, character_id: int, error: str) -> None:
    db.execute(
        """
        UPDATE enrichment_queue
        SET status = IF(attempts >= %s, 'failed', 'pending'),
            last_error = %s
        WHERE character_id = %s
        """,
        (MAX_ATTEMPTS, str(error)[:500], character_id),
    )



def _enrich_character_to_neo4j(
    neo4j: Neo4jClient,
    character_id: int,
    info: dict[str, Any],
    history: list[dict[str, Any]],
) -> None:
    """Upsert a character and their full corp history into Neo4j."""
    # Upsert character node
    neo4j.query(
        """
        MERGE (c:Character {character_id: $id})
        SET c.name = $name,
            c.sec_status = $sec,
            c.enriched_at = datetime(),
            c.enriched = true
        """,
        {
            "id": int(character_id),
            "name": str(info.get("name") or ""),
            "sec": float(info.get("sec_status") or 0.0),
        },
    )

    # Upsert current corporation + alliance
    corp_id = int(info.get("corporation_id") or 0)
    alliance_id = int(info.get("alliance_id") or 0)
    if corp_id > 0:
        is_npc = corp_id < 2_000_000
        neo4j.query(
            """
            MERGE (corp:Corporation {corporation_id: $corpId})
            ON CREATE SET corp.is_npc = $isNpc
            WITH corp
            MATCH (c:Character {character_id: $charId})
            MERGE (c)-[:CURRENT_CORP]->(corp)
            """,
            {"corpId": corp_id, "isNpc": is_npc, "charId": int(character_id)},
        )

    if alliance_id > 0 and corp_id > 0:
        neo4j.query(
            """
            MERGE (a:Alliance {alliance_id: $allianceId})
            WITH a
            MATCH (corp:Corporation {corporation_id: $corpId})
            MERGE (corp)-[r:PART_OF]->(a)
            ON CREATE SET r.as_of = datetime()
            ON MATCH  SET r.as_of = datetime()
            """,
            {"allianceId": alliance_id, "corpId": corp_id},
        )

    # Upsert all corp history entries
    for h in history:
        h_corp_id = int(h.get("corporation_id") or 0)
        if h_corp_id <= 0:
            continue
        is_npc = h_corp_id < 2_000_000
        start_raw = str(h.get("start_date") or "").strip().replace(" ", "T").replace("/", "-")
        end_raw = str(h.get("end_date") or "").strip().replace(" ", "T").replace("/", "-") if h.get("end_date") else None

        if not start_raw:
            continue

        duration_days: int | None = None
        is_short_stay = False
        if end_raw:
            try:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
                duration_days = max(0, int((end_dt - start_dt).total_seconds() / 86400))
                is_short_stay = duration_days < 30
            except (ValueError, TypeError):
                pass

        params: dict[str, Any] = {
            "corpId": h_corp_id,
            "isNpc": is_npc,
            "charId": int(character_id),
            "from": start_raw,
            "to": end_raw,
            "duration": duration_days,
            "isShortStay": is_short_stay,
        }

        neo4j.query(
            """
            MERGE (corp:Corporation {corporation_id: $corpId})
            ON CREATE SET corp.is_npc = $isNpc
            WITH corp
            MATCH (c:Character {character_id: $charId})
            MERGE (c)-[r:MEMBER_OF {corporation_id: $corpId, from: datetime($from)}]->(corp)
            SET r.to = CASE WHEN $to IS NULL THEN null ELSE datetime($to) END,
                r.duration_days = $duration,
                r.is_short_stay = $isShortStay
            """,
            params,
        )


def _process_batch(
    db: SupplyCoreDb,
    neo4j: Neo4jClient,
    adapter: EveWhoAdapter,
    batch: list[dict[str, Any]],
) -> tuple[int, int]:
    """Process a single batch. Returns (processed, written)."""
    processed = 0
    written = 0

    for row in batch:
        character_id = int(row["character_id"])
        processed += 1

        try:
            # Fetch from EveWho
            _, payload = adapter.fetch_character(character_id)
            if not payload:
                _mark_failed(db, character_id, "EveWho returned empty/null response")
                continue

            # Extract info — EveWho returns {info: [{...}], history: [{...}]}
            info_list = payload.get("info")
            if isinstance(info_list, list) and info_list:
                info = info_list[0]
            elif isinstance(payload.get("character_id"), int):
                info = payload
            else:
                _mark_failed(db, character_id, "No character info in EveWho response")
                continue

            history = payload.get("history") or payload.get("corporation_history") or []
            if not isinstance(history, list):
                history = []

            # Write to Neo4j
            _enrich_character_to_neo4j(neo4j, character_id, info, history)
            _mark_done(db, character_id)
            written += 1

        except Neo4jError as e:
            _mark_failed(db, character_id, f"Neo4j error: {e}")
        except Exception as e:
            _mark_failed(db, character_id, str(e))

    return processed, written


def run_evewho_enrichment_sync(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Main entry point for the EveWho enrichment sync job."""
    job = start_job_run(db, JOB_KEY)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    runtime = runtime or {}

    try:
        batch_size = max(1, min(50, int(runtime.get("evewho_enrichment_batch_size") or DEFAULT_BATCH_SIZE)))
        max_batches = max(1, min(20, int(runtime.get("evewho_enrichment_max_batches") or DEFAULT_MAX_BATCHES)))
        user_agent = str(runtime.get("evewho_user_agent") or "SupplyCore Intelligence Platform / contact@supplycore.app")

        neo4j_config = Neo4jConfig.from_runtime(neo4j_raw or {})
        if not neo4j_config.enabled:
            finish_job_run(db, job, status="skipped", rows_processed=0, rows_written=0, error_text="Neo4j disabled")
            return JobResult.skipped(job_key=JOB_KEY, reason="neo4j-disabled").to_dict()

        neo4j = Neo4jClient(neo4j_config)
        rate_limit = max(1, int(runtime.get("evewho_rate_limit_requests") or 0)) if runtime.get("evewho_rate_limit_requests") else None
        adapter = EveWhoAdapter(user_agent, rate_limit_requests=rate_limit) if rate_limit else EveWhoAdapter(user_agent)

        batch_count = 0
        exhausted = False
        while batch_count < max_batches:
            batch = _claim_batch(db, batch_size)
            if not batch:
                exhausted = True
                break

            batch_count += 1
            processed, written = _process_batch(db, neo4j, adapter, batch)
            rows_processed += processed
            rows_written += written

        # has_more is true when we hit the batch cap without emptying the queue.
        has_more = not exhausted and batch_count == max_batches

        # Update sync state cursor with progress
        _sync_state_upsert(db, str(rows_written), "success", rows_written)

        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(
            db, job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={
                "batches": batch_count,
                "batch_size": batch_size,
                "duration_ms": duration_ms,
                "has_more": has_more,
            },
        )
        return JobResult.success(
            job_key=JOB_KEY,
            rows_seen=rows_processed,
            rows_written=rows_written,
            has_more=has_more,
            summary=f"Enriched {rows_written}/{rows_processed} characters in {batch_count} batch(es), {duration_ms}ms",
        ).to_dict()

    except Exception as e:
        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(e)[:500])
        return JobResult.failed(
            job_key=JOB_KEY,
            error=str(e),
            rows_seen=rows_processed,
            rows_written=rows_written,
        ).to_dict()
