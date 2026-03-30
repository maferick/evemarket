"""EveWho Alliance Member Sync — bulk-fetch all members of opponent alliances.

Instead of enriching characters one-by-one from a queue, this job:

1. Reads opponent alliance IDs from killmail_opponent_alliances
2. Fetches the full member list for each alliance via /api/allilist/{id}
3. For each character not yet enriched, fetches their corp history
4. Upserts Character, Corporation, Alliance nodes and MEMBER_OF relationships
   into Neo4j

This is a long-running job (hours for large alliances) that checkpoints
progress so it can resume across runs.  All Neo4j writes use MERGE for
idempotency.
"""
from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..evewho_adapter import EveWhoAdapter
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

log = logging.getLogger(__name__)

JOB_KEY = "evewho_alliance_member_sync"
DATASET_KEY = "evewho_alliance_member_sync_cursor"

# Defaults — overridable via runtime config
DEFAULT_CHARS_PER_RUN = 200          # max characters to enrich per invocation
DEFAULT_SKIP_ENRICHED_WITHIN_DAYS = 7  # skip chars enriched in last N days


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


# ---------------------------------------------------------------------------
# Alliance member list parsing
# ---------------------------------------------------------------------------

def _extract_character_ids_from_allilist(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Walk the nested allilist response and extract character entries.

    EveWho /api/allilist/{id} returns a nested structure.  We walk all dicts
    looking for character_id fields.  Returns a list of dicts with at least
    ``character_id`` and optionally ``corporation_id`` and ``name``.
    """
    characters: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            # Try to extract a character from this dict
            char_id = _safe_int(current, ["character_id", "characterID", "char_id"])
            if char_id and char_id > 0 and char_id not in seen_ids:
                seen_ids.add(char_id)
                characters.append({
                    "character_id": char_id,
                    "corporation_id": _safe_int(current, ["corporation_id", "corporationID", "corp_id"]) or 0,
                    "name": str(current.get("name") or current.get("characterName") or ""),
                })
            # Continue walking nested values
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return characters


def _safe_int(row: dict[str, Any], keys: list[str]) -> int | None:
    for key in keys:
        value = row.get(key)
        try:
            normalized = int(value or 0)
        except (TypeError, ValueError):
            normalized = 0
        if normalized > 0:
            return normalized
    return None


# ---------------------------------------------------------------------------
# Neo4j enrichment (reused from evewho_enrichment_sync pattern)
# ---------------------------------------------------------------------------

def _enrich_character_to_neo4j(
    neo4j: Neo4jClient,
    character_id: int,
    info: dict[str, Any],
    history: list[dict[str, Any]],
) -> None:
    """Upsert a character and their full corp history into Neo4j."""
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
            MERGE (corp)-[:PART_OF {as_of: datetime()}]->(a)
            """,
            {"allianceId": alliance_id, "corpId": corp_id},
        )

    for h in history:
        h_corp_id = int(h.get("corporation_id") or 0)
        if h_corp_id <= 0:
            continue
        is_npc = h_corp_id < 2_000_000
        start_raw = str(h.get("start_date") or "").strip().replace(" ", "T")
        end_raw = str(h.get("end_date") or "").strip().replace(" ", "T") if h.get("end_date") else None
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
            {
                "corpId": h_corp_id,
                "isNpc": is_npc,
                "charId": int(character_id),
                "from": start_raw,
                "to": end_raw,
                "duration": duration_days,
                "isShortStay": is_short_stay,
            },
        )


# ---------------------------------------------------------------------------
# Checkpoint helpers — track progress across runs
# ---------------------------------------------------------------------------

def _load_checkpoint(db: SupplyCoreDb) -> dict[str, Any]:
    """Load the last checkpoint from sync_state."""
    row = db.fetch_one(
        "SELECT last_cursor FROM sync_state WHERE dataset_key = %s",
        (DATASET_KEY,),
    )
    if not row or not row.get("last_cursor"):
        return {}
    try:
        import json
        return json.loads(str(row["last_cursor"]))
    except (ValueError, TypeError):
        return {}


def _save_checkpoint(db: SupplyCoreDb, checkpoint: dict[str, Any], status: str, rows_written: int) -> None:
    import json
    cursor_str = json.dumps(checkpoint, default=str)
    _sync_state_upsert(db, cursor_str, status, rows_written)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_evewho_alliance_member_sync(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Bulk-fetch and enrich all members of opponent alliances from EveWho."""
    job = start_job_run(db, JOB_KEY)
    started = time.perf_counter()
    runtime = runtime or {}

    total_written = 0
    total_skipped = 0
    total_failed = 0
    total_members_found = 0
    alliances_processed = 0
    errors: list[str] = []

    try:
        chars_per_run = max(10, int(runtime.get("evewho_alliance_chars_per_run") or DEFAULT_CHARS_PER_RUN))
        skip_within_days = max(0, int(runtime.get("evewho_alliance_skip_enriched_days") or DEFAULT_SKIP_ENRICHED_WITHIN_DAYS))
        user_agent = str(runtime.get("evewho_user_agent") or "SupplyCore Intelligence Platform / contact@supplycore.app")

        neo4j_config = Neo4jConfig.from_runtime(neo4j_raw or {})
        if not neo4j_config.enabled:
            finish_job_run(db, job, status="skipped", rows_processed=0, rows_written=0, error_text="Neo4j disabled")
            return JobResult.skipped(job_key=JOB_KEY, reason="neo4j-disabled").to_dict()

        neo4j = Neo4jClient(neo4j_config)
        adapter = EveWhoAdapter(user_agent)

        # Load opponent alliances
        alliance_rows = db.fetch_all(
            "SELECT alliance_id, label FROM killmail_opponent_alliances WHERE is_active = 1 ORDER BY alliance_id ASC"
        )
        if not alliance_rows:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0)
            return JobResult.success(
                job_key=JOB_KEY, rows_seen=0, rows_written=0,
                summary="No opponent alliances configured",
            ).to_dict()

        alliance_ids = [int(r["alliance_id"]) for r in alliance_rows if int(r.get("alliance_id") or 0) > 0]
        log.info("Alliance member sync: %d opponent alliances to process", len(alliance_ids))

        # Load checkpoint to resume from where we left off
        checkpoint = _load_checkpoint(db)
        resume_alliance_id = int(checkpoint.get("alliance_id") or 0)
        resume_after_char_id = int(checkpoint.get("last_char_id") or 0)

        budget_remaining = chars_per_run

        for alliance_id in alliance_ids:
            if budget_remaining <= 0:
                break

            # If resuming, skip alliances we already completed
            if resume_alliance_id > 0 and alliance_id < resume_alliance_id:
                continue

            # Fetch full member list for this alliance (single API call)
            log.info("Fetching allilist for alliance %d ...", alliance_id)
            _, allilist_payload = adapter.fetch_allilist(alliance_id)
            if not allilist_payload:
                log.warning("EveWho returned empty allilist for alliance %d", alliance_id)
                errors.append(f"Empty allilist for alliance {alliance_id}")
                # Clear resume point so next run retries this alliance
                resume_after_char_id = 0
                continue

            # Extract all character IDs from the nested response
            members = _extract_character_ids_from_allilist(allilist_payload)
            total_members_found += len(members)
            alliances_processed += 1
            log.info("Alliance %d: found %d members", alliance_id, len(members))

            if not members:
                resume_after_char_id = 0
                continue

            # Sort by character_id for deterministic checkpointing
            members.sort(key=lambda m: m["character_id"])

            # If resuming within this alliance, skip already-processed chars
            if alliance_id == resume_alliance_id and resume_after_char_id > 0:
                members = [m for m in members if m["character_id"] > resume_after_char_id]
                log.info("Resuming after char %d, %d members remaining", resume_after_char_id, len(members))

            # Get set of already-enriched character IDs (recently) to skip
            recently_enriched: set[int] = set()
            if skip_within_days > 0:
                enriched_result = neo4j.query(
                    """
                    MATCH (c:Character)
                    WHERE c.enriched = true
                      AND c.enriched_at > datetime() - duration({days: $days})
                    RETURN c.character_id AS character_id
                    """,
                    {"days": skip_within_days},
                )
                for record in enriched_result:
                    cid = record.get("character_id")
                    if cid:
                        recently_enriched.add(int(cid))
                log.info("Skipping %d recently-enriched characters", len(recently_enriched))

            for member in members:
                if budget_remaining <= 0:
                    # Save checkpoint so we can resume here next run
                    _save_checkpoint(db, {
                        "alliance_id": alliance_id,
                        "last_char_id": member["character_id"],
                        "total_written": total_written,
                    }, "partial", total_written)
                    break

                char_id = member["character_id"]

                # Skip recently enriched
                if char_id in recently_enriched:
                    total_skipped += 1
                    continue

                # Fetch full character data from EveWho
                try:
                    _, char_payload = adapter.fetch_character(char_id)
                    budget_remaining -= 1

                    if not char_payload:
                        log.debug("Empty response for character %d", char_id)
                        total_failed += 1
                        continue

                    # Extract info
                    info_list = char_payload.get("info")
                    if isinstance(info_list, list) and info_list:
                        info = info_list[0]
                    elif isinstance(char_payload.get("character_id"), int):
                        info = char_payload
                    else:
                        total_failed += 1
                        continue

                    history = char_payload.get("history") or char_payload.get("corporation_history") or []
                    if not isinstance(history, list):
                        history = []

                    _enrich_character_to_neo4j(neo4j, char_id, info, history)
                    total_written += 1

                    # Periodic checkpoint every 50 characters
                    if total_written % 50 == 0:
                        _save_checkpoint(db, {
                            "alliance_id": alliance_id,
                            "last_char_id": char_id,
                            "total_written": total_written,
                        }, "running", total_written)
                        log.info("Checkpoint: %d written, %d skipped, %d failed", total_written, total_skipped, total_failed)

                except Neo4jError as e:
                    total_failed += 1
                    log.warning("Neo4j error for character %d: %s", char_id, e)
                except Exception as e:
                    total_failed += 1
                    log.warning("Error enriching character %d: %s", char_id, e)
            else:
                # Inner loop completed without break — alliance fully processed
                resume_after_char_id = 0
                continue
            # Inner loop broke (budget exhausted) — stop outer loop too
            break

        # If we completed all alliances, clear the checkpoint
        if budget_remaining > 0:
            _save_checkpoint(db, {}, "complete", total_written)

        duration_ms = int((time.perf_counter() - started) * 1000)
        error_text = "; ".join(errors) if errors else None

        finish_job_run(
            db, job,
            status="success",
            rows_processed=total_members_found,
            rows_written=total_written,
            error_text=error_text,
            meta={
                "alliances_processed": alliances_processed,
                "members_found": total_members_found,
                "enriched": total_written,
                "skipped_recent": total_skipped,
                "failed": total_failed,
                "budget_per_run": chars_per_run,
                "duration_ms": duration_ms,
            },
        )
        return JobResult.success(
            job_key=JOB_KEY,
            rows_seen=total_members_found,
            rows_written=total_written,
            summary=(
                f"Alliance member sync: {total_written} enriched, {total_skipped} skipped (recent), "
                f"{total_failed} failed out of {total_members_found} members across "
                f"{alliances_processed} alliance(s), {duration_ms}ms"
            ),
        ).to_dict()

    except Exception as e:
        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(
            db, job,
            status="failed",
            rows_processed=total_members_found,
            rows_written=total_written,
            error_text=str(e)[:500],
        )
        return JobResult.failed(
            job_key=JOB_KEY,
            error=str(e),
            rows_seen=total_members_found,
            rows_written=total_written,
        ).to_dict()
