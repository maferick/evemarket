"""Tracked Alliance Member Sync — ESI-based import for friendly alliances.

Imports all members from tracked (friendly) alliances into the Neo4j graph
with full corporation history.  Uses a straightforward approach:

1. Fetch corporation IDs for each tracked alliance via ESI (public endpoint).
2. For each corporation, fetch the current member list via EveWho's corplist
   (ESI's member endpoint requires director-level auth, so EveWho fills the
   gap for the initial character ID list).
3. For each member, fetch character info and corporation history via ESI
   public endpoints.
4. Upsert Character, Corporation, Alliance nodes and CURRENT_CORP, PART_OF,
   MEMBER_OF relationships into Neo4j.

Unlike the opponent evewho_alliance_member_sync job, this job does NOT:
- Use EveWho for join/depart events (the full history comes from ESI)
- Feed an enrichment queue or org discovery pipeline
- Track any counterintelligence metadata

It is designed to run on a schedule and incrementally process corps and
members across runs using a checkpoint cursor.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb
from ..esi_client import EsiClient
from ..evewho_adapter import EveWhoAdapter
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..neo4j import Neo4jClient, Neo4jConfig, Neo4jError

log = logging.getLogger(__name__)

JOB_KEY = "tracked_alliance_member_sync"
DATASET_KEY = "tracked_alliance_member_sync_cursor"

# Defaults — overridable via runtime config
DEFAULT_ESI_BUDGET = 300          # max ESI calls per invocation
DEFAULT_EVEWHO_BUDGET = 30        # max EveWho calls per invocation (only for corp member lists)
DEFAULT_ENRICH_BATCH = 50         # max characters to ESI-enrich per invocation


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Sync state helpers
# ---------------------------------------------------------------------------

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


def _load_checkpoint(db: SupplyCoreDb) -> dict[str, Any]:
    row = db.fetch_one(
        "SELECT last_cursor FROM sync_state WHERE dataset_key = %s",
        (DATASET_KEY,),
    )
    if not row or not row.get("last_cursor"):
        return {}
    try:
        return json.loads(str(row["last_cursor"]))
    except (ValueError, TypeError):
        return {}


def _save_checkpoint(db: SupplyCoreDb, checkpoint: dict[str, Any], status: str, rows_written: int) -> None:
    _sync_state_upsert(db, json.dumps(checkpoint, default=str), status, rows_written)


# ---------------------------------------------------------------------------
# ESI helpers
# ---------------------------------------------------------------------------

def _fetch_alliance_corp_ids(esi: EsiClient, alliance_id: int) -> list[int]:
    """Fetch corporation IDs belonging to an alliance via public ESI."""
    resp = esi.get(f"/latest/alliances/{alliance_id}/corporations/")
    if resp.ok and isinstance(resp.body, list):
        return sorted(int(c) for c in resp.body if isinstance(c, (int, float)) and int(c) > 0)
    log.warning("ESI alliance corps failed for %d (status %s)", alliance_id, resp.status_code)
    return []


def _fetch_corp_info(esi: EsiClient, corp_id: int) -> dict[str, Any] | None:
    """Fetch public corporation info from ESI."""
    resp = esi.get(f"/latest/corporations/{corp_id}/")
    if resp.ok and isinstance(resp.body, dict):
        return resp.body
    return None


def _compute_end_dates(esi_history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compute end_date for each ESI corporation history entry.

    ESI returns history entries sorted by record_id without end_date.
    The end_date of entry N is the start_date of the next entry.
    """
    if not esi_history:
        return []
    sorted_history = sorted(esi_history, key=lambda h: int(h.get("record_id") or 0))
    result: list[dict[str, Any]] = []
    for i, entry in enumerate(sorted_history):
        corp_id = int(entry.get("corporation_id") or 0)
        if corp_id <= 0:
            continue
        start_date = str(entry.get("start_date") or "").strip()
        end_date: str | None = None
        if i + 1 < len(sorted_history):
            end_date = str(sorted_history[i + 1].get("start_date") or "").strip() or None
        result.append({
            "corporation_id": corp_id,
            "start_date": start_date,
            "end_date": end_date,
        })
    return result


# ---------------------------------------------------------------------------
# EveWho member list extraction
# ---------------------------------------------------------------------------

def _extract_corplist_members(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Extract members from an EveWho /api/corplist response.

    Returns [{"character_id": int, "name": str}, ...].
    """
    if not payload:
        return []

    members: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            char_id = _safe_int(current, ["character_id", "characterID", "char_id"])
            if char_id and char_id > 0 and char_id not in seen_ids:
                seen_ids.add(char_id)
                name = str(current.get("name") or current.get("characterName") or "")
                members.append({"character_id": char_id, "name": name})
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return members


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
# Neo4j writes
# ---------------------------------------------------------------------------

def _upsert_corp_members(
    neo4j: Neo4jClient,
    alliance_id: int,
    corp_id: int,
    members: list[dict[str, Any]],
) -> int:
    """Batch MERGE members of a corp into Neo4j.

    Creates/updates Character nodes, Corporation node, Alliance node,
    and CURRENT_CORP / PART_OF relationships.
    """
    if not members:
        return 0

    is_npc = corp_id < 2_000_000
    member_params = [{"character_id": m["character_id"], "name": m["name"]} for m in members]

    neo4j.query(
        """
        UNWIND $members AS m
        MERGE (c:Character {character_id: m.character_id})
        SET c.name = m.name, c.org_synced_at = toString(datetime())
        WITH c
        MERGE (corp:Corporation {corporation_id: $corpId})
        ON CREATE SET corp.is_npc = $isNpc
        MERGE (c)-[:CURRENT_CORP]->(corp)
        """,
        {"members": member_params, "corpId": corp_id, "isNpc": is_npc},
    )

    if alliance_id > 0:
        neo4j.query(
            """
            MERGE (a:Alliance {alliance_id: $allianceId})
            WITH a
            MERGE (corp:Corporation {corporation_id: $corpId})
            MERGE (corp)-[r:PART_OF]->(a)
            ON CREATE SET r.as_of = toString(datetime())
            ON MATCH  SET r.as_of = toString(datetime())
            """,
            {"allianceId": alliance_id, "corpId": corp_id},
        )

    return len(members)


def _clean_stale_current_corp(
    neo4j: Neo4jClient,
    corp_id: int,
    current_member_ids: list[int],
) -> None:
    """Remove CURRENT_CORP edges for characters no longer in this corp."""
    if not current_member_ids:
        return
    neo4j.query(
        """
        MATCH (c:Character)-[r:CURRENT_CORP]->(corp:Corporation {corporation_id: $corpId})
        WHERE NOT c.character_id IN $currentIds
        DELETE r
        """,
        {"corpId": corp_id, "currentIds": current_member_ids},
    )


def _enrich_character(
    neo4j: Neo4jClient,
    character_id: int,
    info: dict[str, Any],
    history: list[dict[str, Any]],
) -> None:
    """Upsert a character and their full corp history into Neo4j via ESI data."""
    neo4j.query(
        """
        MERGE (c:Character {character_id: $id})
        SET c.name = $name,
            c.sec_status = $sec,
            c.enriched_at = toString(datetime()),
            c.enriched = true
        """,
        {
            "id": int(character_id),
            "name": str(info.get("name") or ""),
            "sec": float(info.get("security_status") or 0.0),
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
            MERGE (corp)-[r:PART_OF]->(a)
            ON CREATE SET r.as_of = toString(datetime())
            ON MATCH  SET r.as_of = toString(datetime())
            """,
            {"allianceId": alliance_id, "corpId": corp_id},
        )

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
        if end_raw:
            try:
                start_dt = datetime.fromisoformat(start_raw.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_raw.replace("Z", "+00:00"))
                duration_days = max(0, int((end_dt - start_dt).total_seconds() / 86400))
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
                r.duration_days = $duration
            """,
            {
                "corpId": h_corp_id,
                "isNpc": is_npc,
                "charId": int(character_id),
                "from": start_raw,
                "to": end_raw,
                "duration": duration_days,
            },
        )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_tracked_alliance_member_sync(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Import members from tracked (friendly) alliances via ESI + EveWho corplist."""
    job = start_job_run(db, JOB_KEY)
    started = time.perf_counter()
    runtime = runtime or {}

    total_written = 0
    total_members = 0
    total_enriched = 0
    alliances_processed = 0
    corps_processed = 0
    esi_calls = 0
    evewho_calls = 0
    errors: list[str] = []

    try:
        esi_budget = max(10, int(runtime.get("tracked_alliance_esi_budget") or DEFAULT_ESI_BUDGET))
        evewho_budget = max(5, int(runtime.get("tracked_alliance_evewho_budget") or DEFAULT_EVEWHO_BUDGET))
        enrich_batch = max(5, int(runtime.get("tracked_alliance_enrich_batch") or DEFAULT_ENRICH_BATCH))
        user_agent = str(runtime.get("evewho_user_agent") or "SupplyCore Intelligence Platform / contact@supplycore.app")

        neo4j_config = Neo4jConfig.from_runtime(neo4j_raw or {})
        if not neo4j_config.enabled:
            finish_job_run(db, job, status="skipped", rows_processed=0, rows_written=0, error_text="Neo4j disabled")
            return JobResult.skipped(job_key=JOB_KEY, reason="neo4j-disabled").to_dict()

        neo4j = Neo4jClient(neo4j_config)
        rate_limit = max(1, int(runtime.get("evewho_rate_limit_requests") or 0)) if runtime.get("evewho_rate_limit_requests") else None
        evewho = EveWhoAdapter(user_agent, rate_limit_requests=rate_limit) if rate_limit else EveWhoAdapter(user_agent)
        esi = EsiClient(user_agent=user_agent)

        checkpoint = _load_checkpoint(db)
        phase = str(checkpoint.get("phase") or "corps")
        resume_alliance_id = int(checkpoint.get("alliance_id") or 0)
        resume_corp_id = int(checkpoint.get("corp_id") or 0)
        resume_char_offset = int(checkpoint.get("char_offset") or 0)
        total_written = int(checkpoint.get("written") or 0)

        # Fetch tracked alliances
        alliance_rows = db.fetch_all(
            "SELECT alliance_id, label FROM killmail_tracked_alliances WHERE is_active = 1 ORDER BY alliance_id ASC"
        )
        alliance_ids = [int(r["alliance_id"]) for r in alliance_rows if int(r.get("alliance_id") or 0) > 0]
        log.info("Found %d tracked alliances", len(alliance_ids))

        if not alliance_ids:
            finish_job_run(db, job, status="skipped", rows_processed=0, rows_written=0, error_text="No tracked alliances")
            return JobResult.skipped(job_key=JOB_KEY, reason="no-tracked-alliances").to_dict()

        # ── PHASE 1: Fetch corp lists & upsert members ────────────

        if phase == "corps":
            log.info("Phase: Fetching corp member lists for tracked alliances")

            phase_complete = True

            for alliance_id in alliance_ids:
                if evewho_calls >= evewho_budget or esi_calls >= esi_budget:
                    phase_complete = False
                    break

                if resume_alliance_id > 0 and alliance_id < resume_alliance_id:
                    continue

                corp_ids = _fetch_alliance_corp_ids(esi, alliance_id)
                esi_calls += 1

                if not corp_ids:
                    log.warning("No corps found for tracked alliance %d", alliance_id)
                    errors.append(f"No corps for alliance {alliance_id}")
                    continue

                alliances_processed += 1
                log.info("Alliance %d: %d corps", alliance_id, len(corp_ids))

                for corp_id in corp_ids:
                    if evewho_calls >= evewho_budget or esi_calls >= esi_budget:
                        _save_checkpoint(db, {
                            "phase": "corps", "alliance_id": alliance_id,
                            "corp_id": corp_id, "written": total_written,
                        }, "running", total_written)
                        phase_complete = False
                        break

                    if alliance_id == resume_alliance_id and resume_corp_id > 0 and corp_id <= resume_corp_id:
                        continue

                    # Fetch current members via EveWho corplist
                    _, corplist_payload = evewho.fetch_corplist(corp_id)
                    evewho_calls += 1
                    members = _extract_corplist_members(corplist_payload)
                    total_members += len(members)

                    # Upsert members into Neo4j
                    try:
                        written = _upsert_corp_members(neo4j, alliance_id, corp_id, members)
                        total_written += written

                        # Clean stale membership edges
                        current_ids = [m["character_id"] for m in members]
                        _clean_stale_current_corp(neo4j, corp_id, current_ids)

                        corps_processed += 1
                        log.info("Corp %d: %d members", corp_id, len(members))
                    except Neo4jError as e:
                        log.warning("Neo4j error for corp %d: %s", corp_id, e)
                        errors.append(f"Neo4j error corp {corp_id}: {e}")
                        continue

                    _save_checkpoint(db, {
                        "phase": "corps", "alliance_id": alliance_id,
                        "corp_id": corp_id, "written": total_written,
                    }, "running", total_written)
                else:
                    resume_corp_id = 0
                    continue
                break

            if phase_complete:
                phase = "enrich"
                resume_alliance_id = 0
                resume_corp_id = 0
                resume_char_offset = 0
                _save_checkpoint(db, {
                    "phase": "enrich", "char_offset": 0, "written": total_written,
                }, "running", total_written)
                log.info(
                    "Corp phase complete: %d alliances, %d corps, %d members",
                    alliances_processed, corps_processed, total_members,
                )

        # ── PHASE 2: ESI enrichment of discovered members ─────────

        if phase == "enrich" and esi_calls < esi_budget:
            log.info("Phase: ESI enrichment (offset %d, batch %d)", resume_char_offset, enrich_batch)

            # Query Neo4j for characters that belong to tracked alliance corps
            # but haven't been enriched yet.
            tracked_alliance_id_list = alliance_ids
            try:
                unenriched = neo4j.query(
                    """
                    MATCH (c:Character)-[:CURRENT_CORP]->(corp:Corporation)-[:PART_OF]->(a:Alliance)
                    WHERE a.alliance_id IN $allianceIds
                      AND (c.enriched IS NULL OR c.enriched = false)
                    RETURN c.character_id AS character_id
                    ORDER BY c.character_id ASC
                    SKIP $offset
                    LIMIT $limit
                    """,
                    {
                        "allianceIds": tracked_alliance_id_list,
                        "offset": resume_char_offset,
                        "limit": enrich_batch,
                    },
                )
            except Neo4jError as e:
                log.warning("Neo4j query for unenriched characters failed: %s", e)
                unenriched = []

            char_ids = [int(r["character_id"]) for r in unenriched if r.get("character_id")]
            log.info("Found %d unenriched characters to process", len(char_ids))

            for char_id in char_ids:
                if esi_calls + 2 > esi_budget:  # need 2 calls per char (info + history)
                    break

                try:
                    info_resp = esi.get(f"/latest/characters/{char_id}/")
                    esi_calls += 1
                    if not info_resp.ok or not isinstance(info_resp.body, dict):
                        log.warning("ESI character info failed for %d (status %s)", char_id, info_resp.status_code)
                        continue

                    hist_resp = esi.get(f"/latest/characters/{char_id}/corporationhistory/")
                    esi_calls += 1
                    history = _compute_end_dates(hist_resp.body) if hist_resp.ok and isinstance(hist_resp.body, list) else []

                    _enrich_character(neo4j, char_id, info_resp.body, history)
                    total_enriched += 1

                except Neo4jError as e:
                    log.warning("Neo4j error enriching character %d: %s", char_id, e)
                except Exception as e:
                    log.warning("Error enriching character %d: %s", char_id, e)

            # Update offset for next run
            new_offset = resume_char_offset + len(char_ids)
            if len(char_ids) < enrich_batch:
                # All caught up — reset for next full cycle
                _save_checkpoint(db, {}, "success", total_written)
                log.info("Enrichment complete: %d characters enriched this run", total_enriched)
            else:
                _save_checkpoint(db, {
                    "phase": "enrich", "char_offset": new_offset, "written": total_written,
                }, "running", total_written)
                log.info(
                    "Enrichment paused at offset %d: %d enriched this run",
                    new_offset, total_enriched,
                )

        duration_ms = int((time.perf_counter() - started) * 1000)
        error_text = "; ".join(errors) if errors else None

        finish_job_run(
            db, job,
            status="success",
            rows_processed=total_members,
            rows_written=total_written,
            error_text=error_text,
            meta={
                "alliances_processed": alliances_processed,
                "corps_processed": corps_processed,
                "members_found": total_members,
                "graph_written": total_written,
                "chars_enriched": total_enriched,
                "esi_calls": esi_calls,
                "evewho_calls": evewho_calls,
                "esi_budget": esi_budget,
                "evewho_budget": evewho_budget,
                "duration_ms": duration_ms,
            },
        )
        return JobResult.success(
            job_key=JOB_KEY,
            rows_seen=total_members,
            rows_written=total_written + total_enriched,
            duration_ms=duration_ms,
            summary=(
                f"Tracked alliance sync | "
                f"{alliances_processed} alliances, {corps_processed} corps | "
                f"{total_written} members graphed, {total_enriched} ESI-enriched | "
                f"{esi_calls}/{esi_budget} ESI, {evewho_calls}/{evewho_budget} EveWho, {duration_ms}ms"
            ),
        ).to_dict()

    except Exception as e:
        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(
            db, job,
            status="failed",
            rows_processed=total_members,
            rows_written=total_written,
            error_text=str(e)[:500],
        )
        return JobResult.failed(
            job_key=JOB_KEY,
            error=str(e),
            meta={
                "members_found": total_members,
                "written": total_written,
                "esi_calls": esi_calls,
                "evewho_calls": evewho_calls,
            },
        ).to_dict()
