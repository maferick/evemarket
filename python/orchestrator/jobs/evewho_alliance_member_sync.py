"""EveWho Alliance Member Sync — iterative graph crawl of opponent orgs.

Two-phase approach per run:

Phase 1 (Org-Level Sweep):
  For each opponent alliance, fetch corporation IDs via ESI, then for each corp
  fetch members via EveWho /api/corplist and movement events via /api/corpjoined
  and /api/corpdeparted.  Batch-upsert Character, Corporation, Alliance nodes
  and relationships into Neo4j.
  Queue departed/new characters into enrichment_queue for deep enrichment.

Phase 2 (Character Discovery):
  Pick characters from enrichment_queue and fetch their full history via
  /api/character/{id}.  Extract corp/alliance IDs from history and insert
  newly-discovered orgs into opponent tables for the next Phase 1 sweep.

Over successive runs, the graph crawl expands outward: org-level data fills
membership for many characters at once, while character-level data reveals
new orgs, which feed back into the org-level sweep.
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

JOB_KEY = "evewho_alliance_member_sync"
DATASET_KEY = "evewho_alliance_member_sync_cursor"

# Defaults — overridable via runtime config
DEFAULT_API_BUDGET = 100          # max API calls per invocation
DEFAULT_CHAR_BUDGET = 30          # max character enrichments in phase 2
DEFAULT_DEPARTED_PRIORITY = 8.0   # enrichment_queue priority for departed chars
DEFAULT_NEW_MEMBER_PRIORITY = 2.0 # enrichment_queue priority for new members


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
        cp = json.loads(str(row["last_cursor"]))
    except (ValueError, TypeError):
        return {}
    # Migrate old format
    if "last_char_id" in cp:
        return {}
    return cp


def _save_checkpoint(db: SupplyCoreDb, checkpoint: dict[str, Any], status: str, rows_written: int) -> None:
    cursor_str = json.dumps(checkpoint, default=str)
    _sync_state_upsert(db, cursor_str, status, rows_written)


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

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


def _fetch_alliance_corp_ids(esi: EsiClient, alliance_id: int) -> list[int]:
    """Fetch corporation IDs belonging to an alliance via ESI.

    Returns sorted list of corporation IDs, or empty list on failure.
    """
    resp = esi.get(f"/latest/alliances/{alliance_id}/corporations/")
    if resp.ok and isinstance(resp.body, list):
        return sorted(int(c) for c in resp.body if isinstance(c, (int, float)) and int(c) > 0)
    log.warning("ESI alliance corps failed for %d (status %s)", alliance_id, resp.status_code)
    return []


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
                members.append({
                    "character_id": char_id,
                    "name": name,
                })
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return members


def _extract_corp_events(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Parse a corpjoined or corpdeparted response into a list of events.

    Returns [{"character_id": int, "name": str, "date": str}, ...].
    """
    if not payload:
        return []

    events: list[dict[str, Any]] = []
    seen_ids: set[int] = set()

    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            char_id = _safe_int(current, ["character_id", "characterID", "char_id"])
            if char_id and char_id > 0 and char_id not in seen_ids:
                seen_ids.add(char_id)
                name = str(current.get("name") or current.get("characterName") or "")
                date_raw = str(
                    current.get("date") or current.get("datetime")
                    or current.get("joinedDate") or current.get("departedDate")
                    or ""
                ).strip().replace(" ", "T").replace("/", "-")
                events.append({
                    "character_id": char_id,
                    "name": name,
                    "date": date_raw or None,
                })
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)

    return events


# ---------------------------------------------------------------------------
# Neo4j batch writes
# ---------------------------------------------------------------------------

def _batch_upsert_corp_members(
    neo4j: Neo4jClient,
    alliance_id: int,
    corp_id: int,
    members: list[dict[str, Any]],
) -> int:
    """Batch MERGE all current members of a corp into Neo4j.

    Creates/updates Character nodes, Corporation node, Alliance node,
    CURRENT_CORP and PART_OF relationships.  Returns count of members written.
    """
    if not members:
        return 0

    is_npc = corp_id < 2_000_000
    member_params = [{"character_id": m["character_id"], "name": m["name"]} for m in members]

    # Upsert characters + CURRENT_CORP
    neo4j.query(
        """
        UNWIND $members AS m
        MERGE (c:Character {character_id: m.character_id})
        SET c.name = m.name, c.org_synced_at = datetime()
        WITH c
        MERGE (corp:Corporation {corporation_id: $corpId})
        ON CREATE SET corp.is_npc = $isNpc
        MERGE (c)-[:CURRENT_CORP]->(corp)
        """,
        {"members": member_params, "corpId": corp_id, "isNpc": is_npc},
    )

    # Link corp → alliance
    if alliance_id > 0:
        neo4j.query(
            """
            MERGE (a:Alliance {alliance_id: $allianceId})
            WITH a
            MERGE (corp:Corporation {corporation_id: $corpId})
            MERGE (corp)-[r:PART_OF]->(a)
            ON CREATE SET r.as_of = datetime()
            ON MATCH  SET r.as_of = datetime()
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


def _apply_join_events(
    neo4j: Neo4jClient,
    corp_id: int,
    joined: list[dict[str, Any]],
) -> None:
    """Create MEMBER_OF relationships from corpjoined data."""
    entries = [e for e in joined if e.get("date")]
    if not entries:
        return
    is_npc = corp_id < 2_000_000
    neo4j.query(
        """
        UNWIND $entries AS j
        MERGE (c:Character {character_id: j.character_id})
        ON CREATE SET c.name = j.name
        WITH c, j
        MERGE (corp:Corporation {corporation_id: $corpId})
        ON CREATE SET corp.is_npc = $isNpc
        MERGE (c)-[r:MEMBER_OF {corporation_id: $corpId, from: datetime(j.date)}]->(corp)
        """,
        {"entries": [{"character_id": e["character_id"], "name": e["name"], "date": e["date"]} for e in entries],
         "corpId": corp_id, "isNpc": is_npc},
    )


def _apply_depart_events(
    neo4j: Neo4jClient,
    corp_id: int,
    departed: list[dict[str, Any]],
) -> None:
    """Close open MEMBER_OF relationships from corpdeparted data."""
    entries = [e for e in departed if e.get("date")]
    if not entries:
        return
    neo4j.query(
        """
        UNWIND $entries AS d
        MATCH (c:Character {character_id: d.character_id})-[r:MEMBER_OF]->(corp:Corporation {corporation_id: $corpId})
        WHERE r.to IS NULL
        SET r.to = datetime(d.date)
        """,
        {"entries": [{"character_id": e["character_id"], "date": e["date"]} for e in entries],
         "corpId": corp_id},
    )


# ---------------------------------------------------------------------------
# Character enrichment (Phase 2) — reused from original implementation
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
            MERGE (corp)-[r:PART_OF]->(a)
            ON CREATE SET r.as_of = datetime()
            ON MATCH  SET r.as_of = datetime()
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
# Enrichment queue helpers
# ---------------------------------------------------------------------------

def _queue_for_enrichment(db: SupplyCoreDb, character_ids: list[int], priority: float) -> int:
    """Insert characters into enrichment_queue. Returns count of newly queued."""
    if not character_ids:
        return 0
    queued = 0
    for char_id in character_ids:
        db.execute(
            """
            INSERT INTO enrichment_queue (character_id, status, priority, queued_at)
            VALUES (%s, 'pending', %s, UTC_TIMESTAMP())
            ON DUPLICATE KEY UPDATE
                priority = GREATEST(priority, VALUES(priority)),
                status = CASE WHEN status = 'done' THEN 'pending' ELSE status END
            """,
            (char_id, priority),
        )
        queued += 1
    return queued


def _claim_enrichment_batch(db: SupplyCoreDb, batch_size: int) -> list[int]:
    """Claim a batch of pending characters from enrichment_queue."""
    rows = db.fetch_all(
        """
        SELECT character_id
        FROM enrichment_queue
        WHERE status = 'pending' AND attempts < 3
        ORDER BY priority DESC, queued_at ASC
        LIMIT %s
        """,
        (batch_size,),
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
    return ids


def _mark_enrichment_done(db: SupplyCoreDb, character_id: int) -> None:
    db.execute(
        "UPDATE enrichment_queue SET status = 'done', done_at = UTC_TIMESTAMP() WHERE character_id = %s",
        (character_id,),
    )


def _mark_enrichment_failed(db: SupplyCoreDb, character_id: int, error: str) -> None:
    db.execute(
        """
        UPDATE enrichment_queue
        SET status = IF(attempts >= 3, 'failed', 'pending'),
            last_error = %s
        WHERE character_id = %s
        """,
        (str(error)[:500], character_id),
    )


# ---------------------------------------------------------------------------
# Org discovery from character history
# ---------------------------------------------------------------------------

def _discover_orgs_from_history(
    db: SupplyCoreDb,
    info: dict[str, Any],
    history: list[dict[str, Any]],
) -> tuple[int, int]:
    """Insert newly-discovered corps/alliances into opponent tables.

    Returns (corps_discovered, alliances_discovered).
    """
    corps_found = 0
    alliances_found = 0

    # Current corp/alliance from character info
    corp_id = int(info.get("corporation_id") or 0)
    alliance_id = int(info.get("alliance_id") or 0)

    corp_ids: set[int] = set()
    alliance_ids: set[int] = set()

    if corp_id > 0 and corp_id >= 2_000_000:
        corp_ids.add(corp_id)
    if alliance_id > 0:
        alliance_ids.add(alliance_id)

    # Corps from history
    for h in history:
        h_corp_id = int(h.get("corporation_id") or 0)
        if h_corp_id > 0 and h_corp_id >= 2_000_000:
            corp_ids.add(h_corp_id)

    for cid in corp_ids:
        affected = db.execute(
            """
            INSERT IGNORE INTO killmail_opponent_corporations
                (corporation_id, label, is_active, source)
            VALUES (%s, NULL, 1, 'discovered')
            """,
            (cid,),
        )
        if affected > 0:
            corps_found += 1

    for aid in alliance_ids:
        affected = db.execute(
            """
            INSERT IGNORE INTO killmail_opponent_alliances
                (alliance_id, label, is_active, source)
            VALUES (%s, NULL, 1, 'discovered')
            """,
            (aid,),
        )
        if affected > 0:
            alliances_found += 1

    return corps_found, alliances_found


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
    """Iterative graph crawl: org-level sweep then character discovery."""
    job = start_job_run(db, JOB_KEY)
    started = time.perf_counter()
    runtime = runtime or {}

    total_written = 0
    total_members_found = 0
    total_queued = 0
    total_enriched = 0
    corps_discovered = 0
    alliances_discovered = 0
    alliances_processed = 0
    corps_processed = 0
    api_calls = 0
    errors: list[str] = []

    try:
        api_budget = max(10, int(runtime.get("evewho_alliance_api_budget") or DEFAULT_API_BUDGET))
        char_budget = max(5, int(runtime.get("evewho_alliance_char_budget") or DEFAULT_CHAR_BUDGET))
        user_agent = str(runtime.get("evewho_user_agent") or "SupplyCore Intelligence Platform / contact@supplycore.app")

        neo4j_config = Neo4jConfig.from_runtime(neo4j_raw or {})
        if not neo4j_config.enabled:
            finish_job_run(db, job, status="skipped", rows_processed=0, rows_written=0, error_text="Neo4j disabled")
            return JobResult.skipped(job_key=JOB_KEY, reason="neo4j-disabled").to_dict()

        neo4j = Neo4jClient(neo4j_config)
        rate_limit = max(1, int(runtime.get("evewho_rate_limit_requests") or 0)) if runtime.get("evewho_rate_limit_requests") else None
        adapter = EveWhoAdapter(user_agent, rate_limit_requests=rate_limit) if rate_limit else EveWhoAdapter(user_agent)
        esi = EsiClient(user_agent=user_agent)

        checkpoint = _load_checkpoint(db)
        phase = int(checkpoint.get("p") or 1)
        resume_alliance_id = int(checkpoint.get("a") or 0)
        resume_corp_id = int(checkpoint.get("c") or 0)
        total_written = int(checkpoint.get("w") or 0)

        # ── PHASE 1: Org-Level Sweep ──────────────────────────────

        if phase <= 1:
            log.info("Phase 1: Org-level sweep")

            alliance_rows = db.fetch_all(
                "SELECT alliance_id, label FROM killmail_opponent_alliances WHERE is_active = 1 ORDER BY alliance_id ASC"
            )
            alliance_ids = [int(r["alliance_id"]) for r in alliance_rows if int(r.get("alliance_id") or 0) > 0]
            log.info("Found %d opponent alliances to sweep", len(alliance_ids))

            phase1_done = True

            for alliance_id in alliance_ids:
                if api_calls >= api_budget:
                    phase1_done = False
                    break

                # Skip alliances already completed this cycle
                if resume_alliance_id > 0 and alliance_id < resume_alliance_id:
                    continue

                log.info("Fetching corp IDs for alliance %d via ESI ...", alliance_id)
                corp_ids = _fetch_alliance_corp_ids(esi, alliance_id)
                api_calls += 1

                if not corp_ids:
                    log.warning("No corps found for alliance %d", alliance_id)
                    errors.append(f"No corps for alliance {alliance_id}")
                    continue

                alliances_processed += 1
                log.info("Alliance %d: %d corps to process", alliance_id, len(corp_ids))

                for corp_id in corp_ids:
                    if api_calls >= api_budget:
                        # Save checkpoint at this corp
                        _save_checkpoint(db, {
                            "p": 1, "a": alliance_id, "c": corp_id, "w": total_written,
                        }, "running", total_written)
                        phase1_done = False
                        break

                    # Skip corps already completed within this alliance
                    if alliance_id == resume_alliance_id and resume_corp_id > 0 and corp_id <= resume_corp_id:
                        continue

                    # Fetch current members for this corp
                    _, corplist_payload = adapter.fetch_corplist(corp_id)
                    api_calls += 1
                    members = _extract_corplist_members(corplist_payload)
                    total_members_found += len(members)

                    # Fetch join/depart events for this corp
                    _, joined_payload = adapter.fetch_corpjoined(corp_id)
                    api_calls += 1
                    _, departed_payload = adapter.fetch_corpdeparted(corp_id)
                    api_calls += 1

                    joined_events = _extract_corp_events(joined_payload)
                    departed_events = _extract_corp_events(departed_payload)

                    # Batch upsert members to Neo4j
                    try:
                        written = _batch_upsert_corp_members(neo4j, alliance_id, corp_id, members)
                        total_written += written

                        # Apply movement events
                        _apply_join_events(neo4j, corp_id, joined_events)
                        _apply_depart_events(neo4j, corp_id, departed_events)

                        # Clean stale CURRENT_CORP edges
                        current_ids = [m["character_id"] for m in members]
                        _clean_stale_current_corp(neo4j, corp_id, current_ids)

                        corps_processed += 1
                        log.info(
                            "Corp %d: %d members, %d joined, %d departed",
                            corp_id, len(members), len(joined_events), len(departed_events),
                        )
                    except Neo4jError as e:
                        log.warning("Neo4j error for corp %d: %s", corp_id, e)
                        errors.append(f"Neo4j error corp {corp_id}: {e}")
                        continue

                    # Queue departed characters for deep enrichment
                    departed_char_ids = [e["character_id"] for e in departed_events]
                    if departed_char_ids:
                        total_queued += _queue_for_enrichment(db, departed_char_ids, DEFAULT_DEPARTED_PRIORITY)

                    # Queue current members who haven't been enriched yet
                    new_member_ids = [m["character_id"] for m in members]
                    if new_member_ids:
                        total_queued += _queue_for_enrichment(db, new_member_ids, DEFAULT_NEW_MEMBER_PRIORITY)

                    # Checkpoint after each corp
                    _save_checkpoint(db, {
                        "p": 1, "a": alliance_id, "c": corp_id, "w": total_written,
                    }, "running", total_written)
                else:
                    # All corps in this alliance completed — reset resume point
                    resume_corp_id = 0
                    continue
                # Inner loop broke (budget exhausted)
                break

            # Also sweep standalone opponent corps not tied to any alliance
            if phase1_done and api_calls < api_budget:
                standalone_rows = db.fetch_all(
                    "SELECT corporation_id, label FROM killmail_opponent_corporations WHERE is_active = 1 ORDER BY corporation_id ASC"
                )
                for row in standalone_rows:
                    if api_calls >= api_budget:
                        phase1_done = False
                        break
                    scorp_id = int(row.get("corporation_id") or 0)
                    if scorp_id <= 0:
                        continue

                    _, joined_payload = adapter.fetch_corpjoined(scorp_id)
                    api_calls += 1
                    _, departed_payload = adapter.fetch_corpdeparted(scorp_id)
                    api_calls += 1

                    joined_events = _extract_corp_events(joined_payload)
                    departed_events = _extract_corp_events(departed_payload)

                    try:
                        # For standalone corps we don't know the alliance, use 0
                        # We still create MEMBER_OF relationships from join/depart
                        _apply_join_events(neo4j, scorp_id, joined_events)
                        _apply_depart_events(neo4j, scorp_id, departed_events)
                        corps_processed += 1
                    except Neo4jError as e:
                        log.warning("Neo4j error for standalone corp %d: %s", scorp_id, e)

                    departed_char_ids = [e["character_id"] for e in departed_events]
                    if departed_char_ids:
                        total_queued += _queue_for_enrichment(db, departed_char_ids, DEFAULT_DEPARTED_PRIORITY)

            if phase1_done:
                phase = 2
                resume_alliance_id = 0
                resume_corp_id = 0
                log.info("Phase 1 complete: %d alliances, %d corps processed", alliances_processed, corps_processed)

        # ── PHASE 2: Character Discovery ──────────────────────────

        if phase >= 2 and api_calls < api_budget:
            remaining_char_budget = min(char_budget, api_budget - api_calls)
            log.info("Phase 2: Character discovery (budget: %d)", remaining_char_budget)

            char_ids = _claim_enrichment_batch(db, remaining_char_budget)
            if not char_ids:
                log.info("Phase 2: No characters in enrichment queue")
            else:
                log.info("Phase 2: Claimed %d characters for enrichment", len(char_ids))

            for char_id in char_ids:
                if api_calls >= api_budget:
                    break

                try:
                    _, char_payload = adapter.fetch_character(char_id)
                    api_calls += 1

                    if not char_payload:
                        _mark_enrichment_failed(db, char_id, "Empty response from EveWho")
                        continue

                    # Extract info
                    info_list = char_payload.get("info")
                    if isinstance(info_list, list) and info_list:
                        info = info_list[0]
                    elif isinstance(char_payload.get("character_id"), int):
                        info = char_payload
                    else:
                        _mark_enrichment_failed(db, char_id, "No info in response")
                        continue

                    history = char_payload.get("history") or char_payload.get("corporation_history") or []
                    if not isinstance(history, list):
                        history = []

                    _enrich_character_to_neo4j(neo4j, char_id, info, history)
                    total_enriched += 1

                    # Discover new orgs from this character's history
                    c_disc, a_disc = _discover_orgs_from_history(db, info, history)
                    corps_discovered += c_disc
                    alliances_discovered += a_disc
                    if c_disc or a_disc:
                        log.info(
                            "Character %d revealed %d new corps, %d new alliances",
                            char_id, c_disc, a_disc,
                        )

                    _mark_enrichment_done(db, char_id)

                except Neo4jError as e:
                    _mark_enrichment_failed(db, char_id, str(e))
                    log.warning("Neo4j error for character %d: %s", char_id, e)
                except Exception as e:
                    _mark_enrichment_failed(db, char_id, str(e))
                    log.warning("Error enriching character %d: %s", char_id, e)

            # Phase 2 done — reset checkpoint for next full cycle
            _save_checkpoint(db, {}, "success", total_written)
            log.info(
                "Phase 2 complete: %d enriched, %d corps discovered, %d alliances discovered",
                total_enriched, corps_discovered, alliances_discovered,
            )

        duration_ms = int((time.perf_counter() - started) * 1000)
        error_text = "; ".join(errors) if errors else None

        finish_job_run(
            db, job,
            status="success",
            rows_processed=total_members_found,
            rows_written=total_written,
            error_text=error_text,
            meta={
                "phase_reached": phase,
                "alliances_processed": alliances_processed,
                "corps_processed": corps_processed,
                "members_found": total_members_found,
                "graph_written": total_written,
                "queued_for_enrichment": total_queued,
                "chars_enriched": total_enriched,
                "corps_discovered": corps_discovered,
                "alliances_discovered": alliances_discovered,
                "api_calls": api_calls,
                "api_budget": api_budget,
                "duration_ms": duration_ms,
            },
        )
        return JobResult.success(
            job_key=JOB_KEY,
            rows_seen=total_members_found,
            rows_written=total_written + total_enriched,
            summary=(
                f"Graph crawl: Phase {'1→2' if phase >= 2 else '1'} | "
                f"{alliances_processed} alliances, {corps_processed} corps swept | "
                f"{total_written} members graphed, {total_queued} queued | "
                f"{total_enriched} chars enriched, {corps_discovered}c/{alliances_discovered}a discovered | "
                f"{api_calls}/{api_budget} API calls, {duration_ms}ms"
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
            meta={
                "rows_seen": total_members_found,
                "rows_written": total_written,
                "api_calls": api_calls,
            },
        ).to_dict()
