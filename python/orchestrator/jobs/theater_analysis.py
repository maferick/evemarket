"""Theater Intelligence — Phase 3: Core theater analysis.

For each theater produced by ``theater_clustering``, computes:
  - Timeline buckets (kills/ISK per minute with side breakdown + momentum)
  - Alliance summary (per-side participation, kills, losses, efficiency)
  - Per-participant stats (kills, deaths, damage, role inference)
  - Turning point detection (momentum shifts)
  - Anomaly scoring at the theater level
  - Refines theater aggregate columns (total_kills, total_isk)

Populates: ``theater_timeline``, ``theater_alliance_summary``,
           ``theater_participants``, ``battle_turning_points``
Updates:   ``theaters`` (total_kills, total_isk, anomaly_score)
"""

from __future__ import annotations

import math
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.config import resolve_app_root  # noqa: F401
    from orchestrator.db import SupplyCoreDb
    from orchestrator.eve_constants import (
        FLEET_FUNCTION_BY_GROUP,
        HULL_WEIGHT,
        ROLE_COMBAT_WEIGHT,
        ROLE_MULTIPLIER_WEIGHT,
        SHIP_SIZE_BY_GROUP,
    )
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import finish_job_run, start_job_run
else:
    from ..config import resolve_app_root  # noqa: F401
    from ..db import SupplyCoreDb
    from ..eve_constants import (
        FLEET_FUNCTION_BY_GROUP,
        HULL_WEIGHT,
        ROLE_COMBAT_WEIGHT,
        ROLE_MULTIPLIER_WEIGHT,
        SHIP_SIZE_BY_GROUP,
    )
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import finish_job_run, start_job_run

# ── Configuration ────────────────────────────────────────────────────────────

TIMELINE_BUCKET_SECONDS = 60  # 1-minute buckets
MOMENTUM_SMOOTHING_WINDOW = 5  # buckets for momentum moving average
TURNING_POINT_MAGNITUDE_THRESHOLD = 0.3  # minimum momentum swing to flag
BATCH_SIZE = 500

# Engagement expansion: extend the theater's time window by this many seconds
# on each end when querying for additional killmails in the theater's systems.
# This captures kills at engagement boundaries (early scouts, stragglers,
# cleanup kills, third-party opportunists).
ENGAGEMENT_EXPANSION_MARGIN_SECONDS = 5 * 60  # 5 minutes


def _load_expansion_settings(db: SupplyCoreDb) -> dict[str, int]:
    """Load engagement expansion settings from app_settings."""
    rows = db.fetch_all(
        "SELECT setting_key, setting_value FROM app_settings "
        "WHERE setting_key LIKE 'theater_expansion_%%'"
    )
    raw = {r["setting_key"]: r["setting_value"] for r in rows}
    return {
        "margin_seconds": int(raw.get(
            "theater_expansion_margin_seconds", ENGAGEMENT_EXPANSION_MARGIN_SECONDS,
        )),
    }


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d > 0 else default


def _theater_log(runtime: dict[str, Any] | None, event: str, payload: dict[str, Any]) -> None:
    log_path = str(((runtime or {}).get("log_file") or "")).strip()
    if log_path == "":
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_dumps_safe(record) + "\n")


def _parse_dt(val: Any) -> datetime:
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=UTC)
    return datetime.strptime(str(val), "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)


# ── Data loaders ────────────────────────────────────────────────────────────

def _load_theaters(db: SupplyCoreDb) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT theater_id, start_time, end_time, duration_seconds
        FROM theaters
        WHERE locked_at IS NULL
        ORDER BY start_time ASC
        """
    )


def _load_theater_system_ids(db: SupplyCoreDb, theater_id: str) -> list[int]:
    """Load system IDs for a theater (from theater_systems table)."""
    rows = db.fetch_all(
        "SELECT system_id FROM theater_systems WHERE theater_id = %s",
        (theater_id,),
    )
    return [int(r["system_id"]) for r in rows if int(r.get("system_id") or 0) > 0]


def _load_theater_battle_ids(db: SupplyCoreDb, theater_id: str) -> list[str]:
    rows = db.fetch_all(
        "SELECT battle_id FROM theater_battles WHERE theater_id = %s",
        (theater_id,),
    )
    return [str(r["battle_id"]) for r in rows]


_KM_SELECT_COLS = """
                ke.killmail_id,
                ke.sequence_id,
                ke.solar_system_id AS system_id,
                ke.effective_killmail_at AS killmail_time,
                ke.battle_id,
                ke.victim_character_id,
                ke.victim_corporation_id,
                ke.victim_alliance_id,
                ke.victim_ship_type_id,
                COALESCE(ke.zkb_total_value, 0) AS total_value,
                COALESCE(ke.zkb_npc, 0) AS zkb_npc,
                ka.character_id AS attacker_character_id,
                ka.corporation_id AS attacker_corporation_id,
                ka.alliance_id AS attacker_alliance_id,
                ka.ship_type_id AS attacker_ship_type_id,
                ka.damage_done AS attacker_damage_done,
                ka.final_blow AS attacker_final_blow
"""


def _load_killmails_for_battles(db: SupplyCoreDb, battle_ids: list[str]) -> list[dict[str, Any]]:
    """Load killmail events for the given battles."""
    if not battle_ids:
        return []
    all_rows: list[dict[str, Any]] = []
    for offset in range(0, len(battle_ids), BATCH_SIZE):
        chunk = battle_ids[offset:offset + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(chunk))
        rows = db.fetch_all(
            f"""
            SELECT {_KM_SELECT_COLS}
            FROM killmail_events ke
            LEFT JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
            WHERE ke.battle_id IN ({placeholders})
            ORDER BY ke.effective_killmail_at ASC
            """,
            tuple(chunk),
        )
        all_rows.extend(rows)
    return all_rows


def _load_expanded_killmails(
    db: SupplyCoreDb,
    battle_ids: list[str],
    system_ids: list[int],
    time_start: str,
    time_end: str,
    margin_seconds: int = ENGAGEMENT_EXPANSION_MARGIN_SECONDS,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Load killmails for the theater with engagement expansion.

    Returns (killmails, diagnostics) where killmails includes:
    1. All killmails from the theater's constituent battles (base set)
    2. Additional killmails in the theater's systems within the expanded time
       window that share at least one entity (character, corporation, or
       alliance) with the base set — the overlap guard prevents pulling in
       unrelated kills from busy systems.

    Diagnostics dict contains counts for auditing:
    - base_killmail_ids: unique killmails from battle_id match
    - expanded_killmail_ids: unique killmails from system+time query
    - expansion_passed_overlap: new killmails that passed the overlap check
    - expansion_rejected_no_overlap: new killmails rejected (no entity match)
    """
    # Step 1: Load base killmails from theater battles
    base_rows = _load_killmails_for_battles(db, battle_ids)

    # Collect unique killmail IDs and entity fingerprints from base set
    base_km_ids: set[int] = set()
    base_character_ids: set[int] = set()
    base_corporation_ids: set[int] = set()
    base_alliance_ids: set[int] = set()
    for row in base_rows:
        km_id = int(row.get("killmail_id") or 0)
        if km_id > 0:
            base_km_ids.add(km_id)
        for prefix in ("victim_", "attacker_"):
            cid = int(row.get(f"{prefix}character_id") or 0)
            corp = int(row.get(f"{prefix}corporation_id") or 0)
            ally = int(row.get(f"{prefix}alliance_id") or 0)
            if cid > 0:
                base_character_ids.add(cid)
            if corp > 0:
                base_corporation_ids.add(corp)
            if ally > 0:
                base_alliance_ids.add(ally)

    # Step 2: Query for additional killmails in the same systems + expanded window
    if not system_ids or not time_start or not time_end:
        return base_rows, {
            "base_killmail_ids": len(base_km_ids),
            "expanded_killmail_ids": 0,
            "expansion_passed_overlap": 0,
            "expansion_rejected_no_overlap": 0,
        }

    try:
        dt_start = datetime.strptime(time_start, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        dt_end = datetime.strptime(time_end, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except (ValueError, TypeError):
        return base_rows, {
            "base_killmail_ids": len(base_km_ids),
            "expanded_killmail_ids": 0,
            "expansion_passed_overlap": 0,
            "expansion_rejected_no_overlap": 0,
        }

    expanded_start = (dt_start - timedelta(seconds=margin_seconds)).strftime("%Y-%m-%d %H:%M:%S")
    expanded_end = (dt_end + timedelta(seconds=margin_seconds)).strftime("%Y-%m-%d %H:%M:%S")

    sys_placeholders = ",".join(["%s"] * len(system_ids))
    expansion_rows = db.fetch_all(
        f"""
        SELECT {_KM_SELECT_COLS}
        FROM killmail_events ke
        LEFT JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
        WHERE ke.solar_system_id IN ({sys_placeholders})
          AND ke.effective_killmail_at BETWEEN %s AND %s
        ORDER BY ke.effective_killmail_at ASC
        """,
        (*system_ids, expanded_start, expanded_end),
    )

    # Step 3: Filter expansion rows — only include killmails that share at
    # least one entity (character, corporation, or alliance) with the base set.
    # This prevents contaminating the theater with unrelated noise in busy
    # systems (e.g. staging systems, trade hubs).
    expansion_km_ids: set[int] = set()
    new_km_ids_passed: set[int] = set()
    new_km_ids_rejected: set[int] = set()

    # First pass: determine which new killmail IDs pass the overlap check.
    # A killmail passes if ANY of its rows (victim or attacker) share an entity.
    new_candidate_ids: set[int] = set()
    for row in expansion_rows:
        km_id = int(row.get("killmail_id") or 0)
        if km_id > 0:
            expansion_km_ids.add(km_id)
            if km_id not in base_km_ids:
                new_candidate_ids.add(km_id)

    # For each candidate, check entity overlap across all its rows
    candidate_overlap: dict[int, bool] = {kid: False for kid in new_candidate_ids}
    for row in expansion_rows:
        km_id = int(row.get("killmail_id") or 0)
        if km_id not in new_candidate_ids or candidate_overlap.get(km_id, False):
            continue
        # Check if any entity on this row matches the base set
        for prefix in ("victim_", "attacker_"):
            cid = int(row.get(f"{prefix}character_id") or 0)
            corp = int(row.get(f"{prefix}corporation_id") or 0)
            ally = int(row.get(f"{prefix}alliance_id") or 0)
            if (cid > 0 and cid in base_character_ids) or \
               (corp > 0 and corp in base_corporation_ids) or \
               (ally > 0 and ally in base_alliance_ids):
                candidate_overlap[km_id] = True
                break

    for km_id, passed in candidate_overlap.items():
        if passed:
            new_km_ids_passed.add(km_id)
        else:
            new_km_ids_rejected.add(km_id)

    # Step 4: Merge passed rows into the base set
    if new_km_ids_passed:
        for row in expansion_rows:
            km_id = int(row.get("killmail_id") or 0)
            if km_id in new_km_ids_passed:
                base_rows.append(row)

    # Re-sort by time
    base_rows.sort(key=lambda r: str(r.get("killmail_time") or ""))

    diagnostics = {
        "base_killmail_ids": len(base_km_ids),
        "expanded_killmail_ids": len(expansion_km_ids),
        "expansion_passed_overlap": len(new_km_ids_passed),
        "expansion_rejected_no_overlap": len(new_km_ids_rejected),
    }
    return base_rows, diagnostics


def _load_ship_group_map(db: SupplyCoreDb) -> dict[int, int]:
    """Load type_id → group_id mapping for fleet function resolution."""
    rows = db.fetch_all("SELECT type_id, group_id FROM ref_item_types WHERE group_id IS NOT NULL")
    return {int(r["type_id"]): int(r["group_id"]) for r in rows}


def _resolve_fleet_function(ship_type_id: int, ship_group_map: dict[int, int]) -> str:
    """Resolve a ship type ID to its fleet function string."""
    # Monitor (Flag Cruiser) is exclusively used as an FC ship
    if ship_type_id == 45534:
        return "fc"
    group_id = ship_group_map.get(ship_type_id, 0)
    return FLEET_FUNCTION_BY_GROUP.get(group_id, "mainline_dps")


def _load_battle_participants_for_theater(
    db: SupplyCoreDb, battle_ids: list[str],
) -> list[dict[str, Any]]:
    """Load battle_participants rows for side_key resolution."""
    if not battle_ids:
        return []
    all_rows: list[dict[str, Any]] = []
    for offset in range(0, len(battle_ids), BATCH_SIZE):
        chunk = battle_ids[offset:offset + BATCH_SIZE]
        placeholders = ",".join(["%s"] * len(chunk))
        rows = db.fetch_all(
            f"""
            SELECT
                bp.battle_id, bp.character_id, bp.alliance_id, bp.corporation_id,
                bp.side_key, bp.ship_type_id, bp.is_logi, bp.is_command, bp.is_capital
            FROM battle_participants bp
            WHERE bp.battle_id IN ({placeholders})
            """,
            tuple(chunk),
        )
        all_rows.extend(rows)
    return all_rows


def _supplement_bp_rows_from_killmails(
    bp_rows: list[dict[str, Any]],
    killmails: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    """Add synthetic bp entries for characters in killmails but not in bp_rows.

    Engagement expansion can pull in killmails whose participants aren't in
    battle_participants (sub-threshold battles, boundary kills, etc.).
    Without this, their ISK is counted in the timeline total but silently
    excluded from alliance_summary and participant stats.

    Returns (supplemented_bp_rows, count_of_new_characters).
    """
    existing_cids: set[int] = set()
    for p in bp_rows:
        cid = int(p.get("character_id") or 0)
        if cid > 0:
            existing_cids.add(cid)

    new_chars: dict[int, dict[str, Any]] = {}

    for km in killmails:
        # Victim
        vid = int(km.get("victim_character_id") or 0)
        if vid > 0 and vid not in existing_cids and vid not in new_chars:
            new_chars[vid] = {
                "character_id": vid,
                "alliance_id": int(km.get("victim_alliance_id") or 0),
                "corporation_id": int(km.get("victim_corporation_id") or 0),
                "battle_id": str(km.get("battle_id") or ""),
                "side_key": None,
                "ship_type_id": int(km.get("victim_ship_type_id") or 0),
                "is_logi": 0,
                "is_command": 0,
                "is_capital": 0,
            }

        # Attacker
        aid = int(km.get("attacker_character_id") or 0)
        if aid > 0 and aid not in existing_cids and aid not in new_chars:
            new_chars[aid] = {
                "character_id": aid,
                "alliance_id": int(km.get("attacker_alliance_id") or 0),
                "corporation_id": int(km.get("attacker_corporation_id") or 0),
                "battle_id": str(km.get("battle_id") or ""),
                "side_key": None,
                "ship_type_id": int(km.get("attacker_ship_type_id") or 0),
                "is_logi": 0,
                "is_command": 0,
                "is_capital": 0,
            }

    if not new_chars:
        return bp_rows, 0

    return bp_rows + list(new_chars.values()), len(new_chars)


def _load_side_configuration_ids(db: SupplyCoreDb) -> dict[str, set[int]]:
    """Load friendly/hostile alliance/corporation IDs from ESI contacts + manual additions.

    Source: corp_contacts table (positive standing = friendly, negative = hostile).
    """
    friendly_alliance_ids: set[int] = set()
    friendly_corporation_ids: set[int] = set()
    opponent_alliance_ids: set[int] = set()
    opponent_corporation_ids: set[int] = set()

    try:
        contact_rows = db.fetch_all(
            """SELECT contact_id, contact_type, standing
               FROM corp_contacts
               WHERE contact_type IN ('alliance', 'corporation')
                 AND standing != 0"""
        )
        for row in contact_rows:
            contact_id = int(row.get("contact_id") or 0)
            contact_type = str(row.get("contact_type") or "")
            standing = float(row.get("standing") or 0)
            if contact_id <= 0:
                continue

            if standing > 0:
                if contact_type == "alliance":
                    friendly_alliance_ids.add(contact_id)
                elif contact_type == "corporation":
                    friendly_corporation_ids.add(contact_id)
            elif standing < 0:
                if contact_type == "alliance":
                    opponent_alliance_ids.add(contact_id)
                elif contact_type == "corporation":
                    opponent_corporation_ids.add(contact_id)
    except Exception:
        # corp_contacts table may not exist yet — graceful fallback.
        pass

    return {
        "friendly_alliance_ids": friendly_alliance_ids,
        "friendly_corporation_ids": friendly_corporation_ids,
        "opponent_alliance_ids": opponent_alliance_ids,
        "opponent_corporation_ids": opponent_corporation_ids,
    }


# ── Alliance relationship graph cache ──────────────────────────────────────

def _load_alliance_relationship_graph(db: SupplyCoreDb) -> dict[int, dict[str, list[dict]]]:
    """Load the computed alliance relationship graph for side inference.

    Returns a dict keyed by alliance_id, each containing:
      { "allied": [{"target": int, "confidence": float, "shared_killmails": int}],
        "hostile": [{"target": int, "confidence": float, "shared_killmails": int}] }
    """
    graph: dict[int, dict[str, list[dict]]] = {}

    rows = db.fetch_all(
        """
        SELECT source_alliance_id, target_alliance_id, relationship_type,
               confidence, shared_killmails
        FROM alliance_relationships
        WHERE confidence >= 0.15
        ORDER BY source_alliance_id, relationship_type, confidence DESC
        """
    )

    for r in rows:
        src = int(r["source_alliance_id"])
        if src not in graph:
            graph[src] = {"allied": [], "hostile": []}
        entry = {
            "target": int(r["target_alliance_id"]),
            "confidence": float(r.get("confidence") or 0),
            "shared_killmails": int(r.get("shared_killmails") or 0),
        }
        rel_type = str(r.get("relationship_type") or "")
        if rel_type == "allied":
            graph[src]["allied"].append(entry)
        elif rel_type == "hostile":
            graph[src]["hostile"].append(entry)

    return graph


# Minimum confidence threshold for graph-inferred side classification.
# Below this, the alliance stays as third_party even if edges exist.
_GRAPH_INFERENCE_MIN_CONFIDENCE = 0.3


def _infer_side_from_graph(
    alliance_id: int,
    relationship_graph: dict[int, dict[str, list[dict]]],
    friendly_alliance_ids: set[int],
    opponent_alliance_ids: set[int],
) -> tuple[str, float]:
    """Infer side classification from the alliance relationship graph.

    Checks if the alliance has strong 'allied' edges to known friendlies
    or known opponents. Returns (inferred_side, confidence).

    Returns ("third_party", 0.0) if no strong signal found.
    """
    if alliance_id <= 0 or alliance_id not in relationship_graph:
        return "third_party", 0.0

    allied_edges = relationship_graph[alliance_id].get("allied", [])
    if not allied_edges:
        return "third_party", 0.0

    # Score how strongly this alliance is connected to each known side
    friendly_score = 0.0
    opponent_score = 0.0
    friendly_count = 0
    opponent_count = 0

    for edge in allied_edges:
        target = edge["target"]
        conf = edge["confidence"]
        if target in friendly_alliance_ids:
            friendly_score += conf
            friendly_count += 1
        elif target in opponent_alliance_ids:
            opponent_score += conf
            opponent_count += 1

    # Need at least one edge with sufficient confidence to infer
    if friendly_score <= 0 and opponent_score <= 0:
        return "third_party", 0.0

    # If strongly allied with friendlies and not with opponents → friendly
    if friendly_score > opponent_score and friendly_score >= _GRAPH_INFERENCE_MIN_CONFIDENCE:
        return "friendly", round(friendly_score / max(1, friendly_count), 4)

    # If strongly allied with opponents and not with friendlies → opponent
    if opponent_score > friendly_score and opponent_score >= _GRAPH_INFERENCE_MIN_CONFIDENCE:
        return "opponent", round(opponent_score / max(1, opponent_count), 4)

    # Ambiguous — connected to both sides, leave as third_party
    return "third_party", 0.0


# ── Side determination ──────────────────────────────────────────────────────

def _classify_alliance(
    alliance_id: int,
    corporation_id: int,
    side_configuration: dict[str, set[int]],
    relationship_graph: dict[int, dict[str, list[dict]]] | None = None,
) -> tuple[str, str]:
    """Classify an entity as friendly/opponent/third_party.

    Priority:
      1. Explicit user configuration (tracked alliances/corps) — always wins.
      2. Alliance relationship graph inference — if graph data available.
      3. Default to third_party.

    Returns (classification, source) where source is 'config' or 'graph'.
    """
    friendly_alliance_ids = side_configuration.get("friendly_alliance_ids", set())
    friendly_corporation_ids = side_configuration.get("friendly_corporation_ids", set())
    opponent_alliance_ids = side_configuration.get("opponent_alliance_ids", set())
    opponent_corporation_ids = side_configuration.get("opponent_corporation_ids", set())

    # 1. Explicit configuration always takes priority
    if (alliance_id > 0 and alliance_id in friendly_alliance_ids) or \
       (corporation_id > 0 and corporation_id in friendly_corporation_ids):
        return "friendly", "config"
    if (alliance_id > 0 and alliance_id in opponent_alliance_ids) or \
       (corporation_id > 0 and corporation_id in opponent_corporation_ids):
        return "opponent", "config"

    # 2. Graph-based inference for unknown alliances
    if relationship_graph and alliance_id > 0:
        inferred, confidence = _infer_side_from_graph(
            alliance_id, relationship_graph, friendly_alliance_ids, opponent_alliance_ids,
        )
        if inferred != "third_party":
            return inferred, "graph"

    return "third_party", "none"


def _determine_sides(
    participants: list[dict[str, Any]],
    side_configuration: dict[str, set[int]],
    relationship_graph: dict[int, dict[str, list[dict]]] | None = None,
) -> tuple[dict[str, str], dict[int, str], dict[str, Any]]:
    """Classify each character as friendly/opponent/third_party.

    Side is determined from:
      1. Configured tracked alliances (friendly) / tracked opponents (opponent)
      2. Alliance relationship graph inference (for unknowns)
      3. Everything else remains third_party

    Returns:
        side_labels: empty dict (kept for API compatibility)
        char_sides:  mapping character_id → "friendly" | "opponent" | "third_party"
        side_meta:   classification counts for debug/logging
    """
    char_sides: dict[int, str] = {}
    counts = {"friendly": 0, "opponent": 0, "third_party": 0}
    source_counts = {"config": 0, "graph": 0, "none": 0}

    for p in participants:
        char_id = int(p.get("character_id") or 0)
        if char_id <= 0:
            continue
        alliance_id = int(p.get("alliance_id") or 0)
        corporation_id = int(p.get("corporation_id") or 0)
        classification, source = _classify_alliance(
            alliance_id, corporation_id, side_configuration, relationship_graph,
        )
        char_sides[char_id] = classification
        counts[classification] += 1
        source_counts[source] += 1

    return {}, char_sides, {
        "used_fallback": False,
        "used_graph_inference": source_counts["graph"] > 0,
        "total_friendly_matches": counts["friendly"],
        "total_opponent_matches": counts["opponent"],
        "total_third_party": counts["third_party"],
        "graph_inferred_count": source_counts["graph"],
        "config_classified_count": source_counts["config"],
        "side_scores": {},
        "side_labels": {},
    }


# ── Timeline computation ───────────────────────────────────────────────────

def _compute_timeline(
    killmails: list[dict[str, Any]],
    char_sides: dict[int, str],
    theater_start: datetime,
    theater_end: datetime,
) -> list[dict[str, Any]]:
    """Compute per-minute timeline buckets with momentum scoring.

    Note: killmail rows are expanded per-attacker (LEFT JOIN killmail_attackers),
    so we must deduplicate by killmail_id to avoid counting kills/ISK multiple
    times.
    """
    bucket_data: dict[str, dict[str, Any]] = {}
    seen_killmail_ids: set[int] = set()

    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        if km_id <= 0 or km_id in seen_killmail_ids:
            continue
        seen_killmail_ids.add(km_id)

        km_time = _parse_dt(km.get("killmail_time"))
        # Bucket to nearest minute
        bucket_ts = km_time.replace(second=0, microsecond=0)
        bucket_key = bucket_ts.strftime("%Y-%m-%d %H:%M:%S")

        bucket = bucket_data.setdefault(bucket_key, {
            "bucket_time": bucket_key,
            "kills": 0,
            "isk_destroyed": 0.0,
            "side_a_kills": 0,
            "side_b_kills": 0,
            "side_a_isk": 0.0,
            "side_b_isk": 0.0,
        })

        victim_id = int(km.get("victim_character_id") or 0)
        isk = float(km.get("total_value") or 0)
        victim_side = char_sides.get(victim_id, "third_party")

        # A kill counts against the victim's side
        bucket["kills"] += 1
        bucket["isk_destroyed"] += isk

        if victim_side == "friendly":
            bucket["side_b_kills"] += 1  # opponent scored a kill against friendly
            bucket["side_b_isk"] += isk
        elif victim_side == "opponent":
            bucket["side_a_kills"] += 1  # friendly scored a kill against opponent
            bucket["side_a_isk"] += isk
        # third_party victims don't count toward either side's momentum

    # Sort by time and compute momentum
    sorted_buckets = sorted(bucket_data.values(), key=lambda b: b["bucket_time"])

    # Momentum = rolling ratio of side_a_kills / total_kills (centered on 0.5)
    for i, bucket in enumerate(sorted_buckets):
        window_start = max(0, i - MOMENTUM_SMOOTHING_WINDOW + 1)
        window = sorted_buckets[window_start:i + 1]
        total_a = sum(b["side_a_kills"] for b in window)
        total_b = sum(b["side_b_kills"] for b in window)
        total = total_a + total_b
        # Momentum: >0 = side_a winning, <0 = side_b winning
        bucket["momentum_score"] = round(
            _safe_div(total_a - total_b, total, 0.0), 4
        )

    return sorted_buckets


# ── Turning point detection ─────────────────────────────────────────────────

def _detect_turning_points(
    timeline: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Detect momentum shifts that exceed the threshold."""
    turning_points: list[dict[str, Any]] = []

    for i in range(1, len(timeline)):
        prev = timeline[i - 1]["momentum_score"]
        curr = timeline[i]["momentum_score"]
        delta = curr - prev

        if abs(delta) >= TURNING_POINT_MAGNITUDE_THRESHOLD:
            direction = "friendly_surge" if delta > 0 else "opponent_surge"
            description = (
                f"Momentum shifted {'toward friendlies' if delta > 0 else 'toward opponents'} "
                f"({prev:.2f} → {curr:.2f}, Δ={delta:+.2f})"
            )
            turning_points.append({
                "bucket_time": timeline[i]["bucket_time"],
                "direction": direction,
                "magnitude": round(abs(delta), 4),
                "description": description,
            })

    return turning_points


# ── Per-character ledger (source of truth) ─────────────────────────────────


def _empty_ledger_entry() -> dict[str, Any]:
    return {
        "final_kills": 0,
        "contributed_kills": 0,
        "losses": 0,
        "isk_killed": 0.0,
        "isk_lost": 0.0,
        "damage_done": 0.0,
    }


def _build_character_ledger(
    killmails: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Build per-character stats from killmails — the single source of truth.

    For each killmail (expanded per-attacker via LEFT JOIN):
      - Victim: losses += 1, isk_lost += total_value  (once per killmail_id)
      - Final blow attacker: final_kills += 1, isk_killed += total_value
      - Every listed attacker: contributed_kills += 1
        (includes zero-damage; dedup by killmail_id + attacker_character_id)

    Invariants across the full set:
      sum(losses)      == number of unique killmails
      sum(final_kills) == number of unique killmails
      sum(isk_lost)    == total destroyed ISK
      sum(isk_killed)  == total destroyed ISK
      sum(contributed_kills) >= number of unique killmails
    """
    ledger: dict[int, dict[str, Any]] = {}
    seen_victim: set[int] = set()             # killmail_id
    seen_final_blow: set[int] = set()         # killmail_id
    seen_attacker: set[tuple[int, int]] = set()  # (killmail_id, attacker_character_id)

    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        if km_id <= 0:
            continue

        isk = float(km.get("total_value") or 0)

        # ── Victim (once per killmail) ────────────────────────────────
        victim_id = int(km.get("victim_character_id") or 0)
        if victim_id > 0 and km_id not in seen_victim:
            seen_victim.add(km_id)
            entry = ledger.setdefault(victim_id, _empty_ledger_entry())
            entry["losses"] += 1
            entry["isk_lost"] += isk

        # ── Attacker ─────────────────────────────────────────────────
        attacker_id = int(km.get("attacker_character_id") or 0)
        if attacker_id <= 0:
            continue

        # Final blow (once per killmail)
        is_final = int(km.get("attacker_final_blow") or 0)
        if is_final == 1 and km_id not in seen_final_blow:
            seen_final_blow.add(km_id)
            entry = ledger.setdefault(attacker_id, _empty_ledger_entry())
            entry["final_kills"] += 1
            entry["isk_killed"] += isk

        # Contributed kill — every listed attacker, including dmg=0
        atk_key = (km_id, attacker_id)
        if atk_key not in seen_attacker:
            seen_attacker.add(atk_key)
            entry = ledger.setdefault(attacker_id, _empty_ledger_entry())
            entry["contributed_kills"] += 1

        # Damage accumulation (raw per-row, no dedup needed)
        attacker_damage = float(km.get("attacker_damage_done") or 0)
        if attacker_damage > 0:
            entry = ledger.setdefault(attacker_id, _empty_ledger_entry())
            entry["damage_done"] += attacker_damage

    return ledger


# ── Alliance summary computation ───────────────────────────────────────────

def _compute_alliance_summary(
    character_ledger: dict[int, dict[str, Any]],
    bp_rows: list[dict[str, Any]],
    side_configuration: dict[str, set[int]],
    char_sides: dict[int, str],
    relationship_graph: dict[int, dict[str, list[dict]]] | None = None,
) -> list[dict[str, Any]]:
    """Compute per-alliance (or per-corporation for allianceless entities) summary.

    Derives all totals from the per-character ledger — no direct killmail
    processing.  This guarantees that group totals are exact rollups of
    character truth.

    Grouping key is (alliance_id, corporation_id):
      - Entities WITH an alliance  → (alliance_id, 0)
      - Entities WITHOUT an alliance → (0, corporation_id)
    This ensures NPC-corp / allianceless-corp pilots get their own row
    instead of being lumped under a single alliance_id=0 bucket.
    """
    group_stats: dict[tuple[int, int], dict[str, Any]] = {}

    def _group_key_for(alliance_id: int, corporation_id: int) -> tuple[int, int]:
        if alliance_id > 0:
            return (alliance_id, 0)
        if corporation_id > 0:
            return (0, corporation_id)
        return (0, 0)

    def _get_entry(key: tuple[int, int]) -> dict[str, Any]:
        return group_stats.setdefault(key, _empty_alliance_stats(key[0], key[1]))

    # Build group membership and roll up character ledger entries.
    # Each character appears once in bp_rows (per battle); collect unique
    # characters per group key and accumulate their ledger stats.
    group_participants: dict[tuple[int, int], set[int]] = defaultdict(set)
    seen_char_in_group: set[tuple[int, tuple[int, int]]] = set()

    for p in bp_rows:
        aid = int(p.get("alliance_id") or 0)
        corp_id = int(p.get("corporation_id") or 0)
        cid = int(p.get("character_id") or 0)
        if cid <= 0 or (aid <= 0 and corp_id <= 0):
            continue

        gk = _group_key_for(aid, corp_id)
        group_participants[gk].add(cid)

        # Roll up this character's ledger into the group (once per char+group)
        char_group_key = (cid, gk)
        if char_group_key in seen_char_in_group:
            continue
        seen_char_in_group.add(char_group_key)

        ledger_entry = character_ledger.get(cid)
        if ledger_entry is None:
            continue

        entry = _get_entry(gk)
        entry["total_kills"] += ledger_entry["final_kills"]
        entry["total_contributed_kills"] += ledger_entry["contributed_kills"]
        entry["total_losses"] += ledger_entry["losses"]
        entry["total_isk_killed"] += ledger_entry["isk_killed"]
        entry["total_isk_lost"] += ledger_entry["isk_lost"]
        entry["total_damage"] += ledger_entry["damage_done"]

    # Finalize
    result: list[dict[str, Any]] = []
    for gk, stats in group_stats.items():
        aid, corp_id = gk
        stats["participant_count"] = len(group_participants.get(gk, set()))
        stats["side"], _ = _classify_alliance(aid, corp_id, side_configuration, relationship_graph)
        total_isk = stats["total_isk_killed"] + stats["total_isk_lost"]
        stats["efficiency"] = round(_safe_div(stats["total_isk_killed"], total_isk, 0.0), 4)
        result.append(stats)

    return result


def _empty_alliance_stats(alliance_id: int, corporation_id: int = 0) -> dict[str, Any]:
    return {
        "alliance_id": alliance_id,
        "corporation_id": corporation_id,
        "alliance_name": None,
        "side": "third_party",
        "participant_count": 0,
        "total_kills": 0,
        "total_contributed_kills": 0,
        "total_losses": 0,
        "total_damage": 0.0,
        "total_isk_lost": 0.0,
        "total_isk_killed": 0.0,
        "efficiency": 0.0,
    }


# ── Participant stats computation ──────────────────────────────────────────

def _compute_participants(
    killmails: list[dict[str, Any]],
    bp_rows: list[dict[str, Any]],
    char_sides: dict[int, str],
    theater_id: str,
    ship_group_map: dict[int, int] | None = None,
    character_ledger: dict[int, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Compute per-character stats across the theater.

    Core stats (kills, deaths, isk_killed, isk_lost, damage_done) are taken
    from the character_ledger which is the single source of truth.
    Ship resolution, role inference, entry/exit times, and damage_taken
    are still derived from killmail rows.
    """
    char_stats: dict[int, dict[str, Any]] = {}
    char_battles: dict[int, set[str]] = defaultdict(set)
    char_times: dict[int, list[datetime]] = defaultdict(list)
    char_ship_counts: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    _NON_COMBAT_GROUP_IDS: frozenset[int] = frozenset(
        gid for gid, role in FLEET_FUNCTION_BY_GROUP.items() if role == "non_combat"
    )

    if character_ledger is None:
        character_ledger = {}

    # Initialize from battle participants
    for p in bp_rows:
        cid = int(p.get("character_id") or 0)
        if cid <= 0:
            continue
        bid = str(p.get("battle_id") or "")
        char_battles[cid].add(bid)

        if cid not in char_stats:
            aid = int(p.get("alliance_id") or 0)
            corp_id = int(p.get("corporation_id") or 0)
            ledger = character_ledger.get(cid, _empty_ledger_entry())

            char_stats[cid] = {
                "character_id": cid,
                "character_name": None,
                "alliance_id": aid if aid > 0 else None,
                "corporation_id": corp_id if corp_id > 0 else None,
                "side": char_sides.get(cid, "third_party"),
                "ship_type_ids": [],
                "ships_lost_detail": defaultdict(lambda: {"count": 0, "isk_lost": 0.0, "killmail_ids": []}),
                # Canonical stats from ledger
                "kills": ledger["final_kills"],          # kills = final kills
                "final_kills": ledger["final_kills"],
                "contributed_kills": ledger["contributed_kills"],
                "deaths": ledger["losses"],
                "damage_done": ledger["damage_done"],
                "damage_taken": 0.0,                     # computed below from killmails
                "isk_killed": ledger["isk_killed"],
                "isk_lost": ledger["isk_lost"],
                "role_proxy": "mainline_dps",
                "flying_ship_type_id": None,
            }

        # Track ship types
        st = int(p.get("ship_type_id") or 0)
        if st > 0 and st not in char_stats[cid]["ship_type_ids"]:
            char_stats[cid]["ship_type_ids"].append(st)
        if st > 0:
            char_ship_counts[cid][st] += 1

    # Pre-compute total HP damage taken per killmail (sum of all attacker damage)
    km_total_hp_damage: dict[int, float] = defaultdict(float)
    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        attacker_damage = float(km.get("attacker_damage_done") or 0)
        if km_id > 0:
            km_total_hp_damage[km_id] += attacker_damage

    # Process killmails for damage_taken, ship loss detail, timestamps, and
    # attacker ship counts — stats that require killmail-row-level data.
    # Core kill/death/ISK stats come from the ledger above.
    seen_deaths: set[tuple[int, int]] = set()
    seen_attacker_time: set[tuple[int, int]] = set()
    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        km_time = _parse_dt(km.get("killmail_time"))

        victim_id = int(km.get("victim_character_id") or 0)
        isk = float(km.get("total_value") or 0)

        # Damage taken + ship loss detail (once per killmail per victim)
        death_key = (km_id, victim_id)
        if victim_id > 0 and victim_id in char_stats and death_key not in seen_deaths:
            seen_deaths.add(death_key)
            char_stats[victim_id]["damage_taken"] += km_total_hp_damage.get(km_id, 0.0)
            char_times[victim_id].append(km_time)

            victim_ship = int(km.get("victim_ship_type_id") or 0)
            if victim_ship > 0:
                loss = char_stats[victim_id]["ships_lost_detail"][victim_ship]
                loss["count"] += 1
                loss["isk_lost"] += isk
                seq_id = int(km.get("sequence_id") or 0)
                if km_id > 0:
                    loss["killmail_ids"].append({"killmail_id": km_id, "sequence_id": seq_id})
                if victim_ship not in char_stats[victim_id]["ship_type_ids"]:
                    char_stats[victim_id]["ship_type_ids"].append(victim_ship)

        # Attacker timestamps and ship counts
        attacker_id = int(km.get("attacker_character_id") or 0)
        if attacker_id > 0 and attacker_id in char_stats:
            atk_key = (km_id, attacker_id)
            if atk_key not in seen_attacker_time:
                seen_attacker_time.add(atk_key)
                char_times[attacker_id].append(km_time)
                atk_ship = int(km.get("attacker_ship_type_id") or 0)
                if atk_ship > 0:
                    char_ship_counts[attacker_id][atk_ship] += 1

    # ── Resolve flying ship + role from most-common non-pod ship ──────────
    for cid, stats in char_stats.items():
        counts = char_ship_counts.get(cid, {})
        if not counts:
            continue

        best_ship = 0
        best_count = 0
        fallback_ship = 0
        fallback_count = 0
        for stid, cnt in counts.items():
            group_id = (ship_group_map or {}).get(stid, 0)
            if group_id in _NON_COMBAT_GROUP_IDS:
                if cnt > fallback_count:
                    fallback_ship = stid
                    fallback_count = cnt
            else:
                if cnt > best_count:
                    best_ship = stid
                    best_count = cnt

        flying_ship = best_ship if best_ship > 0 else fallback_ship
        if flying_ship > 0:
            stats["flying_ship_type_id"] = flying_ship
            if ship_group_map is not None:
                stats["role_proxy"] = _resolve_fleet_function(flying_ship, ship_group_map)

    # Build final rows
    result: list[dict[str, Any]] = []
    for cid, stats in char_stats.items():
        times = sorted(char_times.get(cid, []))
        entry_time = times[0].strftime("%Y-%m-%d %H:%M:%S") if times else None
        exit_time = times[-1].strftime("%Y-%m-%d %H:%M:%S") if times else None

        ship_json = json_dumps_safe(stats["ship_type_ids"]) if stats["ship_type_ids"] else None

        lost_detail = dict(stats.get("ships_lost_detail", {}))
        ships_lost_json = json_dumps_safe([
            {"ship_type_id": stid, "count": d["count"], "isk_lost": d["isk_lost"], "killmail_ids": d.get("killmail_ids", [])}
            for stid, d in lost_detail.items()
        ]) if lost_detail else None

        result.append({
            "theater_id": theater_id,
            "character_id": cid,
            "character_name": stats["character_name"],
            "alliance_id": stats["alliance_id"],
            "corporation_id": stats["corporation_id"],
            "side": stats["side"],
            "ship_type_ids": ship_json,
            "ships_lost_detail": ships_lost_json,
            "flying_ship_type_id": stats.get("flying_ship_type_id"),
            "kills": stats["kills"],                        # = final_kills
            "final_kills": stats["final_kills"],
            "contributed_kills": stats["contributed_kills"],
            "deaths": stats["deaths"],
            "damage_done": stats["damage_done"],
            "damage_taken": stats["damage_taken"],
            "isk_killed": stats["isk_killed"],
            "isk_lost": stats["isk_lost"],
            "role_proxy": stats["role_proxy"],
            "entry_time": entry_time,
            "exit_time": exit_time,
            "battles_present": len(char_battles.get(cid, set())),
            "suspicion_score": None,
            "is_suspicious": 0,
        })

    return result


# ── Structure kill detection ───────────────────────────────────────────────

def _compute_structure_kills(
    killmails: list[dict[str, Any]],
    side_configuration: dict[str, set[int]],
    theater_id: str,
    relationship_graph: dict[int, dict[str, list[dict]]] | None = None,
) -> list[dict[str, Any]]:
    """Detect player-owned structure killmails (victim has no character, but has alliance).

    Structures cannot appear in theater_participants (character_id PK), so they
    are stored separately in theater_structure_kills.  A killmail is classified
    as a structure kill when:
      - victim_character_id == 0  (no pilot — only structures/deployables have this)
      - victim_alliance_id > 0    (player-owned — filters out NPC entities)
      - zkb_npc == 0              (sanity guard: not flagged as an NPC engagement)
    """
    # First pass: identify structure killmail IDs and collect final-blow attacker info.
    # Killmails are expanded per-attacker, so we scan all rows to find the final blow.
    structure_km_ids: set[int] = set()
    final_blow_attacker: dict[int, dict[str, int]] = {}  # km_id → {alliance_id, corporation_id}
    structure_victim_data: dict[int, dict[str, Any]] = {}

    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        if km_id <= 0:
            continue

        victim_char = int(km.get("victim_character_id") or 0)
        victim_alliance = int(km.get("victim_alliance_id") or 0)
        zkb_npc = int(km.get("zkb_npc") or 0)

        # Only player-owned structures: no pilot, has an owning alliance, not NPC
        if victim_char != 0 or victim_alliance <= 0 or zkb_npc:
            continue

        # Record victim data once per killmail
        if km_id not in structure_victim_data:
            structure_km_ids.add(km_id)
            victim_corp = int(km.get("victim_corporation_id") or 0)
            side, _ = _classify_alliance(victim_alliance, victim_corp, side_configuration, relationship_graph)
            km_time = km.get("killmail_time")
            if isinstance(km_time, datetime):
                killed_at = km_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                killed_at = str(km_time) if km_time else None

            structure_victim_data[km_id] = {
                "victim_corporation_id": victim_corp if victim_corp > 0 else None,
                "victim_alliance_id": victim_alliance,
                "victim_ship_type_id": int(km.get("victim_ship_type_id") or 0),
                "isk_lost": float(km.get("total_value") or 0),
                "side": side,
                "killed_at": killed_at,
            }

        # Track final-blow attacker for this structure kill
        is_final = int(km.get("attacker_final_blow") or 0)
        if is_final == 1 and km_id not in final_blow_attacker:
            final_blow_attacker[km_id] = {
                "alliance_id": int(km.get("attacker_alliance_id") or 0),
                "corporation_id": int(km.get("attacker_corporation_id") or 0),
            }

    # Build result rows with killer info attached
    result: list[dict[str, Any]] = []
    for km_id in structure_km_ids:
        victim = structure_victim_data[km_id]
        killer = final_blow_attacker.get(km_id, {})
        result.append({
            "theater_id": theater_id,
            "killmail_id": km_id,
            **victim,
            "killer_alliance_id": killer.get("alliance_id") or None,
            "killer_corporation_id": killer.get("corporation_id") or None,
        })

    return result


# ── Side composition computation ──────────────────────────────────────────

def _compute_side_composition(
    bp_rows: list[dict[str, Any]],
    char_sides: dict[int, str],
    ship_group_map: dict[int, int],
    theater_id: str,
) -> list[dict[str, Any]]:
    """Compute per-side composition features for force-composition normalization.

    Returns one row per side with hull distribution, role distribution,
    and composite scores (hull_weight, role_balance, logi_strength, etc.).
    """
    # Per-side accumulators
    sides: dict[str, dict[str, Any]] = {}

    for p in bp_rows:
        cid = int(p.get("character_id") or 0)
        if cid <= 0:
            continue
        side = char_sides.get(cid, "third_party")
        ship_type_id = int(p.get("ship_type_id") or 0)
        group_id = ship_group_map.get(ship_type_id, 0)
        fleet_fn = FLEET_FUNCTION_BY_GROUP.get(group_id, "mainline_dps")
        ship_size = SHIP_SIZE_BY_GROUP.get(group_id, "medium")

        if side not in sides:
            sides[side] = {
                "theater_id": theater_id,
                "side": side,
                "pilots": set(),
                "hull_counts": {"small": 0, "medium": 0, "large": 0, "capital": 0},
                "role_counts": {},
            }

        acc = sides[side]
        acc["pilots"].add(cid)
        acc["hull_counts"][ship_size] = acc["hull_counts"].get(ship_size, 0) + 1
        acc["role_counts"][fleet_fn] = acc["role_counts"].get(fleet_fn, 0) + 1

    result: list[dict[str, Any]] = []
    for side, acc in sides.items():
        pilot_count = len(acc["pilots"])
        hulls = acc["hull_counts"]
        roles = acc["role_counts"]
        total_ships = sum(hulls.values()) or 1

        # Hull weight: weighted average hull class power
        hull_weight_score = sum(
            count * HULL_WEIGHT.get(sz, 1.0)
            for sz, count in hulls.items()
        ) / total_ships

        # Role balance: Shannon entropy of role distribution normalized to [0,1]
        # Higher = more diverse fleet composition
        role_total = sum(roles.values()) or 1
        role_entropy = 0.0
        for count in roles.values():
            if count > 0:
                p = count / role_total
                role_entropy -= p * math.log2(p)
        # Normalize: max entropy = log2(num_roles) ≈ 3.7 for 13 combat roles
        max_entropy = math.log2(13) if len(roles) > 1 else 1.0
        role_balance_score = min(1.0, role_entropy / max_entropy)

        # Logistics strength: ratio of logi pilots to total
        logi_count = roles.get("logistics", 0) + roles.get("capital_logistics", 0)
        logi_strength = logi_count / pilot_count if pilot_count > 0 else 0.0

        # Command strength
        command_count = roles.get("command", 0)
        command_strength = command_count / pilot_count if pilot_count > 0 else 0.0

        # Capital ratio
        capital_count = hulls.get("capital", 0)
        capital_ratio = capital_count / total_ships

        # Expected performance composite:
        # Base = hull_weight (bigger ships hit harder)
        # × (1 + logi_multiplier + command_multiplier + ewar_multiplier) for force multipliers
        # × sqrt(pilot_count) for Lanchester's square law
        force_multiplier = 1.0
        for role, mult in ROLE_MULTIPLIER_WEIGHT.items():
            role_count = roles.get(role, 0)
            if role_count > 0 and pilot_count > 0:
                # Each multiplier role adds its weight proportional to its presence
                force_multiplier += mult * (role_count / pilot_count)

        # Combat power: sum of role-weighted combat contributions
        combat_power = sum(
            count * ROLE_COMBAT_WEIGHT.get(role, 0.5)
            for role, count in roles.items()
        ) / total_ships

        # Expected performance = combat_power × hull_weight × force_multiplier × sqrt(N)
        expected_perf = combat_power * hull_weight_score * force_multiplier * math.sqrt(max(1, pilot_count))

        # Build all role count columns
        all_roles = [
            "mainline_dps", "capital_dps", "logistics", "capital_logistics",
            "tackle", "heavy_tackle", "bubble_control", "command", "ewar",
            "bomber", "scout", "supercapital", "non_combat",
        ]

        row = {
            "theater_id": theater_id,
            "side": side,
            "pilot_count": pilot_count,
            "hull_small_count": hulls.get("small", 0),
            "hull_medium_count": hulls.get("medium", 0),
            "hull_large_count": hulls.get("large", 0),
            "hull_capital_count": hulls.get("capital", 0),
            "side_hull_weight_score": round(hull_weight_score, 4),
            "side_role_balance_score": round(role_balance_score, 4),
            "side_logi_strength_score": round(logi_strength, 4),
            "side_command_strength_score": round(command_strength, 4),
            "side_capital_ratio": round(capital_ratio, 4),
            "side_expected_performance_score": round(expected_perf, 4),
        }
        for role in all_roles:
            row[f"role_{role}"] = roles.get(role, 0)

        result.append(row)

    return result


# ── Anomaly scoring ────────────────────────────────────────────────────────

def _compute_anomaly_score(
    theater: dict[str, Any],
    timeline: list[dict[str, Any]],
    turning_points: list[dict[str, Any]],
    alliance_summary: list[dict[str, Any]],
) -> float:
    """Composite anomaly score for the theater.

    Factors:
    - Number of momentum shifts (more = more chaotic)
    - Efficiency imbalance between sides
    - Duration relative to participant count
    """
    score = 0.0

    # Turning point density (more shifts → higher score)
    tp_count = len(turning_points)
    duration_minutes = max(1, int(theater.get("duration_seconds") or 60) / 60)
    tp_density = tp_count / duration_minutes
    score += min(0.3, tp_density * 0.5)

    # Efficiency imbalance
    friendly_eff = [a["efficiency"] for a in alliance_summary if a["side"] == "friendly"]
    opponent_eff = [a["efficiency"] for a in alliance_summary if a["side"] == "opponent"]
    avg_a = sum(friendly_eff) / len(friendly_eff) if friendly_eff else 0.5
    avg_b = sum(opponent_eff) / len(opponent_eff) if opponent_eff else 0.5
    eff_imbalance = abs(avg_a - avg_b)
    score += min(0.3, eff_imbalance * 0.5)

    # Multi-system complexity
    system_count = int(theater.get("system_count") or 1)
    if system_count > 1:
        score += min(0.2, (system_count - 1) * 0.05)

    # Large battles are inherently more interesting
    participant_count = int(theater.get("participant_count") or 0)
    if participant_count >= 200:
        score += 0.2
    elif participant_count >= 100:
        score += 0.1

    return round(min(1.0, score), 4)


# ── DB writes ──────────────────────────────────────────────────────────────

def _flush_analysis(
    db: SupplyCoreDb,
    theater_id: str,
    timeline_rows: list[dict[str, Any]],
    alliance_rows: list[dict[str, Any]],
    participant_rows: list[dict[str, Any]],
    turning_points: list[dict[str, Any]],
    total_kills: int,
    total_isk: float,
    anomaly_score: float,
    computed_at: str,
    battle_ids: list[str],
    side_composition_rows: list[dict[str, Any]] | None = None,
    structure_kill_rows: list[dict[str, Any]] | None = None,
) -> int:
    """Write analysis results for one theater. Returns rows written."""
    rows_written = 0

    with db.transaction() as (_, cursor):
        # Clean old data for this theater
        cursor.execute("DELETE FROM theater_timeline WHERE theater_id = %s", (theater_id,))
        cursor.execute("DELETE FROM theater_alliance_summary WHERE theater_id = %s", (theater_id,))
        cursor.execute("DELETE FROM theater_participants WHERE theater_id = %s", (theater_id,))
        cursor.execute("DELETE FROM theater_structure_kills WHERE theater_id = %s", (theater_id,))

        # Clean turning points for battles in this theater
        if battle_ids:
            placeholders = ",".join(["%s"] * len(battle_ids))
            cursor.execute(f"DELETE FROM battle_turning_points WHERE battle_id IN ({placeholders})", tuple(battle_ids))

        # Timeline
        if timeline_rows:
            cursor.executemany(
                """
                INSERT INTO theater_timeline (
                    theater_id, bucket_time, bucket_seconds, kills, isk_destroyed,
                    side_a_kills, side_b_kills, side_a_isk, side_b_isk, momentum_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        theater_id, t["bucket_time"], TIMELINE_BUCKET_SECONDS,
                        t["kills"], t["isk_destroyed"],
                        t["side_a_kills"], t["side_b_kills"],
                        t["side_a_isk"], t["side_b_isk"],
                        t["momentum_score"],
                    )
                    for t in timeline_rows
                ],
            )
            rows_written += len(timeline_rows)

        # Alliance summary
        if alliance_rows:
            cursor.executemany(
                """
                INSERT INTO theater_alliance_summary (
                    theater_id, alliance_id, corporation_id, alliance_name, side,
                    participant_count, total_kills, total_contributed_kills, total_losses,
                    total_damage, total_isk_lost, total_isk_killed, efficiency
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        theater_id, a["alliance_id"], a.get("corporation_id", 0),
                        a["alliance_name"], a["side"],
                        a["participant_count"], a["total_kills"],
                        a["total_contributed_kills"], a["total_losses"],
                        a["total_damage"], a["total_isk_lost"], a["total_isk_killed"],
                        a["efficiency"],
                    )
                    for a in alliance_rows
                ],
            )
            rows_written += len(alliance_rows)

        # Participants (batch insert)
        for chunk_start in range(0, len(participant_rows), BATCH_SIZE):
            chunk = participant_rows[chunk_start:chunk_start + BATCH_SIZE]
            cursor.executemany(
                """
                INSERT INTO theater_participants (
                    theater_id, character_id, character_name, alliance_id,
                    corporation_id, side, ship_type_ids, ships_lost_detail,
                    flying_ship_type_id, kills, final_kills, contributed_kills,
                    isk_killed, deaths,
                    damage_done, damage_taken, isk_lost, role_proxy,
                    entry_time, exit_time, battles_present,
                    suspicion_score, is_suspicious
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        p["theater_id"], p["character_id"], p["character_name"],
                        p["alliance_id"], p["corporation_id"], p["side"],
                        p["ship_type_ids"], p["ships_lost_detail"],
                        p.get("flying_ship_type_id"), p["kills"],
                        p["final_kills"], p["contributed_kills"],
                        p["isk_killed"], p["deaths"],
                        p["damage_done"], p["damage_taken"], p["isk_lost"], p["role_proxy"],
                        p["entry_time"], p["exit_time"], p["battles_present"],
                        p["suspicion_score"], p["is_suspicious"],
                    )
                    for p in chunk
                ],
            )
            rows_written += len(chunk)

        # Turning points
        if turning_points:
            bid = battle_ids[0] if battle_ids else theater_id
            cursor.executemany(
                """
                INSERT INTO battle_turning_points (
                    battle_id, turning_point_at, direction, magnitude, description
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                [(bid, tp["bucket_time"], tp["direction"], tp["magnitude"], tp["description"]) for tp in turning_points],
            )
            rows_written += len(turning_points)

        # Side composition
        if side_composition_rows:
            cursor.execute("DELETE FROM theater_side_composition WHERE theater_id = %s", (theater_id,))
            cursor.executemany(
                """
                INSERT INTO theater_side_composition (
                    theater_id, side, pilot_count,
                    hull_small_count, hull_medium_count, hull_large_count, hull_capital_count,
                    role_mainline_dps, role_capital_dps, role_logistics, role_capital_logistics,
                    role_tackle, role_heavy_tackle, role_bubble_control, role_command, role_ewar,
                    role_bomber, role_scout, role_supercapital, role_non_combat,
                    side_hull_weight_score, side_role_balance_score,
                    side_logi_strength_score, side_command_strength_score,
                    side_capital_ratio, side_expected_performance_score
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        sc["theater_id"], sc["side"], sc["pilot_count"],
                        sc["hull_small_count"], sc["hull_medium_count"],
                        sc["hull_large_count"], sc["hull_capital_count"],
                        sc["role_mainline_dps"], sc["role_capital_dps"],
                        sc["role_logistics"], sc["role_capital_logistics"],
                        sc["role_tackle"], sc["role_heavy_tackle"],
                        sc["role_bubble_control"], sc["role_command"], sc["role_ewar"],
                        sc["role_bomber"], sc["role_scout"],
                        sc["role_supercapital"], sc["role_non_combat"],
                        sc["side_hull_weight_score"], sc["side_role_balance_score"],
                        sc["side_logi_strength_score"], sc["side_command_strength_score"],
                        sc["side_capital_ratio"], sc["side_expected_performance_score"],
                    )
                    for sc in side_composition_rows
                ],
            )
            rows_written += len(side_composition_rows)

        # Structure kills
        if structure_kill_rows:
            cursor.executemany(
                """
                INSERT INTO theater_structure_kills (
                    theater_id, killmail_id, victim_corporation_id, victim_alliance_id,
                    victim_ship_type_id, isk_lost, side, killer_alliance_id,
                    killer_corporation_id, killed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        s["theater_id"], s["killmail_id"],
                        s["victim_corporation_id"], s["victim_alliance_id"],
                        s["victim_ship_type_id"], s["isk_lost"],
                        s["side"], s.get("killer_alliance_id"),
                        s.get("killer_corporation_id"), s["killed_at"],
                    )
                    for s in structure_kill_rows
                ],
            )
            rows_written += len(structure_kill_rows)

        # Update theater aggregates
        cursor.execute(
            """
            UPDATE theaters
            SET total_kills = %s,
                total_isk = %s,
                anomaly_score = %s,
                computed_at = %s
            WHERE theater_id = %s
            """,
            (total_kills, total_isk, anomaly_score, computed_at, theater_id),
        )
        rows_written += max(0, int(cursor.rowcount or 0))

    return rows_written


# ── Entry point ─────────────────────────────────────────────────────────────

def run_theater_analysis(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Analyze all theaters: timeline, sides, participants, anomalies."""
    job = start_job_run(db, "theater_analysis")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _theater_log(runtime, "theater_analysis.job.started", {"dry_run": dry_run, "computed_at": computed_at})

    try:
        theaters = _load_theaters(db)
        rows_processed = len(theaters)
        _theater_log(runtime, "theater_analysis.theaters_loaded", {"count": len(theaters)})

        # Load configuration
        expansion_settings = _load_expansion_settings(db)
        expansion_margin = expansion_settings["margin_seconds"]

        # Load ship type → group mapping for fleet function resolution
        ship_group_map = _load_ship_group_map(db)
        side_configuration = _load_side_configuration_ids(db)
        relationship_graph = _load_alliance_relationship_graph(db)
        _theater_log(
            runtime,
            "theater_analysis.side_configuration_loaded",
            {
                "friendly_alliances": len(side_configuration["friendly_alliance_ids"]),
                "friendly_corporations": len(side_configuration["friendly_corporation_ids"]),
                "opponent_alliances": len(side_configuration["opponent_alliance_ids"]),
                "opponent_corporations": len(side_configuration["opponent_corporation_ids"]),
            },
        )

        if not theaters:
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta={"theaters_analyzed": 0})
            duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
            return JobResult.success(
                job_key="theater_analysis", summary="No theaters to analyze.", rows_processed=0, rows_written=0, duration_ms=duration_ms,
            ).to_dict()

        theaters_analyzed = 0
        for theater in theaters:
            theater_id = str(theater["theater_id"])
            theater_start = _parse_dt(theater["start_time"])
            theater_end = _parse_dt(theater["end_time"])

            # Load constituent battles and theater systems
            battle_ids = _load_theater_battle_ids(db, theater_id)
            if not battle_ids:
                continue
            system_ids = _load_theater_system_ids(db, theater_id)

            # Load killmails with engagement expansion — pulls in kills from
            # the theater's systems within an expanded time window, capturing
            # sub-threshold battles, boundary-split kills, and third-party
            # opportunists that belong to the same engagement.
            time_start = theater_start.strftime("%Y-%m-%d %H:%M:%S") if theater_start else ""
            time_end = theater_end.strftime("%Y-%m-%d %H:%M:%S") if theater_end else ""
            killmails, expansion_diag = _load_expanded_killmails(
                db, battle_ids, system_ids, time_start, time_end,
                margin_seconds=expansion_margin,
            )
            _theater_log(runtime, "theater_analysis.killmails_loaded", {
                "theater_id": theater_id,
                "battle_count": len(battle_ids),
                "system_count": len(system_ids),
                "base_killmail_ids": expansion_diag["base_killmail_ids"],
                "expanded_killmail_ids": expansion_diag["expanded_killmail_ids"],
                "expansion_passed_overlap": expansion_diag["expansion_passed_overlap"],
                "expansion_rejected_no_overlap": expansion_diag["expansion_rejected_no_overlap"],
            })

            # Load battle participants for side determination
            bp_rows = _load_battle_participants_for_theater(db, battle_ids)

            # Supplement bp_rows with characters from expanded killmails that
            # aren't in battle_participants — ensures their ISK is included
            # in alliance summary and participant stats.
            original_bp_count = len(bp_rows)
            bp_rows, supplemented_count = _supplement_bp_rows_from_killmails(bp_rows, killmails)
            if supplemented_count > 0:
                _theater_log(runtime, "theater_analysis.bp_supplemented", {
                    "theater_id": theater_id,
                    "original_bp_rows": original_bp_count,
                    "supplemented_characters": supplemented_count,
                    "total_bp_rows": len(bp_rows),
                })

            # Determine sides
            side_labels, char_sides, side_resolution = _determine_sides(bp_rows, side_configuration, relationship_graph)

            # Compute timeline
            timeline = _compute_timeline(killmails, char_sides, theater_start, theater_end)

            # Detect turning points
            turning_points = _detect_turning_points(timeline)

            # Build per-character ledger — single source of truth
            character_ledger = _build_character_ledger(killmails)

            # Compute alliance summary from character ledger
            alliance_summary = _compute_alliance_summary(character_ledger, bp_rows, side_configuration, char_sides, relationship_graph)

            # Compute participant stats from character ledger
            participant_stats = _compute_participants(killmails, bp_rows, char_sides, theater_id, ship_group_map, character_ledger)

            # Detect structure kills (player-owned structures with no victim character)
            structure_kills = _compute_structure_kills(killmails, side_configuration, theater_id, relationship_graph)

            # Compute side composition features for force-normalization
            side_composition = _compute_side_composition(bp_rows, char_sides, ship_group_map, theater_id)

            # Compute totals
            total_kills = sum(t["kills"] for t in timeline)
            total_isk = sum(t["isk_destroyed"] for t in timeline)

            # Compute anomaly score
            anomaly_score = _compute_anomaly_score(theater, timeline, turning_points, alliance_summary)

            # Flush
            if not dry_run:
                written = _flush_analysis(
                    db, theater_id, timeline, alliance_summary, participant_stats,
                    turning_points, total_kills, total_isk, anomaly_score, computed_at,
                    battle_ids, side_composition, structure_kills,
                )
                rows_written += written

            theaters_analyzed += 1
            _theater_log(runtime, "theater_analysis.theater_done", {
                "theater_id": theater_id,
                "kills": total_kills,
                "participants": len(participant_stats),
                "structure_kills": len(structure_kills),
                "alliances": len(alliance_summary),
                "timeline_buckets": len(timeline),
                "turning_points": len(turning_points),
                "anomaly_score": anomaly_score,
                "side_resolution": "settings_based_classification",
                "friendly_count": side_resolution["total_friendly_matches"],
                "opponent_count": side_resolution["total_opponent_matches"],
                "third_party_count": side_resolution.get("total_third_party", 0),
            })

        # Auto-lock theaters whose end_time is before the clustering lookback
        # window.  Theaters inside the window must remain unlocked so that
        # theater_clustering can re-cluster them as new battles arrive.
        # Falls back to 48 hours if the setting is missing.
        auto_locked = 0
        if not dry_run:
            lookback_row = db.fetch_all(
                "SELECT setting_value FROM app_settings WHERE setting_key = 'theater_clustering_lookback_hours' LIMIT 1"
            )
            lookback_hours = int(lookback_row[0]["setting_value"]) if lookback_row else 48
            if lookback_hours > 0:
                auto_lock_threshold = datetime.now(UTC) - timedelta(hours=lookback_hours)
            else:
                # lookback disabled — fall back to conservative 1-hour threshold
                auto_lock_threshold = datetime.now(UTC) - timedelta(hours=1)
            auto_lock_candidates = db.fetch_all(
                """
                SELECT theater_id, end_time
                FROM theaters
                WHERE locked_at IS NULL
                  AND end_time IS NOT NULL
                  AND end_time < %s
                """,
                (auto_lock_threshold.strftime("%Y-%m-%d %H:%M:%S"),),
            )
            for candidate in auto_lock_candidates:
                tid = str(candidate.get("theater_id") or "")
                if tid:
                    db.execute(
                        "UPDATE theaters SET locked_at = NOW() WHERE theater_id = %s AND locked_at IS NULL",
                        (tid,),
                    )
                    auto_locked += 1
            if auto_locked > 0:
                _theater_log(runtime, "theater_analysis.auto_locked", {
                    "count": auto_locked,
                    "threshold": auto_lock_threshold.isoformat(),
                })

        finish_job_run(
            db, job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "theaters_analyzed": theaters_analyzed, "auto_locked": auto_locked},
        )

        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="theater_analysis",
            summary=f"Analyzed {theaters_analyzed} theaters.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "theaters_analyzed": theaters_analyzed,
                "dry_run": dry_run,
            },
        ).to_dict()
        _theater_log(runtime, "theater_analysis.job.success", result)
        return result

    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _theater_log(runtime, "theater_analysis.job.failed", {"status": "failed", "error": str(exc)})
        raise
