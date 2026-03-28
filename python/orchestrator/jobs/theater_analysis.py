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
    package_root = str(Path(__file__).resolve().parents[2])
    if package_root not in sys.path:
        sys.path.insert(0, package_root)
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
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
else:
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
    from ..job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run

# ── Configuration ────────────────────────────────────────────────────────────

TIMELINE_BUCKET_SECONDS = 60  # 1-minute buckets
MOMENTUM_SMOOTHING_WINDOW = 5  # buckets for momentum moving average
TURNING_POINT_MAGNITUDE_THRESHOLD = 0.3  # minimum momentum swing to flag
BATCH_SIZE = 500


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
        ORDER BY start_time ASC
        """
    )


def _load_theater_battle_ids(db: SupplyCoreDb, theater_id: str) -> list[str]:
    rows = db.fetch_all(
        "SELECT battle_id FROM theater_battles WHERE theater_id = %s",
        (theater_id,),
    )
    return [str(r["battle_id"]) for r in rows]


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
            SELECT
                ke.killmail_id,
                ke.solar_system_id AS system_id,
                ke.effective_killmail_at AS killmail_time,
                ke.battle_id,
                ke.victim_character_id,
                ke.victim_corporation_id,
                ke.victim_alliance_id,
                ke.victim_ship_type_id,
                COALESCE(ke.zkb_total_value, 0) AS total_value,
                ka.character_id AS attacker_character_id,
                ka.corporation_id AS attacker_corporation_id,
                ka.alliance_id AS attacker_alliance_id,
                ka.ship_type_id AS attacker_ship_type_id,
                ka.damage_done AS attacker_damage_done,
                ka.final_blow AS attacker_final_blow
            FROM killmail_events ke
            LEFT JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
            WHERE ke.battle_id IN ({placeholders})
            ORDER BY ke.effective_killmail_at ASC
            """,
            tuple(chunk),
        )
        all_rows.extend(rows)
    return all_rows


def _load_ship_group_map(db: SupplyCoreDb) -> dict[int, int]:
    """Load type_id → group_id mapping for fleet function resolution."""
    rows = db.fetch_all("SELECT type_id, group_id FROM ref_item_types WHERE group_id IS NOT NULL")
    return {int(r["type_id"]): int(r["group_id"]) for r in rows}


def _resolve_fleet_function(ship_type_id: int, ship_group_map: dict[int, int]) -> str:
    """Resolve a ship type ID to its fleet function string."""
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


def _load_side_configuration_ids(db: SupplyCoreDb) -> dict[str, set[int]]:
    """Load configured tracked/opponent alliance/corporation IDs once per job run."""
    friendly_alliance_rows = db.fetch_all(
        "SELECT alliance_id FROM killmail_tracked_alliances WHERE is_active = 1"
    )
    friendly_corporation_rows = db.fetch_all(
        "SELECT corporation_id FROM killmail_tracked_corporations WHERE is_active = 1"
    )
    opponent_alliance_rows = db.fetch_all(
        "SELECT alliance_id FROM killmail_opponent_alliances WHERE is_active = 1"
    )
    opponent_corporation_rows = db.fetch_all(
        "SELECT corporation_id FROM killmail_opponent_corporations WHERE is_active = 1"
    )
    return {
        "friendly_alliance_ids": {
            int(row["alliance_id"]) for row in friendly_alliance_rows if int(row.get("alliance_id") or 0) > 0
        },
        "friendly_corporation_ids": {
            int(row["corporation_id"])
            for row in friendly_corporation_rows
            if int(row.get("corporation_id") or 0) > 0
        },
        "opponent_alliance_ids": {
            int(row["alliance_id"]) for row in opponent_alliance_rows if int(row.get("alliance_id") or 0) > 0
        },
        "opponent_corporation_ids": {
            int(row["corporation_id"])
            for row in opponent_corporation_rows
            if int(row.get("corporation_id") or 0) > 0
        },
    }


# ── Side determination ──────────────────────────────────────────────────────

def _determine_sides(
    participants: list[dict[str, Any]],
    side_configuration: dict[str, set[int]],
) -> tuple[dict[str, str], dict[int, str], dict[str, Any]]:
    """Determine the two largest sides and assign characters.

    Returns:
        side_labels: mapping side_key → "side_a" or "side_b"
        char_sides:  mapping character_id → "side_a" or "side_b"
        side_meta:   scoring/debug metadata
    """
    side_counts: dict[str, int] = defaultdict(int)
    char_side_keys: dict[int, str] = {}
    side_scores: dict[str, dict[str, int]] = {}

    friendly_alliance_ids = side_configuration.get("friendly_alliance_ids", set())
    friendly_corporation_ids = side_configuration.get("friendly_corporation_ids", set())
    opponent_alliance_ids = side_configuration.get("opponent_alliance_ids", set())
    opponent_corporation_ids = side_configuration.get("opponent_corporation_ids", set())

    for p in participants:
        side_key = str(p.get("side_key") or "unknown")
        char_id = int(p.get("character_id") or 0)
        alliance_id = int(p.get("alliance_id") or 0)
        corporation_id = int(p.get("corporation_id") or 0)
        if char_id <= 0:
            continue
        side_counts[side_key] += 1
        char_side_keys[char_id] = side_key
        score_entry = side_scores.setdefault(
            side_key,
            {"friendly_score": 0, "opponent_score": 0, "participant_count": 0},
        )
        score_entry["participant_count"] += 1
        if alliance_id > 0 and alliance_id in friendly_alliance_ids:
            score_entry["friendly_score"] += 1
        if corporation_id > 0 and corporation_id in friendly_corporation_ids:
            score_entry["friendly_score"] += 1
        if alliance_id > 0 and alliance_id in opponent_alliance_ids:
            score_entry["opponent_score"] += 1
        if corporation_id > 0 and corporation_id in opponent_corporation_ids:
            score_entry["opponent_score"] += 1

    # Rank sides by participant count
    ranked = sorted(side_counts.items(), key=lambda x: -x[1])

    side_labels: dict[str, str] = {}
    total_friendly_matches = sum(entry["friendly_score"] for entry in side_scores.values())
    total_opponent_matches = sum(entry["opponent_score"] for entry in side_scores.values())
    used_fallback = total_friendly_matches == 0 and total_opponent_matches == 0

    if used_fallback:
        if len(ranked) >= 1:
            side_labels[ranked[0][0]] = "side_a"
        if len(ranked) >= 2:
            side_labels[ranked[1][0]] = "side_b"
        # All others are "side_b" (smaller coalition)
        for sk in side_counts:
            if sk not in side_labels:
                side_labels[sk] = "side_b"
    else:
        scored_side_keys = list(side_scores.keys())
        friendly_ranked = sorted(
            scored_side_keys,
            key=lambda sk: (
                -side_scores[sk]["friendly_score"],
                side_scores[sk]["opponent_score"],
                sk,
            ),
        )
        opponent_ranked = sorted(
            scored_side_keys,
            key=lambda sk: (
                -side_scores[sk]["opponent_score"],
                side_scores[sk]["friendly_score"],
                sk,
            ),
        )

        if friendly_ranked:
            side_labels[friendly_ranked[0]] = "side_a"
        for sk in opponent_ranked:
            if sk not in side_labels:
                side_labels[sk] = "side_b"
                break

        if "side_b" not in side_labels.values():
            for sk, _ in ranked:
                if sk not in side_labels:
                    side_labels[sk] = "side_b"
                    break

        for sk in side_counts:
            if sk not in side_labels:
                side_labels[sk] = "side_b"

    char_sides: dict[int, str] = {}
    for char_id, sk in char_side_keys.items():
        char_sides[char_id] = side_labels.get(sk, "side_b")

    return side_labels, char_sides, {
        "used_fallback": used_fallback,
        "total_friendly_matches": total_friendly_matches,
        "total_opponent_matches": total_opponent_matches,
        "side_scores": side_scores,
        "side_labels": side_labels,
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
        victim_side = char_sides.get(victim_id, "side_b")

        # A kill counts against the victim's side
        bucket["kills"] += 1
        bucket["isk_destroyed"] += isk

        if victim_side == "side_a":
            bucket["side_b_kills"] += 1  # side_b scored a kill
            bucket["side_b_isk"] += isk
        else:
            bucket["side_a_kills"] += 1  # side_a scored a kill
            bucket["side_a_isk"] += isk

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
            direction = "side_a_surge" if delta > 0 else "side_b_surge"
            description = (
                f"Momentum shifted {'toward side A' if delta > 0 else 'toward side B'} "
                f"({prev:.2f} → {curr:.2f}, Δ={delta:+.2f})"
            )
            turning_points.append({
                "bucket_time": timeline[i]["bucket_time"],
                "direction": direction,
                "magnitude": round(abs(delta), 4),
                "description": description,
            })

    return turning_points


# ── Alliance summary computation ───────────────────────────────────────────

def _compute_alliance_summary(
    killmails: list[dict[str, Any]],
    bp_rows: list[dict[str, Any]],
    side_labels: dict[str, str],
    char_sides: dict[int, str],
) -> list[dict[str, Any]]:
    """Compute per-alliance summary across the theater."""
    # alliance_id → accumulated stats
    alliance_stats: dict[int, dict[str, Any]] = {}

    # Get alliance names from participants
    alliance_names: dict[int, str] = {}

    # Track which characters belong to which alliance
    char_alliance: dict[int, int] = {}
    for p in bp_rows:
        aid = int(p.get("alliance_id") or 0)
        cid = int(p.get("character_id") or 0)
        if cid > 0 and aid > 0:
            char_alliance[cid] = aid

    # Count participants per alliance
    alliance_participants: dict[int, set[int]] = defaultdict(set)
    for p in bp_rows:
        aid = int(p.get("alliance_id") or 0)
        cid = int(p.get("character_id") or 0)
        if aid > 0 and cid > 0:
            alliance_participants[aid].add(cid)

    # Process killmails for kills/losses/damage — deduplicate by killmail_id
    # since rows are expanded per-attacker from the LEFT JOIN.
    seen_loss_km: set[int] = set()
    seen_kill_km: set[tuple[int, int]] = set()  # (killmail_id, alliance_id)
    km_alliance_damage: dict[int, dict[int, float]] = {}  # km_id -> {alliance_id -> total_damage}
    km_isk: dict[int, float] = {}  # km_id -> isk value
    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        victim_id = int(km.get("victim_character_id") or 0)
        victim_alliance = int(km.get("victim_alliance_id") or 0)
        isk = float(km.get("total_value") or 0)

        attacker_id = int(km.get("attacker_character_id") or 0)
        attacker_alliance = int(km.get("attacker_alliance_id") or 0)
        attacker_damage = float(km.get("attacker_damage_done") or 0)

        # Record loss for victim alliance (once per killmail)
        if victim_alliance > 0 and km_id not in seen_loss_km:
            seen_loss_km.add(km_id)
            entry = alliance_stats.setdefault(victim_alliance, _empty_alliance_stats(victim_alliance))
            entry["total_losses"] += 1
            entry["total_isk_lost"] += isk

        # Record kill for attacker alliance (once per killmail per alliance)
        # ISK is attributed proportionally by damage dealt, not duplicated.
        if attacker_alliance > 0:
            entry = alliance_stats.setdefault(attacker_alliance, _empty_alliance_stats(attacker_alliance))
            entry["total_damage"] += attacker_damage
            kill_key = (km_id, attacker_alliance)
            if kill_key not in seen_kill_km:
                seen_kill_km.add(kill_key)
                entry["total_kills"] += 1
                # Track per-killmail damage by alliance for proportional ISK later
                km_alliance_damage.setdefault(km_id, {})[attacker_alliance] = \
                    km_alliance_damage.get(km_id, {}).get(attacker_alliance, 0.0) + attacker_damage
                km_isk[km_id] = isk

    # Distribute ISK proportionally by damage dealt per alliance
    for km_id, alliance_damages in km_alliance_damage.items():
        total_damage = sum(alliance_damages.values())
        isk = km_isk.get(km_id, 0.0)
        for aid, dmg in alliance_damages.items():
            share = dmg / total_damage if total_damage > 0 else 1.0 / len(alliance_damages)
            entry = alliance_stats.get(aid)
            if entry is not None:
                entry["total_isk_killed"] += isk * share

    # Finalize
    result: list[dict[str, Any]] = []
    for aid, stats in alliance_stats.items():
        stats["participant_count"] = len(alliance_participants.get(aid, set()))
        # Determine side from majority of characters
        side_votes: dict[str, int] = defaultdict(int)
        for cid in alliance_participants.get(aid, set()):
            side_votes[char_sides.get(cid, "side_b")] += 1
        stats["side"] = max(side_votes, key=side_votes.get) if side_votes else "side_b"
        # Efficiency
        total_isk = stats["total_isk_killed"] + stats["total_isk_lost"]
        stats["efficiency"] = round(_safe_div(stats["total_isk_killed"], total_isk, 0.0), 4)
        result.append(stats)

    return result


def _empty_alliance_stats(alliance_id: int) -> dict[str, Any]:
    return {
        "alliance_id": alliance_id,
        "alliance_name": None,
        "side": "side_b",
        "participant_count": 0,
        "total_kills": 0,
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
) -> list[dict[str, Any]]:
    """Compute per-character stats across the theater."""
    char_stats: dict[int, dict[str, Any]] = {}
    char_battles: dict[int, set[str]] = defaultdict(set)
    char_times: dict[int, list[datetime]] = defaultdict(list)

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
            ship_type_id = int(p.get("ship_type_id") or 0)

            # Resolve fleet function from ship group
            if ship_group_map is not None:
                role_proxy = _resolve_fleet_function(ship_type_id, ship_group_map)
            else:
                # Fallback to legacy boolean flags
                is_logi = int(p.get("is_logi") or 0)
                is_command = int(p.get("is_command") or 0)
                is_capital = int(p.get("is_capital") or 0)
                role_proxy = "mainline_dps"
                if is_logi:
                    role_proxy = "logistics"
                elif is_command:
                    role_proxy = "command"
                elif is_capital:
                    role_proxy = "capital_dps"

            char_stats[cid] = {
                "character_id": cid,
                "character_name": None,
                "alliance_id": aid if aid > 0 else None,
                "corporation_id": corp_id if corp_id > 0 else None,
                "side": char_sides.get(cid, "side_b"),
                "ship_type_ids": [],
                "kills": 0,
                "deaths": 0,
                "damage_done": 0.0,
                "damage_taken": 0.0,
                "isk_lost": 0.0,
                "role_proxy": role_proxy,
            }

        # Track ship types
        st = int(p.get("ship_type_id") or 0)
        if st > 0 and st not in char_stats[cid]["ship_type_ids"]:
            char_stats[cid]["ship_type_ids"].append(st)

    # Pre-compute total HP damage taken per killmail (sum of all attacker damage)
    km_total_hp_damage: dict[int, float] = defaultdict(float)
    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        attacker_damage = float(km.get("attacker_damage_done") or 0)
        if km_id > 0:
            km_total_hp_damage[km_id] += attacker_damage

    # Process killmails — deduplicate by killmail_id since rows are expanded
    # per-attacker from the LEFT JOIN.
    seen_deaths: set[tuple[int, int]] = set()  # (killmail_id, victim_id)
    seen_attacker_kills: set[tuple[int, int]] = set()  # (killmail_id, attacker_id)
    for km in killmails:
        km_id = int(km.get("killmail_id") or 0)
        km_time = _parse_dt(km.get("killmail_time"))

        victim_id = int(km.get("victim_character_id") or 0)
        isk = float(km.get("total_value") or 0)

        # Count death only once per (killmail, victim) pair
        death_key = (km_id, victim_id)
        if victim_id > 0 and victim_id in char_stats and death_key not in seen_deaths:
            seen_deaths.add(death_key)
            char_stats[victim_id]["deaths"] += 1
            char_stats[victim_id]["damage_taken"] += km_total_hp_damage.get(km_id, 0.0)
            char_stats[victim_id]["isk_lost"] += isk
            char_times[victim_id].append(km_time)

        attacker_id = int(km.get("attacker_character_id") or 0)
        attacker_damage = float(km.get("attacker_damage_done") or 0)

        if attacker_id > 0 and attacker_id in char_stats:
            # Count kill participation once per (killmail, attacker) pair
            atk_key = (km_id, attacker_id)
            if atk_key not in seen_attacker_kills:
                seen_attacker_kills.add(atk_key)
                char_stats[attacker_id]["kills"] += 1
                char_times[attacker_id].append(km_time)
            char_stats[attacker_id]["damage_done"] += attacker_damage

    # Build final rows
    result: list[dict[str, Any]] = []
    for cid, stats in char_stats.items():
        times = sorted(char_times.get(cid, []))
        entry_time = times[0].strftime("%Y-%m-%d %H:%M:%S") if times else None
        exit_time = times[-1].strftime("%Y-%m-%d %H:%M:%S") if times else None

        ship_json = json_dumps_safe(stats["ship_type_ids"]) if stats["ship_type_ids"] else None

        result.append({
            "theater_id": theater_id,
            "character_id": cid,
            "character_name": stats["character_name"],
            "alliance_id": stats["alliance_id"],
            "corporation_id": stats["corporation_id"],
            "side": stats["side"],
            "ship_type_ids": ship_json,
            "kills": stats["kills"],
            "deaths": stats["deaths"],
            "damage_done": stats["damage_done"],
            "damage_taken": stats["damage_taken"],
            "isk_lost": stats["isk_lost"],
            "role_proxy": stats["role_proxy"],
            "entry_time": entry_time,
            "exit_time": exit_time,
            "battles_present": len(char_battles.get(cid, set())),
            "suspicion_score": None,  # populated in theater_suspicion phase
            "is_suspicious": 0,
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
        side = char_sides.get(cid, "side_b")
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
    side_a_eff = [a["efficiency"] for a in alliance_summary if a["side"] == "side_a"]
    side_b_eff = [a["efficiency"] for a in alliance_summary if a["side"] == "side_b"]
    avg_a = sum(side_a_eff) / len(side_a_eff) if side_a_eff else 0.5
    avg_b = sum(side_b_eff) / len(side_b_eff) if side_b_eff else 0.5
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
) -> int:
    """Write analysis results for one theater. Returns rows written."""
    rows_written = 0

    with db.transaction() as (_, cursor):
        # Clean old data for this theater
        cursor.execute("DELETE FROM theater_timeline WHERE theater_id = %s", (theater_id,))
        cursor.execute("DELETE FROM theater_alliance_summary WHERE theater_id = %s", (theater_id,))
        cursor.execute("DELETE FROM theater_participants WHERE theater_id = %s", (theater_id,))

        # Clean turning points for battles in this theater
        for bid in battle_ids:
            cursor.execute("DELETE FROM battle_turning_points WHERE battle_id = %s", (bid,))

        # Timeline
        for t in timeline_rows:
            cursor.execute(
                """
                INSERT INTO theater_timeline (
                    theater_id, bucket_time, bucket_seconds, kills, isk_destroyed,
                    side_a_kills, side_b_kills, side_a_isk, side_b_isk, momentum_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    theater_id, t["bucket_time"], TIMELINE_BUCKET_SECONDS,
                    t["kills"], t["isk_destroyed"],
                    t["side_a_kills"], t["side_b_kills"],
                    t["side_a_isk"], t["side_b_isk"],
                    t["momentum_score"],
                ),
            )
            rows_written += 1

        # Alliance summary
        for a in alliance_rows:
            cursor.execute(
                """
                INSERT INTO theater_alliance_summary (
                    theater_id, alliance_id, alliance_name, side,
                    participant_count, total_kills, total_losses,
                    total_damage, total_isk_lost, total_isk_killed, efficiency
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    theater_id, a["alliance_id"], a["alliance_name"], a["side"],
                    a["participant_count"], a["total_kills"], a["total_losses"],
                    a["total_damage"], a["total_isk_lost"], a["total_isk_killed"],
                    a["efficiency"],
                ),
            )
            rows_written += 1

        # Participants (batch insert)
        for chunk_start in range(0, len(participant_rows), BATCH_SIZE):
            chunk = participant_rows[chunk_start:chunk_start + BATCH_SIZE]
            cursor.executemany(
                """
                INSERT INTO theater_participants (
                    theater_id, character_id, character_name, alliance_id,
                    corporation_id, side, ship_type_ids, kills, deaths,
                    damage_done, damage_taken, isk_lost, role_proxy,
                    entry_time, exit_time, battles_present,
                    suspicion_score, is_suspicious
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        p["theater_id"], p["character_id"], p["character_name"],
                        p["alliance_id"], p["corporation_id"], p["side"],
                        p["ship_type_ids"], p["kills"], p["deaths"],
                        p["damage_done"], p["damage_taken"], p["isk_lost"], p["role_proxy"],
                        p["entry_time"], p["exit_time"], p["battles_present"],
                        p["suspicion_score"], p["is_suspicious"],
                    )
                    for p in chunk
                ],
            )
            rows_written += len(chunk)

        # Turning points
        for tp in turning_points:
            # Assign to the first battle in the theater (convention)
            bid = battle_ids[0] if battle_ids else theater_id
            cursor.execute(
                """
                INSERT INTO battle_turning_points (
                    battle_id, turning_point_at, direction, magnitude, description
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (bid, tp["bucket_time"], tp["direction"], tp["magnitude"], tp["description"]),
            )
            rows_written += 1

        # Side composition
        if side_composition_rows:
            cursor.execute("DELETE FROM theater_side_composition WHERE theater_id = %s", (theater_id,))
            for sc in side_composition_rows:
                cursor.execute(
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
                    ),
                )
                rows_written += 1

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
    lock_key = "theater_analysis"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=1800)
    if owner is None:
        result = JobResult.skipped(job_key="theater_analysis", reason="lock-not-acquired").to_dict()
        _theater_log(runtime, "theater_analysis.job.skipped", result)
        return result

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

        # Load ship type → group mapping for fleet function resolution
        ship_group_map = _load_ship_group_map(db)
        side_configuration = _load_side_configuration_ids(db)
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

            # Load constituent battles
            battle_ids = _load_theater_battle_ids(db, theater_id)
            if not battle_ids:
                continue

            # Load killmails
            killmails = _load_killmails_for_battles(db, battle_ids)

            # Load battle participants for side determination
            bp_rows = _load_battle_participants_for_theater(db, battle_ids)

            # Determine sides
            side_labels, char_sides, side_resolution = _determine_sides(bp_rows, side_configuration)

            # Compute timeline
            timeline = _compute_timeline(killmails, char_sides, theater_start, theater_end)

            # Detect turning points
            turning_points = _detect_turning_points(timeline)

            # Compute alliance summary
            alliance_summary = _compute_alliance_summary(killmails, bp_rows, side_labels, char_sides)

            # Compute participant stats
            participant_stats = _compute_participants(killmails, bp_rows, char_sides, theater_id, ship_group_map)

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
                    battle_ids, side_composition,
                )
                rows_written += written

            theaters_analyzed += 1
            _theater_log(runtime, "theater_analysis.theater_done", {
                "theater_id": theater_id,
                "kills": total_kills,
                "participants": len(participant_stats),
                "alliances": len(alliance_summary),
                "timeline_buckets": len(timeline),
                "turning_points": len(turning_points),
                "anomaly_score": anomaly_score,
                "side_resolution": "unresolved_fallback_size_based" if side_resolution["used_fallback"] else "configured_entity_match",
                "friendly_matches": side_resolution["total_friendly_matches"],
                "opponent_matches": side_resolution["total_opponent_matches"],
                "side_labels": side_resolution["side_labels"],
                "side_scores": side_resolution["side_scores"],
            })

        finish_job_run(
            db, job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "theaters_analyzed": theaters_analyzed},
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
    finally:
        release_job_lock(db, lock_key, owner)
