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
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
else:
    from ..db import SupplyCoreDb
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


# ── Side determination ──────────────────────────────────────────────────────

def _determine_sides(
    participants: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[int, str]]:
    """Determine the two largest sides and assign characters.

    Returns:
        side_labels: mapping side_key → "side_a" or "side_b"
        char_sides:  mapping character_id → "side_a" or "side_b"
    """
    side_counts: dict[str, int] = defaultdict(int)
    char_side_keys: dict[int, str] = {}

    for p in participants:
        side_key = str(p.get("side_key") or "unknown")
        char_id = int(p.get("character_id") or 0)
        if char_id <= 0:
            continue
        side_counts[side_key] += 1
        char_side_keys[char_id] = side_key

    # Rank sides by participant count
    ranked = sorted(side_counts.items(), key=lambda x: -x[1])

    side_labels: dict[str, str] = {}
    if len(ranked) >= 1:
        side_labels[ranked[0][0]] = "side_a"
    if len(ranked) >= 2:
        side_labels[ranked[1][0]] = "side_b"
    # All others are "side_b" (smaller coalition)
    for sk in side_counts:
        if sk not in side_labels:
            side_labels[sk] = "side_b"

    char_sides: dict[int, str] = {}
    for char_id, sk in char_side_keys.items():
        char_sides[char_id] = side_labels.get(sk, "side_b")

    return side_labels, char_sides


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

    # Process killmails for kills/losses/damage — deduplicate losses by
    # killmail_id (rows are per-attacker), but attacker kills are per-row
    seen_loss_km: set[int] = set()
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

        # Record kill for attacker alliance (final blow only, like zKillboard)
        if attacker_alliance > 0:
            entry = alliance_stats.setdefault(attacker_alliance, _empty_alliance_stats(attacker_alliance))
            entry["total_damage"] += attacker_damage
            if int(km.get("attacker_final_blow") or 0) == 1:
                entry["total_kills"] += 1
                entry["total_isk_killed"] += isk

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
            is_logi = int(p.get("is_logi") or 0)
            is_command = int(p.get("is_command") or 0)
            is_capital = int(p.get("is_capital") or 0)

            role_proxy = "dps"
            if is_logi:
                role_proxy = "logi"
            elif is_command:
                role_proxy = "command"
            elif is_capital:
                role_proxy = "capital"

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
                "role_proxy": role_proxy,
            }

        # Track ship types
        st = int(p.get("ship_type_id") or 0)
        if st > 0 and st not in char_stats[cid]["ship_type_ids"]:
            char_stats[cid]["ship_type_ids"].append(st)

    # Process killmails — deduplicate deaths by killmail_id (rows are
    # expanded per-attacker), but attacker kills/damage are per-row
    seen_deaths: set[tuple[int, int]] = set()  # (killmail_id, victim_id)
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
            char_stats[victim_id]["damage_taken"] += isk
            char_times[victim_id].append(km_time)

        attacker_id = int(km.get("attacker_character_id") or 0)
        attacker_damage = float(km.get("attacker_damage_done") or 0)

        if attacker_id > 0 and attacker_id in char_stats:
            if int(km.get("attacker_final_blow") or 0) == 1:
                char_stats[attacker_id]["kills"] += 1
            char_stats[attacker_id]["damage_done"] += attacker_damage
            char_times[attacker_id].append(km_time)

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
            "role_proxy": stats["role_proxy"],
            "entry_time": entry_time,
            "exit_time": exit_time,
            "battles_present": len(char_battles.get(cid, set())),
            "suspicion_score": None,  # populated in theater_suspicion phase
            "is_suspicious": 0,
        })

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
                    damage_done, damage_taken, role_proxy,
                    entry_time, exit_time, battles_present,
                    suspicion_score, is_suspicious
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        p["theater_id"], p["character_id"], p["character_name"],
                        p["alliance_id"], p["corporation_id"], p["side"],
                        p["ship_type_ids"], p["kills"], p["deaths"],
                        p["damage_done"], p["damage_taken"], p["role_proxy"],
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
            side_labels, char_sides = _determine_sides(bp_rows)

            # Compute timeline
            timeline = _compute_timeline(killmails, char_sides, theater_start, theater_end)

            # Detect turning points
            turning_points = _detect_turning_points(timeline)

            # Compute alliance summary
            alliance_summary = _compute_alliance_summary(killmails, bp_rows, side_labels, char_sides)

            # Compute participant stats
            participant_stats = _compute_participants(killmails, bp_rows, char_sides, theater_id)

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
                    battle_ids,
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
