from __future__ import annotations

import bisect
import math
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
import hashlib
from pathlib import Path
from typing import Any

import pymysql.err

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from orchestrator.config import resolve_app_root  # noqa: F401
    from orchestrator.db import SupplyCoreDb
    from orchestrator.horizon import (
        HorizonState,
        horizon_state_get,
        should_use_incremental_only,
        update_watermark_and_stall,
    )
    from orchestrator.job_result import JobResult
    from orchestrator.json_utils import json_dumps_safe
    from orchestrator.job_utils import (
        acquire_job_lock,
        finish_job_run,
        release_job_lock,
        start_job_run,
    )
    from orchestrator.neo4j import Neo4jClient, Neo4jConfig
else:
    from ..config import resolve_app_root  # noqa: F401
    from ..db import SupplyCoreDb
    from ..horizon import (
        HorizonState,
        horizon_state_get,
        should_use_incremental_only,
        update_watermark_and_stall,
    )
    from ..job_result import JobResult
    from ..json_utils import json_dumps_safe
    from ..job_utils import (
        acquire_job_lock,
        finish_job_run,
        release_job_lock,
        start_job_run,
    )
    from ..neo4j import Neo4jClient, Neo4jConfig

WINDOW_SECONDS = 15 * 60
MIN_ELIGIBLE_PARTICIPANTS = 20
MIN_SAMPLE_COUNT = 5
EPSILON = 1e-6
ROLLUP_BATCH_SIZE = 1000
TARGET_METRICS_BATCH_SIZE = 1000
SYNC_STATE_KEY_BATTLE_ROLLUPS_CURSOR = "compute_battle_rollups:last_sequence_id"
SYNC_STATE_KEY_BATTLE_TARGET_CURSOR = "compute_battle_target_metrics:last_sequence_id"

# Explainable suspicion model weights (must sum to 1.0).
SUSPICION_WEIGHTS: dict[str, float] = {
    "high_sustain_frequency": 0.20,
    "cross_side_rate": 0.12,
    "enemy_efficiency_uplift": 0.22,
    "high_minus_low": 0.08,
    "role_weight": 0.08,
    "co_occurrence_density": 0.10,
    "anomalous_co_occurrence_density": 0.08,
    "cross_side_cluster_score": 0.05,
    "neighbor_anomaly_score": 0.04,
    "bridge_score": 0.03,
}


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator <= 0:
        return default
    return numerator / denominator


def _dps_bucket(dps: float) -> str:
    if dps < 100:
        return "lt_100"
    if dps < 250:
        return "100_249"
    if dps < 500:
        return "250_499"
    if dps < 1000:
        return "500_999"
    if dps < 2000:
        return "1000_1999"
    return "gte_2000"


def _battle_size_class(participant_count: int) -> str:
    if participant_count >= 200:
        return "mega"
    if participant_count >= 100:
        return "large"
    if participant_count >= 50:
        return "medium"
    if participant_count >= 20:
        return "small"
    return "micro"


def _percentile(sorted_values: list[float], value: float) -> float:
    if not sorted_values:
        return 0.0
    below = bisect.bisect_right(sorted_values, value)
    return _safe_div(float(below), float(len(sorted_values)), 0.0)


def _stddev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(max(0.0, variance))


def _battle_log(runtime: dict[str, Any] | None, event: str, payload: dict[str, Any]) -> None:
    log_path = str(((runtime or {}).get("log_file") or "")).strip()
    if log_path == "":
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json_dumps_safe(record) + "\n")


def _sync_state_cursor_get(db: SupplyCoreDb, dataset_key: str) -> int:
    row = db.fetch_one("SELECT last_cursor FROM sync_state WHERE dataset_key = %s LIMIT 1", (dataset_key,))
    if not row:
        return 0
    raw = row.get("last_cursor")
    try:
        return max(0, int(raw or 0))
    except (TypeError, ValueError):
        return 0


def _validate_killmail_cursor(db: SupplyCoreDb, cursor: int) -> int:
    """Reset cursor if it is ahead of data or if unprocessed killmails exist below it.

    This handles two scenarios:
    1. External sequence resets (e.g. zKill renumbering) that strand the cursor
       above all new data.
    2. Mixed sequence_id numbering caused by historical backfill using killmail_id
       as sequence_id while the live R2Z2 stream uses its own sequence numbering.
       In that case, recently ingested killmails may sit below the cursor and
       never get picked up by the rollup.
    """
    if cursor <= 0:
        return cursor
    row = db.fetch_one("SELECT MAX(sequence_id) AS max_seq FROM killmail_events")
    max_seq = int(row.get("max_seq") or 0) if row else 0
    if max_seq > 0 and cursor > max_seq:
        return 0
    # Detect unprocessed killmails below the cursor.  These appear when the
    # history backfill inserts rows with sequence_id = killmail_id (high range)
    # while the live stream inserts rows with R2Z2 sequence numbers (lower
    # range).  The cursor advances past the backfill data and the live rows
    # become invisible.  We look for recently-created rows that still lack a
    # battle_id assignment — a strong signal they were never seen by the rollup.
    gap_row = db.fetch_one(
        """
        SELECT MIN(sequence_id) AS min_unprocessed
        FROM killmail_events
        WHERE sequence_id < %s
          AND solar_system_id IS NOT NULL
          AND battle_id IS NULL
          AND created_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR)
        """,
        (cursor,),
    )
    min_unprocessed = int(gap_row.get("min_unprocessed") or 0) if gap_row else 0
    if min_unprocessed > 0:
        return max(0, min_unprocessed - 1)
    return cursor


def _horizon_apply_repair_window(
    db: SupplyCoreDb,
    state: HorizonState | None,
    cursor: int,
) -> tuple[int, dict[str, Any]]:
    """Apply the horizon rolling-repair-window to a killmail sequence cursor.

    When the dataset is gated into incremental-only horizon mode, drop
    the read-from sequence id to the smallest killmail recorded within
    the last ``repair_window_seconds`` window. That lets late-arriving
    killmails (inside the window) flow through on every run -- the
    existing ``battle_id IS NULL`` filter on the rollup query keeps it
    idempotent, so re-reading the tail costs nothing correctness-wise.

    Returns the (possibly lowered) cursor and a meta dict describing
    what happened for observability.

    When horizon mode is off (the default), returns the cursor unchanged
    and meta = ``{"applied": False, ...}``.
    """
    meta: dict[str, Any] = {
        "applied": False,
        "reason": "gate_off",
        "repair_window_seconds": None,
        "rewound_to": None,
    }
    if not should_use_incremental_only(state):
        return cursor, meta

    assert state is not None  # for type-checkers
    repair_window = max(0, state.repair_window_seconds)
    meta["repair_window_seconds"] = repair_window
    if repair_window == 0:
        meta["reason"] = "zero_window"
        return cursor, meta

    row = db.fetch_one(
        """
        SELECT MIN(sequence_id) AS min_seq
          FROM killmail_events
         WHERE effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL %s SECOND)
        """,
        (repair_window,),
    ) or {}
    min_seq = row.get("min_seq")
    if min_seq is None:
        meta["reason"] = "no_killmails_in_window"
        return cursor, meta

    window_start = max(0, int(min_seq) - 1)
    if window_start >= cursor:
        meta["reason"] = "window_start_not_lower"
        return cursor, meta

    meta["applied"] = True
    meta["reason"] = "window_applied"
    meta["rewound_to"] = window_start
    meta["rewound_from"] = cursor
    return window_start, meta


def _sync_state_cursor_upsert(
    db: SupplyCoreDb,
    dataset_key: str,
    *,
    status: str,
    last_cursor: int,
    last_row_count: int = 0,
    error_text: str | None = None,
) -> None:
    db.execute(
        """
        INSERT INTO sync_state (
            dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message
        ) VALUES (
            %s, 'incremental', %s, %s, %s, %s, NULL, %s
        )
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            last_success_at = VALUES(last_success_at),
            last_cursor = VALUES(last_cursor),
            last_row_count = VALUES(last_row_count),
            last_error_message = VALUES(last_error_message)
        """,
        (
            dataset_key,
            status,
            _now_sql() if status == "success" else None,
            str(max(0, int(last_cursor))),
            max(0, int(last_row_count)),
            (str(error_text)[:500] if error_text else None),
        ),
    )


def _maybe_float(raw: Any) -> float | None:
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _neo4j_sync_participation(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    config = Neo4jConfig.from_runtime(neo4j_raw or {})
    if not config.enabled:
        return {"status": "skipped", "reason": "neo4j disabled", "rows_written": 0}

    rows = db.fetch_all(
        """
        SELECT
            baf.battle_id,
            br.system_id,
            br.started_at,
            br.ended_at,
            baf.character_id,
            baf.side_key,
            baf.centrality_score,
            baf.visibility_score,
            COALESCE(bp.alliance_id, 0) AS alliance_id,
            COALESCE(bp.corporation_id, 0) AS corporation_id
        FROM battle_actor_features baf
        INNER JOIN battle_rollups br ON br.battle_id = baf.battle_id
        INNER JOIN battle_participants bp ON bp.battle_id = baf.battle_id AND bp.character_id = baf.character_id
        WHERE br.eligible_for_suspicion = 1
        ORDER BY br.started_at DESC
        LIMIT 20000
        """
    )
    if not rows:
        return {"status": "success", "rows_written": 0}

    client = Neo4jClient(config)
    client.query("CREATE CONSTRAINT battle_battle_id IF NOT EXISTS FOR (b:Battle) REQUIRE b.battle_id IS UNIQUE")
    client.query("CREATE CONSTRAINT character_character_id IF NOT EXISTS FOR (c:Character) REQUIRE c.character_id IS UNIQUE")

    client.query(
        """
        UNWIND $rows AS row
        MERGE (b:Battle {battle_id: row.battle_id})
          SET b.system_id = row.system_id,
              b.started_at = row.started_at,
              b.ended_at = row.ended_at
        MERGE (c:Character {character_id: row.character_id})
        MERGE (c)-[r:PARTICIPATED_IN]->(b)
          SET r.side_key = row.side_key,
              r.centrality = row.centrality_score,
              r.visibility = row.visibility_score,
              r.alliance_id = row.alliance_id,
              r.corporation_id = row.corporation_id
        """,
        {"rows": rows},
    )
    return {"status": "success", "rows_written": len(rows)}


def run_compute_battle_rollups(db: SupplyCoreDb, runtime: dict[str, Any] | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    job = start_job_run(db, "compute_battle_rollups")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    raw_cursor = _sync_state_cursor_get(db, SYNC_STATE_KEY_BATTLE_ROLLUPS_CURSOR)
    raw_cursor = _validate_killmail_cursor(db, raw_cursor)
    horizon_state = horizon_state_get(db, SYNC_STATE_KEY_BATTLE_ROLLUPS_CURSOR)
    cursor_start, horizon_meta = _horizon_apply_repair_window(db, horizon_state, raw_cursor)
    cursor_end = cursor_start
    watermark_event_time: str | None = None
    batch_count = 0
    _sync_state_cursor_upsert(db, SYNC_STATE_KEY_BATTLE_ROLLUPS_CURSOR, status="running", last_cursor=cursor_start, last_row_count=0)
    _battle_log(
        runtime,
        "battle_intelligence.job.started",
        {
            "job_name": "compute_battle_rollups",
            "dry_run": dry_run,
            "computed_at": computed_at,
            "horizon": horizon_meta,
        },
    )
    try:
        role_rows = db.fetch_all(
            """
            SELECT
                rit.type_id,
                LOWER(COALESCE(rig.group_name, '')) AS group_name,
                LOWER(COALESCE(ric.category_name, '')) AS category_name
            FROM ref_item_types rit
            LEFT JOIN ref_item_groups rig ON rig.group_id = rit.group_id
            LEFT JOIN ref_item_categories ric ON ric.category_id = rit.category_id
            """
        )
        role_map: dict[int, tuple[int, int, int]] = {}
        for role_row in role_rows:
            type_id = int(role_row.get("type_id") or 0)
            if type_id <= 0:
                continue
            group_name = str(role_row.get("group_name") or "")
            category_name = str(role_row.get("category_name") or "")
            is_logi = 1 if "logistics" in group_name else 0
            is_command = 1 if "command" in group_name else 0
            capital_groups = ["dreadnought", "carrier", "force auxiliary", "supercarrier", "titan", "capital"]
            is_capital = 1 if any(token in group_name for token in capital_groups) or "capital" in category_name else 0
            role_map[type_id] = (is_logi, is_command, is_capital)

        while True:
            batch_started = datetime.now(UTC)
            # The `battle_id IS NULL` filter is critical for idempotency: when
            # `_validate_killmail_cursor` rewinds the cursor (because the live
            # R2Z2 stream interleaves with backfilled history), we must skip
            # killmails that have already been rolled up.  Without this guard,
            # `participation_count` accumulates across runs via the
            # `ON DUPLICATE KEY UPDATE participation_count = participation_count + …`
            # clause below and eventually overflows the SUM in
            # `compute_battle_anomalies`.
            killmails = db.fetch_all(
                """
                SELECT
                    ke.sequence_id,
                    ke.killmail_id,
                    ke.solar_system_id AS system_id,
                    ke.effective_killmail_at AS killmail_time,
                    ke.victim_character_id,
                    ke.victim_corporation_id,
                    ke.victim_alliance_id,
                    ke.victim_ship_type_id,
                    ka.character_id AS attacker_character_id,
                    ka.corporation_id AS attacker_corporation_id,
                    ka.alliance_id AS attacker_alliance_id,
                    ka.ship_type_id AS attacker_ship_type_id
                FROM killmail_events ke
                LEFT JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
                WHERE ke.sequence_id > %s
                  AND ke.solar_system_id IS NOT NULL
                  AND ke.battle_id IS NULL
                ORDER BY ke.sequence_id ASC
                LIMIT %s
                """,
                (cursor_end, ROLLUP_BATCH_SIZE),
            )
            if not killmails:
                break

            batch_count += 1
            rows_processed += len(killmails)
            battles: dict[str, dict[str, Any]] = {}
            participant_rows: dict[tuple[str, int], dict[str, Any]] = {}
            killmail_assignments: list[tuple[str, int]] = []
            touched_battles: set[str] = set()
            max_sequence_id = cursor_end

            for row in killmails:
                sequence_id = int(row.get("sequence_id") or 0)
                if sequence_id <= 0:
                    continue
                max_sequence_id = max(max_sequence_id, sequence_id)
                system_id = int(row.get("system_id") or 0)
                killmail_id = int(row.get("killmail_id") or 0)
                killmail_time = str(row.get("killmail_time") or "")
                if system_id <= 0 or killmail_id <= 0 or killmail_time == "":
                    continue
                # Track the latest source event time for the horizon
                # watermark (used by the freshness report + SLA checks).
                if watermark_event_time is None or killmail_time > watermark_event_time:
                    watermark_event_time = killmail_time

                bucket_unix = int(datetime.strptime(killmail_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).timestamp() // WINDOW_SECONDS * WINDOW_SECONDS)
                battle_id = f"{system_id}:{bucket_unix}"
                battle_hash = hashlib.sha256(battle_id.encode("utf-8")).hexdigest()
                touched_battles.add(battle_hash)
                killmail_assignments.append((battle_hash, sequence_id))

                battle = battles.setdefault(
                    battle_hash,
                    {"battle_id": battle_hash, "system_id": system_id, "started_at": killmail_time, "ended_at": killmail_time},
                )
                if killmail_time < battle["started_at"]:
                    battle["started_at"] = killmail_time
                if killmail_time > battle["ended_at"]:
                    battle["ended_at"] = killmail_time

                for role in (
                    (int(row.get("victim_character_id") or 0), int(row.get("victim_corporation_id") or 0), int(row.get("victim_alliance_id") or 0), int(row.get("victim_ship_type_id") or 0)),
                    (int(row.get("attacker_character_id") or 0), int(row.get("attacker_corporation_id") or 0), int(row.get("attacker_alliance_id") or 0), int(row.get("attacker_ship_type_id") or 0)),
                ):
                    character_id, corporation_id, alliance_id, ship_type_id = role
                    if character_id <= 0:
                        continue
                    side_key = f"a:{alliance_id}" if alliance_id > 0 else (f"c:{corporation_id}" if corporation_id > 0 else "unknown")
                    key = (battle_hash, character_id)
                    current = participant_rows.setdefault(
                        key,
                        {
                            "battle_id": battle_hash,
                            "character_id": character_id,
                            "corporation_id": corporation_id if corporation_id > 0 else None,
                            "alliance_id": alliance_id if alliance_id > 0 else None,
                            "side_key": side_key,
                            "ship_type_id": ship_type_id if ship_type_id > 0 else None,
                            "participation_count": 0,
                        },
                    )
                    if current.get("corporation_id") is None and corporation_id > 0:
                        current["corporation_id"] = corporation_id
                    if current.get("alliance_id") is None and alliance_id > 0:
                        current["alliance_id"] = alliance_id
                    if int(current.get("ship_type_id") or 0) <= 0 and ship_type_id > 0:
                        current["ship_type_id"] = ship_type_id
                    if str(current.get("side_key") or "") == "unknown" and side_key != "unknown":
                        current["side_key"] = side_key
                    current["participation_count"] = int(current["participation_count"]) + 1

            batch_written = 0
            if not dry_run:
                rollup_batch = []
                for battle in battles.values():
                    started = datetime.strptime(str(battle["started_at"]), "%Y-%m-%d %H:%M:%S")
                    ended = datetime.strptime(str(battle["ended_at"]), "%Y-%m-%d %H:%M:%S")
                    duration_seconds = max(1, int((ended - started).total_seconds()))
                    rollup_batch.append(
                        (str(battle["battle_id"]), int(battle["system_id"]), str(battle["started_at"]), str(battle["ended_at"]), duration_seconds, computed_at),
                    )
                # Sort by battle_id so concurrent transactions acquire locks in the same order.
                rollup_batch.sort(key=lambda r: r[0])

                participant_batch = []
                for participant in participant_rows.values():
                    ship_type_id = int(participant.get("ship_type_id") or 0)
                    flags = role_map.get(ship_type_id, (0, 0, 0))
                    participant_batch.append(
                        (
                            str(participant["battle_id"]),
                            int(participant["character_id"]),
                            participant.get("corporation_id"),
                            participant.get("alliance_id"),
                            str(participant["side_key"]),
                            participant.get("ship_type_id"),
                            int(flags[0]),
                            int(flags[1]),
                            int(flags[2]),
                            int(participant.get("participation_count") or 0),
                            computed_at,
                        ),
                    )
                # Sort by (battle_id, character_id) for consistent lock ordering.
                participant_batch.sort(key=lambda r: (r[0], r[1]))

                # Sort killmail assignments by sequence_id for consistent lock ordering.
                sorted_killmail_assignments = sorted(killmail_assignments, key=lambda x: x[1])

                _DEADLOCK_RETRIES = 3
                for _attempt in range(_DEADLOCK_RETRIES + 1):
                    try:
                        with db.transaction() as (_, cursor):
                            if rollup_batch:
                                cursor.executemany(
                                    """
                                    INSERT INTO battle_rollups (
                                        battle_id, system_id, started_at, ended_at, duration_seconds,
                                        participant_count, eligible_for_suspicion, battle_size_class, computed_at
                                    ) VALUES (%s, %s, %s, %s, %s, 0, 0, 'small', %s)
                                    ON DUPLICATE KEY UPDATE
                                        system_id = VALUES(system_id),
                                        started_at = LEAST(started_at, VALUES(started_at)),
                                        ended_at = GREATEST(ended_at, VALUES(ended_at)),
                                        duration_seconds = TIMESTAMPDIFF(SECOND, LEAST(started_at, VALUES(started_at)), GREATEST(ended_at, VALUES(ended_at))),
                                        computed_at = IF(
                                            system_id <> VALUES(system_id)
                                            OR started_at <> LEAST(started_at, VALUES(started_at))
                                            OR ended_at <> GREATEST(ended_at, VALUES(ended_at)),
                                            VALUES(computed_at),
                                            computed_at
                                        )
                                    """,
                                    rollup_batch,
                                )
                                batch_written += len(rollup_batch)

                            if participant_batch:
                                cursor.executemany(
                                    """
                                    INSERT INTO battle_participants (
                                        battle_id, character_id, corporation_id, alliance_id, side_key, ship_type_id,
                                        is_logi, is_command, is_capital, participation_count, computed_at
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                        corporation_id = COALESCE(VALUES(corporation_id), corporation_id),
                                        alliance_id = COALESCE(VALUES(alliance_id), alliance_id),
                                        side_key = IF(side_key = 'unknown' AND VALUES(side_key) <> 'unknown', VALUES(side_key), side_key),
                                        ship_type_id = COALESCE(VALUES(ship_type_id), ship_type_id),
                                        is_logi = GREATEST(is_logi, VALUES(is_logi)),
                                        is_command = GREATEST(is_command, VALUES(is_command)),
                                        is_capital = GREATEST(is_capital, VALUES(is_capital)),
                                        participation_count = participation_count + VALUES(participation_count),
                                        computed_at = VALUES(computed_at)
                                    """,
                                    participant_batch,
                                )
                                batch_written += len(participant_batch)

                            if sorted_killmail_assignments:
                                cursor.executemany(
                                    "UPDATE killmail_events SET battle_id = %s WHERE sequence_id = %s AND COALESCE(battle_id, '') <> %s",
                                    [(bid, sid, bid) for bid, sid in sorted_killmail_assignments],
                                )
                                batch_written += len(sorted_killmail_assignments)

                            if touched_battles:
                                tb_list = sorted(touched_battles)
                                tb_placeholders = ",".join(["%s"] * len(tb_list))
                                # Compute participant counts for all touched battles in one query.
                                cursor.execute(
                                    f"""
                                    SELECT battle_id, COUNT(*) AS participant_count
                                    FROM battle_participants
                                    WHERE battle_id IN ({tb_placeholders})
                                    GROUP BY battle_id
                                    """,
                                    tuple(tb_list),
                                )
                                counts = {str(r["battle_id"]): int(r["participant_count"]) for r in cursor.fetchall()}
                                update_batch = []
                                for battle_id_value in tb_list:
                                    participant_count = counts.get(str(battle_id_value), 0)
                                    update_batch.append(
                                        (
                                            participant_count,
                                            1 if participant_count >= MIN_ELIGIBLE_PARTICIPANTS else 0,
                                            _battle_size_class(participant_count),
                                            computed_at,
                                            battle_id_value,
                                            participant_count,
                                            1 if participant_count >= MIN_ELIGIBLE_PARTICIPANTS else 0,
                                            _battle_size_class(participant_count),
                                            computed_at,
                                        ),
                                    )
                                # Sort by battle_id (index 4) for consistent lock ordering.
                                update_batch.sort(key=lambda r: r[4])
                                cursor.executemany(
                                    """
                                    UPDATE battle_rollups
                                    SET participant_count = %s,
                                        eligible_for_suspicion = %s,
                                        battle_size_class = %s,
                                        computed_at = %s
                                    WHERE battle_id = %s
                                      AND (
                                        participant_count <> %s
                                        OR eligible_for_suspicion <> %s
                                        OR battle_size_class <> %s
                                        OR computed_at <> %s
                                      )
                                    """,
                                    update_batch,
                                )
                                batch_written += len(update_batch)
                        break  # transaction committed successfully
                    except Exception as _exc:
                        if (
                            _attempt < _DEADLOCK_RETRIES
                            and isinstance(_exc, pymysql.err.OperationalError)
                            and _exc.args[0] == 1213
                        ):
                            batch_written = 0
                            time.sleep(0.2 * (2 ** _attempt))
                            continue
                        raise

            cursor_end = max_sequence_id
            rows_written += batch_written
            _sync_state_cursor_upsert(
                db,
                SYNC_STATE_KEY_BATTLE_ROLLUPS_CURSOR,
                status="success",
                last_cursor=cursor_end,
                last_row_count=len(killmails),
            )
            # Horizon watermark update runs after the cursor upsert so the
            # freshness report reflects the newest source event time we
            # actually rolled up this batch.  compute_battle_rollups uses a
            # plain integer sequence_id cursor, so we pass the stringified
            # cursor for equality-only stall detection.
            update_watermark_and_stall(
                db,
                SYNC_STATE_KEY_BATTLE_ROLLUPS_CURSOR,
                new_cursor=str(cursor_end),
                event_time=watermark_event_time,
            )
            _battle_log(
                runtime,
                "battle_intelligence.job.batch",
                {
                    "job_name": "compute_battle_rollups",
                    "batch_index": batch_count,
                    "cursor_start": cursor_start if batch_count == 1 else None,
                    "cursor_end": cursor_end,
                    "source_rows": len(killmails),
                    "battles_touched": len(touched_battles),
                    "participants_touched": len(participant_rows),
                    "rows_written": batch_written,
                    "duration_ms": int((datetime.now(UTC) - batch_started).total_seconds() * 1000),
                    "dry_run": dry_run,
                },
            )

        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "batch_count": batch_count, "cursor_start": cursor_start, "cursor_end": cursor_end},
        )
        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="compute_battle_rollups",
            summary=f"Rolled up {rows_processed} killmails into battles across {batch_count} batches.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            batches_completed=batch_count,
            meta={
                "computed_at": computed_at,
                "rows_would_write": rows_written,
                "cursor_start": cursor_start,
                "cursor_end": cursor_end,
                "dry_run": dry_run,
            },
        ).to_dict()
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        _sync_state_cursor_upsert(db, SYNC_STATE_KEY_BATTLE_ROLLUPS_CURSOR, status="failed", last_cursor=cursor_end, last_row_count=0, error_text=str(exc))
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(
            runtime,
            "battle_intelligence.job.failed",
            {"job_name": "compute_battle_rollups", "status": "failed", "rows_processed": rows_processed, "rows_written": rows_written, "error_text": str(exc), "dry_run": dry_run},
        )
        raise


def run_compute_battle_target_metrics(db: SupplyCoreDb, runtime: dict[str, Any] | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    job = start_job_run(db, "compute_battle_target_metrics")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    unscored_targets = 0
    raw_cursor = _sync_state_cursor_get(db, SYNC_STATE_KEY_BATTLE_TARGET_CURSOR)
    raw_cursor = _validate_killmail_cursor(db, raw_cursor)
    horizon_state = horizon_state_get(db, SYNC_STATE_KEY_BATTLE_TARGET_CURSOR)
    cursor_start, horizon_meta = _horizon_apply_repair_window(db, horizon_state, raw_cursor)
    cursor_end = cursor_start
    watermark_event_time: str | None = None
    batch_count = 0
    _sync_state_cursor_upsert(db, SYNC_STATE_KEY_BATTLE_TARGET_CURSOR, status="running", last_cursor=cursor_start, last_row_count=0)
    _battle_log(
        runtime,
        "battle_intelligence.job.started",
        {
            "job_name": "compute_battle_target_metrics",
            "dry_run": dry_run,
            "computed_at": computed_at,
            "horizon": horizon_meta,
        },
    )
    try:
        while True:
            batch_started = datetime.now(UTC)
            rows = db.fetch_all(
                """
                SELECT
                    ke.sequence_id,
                    br.battle_id,
                    br.started_at,
                    ke.killmail_id,
                    ke.effective_killmail_at AS killmail_time,
                    ke.victim_character_id,
                    ke.victim_ship_type_id,
                    ke.victim_alliance_id,
                    ke.victim_corporation_id,
                    JSON_UNQUOTE(JSON_EXTRACT(kep.raw_killmail_json, '$.victim.damage_taken')) AS victim_damage_taken
                FROM killmail_events ke
                INNER JOIN battle_rollups br ON br.battle_id = ke.battle_id
                LEFT JOIN killmail_event_payloads kep ON kep.sequence_id = ke.sequence_id
                WHERE ke.sequence_id > %s
                  AND br.eligible_for_suspicion = 1
                ORDER BY ke.sequence_id ASC
                LIMIT %s
                """,
                (cursor_end, TARGET_METRICS_BATCH_SIZE),
            )
            if not rows:
                break

            batch_count += 1
            rows_processed += len(rows)
            prepared: list[dict[str, Any]] = []
            baseline: dict[tuple[int, str], list[float]] = defaultdict(list)
            max_sequence_id = cursor_end
            for row in rows:
                sequence_id = int(row.get("sequence_id") or 0)
                if sequence_id > 0:
                    max_sequence_id = max(max_sequence_id, sequence_id)
                damage_value = _maybe_float(row.get("victim_damage_taken"))
                damage = max(0.0, float(damage_value or 0.0))
                started_at = datetime.strptime(str(row.get("started_at")), "%Y-%m-%d %H:%M:%S")
                killmail_time = datetime.strptime(str(row.get("killmail_time")), "%Y-%m-%d %H:%M:%S")
                # Track the newest source event time for the horizon
                # watermark (used by the freshness report + SLA checks).
                killmail_time_str = killmail_time.strftime("%Y-%m-%d %H:%M:%S")
                if watermark_event_time is None or killmail_time_str > watermark_event_time:
                    watermark_event_time = killmail_time_str
                ttd = max(1.0, (killmail_time - started_at).total_seconds())
                dps = _safe_div(damage, ttd, 0.0)
                ship_type_id = int(row.get("victim_ship_type_id") or 0)
                bucket = _dps_bucket(dps)
                if ship_type_id <= 0 or damage <= 0:
                    unscored_targets += 1
                    continue
                baseline[(ship_type_id, bucket)].append(ttd)
                prepared.append(
                    {
                        "battle_id": str(row.get("battle_id")),
                        "killmail_id": int(row.get("killmail_id") or 0),
                        "victim_character_id": int(row.get("victim_character_id") or 0) or None,
                        "victim_ship_type_id": ship_type_id,
                        "side_key": f"a:{int(row.get('victim_alliance_id') or 0)}"
                        if int(row.get("victim_alliance_id") or 0) > 0
                        else (f"c:{int(row.get('victim_corporation_id') or 0)}" if int(row.get("victim_corporation_id") or 0) > 0 else "unknown"),
                        "first_damage_ts": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "last_damage_ts": killmail_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "time_to_die_seconds": ttd,
                        "total_damage_taken": damage,
                        "estimated_incoming_dps": dps,
                        "dps_bucket": bucket,
                    }
                )

            expected_ttd: dict[tuple[int, str], float] = {}
            for key, values in baseline.items():
                values_sorted = sorted(values)
                expected_ttd[key] = values_sorted[len(values_sorted) // 2]

            batch_written = 0
            if not dry_run:
                _TARGET_BATCH_SIZE = 200
                for tgt_start in range(0, len(prepared), _TARGET_BATCH_SIZE):
                    tgt_batch = prepared[tgt_start:tgt_start + _TARGET_BATCH_SIZE]
                    with db.transaction() as (_, cursor):
                        for target in tgt_batch:
                            baseline_ttd = max(1.0, expected_ttd.get((int(target["victim_ship_type_id"]), str(target["dps_bucket"])), float(target["time_to_die_seconds"])))
                            sustain_factor = max(0.05, min(8.0, _safe_div(float(target["time_to_die_seconds"]), baseline_ttd, 1.0)))
                            cursor.execute(
                                """
                                INSERT INTO battle_target_metrics (
                                    battle_id, killmail_id, victim_character_id, victim_ship_type_id, side_key,
                                    first_damage_ts, last_damage_ts, time_to_die_seconds, total_damage_taken,
                                    estimated_incoming_dps, dps_bucket, expected_time_to_die_seconds, sustain_factor, computed_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    victim_character_id = VALUES(victim_character_id),
                                    victim_ship_type_id = VALUES(victim_ship_type_id),
                                    side_key = VALUES(side_key),
                                    first_damage_ts = VALUES(first_damage_ts),
                                    last_damage_ts = VALUES(last_damage_ts),
                                    time_to_die_seconds = VALUES(time_to_die_seconds),
                                    total_damage_taken = VALUES(total_damage_taken),
                                    estimated_incoming_dps = VALUES(estimated_incoming_dps),
                                    dps_bucket = VALUES(dps_bucket),
                                    expected_time_to_die_seconds = VALUES(expected_time_to_die_seconds),
                                    sustain_factor = VALUES(sustain_factor),
                                    computed_at = VALUES(computed_at)
                                """,
                                (
                                    str(target["battle_id"]),
                                    int(target["killmail_id"]),
                                    target["victim_character_id"],
                                    int(target["victim_ship_type_id"]),
                                    str(target["side_key"]),
                                    str(target["first_damage_ts"]),
                                    str(target["last_damage_ts"]),
                                    float(target["time_to_die_seconds"]),
                                    float(target["total_damage_taken"]),
                                    float(target["estimated_incoming_dps"]),
                                    str(target["dps_bucket"]),
                                    float(baseline_ttd),
                                    float(sustain_factor),
                                    computed_at,
                                ),
                            )
                            batch_written += max(0, int(cursor.rowcount or 0))
            cursor_end = max_sequence_id
            rows_written += batch_written
            _sync_state_cursor_upsert(
                db,
                SYNC_STATE_KEY_BATTLE_TARGET_CURSOR,
                status="success",
                last_cursor=cursor_end,
                last_row_count=len(rows),
            )
            # Horizon watermark update runs after the cursor upsert so
            # the freshness report reflects the newest source event
            # time actually scored by this batch.  This job uses the
            # same killmail sequence_id cursor as compute_battle_rollups
            # so we pass the stringified cursor for equality-only stall
            # detection.
            update_watermark_and_stall(
                db,
                SYNC_STATE_KEY_BATTLE_TARGET_CURSOR,
                new_cursor=str(cursor_end),
                event_time=watermark_event_time,
            )
            _battle_log(
                runtime,
                "battle_intelligence.job.batch",
                {
                    "job_name": "compute_battle_target_metrics",
                    "batch_index": batch_count,
                    "cursor_start": cursor_start if batch_count == 1 else None,
                    "cursor_end": cursor_end,
                    "source_rows": len(rows),
                    "prepared_rows": len(prepared),
                    "unscored_targets": unscored_targets,
                    "rows_written": batch_written,
                    "duration_ms": int((datetime.now(UTC) - batch_started).total_seconds() * 1000),
                    "dry_run": dry_run,
                },
            )

        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "unscored_targets": unscored_targets, "batch_count": batch_count, "cursor_start": cursor_start, "cursor_end": cursor_end},
        )
        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="compute_battle_target_metrics",
            summary=f"Scored {rows_processed} target metrics across {batch_count} batches ({unscored_targets} unscored).",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            batches_completed=batch_count,
            meta={
                "computed_at": computed_at,
                "rows_would_write": rows_written,
                "unscored_targets": unscored_targets,
                "cursor_start": cursor_start,
                "cursor_end": cursor_end,
                "dry_run": dry_run,
            },
        ).to_dict()
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        _sync_state_cursor_upsert(db, SYNC_STATE_KEY_BATTLE_TARGET_CURSOR, status="failed", last_cursor=cursor_end, last_row_count=0, error_text=str(exc))
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_battle_target_metrics", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise


def run_compute_battle_anomalies(db: SupplyCoreDb, runtime: dict[str, Any] | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    # Serialize concurrent runs via a compute_job_locks lease.
    #
    # The write phase below wipes `battle_anomalies` and `battle_side_metrics`
    # in a single transaction before bulk-inserting the recomputed rows.  If
    # two instances of this job run at the same time (e.g. a recurring
    # worker-pool dispatch overlapping with a manual CLI run, or a zombie
    # worker whose DB connection has not yet released its locks), their
    # DELETE statements collide on row locks and fail with
    # `(1205, 'Lock wait timeout exceeded; try restarting transaction')` even
    # though `run_in_transaction` already retries 3 times — see issue #967.
    #
    # `acquire_job_lock` uses the shared `compute_job_locks` table and
    # transparently reclaims leases whose TTL has expired, so a crashed
    # worker will not block future runs for longer than the TTL.  The TTL
    # here is a little over 2× the worker-pool `timeout_seconds` (420s) to
    # cover the occasional slow run without permanently jamming the job.
    _ANOMALIES_LOCK_KEY = "compute_battle_anomalies"
    _ANOMALIES_LOCK_TTL_SECONDS = 900
    lock_owner = acquire_job_lock(db, _ANOMALIES_LOCK_KEY, ttl_seconds=_ANOMALIES_LOCK_TTL_SECONDS)
    if lock_owner is None:
        _battle_log(
            runtime,
            "battle_intelligence.job.skipped",
            {
                "job_name": "compute_battle_anomalies",
                "reason": "another instance already holds the compute_job_locks lease",
                "lock_key": _ANOMALIES_LOCK_KEY,
                "dry_run": dry_run,
            },
        )
        return JobResult.skipped(
            job_key="compute_battle_anomalies",
            reason="Another run already holds the compute_job_locks lease for compute_battle_anomalies.",
            meta={"lock_key": _ANOMALIES_LOCK_KEY},
        ).to_dict()

    job = start_job_run(db, "compute_battle_anomalies")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _battle_log(runtime, "battle_intelligence.job.started", {"job_name": "compute_battle_anomalies", "dry_run": dry_run, "computed_at": computed_at})
    try:
        side_rows = db.fetch_all(
            """
            SELECT
                bp.battle_id,
                bp.side_key,
                br.duration_seconds,
                SUM(bp.participation_count) AS participant_count,
                SUM(CASE WHEN bp.is_logi = 1 THEN 1 ELSE 0 END) AS logi_count,
                SUM(CASE WHEN bp.is_command = 1 THEN 1 ELSE 0 END) AS command_count,
                SUM(CASE WHEN bp.is_capital = 1 THEN 1 ELSE 0 END) AS capital_count,
                COUNT(DISTINCT btm.killmail_id) AS total_kills,
                AVG(btm.sustain_factor) AS avg_sustain_factor,
                AVG(btm.sustain_factor) AS med_sustain_factor,
                STDDEV_POP(btm.sustain_factor) AS switch_pressure,
                br.battle_size_class
            FROM battle_participants bp
            INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
            LEFT JOIN battle_target_metrics btm ON btm.battle_id = bp.battle_id AND btm.side_key = bp.side_key
            WHERE br.eligible_for_suspicion = 1
            GROUP BY bp.battle_id, bp.side_key, br.duration_seconds, br.battle_size_class
            """
        )
        rows_processed = len(side_rows)

        per_size: dict[str, list[float]] = defaultdict(list)
        shaped: list[dict[str, Any]] = []
        for row in side_rows:
            duration_minutes = max(1.0, float(row.get("duration_seconds") or 0) / 60.0)
            kill_rate = _safe_div(float(row.get("total_kills") or 0), duration_minutes, 0.0)
            median_sustain = float(row.get("med_sustain_factor") or 0.0)
            average_sustain = float(row.get("avg_sustain_factor") or 0.0)
            efficiency = _safe_div(median_sustain, math.log(1.0 + kill_rate + EPSILON), 0.0)
            shape = {
                "battle_id": str(row.get("battle_id")),
                "side_key": str(row.get("side_key")),
                "participant_count": int(row.get("participant_count") or 0),
                "logi_count": int(row.get("logi_count") or 0),
                "command_count": int(row.get("command_count") or 0),
                "capital_count": int(row.get("capital_count") or 0),
                "total_kills": int(row.get("total_kills") or 0),
                "kill_rate_per_minute": kill_rate,
                "median_sustain_factor": median_sustain,
                "average_sustain_factor": average_sustain,
                "switch_pressure": float(row.get("switch_pressure") or 0.0),
                "efficiency_score": efficiency,
                "battle_size_class": str(row.get("battle_size_class") or "small"),
            }
            per_size[shape["battle_size_class"]].append(efficiency)
            shaped.append(shape)

        stats: dict[str, tuple[float, float]] = {}
        for size_class, values in per_size.items():
            mean = sum(values) / max(1, len(values))
            stddev = _stddev(values)
            stats[size_class] = (mean, stddev)

        if not dry_run:
            # Prepare batches outside the transaction to minimise lock hold time.
            efficiency_sorted = sorted((float(item["efficiency_score"]) for item in shaped))
            metrics_batch: list[tuple[Any, ...]] = []
            anomalies_batch: list[tuple[Any, ...]] = []
            for item in shaped:
                mean, stddev = stats.get(str(item["battle_size_class"]), (0.0, 0.0))
                z = _safe_div(float(item["efficiency_score"]) - mean, stddev, 0.0) if stddev > 0 else 0.0
                percentile_rank = _percentile(efficiency_sorted, float(item["efficiency_score"]))
                anomaly_class = "normal"
                if z > 2.0:
                    anomaly_class = "high_sustain"
                elif z < -1.0:
                    anomaly_class = "low_sustain"

                metrics_batch.append(
                    (
                        str(item["battle_id"]),
                        str(item["side_key"]),
                        int(item["participant_count"]),
                        int(item["logi_count"]),
                        int(item["command_count"]),
                        int(item["capital_count"]),
                        int(item["total_kills"]),
                        float(item["kill_rate_per_minute"]),
                        float(item["median_sustain_factor"]),
                        float(item["average_sustain_factor"]),
                        float(item["switch_pressure"]),
                        float(item["efficiency_score"]),
                        float(z),
                        computed_at,
                    ),
                )
                explanation = {
                    "efficiency_formula": "median_sustain_factor / log(1 + kill_rate_per_minute + epsilon)",
                    "median_sustain_factor": item["median_sustain_factor"],
                    "kill_rate_per_minute": item["kill_rate_per_minute"],
                    "efficiency_score": item["efficiency_score"],
                    "z_efficiency_score": z,
                    "peer_group": item["battle_size_class"],
                }
                anomalies_batch.append(
                    (
                        str(item["battle_id"]),
                        str(item["side_key"]),
                        anomaly_class,
                        float(z),
                        float(percentile_rank),
                        json_dumps_safe(explanation),
                        computed_at,
                    ),
                )

            # Sort by primary key for consistent lock ordering across concurrent jobs.
            metrics_batch.sort(key=lambda r: (r[0], r[1]))
            anomalies_batch.sort(key=lambda r: (r[0], r[1]))

            _BATCH_CHUNK = 500

            def _write_anomalies(_conn, cursor):
                cursor.execute("DELETE FROM battle_anomalies")
                cursor.execute("DELETE FROM battle_side_metrics")
                for offset in range(0, len(metrics_batch), _BATCH_CHUNK):
                    cursor.executemany(
                        """
                        INSERT INTO battle_side_metrics (
                            battle_id, side_key, participant_count, logi_count, command_count, capital_count,
                            total_kills, kill_rate_per_minute, median_sustain_factor, average_sustain_factor,
                            switch_pressure, efficiency_score, z_efficiency_score, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        metrics_batch[offset:offset + _BATCH_CHUNK],
                    )
                for offset in range(0, len(anomalies_batch), _BATCH_CHUNK):
                    cursor.executemany(
                        """
                        INSERT INTO battle_anomalies (
                            battle_id, side_key, anomaly_class, z_efficiency_score, percentile_rank, explanation_json, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        anomalies_batch[offset:offset + _BATCH_CHUNK],
                    )

            # run_in_transaction auto-retries on deadlock (1213) and lock-wait timeout (1205).
            db.run_in_transaction(_write_anomalies)
        rows_written = len(shaped) * 2

        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "anomaly_rows": len(shaped)},
        )
        high_count = 0
        for item in shaped:
            mean, stddev = stats.get(str(item["battle_size_class"]), (0.0, 0.0))
            if stddev <= 0:
                continue
            z_value = _safe_div(float(item["efficiency_score"]) - mean, stddev, 0.0)
            if z_value > 2.0:
                high_count += 1
        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="compute_battle_anomalies",
            summary=f"Detected {len(shaped)} anomalies ({high_count} high-sustain) from {rows_processed} rows.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "rows_would_write": rows_written if dry_run else rows_written,
                "anomaly_count": len(shaped),
                "high_sustain_count": high_count,
                "dry_run": dry_run,
            },
        ).to_dict()
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_battle_anomalies", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise
    finally:
        # Always release the compute_job_locks lease, even if the work raised.
        # The helper is idempotent — if the row has already been reaped by
        # another process (TTL expired) it will simply be a no-op DELETE.
        try:
            release_job_lock(db, _ANOMALIES_LOCK_KEY, lock_owner)
        except Exception as release_exc:  # pragma: no cover — best-effort cleanup
            _battle_log(
                runtime,
                "battle_intelligence.job.lock_release_failed",
                {
                    "job_name": "compute_battle_anomalies",
                    "lock_key": _ANOMALIES_LOCK_KEY,
                    "error_text": str(release_exc),
                },
            )


def _queue_enrichment(db: SupplyCoreDb, actor_rows: list[dict[str, Any]]) -> None:
    """Queue battle participants for EveWho/ESI enrichment.

    Inserts unique character_ids into enrichment_queue so that
    evewho_enrichment_sync can populate their corp history into Neo4j.
    Priority is based on visibility_score — higher visibility characters
    (capitals, logi, command) are enriched first.
    """
    # Deduplicate and pick highest visibility per character
    char_priority: dict[int, float] = {}
    for row in actor_rows:
        cid = int(row.get("character_id") or 0)
        if cid <= 0:
            continue
        vis = float(row.get("visibility_score") or row.get("participation_count") or 1)
        if cid not in char_priority or vis > char_priority[cid]:
            char_priority[cid] = vis

    if not char_priority:
        return

    BATCH = 500
    items = list(char_priority.items())
    for offset in range(0, len(items), BATCH):
        chunk = items[offset:offset + BATCH]
        placeholders = ",".join(["(%s, 'pending', %s, UTC_TIMESTAMP())"] * len(chunk))
        params: list[int | float] = []
        for cid, priority in chunk:
            params.extend([cid, round(priority, 4)])
        db.execute(
            f"""
            INSERT INTO enrichment_queue (character_id, status, priority, queued_at)
            VALUES {placeholders}
            ON DUPLICATE KEY UPDATE
                priority = GREATEST(priority, VALUES(priority))
            """,
            tuple(params),
        )


def _queue_enrichment_from_priorities(db: SupplyCoreDb, char_priority: dict[int, float]) -> None:
    """Queue enrichment from a pre-built character_id -> visibility mapping."""
    if not char_priority:
        return

    BATCH = 500
    items = list(char_priority.items())
    for offset in range(0, len(items), BATCH):
        chunk = items[offset:offset + BATCH]
        placeholders = ",".join(["(%s, 'pending', %s, UTC_TIMESTAMP())"] * len(chunk))
        params: list[int | float] = []
        for cid, priority in chunk:
            params.extend([cid, round(priority, 4)])
        db.execute(
            f"""
            INSERT INTO enrichment_queue (character_id, status, priority, queued_at)
            VALUES {placeholders}
            ON DUPLICATE KEY UPDATE
                priority = GREATEST(priority, VALUES(priority))
            """,
            tuple(params),
        )


def run_compute_battle_actor_features(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    job = start_job_run(db, "compute_battle_actor_features")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _battle_log(runtime, "battle_intelligence.job.started", {"job_name": "compute_battle_actor_features", "dry_run": dry_run, "computed_at": computed_at})
    try:
        # Track character priorities for enrichment (character_id -> max visibility).
        enrichment_priorities: dict[int, float] = {}

        if not dry_run:
            # Single INSERT...SELECT with window functions computes centrality
            # and visibility server-side, eliminating the max_participation
            # pre-load and per-row Python computation.
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM battle_actor_features")
                rows_written = cursor.execute(
                    """
                    INSERT INTO battle_actor_features (
                        battle_id, character_id, side_key, participation_count,
                        centrality_score, visibility_score,
                        is_logi, is_command, is_capital,
                        participated_in_high_sustain, participated_in_low_sustain,
                        computed_at
                    )
                    SELECT
                        bp.battle_id,
                        bp.character_id,
                        bp.side_key,
                        bp.participation_count,
                        bp.participation_count / GREATEST(
                            MAX(bp.participation_count) OVER (PARTITION BY bp.battle_id), 1
                        ) AS centrality_score,
                        LEAST(1.0,
                            bp.participation_count / GREATEST(
                                MAX(bp.participation_count) OVER (PARTITION BY bp.battle_id), 1
                            )
                            + 0.2 * COALESCE(bp.is_logi, 0)
                            + 0.2 * COALESCE(bp.is_command, 0)
                            + 0.3 * COALESCE(bp.is_capital, 0)
                        ) AS visibility_score,
                        COALESCE(bp.is_logi, 0),
                        COALESCE(bp.is_command, 0),
                        COALESCE(bp.is_capital, 0),
                        CASE WHEN COALESCE(ba.anomaly_class, 'normal') = 'high_sustain' THEN 1 ELSE 0 END,
                        CASE WHEN COALESCE(ba.anomaly_class, 'normal') = 'low_sustain' THEN 1 ELSE 0 END,
                        %s
                    FROM battle_participants bp
                    INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
                    LEFT JOIN battle_anomalies ba
                        ON ba.battle_id = bp.battle_id AND ba.side_key = bp.side_key
                    WHERE br.eligible_for_suspicion = 1
                    """,
                    (computed_at,),
                )
            rows_processed = rows_written

            # Derive enrichment priorities from the just-written rows instead
            # of tracking per-row in Python.
            for batch in db.iterate_batches(
                "SELECT character_id, MAX(visibility_score) AS max_vis "
                "FROM battle_actor_features WHERE character_id > 0 "
                "GROUP BY character_id",
                batch_size=5000,
            ):
                for row in batch:
                    enrichment_priorities[int(row["character_id"])] = float(row["max_vis"])
        else:
            rows_processed = db.fetch_scalar(
                "SELECT COUNT(*) FROM battle_participants bp "
                "INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id "
                "WHERE br.eligible_for_suspicion = 1"
            ) or 0

        # Queue all battle participants for EveWho/ESI enrichment so that
        # cross-alliance history is populated before users view theater pages.
        if not dry_run and enrichment_priorities:
            _queue_enrichment_from_priorities(db, enrichment_priorities)

            # Also queue for full pipeline processing (histograms, counterintel, temporal)
            from .character_pipeline_worker import enqueue_characters as _enqueue_pipeline
            _enqueue_pipeline(db, list(enrichment_priorities.keys()), reason="battle_participation")

        neo_result = {"status": "skipped", "reason": "dry-run"} if dry_run else _neo4j_sync_participation(db, neo4j_raw)
        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "neo4j": neo_result},
        )
        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="compute_battle_actor_features",
            summary=f"Extracted actor features for {rows_processed} participants, wrote {rows_written} rows.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "rows_would_write": rows_written if dry_run else rows_written,
                "neo4j": neo_result,
                "dry_run": dry_run,
            },
        ).to_dict()
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_battle_actor_features", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise


def run_compute_suspicion_scores(db: SupplyCoreDb, runtime: dict[str, Any] | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    # `runtime` is intentionally optional so this processor can run identically from:
    # - worker pool context
    # - scheduler-dispatched Python runtime
    # - manual Python CLI invocation
    # Missing runtime values only disable optional file logging via `_battle_log`.
    job = start_job_run(db, "compute_suspicion_scores")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _battle_log(runtime, "battle_intelligence.job.started", {"job_name": "compute_suspicion_scores", "dry_run": dry_run, "computed_at": computed_at})
    try:
        _SUSPICION_SQL = """
            SELECT
                baf.character_id,
                baf.battle_id,
                baf.side_key,
                baf.is_logi,
                baf.is_command,
                baf.is_capital,
                baf.participated_in_high_sustain,
                baf.participated_in_low_sustain,
                bsm.z_efficiency_score,
                bsm.efficiency_score,
                br.eligible_for_suspicion
            FROM battle_actor_features baf
            INNER JOIN battle_rollups br ON br.battle_id = baf.battle_id
            LEFT JOIN battle_side_metrics bsm ON bsm.battle_id = baf.battle_id AND bsm.side_key = baf.side_key
            WHERE baf.character_id > 0
        """

        # Stream rows into indexed dicts without keeping a separate raw list.
        by_character: dict[int, list[dict[str, Any]]] = defaultdict(list)
        battle_side_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
        global_eligible_z_sum = 0.0
        global_eligible_count = 0

        for batch in db.iterate_batches(_SUSPICION_SQL, batch_size=5000):
            rows_processed += len(batch)
            for row in batch:
                character_id = int(row.get("character_id") or 0)
                if character_id <= 0:
                    continue
                by_character[character_id].append(row)
                battle_side_index[str(row.get("battle_id"))].append(row)
                if int(row.get("eligible_for_suspicion") or 0) == 1:
                    global_eligible_z_sum += float(row.get("z_efficiency_score") or 0.0)
                    global_eligible_count += 1

        intelligence_rows: list[dict[str, Any]] = []
        score_rows: list[dict[str, Any]] = []

        for character_id, rows in by_character.items():
            total_battle_count = len({str(row.get("battle_id")) for row in rows})
            eligible_rows = [row for row in rows if int(row.get("eligible_for_suspicion") or 0) == 1]
            eligible_battle_ids = {str(row.get("battle_id")) for row in eligible_rows}
            eligible_battle_count = len(eligible_battle_ids)
            high_sustain_battle_count = len({str(row.get("battle_id")) for row in eligible_rows if int(row.get("participated_in_high_sustain") or 0) == 1})
            low_sustain_battle_count = len({str(row.get("battle_id")) for row in eligible_rows if int(row.get("participated_in_low_sustain") or 0) == 1})
            high_sustain_frequency = _safe_div(float(high_sustain_battle_count), float(max(eligible_battle_count, 1)), 0.0)
            low_sustain_frequency = _safe_div(float(low_sustain_battle_count), float(max(eligible_battle_count, 1)), 0.0)
            side_count = len({str(row.get("side_key")) for row in eligible_rows})
            cross_side_battle_count = max(0, side_count - 1)
            cross_side_rate = _safe_div(float(cross_side_battle_count), float(max(eligible_battle_count, 1)), 0.0)
            role_weight = min(
                1.0,
                _safe_div(
                    float(
                        sum(int(row.get("is_logi") or 0) * 2 + int(row.get("is_command") or 0) * 2 + int(row.get("is_capital") or 0) * 3 for row in eligible_rows)
                    ),
                    float(max(len(eligible_rows) * 3, 1)),
                    0.0,
                ),
            )
            anomalous_battle_density = _safe_div(
                float(high_sustain_battle_count + low_sustain_battle_count),
                float(max(eligible_battle_count, 1)),
                0.0,
            )

            co_occurrence_density = _safe_div(sum(float(row.get("co_occurrence_density") or 0.0) for row in rows), float(max(len(rows), 1)), 0.0)
            anomalous_co_occurrence_density = _safe_div(sum(float(row.get("anomalous_co_occurrence_density") or 0.0) for row in rows), float(max(len(rows), 1)), 0.0)
            cross_side_cluster_score = _safe_div(sum(float(row.get("cross_side_cluster_score") or 0.0) for row in rows), float(max(len(rows), 1)), 0.0)
            neighbor_anomaly_score = _safe_div(sum(float(row.get("neighbor_anomaly_score") or 0.0) for row in rows), float(max(len(rows), 1)), 0.0)
            recurrence_centrality = _safe_div(sum(float(row.get("recurrence_centrality") or 0.0) for row in rows), float(max(len(rows), 1)), 0.0)
            bridge_score = _safe_div(sum(float(row.get("bridge_score") or 0.0) for row in rows), float(max(len(rows), 1)), 0.0)

            present_enemy_z: list[float] = []
            for row in eligible_rows:
                battle_id = str(row.get("battle_id"))
                side_key = str(row.get("side_key"))
                for other in battle_side_index.get(battle_id, []):
                    if str(other.get("side_key")) == side_key:
                        continue
                    present_enemy_z.append(float(other.get("z_efficiency_score") or 0.0))
            present_sum = sum(present_enemy_z)
            present_count = len(present_enemy_z)
            present_avg = _safe_div(present_sum, float(present_count), 0.0)
            # Derive absent stats from pre-computed global totals instead of
            # iterating all other characters (eliminates O(n²) inner loop).
            absent_count = global_eligible_count - present_count
            absent_sum = global_eligible_z_sum - present_sum
            absent_avg = _safe_div(absent_sum, float(absent_count), 0.0)
            enemy_eff_uplift = present_avg - absent_avg
            ally_eff_uplift = _safe_div(sum(float(row.get("z_efficiency_score") or 0.0) for row in eligible_rows), float(max(len(eligible_rows), 1)), 0.0)

            intelligence_rows.append(
                {
                    "character_id": character_id,
                    "total_battle_count": total_battle_count,
                    "eligible_battle_count": eligible_battle_count,
                    "high_sustain_battle_count": high_sustain_battle_count,
                    "low_sustain_battle_count": low_sustain_battle_count,
                    "high_sustain_frequency": high_sustain_frequency,
                    "low_sustain_frequency": low_sustain_frequency,
                    "cross_side_battle_count": cross_side_battle_count,
                    "cross_side_rate": cross_side_rate,
                    "enemy_efficiency_uplift": enemy_eff_uplift,
                    "ally_efficiency_uplift": ally_eff_uplift,
                    "role_weight": role_weight,
                    "anomalous_battle_density": anomalous_battle_density,
                    "co_occurrence_density": co_occurrence_density,
                    "anomalous_co_occurrence_density": anomalous_co_occurrence_density,
                    "cross_side_cluster_score": cross_side_cluster_score,
                    "neighbor_anomaly_score": neighbor_anomaly_score,
                    "recurrence_centrality": recurrence_centrality,
                    "bridge_score": bridge_score,
                }
            )

            if eligible_battle_count < MIN_SAMPLE_COUNT:
                suspicion_score = 0.0
            else:
                suspicion_score = (
                    SUSPICION_WEIGHTS["high_sustain_frequency"] * high_sustain_frequency
                    + SUSPICION_WEIGHTS["cross_side_rate"] * cross_side_rate
                    + SUSPICION_WEIGHTS["enemy_efficiency_uplift"] * enemy_eff_uplift
                    + SUSPICION_WEIGHTS["high_minus_low"] * (high_sustain_frequency - low_sustain_frequency)
                    + SUSPICION_WEIGHTS["role_weight"] * role_weight
                    + SUSPICION_WEIGHTS["co_occurrence_density"] * min(1.0, co_occurrence_density / 10.0)
                    + SUSPICION_WEIGHTS["anomalous_co_occurrence_density"] * min(1.0, anomalous_co_occurrence_density / 10.0)
                    + SUSPICION_WEIGHTS["cross_side_cluster_score"] * min(1.0, cross_side_cluster_score)
                    + SUSPICION_WEIGHTS["neighbor_anomaly_score"] * min(1.0, max(0.0, neighbor_anomaly_score))
                    + SUSPICION_WEIGHTS["bridge_score"] * min(1.0, bridge_score / 10.0)
                )
            suspicion_score = max(0.0, min(1.0, suspicion_score))

            support_battles = sorted(
                [
                    {
                        "battle_id": str(row.get("battle_id")),
                        "side_key": str(row.get("side_key")),
                        "z_efficiency_score": float(row.get("z_efficiency_score") or 0.0),
                        "high_sustain": int(row.get("participated_in_high_sustain") or 0) == 1,
                    }
                    for row in eligible_rows
                ],
                key=lambda candidate: abs(float(candidate["z_efficiency_score"])),
                reverse=True,
            )[:5]

            explanation = {
                "formula": "weighted sum of battle + graph intelligence metrics",
                "weights": SUSPICION_WEIGHTS,
                "minimum_sample_count": MIN_SAMPLE_COUNT,
                "eligible_battle_count": eligible_battle_count,
                "high_sustain_frequency": high_sustain_frequency,
                "low_sustain_frequency": low_sustain_frequency,
                "cross_side_rate": cross_side_rate,
                "enemy_efficiency_uplift": enemy_eff_uplift,
                "role_weight": role_weight,
                "co_occurrence_density": co_occurrence_density,
                "anomalous_co_occurrence_density": anomalous_co_occurrence_density,
                "cross_side_cluster_score": cross_side_cluster_score,
                "neighbor_anomaly_score": neighbor_anomaly_score,
                "bridge_score": bridge_score,
            }
            score_rows.append(
                {
                    "character_id": character_id,
                    "suspicion_score": suspicion_score,
                    "high_sustain_frequency": high_sustain_frequency,
                    "low_sustain_frequency": low_sustain_frequency,
                    "cross_side_rate": cross_side_rate,
                    "enemy_efficiency_uplift": enemy_eff_uplift,
                    "role_weight": role_weight,
                    "supporting_battle_count": eligible_battle_count,
                    "top_supporting_battles_json": json_dumps_safe(support_battles),
                    "top_graph_neighbors_json": json_dumps_safe([]),
                    "explanation_json": json_dumps_safe(explanation),
                }
            )

        scores_sorted = sorted((float(item["suspicion_score"]) for item in score_rows))

        minimum_sample_filtered_count = sum(1 for row in intelligence_rows if int(row["eligible_battle_count"]) < MIN_SAMPLE_COUNT)

        if not dry_run:
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM character_battle_intelligence")
                cursor.execute("DELETE FROM character_suspicion_scores")

                if intelligence_rows:
                    cursor.executemany(
                        """
                        INSERT INTO character_battle_intelligence (
                            character_id, total_battle_count, eligible_battle_count, high_sustain_battle_count,
                            low_sustain_battle_count, high_sustain_frequency, low_sustain_frequency,
                            cross_side_battle_count, cross_side_rate, enemy_efficiency_uplift, ally_efficiency_uplift,
                            role_weight, anomalous_battle_density, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                int(row["character_id"]),
                                int(row["total_battle_count"]),
                                int(row["eligible_battle_count"]),
                                int(row["high_sustain_battle_count"]),
                                int(row["low_sustain_battle_count"]),
                                float(row["high_sustain_frequency"]),
                                float(row["low_sustain_frequency"]),
                                int(row["cross_side_battle_count"]),
                                float(row["cross_side_rate"]),
                                float(row["enemy_efficiency_uplift"]),
                                float(row["ally_efficiency_uplift"]),
                                float(row["role_weight"]),
                                float(row["anomalous_battle_density"]),
                                computed_at,
                            )
                            for row in intelligence_rows
                        ],
                    )

                # Pre-compute percentile ranks before insert.
                score_tuples = []
                for row in score_rows:
                    percentile_rank = _percentile(scores_sorted, float(row["suspicion_score"]))
                    score_tuples.append(
                        (
                            int(row["character_id"]),
                            float(row["suspicion_score"]),
                            float(percentile_rank),
                            float(row["high_sustain_frequency"]),
                            float(row["low_sustain_frequency"]),
                            float(row["cross_side_rate"]),
                            float(row["enemy_efficiency_uplift"]),
                            float(row["role_weight"]),
                            int(row["supporting_battle_count"]),
                            str(row["top_supporting_battles_json"]),
                            str(row["top_graph_neighbors_json"]),
                            str(row["explanation_json"]),
                            computed_at,
                        )
                    )
                if score_tuples:
                    cursor.executemany(
                        """
                        INSERT INTO character_suspicion_scores (
                            character_id, suspicion_score, percentile_rank, high_sustain_frequency,
                            low_sustain_frequency, cross_side_rate, enemy_efficiency_uplift, role_weight,
                            supporting_battle_count, top_supporting_battles_json, top_graph_neighbors_json,
                            explanation_json, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        score_tuples,
                    )
        rows_written = len(intelligence_rows) + len(score_rows)

        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "scored_characters": len(score_rows)},
        )
        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = JobResult.success(
            job_key="compute_suspicion_scores",
            summary=f"Scored {len(score_rows)} characters ({minimum_sample_filtered_count} filtered below sample threshold).",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "rows_would_write": rows_written if dry_run else rows_written,
                "scored_character_count": len(score_rows),
                "minimum_sample_filtered_count": minimum_sample_filtered_count,
                "dry_run": dry_run,
            },
        ).to_dict()
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_suspicion_scores", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise
