from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    package_root = str(Path(__file__).resolve().parents[2])
    if package_root not in sys.path:
        sys.path.insert(0, package_root)
    from orchestrator.db import SupplyCoreDb
    from orchestrator.job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
    from orchestrator.neo4j import Neo4jClient, Neo4jConfig
else:
    from ..db import SupplyCoreDb
    from ..job_utils import acquire_job_lock, finish_job_run, release_job_lock, start_job_run
    from ..neo4j import Neo4jClient, Neo4jConfig

WINDOW_SECONDS = 15 * 60
MIN_ELIGIBLE_PARTICIPANTS = 100
MIN_SAMPLE_COUNT = 5
EPSILON = 1e-6

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
    return "small"


def _percentile(sorted_values: list[float], value: float) -> float:
    if not sorted_values:
        return 0.0
    below = 0
    for candidate in sorted_values:
        if candidate <= value:
            below += 1
        else:
            break
    return _safe_div(float(below), float(len(sorted_values)), 0.0)


def _stddev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(max(0.0, variance))


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    if isinstance(value, Exception):
        return {"error_type": value.__class__.__name__, "error": str(value)}
    return str(value)


def _battle_log(runtime: dict[str, Any] | None, event: str, payload: dict[str, Any]) -> None:
    log_path = str(((runtime or {}).get("log_file") or "")).strip()
    if log_path == "":
        return
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"event": event, "timestamp": datetime.now(UTC).isoformat(), **payload}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, default=_json_default) + "\n")


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
    client.query("CREATE CONSTRAINT battle_id IF NOT EXISTS FOR (b:Battle) REQUIRE b.id IS UNIQUE")
    client.query("CREATE CONSTRAINT character_id IF NOT EXISTS FOR (c:Character) REQUIRE c.id IS UNIQUE")

    client.query(
        """
        UNWIND $rows AS row
        MERGE (b:Battle {id: row.battle_id})
          SET b.system_id = row.system_id,
              b.started_at = row.started_at,
              b.ended_at = row.ended_at
        MERGE (c:Character {id: row.character_id})
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
    lock_key = "compute_battle_rollups"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=900)
    if owner is None:
        result = {"rows_processed": 0, "rows_written": 0, "status": "skipped", "reason": "lock-not-acquired", "job_name": "compute_battle_rollups"}
        _battle_log(runtime, "battle_intelligence.job.skipped", result)
        return result

    job = start_job_run(db, "compute_battle_rollups")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _battle_log(runtime, "battle_intelligence.job.started", {"job_name": "compute_battle_rollups", "dry_run": dry_run, "computed_at": computed_at})
    try:
        killmails = db.fetch_all(
            """
            SELECT
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
            WHERE ke.effective_killmail_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 45 DAY)
              AND ke.solar_system_id IS NOT NULL
            ORDER BY ke.solar_system_id ASC, ke.effective_killmail_at ASC, ke.killmail_id ASC
            """
        )
        rows_processed = len(killmails)

        battles: dict[str, dict[str, Any]] = {}
        participant_sets: dict[str, set[int]] = defaultdict(set)
        participant_rows: dict[tuple[str, int], dict[str, Any]] = {}

        for row in killmails:
            system_id = int(row.get("system_id") or 0)
            killmail_id = int(row.get("killmail_id") or 0)
            killmail_time = str(row.get("killmail_time") or "")
            if system_id <= 0 or killmail_id <= 0 or killmail_time == "":
                continue

            bucket_unix = int(datetime.strptime(killmail_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC).timestamp() // WINDOW_SECONDS * WINDOW_SECONDS)
            battle_id = f"{system_id}:{bucket_unix}"
            battle_hash = __import__("hashlib").sha256(battle_id.encode("utf-8")).hexdigest()

            battle = battles.setdefault(
                battle_hash,
                {
                    "battle_id": battle_hash,
                    "system_id": system_id,
                    "started_at": killmail_time,
                    "ended_at": killmail_time,
                },
            )
            if killmail_time < battle["started_at"]:
                battle["started_at"] = killmail_time
            if killmail_time > battle["ended_at"]:
                battle["ended_at"] = killmail_time

            for role in (
                (
                    int(row.get("victim_character_id") or 0),
                    int(row.get("victim_corporation_id") or 0),
                    int(row.get("victim_alliance_id") or 0),
                    int(row.get("victim_ship_type_id") or 0),
                ),
                (
                    int(row.get("attacker_character_id") or 0),
                    int(row.get("attacker_corporation_id") or 0),
                    int(row.get("attacker_alliance_id") or 0),
                    int(row.get("attacker_ship_type_id") or 0),
                ),
            ):
                character_id, corporation_id, alliance_id, ship_type_id = role
                if character_id <= 0:
                    continue
                participant_sets[battle_hash].add(character_id)
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
                current["participation_count"] = int(current["participation_count"]) + 1

        if not dry_run:
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM battle_participants")
                cursor.execute("DELETE FROM battle_rollups")

                for battle in battles.values():
                    participants = len(participant_sets.get(str(battle["battle_id"]), set()))
                    started = datetime.strptime(str(battle["started_at"]), "%Y-%m-%d %H:%M:%S")
                    ended = datetime.strptime(str(battle["ended_at"]), "%Y-%m-%d %H:%M:%S")
                    duration_seconds = max(1, int((ended - started).total_seconds()))
                    cursor.execute(
                        """
                        INSERT INTO battle_rollups (
                            battle_id, system_id, started_at, ended_at, duration_seconds,
                            participant_count, eligible_for_suspicion, battle_size_class, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(battle["battle_id"]),
                            int(battle["system_id"]),
                            str(battle["started_at"]),
                            str(battle["ended_at"]),
                            duration_seconds,
                            participants,
                            1 if participants >= MIN_ELIGIBLE_PARTICIPANTS else 0,
                            _battle_size_class(participants),
                            computed_at,
                        ),
                    )
                    rows_written += 1

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

                for participant in participant_rows.values():
                    ship_type_id = int(participant.get("ship_type_id") or 0)
                    flags = role_map.get(ship_type_id, (0, 0, 0))
                    cursor.execute(
                        """
                        INSERT INTO battle_participants (
                            battle_id, character_id, corporation_id, alliance_id, side_key, ship_type_id,
                            is_logi, is_command, is_capital, participation_count, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
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
        rows_written = len(battles) + len(participant_rows)

        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "battle_count": len(battles), "participant_rows": len(participant_rows)},
        )
        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = {
            "rows_processed": rows_processed,
            "rows_written": 0 if dry_run else rows_written,
            "rows_would_write": rows_written if dry_run else rows_written,
            "battle_count": len(battles),
            "eligible_battle_count": sum(1 for battle in battles.values() if len(participant_sets.get(str(battle["battle_id"]), set())) >= MIN_ELIGIBLE_PARTICIPANTS),
            "computed_at": computed_at,
            "duration_ms": duration_ms,
            "dry_run": dry_run,
            "job_name": "compute_battle_rollups",
            "status": "success",
        }
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(
            runtime,
            "battle_intelligence.job.failed",
            {"job_name": "compute_battle_rollups", "status": "failed", "rows_processed": rows_processed, "rows_written": rows_written, "error_text": str(exc), "dry_run": dry_run},
        )
        raise
    finally:
        release_job_lock(db, lock_key, owner)


def run_compute_battle_target_metrics(db: SupplyCoreDb, runtime: dict[str, Any] | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    lock_key = "compute_battle_target_metrics"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=900)
    if owner is None:
        result = {"rows_processed": 0, "rows_written": 0, "status": "skipped", "reason": "lock-not-acquired", "job_name": "compute_battle_target_metrics"}
        _battle_log(runtime, "battle_intelligence.job.skipped", result)
        return result

    job = start_job_run(db, "compute_battle_target_metrics")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    unscored_targets = 0
    _battle_log(runtime, "battle_intelligence.job.started", {"job_name": "compute_battle_target_metrics", "dry_run": dry_run, "computed_at": computed_at})
    try:
        rows = db.fetch_all(
            """
            SELECT
                br.battle_id,
                br.started_at,
                ke.killmail_id,
                ke.effective_killmail_at AS killmail_time,
                ke.victim_character_id,
                ke.victim_ship_type_id,
                ke.victim_alliance_id,
                ke.victim_corporation_id,
                JSON_UNQUOTE(JSON_EXTRACT(kep.raw_killmail_json, '$.victim.damage_taken')) AS victim_damage_taken
            FROM battle_rollups br
            INNER JOIN killmail_events ke
                ON ke.solar_system_id = br.system_id
               AND ke.effective_killmail_at BETWEEN br.started_at AND br.ended_at
            LEFT JOIN killmail_event_payloads kep ON kep.sequence_id = ke.sequence_id
            WHERE br.eligible_for_suspicion = 1
            ORDER BY br.started_at DESC, ke.killmail_id ASC
            """
        )
        rows_processed = len(rows)

        prepared: list[dict[str, Any]] = []
        baseline: dict[tuple[int, str], list[float]] = defaultdict(list)
        for row in rows:
            damage = float(row.get("victim_damage_taken") or 0.0)
            started_at = datetime.strptime(str(row.get("started_at")), "%Y-%m-%d %H:%M:%S")
            killmail_time = datetime.strptime(str(row.get("killmail_time")), "%Y-%m-%d %H:%M:%S")
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

        if not dry_run:
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM battle_target_metrics")
                for target in prepared:
                    baseline_ttd = max(1.0, expected_ttd.get((int(target["victim_ship_type_id"]), str(target["dps_bucket"])), float(target["time_to_die_seconds"])))
                    sustain_factor = max(0.05, min(8.0, _safe_div(float(target["time_to_die_seconds"]), baseline_ttd, 1.0)))
                    cursor.execute(
                        """
                        INSERT INTO battle_target_metrics (
                            battle_id, killmail_id, victim_character_id, victim_ship_type_id, side_key,
                            first_damage_ts, last_damage_ts, time_to_die_seconds, total_damage_taken,
                            estimated_incoming_dps, dps_bucket, expected_time_to_die_seconds, sustain_factor, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        rows_written = len(prepared)

        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta={"computed_at": computed_at, "unscored_targets": unscored_targets},
        )
        duration_ms = int((datetime.now(UTC) - started_monotonic).total_seconds() * 1000)
        result = {
            "rows_processed": rows_processed,
            "rows_written": 0 if dry_run else rows_written,
            "rows_would_write": rows_written if dry_run else rows_written,
            "computed_at": computed_at,
            "unscored_targets": unscored_targets,
            "duration_ms": duration_ms,
            "dry_run": dry_run,
            "job_name": "compute_battle_target_metrics",
            "status": "success",
        }
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_battle_target_metrics", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise
    finally:
        release_job_lock(db, lock_key, owner)


def run_compute_battle_anomalies(db: SupplyCoreDb, runtime: dict[str, Any] | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    lock_key = "compute_battle_anomalies"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=900)
    if owner is None:
        result = {"rows_processed": 0, "rows_written": 0, "status": "skipped", "reason": "lock-not-acquired", "job_name": "compute_battle_anomalies"}
        _battle_log(runtime, "battle_intelligence.job.skipped", result)
        return result

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
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM battle_anomalies")
                cursor.execute("DELETE FROM battle_side_metrics")

                efficiency_sorted = sorted((float(item["efficiency_score"]) for item in shaped))
                for item in shaped:
                    mean, stddev = stats.get(str(item["battle_size_class"]), (0.0, 0.0))
                    z = _safe_div(float(item["efficiency_score"]) - mean, stddev, 0.0) if stddev > 0 else 0.0
                    percentile_rank = _percentile(efficiency_sorted, float(item["efficiency_score"]))
                    anomaly_class = "normal"
                    if z > 2.0:
                        anomaly_class = "high_sustain"
                    elif z < -1.0:
                        anomaly_class = "low_sustain"

                    cursor.execute(
                        """
                        INSERT INTO battle_side_metrics (
                            battle_id, side_key, participant_count, logi_count, command_count, capital_count,
                            total_kills, kill_rate_per_minute, median_sustain_factor, average_sustain_factor,
                            switch_pressure, efficiency_score, z_efficiency_score, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
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
                    cursor.execute(
                        """
                        INSERT INTO battle_anomalies (
                            battle_id, side_key, anomaly_class, z_efficiency_score, percentile_rank, explanation_json, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(item["battle_id"]),
                            str(item["side_key"]),
                            anomaly_class,
                            float(z),
                            float(percentile_rank),
                            json.dumps(explanation, separators=(",", ":"), ensure_ascii=False),
                            computed_at,
                        ),
                    )
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
        result = {
            "rows_processed": rows_processed,
            "rows_written": 0 if dry_run else rows_written,
            "rows_would_write": rows_written if dry_run else rows_written,
            "anomaly_count": len(shaped),
            "high_sustain_count": high_count,
            "computed_at": computed_at,
            "duration_ms": duration_ms,
            "dry_run": dry_run,
            "job_name": "compute_battle_anomalies",
            "status": "success",
        }
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_battle_anomalies", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise
    finally:
        release_job_lock(db, lock_key, owner)


def run_compute_battle_actor_features(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    lock_key = "compute_battle_actor_features"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=900)
    if owner is None:
        result = {"rows_processed": 0, "rows_written": 0, "status": "skipped", "reason": "lock-not-acquired", "job_name": "compute_battle_actor_features"}
        _battle_log(runtime, "battle_intelligence.job.skipped", result)
        return result

    job = start_job_run(db, "compute_battle_actor_features")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _battle_log(runtime, "battle_intelligence.job.started", {"job_name": "compute_battle_actor_features", "dry_run": dry_run, "computed_at": computed_at})
    try:
        rows = db.fetch_all(
            """
            SELECT
                bp.battle_id,
                bp.character_id,
                bp.side_key,
                bp.participation_count,
                bp.is_logi,
                bp.is_command,
                bp.is_capital,
                COALESCE(ba.anomaly_class, 'normal') AS anomaly_class
            FROM battle_participants bp
            INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
            LEFT JOIN battle_anomalies ba ON ba.battle_id = bp.battle_id AND ba.side_key = bp.side_key
            WHERE br.eligible_for_suspicion = 1
            """
        )
        rows_processed = len(rows)

        max_participation: dict[str, int] = defaultdict(int)
        for row in rows:
            battle_id = str(row.get("battle_id") or "")
            max_participation[battle_id] = max(max_participation[battle_id], int(row.get("participation_count") or 0))

        if not dry_run:
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM battle_actor_features")
                for row in rows:
                    battle_id = str(row.get("battle_id") or "")
                    participation_count = int(row.get("participation_count") or 0)
                    centrality = _safe_div(float(participation_count), float(max_participation.get(battle_id) or 1), 0.0)
                    role_weight = 0.2 * int(row.get("is_logi") or 0) + 0.2 * int(row.get("is_command") or 0) + 0.3 * int(row.get("is_capital") or 0)
                    visibility = min(1.0, centrality + role_weight)
                    anomaly_class = str(row.get("anomaly_class") or "normal")
                    cursor.execute(
                        """
                        INSERT INTO battle_actor_features (
                            battle_id, character_id, side_key, participation_count, centrality_score, visibility_score,
                            is_logi, is_command, is_capital, participated_in_high_sustain, participated_in_low_sustain, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            battle_id,
                            int(row.get("character_id") or 0),
                            str(row.get("side_key") or "unknown"),
                            participation_count,
                            float(centrality),
                            float(visibility),
                            int(row.get("is_logi") or 0),
                            int(row.get("is_command") or 0),
                            int(row.get("is_capital") or 0),
                            1 if anomaly_class == "high_sustain" else 0,
                            1 if anomaly_class == "low_sustain" else 0,
                            computed_at,
                        ),
                    )
        rows_written = len(rows)

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
        result = {
            "rows_processed": rows_processed,
            "rows_written": 0 if dry_run else rows_written,
            "rows_would_write": rows_written if dry_run else rows_written,
            "computed_at": computed_at,
            "neo4j": neo_result,
            "duration_ms": duration_ms,
            "dry_run": dry_run,
            "job_name": "compute_battle_actor_features",
            "status": "success",
        }
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_battle_actor_features", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise
    finally:
        release_job_lock(db, lock_key, owner)


def run_compute_suspicion_scores(db: SupplyCoreDb, runtime: dict[str, Any] | None = None, *, dry_run: bool = False) -> dict[str, Any]:
    lock_key = "compute_suspicion_scores"
    owner = acquire_job_lock(db, lock_key, ttl_seconds=900)
    if owner is None:
        result = {"rows_processed": 0, "rows_written": 0, "status": "skipped", "reason": "lock-not-acquired", "job_name": "compute_suspicion_scores"}
        _battle_log(runtime, "battle_intelligence.job.skipped", result)
        return result

    job = start_job_run(db, "compute_suspicion_scores")
    started_monotonic = datetime.now(UTC)
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    _battle_log(runtime, "battle_intelligence.job.started", {"job_name": "compute_suspicion_scores", "dry_run": dry_run, "computed_at": computed_at})
    try:
        actor_rows = db.fetch_all(
            """
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
            LEFT JOIN character_graph_intelligence cgi ON cgi.character_id = baf.character_id
            WHERE baf.character_id > 0
            """
        )
        rows_processed = len(actor_rows)

        by_character: dict[int, list[dict[str, Any]]] = defaultdict(list)
        battle_side_index: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in actor_rows:
            character_id = int(row.get("character_id") or 0)
            if character_id <= 0:
                continue
            by_character[character_id].append(row)
            battle_side_index[str(row.get("battle_id"))].append(row)

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
            absent_enemy_z: list[float] = []
            for other_character_id, other_rows in by_character.items():
                if other_character_id == character_id:
                    continue
                for other in other_rows:
                    if str(other.get("battle_id")) in eligible_battle_ids:
                        continue
                    if int(other.get("eligible_for_suspicion") or 0) == 1:
                        absent_enemy_z.append(float(other.get("z_efficiency_score") or 0.0))
            present_avg = _safe_div(sum(present_enemy_z), float(len(present_enemy_z)), 0.0)
            absent_avg = _safe_div(sum(absent_enemy_z), float(len(absent_enemy_z)), 0.0)
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
                    "top_supporting_battles_json": json.dumps(support_battles, separators=(",", ":"), ensure_ascii=False),
                    "explanation_json": json.dumps(explanation, separators=(",", ":"), ensure_ascii=False),
                }
            )

        scores_sorted = sorted((float(item["suspicion_score"]) for item in score_rows))

        minimum_sample_filtered_count = sum(1 for row in intelligence_rows if int(row["eligible_battle_count"]) < MIN_SAMPLE_COUNT)

        if not dry_run:
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM character_battle_intelligence")
                cursor.execute("DELETE FROM character_suspicion_scores")

                for row in intelligence_rows:
                    cursor.execute(
                    """
                    INSERT INTO character_battle_intelligence (
                        character_id, total_battle_count, eligible_battle_count, high_sustain_battle_count,
                        low_sustain_battle_count, high_sustain_frequency, low_sustain_frequency,
                        cross_side_battle_count, cross_side_rate, enemy_efficiency_uplift, ally_efficiency_uplift,
                        role_weight, anomalous_battle_density, computed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
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
                    ),
                )

                for row in score_rows:
                    percentile_rank = _percentile(scores_sorted, float(row["suspicion_score"]))
                    cursor.execute(
                    """
                    INSERT INTO character_suspicion_scores (
                        character_id, suspicion_score, percentile_rank, high_sustain_frequency,
                        low_sustain_frequency, cross_side_rate, enemy_efficiency_uplift, role_weight,
                        supporting_battle_count, top_supporting_battles_json, explanation_json, computed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
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
                        str(row["explanation_json"]),
                        computed_at,
                    ),
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
        result = {
            "rows_processed": rows_processed,
            "rows_written": 0 if dry_run else rows_written,
            "rows_would_write": rows_written if dry_run else rows_written,
            "computed_at": computed_at,
            "scored_character_count": len(score_rows),
            "minimum_sample_filtered_count": minimum_sample_filtered_count,
            "duration_ms": duration_ms,
            "dry_run": dry_run,
            "job_name": "compute_suspicion_scores",
            "status": "success",
        }
        _battle_log(runtime, "battle_intelligence.job.success", result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _battle_log(runtime, "battle_intelligence.job.failed", {"job_name": "compute_suspicion_scores", "status": "failed", "error_text": str(exc), "rows_processed": rows_processed, "rows_written": rows_written, "dry_run": dry_run})
        raise
    finally:
        release_job_lock(db, lock_key, owner)
