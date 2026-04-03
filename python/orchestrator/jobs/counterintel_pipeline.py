from __future__ import annotations

import bisect
from collections import defaultdict
from datetime import UTC, datetime, timedelta
import json
import math
import statistics
import time
from typing import Any

from ..db import SupplyCoreDb
from ..evewho_adapter import EveWhoAdapter
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

COUNTERINTEL_DATASET_KEY = "compute_counterintel_pipeline_cursor"
DEFAULT_BATTLE_BATCH_SIZE = 200
DEFAULT_MAX_BATCHES = 20
DEFAULT_EVEWHO_BATCH_SIZE = 20


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator <= 0:
        return default
    return numerator / denominator


def _cohort_normalize(evidence_rows: list[dict[str, Any]]) -> None:
    """Enrich evidence rows in-place with cohort statistics (z_score, mad_score, cohort_percentile, confidence_flag)."""
    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence_rows:
        if row.get("evidence_value") is not None:
            by_key[row["evidence_key"]].append(row)

    for key, rows in by_key.items():
        values = [float(r["evidence_value"]) for r in rows]
        n = len(values)
        if n == 0:
            continue

        mean = statistics.mean(values)
        std = statistics.pstdev(values) if n > 1 else 0.0
        median = statistics.median(values)
        diffs = [abs(v - median) for v in values]
        mad = statistics.median(diffs) if diffs else 0.0
        sorted_vals = sorted(values)

        for row in rows:
            raw = float(row["evidence_value"])
            dev = raw - mean
            row["expected_value"] = round(mean, 6)
            row["deviation_value"] = round(dev, 6)
            row["z_score"] = round(dev / std, 6) if std > 0 else 0.0
            row["mad_score"] = round((raw - median) / (mad * 1.4826), 6) if mad > 0 else 0.0
            row["cohort_percentile"] = round(
                bisect.bisect_right(sorted_vals, raw) / max(1, n), 6
            )
            if n >= 10:
                row["confidence_flag"] = "high"
            elif n >= 5:
                row["confidence_flag"] = "medium"
            else:
                row["confidence_flag"] = "low"


def _sync_state_get(db: SupplyCoreDb, dataset_key: str) -> dict[str, Any] | None:
    return db.fetch_one("SELECT dataset_key, last_cursor FROM sync_state WHERE dataset_key = %s LIMIT 1", (dataset_key,))


def _sync_state_upsert(db: SupplyCoreDb, dataset_key: str, cursor: str, status: str, row_count: int) -> None:
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
        (dataset_key, status, cursor, max(0, int(row_count))),
    )



# _http_json removed — use EveWhoAdapter from evewho_adapter module instead.


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _normalize_iso_string(value: Any) -> str | None:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return None
    return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_history_projection_rows(history_json: Any, source: str) -> list[dict[str, Any]]:
    try:
        parsed = json.loads(history_json) if isinstance(history_json, str) and history_json.strip() else {}
    except json.JSONDecodeError:
        parsed = {}
    history_rows = parsed.get("corporation_history") if isinstance(parsed, dict) else None
    if not isinstance(history_rows, list):
        return []

    timeline: list[tuple[int, datetime]] = []
    for row in history_rows:
        if not isinstance(row, dict):
            continue
        corp_id = int(row.get("corporation_id") or 0)
        start_dt = _parse_iso_datetime(row.get("start_date"))
        if corp_id <= 0 or start_dt is None:
            continue
        timeline.append((corp_id, start_dt.astimezone(UTC)))
    if not timeline:
        return []

    timeline.sort(key=lambda item: item[1])
    projection_rows: list[dict[str, Any]] = []
    for idx, (corp_id, start_dt) in enumerate(timeline):
        end_dt = timeline[idx + 1][1] if idx + 1 < len(timeline) else None
        projection_rows.append(
            {
                "corporation_id": corp_id,
                "start": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ") if end_dt else None,
                "source": source,
            }
        )
    return projection_rows


def _walk_nested_rows(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            rows.append(current)
            for value in current.values():
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(current, list):
            for item in current:
                if isinstance(item, (dict, list)):
                    stack.append(item)
    return rows


def _extract_first_int(row: dict[str, Any], keys: list[str]) -> int | None:
    for key in keys:
        value = row.get(key)
        try:
            normalized = int(value or 0)
        except (TypeError, ValueError):
            normalized = 0
        if normalized > 0:
            return normalized
    return None



# Local EveWhoAdapter removed — use shared EveWhoAdapter from evewho_adapter module (imported above).


def _enrich_org_history_cache(db: SupplyCoreDb, character_ids: list[int], user_agent: str, ttl_hours: int, max_fetches: int, fetch_batch_size: int) -> int:
    if not character_ids:
        return 0

    cutoff = (datetime.now(UTC) - timedelta(hours=max(1, ttl_hours))).strftime("%Y-%m-%d %H:%M:%S")
    fresh = db.fetch_all(
        """
        SELECT character_id
        FROM character_org_history_cache
        WHERE source = 'evewho'
          AND fetched_at >= %s
          AND character_id IN (""" + ",".join(["%s"] * len(character_ids)) + ")",
        tuple([cutoff, *character_ids]),
    )
    fresh_ids = {int(row.get("character_id") or 0) for row in fresh}
    pending = [cid for cid in character_ids if cid not in fresh_ids][:max(0, max_fetches)]
    if not pending:
        return 0

    adapter = EveWhoAdapter(user_agent)
    character_payloads: dict[int, tuple[str, dict[str, Any]]] = {}
    corp_ids: set[int] = set()
    alliance_ids: set[int] = set()
    chunk_size = max(1, min(50, fetch_batch_size))
    for idx in range(0, len(pending), chunk_size):
        for character_id in pending[idx : idx + chunk_size]:
            endpoint, payload = adapter.fetch_character(character_id)
            if not payload:
                continue
            character_payloads[character_id] = (endpoint, payload)
            corp_id = int(payload.get("corporation_id") or 0)
            alliance_id = int(payload.get("alliance_id") or 0)
            if corp_id > 0:
                corp_ids.add(corp_id)
            if alliance_id > 0:
                alliance_ids.add(alliance_id)

    remaining_fetch_budget = max(0, max_fetches - len(character_payloads))
    corp_list_payloads: dict[int, tuple[str, dict[str, Any]]] = {}
    corp_joined_payloads: dict[int, tuple[str, dict[str, Any]]] = {}
    corp_departed_payloads: dict[int, tuple[str, dict[str, Any]]] = {}
    allilist_payloads: dict[int, tuple[str, dict[str, Any]]] = {}
    for corp_id in sorted(corp_ids):
        if remaining_fetch_budget <= 0:
            break
        endpoint, payload = adapter.fetch_corplist(corp_id)
        if payload:
            corp_list_payloads[corp_id] = (endpoint, payload)
        remaining_fetch_budget -= 1
        if remaining_fetch_budget <= 0:
            break
        endpoint, payload = adapter.fetch_corpjoined(corp_id)
        if payload:
            corp_joined_payloads[corp_id] = (endpoint, payload)
        remaining_fetch_budget -= 1
        if remaining_fetch_budget <= 0:
            break
        endpoint, payload = adapter.fetch_corpdeparted(corp_id)
        if payload:
            corp_departed_payloads[corp_id] = (endpoint, payload)
        remaining_fetch_budget -= 1
    for alliance_id in sorted(alliance_ids):
        if remaining_fetch_budget <= 0:
            break
        endpoint, payload = adapter.fetch_allilist(alliance_id)
        if payload:
            allilist_payloads[alliance_id] = (endpoint, payload)
        remaining_fetch_budget -= 1

    written = 0
    fetched_at = _now_sql()
    expires_at = (datetime.now(UTC) + timedelta(hours=max(1, ttl_hours))).strftime("%Y-%m-%d %H:%M:%S")
    with db.transaction() as (_, cursor):
        for character_id in pending:
            loaded = character_payloads.get(character_id)
            if not loaded:
                continue
            source_endpoint, payload = loaded
            history = payload.get("corporation_history") if isinstance(payload.get("corporation_history"), list) else []
            corp_hops = len(history)
            short_tenure = 0
            event_rows: list[tuple[int, str, str | None, str]] = []
            for idx, row in enumerate(history):
                joined = str(row.get("start_date") or "")
                left = str(history[idx + 1].get("start_date") or "") if idx + 1 < len(history) else ""
                corp_id = int(row.get("corporation_id") or 0)
                if corp_id > 0 and joined:
                    event_rows.append((corp_id, "joined", joined, source_endpoint))
                if corp_id > 0 and left:
                    event_rows.append((corp_id, "departed", left, source_endpoint))
                if joined and left:
                    try:
                        joined_dt = _parse_iso_datetime(joined)
                        left_dt = _parse_iso_datetime(left)
                        delta = abs((left_dt - joined_dt).days) if joined_dt and left_dt else 999999
                        if delta <= 30:
                            short_tenure += 1
                    except ValueError:
                        pass
            current_corp_id = int(payload.get("corporation_id") or 0)
            current_alliance_id = int(payload.get("alliance_id") or 0)
            if current_corp_id > 0 and current_corp_id in corp_joined_payloads:
                joined_endpoint, joined_payload = corp_joined_payloads[current_corp_id]
                for row in _walk_nested_rows(joined_payload):
                    row_character_id = _extract_first_int(row, ["character_id", "characterID", "char_id", "id"])
                    if row_character_id != character_id:
                        continue
                    moved_at = row.get("start_date") or row.get("date") or row.get("joined_at")
                    event_rows.append((current_corp_id, "joined", str(moved_at or ""), joined_endpoint))
            if current_corp_id > 0 and current_corp_id in corp_departed_payloads:
                departed_endpoint, departed_payload = corp_departed_payloads[current_corp_id]
                for row in _walk_nested_rows(departed_payload):
                    row_character_id = _extract_first_int(row, ["character_id", "characterID", "char_id", "id"])
                    if row_character_id != character_id:
                        continue
                    moved_at = row.get("start_date") or row.get("date") or row.get("departed_at")
                    event_rows.append((current_corp_id, "departed", str(moved_at or ""), departed_endpoint))

            corplist_loaded = corp_list_payloads.get(current_corp_id) if current_corp_id > 0 else None
            if corplist_loaded:
                corplist_endpoint, corplist_payload = corplist_loaded
                for row in _walk_nested_rows(corplist_payload):
                    row_character_id = _extract_first_int(row, ["character_id", "characterID", "char_id", "id"])
                    if row_character_id != character_id:
                        continue
                    joined_at = row.get("start_date") or row.get("date") or row.get("joined_at")
                    event_rows.append((current_corp_id, "joined", str(joined_at or ""), corplist_endpoint))

            cursor.execute(
                """
                INSERT INTO character_org_history_cache (
                    character_id, source, current_corporation_id, current_alliance_id,
                    corp_hops_180d, short_tenure_hops_180d, hostile_adjacent_hops_180d,
                    history_json, source_endpoint, fetched_at, expires_at
                ) VALUES (%s, 'evewho', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    current_corporation_id = VALUES(current_corporation_id),
                    current_alliance_id = VALUES(current_alliance_id),
                    corp_hops_180d = VALUES(corp_hops_180d),
                    short_tenure_hops_180d = VALUES(short_tenure_hops_180d),
                    hostile_adjacent_hops_180d = VALUES(hostile_adjacent_hops_180d),
                    history_json = VALUES(history_json),
                    source_endpoint = VALUES(source_endpoint),
                    fetched_at = VALUES(fetched_at),
                    expires_at = VALUES(expires_at)
                """,
                (
                    character_id,
                    current_corp_id or None,
                    current_alliance_id or None,
                    corp_hops,
                    short_tenure,
                    0,
                    json_dumps_safe(payload),
                    source_endpoint,
                    fetched_at,
                    expires_at,
                ),
            )
            cursor.execute(
                "DELETE FROM character_org_history_events WHERE character_id = %s AND source = 'evewho'",
                (character_id,),
            )
            for corp_id, event_type, event_at_raw, event_endpoint in event_rows:
                event_dt = _parse_iso_datetime(event_at_raw)
                cursor.execute(
                    """
                    INSERT INTO character_org_history_events (
                        character_id, source, corporation_id, event_type, event_at, source_endpoint, fetched_at
                    ) VALUES (%s, 'evewho', %s, %s, %s, %s, %s)
                    """,
                    (character_id, corp_id, event_type, event_dt.strftime("%Y-%m-%d %H:%M:%S") if event_dt else None, event_endpoint, fetched_at),
                )

            cursor.execute(
                "DELETE FROM character_org_alliance_adjacency_snapshots WHERE character_id = %s AND source = 'evewho'",
                (character_id,),
            )
            alliance_corp_ids: set[int] = set()
            if current_corp_id > 0:
                alliance_corp_ids.add(current_corp_id)
                if corplist_loaded:
                    _, corplist_payload = corplist_loaded
                    for row in _walk_nested_rows(corplist_payload):
                        corp_id = _extract_first_int(row, ["corporation_id", "corporationID", "corp_id"])
                        if corp_id and corp_id > 0:
                            alliance_corp_ids.add(corp_id)
            if current_alliance_id > 0:
                allilist_loaded = allilist_payloads.get(current_alliance_id)
                if allilist_loaded:
                    allilist_endpoint, allilist_payload = allilist_loaded
                    for row in _walk_nested_rows(allilist_payload):
                        corp_id = _extract_first_int(row, ["corporation_id", "corporationID", "corp_id"])
                        if corp_id and corp_id > 0:
                            alliance_corp_ids.add(corp_id)
                    for corp_id in alliance_corp_ids:
                        cursor.execute(
                            """
                            INSERT INTO character_org_alliance_adjacency_snapshots (
                                character_id, source, alliance_id, corporation_id, source_endpoint, fetched_at, expires_at
                            ) VALUES (%s, 'evewho', %s, %s, %s, %s, %s)
                            """,
                            (character_id, current_alliance_id, corp_id, allilist_endpoint, fetched_at, expires_at),
                        )
                elif source_endpoint:
                    for corp_id in alliance_corp_ids:
                        cursor.execute(
                            """
                            INSERT INTO character_org_alliance_adjacency_snapshots (
                                character_id, source, alliance_id, corporation_id, source_endpoint, fetched_at, expires_at
                            ) VALUES (%s, 'evewho', %s, %s, %s, %s, %s)
                            """,
                            (character_id, current_alliance_id, corp_id, source_endpoint, fetched_at, expires_at),
                        )
            written += 1
    return written


def run_compute_counterintel_pipeline(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    lock_key = "compute_counterintel_pipeline"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    runtime = runtime or {}
    batch_size = max(10, min(200, int(runtime.get("counterintel_batch_size") or DEFAULT_BATTLE_BATCH_SIZE)))
    max_batches = max(1, min(20, int(runtime.get("counterintel_max_batches") or DEFAULT_MAX_BATCHES)))

    try:
        cursor = str((_sync_state_get(db, COUNTERINTEL_DATASET_KEY) or {}).get("last_cursor") or "")
        user_agent = str(runtime.get("evewho_user_agent") or "SupplyCoreCounterIntel/1.0 (+https://supplycore)")
        org_cache_ttl_hours = max(6, int(runtime.get("evewho_cache_ttl_hours") or 24))
        org_max_fetches = max(5, int(runtime.get("evewho_max_fetches_per_run") or 100))
        org_fetch_batch_size = max(1, int(runtime.get("evewho_fetch_batch_size") or DEFAULT_EVEWHO_BATCH_SIZE))

        neo4j_config = Neo4jConfig.from_runtime(neo4j_raw or {})
        neo4j = Neo4jClient(neo4j_config) if neo4j_config.enabled else None

        processed_battles = 0
        batch_count = 0
        last_battle_id = cursor
        assignment_mismatch_count = 0
        assignment_mismatch_samples: list[dict[str, Any]] = []
        while batch_count < max_batches:
            battles = db.fetch_all(
                """
                SELECT br.battle_id, br.system_id, br.started_at, br.ended_at, br.participant_count
                FROM battle_rollups br
                WHERE br.eligible_for_suspicion = 1
                  AND br.participant_count >= 10
                  AND br.battle_id > %s
                ORDER BY br.battle_id ASC
                LIMIT %s
                """,
                (last_battle_id, batch_size),
            )
            if not battles:
                break

            battle_ids = [str(row["battle_id"]) for row in battles]
            last_battle_id = battle_ids[-1]
            processed_battles += len(battle_ids)
            batch_count += 1

            placeholders = ",".join(["%s"] * len(battle_ids))
            target_rows = db.fetch_all(
                f"""
                SELECT btm.battle_id, btm.side_key, btm.victim_ship_type_id,
                       AVG(btm.time_to_die_seconds) AS hull_survival_seconds,
                       AVG(btm.expected_time_to_die_seconds) AS baseline_survival_seconds,
                       COUNT(*) AS sample_count,
                       AVG(btm.sustain_factor) AS sustain_factor
                FROM battle_target_metrics btm
                WHERE btm.battle_id IN ({placeholders})
                GROUP BY btm.battle_id, btm.side_key, btm.victim_ship_type_id
                """,
                tuple(battle_ids),
            )
            rows_processed += len(target_rows)

            overperformance_rows: list[dict[str, Any]] = []
            hull_rows: list[dict[str, Any]] = []
            side_lifts: dict[tuple[str, str], list[float]] = defaultdict(list)
            for row in target_rows:
                battle_id = str(row.get("battle_id") or "")
                side_key = str(row.get("side_key") or "unknown")
                hull_survival = float(row.get("hull_survival_seconds") or 0.0)
                baseline_survival = max(1.0, float(row.get("baseline_survival_seconds") or 0.0))
                survival_lift = hull_survival / baseline_survival
                side_lifts[(battle_id, side_key)].append(survival_lift)
                hull_rows.append(
                    {
                        "battle_id": battle_id,
                        "side_key": side_key,
                        "victim_ship_type_id": int(row.get("victim_ship_type_id") or 0),
                        "hull_survival_seconds": hull_survival,
                        "baseline_survival_seconds": baseline_survival,
                        "survival_lift": survival_lift,
                        "sample_count": int(row.get("sample_count") or 0),
                    }
                )

            for (battle_id, side_key), lifts in side_lifts.items():
                sustain_lift = _safe_div(sum(lifts), float(max(1, len(lifts))), 0.0)
                control_delta = sustain_lift - 1.0
                anomaly_class = "normal"
                if sustain_lift >= 1.25:
                    anomaly_class = "high_enemy_overperformance"
                elif sustain_lift <= 0.85:
                    anomaly_class = "underperforming"
                overperformance_rows.append(
                    {
                        "battle_id": battle_id,
                        "side_key": side_key,
                        "overperformance_score": max(0.0, min(3.0, sustain_lift)),
                        "sustain_lift_score": sustain_lift,
                        "hull_survival_lift_score": sustain_lift,
                        "control_delta_score": control_delta,
                        "anomaly_class": anomaly_class,
                        "evidence_json": json_dumps_safe({"mean_hull_survival_lift": sustain_lift, "sample_size": len(lifts)}),
                    }
                )

            participants = db.fetch_all(
                f"""
                SELECT bp.battle_id, bp.character_id, bp.side_key,
                       bp.corporation_id, bp.alliance_id,
                       COALESCE(cgi.bridge_score, 0) AS bridge_score
                FROM battle_participants bp
                LEFT JOIN character_graph_intelligence cgi ON cgi.character_id = bp.character_id
                WHERE bp.battle_id IN ({placeholders})
                """,
                tuple(battle_ids),
            )
            rows_processed += len(participants)
            by_character: dict[int, list[dict[str, Any]]] = defaultdict(list)
            anomalous_battles = {f"{row['battle_id']}|{row['side_key']}" for row in overperformance_rows if row["anomaly_class"] == "high_enemy_overperformance"}
            control_battles = {f"{row['battle_id']}|{row['side_key']}" for row in overperformance_rows if row["anomaly_class"] != "high_enemy_overperformance"}
            for row in participants:
                cid = int(row.get("character_id") or 0)
                if cid <= 0:
                    continue
                by_character[cid].append(row)

            org_written = _enrich_org_history_cache(db, list(by_character.keys()), user_agent, org_cache_ttl_hours, org_max_fetches, org_fetch_batch_size)

            org_rows = db.fetch_all(
                "SELECT character_id, corp_hops_180d, short_tenure_hops_180d, history_json, source_endpoint FROM character_org_history_cache WHERE source = 'evewho' AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP()) AND character_id IN ("
                + ",".join(["%s"] * len(by_character))
                + ")",
                tuple(by_character.keys()) if by_character else tuple([0]),
            ) if by_character else []
            org_by_character = {int(row["character_id"]): row for row in org_rows}
            alliance_rows = db.fetch_all(
                """
                SELECT character_id, alliance_id, corporation_id, source_endpoint, fetched_at, expires_at
                FROM character_org_alliance_adjacency_snapshots
                WHERE source = 'evewho'
                  AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP())
                  AND character_id IN ("""
                + ",".join(["%s"] * len(by_character))
                + ")",
                tuple(by_character.keys()) if by_character else tuple([0]),
            ) if by_character else []

            battle_to_character_rows: dict[str, list[tuple[int, bool]]] = defaultdict(list)
            seen_battle_character: set[tuple[str, int]] = set()
            for row in participants:
                battle_id = str(row.get("battle_id") or "")
                character_id = int(row.get("character_id") or 0)
                if not battle_id or character_id <= 0:
                    continue
                dedupe_key = (battle_id, character_id)
                if dedupe_key in seen_battle_character:
                    continue
                seen_battle_character.add(dedupe_key)
                side_key = str(row.get("side_key") or "unknown")
                battle_to_character_rows[battle_id].append((character_id, f"{battle_id}|{side_key}" in anomalous_battles))

            copresence_aggregate: dict[tuple[int, int], dict[str, int]] = defaultdict(lambda: {"count": 0, "anomalous_count": 0})
            for characters in battle_to_character_rows.values():
                sorted_characters = sorted(characters, key=lambda item: item[0])
                for left_index, (left_character_id, left_anomalous) in enumerate(sorted_characters):
                    for right_character_id, right_anomalous in sorted_characters[left_index + 1 :]:
                        key = (left_character_id, right_character_id)
                        copresence_aggregate[key]["count"] += 1
                        if left_anomalous and right_anomalous:
                            copresence_aggregate[key]["anomalous_count"] += 1

            copresence_rows = [
                {
                    "left_character_id": left_character_id,
                    "right_character_id": right_character_id,
                    "count": int(metrics.get("count") or 0),
                    "anomalous_count": int(metrics.get("anomalous_count") or 0),
                    "source": "battle_co_presence_aggregate",
                }
                for (left_character_id, right_character_id), metrics in copresence_aggregate.items()
                if int(metrics.get("count") or 0) > 0
            ]
            historical_membership_rows: list[dict[str, Any]] = []
            for character_id, org in org_by_character.items():
                source_endpoint = str(org.get("source_endpoint") or "").strip() or "evewho"
                history_rows = _build_history_projection_rows(org.get("history_json"), source_endpoint)
                for row in history_rows:
                    historical_membership_rows.append(
                        {
                            "character_id": character_id,
                            "corporation_id": int(row["corporation_id"]),
                            "start": row["start"],
                            "end": row["end"],
                            "source": row["source"],
                        }
                    )

            corp_alliance_projection: dict[tuple[int, int], dict[str, str | None]] = {}
            for row in alliance_rows:
                alliance_id = int(row.get("alliance_id") or 0)
                corporation_id = int(row.get("corporation_id") or 0)
                if alliance_id <= 0 or corporation_id <= 0:
                    continue
                start_iso = _normalize_iso_string(row.get("fetched_at"))
                end_iso = _normalize_iso_string(row.get("expires_at"))
                source = str(row.get("source_endpoint") or "").strip() or "evewho"
                key = (corporation_id, alliance_id)
                existing = corp_alliance_projection.get(key)
                if existing is None:
                    corp_alliance_projection[key] = {"start": start_iso, "end": end_iso, "source": source}
                    continue
                existing_start = existing.get("start")
                existing_end = existing.get("end")
                if start_iso and (existing_start is None or start_iso < existing_start):
                    existing["start"] = start_iso
                if end_iso and (existing_end is None or end_iso > existing_end):
                    existing["end"] = end_iso
                if not str(existing.get("source") or "").strip():
                    existing["source"] = source
            corp_alliance_rows = [
                {
                    "corporation_id": corporation_id,
                    "alliance_id": alliance_id,
                    "start": values.get("start"),
                    "end": values.get("end"),
                    "source": str(values.get("source") or "evewho"),
                }
                for (corporation_id, alliance_id), values in corp_alliance_projection.items()
            ]

            # --- Compute hostile_adjacent_hops_180d per character ---
            # Build mapping: battle_id → side_key → set of alliance_ids
            battle_side_alliances: dict[str, dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
            for row in participants:
                bid = str(row.get("battle_id") or "")
                sk = str(row.get("side_key") or "unknown")
                aid = int(row.get("alliance_id") or 0)
                if bid and aid > 0:
                    battle_side_alliances[bid][sk].add(aid)

            # Build mapping: corporation_id → set of alliance_ids from adjacency data
            corp_to_alliances: dict[int, set[int]] = defaultdict(set)
            for corp_id, alliance_id in corp_alliance_projection.keys():
                corp_to_alliances[corp_id].add(alliance_id)

            cutoff_180d = datetime.now(UTC) - timedelta(days=180)
            hostile_hops_by_character: dict[int, int] = {}
            for character_id, presences in by_character.items():
                # Determine hostile alliances: alliances on the opposing side(s)
                hostile_alliances: set[int] = set()
                for row in presences:
                    bid = str(row.get("battle_id") or "")
                    char_side = str(row.get("side_key") or "unknown")
                    for sk, alli_set in battle_side_alliances.get(bid, {}).items():
                        if sk != char_side:
                            hostile_alliances |= alli_set

                if not hostile_alliances:
                    hostile_hops_by_character[character_id] = 0
                    continue

                # Walk org history and count hops into corps with hostile alliance adjacency
                org = org_by_character.get(character_id, {})
                history_rows = _build_history_projection_rows(org.get("history_json"), "evewho")
                hostile_count = 0
                for hrow in history_rows:
                    start_dt = _parse_iso_datetime(hrow.get("start"))
                    if start_dt and start_dt >= cutoff_180d:
                        corp_id = int(hrow["corporation_id"])
                        corp_alli = corp_to_alliances.get(corp_id, set())
                        if corp_alli & hostile_alliances:
                            hostile_count += 1
                hostile_hops_by_character[character_id] = hostile_count

            # Batch-update hostile_adjacent_hops_180d in the org cache
            if not dry_run:
                with db.transaction() as (_, cursor_hostile):
                    for cid, hops in hostile_hops_by_character.items():
                        if hops > 0:
                            cursor_hostile.execute(
                                "UPDATE character_org_history_cache SET hostile_adjacent_hops_180d = %s WHERE character_id = %s AND source = 'evewho'",
                                (hops, cid),
                            )

            feature_rows: list[dict[str, Any]] = []
            score_rows: list[dict[str, Any]] = []
            evidence_rows: list[dict[str, Any]] = []
            control_membership_rows: list[tuple[str, str, int]] = []
            anomalous_battle_denominator = len(anomalous_battles)
            control_battle_denominator = len(control_battles)
            battle_started_by_id = {str(row.get("battle_id") or ""): row.get("started_at") for row in battles}
            for character_id, presences in by_character.items():
                anomaly_hits = 0
                control_hits = 0
                sustain_lifts: list[float] = []
                anomalous_battle_ids: set[str] = set()
                control_battle_ids: set[str] = set()
                repeatability_windows_7d: set[str] = set()
                repeatability_windows_30d: set[str] = set()
                for row in presences:
                    battle_id = str(row.get("battle_id") or "")
                    side_key = str(row.get("side_key") or "unknown")
                    key = f"{battle_id}|{side_key}"
                    if key in anomalous_battles:
                        anomaly_hits += 1
                        anomalous_battle_ids.add(battle_id)
                    if key in control_battles:
                        control_hits += 1
                        control_battle_ids.add(battle_id)
                        control_membership_rows.append((battle_id, side_key, character_id))
                    started_at = battle_started_by_id.get(battle_id)
                    started_dt = _parse_iso_datetime(started_at) if isinstance(started_at, str) else started_at
                    if isinstance(started_dt, datetime):
                        repeatability_windows_7d.add(started_dt.strftime("%Y-W%U"))
                        repeatability_windows_30d.add(started_dt.strftime("%Y-%m"))
                    for over in overperformance_rows:
                        if over["battle_id"] == battle_id and over["side_key"] != side_key:
                            sustain_lifts.append(float(over["sustain_lift_score"]))
                anomalous_rate = _safe_div(float(anomaly_hits), float(max(1, anomalous_battle_denominator)), 0.0)
                control_rate = _safe_div(float(control_hits), float(max(1, control_battle_denominator)), 0.0)
                presence_delta = anomalous_rate - control_rate
                presence_lift = _safe_div(anomalous_rate, control_rate, 0.0) if control_rate > 0 else (1.0 if anomalous_rate > 0 else 0.0)
                enemy_sustain_lift = _safe_div(sum(sustain_lifts), float(max(1, len(sustain_lifts))), 0.0)
                enemy_sustain_min = min(sustain_lifts) if sustain_lifts else 0.0
                enemy_sustain_max = max(sustain_lifts) if sustain_lifts else 0.0
                bridge = _safe_div(sum(float(r.get("bridge_score") or 0.0) for r in presences), float(max(1, len(presences))), 0.0)
                cluster_proximity = min(1.0, _safe_div(bridge, 5.0, 0.0))
                org = org_by_character.get(character_id, {})
                corp_hops = int(org.get("corp_hops_180d") or 0)
                short_hops = int(org.get("short_tenure_hops_180d") or 0)
                corp_hop_frequency = _safe_div(float(corp_hops), 180.0, 0.0)
                short_ratio = _safe_div(float(short_hops), float(max(1, corp_hops)), 0.0)
                repeatability = min(1.0, _safe_div(float(anomaly_hits), 3.0, 0.0))
                repeatability_distinct_battles = len(anomalous_battle_ids)
                repeatability_weeks = len(repeatability_windows_7d)
                repeatability_months = len(repeatability_windows_30d)
                feature_rows.append(
                    {
                        "character_id": character_id,
                        "anomalous_battle_presence_count": anomaly_hits,
                        "control_battle_presence_count": control_hits,
                        "anomalous_battle_denominator": anomalous_battle_denominator,
                        "control_battle_denominator": control_battle_denominator,
                        "anomalous_presence_rate": anomalous_rate,
                        "control_presence_rate": control_rate,
                        "enemy_same_hull_survival_lift": enemy_sustain_lift,
                        "enemy_sustain_lift": enemy_sustain_lift,
                        "co_presence_anomalous_density": anomalous_rate,
                        "graph_bridge_score": bridge,
                        "corp_hop_frequency_180d": corp_hop_frequency,
                        "short_tenure_ratio_180d": short_ratio,
                        "repeatability_score": repeatability,
                    }
                )
                review_score = max(0.0, min(1.0, 0.24 * anomalous_rate + 0.1 * max(0.0, presence_delta) + 0.26 * min(1.0, enemy_sustain_lift / 1.5) + 0.2 * min(1.0, bridge / 5.0) + 0.1 * min(1.0, corp_hop_frequency * 10) + 0.1 * repeatability))
                numerator_denominator_payload = json_dumps_safe(
                    {
                        "anomalous": {"numerator": anomaly_hits, "denominator": anomalous_battle_denominator, "rate": anomalous_rate},
                        "control": {"numerator": control_hits, "denominator": control_battle_denominator, "rate": control_rate},
                        "delta": presence_delta,
                        "lift": presence_lift,
                    }
                )
                survival_lift_payload = json_dumps_safe(
                    {
                        "enemy_same_hull_survival_lift": enemy_sustain_lift,
                        "sample_count": len(sustain_lifts),
                        "min_lift": enemy_sustain_min,
                        "max_lift": enemy_sustain_max,
                    }
                )
                graph_payload = json_dumps_safe(
                    {
                        "co_presence_anomalous_density": anomalous_rate,
                        "graph_bridge_score": bridge,
                        "cluster_proximity_score": cluster_proximity,
                    }
                )
                hostile_hops = hostile_hops_by_character.get(character_id, 0)
                org_history_payload = json_dumps_safe(
                    {
                        "window_days": 180,
                        "corp_hops": corp_hops,
                        "short_tenure_hops": short_hops,
                        "hostile_adjacent_hops": hostile_hops,
                        "corp_hop_frequency_per_day": corp_hop_frequency,
                        "short_tenure_ratio": short_ratio,
                        "high_movement_indicator": corp_hops >= 3,
                    }
                )
                repeatability_payload = json_dumps_safe(
                    {
                        "anomalous_battle_count": repeatability_distinct_battles,
                        "control_battle_count": len(control_battle_ids),
                        "distinct_week_windows_7d": repeatability_weeks,
                        "distinct_month_windows_30d": repeatability_months,
                        "repeatability_score": repeatability,
                    }
                )
                character_evidence_rows = [
                    {"character_id": character_id, "evidence_key": "anomalous_battle_presence_count", "window_label": "all_time", "evidence_value": float(anomaly_hits), "evidence_text": f"present in {anomaly_hits}/{anomalous_battle_denominator} anomalous large battle-sides", "evidence_payload_json": numerator_denominator_payload},
                    {"character_id": character_id, "evidence_key": "anomalous_presence_rate", "window_label": "all_time", "evidence_value": anomalous_rate, "evidence_text": f"anomalous presence rate {anomalous_rate:.3f} ({anomaly_hits}/{anomalous_battle_denominator}) vs control {control_rate:.3f} ({control_hits}/{control_battle_denominator})", "evidence_payload_json": numerator_denominator_payload},
                    {"character_id": character_id, "evidence_key": "presence_rate_delta", "window_label": "all_time", "evidence_value": presence_delta, "evidence_text": f"presence delta {presence_delta:.3f}, lift {presence_lift:.3f}", "evidence_payload_json": numerator_denominator_payload},
                    {"character_id": character_id, "evidence_key": "enemy_sustain_lift", "window_label": "all_time", "evidence_value": enemy_sustain_lift, "evidence_text": f"enemy sustain lift {enemy_sustain_lift:.3f} when present", "evidence_payload_json": survival_lift_payload},
                    {"character_id": character_id, "evidence_key": "enemy_same_hull_survival_lift_detail", "window_label": "all_time", "evidence_value": enemy_sustain_lift, "evidence_text": f"same-hull enemy survival lift {enemy_sustain_lift:.3f} across {len(sustain_lifts)} samples (min {enemy_sustain_min:.3f}, max {enemy_sustain_max:.3f})", "evidence_payload_json": survival_lift_payload},
                    {"character_id": character_id, "evidence_key": "graph_copresence_cluster_proximity", "window_label": "all_time", "evidence_value": cluster_proximity, "evidence_text": f"graph bridge {bridge:.3f}, anomalous co-presence density {anomalous_rate:.3f}, cluster proximity {cluster_proximity:.3f}", "evidence_payload_json": graph_payload},
                    {"character_id": character_id, "evidence_key": "org_history_movement_180d", "window_label": "180d", "evidence_value": corp_hop_frequency, "evidence_text": f"org movement over 180d: {corp_hops} hops, {short_hops} short-tenure, {hostile_hops} hostile-adjacent, ratio {short_ratio:.3f}", "evidence_payload_json": org_history_payload},
                    {"character_id": character_id, "evidence_key": "repeatability_across_battles_windows", "window_label": "all_time", "evidence_value": repeatability, "evidence_text": f"repeatability {repeatability:.3f}: {repeatability_distinct_battles} anomalous battles across {repeatability_weeks} weekly and {repeatability_months} monthly windows", "evidence_payload_json": repeatability_payload},
                ]
                evidence_rows.extend(character_evidence_rows)
                score_rows.append({"character_id": character_id, "review_priority_score": review_score, "confidence_score": min(1.0, _safe_div(float(anomaly_hits + control_hits), 8.0, 0.0)), "evidence_count": len(character_evidence_rows)})

            sorted_scores = sorted([float(row["review_priority_score"]) for row in score_rows])
            for row in score_rows:
                row["percentile_rank"] = _safe_div(float(sum(1 for x in sorted_scores if x <= float(row["review_priority_score"]))), float(max(1, len(sorted_scores))), 0.0)

            _cohort_normalize(evidence_rows)

            if not dry_run:
                with db.transaction() as (_, cursor_db):
                    for row in hull_rows:
                        cursor_db.execute(
                            """
                            INSERT INTO hull_survival_anomaly_metrics (
                                battle_id, side_key, victim_ship_type_id, hull_survival_seconds, baseline_survival_seconds, survival_lift, sample_count, computed_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                hull_survival_seconds = VALUES(hull_survival_seconds),
                                baseline_survival_seconds = VALUES(baseline_survival_seconds),
                                survival_lift = VALUES(survival_lift),
                                sample_count = VALUES(sample_count),
                                computed_at = VALUES(computed_at)
                            """,
                            (row["battle_id"], row["side_key"], row["victim_ship_type_id"], row["hull_survival_seconds"], row["baseline_survival_seconds"], row["survival_lift"], row["sample_count"], computed_at),
                        )
                    for row in overperformance_rows:
                        cursor_db.execute(
                            """
                            INSERT INTO battle_enemy_overperformance_scores (
                                battle_id, side_key, overperformance_score, sustain_lift_score, hull_survival_lift_score,
                                control_delta_score, anomaly_class, evidence_json, computed_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                overperformance_score = VALUES(overperformance_score),
                                sustain_lift_score = VALUES(sustain_lift_score),
                                hull_survival_lift_score = VALUES(hull_survival_lift_score),
                                control_delta_score = VALUES(control_delta_score),
                                anomaly_class = VALUES(anomaly_class),
                                evidence_json = VALUES(evidence_json),
                                computed_at = VALUES(computed_at)
                            """,
                            (row["battle_id"], row["side_key"], row["overperformance_score"], row["sustain_lift_score"], row["hull_survival_lift_score"], row["control_delta_score"], row["anomaly_class"], row["evidence_json"], computed_at),
                        )
                    for row in feature_rows:
                        cursor_db.execute(
                            """
                            INSERT INTO character_counterintel_features (
                                character_id, anomalous_battle_presence_count, control_battle_presence_count,
                                anomalous_battle_denominator, control_battle_denominator,
                                anomalous_presence_rate, control_presence_rate, enemy_same_hull_survival_lift,
                                enemy_sustain_lift, co_presence_anomalous_density, graph_bridge_score,
                                corp_hop_frequency_180d, short_tenure_ratio_180d, repeatability_score, computed_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                anomalous_battle_presence_count = VALUES(anomalous_battle_presence_count),
                                control_battle_presence_count = VALUES(control_battle_presence_count),
                                anomalous_battle_denominator = VALUES(anomalous_battle_denominator),
                                control_battle_denominator = VALUES(control_battle_denominator),
                                anomalous_presence_rate = VALUES(anomalous_presence_rate),
                                control_presence_rate = VALUES(control_presence_rate),
                                enemy_same_hull_survival_lift = VALUES(enemy_same_hull_survival_lift),
                                enemy_sustain_lift = VALUES(enemy_sustain_lift),
                                co_presence_anomalous_density = VALUES(co_presence_anomalous_density),
                                graph_bridge_score = VALUES(graph_bridge_score),
                                corp_hop_frequency_180d = VALUES(corp_hop_frequency_180d),
                                short_tenure_ratio_180d = VALUES(short_tenure_ratio_180d),
                                repeatability_score = VALUES(repeatability_score),
                                computed_at = VALUES(computed_at)
                            """,
                            (row["character_id"], row["anomalous_battle_presence_count"], row["control_battle_presence_count"], row["anomalous_battle_denominator"], row["control_battle_denominator"], row["anomalous_presence_rate"], row["control_presence_rate"], row["enemy_same_hull_survival_lift"], row["enemy_sustain_lift"], row["co_presence_anomalous_density"], row["graph_bridge_score"], row["corp_hop_frequency_180d"], row["short_tenure_ratio_180d"], row["repeatability_score"], computed_at),
                        )
                    if control_membership_rows:
                        for battle_id, side_key, character_id in control_membership_rows:
                            cursor_db.execute(
                                """
                                INSERT INTO battle_side_control_cohort_membership (
                                    battle_id, side_key, character_id, computed_at
                                ) VALUES (%s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    computed_at = VALUES(computed_at)
                                """,
                                (battle_id, side_key, character_id, computed_at),
                            )
                    for row in score_rows:
                        cursor_db.execute(
                            """
                            INSERT INTO character_counterintel_scores (
                                character_id, review_priority_score, percentile_rank, confidence_score, evidence_count, computed_at
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                review_priority_score = VALUES(review_priority_score),
                                percentile_rank = VALUES(percentile_rank),
                                confidence_score = VALUES(confidence_score),
                                evidence_count = VALUES(evidence_count),
                                computed_at = VALUES(computed_at)
                            """,
                            (row["character_id"], row["review_priority_score"], row["percentile_rank"], row["confidence_score"], row["evidence_count"], computed_at),
                        )
                    for row in evidence_rows:
                        cursor_db.execute(
                            """
                            INSERT INTO character_counterintel_evidence (
                                character_id, evidence_key, window_label,
                                evidence_value, expected_value, deviation_value,
                                z_score, mad_score, cohort_percentile, confidence_flag,
                                evidence_text, evidence_payload_json, computed_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                evidence_value = VALUES(evidence_value),
                                expected_value = VALUES(expected_value),
                                deviation_value = VALUES(deviation_value),
                                z_score = VALUES(z_score),
                                mad_score = VALUES(mad_score),
                                cohort_percentile = VALUES(cohort_percentile),
                                confidence_flag = VALUES(confidence_flag),
                                evidence_text = VALUES(evidence_text),
                                evidence_payload_json = VALUES(evidence_payload_json),
                                computed_at = VALUES(computed_at)
                            """,
                            (
                                row["character_id"], row["evidence_key"], row.get("window_label", "all_time"),
                                row["evidence_value"], row.get("expected_value"), row.get("deviation_value"),
                                row.get("z_score"), row.get("mad_score"), row.get("cohort_percentile"),
                                row.get("confidence_flag", "low"),
                                row["evidence_text"], row["evidence_payload_json"], computed_at,
                            ),
                        )
                    if battles:
                        for battle in battles:
                            battle_id = str(battle.get("battle_id") or "")
                            system_id = int(battle.get("system_id") or 0)
                            started_at = battle.get("started_at")
                            ended_at = battle.get("ended_at")
                            if not battle_id or system_id <= 0 or started_at is None or ended_at is None:
                                continue
                            cursor_db.execute(
                                """
                                UPDATE killmail_events
                                SET battle_id = %s
                                WHERE battle_id IS NULL
                                  AND solar_system_id = %s
                                  AND effective_killmail_at BETWEEN %s AND %s
                                """,
                                (battle_id, system_id, started_at, ended_at),
                            )

                    if battle_ids:
                        cursor_db.execute(
                            f"""
                            SELECT
                                ke.killmail_id,
                                ke.battle_id,
                                ke.solar_system_id,
                                ke.effective_killmail_at,
                                br.system_id AS expected_system_id,
                                br.started_at,
                                br.ended_at
                            FROM killmail_events ke
                            INNER JOIN battle_rollups br ON br.battle_id = ke.battle_id
                            WHERE ke.battle_id IN ({placeholders})
                              AND (
                                    ke.solar_system_id <> br.system_id
                                    OR ke.effective_killmail_at < br.started_at
                                    OR ke.effective_killmail_at > br.ended_at
                                  )
                            ORDER BY ke.battle_id ASC, ke.killmail_id ASC
                            LIMIT 25
                            """,
                            tuple(battle_ids),
                        )
                        mismatch_rows = [dict(row) for row in cursor_db.fetchall()]
                        cursor_db.execute(
                            f"""
                            SELECT COUNT(*) AS mismatch_count
                            FROM killmail_events ke
                            INNER JOIN battle_rollups br ON br.battle_id = ke.battle_id
                            WHERE ke.battle_id IN ({placeholders})
                              AND (
                                    ke.solar_system_id <> br.system_id
                                    OR ke.effective_killmail_at < br.started_at
                                    OR ke.effective_killmail_at > br.ended_at
                                  )
                            """,
                            tuple(battle_ids),
                        )
                        mismatch_count_row = dict(cursor_db.fetchone() or {"mismatch_count": 0})
                        assignment_mismatch_count += int(mismatch_count_row.get("mismatch_count") or 0)
                        for row in mismatch_rows:
                            if len(assignment_mismatch_samples) >= 25:
                                break
                            assignment_mismatch_samples.append(
                                {
                                    "killmail_id": int(row.get("killmail_id") or 0),
                                    "battle_id": str(row.get("battle_id") or ""),
                                    "solar_system_id": int(row.get("solar_system_id") or 0),
                                    "effective_killmail_at": str(row.get("effective_killmail_at") or ""),
                                    "expected_system_id": int(row.get("expected_system_id") or 0),
                                    "started_at": str(row.get("started_at") or ""),
                                    "ended_at": str(row.get("ended_at") or ""),
                                }
                            )

            if neo4j:
                neo4j_batch = 500
                neo4j_timeout = 60
                if overperformance_rows:
                    tagged_rows = [{**row, "computed_at": computed_at} for row in overperformance_rows]
                    for i in range(0, len(tagged_rows), neo4j_batch):
                        neo4j.query(
                            """
                            UNWIND $rows AS row
                            MERGE (b:Battle {battle_id: row.battle_id})
                            MERGE (s:BattleSide {side_uid: row.battle_id + '|' + row.side_key})
                            MERGE (s)-[:BELONGS_TO]->(b)
                            SET s.overperformance_score = row.overperformance_score,
                                s.anomaly_class = row.anomaly_class,
                                s.computed_at = row.computed_at
                            """,
                            {"rows": tagged_rows[i:i + neo4j_batch]},
                            timeout_seconds=neo4j_timeout,
                        )
                    anomalous_battle_rows = [
                        {"character_id": row["character_id"], "battle_id": p["battle_id"], "review_priority_score": row["review_priority_score"], "computed_at": computed_at}
                        for row in score_rows
                        for p in by_character.get(int(row["character_id"]), [])
                        if f"{p.get('battle_id')}|{p.get('side_key')}" in anomalous_battles
                    ]
                    for i in range(0, len(anomalous_battle_rows), neo4j_batch):
                        neo4j.query(
                            """
                            UNWIND $rows AS row
                            MERGE (c:Character {character_id: row.character_id})
                            MERGE (b:Battle {battle_id: row.battle_id})
                            MERGE (c)-[r:PRESENT_IN_ANOMALOUS_BATTLE]->(b)
                            SET r.review_priority_score = row.review_priority_score,
                                r.computed_at = row.computed_at
                            """,
                            {"rows": anomalous_battle_rows[i:i + neo4j_batch]},
                            timeout_seconds=neo4j_timeout,
                        )
                if copresence_rows:
                    for i in range(0, len(copresence_rows), neo4j_batch):
                        neo4j.query(
                            """
                            UNWIND $rows AS row
                            MATCH (left:Character {character_id: row.left_character_id})
                            MATCH (right:Character {character_id: row.right_character_id})
                            MERGE (left)-[r:CO_PRESENT_WITH]->(right)
                            SET r.count = toInteger(COALESCE(r.count, 0)) + toInteger(row.count),
                                r.anomalous_count = toInteger(COALESCE(r.anomalous_count, 0)) + toInteger(row.anomalous_count),
                                r.source = row.source,
                                r.computed_at = $computed_at
                            """,
                            {"rows": copresence_rows[i:i + neo4j_batch], "computed_at": computed_at},
                            timeout_seconds=neo4j_timeout,
                        )
                if historical_membership_rows:
                    for i in range(0, len(historical_membership_rows), neo4j_batch):
                        neo4j.query(
                            """
                            UNWIND $rows AS row
                            WITH row WHERE row.start IS NOT NULL AND row.source IS NOT NULL
                            MERGE (c:Character {character_id: row.character_id})
                            MERGE (corp:Corporation {corporation_id: toInteger(row.corporation_id)})
                            MERGE (c)-[r:HISTORICALLY_IN]->(corp)
                            SET r.start = COALESCE(row.start, r.start),
                                r.source = COALESCE(row.source, r.source),
                                r.end = row.end
                            """,
                            {"rows": historical_membership_rows[i:i + neo4j_batch]},
                            timeout_seconds=neo4j_timeout,
                        )
                if corp_alliance_rows:
                    for i in range(0, len(corp_alliance_rows), neo4j_batch):
                        neo4j.query(
                            """
                            UNWIND $rows AS row
                            MERGE (corp:Corporation {corporation_id: toInteger(row.corporation_id)})
                            MERGE (alliance:Alliance {alliance_id: toInteger(row.alliance_id)})
                            MERGE (corp)-[r:PART_OF]->(alliance)
                            SET r.start = CASE
                                    WHEN row.start IS NULL THEN r.start
                                    WHEN r.start IS NULL OR row.start < r.start THEN row.start
                                    ELSE r.start
                                END,
                                r.end = CASE
                                    WHEN row.end IS NULL THEN r.end
                                    WHEN r.end IS NULL OR row.end > r.end THEN row.end
                                    ELSE r.end
                                END,
                                r.source = row.source
                            """,
                            {"rows": corp_alliance_rows[i:i + neo4j_batch]},
                            timeout_seconds=neo4j_timeout,
                        )

            rows_written += len(hull_rows) + len(overperformance_rows) + len(feature_rows) + len(score_rows) + len(evidence_rows) + org_written
            _sync_state_upsert(db, COUNTERINTEL_DATASET_KEY, last_battle_id, "success", rows_written)

        has_more = batch_count == max_batches

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Processed {processed_battles} eligible 100+ participant battles across {batch_count} batches.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            batches_completed=batch_count,
            has_more=has_more,
            meta={
                "computed_at": computed_at,
                "rows_would_write": rows_written if dry_run else rows_written,
                "cursor": last_battle_id,
                "dry_run": dry_run,
                "battle_assignment_validation": {
                    "mismatch_count": assignment_mismatch_count,
                    "sample_rows": assignment_mismatch_samples,
                },
            },
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written, meta=result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        _sync_state_upsert(db, COUNTERINTEL_DATASET_KEY, "", "failed", rows_written)
        raise
