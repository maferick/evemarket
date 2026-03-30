"""Compute per-character feature snapshots across time windows (7d/30d/90d/lifetime).

Reads battle participation, killmail events, org history, and graph metrics
to produce a canonical feature row per character per window.  Histogram data
(hour-of-day, day-of-week) is stored in a companion table to keep the main
feature rows compact.

Incremental processing: uses a cursor on ``battle_rollups.battle_id`` to avoid
reprocessing the full battle history on every run.
"""

from __future__ import annotations

import json
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASET_KEY = "compute_character_feature_windows_cursor"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_BATCHES = 8

WINDOW_DEFS: list[tuple[str, timedelta | None]] = [
    ("7d", timedelta(days=7)),
    ("30d", timedelta(days=30)),
    ("90d", timedelta(days=90)),
    ("lifetime", None),
]


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    if denominator <= 0:
        return default
    return numerator / denominator


# ---------------------------------------------------------------------------
# Sync-state helpers — reuse the same pattern as counterintel_pipeline
# ---------------------------------------------------------------------------

def _sync_state_get(db: SupplyCoreDb, dataset_key: str) -> dict[str, Any] | None:
    return db.fetch_one(
        "SELECT dataset_key, last_cursor FROM sync_state WHERE dataset_key = %s LIMIT 1",
        (dataset_key,),
    )


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


# ---------------------------------------------------------------------------
# Data collection helpers
# ---------------------------------------------------------------------------

def _fetch_battle_characters(
    db: SupplyCoreDb,
    battle_ids: list[str],
) -> list[dict[str, Any]]:
    """Return participant rows for the given battles."""
    if not battle_ids:
        return []
    placeholders = ",".join(["%s"] * len(battle_ids))
    return db.fetch_all(
        f"""
        SELECT
            bp.character_id,
            bp.battle_id,
            br.system_id,
            br.started_at,
            HOUR(br.started_at) AS battle_hour,
            DAYOFWEEK(br.started_at) AS battle_dow
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        WHERE bp.battle_id IN ({placeholders})
          AND bp.character_id > 0
        """,
        tuple(battle_ids),
    )


def _fetch_org_transitions(
    db: SupplyCoreDb,
    character_ids: list[int],
    cutoff: str | None,
) -> dict[int, dict[str, int]]:
    """Return per-character corp/alliance transition counts since cutoff."""
    if not character_ids:
        return {}
    placeholders = ",".join(["%s"] * len(character_ids))
    params: list[Any] = list(character_ids)
    where_cutoff = ""
    if cutoff:
        where_cutoff = "AND event_date >= %s"
        params.append(cutoff)
    rows = db.fetch_all(
        f"""
        SELECT
            character_id,
            SUM(CASE WHEN event_type = 'corp_change' THEN 1 ELSE 0 END) AS corp_transitions,
            SUM(CASE WHEN event_type = 'alliance_change' THEN 1 ELSE 0 END) AS alliance_transitions
        FROM character_org_history_events
        WHERE character_id IN ({placeholders})
          {where_cutoff}
        GROUP BY character_id
        """,
        tuple(params),
    )
    result: dict[int, dict[str, int]] = {}
    for row in rows:
        cid = int(row["character_id"])
        result[cid] = {
            "corp_transitions": int(row.get("corp_transitions") or 0),
            "alliance_transitions": int(row.get("alliance_transitions") or 0),
        }
    return result


def _fetch_graph_metrics(
    db: SupplyCoreDb,
    character_ids: list[int],
) -> dict[int, dict[str, Any]]:
    """Return latest graph intelligence metrics for the given characters."""
    if not character_ids:
        return {}
    placeholders = ",".join(["%s"] * len(character_ids))
    rows = db.fetch_all(
        f"""
        SELECT character_id, pagerank_score, bridge_score, community_id
        FROM character_graph_intelligence
        WHERE character_id IN ({placeholders})
        """,
        tuple(character_ids),
    )
    return {
        int(r["character_id"]): {
            "pagerank": float(r.get("pagerank_score") or 0),
            "bridge": float(r.get("bridge_score") or 0),
            "community_id": int(r.get("community_id") or 0),
        }
        for r in rows
    }


# ---------------------------------------------------------------------------
# Feature computation per window
# ---------------------------------------------------------------------------

def _compute_window_features(
    participations: list[dict[str, Any]],
    window_label: str,
    window_delta: timedelta | None,
    now_dt: datetime,
    org_transitions: dict[str, int],
    graph_metrics: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compute feature row + histogram row for one character in one window.

    ``participations`` are all battle participations for this character.
    Only rows within the window are considered.
    """
    if window_delta is not None:
        cutoff = now_dt - window_delta
        rows = [p for p in participations if p["started_at"] and p["started_at"] >= cutoff]
    else:
        rows = participations

    battles = {str(r["battle_id"]) for r in rows}
    systems = {int(r["system_id"]) for r in rows if r.get("system_id")}

    # Co-presence: other characters seen in the same battles
    # (this is a count of distinct co-present character_ids across all battles in-window,
    #  but since we process per-character we just count unique battle_ids here and
    #  the co_presence_count is filled from the batch-level accumulator outside)
    co_presence_count = 0  # filled by caller

    # Hour / weekday histograms
    hour_hist: dict[int, int] = defaultdict(int)
    dow_hist: dict[int, int] = defaultdict(int)
    for r in rows:
        hour_hist[int(r.get("battle_hour") or 0)] += 1
        dow_hist[int(r.get("battle_dow") or 1)] += 1

    # Dominant region — approximate via system_id frequency
    system_counts: dict[int, int] = defaultdict(int)
    for r in rows:
        sid = int(r.get("system_id") or 0)
        if sid:
            system_counts[sid] += 1
    dominant_system = max(system_counts, key=system_counts.get, default=0) if system_counts else 0
    dominant_ratio = _safe_div(system_counts.get(dominant_system, 0), len(rows)) if rows else 0.0

    # Recurring associates placeholder — filled by batch-level co-presence analysis
    recurring_associates = 0  # filled by caller

    feature = {
        "window_label": window_label,
        "battles_total": len(battles),
        "unique_systems": len(systems),
        "recurring_associates": recurring_associates,
        "co_presence_count": co_presence_count,
        "corp_transitions": org_transitions.get("corp_transitions", 0),
        "alliance_transitions": org_transitions.get("alliance_transitions", 0),
        "dominant_region_id": dominant_system,
        "dominant_region_ratio": round(dominant_ratio, 6),
        "graph_pagerank": graph_metrics.get("pagerank", 0.0),
        "graph_bridge_score": graph_metrics.get("bridge", 0.0),
        "graph_community_id": graph_metrics.get("community_id", 0),
    }

    histogram = {
        "window_label": window_label,
        "hour_histogram": {str(k): v for k, v in sorted(hour_hist.items())},
        "weekday_histogram": {str(k): v for k, v in sorted(dow_hist.items())},
    }

    return feature, histogram


# ---------------------------------------------------------------------------
# Batch-level co-presence analysis
# ---------------------------------------------------------------------------

def _enrich_co_presence(
    character_features: dict[int, dict[str, dict[str, Any]]],
    char_battles: dict[int, dict[str, set[str]]],
    battle_chars: dict[str, set[int]],
) -> None:
    """Fill recurring_associates and co_presence_count on computed features.

    ``char_battles[cid][window_label]`` = set of battle_ids.
    ``battle_chars[battle_id]`` = set of character_ids that participated.
    """
    for cid, windows in character_features.items():
        for wlabel, feat in windows.items():
            battles_in_window = char_battles.get(cid, {}).get(wlabel, set())
            co_chars: dict[int, int] = defaultdict(int)
            for bid in battles_in_window:
                for other_cid in battle_chars.get(bid, set()):
                    if other_cid != cid:
                        co_chars[other_cid] += 1
            feat["co_presence_count"] = len(co_chars)
            # Recurring: seen together in 2+ battles within the window
            feat["recurring_associates"] = sum(1 for c in co_chars.values() if c >= 2)


# ---------------------------------------------------------------------------
# Main job entry point
# ---------------------------------------------------------------------------

def run_compute_character_feature_windows(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute and upsert per-character feature snapshots for all windows."""
    lock_key = "compute_character_feature_windows"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    now_dt = datetime.now(UTC)
    runtime = runtime or {}
    batch_size = max(10, min(500, int(runtime.get("feature_windows_batch_size") or DEFAULT_BATCH_SIZE)))
    max_batches = max(1, min(30, int(runtime.get("feature_windows_max_batches") or DEFAULT_MAX_BATCHES)))

    try:
        cursor = str((_sync_state_get(db, DATASET_KEY) or {}).get("last_cursor") or "")

        batch_count = 0
        last_battle_id = cursor

        while batch_count < max_batches:
            # Fetch next batch of battles by cursor
            battles = db.fetch_all(
                """
                SELECT br.battle_id, br.system_id, br.started_at, br.participant_count
                FROM battle_rollups br
                WHERE br.battle_id > %s
                ORDER BY br.battle_id ASC
                LIMIT %s
                """,
                (last_battle_id, batch_size),
            )
            if not battles:
                break

            battle_ids = [str(row["battle_id"]) for row in battles]
            last_battle_id = battle_ids[-1]
            batch_count += 1

            # Fetch all participants for these battles
            participations = _fetch_battle_characters(db, battle_ids)
            rows_processed += len(participations)
            if not participations:
                _sync_state_upsert(db, DATASET_KEY, last_battle_id, "success", rows_written)
                continue

            # Build per-character participation index
            char_participations: dict[int, list[dict[str, Any]]] = defaultdict(list)
            battle_chars: dict[str, set[int]] = defaultdict(set)
            for p in participations:
                cid = int(p["character_id"])
                char_participations[cid].append(p)
                battle_chars[str(p["battle_id"])].add(cid)

            character_ids = list(char_participations.keys())

            # Fetch org transitions for all windows
            org_by_window: dict[str, dict[int, dict[str, int]]] = {}
            for wlabel, wdelta in WINDOW_DEFS:
                cutoff_str = (now_dt - wdelta).strftime("%Y-%m-%d") if wdelta else None
                org_by_window[wlabel] = _fetch_org_transitions(db, character_ids, cutoff_str)

            # Fetch graph metrics (window-agnostic snapshot)
            graph_metrics = _fetch_graph_metrics(db, character_ids)

            # Compute features per character per window
            # character_features[cid][window_label] = feature dict
            character_features: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
            character_histograms: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
            char_battles: dict[int, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

            for cid, parts in char_participations.items():
                gm = graph_metrics.get(cid, {})
                for wlabel, wdelta in WINDOW_DEFS:
                    org = org_by_window[wlabel].get(cid, {})
                    feat, hist = _compute_window_features(parts, wlabel, wdelta, now_dt, org, gm)
                    character_features[cid][wlabel] = feat
                    character_histograms[cid][wlabel] = hist

                    # Track battles per character per window for co-presence
                    cutoff = (now_dt - wdelta) if wdelta else None
                    for p in parts:
                        if cutoff is None or (p["started_at"] and p["started_at"] >= cutoff):
                            char_battles[cid][wlabel].add(str(p["battle_id"]))

            # Enrich co-presence counts
            _enrich_co_presence(character_features, char_battles, battle_chars)

            # Upsert
            if not dry_run:
                with db.transaction() as (_, cur):
                    for cid, windows in character_features.items():
                        for wlabel, feat in windows.items():
                            cur.execute(
                                """
                                INSERT INTO character_feature_windows (
                                    character_id, window_label, battles_total, unique_systems,
                                    recurring_associates, co_presence_count,
                                    corp_transitions, alliance_transitions,
                                    dominant_region_id, dominant_region_ratio,
                                    graph_pagerank, graph_bridge_score, graph_community_id,
                                    computed_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    battles_total = VALUES(battles_total),
                                    unique_systems = VALUES(unique_systems),
                                    recurring_associates = VALUES(recurring_associates),
                                    co_presence_count = VALUES(co_presence_count),
                                    corp_transitions = VALUES(corp_transitions),
                                    alliance_transitions = VALUES(alliance_transitions),
                                    dominant_region_id = VALUES(dominant_region_id),
                                    dominant_region_ratio = VALUES(dominant_region_ratio),
                                    graph_pagerank = VALUES(graph_pagerank),
                                    graph_bridge_score = VALUES(graph_bridge_score),
                                    graph_community_id = VALUES(graph_community_id),
                                    computed_at = VALUES(computed_at)
                                """,
                                (
                                    cid,
                                    wlabel,
                                    feat["battles_total"],
                                    feat["unique_systems"],
                                    feat["recurring_associates"],
                                    feat["co_presence_count"],
                                    feat["corp_transitions"],
                                    feat["alliance_transitions"],
                                    feat["dominant_region_id"],
                                    feat["dominant_region_ratio"],
                                    feat["graph_pagerank"],
                                    feat["graph_bridge_score"],
                                    feat["graph_community_id"],
                                    computed_at,
                                ),
                            )
                            rows_written += 1

                    for cid, windows in character_histograms.items():
                        for wlabel, hist in windows.items():
                            cur.execute(
                                """
                                INSERT INTO character_feature_histograms (
                                    character_id, window_label, hour_histogram, weekday_histogram, computed_at
                                ) VALUES (%s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    hour_histogram = VALUES(hour_histogram),
                                    weekday_histogram = VALUES(weekday_histogram),
                                    computed_at = VALUES(computed_at)
                                """,
                                (
                                    cid,
                                    wlabel,
                                    json.dumps(hist["hour_histogram"]),
                                    json.dumps(hist["weekday_histogram"]),
                                    computed_at,
                                ),
                            )

            _sync_state_upsert(db, DATASET_KEY, last_battle_id, "success", rows_written)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Computed feature windows across {batch_count} batches, wrote {rows_written} feature rows.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            batches_completed=batch_count,
            meta={
                "computed_at": computed_at,
                "cursor": last_battle_id,
                "dry_run": dry_run,
                "windows": [w[0] for w in WINDOW_DEFS],
            },
            checkpoint_before=cursor,
            checkpoint_after=last_battle_id,
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written, meta=result)
        return result

    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise
