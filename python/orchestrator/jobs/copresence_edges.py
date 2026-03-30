"""Compute generalized pairwise co-presence edge weights across multiple event types.

Event types produced:
  * ``same_battle``              — both characters appeared in the same battle
  * ``same_side``                — both characters fought on the same side
  * ``same_system_time_window``  — both active in the same system within a time window
  * ``related_engagement``       — both involved in recurrent engagements together
  * ``same_operational_area``    — both share a dominant operational region

Edges are written to ``character_copresence_edges`` with per-window rolling
weights (7d / 30d / 90d).  Detection signals (pair frequency deltas,
out-of-cluster ratio, expected-cluster decay, cohort percentiles) are written
to ``character_copresence_signals``.

Neo4j is used to push the aggregated CO_PRESENT_GENERALIZED relationships for
graph topology consumption.
"""

from __future__ import annotations

import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from ..neo4j import Neo4jClient, Neo4jConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATASET_KEY = "compute_copresence_edges_cursor"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_BATCHES = 8

WINDOW_DEFS: list[tuple[str, timedelta]] = [
    ("7d", timedelta(days=7)),
    ("30d", timedelta(days=30)),
    ("90d", timedelta(days=90)),
]

# Weight multipliers per event type (higher = stronger signal)
EVENT_WEIGHTS: dict[str, float] = {
    "same_battle": 1.0,
    "same_side": 1.5,
    "same_system_time_window": 0.6,
    "related_engagement": 1.2,
    "same_operational_area": 0.4,
}

# Two characters active in the same system within this many hours count as
# co-located even if they weren't in the exact same battle.
SYSTEM_TIME_PROXIMITY_HOURS = 4

# Minimum co-battle count within a window to qualify as a "related engagement"
RELATED_ENGAGEMENT_MIN_BATTLES = 2


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(num: float, den: float, default: float = 0.0) -> float:
    return num / den if den > 0 else default


# ---------------------------------------------------------------------------
# Sync-state helpers
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
# Data collection
# ---------------------------------------------------------------------------

def _fetch_battle_participants(
    db: SupplyCoreDb,
    battle_ids: list[str],
) -> list[dict[str, Any]]:
    """Return participant rows with side_key and system context."""
    if not battle_ids:
        return []
    placeholders = ",".join(["%s"] * len(battle_ids))
    return db.fetch_all(
        f"""
        SELECT
            bp.character_id,
            bp.battle_id,
            bp.side_key,
            br.system_id,
            br.started_at
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        WHERE bp.battle_id IN ({placeholders})
          AND bp.character_id > 0
        """,
        tuple(battle_ids),
    )


def _fetch_character_dominant_regions(
    db: SupplyCoreDb,
    character_ids: list[int],
    window_label: str,
) -> dict[int, int]:
    """Return dominant_region_id per character from feature windows."""
    if not character_ids:
        return {}
    placeholders = ",".join(["%s"] * len(character_ids))
    rows = db.fetch_all(
        f"""
        SELECT character_id, dominant_region_id
        FROM character_feature_windows
        WHERE character_id IN ({placeholders})
          AND window_label = %s
          AND dominant_region_id > 0
        """,
        (*character_ids, window_label),
    )
    return {int(r["character_id"]): int(r["dominant_region_id"]) for r in rows}


def _fetch_character_communities(
    db: SupplyCoreDb,
    character_ids: list[int],
) -> dict[int, int]:
    """Return community_id per character from graph intelligence."""
    if not character_ids:
        return {}
    placeholders = ",".join(["%s"] * len(character_ids))
    rows = db.fetch_all(
        f"""
        SELECT character_id, community_id
        FROM character_graph_intelligence
        WHERE character_id IN ({placeholders})
          AND community_id > 0
        """,
        tuple(character_ids),
    )
    return {int(r["character_id"]): int(r["community_id"]) for r in rows}


def _fetch_previous_signals(
    db: SupplyCoreDb,
    character_ids: list[int],
    window_label: str,
) -> dict[int, dict[str, float]]:
    """Return previous copresence signal values for delta computation."""
    if not character_ids:
        return {}
    placeholders = ",".join(["%s"] * len(character_ids))
    rows = db.fetch_all(
        f"""
        SELECT character_id, pair_frequency_delta, out_of_cluster_ratio,
               total_edge_weight, recurring_pair_count
        FROM character_copresence_signals
        WHERE character_id IN ({placeholders})
          AND window_label = %s
        """,
        (*character_ids, window_label),
    )
    return {
        int(r["character_id"]): {
            "prev_total_edge_weight": float(r.get("total_edge_weight") or 0),
            "prev_out_of_cluster_ratio": float(r.get("out_of_cluster_ratio") or 0),
            "prev_recurring_pair_count": int(r.get("recurring_pair_count") or 0),
        }
        for r in rows
    }


# ---------------------------------------------------------------------------
# Edge generation per event type
# ---------------------------------------------------------------------------

# Edge accumulator type: (char_a, char_b, window, event_type) -> {count, weight, last_event_at, system_id}
EdgeKey = tuple[int, int, str, str]
EdgeData = dict[str, Any]


def _ordered_pair(a: int, b: int) -> tuple[int, int]:
    return (a, b) if a < b else (b, a)


def _generate_battle_edges(
    participations: list[dict[str, Any]],
    window_defs: list[tuple[str, timedelta]],
    now_dt: datetime,
) -> dict[EdgeKey, EdgeData]:
    """Generate same_battle and same_side edges from battle participations."""
    edges: dict[EdgeKey, EdgeData] = defaultdict(lambda: {"count": 0, "weight": 0.0, "last_event_at": None, "system_id": None})

    # Group by battle
    battle_chars: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in participations:
        battle_chars[str(p["battle_id"])].append(p)

    for battle_id, chars in battle_chars.items():
        if len(chars) < 2:
            continue

        battle_time = chars[0].get("started_at")
        system_id = int(chars[0].get("system_id") or 0) or None

        for wlabel, wdelta in window_defs:
            cutoff = now_dt - wdelta
            if battle_time and battle_time < cutoff:
                continue

            # same_battle: all pairs
            for i, c1 in enumerate(chars):
                cid1 = int(c1["character_id"])
                for c2 in chars[i + 1:]:
                    cid2 = int(c2["character_id"])
                    a, b = _ordered_pair(cid1, cid2)

                    key_battle: EdgeKey = (a, b, wlabel, "same_battle")
                    edges[key_battle]["count"] += 1
                    edges[key_battle]["weight"] += EVENT_WEIGHTS["same_battle"]
                    edges[key_battle]["last_event_at"] = battle_time
                    edges[key_battle]["system_id"] = system_id

                    # same_side: only if on same side_key
                    if c1.get("side_key") and c1["side_key"] == c2.get("side_key"):
                        key_side: EdgeKey = (a, b, wlabel, "same_side")
                        edges[key_side]["count"] += 1
                        edges[key_side]["weight"] += EVENT_WEIGHTS["same_side"]
                        edges[key_side]["last_event_at"] = battle_time
                        edges[key_side]["system_id"] = system_id

    return edges


def _generate_system_time_edges(
    participations: list[dict[str, Any]],
    window_defs: list[tuple[str, timedelta]],
    now_dt: datetime,
) -> dict[EdgeKey, EdgeData]:
    """Generate same_system_time_window edges for characters active in the
    same system within SYSTEM_TIME_PROXIMITY_HOURS of each other, even if
    not in the same battle."""
    edges: dict[EdgeKey, EdgeData] = defaultdict(lambda: {"count": 0, "weight": 0.0, "last_event_at": None, "system_id": None})

    # Group by system
    system_events: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for p in participations:
        sid = int(p.get("system_id") or 0)
        if sid > 0 and p.get("started_at"):
            system_events[sid].append(p)

    proximity = timedelta(hours=SYSTEM_TIME_PROXIMITY_HOURS)

    for sid, events in system_events.items():
        if len(events) < 2:
            continue
        events.sort(key=lambda e: e["started_at"])

        for wlabel, wdelta in window_defs:
            cutoff = now_dt - wdelta
            win_events = [e for e in events if e["started_at"] >= cutoff]

            for i, e1 in enumerate(win_events):
                cid1 = int(e1["character_id"])
                t1 = e1["started_at"]
                for e2 in win_events[i + 1:]:
                    cid2 = int(e2["character_id"])
                    if cid1 == cid2:
                        continue
                    t2 = e2["started_at"]
                    if abs((t2 - t1).total_seconds()) > proximity.total_seconds():
                        break  # sorted, so no more matches
                    # Skip if they were already in the same battle (handled by battle edges)
                    if e1["battle_id"] == e2["battle_id"]:
                        continue

                    a, b = _ordered_pair(cid1, cid2)
                    key: EdgeKey = (a, b, wlabel, "same_system_time_window")
                    edges[key]["count"] += 1
                    edges[key]["weight"] += EVENT_WEIGHTS["same_system_time_window"]
                    edges[key]["last_event_at"] = max(t1, t2)
                    edges[key]["system_id"] = sid

    return edges


def _generate_related_engagement_edges(
    battle_edges: dict[EdgeKey, EdgeData],
) -> dict[EdgeKey, EdgeData]:
    """Generate related_engagement edges for pairs that co-occur in 2+ battles."""
    edges: dict[EdgeKey, EdgeData] = defaultdict(lambda: {"count": 0, "weight": 0.0, "last_event_at": None, "system_id": None})

    for (a, b, wlabel, etype), data in battle_edges.items():
        if etype != "same_battle":
            continue
        if data["count"] >= RELATED_ENGAGEMENT_MIN_BATTLES:
            key: EdgeKey = (a, b, wlabel, "related_engagement")
            edges[key]["count"] = data["count"]
            edges[key]["weight"] = data["count"] * EVENT_WEIGHTS["related_engagement"]
            edges[key]["last_event_at"] = data["last_event_at"]
            edges[key]["system_id"] = data["system_id"]

    return edges


def _generate_area_edges(
    character_ids: list[int],
    dominant_regions: dict[int, int],
    window_label: str,
) -> dict[EdgeKey, EdgeData]:
    """Generate same_operational_area edges for pairs sharing a dominant region."""
    edges: dict[EdgeKey, EdgeData] = defaultdict(lambda: {"count": 0, "weight": 0.0, "last_event_at": None, "system_id": None})

    # Group characters by region
    region_chars: dict[int, list[int]] = defaultdict(list)
    for cid in character_ids:
        rid = dominant_regions.get(cid)
        if rid and rid > 0:
            region_chars[rid].append(cid)

    for rid, chars in region_chars.items():
        if len(chars) < 2:
            continue
        chars.sort()
        for i, cid1 in enumerate(chars):
            for cid2 in chars[i + 1:]:
                key: EdgeKey = (cid1, cid2, window_label, "same_operational_area")
                edges[key]["count"] = 1
                edges[key]["weight"] = EVENT_WEIGHTS["same_operational_area"]

    return edges


# ---------------------------------------------------------------------------
# Detection signal computation
# ---------------------------------------------------------------------------

def _compute_detection_signals(
    edges: dict[EdgeKey, EdgeData],
    character_ids: list[int],
    communities: dict[int, int],
    prev_signals: dict[int, dict[str, float]],
    window_label: str,
) -> list[dict[str, Any]]:
    """Compute per-character detection signals from accumulated edges."""
    # Aggregate per character
    char_stats: dict[int, dict[str, Any]] = defaultdict(lambda: {
        "total_weight": 0.0,
        "unique_associates": set(),
        "recurring_pairs": 0,
        "out_of_cluster_events": 0,
        "total_events": 0,
    })

    # Count pair occurrences across all event types for recurring detection
    pair_counts: dict[tuple[int, int], int] = defaultdict(int)
    for (a, b, wl, etype), data in edges.items():
        if wl != window_label:
            continue
        pair_counts[(a, b)] += data["count"]

    for (a, b, wl, etype), data in edges.items():
        if wl != window_label:
            continue

        for cid, other in [(a, b), (b, a)]:
            if cid not in char_stats:
                char_stats[cid] = {
                    "total_weight": 0.0,
                    "unique_associates": set(),
                    "recurring_pairs": 0,
                    "out_of_cluster_events": 0,
                    "total_events": 0,
                }
            char_stats[cid]["total_weight"] += data["weight"]
            char_stats[cid]["unique_associates"].add(other)
            char_stats[cid]["total_events"] += data["count"]

            # Out-of-cluster: other character is in a different community
            cid_comm = communities.get(cid, 0)
            other_comm = communities.get(other, 0)
            if cid_comm > 0 and other_comm > 0 and cid_comm != other_comm:
                char_stats[cid]["out_of_cluster_events"] += data["count"]

    # Count recurring pairs per character
    for (a, b), count in pair_counts.items():
        if count >= RELATED_ENGAGEMENT_MIN_BATTLES:
            if a in char_stats:
                char_stats[a]["recurring_pairs"] += 1
            if b in char_stats:
                char_stats[b]["recurring_pairs"] += 1

    # Build signal rows
    signals: list[dict[str, Any]] = []
    all_weights = [s["total_weight"] for s in char_stats.values() if s["total_weight"] > 0]
    all_weights.sort()

    for cid in character_ids:
        stats = char_stats.get(cid)
        if not stats:
            continue

        total_weight = stats["total_weight"]
        unique_assoc = len(stats["unique_associates"])
        recurring = stats["recurring_pairs"]
        out_of_cluster_ratio = _safe_div(stats["out_of_cluster_events"], stats["total_events"])

        # Previous values for delta computation
        prev = prev_signals.get(cid, {})
        prev_weight = prev.get("prev_total_edge_weight", 0.0)
        prev_oc_ratio = prev.get("prev_out_of_cluster_ratio", 0.0)
        prev_recurring = prev.get("prev_recurring_pair_count", 0)

        # Pair frequency delta: how much edge weight changed
        pair_freq_delta = total_weight - prev_weight

        # Out-of-cluster ratio delta
        oc_ratio_delta = out_of_cluster_ratio - prev_oc_ratio

        # Expected-cluster decay: if recurring pairs decreased, the expected
        # cluster is decaying (old associates dropping off)
        expected_cluster_decay = max(0.0, prev_recurring - recurring) / max(1, prev_recurring) if prev_recurring > 0 else 0.0

        # Cohort percentile
        if all_weights and total_weight > 0:
            rank = sum(1 for w in all_weights if w <= total_weight)
            cohort_pct = rank / len(all_weights)
        else:
            cohort_pct = 0.0

        signals.append({
            "character_id": cid,
            "window_label": window_label,
            "pair_frequency_delta": round(pair_freq_delta, 6),
            "out_of_cluster_ratio": round(out_of_cluster_ratio, 6),
            "out_of_cluster_ratio_delta": round(oc_ratio_delta, 6),
            "expected_cluster_decay": round(expected_cluster_decay, 6),
            "total_edge_weight": round(total_weight, 6),
            "unique_associates": unique_assoc,
            "recurring_pair_count": recurring,
            "cohort_percentile": round(cohort_pct, 6),
        })

    return signals


# ---------------------------------------------------------------------------
# Neo4j sync
# ---------------------------------------------------------------------------

def _sync_edges_to_neo4j(
    neo4j: Neo4jClient,
    edges: dict[EdgeKey, EdgeData],
    computed_at: str,
    neo4j_batch: int = 200,
    neo4j_timeout: int = 30,
) -> int:
    """Push aggregated edges to Neo4j as CO_PRESENT_GENERALIZED relationships."""
    rows = []
    for (a, b, wlabel, etype), data in edges.items():
        if wlabel != "30d":
            continue  # Only push 30d window to Neo4j to keep graph manageable
        rows.append({
            "character_id_a": a,
            "character_id_b": b,
            "event_type": etype,
            "edge_weight": round(data["weight"], 6),
            "event_count": data["count"],
        })

    written = 0
    for i in range(0, len(rows), neo4j_batch):
        chunk = rows[i:i + neo4j_batch]
        neo4j.query(
            """
            UNWIND $rows AS row
            MATCH (a:Character {character_id: row.character_id_a})
            MATCH (b:Character {character_id: row.character_id_b})
            MERGE (a)-[r:CO_PRESENT_GENERALIZED {event_type: row.event_type}]->(b)
            SET r.edge_weight = toFloat(row.edge_weight),
                r.event_count = toInteger(row.event_count),
                r.computed_at = $computed_at
            """,
            {"rows": chunk, "computed_at": computed_at},
            timeout_seconds=neo4j_timeout,
        )
        written += len(chunk)

    return written


# ---------------------------------------------------------------------------
# Main job entry point
# ---------------------------------------------------------------------------

def run_compute_copresence_edges(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute and upsert generalized co-presence edges and detection signals."""
    lock_key = "compute_copresence_edges"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    neo4j_rows = 0
    computed_at = _now_sql()
    now_dt = datetime.now(UTC)

    neo4j_config = Neo4jConfig.from_runtime(neo4j_raw or {})
    neo4j_client: Neo4jClient | None = None
    if neo4j_config.enabled:
        neo4j_client = Neo4jClient(neo4j_config)

    runtime: dict[str, Any] = {}
    batch_size = max(10, min(500, int(runtime.get("copresence_batch_size") or DEFAULT_BATCH_SIZE)))
    max_batches = max(1, min(30, int(runtime.get("copresence_max_batches") or DEFAULT_MAX_BATCHES)))

    try:
        cursor = str((_sync_state_get(db, DATASET_KEY) or {}).get("last_cursor") or "")
        batch_count = 0
        last_battle_id = cursor

        # Accumulate all edges across batches for signal computation
        all_edges: dict[EdgeKey, EdgeData] = defaultdict(lambda: {"count": 0, "weight": 0.0, "last_event_at": None, "system_id": None})
        all_character_ids: set[int] = set()

        while batch_count < max_batches:
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

            participations = _fetch_battle_participants(db, battle_ids)
            rows_processed += len(participations)
            if not participations:
                _sync_state_upsert(db, DATASET_KEY, last_battle_id, "success", rows_written)
                continue

            char_ids_in_batch = list({int(p["character_id"]) for p in participations})
            all_character_ids.update(char_ids_in_batch)

            # Generate battle-based edges (same_battle + same_side)
            battle_edges = _generate_battle_edges(participations, WINDOW_DEFS, now_dt)
            for k, v in battle_edges.items():
                all_edges[k]["count"] += v["count"]
                all_edges[k]["weight"] += v["weight"]
                if v["last_event_at"]:
                    existing = all_edges[k]["last_event_at"]
                    if existing is None or v["last_event_at"] > existing:
                        all_edges[k]["last_event_at"] = v["last_event_at"]
                if v["system_id"]:
                    all_edges[k]["system_id"] = v["system_id"]

            # Generate system-time proximity edges
            sys_edges = _generate_system_time_edges(participations, WINDOW_DEFS, now_dt)
            for k, v in sys_edges.items():
                all_edges[k]["count"] += v["count"]
                all_edges[k]["weight"] += v["weight"]
                if v["last_event_at"]:
                    existing = all_edges[k]["last_event_at"]
                    if existing is None or v["last_event_at"] > existing:
                        all_edges[k]["last_event_at"] = v["last_event_at"]
                if v["system_id"]:
                    all_edges[k]["system_id"] = v["system_id"]

            _sync_state_upsert(db, DATASET_KEY, last_battle_id, "success", rows_written)

        if not all_character_ids:
            duration_ms = int((time.perf_counter() - started) * 1000)
            result = JobResult.success(
                job_key=lock_key,
                summary="No characters to process.",
                rows_processed=0,
                rows_written=0,
                duration_ms=duration_ms,
            ).to_dict()
            finish_job_run(db, job, status="success", rows_processed=0, rows_written=0, meta=result)
            return result

        char_id_list = sorted(all_character_ids)

        # Generate related_engagement edges from battle co-occurrence counts
        related_edges = _generate_related_engagement_edges(all_edges)
        for k, v in related_edges.items():
            all_edges[k] = v

        # Generate area edges per window
        for wlabel, _wdelta in WINDOW_DEFS:
            dom_regions = _fetch_character_dominant_regions(db, char_id_list, wlabel)
            area_edges = _generate_area_edges(char_id_list, dom_regions, wlabel)
            for k, v in area_edges.items():
                all_edges[k]["count"] += v["count"]
                all_edges[k]["weight"] += v["weight"]

        # Fetch community data for out-of-cluster ratio
        communities = _fetch_character_communities(db, char_id_list)

        # Upsert edges to MariaDB
        if not dry_run:
            edge_values = []
            edge_params: list[Any] = []
            for (a, b, wlabel, etype), data in all_edges.items():
                if data["count"] <= 0:
                    continue
                last_event_str = data["last_event_at"].strftime("%Y-%m-%d %H:%M:%S") if data["last_event_at"] else None
                edge_values.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s)")
                edge_params.extend([
                    a, b, wlabel, etype,
                    round(data["weight"], 6),
                    data["count"],
                    last_event_str,
                    data["system_id"],
                    computed_at,
                ])

                # Flush in batches of 500
                if len(edge_values) >= 500:
                    db.execute(
                        "INSERT INTO character_copresence_edges "
                        "(character_id_a, character_id_b, window_label, event_type, "
                        "edge_weight, event_count, last_event_at, system_id, computed_at) "
                        "VALUES " + ", ".join(edge_values) + " "
                        "ON DUPLICATE KEY UPDATE "
                        "edge_weight = VALUES(edge_weight), event_count = VALUES(event_count), "
                        "last_event_at = VALUES(last_event_at), system_id = VALUES(system_id), "
                        "computed_at = VALUES(computed_at)",
                        tuple(edge_params),
                    )
                    rows_written += len(edge_values)
                    edge_values = []
                    edge_params = []

            if edge_values:
                db.execute(
                    "INSERT INTO character_copresence_edges "
                    "(character_id_a, character_id_b, window_label, event_type, "
                    "edge_weight, event_count, last_event_at, system_id, computed_at) "
                    "VALUES " + ", ".join(edge_values) + " "
                    "ON DUPLICATE KEY UPDATE "
                    "edge_weight = VALUES(edge_weight), event_count = VALUES(event_count), "
                    "last_event_at = VALUES(last_event_at), system_id = VALUES(system_id), "
                    "computed_at = VALUES(computed_at)",
                    tuple(edge_params),
                )
                rows_written += len(edge_values)

        # Compute and write detection signals per window
        signals_written = 0
        if not dry_run:
            for wlabel, _wdelta in WINDOW_DEFS:
                prev_signals = _fetch_previous_signals(db, char_id_list, wlabel)
                signals = _compute_detection_signals(
                    all_edges, char_id_list, communities, prev_signals, wlabel,
                )

                sig_values = []
                sig_params: list[Any] = []
                for sig in signals:
                    sig_values.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                    sig_params.extend([
                        sig["character_id"],
                        sig["window_label"],
                        sig["pair_frequency_delta"],
                        sig["out_of_cluster_ratio"],
                        sig["out_of_cluster_ratio_delta"],
                        sig["expected_cluster_decay"],
                        sig["total_edge_weight"],
                        sig["unique_associates"],
                        sig["recurring_pair_count"],
                        sig["cohort_percentile"],
                        computed_at,
                    ])

                    if len(sig_values) >= 500:
                        db.execute(
                            "INSERT INTO character_copresence_signals "
                            "(character_id, window_label, pair_frequency_delta, out_of_cluster_ratio, "
                            "out_of_cluster_ratio_delta, expected_cluster_decay, total_edge_weight, "
                            "unique_associates, recurring_pair_count, cohort_percentile, computed_at) "
                            "VALUES " + ", ".join(sig_values) + " "
                            "ON DUPLICATE KEY UPDATE "
                            "pair_frequency_delta = VALUES(pair_frequency_delta), "
                            "out_of_cluster_ratio = VALUES(out_of_cluster_ratio), "
                            "out_of_cluster_ratio_delta = VALUES(out_of_cluster_ratio_delta), "
                            "expected_cluster_decay = VALUES(expected_cluster_decay), "
                            "total_edge_weight = VALUES(total_edge_weight), "
                            "unique_associates = VALUES(unique_associates), "
                            "recurring_pair_count = VALUES(recurring_pair_count), "
                            "cohort_percentile = VALUES(cohort_percentile), "
                            "computed_at = VALUES(computed_at)",
                            tuple(sig_params),
                        )
                        signals_written += len(sig_values)
                        sig_values = []
                        sig_params = []

                if sig_values:
                    db.execute(
                        "INSERT INTO character_copresence_signals "
                        "(character_id, window_label, pair_frequency_delta, out_of_cluster_ratio, "
                        "out_of_cluster_ratio_delta, expected_cluster_decay, total_edge_weight, "
                        "unique_associates, recurring_pair_count, cohort_percentile, computed_at) "
                        "VALUES " + ", ".join(sig_values) + " "
                        "ON DUPLICATE KEY UPDATE "
                        "pair_frequency_delta = VALUES(pair_frequency_delta), "
                        "out_of_cluster_ratio = VALUES(out_of_cluster_ratio), "
                        "out_of_cluster_ratio_delta = VALUES(out_of_cluster_ratio_delta), "
                        "expected_cluster_decay = VALUES(expected_cluster_decay), "
                        "total_edge_weight = VALUES(total_edge_weight), "
                        "unique_associates = VALUES(unique_associates), "
                        "recurring_pair_count = VALUES(recurring_pair_count), "
                        "cohort_percentile = VALUES(cohort_percentile), "
                        "computed_at = VALUES(computed_at)",
                        tuple(sig_params),
                    )
                    signals_written += len(sig_values)

        # Sync to Neo4j
        if neo4j_client and not dry_run:
            neo4j_rows = _sync_edges_to_neo4j(neo4j_client, all_edges, computed_at)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Computed copresence edges across {batch_count} batches: {rows_written} edge rows, {signals_written} signal rows, {neo4j_rows} neo4j edges.",
            rows_processed=rows_processed,
            rows_written=rows_written + signals_written,
            duration_ms=duration_ms,
            batches_completed=batch_count,
            meta={
                "computed_at": computed_at,
                "cursor": last_battle_id,
                "dry_run": dry_run,
                "windows": [w[0] for w in WINDOW_DEFS],
                "edge_rows": rows_written,
                "signal_rows": signals_written,
                "neo4j_rows": neo4j_rows,
                "characters_processed": len(all_character_ids),
            },
            checkpoint_before=cursor,
            checkpoint_after=last_battle_id,
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written + signals_written, meta=result)
        return result

    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise
