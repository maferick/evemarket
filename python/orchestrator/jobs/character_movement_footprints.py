"""Compute per-character movement footprints across time windows.

Analyses each character's battle participation to build a geographic
distribution (system/region), computes concentration metrics (Shannon entropy,
HHI), cross-window distribution similarity (Jensen-Shannon divergence, cosine
distance), and hostile-overlap scoring.

Derived signals written to ``character_movement_footprints``:
  - ``footprint_expansion``   — unique-systems grew vs prior snapshot
  - ``footprint_contraction`` — unique-systems shrank vs prior snapshot
  - ``new_area_entry``        — character appeared in previously-unseen regions
  - ``hostile_overlap_change``— shift in overlap with hostile-tagged systems

Normalised evidence rows are also inserted into
``character_counterintel_evidence`` so they appear in the unified evidence
breakdown on the character page.

All output is written to MariaDB.
"""

from __future__ import annotations

import bisect
import json
import math
import statistics
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

DATASET_KEY = "compute_character_movement_footprints_cursor"
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_BATCHES = 8

WINDOW_DEFS: list[tuple[str, timedelta | None]] = [
    ("7d", timedelta(days=7)),
    ("30d", timedelta(days=30)),
    ("90d", timedelta(days=90)),
    ("lifetime", None),
]

EVIDENCE_KEYS = [
    "footprint_expansion",
    "footprint_contraction",
    "new_area_entry",
    "hostile_overlap_change",
]

LOG2 = math.log(2)


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d > 0 else default


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
# Math helpers
# ---------------------------------------------------------------------------

def _shannon_entropy(counts: dict[int, int]) -> float:
    """Shannon entropy in bits over a count distribution."""
    total = sum(counts.values())
    if total <= 0:
        return 0.0
    entropy = 0.0
    for c in counts.values():
        if c > 0:
            p = c / total
            entropy -= p * math.log2(p)
    return entropy


def _hhi(counts: dict[int, int]) -> float:
    """Herfindahl-Hirschman Index (sum of squared shares, 0-1)."""
    total = sum(counts.values())
    if total <= 0:
        return 0.0
    return sum((c / total) ** 2 for c in counts.values())


def _normalize_distribution(counts: dict[int, int]) -> dict[int, float]:
    """Convert counts to a probability distribution."""
    total = sum(counts.values())
    if total <= 0:
        return {}
    return {k: v / total for k, v in counts.items()}


def _jensen_shannon_divergence(p: dict[int, float], q: dict[int, float]) -> float:
    """Jensen-Shannon divergence between two distributions (0 to 1 in bits)."""
    all_keys = set(p.keys()) | set(q.keys())
    if not all_keys:
        return 0.0
    # Build M = (P + Q) / 2
    m: dict[int, float] = {}
    for k in all_keys:
        m[k] = (p.get(k, 0.0) + q.get(k, 0.0)) / 2.0

    def _kl(dist: dict[int, float], ref: dict[int, float]) -> float:
        kl = 0.0
        for k, pk in dist.items():
            if pk > 0 and ref.get(k, 0) > 0:
                kl += pk * math.log2(pk / ref[k])
        return kl

    return (_kl(p, m) + _kl(q, m)) / 2.0


def _cosine_distance(p: dict[int, float], q: dict[int, float]) -> float:
    """Cosine distance (1 - cosine_similarity) between two distributions."""
    all_keys = set(p.keys()) | set(q.keys())
    if not all_keys:
        return 0.0
    dot = sum(p.get(k, 0.0) * q.get(k, 0.0) for k in all_keys)
    norm_p = math.sqrt(sum(v ** 2 for v in p.values()))
    norm_q = math.sqrt(sum(v ** 2 for v in q.values()))
    if norm_p == 0 or norm_q == 0:
        return 1.0
    return 1.0 - (dot / (norm_p * norm_q))


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------

def _fetch_battle_system_data(
    db: SupplyCoreDb,
    battle_ids: list[str],
) -> list[dict[str, Any]]:
    """Return participant rows with system/region info for battles."""
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
            br.started_at,
            COALESCE(rs.region_id, 0) AS region_id,
            COALESCE(rs.constellation_id, 0) AS constellation_id
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
        WHERE bp.battle_id IN ({placeholders})
          AND bp.character_id > 0
        """,
        tuple(battle_ids),
    )


def _fetch_hostile_systems(
    db: SupplyCoreDb,
    window_delta: timedelta | None,
    now_dt: datetime,
) -> set[int]:
    """Return set of system_ids where hostile-side activity was observed."""
    params: list[Any] = []
    where_cutoff = ""
    if window_delta is not None:
        cutoff = (now_dt - window_delta).strftime("%Y-%m-%d %H:%M:%S")
        where_cutoff = "AND br.started_at >= %s"
        params.append(cutoff)
    rows = db.fetch_all(
        f"""
        SELECT DISTINCT br.system_id
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        WHERE bp.side_key = 'hostile'
          AND br.system_id > 0
          {where_cutoff}
        LIMIT 10000
        """,
        tuple(params),
    )
    return {int(r["system_id"]) for r in rows}


def _fetch_prior_footprints(
    db: SupplyCoreDb,
    character_ids: list[int],
) -> dict[tuple[int, str], dict[str, Any]]:
    """Return previous footprint snapshots keyed by (character_id, window_label)."""
    if not character_ids:
        return {}
    placeholders = ",".join(["%s"] * len(character_ids))
    rows = db.fetch_all(
        f"""
        SELECT character_id, window_label,
               unique_systems_count, unique_regions_count,
               hostile_system_overlap_count, hostile_system_overlap_ratio,
               top_systems_json, top_regions_json,
               system_entropy, computed_at
        FROM character_movement_footprints
        WHERE character_id IN ({placeholders})
        """,
        tuple(character_ids),
    )
    return {
        (int(r["character_id"]), str(r["window_label"])): r
        for r in rows
    }


def _fetch_prior_distributions(
    db: SupplyCoreDb,
    character_ids: list[int],
) -> dict[tuple[int, str], dict[int, float]]:
    """Return prior system distributions keyed by (character_id, window_label)."""
    if not character_ids:
        return {}
    placeholders = ",".join(["%s"] * len(character_ids))
    rows = db.fetch_all(
        f"""
        SELECT character_id, window_label, system_id, ratio
        FROM character_system_distribution
        WHERE character_id IN ({placeholders})
        """,
        tuple(character_ids),
    )
    result: dict[tuple[int, str], dict[int, float]] = defaultdict(dict)
    for r in rows:
        key = (int(r["character_id"]), str(r["window_label"]))
        result[key][int(r["system_id"])] = float(r["ratio"])
    return dict(result)


# ---------------------------------------------------------------------------
# Cohort normalization
# ---------------------------------------------------------------------------

def _cohort_normalize(evidence_rows: list[dict[str, Any]]) -> None:
    """Enrich evidence rows in-place with cohort statistics."""
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


# ---------------------------------------------------------------------------
# Footprint computation for one character in one window
# ---------------------------------------------------------------------------

def _compute_footprint(
    participations: list[dict[str, Any]],
    window_label: str,
    window_delta: timedelta | None,
    now_dt: datetime,
    hostile_systems: set[int],
    prior_footprint: dict[str, Any] | None,
    prior_distribution: dict[int, float] | None,
    computed_at: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Compute footprint row, distribution rows, and evidence rows for one character/window."""
    if window_delta is not None:
        cutoff = now_dt - window_delta
        rows = [p for p in participations if p["started_at"] and p["started_at"] >= cutoff]
    else:
        rows = participations

    # System / region counts
    system_counts: dict[int, int] = defaultdict(int)
    region_counts: dict[int, int] = defaultdict(int)
    constellation_set: set[int] = set()

    for r in rows:
        sid = int(r.get("system_id") or 0)
        rid = int(r.get("region_id") or 0)
        cid = int(r.get("constellation_id") or 0)
        if sid:
            system_counts[sid] += 1
        if rid:
            region_counts[rid] += 1
        if cid:
            constellation_set.add(cid)

    unique_systems = len(system_counts)
    unique_regions = len(region_counts)
    unique_constellations = len(constellation_set)
    battles_in_window = len({str(r["battle_id"]) for r in rows})

    # Concentration metrics
    sys_entropy = _shannon_entropy(system_counts)
    sys_hhi = _hhi(system_counts)
    reg_entropy = _shannon_entropy(region_counts)
    reg_hhi = _hhi(region_counts)

    # Dominant system/region
    dominant_sys = max(system_counts, key=system_counts.get, default=0) if system_counts else 0
    dominant_sys_ratio = _safe_div(system_counts.get(dominant_sys, 0), battles_in_window) if battles_in_window else 0.0
    dominant_reg = max(region_counts, key=region_counts.get, default=0) if region_counts else 0
    dominant_reg_ratio = _safe_div(region_counts.get(dominant_reg, 0), battles_in_window) if battles_in_window else 0.0

    # Top systems/regions
    sorted_systems = sorted(system_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_systems = [
        {"id": sid, "count": cnt, "ratio": round(_safe_div(cnt, battles_in_window), 4)}
        for sid, cnt in sorted_systems
    ]
    sorted_regions = sorted(region_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_regions = [
        {"id": rid, "count": cnt, "ratio": round(_safe_div(cnt, battles_in_window), 4)}
        for rid, cnt in sorted_regions
    ]

    # Current distribution (normalized)
    current_dist = _normalize_distribution(system_counts)
    current_region_dist = _normalize_distribution(region_counts)

    # Cross-window similarity vs prior snapshot
    js_div_sys = None
    cos_dist_sys = None
    js_div_reg = None
    cos_dist_reg = None
    if prior_distribution and current_dist:
        js_div_sys = round(_jensen_shannon_divergence(current_dist, prior_distribution), 6)
        cos_dist_sys = round(_cosine_distance(current_dist, prior_distribution), 6)

    # Hostile overlap
    current_systems = set(system_counts.keys())
    hostile_overlap = current_systems & hostile_systems
    hostile_overlap_count = len(hostile_overlap)
    hostile_overlap_ratio = round(_safe_div(hostile_overlap_count, unique_systems), 6) if unique_systems else 0.0

    hostile_regions = set()
    for r in rows:
        sid = int(r.get("system_id") or 0)
        if sid in hostile_systems:
            rid = int(r.get("region_id") or 0)
            if rid:
                hostile_regions.add(rid)
    hostile_region_overlap_count = len(hostile_regions)
    hostile_region_overlap_ratio = round(_safe_div(hostile_region_overlap_count, unique_regions), 6) if unique_regions else 0.0

    # Derived signals vs prior
    prev_unique = int(prior_footprint.get("unique_systems_count") or 0) if prior_footprint else 0
    prev_regions = int(prior_footprint.get("unique_regions_count") or 0) if prior_footprint else 0
    prev_hostile = float(prior_footprint.get("hostile_system_overlap_ratio") or 0) if prior_footprint else 0.0

    # Expansion: new systems ratio (clamped 0-1)
    if prev_unique > 0:
        expansion_raw = max(0, unique_systems - prev_unique) / prev_unique
        contraction_raw = max(0, prev_unique - unique_systems) / prev_unique
    else:
        expansion_raw = 1.0 if unique_systems > 0 else 0.0
        contraction_raw = 0.0

    expansion_score = round(min(1.0, expansion_raw), 6)
    contraction_score = round(min(1.0, contraction_raw), 6)

    # New area entry: how many current regions were not in prior
    if prior_footprint and prior_footprint.get("top_regions_json"):
        try:
            prev_region_ids = {int(r["id"]) for r in json.loads(str(prior_footprint["top_regions_json"]))}
        except (json.JSONDecodeError, KeyError, TypeError):
            prev_region_ids = set()
    else:
        prev_region_ids = set()

    current_region_ids = set(region_counts.keys())
    new_regions = current_region_ids - prev_region_ids if prev_region_ids else set()
    new_area_score = round(min(1.0, _safe_div(len(new_regions), max(1, len(current_region_ids)))), 6) if current_region_ids else 0.0

    # Hostile overlap change
    hostile_change = abs(hostile_overlap_ratio - prev_hostile)
    hostile_change_score = round(min(1.0, hostile_change * 2.0), 6)  # amplify small changes

    prev_computed = str(prior_footprint["computed_at"]) if prior_footprint and prior_footprint.get("computed_at") else None

    footprint = {
        "window_label": window_label,
        "unique_systems_count": unique_systems,
        "unique_regions_count": unique_regions,
        "unique_constellations_count": unique_constellations,
        "battles_in_window": battles_in_window,
        "top_systems_json": json.dumps(top_systems, separators=(",", ":")),
        "top_regions_json": json.dumps(top_regions, separators=(",", ":")),
        "system_entropy": round(sys_entropy, 6),
        "system_hhi": round(sys_hhi, 6),
        "region_entropy": round(reg_entropy, 6),
        "region_hhi": round(reg_hhi, 6),
        "dominant_system_id": dominant_sys,
        "dominant_system_ratio": round(dominant_sys_ratio, 6),
        "dominant_region_id": dominant_reg,
        "dominant_region_ratio": round(dominant_reg_ratio, 6),
        "js_divergence_systems": js_div_sys,
        "cosine_distance_systems": cos_dist_sys,
        "js_divergence_regions": js_div_reg,
        "cosine_distance_regions": cos_dist_reg,
        "hostile_system_overlap_count": hostile_overlap_count,
        "hostile_system_overlap_ratio": hostile_overlap_ratio,
        "hostile_region_overlap_count": hostile_region_overlap_count,
        "hostile_region_overlap_ratio": hostile_region_overlap_ratio,
        "footprint_expansion_score": expansion_score,
        "footprint_contraction_score": contraction_score,
        "new_area_entry_score": new_area_score,
        "hostile_overlap_change_score": hostile_change_score,
        "computed_at": computed_at,
        "prev_computed_at": prev_computed,
    }

    # Distribution rows for this window
    dist_rows: list[dict[str, Any]] = []
    for sid, cnt in system_counts.items():
        dist_rows.append({
            "system_id": sid,
            "region_id": int(next((r.get("region_id") or 0 for r in rows if int(r.get("system_id") or 0) == sid), 0)),
            "battle_count": cnt,
            "ratio": round(_safe_div(cnt, battles_in_window), 6),
        })

    # Evidence rows for cohort scoring
    evidence_rows: list[dict[str, Any]] = []
    signal_map = {
        "footprint_expansion": (expansion_score, f"Footprint expanded by {expansion_score * 100:.0f}% ({prev_unique} → {unique_systems} systems)"),
        "footprint_contraction": (contraction_score, f"Footprint contracted by {contraction_score * 100:.0f}% ({prev_unique} → {unique_systems} systems)"),
        "new_area_entry": (new_area_score, f"Entered {len(new_regions)} new region(s) out of {len(current_region_ids)} active"),
        "hostile_overlap_change": (hostile_change_score, f"Hostile overlap shifted {hostile_change * 100:.1f}pp ({prev_hostile * 100:.1f}% → {hostile_overlap_ratio * 100:.1f}%)"),
    }
    for ekey, (evalue, etext) in signal_map.items():
        evidence_rows.append({
            "evidence_key": ekey,
            "window_label": window_label,
            "evidence_value": evalue,
            "evidence_text": etext,
            "evidence_payload_json": json.dumps({
                "unique_systems": unique_systems,
                "prev_unique_systems": prev_unique,
                "unique_regions": unique_regions,
                "hostile_overlap_ratio": hostile_overlap_ratio,
                "prev_hostile_overlap": prev_hostile,
                "system_entropy": round(sys_entropy, 4),
                "system_hhi": round(sys_hhi, 4),
                "js_divergence": js_div_sys,
            }, separators=(",", ":")),
        })

    return footprint, dist_rows, evidence_rows


# ---------------------------------------------------------------------------
# Main job entry point
# ---------------------------------------------------------------------------

def run_compute_character_movement_footprints(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute and upsert per-character movement footprints for all windows."""
    lock_key = "compute_character_movement_footprints"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    now_dt = datetime.now(UTC)
    runtime = runtime or {}
    batch_size = max(10, min(500, int(runtime.get("footprint_batch_size") or DEFAULT_BATCH_SIZE)))
    max_batches = max(1, min(30, int(runtime.get("footprint_max_batches") or DEFAULT_MAX_BATCHES)))

    try:
        cursor = str((_sync_state_get(db, DATASET_KEY) or {}).get("last_cursor") or "")

        # Precompute hostile systems per window
        hostile_by_window: dict[str, set[int]] = {}
        for wlabel, wdelta in WINDOW_DEFS:
            hostile_by_window[wlabel] = _fetch_hostile_systems(db, wdelta, now_dt)

        batch_count = 0
        last_battle_id = cursor
        all_evidence: list[dict[str, Any]] = []

        while batch_count < max_batches:
            battles = db.fetch_all(
                """
                SELECT br.battle_id, br.system_id, br.started_at
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

            participations = _fetch_battle_system_data(db, battle_ids)
            rows_processed += len(participations)
            if not participations:
                _sync_state_upsert(db, DATASET_KEY, last_battle_id, "success", rows_written)
                continue

            # Build per-character participation index
            char_participations: dict[int, list[dict[str, Any]]] = defaultdict(list)
            for p in participations:
                cid = int(p["character_id"])
                char_participations[cid].append(p)

            character_ids = list(char_participations.keys())

            # Fetch prior data for comparison
            prior_footprints = _fetch_prior_footprints(db, character_ids)
            prior_distributions = _fetch_prior_distributions(db, character_ids)

            if not dry_run:
                with db.transaction() as (_, cur):
                    for cid, parts in char_participations.items():
                        for wlabel, wdelta in WINDOW_DEFS:
                            prior_fp = prior_footprints.get((cid, wlabel))
                            prior_dist = prior_distributions.get((cid, wlabel))
                            hostile_sys = hostile_by_window[wlabel]

                            footprint, dist_rows, evidence_rows = _compute_footprint(
                                parts, wlabel, wdelta, now_dt,
                                hostile_sys, prior_fp, prior_dist, computed_at,
                            )

                            # Upsert footprint
                            cur.execute(
                                """
                                INSERT INTO character_movement_footprints (
                                    character_id, window_label,
                                    unique_systems_count, unique_regions_count, unique_constellations_count,
                                    battles_in_window,
                                    top_systems_json, top_regions_json,
                                    system_entropy, system_hhi, region_entropy, region_hhi,
                                    dominant_system_id, dominant_system_ratio,
                                    dominant_region_id, dominant_region_ratio,
                                    js_divergence_systems, cosine_distance_systems,
                                    js_divergence_regions, cosine_distance_regions,
                                    hostile_system_overlap_count, hostile_system_overlap_ratio,
                                    hostile_region_overlap_count, hostile_region_overlap_ratio,
                                    footprint_expansion_score, footprint_contraction_score,
                                    new_area_entry_score, hostile_overlap_change_score,
                                    computed_at, prev_computed_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s
                                )
                                ON DUPLICATE KEY UPDATE
                                    unique_systems_count = VALUES(unique_systems_count),
                                    unique_regions_count = VALUES(unique_regions_count),
                                    unique_constellations_count = VALUES(unique_constellations_count),
                                    battles_in_window = VALUES(battles_in_window),
                                    top_systems_json = VALUES(top_systems_json),
                                    top_regions_json = VALUES(top_regions_json),
                                    system_entropy = VALUES(system_entropy),
                                    system_hhi = VALUES(system_hhi),
                                    region_entropy = VALUES(region_entropy),
                                    region_hhi = VALUES(region_hhi),
                                    dominant_system_id = VALUES(dominant_system_id),
                                    dominant_system_ratio = VALUES(dominant_system_ratio),
                                    dominant_region_id = VALUES(dominant_region_id),
                                    dominant_region_ratio = VALUES(dominant_region_ratio),
                                    js_divergence_systems = VALUES(js_divergence_systems),
                                    cosine_distance_systems = VALUES(cosine_distance_systems),
                                    js_divergence_regions = VALUES(js_divergence_regions),
                                    cosine_distance_regions = VALUES(cosine_distance_regions),
                                    hostile_system_overlap_count = VALUES(hostile_system_overlap_count),
                                    hostile_system_overlap_ratio = VALUES(hostile_system_overlap_ratio),
                                    hostile_region_overlap_count = VALUES(hostile_region_overlap_count),
                                    hostile_region_overlap_ratio = VALUES(hostile_region_overlap_ratio),
                                    footprint_expansion_score = VALUES(footprint_expansion_score),
                                    footprint_contraction_score = VALUES(footprint_contraction_score),
                                    new_area_entry_score = VALUES(new_area_entry_score),
                                    hostile_overlap_change_score = VALUES(hostile_overlap_change_score),
                                    computed_at = VALUES(computed_at),
                                    prev_computed_at = VALUES(prev_computed_at)
                                """,
                                (
                                    cid,
                                    footprint["window_label"],
                                    footprint["unique_systems_count"],
                                    footprint["unique_regions_count"],
                                    footprint["unique_constellations_count"],
                                    footprint["battles_in_window"],
                                    footprint["top_systems_json"],
                                    footprint["top_regions_json"],
                                    footprint["system_entropy"],
                                    footprint["system_hhi"],
                                    footprint["region_entropy"],
                                    footprint["region_hhi"],
                                    footprint["dominant_system_id"],
                                    footprint["dominant_system_ratio"],
                                    footprint["dominant_region_id"],
                                    footprint["dominant_region_ratio"],
                                    footprint["js_divergence_systems"],
                                    footprint["cosine_distance_systems"],
                                    footprint["js_divergence_regions"],
                                    footprint["cosine_distance_regions"],
                                    footprint["hostile_system_overlap_count"],
                                    footprint["hostile_system_overlap_ratio"],
                                    footprint["hostile_region_overlap_count"],
                                    footprint["hostile_region_overlap_ratio"],
                                    footprint["footprint_expansion_score"],
                                    footprint["footprint_contraction_score"],
                                    footprint["new_area_entry_score"],
                                    footprint["hostile_overlap_change_score"],
                                    footprint["computed_at"],
                                    footprint["prev_computed_at"],
                                ),
                            )
                            rows_written += 1

                            # Upsert distribution rows
                            for dr in dist_rows:
                                cur.execute(
                                    """
                                    INSERT INTO character_system_distribution (
                                        character_id, window_label, system_id, region_id,
                                        battle_count, ratio, computed_at
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                        region_id = VALUES(region_id),
                                        battle_count = VALUES(battle_count),
                                        ratio = VALUES(ratio),
                                        computed_at = VALUES(computed_at)
                                    """,
                                    (cid, wlabel, dr["system_id"], dr["region_id"],
                                     dr["battle_count"], dr["ratio"], computed_at),
                                )

                            # Collect evidence for cohort normalization
                            for ev in evidence_rows:
                                ev["character_id"] = cid
                                all_evidence.append(ev)

            _sync_state_upsert(db, DATASET_KEY, last_battle_id, "success", rows_written)

        # Cohort-normalize all evidence and write
        if all_evidence and not dry_run:
            _cohort_normalize(all_evidence)
            with db.transaction() as (_, cur):
                for ev in all_evidence:
                    cur.execute(
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
                            ev["character_id"],
                            ev["evidence_key"],
                            ev["window_label"],
                            ev.get("evidence_value"),
                            ev.get("expected_value"),
                            ev.get("deviation_value"),
                            ev.get("z_score"),
                            ev.get("mad_score"),
                            ev.get("cohort_percentile"),
                            ev.get("confidence_flag", "low"),
                            ev["evidence_text"],
                            ev.get("evidence_payload_json"),
                            computed_at,
                        ),
                    )

            # Also update cohort z-scores on the footprint rows
            _update_cohort_scores(db, all_evidence)

        has_more = batch_count == max_batches

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Computed movement footprints across {batch_count} batches, wrote {rows_written} footprint rows and {len(all_evidence)} evidence rows.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            batches_completed=batch_count,
            has_more=has_more,
            meta={
                "computed_at": computed_at,
                "cursor": last_battle_id,
                "dry_run": dry_run,
                "windows": [w[0] for w in WINDOW_DEFS],
                "evidence_rows": len(all_evidence),
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


def _update_cohort_scores(db: SupplyCoreDb, evidence_rows: list[dict[str, Any]]) -> None:
    """Write cohort z-scores back to the footprint table for quick access."""
    updates: dict[tuple[int, str], dict[str, float]] = {}
    for ev in evidence_rows:
        cid = ev["character_id"]
        wlabel = ev["window_label"]
        key = (cid, wlabel)
        if key not in updates:
            updates[key] = {}
        ekey = ev["evidence_key"]
        if ekey == "footprint_expansion":
            updates[key]["cohort_z_footprint_size"] = ev.get("z_score", 0.0)
            updates[key]["cohort_percentile_footprint"] = ev.get("cohort_percentile", 0.0)
        elif ekey == "footprint_expansion":
            updates[key]["cohort_z_entropy"] = ev.get("z_score", 0.0)
        elif ekey == "hostile_overlap_change":
            updates[key]["cohort_z_hostile_overlap"] = ev.get("z_score", 0.0)

    if not updates:
        return

    with db.transaction() as (_, cur):
        for (cid, wlabel), scores in updates.items():
            cur.execute(
                """
                UPDATE character_movement_footprints
                SET cohort_z_footprint_size = %s,
                    cohort_z_entropy = %s,
                    cohort_z_hostile_overlap = %s,
                    cohort_percentile_footprint = %s
                WHERE character_id = %s AND window_label = %s
                """,
                (
                    scores.get("cohort_z_footprint_size"),
                    scores.get("cohort_z_entropy"),
                    scores.get("cohort_z_hostile_overlap"),
                    scores.get("cohort_percentile_footprint"),
                    cid,
                    wlabel,
                ),
            )
