"""Compute Economic Warfare scores from opponent killmail data.

Phase 1: Extract fitted modules from opponent losses (victim alliance in killmail_opponent_alliances)
Phase 2: Cluster into fit families per hull type (exact fingerprint + Jaccard merge)
Phase 3: Score modules on 5 dimensions (doctrine penetration, fit constraint,
         substitution penalty, replacement friction, loss pressure)
Phase 4: Write composite economic_warfare_score for each module
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import time
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

from ..eve_constants import (
    FITTING_CONSTRAINED_META_GROUPS,
    FITTING_VARIANT_KEYWORDS,
    ITEM_FLAG_CATEGORY,
)

logger = logging.getLogger("supplycore.economic_warfare")

# ── Scoring weights ──────────────────────────────────────────────────────────
# Fit constraint (0.30) is the dominant signal per design:
# fitting constraint matters more than price.
_W_DOCTRINE_PENETRATION = 0.15
_W_FIT_CONSTRAINT = 0.30
_W_SUBSTITUTION_PENALTY = 0.25
_W_REPLACEMENT_FRICTION = 0.20
_W_LOSS_PRESSURE = 0.10

_JACCARD_MERGE_THRESHOLD = 0.80
_CORE_FREQUENCY_THRESHOLD = 0.80
_DEFAULT_WINDOW_DAYS = 90
_CONFIDENCE_DECAY = 5.0  # 1 - exp(-count/5)


def _fingerprint(modules: list[tuple[int, str]]) -> str:
    """MD5 of sorted (type_id, flag_category) multiset — preserves duplicates for guns."""
    canon = sorted(modules)
    return hashlib.md5(json.dumps(canon, separators=(",", ":")).encode()).hexdigest()


def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0


def _confidence(observation_count: int) -> float:
    return 1.0 - math.exp(-observation_count / _CONFIDENCE_DECAY)


def _flag_category(item_flag: int) -> str:
    return ITEM_FLAG_CATEGORY.get(item_flag, "other")


def _is_fitting_variant(type_name: str, meta_group_id: int) -> bool:
    if meta_group_id in FITTING_CONSTRAINED_META_GROUPS:
        return True
    return any(kw in type_name for kw in FITTING_VARIANT_KEYWORDS)


# ── Phase 1: Extract fitted modules from opponent losses ─────────────────────

def _extract_opponent_fits(db: Any, window_days: int) -> dict[int, dict]:
    """Return {sequence_id: {hull_type_id, alliance_id, modules: [(type_id, flag_cat)]}}."""
    sql = """
        SELECT ke.sequence_id, ke.victim_ship_type_id, ke.victim_alliance_id,
               ki.item_type_id, ki.item_flag
        FROM killmail_events ke
        INNER JOIN killmail_items ki ON ki.sequence_id = ke.sequence_id
        INNER JOIN killmail_opponent_alliances koa
            ON koa.alliance_id = ke.victim_alliance_id AND koa.is_active = 1
        WHERE ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
          AND (ki.item_flag BETWEEN 11 AND 34 OR ki.item_flag BETWEEN 92 AND 94 OR ki.item_flag BETWEEN 125 AND 132)
        ORDER BY ke.sequence_id
    """
    kills: dict[int, dict] = {}
    for batch in db.iterate_batches(sql, (window_days,), batch_size=5000):
        for row in batch:
            seq_id = int(row["sequence_id"])
            if seq_id not in kills:
                kills[seq_id] = {
                    "hull_type_id": int(row["victim_ship_type_id"] or 0),
                    "alliance_id": int(row["victim_alliance_id"] or 0),
                    "modules": [],
                }
            flag_cat = _flag_category(int(row["item_flag"] or 0))
            kills[seq_id]["modules"].append((int(row["item_type_id"]), flag_cat))

    logger.info("Phase 1: extracted %d opponent kills with fitted modules", len(kills))
    return kills


# ── Phase 2: Cluster into fit families ───────────────────────────────────────

def _cluster_fit_families(kills: dict[int, dict]) -> list[dict]:
    """Group kills by hull then fingerprint, with Jaccard fuzzy merge."""
    # Group by hull
    by_hull: dict[int, list[dict]] = defaultdict(list)
    for seq_id, kill in kills.items():
        if kill["hull_type_id"] <= 0:
            continue
        by_hull[kill["hull_type_id"]].append({
            "sequence_id": seq_id,
            "alliance_id": kill["alliance_id"],
            "modules": kill["modules"],
            "fingerprint": _fingerprint(kill["modules"]),
            "module_set": set(kill["modules"]),
        })

    families: list[dict] = []

    for hull_type_id, hull_kills in by_hull.items():
        # Fast path: exact fingerprint grouping
        fp_groups: dict[str, list[dict]] = defaultdict(list)
        for kill in hull_kills:
            fp_groups[kill["fingerprint"]].append(kill)

        hull_families: list[dict] = []
        for fp, group in fp_groups.items():
            alliance_ids = {k["alliance_id"] for k in group if k["alliance_id"] > 0}
            # Count module frequencies
            module_counts: dict[tuple[int, str], int] = defaultdict(int)
            for kill in group:
                for mod in kill["modules"]:
                    module_counts[mod] += 1
            obs = len(group)
            hull_families.append({
                "hull_type_id": hull_type_id,
                "fingerprint": fp,
                "observation_count": obs,
                "alliance_ids": alliance_ids,
                "module_counts": module_counts,
                "module_set": group[0]["module_set"],
                "first_seen": None,
                "last_seen": None,
            })

        # Fuzzy merge: try to merge small families into larger ones via Jaccard
        merged = True
        while merged:
            merged = False
            hull_families.sort(key=lambda f: f["observation_count"], reverse=True)
            i = 0
            while i < len(hull_families):
                j = i + 1
                while j < len(hull_families):
                    sim = _jaccard(hull_families[i]["module_set"], hull_families[j]["module_set"])
                    if sim >= _JACCARD_MERGE_THRESHOLD:
                        # Merge j into i
                        target = hull_families[i]
                        source = hull_families[j]
                        target["observation_count"] += source["observation_count"]
                        target["alliance_ids"] |= source["alliance_ids"]
                        for mod, cnt in source["module_counts"].items():
                            target["module_counts"][mod] += cnt
                        target["module_set"] |= source["module_set"]
                        # Recompute fingerprint based on dominant family
                        hull_families.pop(j)
                        merged = True
                    else:
                        j += 1
                i += 1

        families.extend(hull_families)

    logger.info("Phase 2: clustered into %d fit families across %d hulls",
                len(families), len(by_hull))
    return families


# ── Phase 3: Score modules ───────────────────────────────────────────────────

def _score_modules(
    db: Any,
    families: list[dict],
    window_days: int,
) -> list[dict]:
    """Compute 5 sub-scores + composite for each module appearing in hostile families."""
    if not families:
        return []

    # Collect all unique type_ids from families
    all_type_ids: set[int] = set()
    for fam in families:
        for (type_id, _flag_cat) in fam["module_counts"]:
            all_type_ids.add(type_id)

    if not all_type_ids:
        return []

    # Load item reference data
    type_id_list = list(all_type_ids)
    placeholders = ",".join(["%s"] * len(type_id_list))
    ref_rows = db.fetch_all(
        f"SELECT type_id, type_name, group_id, meta_group_id FROM ref_item_types WHERE type_id IN ({placeholders})",
        tuple(type_id_list),
    )
    ref_map: dict[int, dict] = {int(r["type_id"]): r for r in ref_rows}

    # Count substitutes per group_id (viable alternatives at same/lower meta_group)
    group_ids = {int(r.get("group_id") or 0) for r in ref_rows if int(r.get("group_id") or 0) > 0}
    group_substitute_count: dict[int, int] = {}
    if group_ids:
        gp = ",".join(["%s"] * len(group_ids))
        sub_rows = db.fetch_all(
            f"SELECT group_id, COUNT(*) AS cnt FROM ref_item_types WHERE group_id IN ({gp}) AND published = 1 GROUP BY group_id",
            tuple(group_ids),
        )
        for sr in sub_rows:
            group_substitute_count[int(sr["group_id"])] = int(sr["cnt"])

    # Load market data for replacement friction
    market_data: dict[int, dict] = {}
    if type_id_list:
        mp = ",".join(["%s"] * len(type_id_list))
        market_rows = db.fetch_all(
            f"""SELECT type_id, best_sell_price, total_sell_volume
                FROM market_order_snapshots_summary
                WHERE type_id IN ({mp})
                  AND source_type = 'market_hub'
                  AND observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                ORDER BY observed_at DESC""",
            tuple(type_id_list),
        )
        for mr in market_rows:
            tid = int(mr["type_id"])
            if tid not in market_data:
                market_data[tid] = {
                    "sell_price": float(mr.get("best_sell_price") or 0),
                    "sell_volume": int(mr.get("total_sell_volume") or 0),
                }

    # Load loss pressure (30d destroyed count from killmail_items)
    loss_counts: dict[int, int] = {}
    if type_id_list:
        lp = ",".join(["%s"] * len(type_id_list))
        loss_rows = db.fetch_all(
            f"""SELECT ki.item_type_id, COUNT(*) AS cnt
                FROM killmail_items ki
                INNER JOIN killmail_events ke ON ke.sequence_id = ki.sequence_id
                INNER JOIN killmail_opponent_alliances koa
                    ON koa.alliance_id = ke.victim_alliance_id AND koa.is_active = 1
                WHERE ki.item_type_id IN ({lp})
                  AND ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)
                  AND (ki.item_flag BETWEEN 11 AND 34 OR ki.item_flag BETWEEN 92 AND 94 OR ki.item_flag BETWEEN 125 AND 132)
                GROUP BY ki.item_type_id""",
            tuple(type_id_list),
        )
        for lr in loss_rows:
            loss_counts[int(lr["item_type_id"])] = int(lr["cnt"])

    # Pre-compute per-module family membership
    total_families = len(families)
    total_alliances = len({aid for fam in families for aid in fam["alliance_ids"]})

    # Per hull: how many families exist, and which modules appear in how many
    hull_family_count: dict[int, int] = defaultdict(int)
    for fam in families:
        hull_family_count[fam["hull_type_id"]] += 1

    # Per module: which families use it, which alliances use it
    module_family_ids: dict[int, set[int]] = defaultdict(set)
    module_alliance_ids: dict[int, set[int]] = defaultdict(set)
    # Per (module, hull): family count for cross-fit persistence
    module_hull_families: dict[tuple[int, int], int] = defaultdict(int)

    for fam_idx, fam in enumerate(families):
        hull = fam["hull_type_id"]
        for (type_id, _flag_cat) in fam["module_counts"]:
            module_family_ids[type_id].add(fam_idx)
            module_alliance_ids[type_id] |= fam["alliance_ids"]
            module_hull_families[(type_id, hull)] += 1

    # Score each module
    scored: list[dict] = []
    now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    for type_id in all_type_ids:
        ref = ref_map.get(type_id, {})
        type_name = str(ref.get("type_name") or "")
        group_id = int(ref.get("group_id") or 0)
        meta_group_id = int(ref.get("meta_group_id") or 0)

        # (a) Doctrine penetration
        fam_count = len(module_family_ids.get(type_id, set()))
        alliance_count = len(module_alliance_ids.get(type_id, set()))
        family_ratio = fam_count / total_families if total_families > 0 else 0
        alliance_ratio = alliance_count / total_alliances if total_alliances > 0 else 0
        doctrine_penetration = 0.6 * family_ratio + 0.4 * alliance_ratio

        # (b) Fit constraint
        is_fv = _is_fitting_variant(type_name, meta_group_id)
        # Cross-fit persistence: across all hulls this module appears in,
        # what fraction of families for that hull include it?
        cross_fit_values = []
        for (tid, hull), hfc in module_hull_families.items():
            if tid == type_id:
                total_for_hull = hull_family_count.get(hull, 1)
                cross_fit_values.append(hfc / total_for_hull)
        cross_fit_persistence = max(cross_fit_values) if cross_fit_values else 0.0
        fit_constraint = 0.4 * (1.0 if is_fv else 0.0) + 0.6 * cross_fit_persistence

        # (c) Substitution penalty
        subs = group_substitute_count.get(group_id, 0)
        substitution_penalty = 1.0 - min(1.0, subs / 8.0)

        # (d) Replacement friction
        mkt = market_data.get(type_id)
        if mkt is not None:
            vol = mkt["sell_volume"]
            price = mkt["sell_price"]
            volume_thinness = 1.0 - min(1.0, vol / 1000.0)
            price_factor = min(1.0, math.log10(max(price, 1.0)) / 10.0)
            replacement_friction = 0.7 * volume_thinness + 0.3 * price_factor
        else:
            replacement_friction = 0.8  # absence = scarcity signal

        # (e) Loss pressure
        destroyed_30d = loss_counts.get(type_id, 0)
        attrition_per_day = destroyed_30d / 30.0
        loss_pressure = min(1.0, attrition_per_day / 10.0)

        # (f) Composite
        ew_score = (
            _W_DOCTRINE_PENETRATION * doctrine_penetration
            + _W_FIT_CONSTRAINT * fit_constraint
            + _W_SUBSTITUTION_PENALTY * substitution_penalty
            + _W_REPLACEMENT_FRICTION * replacement_friction
            + _W_LOSS_PRESSURE * loss_pressure
        )

        scored.append({
            "type_id": type_id,
            "type_name": type_name,
            "group_id": group_id,
            "meta_group_id": meta_group_id,
            "doctrine_penetration_score": round(doctrine_penetration, 4),
            "fit_constraint_score": round(fit_constraint, 4),
            "substitution_penalty_score": round(substitution_penalty, 4),
            "replacement_friction_score": round(replacement_friction, 4),
            "loss_pressure_score": round(loss_pressure, 4),
            "economic_warfare_score": round(ew_score, 4),
            "hostile_family_count": fam_count,
            "hostile_alliance_count": alliance_count,
            "total_destroyed_30d": destroyed_30d,
            "cross_fit_persistence": round(cross_fit_persistence, 4),
            "is_fitting_variant": 1 if is_fv else 0,
            "substitute_count": subs,
            "computed_at": now_str,
        })

    scored.sort(key=lambda s: s["economic_warfare_score"], reverse=True)
    logger.info("Phase 3: scored %d unique modules", len(scored))
    return scored


# ── Phase 4: Write results ───────────────────────────────────────────────────

def _write_families(db: Any, families: list[dict]) -> int:
    """UPSERT hostile_fit_families and hostile_fit_family_modules."""
    now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    rows_written = 0

    with db.transaction() as (_, cursor):
        for fam in families:
            hull = fam["hull_type_id"]
            fp = fam["fingerprint"]
            obs = fam["observation_count"]
            conf = round(_confidence(obs), 4)
            alliance_json = json.dumps(sorted(fam["alliance_ids"]))
            module_set_json = json.dumps([
                {"type_id": tid, "flag_category": fc, "count": cnt}
                for (tid, fc), cnt in sorted(fam["module_counts"].items())
            ])

            cursor.execute(
                """INSERT INTO hostile_fit_families
                   (hull_type_id, family_fingerprint, module_set_json, observation_count,
                    confidence, first_seen, last_seen, alliance_ids_json, computed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                       module_set_json = VALUES(module_set_json),
                       observation_count = VALUES(observation_count),
                       confidence = VALUES(confidence),
                       last_seen = VALUES(last_seen),
                       alliance_ids_json = VALUES(alliance_ids_json),
                       computed_at = VALUES(computed_at)""",
                (hull, fp, module_set_json, obs, conf, now_str, now_str, alliance_json, now_str),
            )
            rows_written += 1

            # Get family ID for module rows
            cursor.execute(
                "SELECT id FROM hostile_fit_families WHERE hull_type_id = %s AND family_fingerprint = %s LIMIT 1",
                (hull, fp),
            )
            fam_row = cursor.fetchone()
            if not fam_row:
                continue
            family_id = int(fam_row["id"])

            # Write per-module membership
            for (type_id, flag_cat), cnt in fam["module_counts"].items():
                freq = round(cnt / obs, 4) if obs > 0 else 1.0
                is_core = 1 if freq >= _CORE_FREQUENCY_THRESHOLD else 0
                cursor.execute(
                    """INSERT INTO hostile_fit_family_modules
                       (family_id, item_type_id, flag_category, frequency, is_core)
                       VALUES (%s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                           frequency = VALUES(frequency),
                           is_core = VALUES(is_core)""",
                    (family_id, type_id, flag_cat, freq, is_core),
                )
                rows_written += 1

    return rows_written


def _write_scores(db: Any, scored: list[dict]) -> int:
    """REPLACE INTO economic_warfare_scores."""
    if not scored:
        return 0

    rows_written = 0
    with db.transaction() as (_, cursor):
        for s in scored:
            cursor.execute(
                """REPLACE INTO economic_warfare_scores
                   (type_id, type_name, group_id, meta_group_id,
                    doctrine_penetration_score, fit_constraint_score,
                    substitution_penalty_score, replacement_friction_score,
                    loss_pressure_score, economic_warfare_score,
                    hostile_family_count, hostile_alliance_count,
                    total_destroyed_30d, cross_fit_persistence,
                    is_fitting_variant, substitute_count, computed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    s["type_id"], s["type_name"], s["group_id"], s["meta_group_id"],
                    s["doctrine_penetration_score"], s["fit_constraint_score"],
                    s["substitution_penalty_score"], s["replacement_friction_score"],
                    s["loss_pressure_score"], s["economic_warfare_score"],
                    s["hostile_family_count"], s["hostile_alliance_count"],
                    s["total_destroyed_30d"], s["cross_fit_persistence"],
                    s["is_fitting_variant"], s["substitute_count"], s["computed_at"],
                ),
            )
            rows_written += 1

    return rows_written


# ── Entry point ──────────────────────────────────────────────────────────────

def run_compute_economic_warfare(db: Any, influx_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    """Main entry point for the economic warfare compute job."""
    started_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    t0 = time.monotonic()

    window_days = _DEFAULT_WINDOW_DAYS

    # Phase 1: Extract
    kills = _extract_opponent_fits(db, window_days)
    if not kills:
        return {
            "status": "success",
            "summary": "No opponent kills found — nothing to score.",
            "started_at": started_at,
            "rows_seen": 0,
            "rows_processed": 0,
            "rows_written": 0,
        }

    # Phase 2: Cluster
    families = _cluster_fit_families(kills)

    # Phase 3: Score
    scored = _score_modules(db, families, window_days)

    # Phase 4: Write
    family_rows = _write_families(db, families)
    score_rows = _write_scores(db, scored)

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    return {
        "status": "success",
        "summary": f"Economic warfare: {len(families)} fit families, {len(scored)} modules scored.",
        "started_at": started_at,
        "finished_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_ms": elapsed_ms,
        "rows_seen": len(kills),
        "rows_processed": len(kills),
        "rows_written": family_rows + score_rows,
        "meta": {
            "window_days": window_days,
            "kills_analyzed": len(kills),
            "fit_families": len(families),
            "modules_scored": len(scored),
            "family_rows_written": family_rows,
            "score_rows_written": score_rows,
            "top_5": [
                {"type_id": s["type_id"], "type_name": s["type_name"], "score": s["economic_warfare_score"]}
                for s in scored[:5]
            ],
        },
    }
