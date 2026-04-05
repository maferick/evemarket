"""Shared fit clustering primitives.

Used by both `compute_economic_warfare` (opponent losses) and
`compute_auto_doctrines` (our losses). Pure functions — no DB access,
no logging side effects, no state. Caller pulls killmail rows, shapes
them, and feeds them into `cluster_fit_families`.

The algorithm is identical to the one previously embedded in
compute_economic_warfare.py: group by hull, fingerprint exact matches,
then fuzzy-merge via Jaccard similarity on the module multiset.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict

from ..eve_constants import ITEM_FLAG_CATEGORY

# Tunables — shared defaults. Callers can override per-invocation.
JACCARD_MERGE_THRESHOLD = 0.80
CORE_FREQUENCY_THRESHOLD = 0.80
CONFIDENCE_DECAY = 5.0  # 1 - exp(-count/5)

# EVE item_flag ranges that correspond to a fitted module (not cargo/drone-bay).
#   11–34  = high/mid/low/turret/launcher slots
#   92–94  = rigs
#   125–132 = T3 subsystems
FITTED_FLAG_RANGES: tuple[tuple[int, int], ...] = (
    (11, 34),
    (92, 94),
    (125, 132),
)


def fingerprint(modules: list[tuple[int, str]]) -> str:
    """MD5 of sorted ``(type_id, flag_category)`` multiset.

    Duplicates are preserved so a 5-gun fit fingerprints differently from a
    1-gun fit. Used as the stable identity for upserts.
    """
    canon = sorted(modules)
    return hashlib.md5(json.dumps(canon, separators=(",", ":")).encode()).hexdigest()


def jaccard(a: set, b: set) -> float:
    """Jaccard similarity of two sets. Empty ∩ empty returns 1.0."""
    if not a and not b:
        return 1.0
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0


def confidence(observation_count: int) -> float:
    """Exponential-decay confidence score: ``1 − exp(−n/5)``."""
    return 1.0 - math.exp(-observation_count / CONFIDENCE_DECAY)


def flag_category(item_flag: int) -> str:
    """Map raw EVE item_flag → slot category string (``'high'``, ``'rig'``, …)."""
    return ITEM_FLAG_CATEGORY.get(item_flag, "other")


def is_fitted_flag(item_flag: int) -> bool:
    """True if ``item_flag`` is any fitted-module slot (high/mid/low/rig/sub)."""
    for lo, hi in FITTED_FLAG_RANGES:
        if lo <= item_flag <= hi:
            return True
    return False


def cluster_fit_families(
    kills: dict[int, dict],
    jaccard_threshold: float = JACCARD_MERGE_THRESHOLD,
) -> list[dict]:
    """Cluster killmails into fit families per hull.

    ``kills`` shape: ``{sequence_id: {hull_type_id, alliance_id, modules: [(type_id, flag_cat)]}}``

    Each returned family has:
        hull_type_id, fingerprint, observation_count, alliance_ids,
        module_counts, module_set, first_seen, last_seen

    The algorithm runs in two passes:
      1. Group kills by hull and exact fingerprint (fast path).
      2. Fuzzy merge: while any pair of families on the same hull has
         Jaccard(module_set) ≥ threshold, merge the smaller into the larger.
    """
    by_hull: dict[int, list[dict]] = defaultdict(list)
    for seq_id, kill in kills.items():
        if kill["hull_type_id"] <= 0:
            continue
        by_hull[kill["hull_type_id"]].append({
            "sequence_id": seq_id,
            "alliance_id": kill["alliance_id"],
            "modules": kill["modules"],
            "fingerprint": fingerprint(kill["modules"]),
            "module_set": set(kill["modules"]),
        })

    families: list[dict] = []

    for hull_type_id, hull_kills in by_hull.items():
        fp_groups: dict[str, list[dict]] = defaultdict(list)
        for kill in hull_kills:
            fp_groups[kill["fingerprint"]].append(kill)

        hull_families: list[dict] = []
        for fp, group in fp_groups.items():
            alliance_ids = {k["alliance_id"] for k in group if k["alliance_id"] > 0}
            module_counts: dict[tuple[int, str], int] = defaultdict(int)
            for kill in group:
                for mod in kill["modules"]:
                    module_counts[mod] += 1
            hull_families.append({
                "hull_type_id": hull_type_id,
                "fingerprint": fp,
                "observation_count": len(group),
                "alliance_ids": alliance_ids,
                "module_counts": module_counts,
                "module_set": group[0]["module_set"],
                "first_seen": None,
                "last_seen": None,
            })

        merged = True
        while merged:
            merged = False
            hull_families.sort(key=lambda f: f["observation_count"], reverse=True)
            i = 0
            while i < len(hull_families):
                j = i + 1
                while j < len(hull_families):
                    sim = jaccard(hull_families[i]["module_set"], hull_families[j]["module_set"])
                    if sim >= jaccard_threshold:
                        target = hull_families[i]
                        source = hull_families[j]
                        target["observation_count"] += source["observation_count"]
                        target["alliance_ids"] |= source["alliance_ids"]
                        for mod, cnt in source["module_counts"].items():
                            target["module_counts"][mod] += cnt
                        target["module_set"] |= source["module_set"]
                        hull_families.pop(j)
                        merged = True
                    else:
                        j += 1
                i += 1

        families.extend(hull_families)

    return families


def canonical_module_set(
    module_counts: dict[tuple[int, str], int],
    observation_count: int,
    frequency_threshold: float = CORE_FREQUENCY_THRESHOLD,
) -> list[tuple[int, str, int, float]]:
    """Return the "core" modules appearing in ≥ ``frequency_threshold`` of cluster members.

    Each entry: ``(type_id, flag_category, quantity, observation_frequency)``.
    ``quantity`` is ``round(count / observation_count)``: if 5 kills show 5 guns
    each, you get quantity=5. Fringe modules below the threshold are excluded
    from the canonical set (but callers may still persist them at lower
    frequency for display).
    """
    if observation_count <= 0:
        return []

    # Aggregate by (type_id, flag) — count is already a per-kill occurrence tally.
    # Per-kill frequency = fraction of kills that fit this mod at least once.
    # We approximate by: occurrences / observation_count, capped at 1.0 for the
    # frequency signal and used as the quantity signal for multi-slot mods.
    result: list[tuple[int, str, int, float]] = []
    for (type_id, flag_cat), count in module_counts.items():
        freq = min(1.0, count / observation_count)
        if freq < frequency_threshold:
            continue
        # quantity: how many copies per fit on average (guns, launchers)
        quantity = max(1, round(count / observation_count))
        result.append((type_id, flag_cat, quantity, round(count / observation_count, 4)))
    result.sort()
    return result
