"""Automatically detect doctrines from our own killmail losses.

Pipeline:
  1. Load fitted modules for every killmail_events row with
     mail_type='loss' within the rolling window. ``mail_type`` is set by the
     ingest-time classifier (``src/functions.php``
     ``killmail_persist_r2z2_payload``) so this already scopes to tracked
     entities.
  2. Cluster per hull by exact fingerprint + Jaccard >= 0.80 fuzzy merge
     via the shared helper in ``_fit_clustering``.
  3. Derive a stable canonical fingerprint from modules observed in
     >= ``CORE_FREQUENCY_THRESHOLD`` of cluster members and upsert the
     doctrine row. Rewrite ``auto_doctrine_modules`` for each doctrine.
  4. Repopulate ``killmail_hull_loss_1d.doctrine_fit_id`` and
     ``killmail_item_loss_1d.doctrine_fit_id`` with the best-match
     ``auto_doctrines.id`` (these columns were repurposed by the drop
     migration; they now carry auto_doctrine ids, not legacy doctrine_fit
     ids). This lets compute_auto_buyall read loss counts per doctrine.
  5. Emit per-doctrine rows into ``auto_doctrine_fit_demand_1d`` for the
     UI to render without recomputing.

No DB access or logging happens in ``_fit_clustering`` — that module stays
pure so economic_warfare keeps using the identical clustering.
"""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

from ._fit_clustering import (
    cluster_fit_families,
    fingerprint,
)

logger = logging.getLogger("supplycore.auto_doctrines")

_DEFAULT_WINDOW_DAYS = 30
_DEFAULT_MIN_LOSSES = 30
# Capitals die rarely enough that a 30/30 floor hides every real cap
# doctrine. 5 is the sensible floor for dreads, carriers, supercarriers,
# titans, and force auxiliaries — tunable via the
# ``auto_doctrines.capital_min_losses_threshold`` setting.
_DEFAULT_CAPITAL_MIN_LOSSES = 5
# 0.65 merges clusters that share roughly two-thirds of their modules.
# The original 0.80 value (inherited from the opponent-kill economic
# warfare clusterer) was too strict for our own losses: incomplete
# killmails, mid-fight refits, and meta variance were producing huge
# numbers of single-kill fragments from what were really the same
# doctrine operationally.
_DEFAULT_JACCARD = 0.65

# EVE group IDs considered "capital" for the tiered activation
# threshold: Dreadnought, Carrier, Supercarrier, Titan, Force Auxiliary.
# Rorqual / freighters are excluded from the detector entirely so they
# do not need to appear here.
_CAPITAL_HULL_GROUPS: frozenset[int] = frozenset({485, 547, 659, 30, 1538})

# Hull groups that must never appear as doctrines. The extractor SQL
# filters by this list, and ``_purge_excluded_hull_rows`` deletes any
# stale rows that leaked in from earlier detector runs before the
# filter was in place. Single source of truth so the two can't drift.
#
#     29   Capsule (pods)              31  Shuttle
#    237   Rookie ship                 28  Industrial
#    380   Deep Space Transport       513  Freighter
#    902   Jump Freighter             883  Capital Industrial Ship (Rorqual)
#    463   Mining Barge               543  Exhumer
#   1283   Expedition Frigate (Venture et al)
#    941   Industrial Command Ship (Orca, Porpoise)
_EXCLUDED_HULL_GROUPS: frozenset[int] = frozenset({
    29, 31, 237,
    28, 380, 513, 902,
    883, 941,
    463, 543, 1283,
})

_CORE_FREQUENCY_THRESHOLD = 0.80


def _load_settings(db: Any) -> dict[str, Any]:
    rows = db.fetch_all(
        """SELECT setting_key, setting_value FROM app_settings
            WHERE setting_key IN (
                'auto_doctrines.window_days',
                'auto_doctrines.min_losses_threshold',
                'auto_doctrines.capital_min_losses_threshold',
                'auto_doctrines.default_runway_days',
                'auto_doctrines.jaccard_threshold'
            )"""
    )
    by_key = {r["setting_key"]: r["setting_value"] for r in rows}

    def _int(key: str, default: int) -> int:
        try:
            return int(by_key.get(key, default))
        except (TypeError, ValueError):
            return default

    def _float(key: str, default: float) -> float:
        try:
            return float(by_key.get(key, default))
        except (TypeError, ValueError):
            return default

    return {
        "window_days": _int("auto_doctrines.window_days", _DEFAULT_WINDOW_DAYS),
        "min_losses": _int("auto_doctrines.min_losses_threshold", _DEFAULT_MIN_LOSSES),
        "capital_min_losses": _int(
            "auto_doctrines.capital_min_losses_threshold", _DEFAULT_CAPITAL_MIN_LOSSES
        ),
        "default_runway_days": _int("auto_doctrines.default_runway_days", 14),
        "jaccard_threshold": _float("auto_doctrines.jaccard_threshold", _DEFAULT_JACCARD),
    }


def _extract_our_losses(db: Any, window_days: int) -> dict[int, dict]:
    """Return ``{sequence_id: {hull_type_id, alliance_id, modules}}`` for our losses.

    Uses ``mail_type='loss'`` which is set by the ingest classifier
    (``killmail_persist_r2z2_payload``) and is authoritative for "this is
    one of ours".
    """
    # The ``item_flag`` slot ranges are authoritative for "fitted module"
    # on their own — those EVE flags map exclusively to high/mid/low/rig/
    # subsystem slots. Some ingest classifiers only ever write
    # ``'destroyed'`` / ``'dropped'`` into ``item_role`` and never tag
    # anything ``'fitted'``, which previously caused this query to return
    # zero rows even when thousands of loss killmails were in-window.
    #
    # We additionally filter to ``ref_item_types.category_id IN (7, 32)``
    # (Module + Subsystem) so that charges loaded into turret / launcher
    # hardpoints (category 8 — ammo, missiles, crystals, scripts) and any
    # stray booster / implant rows do not enter the fingerprint.
    #
    # Hull-side: exclude non-doctrine hulls via the ``_EXCLUDED_HULL_GROUPS``
    # module constant. See the constant's docstring for the full list.
    excluded_groups_sql = ",".join(str(g) for g in sorted(_EXCLUDED_HULL_GROUPS))
    sql = f"""
        SELECT ke.sequence_id, ke.victim_ship_type_id, ke.victim_alliance_id,
               ki.item_type_id, ki.item_flag
        FROM killmail_events ke
        INNER JOIN killmail_items ki ON ki.sequence_id = ke.sequence_id
        INNER JOIN ref_item_types rit ON rit.type_id = ki.item_type_id
        INNER JOIN ref_item_types hull ON hull.type_id = ke.victim_ship_type_id
        WHERE ke.mail_type = 'loss'
          AND ke.killmail_time >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
          AND (ki.item_flag BETWEEN 11 AND 34
               OR ki.item_flag BETWEEN 92 AND 94
               OR ki.item_flag BETWEEN 125 AND 132)
          AND rit.category_id IN (7, 32)
          AND hull.group_id NOT IN ({excluded_groups_sql})
        ORDER BY ke.sequence_id
    """
    # Use _fit_clustering.flag_category via local import to avoid a
    # circular on module load.
    from ._fit_clustering import flag_category

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
            kills[seq_id]["modules"].append(
                (int(row["item_type_id"]), flag_category(int(row["item_flag"] or 0)))
            )

    logger.info("auto_doctrines: extracted %d loss kills with fitted modules", len(kills))
    return kills


def _core_modules(family: dict) -> list[tuple[int, str, int, float]]:
    """Return ``[(type_id, flag_cat, quantity, frequency)]`` for modules seen in
    ≥ :data:`_CORE_FREQUENCY_THRESHOLD` of cluster members.

    ``quantity`` is the rounded average per-kill occurrence (5 guns → 5).
    ``frequency`` is capped at 1.0 per kill (share of kills that carry the
    module at all).
    """
    obs = max(1, int(family["observation_count"]))
    out: list[tuple[int, str, int, float]] = []
    for (type_id, flag_cat), count in family["module_counts"].items():
        avg_per_kill = count / obs
        frequency = min(1.0, avg_per_kill)
        if frequency < _CORE_FREQUENCY_THRESHOLD:
            continue
        quantity = max(1, round(avg_per_kill))
        out.append((int(type_id), str(flag_cat), int(quantity), round(frequency, 4)))
    out.sort()
    return out


def _canonical_fingerprint(core: list[tuple[int, str, int, float]]) -> str:
    """Deterministic fingerprint over the core module set, including quantities.

    Two clusters that resolve to the same (type_id, flag, quantity) tuples
    produce the same hash, which is what we want for upsert stability even
    when fringe cargo/spare modules wobble.
    """
    canon: list[tuple[int, str]] = []
    for type_id, flag_cat, quantity, _freq in core:
        canon.extend([(type_id, flag_cat)] * quantity)
    return fingerprint(canon)


def _hull_metadata_map(db: Any, hull_ids: set[int]) -> dict[int, dict[str, Any]]:
    """Return ``{hull_type_id: {name, group_id}}`` for the given hulls."""
    if not hull_ids:
        return {}
    placeholders = ",".join(["%s"] * len(hull_ids))
    rows = db.fetch_all(
        f"SELECT type_id, type_name, group_id FROM ref_item_types WHERE type_id IN ({placeholders})",
        tuple(hull_ids),
    )
    return {
        int(r["type_id"]): {
            "name": str(r.get("type_name") or ""),
            "group_id": int(r.get("group_id") or 0),
        }
        for r in rows
    }


def _alliance_name_map(db: Any, alliance_ids: set[int]) -> dict[int, str]:
    """Best-effort alliance id → name lookup via entity_metadata_cache.

    Falls back to ``Alliance #<id>`` for anything not yet resolved by
    the ESI metadata worker.
    """
    if not alliance_ids:
        return {}
    placeholders = ",".join(["%s"] * len(alliance_ids))
    rows = db.fetch_all(
        f"""SELECT entity_id, entity_name
              FROM entity_metadata_cache
             WHERE entity_type = 'alliance'
               AND entity_id IN ({placeholders})""",
        tuple(alliance_ids),
    )
    return {
        int(r["entity_id"]): str(r.get("entity_name") or "").strip()
        for r in rows
    }


def _canonical_name(
    hull_name: str,
    core: list[tuple[int, str, int, float]],
    alliance_label: str | None = None,
) -> str:
    """Produce a human-ish label: hull + dominant high-slot module, and
    the owning alliance's name when the cluster is fielded by exactly
    one alliance.
    """
    if not hull_name:
        hull_name = "Unknown hull"
    high_mods = [m for m in core if m[1] in {"high", "high_slot", "turret", "launcher"}]
    if high_mods:
        top = max(high_mods, key=lambda m: (m[2], m[3]))
        base = f"{hull_name} — auto (top: type {top[0]})"
    else:
        base = f"{hull_name} — auto"
    if alliance_label:
        base = f"{base} — {alliance_label}"
    return base[:191]


def _upsert_doctrines(
    db: Any,
    families: list[dict],
    now: datetime,
    window_days: int,
    min_losses: int,
    capital_min_losses: int,
) -> dict[tuple[int, str], int]:
    """Upsert every family whose core set is non-empty. Returns a map
    ``(hull_type_id, canonical_fingerprint) -> doctrine_id``.

    Rows that already exist keep their ``first_seen_at`` but receive a
    refreshed ``last_seen_at``, window counts, and activation flag. Pinned
    rows are never deactivated.

    ``capital_min_losses`` is the lower activation threshold used for
    capital hull groups (dreads, carriers, supers, titans, FAX) where
    the 30-loss subcap default hides every real cap doctrine.
    """
    hull_ids = {int(f["hull_type_id"]) for f in families}
    hull_meta = _hull_metadata_map(db, hull_ids)
    hull_names = {hid: m["name"] for hid, m in hull_meta.items()}

    # Collect every alliance id that shows up in any family so we can
    # resolve names in one round-trip. We only tag clusters with a single
    # distinct alliance, but the resolution batch includes all of them.
    single_alliance_candidates: set[int] = set()
    for family in families:
        ally_ids = {int(a) for a in family.get("alliance_ids", set()) if int(a) > 0}
        if len(ally_ids) == 1:
            single_alliance_candidates.update(ally_ids)
    alliance_names = _alliance_name_map(db, single_alliance_candidates)

    id_map: dict[tuple[int, str], int] = {}

    for family in families:
        core = _core_modules(family)
        if not core:
            continue
        canonical_fp = _canonical_fingerprint(core)
        hull_type_id = int(family["hull_type_id"])
        loss_count = int(family["observation_count"])

        # Tag the canonical name with the single fielding alliance when
        # every kill in this cluster came from the same alliance id.
        family_ally_ids = {int(a) for a in family.get("alliance_ids", set()) if int(a) > 0}
        alliance_label: str | None = None
        if len(family_ally_ids) == 1:
            (only_id,) = family_ally_ids
            name = alliance_names.get(only_id, "").strip()
            alliance_label = name or f"Alliance #{only_id}"

        canonical_name = _canonical_name(
            hull_names.get(hull_type_id, ""), core, alliance_label=alliance_label
        )

        # Tier the activation threshold: capitals use the lower
        # capital_min_losses floor because they die rarely enough that
        # the 30-loss subcap default would hide every real cap doctrine.
        hull_group_id = int(hull_meta.get(hull_type_id, {}).get("group_id") or 0)
        effective_min_losses = (
            capital_min_losses if hull_group_id in _CAPITAL_HULL_GROUPS else min_losses
        )

        existing = db.fetch_one(
            """SELECT id, loss_count_total, is_pinned
                 FROM auto_doctrines
                WHERE hull_type_id = %s AND fingerprint_hash = %s""",
            (hull_type_id, canonical_fp),
        )

        is_active = 1 if loss_count >= effective_min_losses else 0

        if existing is None:
            db.execute(
                """INSERT INTO auto_doctrines
                      (hull_type_id, fingerprint_hash, canonical_name,
                       first_seen_at, last_seen_at,
                       loss_count_window, loss_count_total,
                       window_days, min_losses_threshold,
                       is_active, is_hidden, is_pinned)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0)""",
                (
                    hull_type_id, canonical_fp, canonical_name,
                    now, now,
                    loss_count, loss_count,
                    window_days, effective_min_losses,
                    is_active,
                ),
            )
            row = db.fetch_one(
                "SELECT id FROM auto_doctrines WHERE hull_type_id = %s AND fingerprint_hash = %s",
                (hull_type_id, canonical_fp),
            )
            doctrine_id = int((row or {}).get("id") or 0)
        else:
            doctrine_id = int(existing["id"])
            pinned = bool(existing.get("is_pinned"))
            next_total = int(existing.get("loss_count_total") or 0) + loss_count
            # Pinned doctrines stay active regardless of current window.
            effective_active = 1 if (is_active or pinned) else 0
            db.execute(
                """UPDATE auto_doctrines
                      SET canonical_name = %s,
                          last_seen_at = %s,
                          loss_count_window = %s,
                          loss_count_total = %s,
                          window_days = %s,
                          min_losses_threshold = %s,
                          is_active = %s
                    WHERE id = %s""",
                (
                    canonical_name, now, loss_count, next_total,
                    window_days, effective_min_losses, effective_active, doctrine_id,
                ),
            )

        # Rewrite modules (small row count, safer than diff).
        db.execute("DELETE FROM auto_doctrine_modules WHERE doctrine_id = %s", (doctrine_id,))
        for type_id, flag_cat, quantity, frequency in core:
            db.execute(
                """INSERT INTO auto_doctrine_modules
                      (doctrine_id, type_id, flag_category, quantity, observation_frequency)
                   VALUES (%s, %s, %s, %s, %s)""",
                (doctrine_id, type_id, flag_cat, quantity, frequency),
            )

        id_map[(hull_type_id, canonical_fp)] = doctrine_id

    # Doctrines whose clusters fell out of the window this run: reset
    # window count but keep the row and any pin.
    db.execute(
        """UPDATE auto_doctrines
              SET loss_count_window = 0,
                  is_active = CASE WHEN is_pinned = 1 THEN is_active ELSE 0 END
            WHERE last_seen_at < DATE_SUB(%s, INTERVAL %s DAY)""",
        (now, window_days),
    )

    return id_map


def _purge_excluded_hull_rows(db: Any) -> int:
    """Delete ``auto_doctrines`` rows for hulls that are currently in the
    exclude list.

    The extractor SQL already filters excluded hulls out of every new
    run's input, but rows inserted by *earlier* runs (before the filter
    was tightened, or via the original migration that may have raced
    with a concurrent insert) linger forever otherwise. Running this
    self-heal at the start of every detector run keeps the table clean
    against any drift — it's idempotent and the list is small.

    Foreign keys on ``auto_doctrine_modules`` and
    ``auto_doctrine_fit_demand_1d`` cascade, so a single DELETE sweeps
    their children too.
    """
    if not _EXCLUDED_HULL_GROUPS:
        return 0
    placeholders = ",".join(["%s"] * len(_EXCLUDED_HULL_GROUPS))
    return db.execute(
        f"""DELETE ad
              FROM auto_doctrines ad
              JOIN ref_item_types rit ON rit.type_id = ad.hull_type_id
             WHERE rit.group_id IN ({placeholders})""",
        tuple(sorted(_EXCLUDED_HULL_GROUPS)),
    )


def _sweep_is_active(db: Any, min_losses: int, capital_min_losses: int) -> int:
    """Recompute ``is_active`` for every non-pinned row against the
    current thresholds.

    Without this sweep a row only gets its ``is_active`` flag updated
    when the detector's clustering pass touches it — i.e. when a
    cluster with the same ``(hull_type_id, fingerprint_hash)`` shows
    up again in the current window. Rows from earlier runs whose
    fingerprint no longer appears (because the thresholds changed,
    the fit mutated, or the hull was purged) retain their stale
    ``is_active`` value forever. The fix is a single bulk UPDATE that
    re-evaluates every non-pinned row in one shot, tiered by hull
    class.
    """
    cap_list = ",".join(str(g) for g in sorted(_CAPITAL_HULL_GROUPS))
    return db.execute(
        f"""UPDATE auto_doctrines ad
              JOIN ref_item_types rit ON rit.type_id = ad.hull_type_id
               SET ad.is_active = CASE
                   WHEN rit.group_id IN ({cap_list})
                       THEN IF(ad.loss_count_window >= %s, 1, 0)
                       ELSE IF(ad.loss_count_window >= %s, 1, 0)
               END
             WHERE ad.is_pinned = 0""",
        (capital_min_losses, min_losses),
    )


def _repopulate_loss_aggregates(db: Any) -> int:
    """Null out and repopulate ``doctrine_fit_id`` on loss rollups against the
    current active doctrine space.

    The join uses ``hull_type_id`` as the primary match — a rollup row
    only gets wired up if exactly one active doctrine exists for that
    hull. When multiple doctrines exist per hull the column stays NULL
    and the buy-all job falls back to a hull-level attribution.

    This is a pragmatic compromise: the legacy rollups don't store enough
    module fingerprint detail to reliably pick between competing fits,
    and compute_auto_buyall only needs a per-doctrine loss rate — which
    we can derive by redistributing hull totals when needed.
    """
    # The unique keys on killmail_hull_loss_1d / killmail_item_loss_1d
    # are built on GENERATED columns:
    #
    #     UNIQUE (bucket_start, hull_type_id,
    #             doctrine_fit_key    -- COALESCE(doctrine_fit_id, 0)
    #             doctrine_group_key) -- COALESCE(doctrine_group_id, 0)
    #
    # A naive ``UPDATE ... SET doctrine_fit_id = NULL`` collapses every
    # stale row for the same ``(bucket_start, hull_type_id)`` into the
    # same key (0, 0) and fails with a duplicate-key error. Instead we
    # DELETE the rows that currently hold a non-NULL attribution — the
    # analytics bucket job re-writes the canonical NULL row every run,
    # so the delete is safe: we are only removing stale carbon copies,
    # not historical data.
    db.execute(
        "DELETE FROM killmail_hull_loss_1d WHERE doctrine_fit_id IS NOT NULL OR doctrine_group_id IS NOT NULL"
    )
    db.execute(
        "DELETE FROM killmail_item_loss_1d WHERE doctrine_fit_id IS NOT NULL OR doctrine_group_id IS NOT NULL"
    )

    # Match: hulls that currently have exactly one active doctrine.
    # We can't UPDATE in place for the same reason (flipping the NULL
    # canonical row to a non-NULL id would produce a new (bucket, hull,
    # id, 0) tuple that collides with nothing, so that path IS safe,
    # but only because we just deleted the stale non-NULL rows above).
    rows = db.fetch_all(
        """SELECT hull_type_id, MIN(id) AS doctrine_id, COUNT(*) AS cnt
             FROM auto_doctrines
            WHERE is_active = 1 AND is_hidden = 0
            GROUP BY hull_type_id
           HAVING COUNT(*) = 1"""
    )
    updated = 0
    for row in rows:
        hull_type_id = int(row["hull_type_id"])
        doctrine_id = int(row["doctrine_id"])
        updated += db.execute(
            "UPDATE killmail_hull_loss_1d SET doctrine_fit_id = %s "
            "WHERE hull_type_id = %s AND doctrine_fit_id IS NULL",
            (doctrine_id, hull_type_id),
        )
        updated += db.execute(
            "UPDATE killmail_item_loss_1d SET doctrine_fit_id = %s "
            "WHERE hull_type_id = %s AND doctrine_fit_id IS NULL",
            (doctrine_id, hull_type_id),
        )
    return updated


def _upsert_daily_demand(
    db: Any,
    today: date,
    default_runway_days: int,
    window_days: int,
) -> int:
    """Write one ``auto_doctrine_fit_demand_1d`` row per active/pinned doctrine.

    Uses the per-fingerprint ``loss_count_window`` already stored on
    ``auto_doctrines`` (computed during this run's clustering pass)
    instead of re-querying ``killmail_hull_loss_1d``. The hull rollup
    is keyed by hull only, so every doctrine sharing the same hull
    would otherwise inherit the *total* hull loss rate — e.g. every
    Muninn variant ends up with the same daily rate regardless of
    fingerprint, and buy-all multiplies that inflated rate by the
    number of clusters for the hull. The clustering already produced
    the correct per-fingerprint count; trust it.
    """
    import math

    db.execute("DELETE FROM auto_doctrine_fit_demand_1d WHERE bucket_start = %s", (today,))

    doctrines = db.fetch_all(
        """SELECT id, hull_type_id, runway_days_override, is_pinned, loss_count_window
             FROM auto_doctrines
            WHERE is_hidden = 0 AND (is_active = 1 OR is_pinned = 1)"""
    )

    written = 0
    for d in doctrines:
        doctrine_id = int(d["id"])
        runway_days = int(d.get("runway_days_override") or default_runway_days)
        is_pinned = bool(d.get("is_pinned"))
        loss_count = int(d.get("loss_count_window") or 0)

        daily_loss_rate = loss_count / max(1, window_days)
        target_fits = math.ceil(daily_loss_rate * runway_days)
        if target_fits < 1 and is_pinned:
            target_fits = 1
        priority_score = round(daily_loss_rate * runway_days, 2)

        db.execute(
            """INSERT INTO auto_doctrine_fit_demand_1d
                  (bucket_start, doctrine_id, loss_count, daily_loss_rate,
                   target_fits, complete_fits_available, fit_gap, priority_score)
               VALUES (%s, %s, %s, %s, %s, 0, %s, %s)""",
            (
                today, doctrine_id, loss_count, round(daily_loss_rate, 3),
                int(target_fits), int(target_fits), priority_score,
            ),
        )
        written += 1
    return written


def run_compute_auto_doctrines(db: Any) -> dict[str, Any]:
    """Entry point — called by bin/python_compute_auto_doctrines.py."""
    started_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    t0 = time.monotonic()
    now = datetime.now(UTC).replace(microsecond=0, tzinfo=None)

    settings = _load_settings(db)

    # Self-heal: delete any stale rows for hulls that are now in the
    # exclude list. Catches rows inserted by earlier detector runs
    # before the filter was tightened.
    purged_excluded = _purge_excluded_hull_rows(db)
    if purged_excluded:
        logger.info("auto_doctrines: purged %d stale rows for excluded hull groups", purged_excluded)

    kills = _extract_our_losses(db, settings["window_days"])
    if not kills:
        return {
            "status": "success",
            "summary": "No tracked losses in window — nothing to detect.",
            "started_at": started_at,
            "rows_seen": 0,
            "rows_processed": 0,
            "rows_written": 0,
            "meta": {
                "window_days": settings["window_days"],
                "purged_excluded_hull_rows": purged_excluded,
            },
        }

    families = cluster_fit_families(kills, jaccard_threshold=settings["jaccard_threshold"])
    logger.info("auto_doctrines: %d fit families clustered from %d kills", len(families), len(kills))

    id_map = _upsert_doctrines(
        db, families, now,
        window_days=settings["window_days"],
        min_losses=settings["min_losses"],
        capital_min_losses=settings["capital_min_losses"],
    )

    # Recompute is_active for every non-pinned row against the current
    # thresholds. This catches stale ``is_active=1`` flags from earlier
    # runs where the threshold was different — without it, rows whose
    # fingerprint no longer appears in the clustering pass retain their
    # old activation state forever.
    reactivated = _sweep_is_active(
        db,
        min_losses=settings["min_losses"],
        capital_min_losses=settings["capital_min_losses"],
    )
    logger.info("auto_doctrines: is_active sweep touched %d rows", reactivated)

    _repopulate_loss_aggregates(db)

    today = (now - timedelta(hours=0)).date()
    demand_rows = _upsert_daily_demand(
        db, today,
        default_runway_days=settings["default_runway_days"],
        window_days=settings["window_days"],
    )

    elapsed_ms = int((time.monotonic() - t0) * 1000)

    return {
        "status": "success",
        "summary": f"Auto-doctrines: {len(id_map)} doctrines upserted from {len(kills)} loss kills.",
        "started_at": started_at,
        "finished_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_ms": elapsed_ms,
        "rows_seen": len(kills),
        "rows_processed": len(families),
        "rows_written": len(id_map) + demand_rows,
        "meta": {
            "window_days": settings["window_days"],
            "min_losses_threshold": settings["min_losses"],
            "capital_min_losses_threshold": settings["capital_min_losses"],
            "families_clustered": len(families),
            "doctrines_upserted": len(id_map),
            "demand_rows": demand_rows,
            "purged_excluded_hull_rows": purged_excluded,
            "is_active_sweep_rows": reactivated,
        },
    }
