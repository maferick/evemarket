"""Lane 2: Small-engagement behavioral scoring.

Engagement tiers
----------------
  Solo / gank   1–4   participants  → behavioral signals only
  Small gang    5–19  participants  → behavioral signals + light statistical context
  Full battle   20+   participants  → covered by Lane 1 (counterintel / suspicion pipeline)
                                      matches MIN_ELIGIBLE_PARTICIPANTS in battle_intelligence.py

For each character we compute 8 behavioral signals from solo/gang activity and
store them separately from Lane 1 scores.  A blended headline is produced by
db_character_blended_intelligence() in PHP, not here.
"""

from __future__ import annotations

import bisect
import math
import statistics
import time
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..job_utils import finish_job_run, start_job_run


# ── Engagement tier thresholds ────────────────────────────────────────────────
TIER_SOLO_MAX = 4      # 1–4   participants: behavioral only
TIER_GANG_MAX = 19     # 5–19  participants: behavioral + light statistical
TIER_BATTLE_MIN = 20   # 20+   participants: full Lane 1 battle model (matches MIN_ELIGIBLE_PARTICIPANTS)

# Minimum kill participations to produce a score.
MIN_KILL_PARTICIPATIONS = 3

# Post-engagement continuation window (minutes).
CONTINUATION_WINDOW_MINUTES = 30

# Lookback window for scoring.
LOOKBACK_DAYS = 90

# Behavioral signal weights (must sum to 1.0).
# Gang-tier weight is slightly lower on companion/cross-side since context is
# larger, but still behaviorally meaningful.
BEHAVIORAL_WEIGHTS: dict[str, float] = {
    "fleet_absence_ratio": 0.15,
    "post_engagement_continuation_rate": 0.10,
    "kill_concentration_score": 0.10,
    "geographic_concentration_score": 0.10,
    "temporal_regularity_score": 0.10,
    "companion_consistency_score": 0.20,
    "cross_side_small_rate": 0.15,
    "asymmetry_preference": 0.10,
}

# Gang tier (5-9) contribution to behavioral signals is discounted slightly
# because the context is richer than pure solo/gank.
GANG_TIER_WEIGHT = 0.7  # multiply gang-kill contribution by this factor

BATCH_SIZE = 5000


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d > 0 else default


def _bounded(value: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, value))


def _confidence_tier(kill_count: int) -> str:
    if kill_count >= 30:
        return "high"
    if kill_count >= 10:
        return "medium"
    return "low"


def _percentile(sorted_vals: list[float], value: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    return bisect.bisect_right(sorted_vals, value) / n


def _gini(values: list[float]) -> float:
    """Gini coefficient for concentration (0 = uniform, 1 = concentrated)."""
    n = len(values)
    if n <= 1 or sum(values) == 0:
        return 0.0
    sorted_v = sorted(values)
    total = sum(sorted_v)
    cum = 0.0
    area = 0.0
    for i, v in enumerate(sorted_v):
        cum += v
        area += cum / total - (i + 1) / n
    return _bounded(2.0 * area / n * n / (n - 1))


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    try:
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run_compute_behavioral_scoring(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute Lane 2 behavioral risk scores from all killmail activity."""

    lock_key = "compute_behavioral_scoring"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    runtime = runtime or {}
    lookback_days = max(30, int(runtime.get("behavioral_lookback_days") or LOOKBACK_DAYS))
    cutoff = (datetime.now(UTC) - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")

    try:
        # ── Step 1a: Build kill→attacker index for small engagements only ────
        # Only small-engagement kills are needed for companion/copresence signals.
        # Using iterate_batches avoids loading the full attacker table into memory.
        kill_attackers: dict[int, list[int]] = defaultdict(list)
        for batch in db.iterate_batches(
            """
            SELECT ka.sequence_id, ka.character_id
            FROM killmail_attackers ka
            INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
            LEFT JOIN battle_rollups br ON br.battle_id = ke.battle_id
            WHERE ka.character_id > 0
              AND ke.effective_killmail_at >= %s
              AND COALESCE(br.participant_count, 0) <= %s
            """,
            (cutoff, TIER_GANG_MAX),
            batch_size=10_000,
        ):
            for row in batch:
                kill_attackers[int(row["sequence_id"])].append(int(row["character_id"]))

        # ── Step 1b: Lane 1 large-battle counts per character ────────────────
        # fetch_all is fine here — one aggregated row per character, not raw kills.
        large_battle_counts: dict[int, int] = {}
        for row in db.fetch_all(
            """
            SELECT bp.character_id, COUNT(DISTINCT bp.battle_id) AS cnt
            FROM battle_participants bp
            INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
            WHERE br.participant_count >= %s
              AND br.started_at >= %s
            GROUP BY bp.character_id
            """,
            (TIER_BATTLE_MIN, cutoff),
        ):
            large_battle_counts[int(row["character_id"])] = int(row["cnt"])

        # ── Step 2: Per-character behavioral computation (streaming) ─────────
        # The query is ordered by character_id so we can process each character
        # as its rows arrive without holding all kill participations in memory.
        score_rows: list[dict[str, Any]] = []
        signal_rows: list[dict[str, Any]] = []
        copresence_map: dict[tuple[int, int], dict[str, Any]] = {}

        _KILL_SQL = """
            SELECT
                ka.character_id,
                ka.corporation_id,
                ka.alliance_id,
                ka.damage_done,
                ka.final_blow,
                ka.ship_type_id,
                ke.sequence_id,
                ke.killmail_time,
                ke.solar_system_id,
                ke.region_id,
                ke.victim_character_id,
                ke.victim_alliance_id,
                ke.victim_corporation_id,
                ke.zkb_total_value,
                ke.battle_id,
                COALESCE(br.participant_count, 0) AS battle_participant_count
            FROM killmail_attackers ka
            INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
            LEFT JOIN battle_rollups br ON br.battle_id = ke.battle_id
            WHERE ka.character_id > 0
              AND ke.effective_killmail_at >= %s
            ORDER BY ka.character_id, ke.killmail_time
            """

        def _flush_character(character_id: int, kills: list[dict[str, Any]]) -> None:
            """Score one character and append to score_rows/signal_rows/copresence_map."""
            total_kill_count = len(kills)
            if total_kill_count < MIN_KILL_PARTICIPATIONS:
                return

            large_battle_count = large_battle_counts.get(character_id, 0)

            # Classify kills into tiers by participant count.
            solo_kills = [k for k in kills if int(k["battle_participant_count"]) <= TIER_SOLO_MAX]
            gang_kills  = [k for k in kills if TIER_SOLO_MAX < int(k["battle_participant_count"]) <= TIER_GANG_MAX]
            # Kills in large battles (10+) are Lane 1 territory; we track them
            # only for the fleet-absence ratio.
            small_kills = solo_kills + gang_kills  # both sub-battle tiers
            solo_kill_count = len(solo_kills)
            gang_kill_count  = len(gang_kills)
            small_kill_count = len(small_kills)

            # ── Fleet-absence ratio ───────────────────────────────────────────
            # High = operates mostly in sub-battle engagements vs fleet ops.
            fleet_absence_ratio = _safe_div(
                float(small_kill_count),
                float(small_kill_count + large_battle_count),
            )

            # ── Post-engagement continuation rate ─────────────────────────────
            # After a kill, does this character stay engaged or vanish?
            # Measured across all kills (solo + gang + battle) to capture the
            # "one precise kill then gone" pattern.
            sorted_kills = sorted(kills, key=lambda k: str(k["killmail_time"]))
            continuation_hits = 0
            continuation_eligible = 0
            for i, kill in enumerate(sorted_kills[:-1]):
                t0 = _parse_dt(kill["killmail_time"])
                if t0 is None:
                    continue
                continuation_eligible += 1
                for j in range(i + 1, min(i + 10, len(sorted_kills))):
                    t1 = _parse_dt(sorted_kills[j]["killmail_time"])
                    if t1 is None:
                        break
                    diff = (t1 - t0).total_seconds()
                    if diff <= CONTINUATION_WINDOW_MINUTES * 60:
                        continuation_hits += 1
                        break
                    if diff > CONTINUATION_WINDOW_MINUTES * 60:
                        break
            post_engagement_continuation_rate = _safe_div(float(continuation_hits), float(continuation_eligible))

            # ── Kill concentration (asymmetric fight preference) ───────────────
            # Fraction of ALL kills where there are 5+ attackers.  Gang kills
            # are expected to have more attackers so they contribute less to this
            # signal — only solo-tier is fully suspicious for asymmetry.
            attacker_counts_solo = [len(kill_attackers.get(int(k["sequence_id"]), [])) for k in solo_kills]
            attacker_counts_gang = [len(kill_attackers.get(int(k["sequence_id"]), [])) for k in gang_kills]
            asymmetric_solo = sum(1 for c in attacker_counts_solo if c >= 5)
            # For gangs (5-9 participants), a 5:1 attacker ratio is less noteworthy —
            # use a higher threshold (all attackers, i.e., fully one-sided).
            asymmetric_gang = sum(1 for c in attacker_counts_gang if c >= 8)
            kill_concentration = _safe_div(
                float(asymmetric_solo + asymmetric_gang * GANG_TIER_WEIGHT),
                float(max(1, solo_kill_count + gang_kill_count)),
            )

            # ── Geographic concentration ──────────────────────────────────────
            # Computed from solo + gang kills only (not large battles).
            system_counts: dict[int, int] = defaultdict(int)
            for kill in small_kills:
                sys_id = int(kill["solar_system_id"] or 0)
                if sys_id > 0:
                    system_counts[sys_id] += 1
            geo_gini = _gini(list(system_counts.values())) if system_counts else 0.0

            # ── Temporal burstiness ───────────────────────────────────────────
            # Computed from all kills to capture overall activity rhythm.
            timestamps = [_parse_dt(k["killmail_time"]) for k in sorted_kills]
            timestamps = [t for t in timestamps if t is not None]
            temporal_regularity = 0.0
            if len(timestamps) >= 3:
                intervals = [
                    (timestamps[i + 1] - timestamps[i]).total_seconds()
                    for i in range(len(timestamps) - 1)
                    if (timestamps[i + 1] - timestamps[i]).total_seconds() > 0
                ]
                if len(intervals) >= 2:
                    mu = statistics.mean(intervals)
                    sigma = statistics.pstdev(intervals)
                    burstiness = (sigma - mu) / (sigma + mu) if (sigma + mu) > 0 else 0.0
                    temporal_regularity = _bounded((burstiness + 1.0) / 2.0)

            # ── Companion consistency ─────────────────────────────────────────
            # Solo-tier companions are fully weighted; gang-tier companions are
            # discounted because larger groups naturally have more co-fliers.
            companion_counts: dict[int, float] = defaultdict(float)
            for kill in solo_kills:
                for comp in kill_attackers.get(int(kill["sequence_id"]), []):
                    if comp != character_id:
                        companion_counts[comp] += 1.0
            for kill in gang_kills:
                for comp in kill_attackers.get(int(kill["sequence_id"]), []):
                    if comp != character_id:
                        companion_counts[comp] += GANG_TIER_WEIGHT

            companion_consistency = 0.0
            if companion_counts and small_kill_count >= 3:
                recurring = {cid: w for cid, w in companion_counts.items() if w >= 2.0}
                if recurring:
                    kills_with_recurring = 0
                    for kill in small_kills:
                        attackers = set(kill_attackers.get(int(kill["sequence_id"]), []))
                        if attackers.intersection(recurring.keys()):
                            kills_with_recurring += 1
                    companion_consistency = _safe_div(float(kills_with_recurring), float(small_kill_count))

                    # Build copresence edges (top 20 recurring companions).
                    for comp_id, weight in sorted(recurring.items(), key=lambda x: -x[1])[:20]:
                        a, b = min(character_id, comp_id), max(character_id, comp_id)
                        key = (a, b)
                        shared_victims: set[int] = set()
                        shared_systems: set[int] = set()
                        last_event = ""
                        for kill in small_kills:
                            if comp_id in kill_attackers.get(int(kill["sequence_id"]), []):
                                v = int(kill["victim_character_id"] or 0)
                                if v > 0:
                                    shared_victims.add(v)
                                s = int(kill["solar_system_id"] or 0)
                                if s > 0:
                                    shared_systems.add(s)
                                t = str(kill["killmail_time"] or "")
                                if t > last_event:
                                    last_event = t
                        edge = {
                            "character_id_a": a,
                            "character_id_b": b,
                            "co_kill_count": int(weight + 0.5),
                            "unique_victim_count": len(shared_victims),
                            "unique_system_count": len(shared_systems),
                            "edge_weight": round(math.log1p(weight) * (1.0 + 0.3 * len(shared_systems)), 6),
                            "last_event_at": last_event or computed_at,
                        }
                        if key not in copresence_map or copresence_map[key]["edge_weight"] < edge["edge_weight"]:
                            copresence_map[key] = edge

            # ── Cross-side rate (solo + gang only) ────────────────────────────
            # Attacking your own alliance in small engagements.
            own_alliances: set[int] = set()
            for kill in small_kills:
                a = int(kill["alliance_id"] or 0)
                if a > 0:
                    own_alliances.add(a)
            cross_side_kills = sum(
                1 for k in small_kills
                if int(k["victim_alliance_id"] or 0) > 0
                and int(k["victim_alliance_id"] or 0) in own_alliances
            )
            cross_side_small_rate = _safe_div(float(cross_side_kills), float(small_kill_count))

            # ── Asymmetry preference (solo tier only) ─────────────────────────
            # Only meaningful in 1-4 participant context.
            asymmetry_preference = 0.0
            if attacker_counts_solo:
                high_asym = sum(1 for c in attacker_counts_solo if c >= 5)
                asymmetry_preference = _safe_div(float(high_asym), float(len(attacker_counts_solo)))

            # ── Composite behavioral risk score ───────────────────────────────
            components = {
                "fleet_absence_ratio": fleet_absence_ratio,
                "post_engagement_continuation_rate": 1.0 - post_engagement_continuation_rate,
                "kill_concentration_score": kill_concentration,
                "geographic_concentration_score": geo_gini,
                "temporal_regularity_score": temporal_regularity,
                "companion_consistency_score": companion_consistency,
                "cross_side_small_rate": cross_side_small_rate,
                "asymmetry_preference": asymmetry_preference,
            }
            behavioral_risk_score = sum(
                BEHAVIORAL_WEIGHTS[k] * _bounded(v) for k, v in components.items()
            )

            score_rows.append({
                "character_id": character_id,
                "behavioral_risk_score": round(behavioral_risk_score, 6),
                "confidence_tier": _confidence_tier(small_kill_count),
                "total_kill_count": total_kill_count,
                "solo_kill_count": solo_kill_count,
                "gang_kill_count": gang_kill_count,
                "large_battle_count": large_battle_count,
                **{k: round(v, 6) for k, v in components.items()},
            })

            # ── Individual signals ────────────────────────────────────────────
            recurring_companion_count = len({c for c, w in companion_counts.items() if w >= 2.0})
            for sig_key, sig_value, sig_text in [
                ("fleet_absence_ratio", fleet_absence_ratio,
                 f"{solo_kill_count} solo/gank + {gang_kill_count} gang kills vs {large_battle_count} large battles"),
                ("post_engagement_continuation", post_engagement_continuation_rate,
                 f"continued within {CONTINUATION_WINDOW_MINUTES}m in {continuation_hits}/{continuation_eligible} cases"),
                ("kill_concentration", kill_concentration,
                 f"{asymmetric_solo} solo asymmetric, {asymmetric_gang} gang one-sided of {total_kill_count} kills"),
                ("geographic_concentration", geo_gini,
                 f"Gini {geo_gini:.3f} across {len(system_counts)} systems (small engagements)"),
                ("temporal_burstiness", temporal_regularity,
                 f"burstiness {temporal_regularity:.3f} from {len(timestamps)} events"),
                ("companion_consistency", companion_consistency,
                 f"{recurring_companion_count} recurring companions across {small_kill_count} sub-battle kills"),
                ("cross_side_small", cross_side_small_rate,
                 f"{cross_side_kills}/{small_kill_count} small kills against own alliance"),
                ("asymmetry_preference", asymmetry_preference,
                 f"solo-tier 5:1+ gank preference: {asymmetry_preference:.3f}"),
            ]:
                signal_rows.append({
                    "character_id": character_id,
                    "signal_key": sig_key,
                    "window_label": f"{lookback_days}d",
                    "signal_value": round(sig_value, 6),
                    "confidence_flag": _confidence_tier(small_kill_count),
                    "signal_text": sig_text,
                    "signal_payload_json": json_dumps_safe({
                        "solo_kill_count": solo_kill_count,
                        "gang_kill_count": gang_kill_count,
                        "large_battle_count": large_battle_count,
                        "lookback_days": lookback_days,
                        "tier_solo_max": TIER_SOLO_MAX,
                        "tier_gang_max": TIER_GANG_MAX,
                        "tier_battle_min": TIER_BATTLE_MIN,
                    }),
                })

        # ── Stream all kill participations and flush per character ────────────
        _current_cid: int | None = None
        _current_kills: list[dict[str, Any]] = []
        for batch in db.iterate_batches(_KILL_SQL, (cutoff,), batch_size=5_000):
            for row in batch:
                rows_processed += 1
                cid = int(row["character_id"])
                if cid != _current_cid:
                    if _current_cid is not None:
                        _flush_character(_current_cid, _current_kills)
                    _current_cid = cid
                    _current_kills = []
                _current_kills.append(row)
        if _current_cid is not None:
            _flush_character(_current_cid, _current_kills)

        sorted_scores = sorted(float(r["behavioral_risk_score"]) for r in score_rows)
        for row in score_rows:
            row["percentile_rank"] = round(_percentile(sorted_scores, float(row["behavioral_risk_score"])), 6)

        # ── Step 3: Write ─────────────────────────────────────────────────────
        if not dry_run and score_rows:
            with db.transaction() as (_, cursor):
                # Ensure table has the expected schema (columns may be missing
                # if the table was created before the full migration ran).
                _expected_cols = {
                    "solo_kill_count", "gang_kill_count", "large_battle_count",
                }
                cursor.execute(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'character_behavioral_scores'"
                )
                existing_cols = {row["COLUMN_NAME"] for row in cursor.fetchall()}
                missing = _expected_cols - existing_cols
                if missing:
                    for col in sorted(missing):
                        cursor.execute(
                            f"ALTER TABLE character_behavioral_scores "
                            f"ADD COLUMN `{col}` INT UNSIGNED NOT NULL DEFAULT 0"
                        )

                cursor.execute("DELETE FROM character_behavioral_scores")
                cursor.execute("DELETE FROM character_behavioral_signals")
                cursor.execute("DELETE FROM small_engagement_copresence WHERE window_label = %s", (f"{lookback_days}d",))

                for i in range(0, len(score_rows), BATCH_SIZE):
                    cursor.executemany(
                        """
                        INSERT INTO character_behavioral_scores (
                            character_id, behavioral_risk_score, percentile_rank, confidence_tier,
                            total_kill_count, solo_kill_count, gang_kill_count, large_battle_count,
                            fleet_absence_ratio, post_engagement_continuation_rate,
                            kill_concentration_score, geographic_concentration_score,
                            temporal_regularity_score, companion_consistency_score,
                            cross_side_small_rate, asymmetry_preference, computed_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        [
                            (
                                int(r["character_id"]), float(r["behavioral_risk_score"]),
                                float(r["percentile_rank"]), str(r["confidence_tier"]),
                                int(r["total_kill_count"]), int(r["solo_kill_count"]),
                                int(r["gang_kill_count"]), int(r["large_battle_count"]),
                                float(r["fleet_absence_ratio"]),
                                float(r["post_engagement_continuation_rate"]),
                                float(r["kill_concentration_score"]),
                                float(r["geographic_concentration_score"]),
                                float(r["temporal_regularity_score"]),
                                float(r["companion_consistency_score"]),
                                float(r["cross_side_small_rate"]),
                                float(r["asymmetry_preference"]), computed_at,
                            )
                            for r in score_rows[i:i + BATCH_SIZE]
                        ],
                    )

                for i in range(0, len(signal_rows), BATCH_SIZE):
                    cursor.executemany(
                        """
                        INSERT INTO character_behavioral_signals (
                            character_id, signal_key, window_label, signal_value,
                            confidence_flag, signal_text, signal_payload_json, computed_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        """,
                        [
                            (
                                int(r["character_id"]), str(r["signal_key"]),
                                str(r["window_label"]), float(r["signal_value"]),
                                str(r["confidence_flag"]), str(r["signal_text"]),
                                str(r["signal_payload_json"]), computed_at,
                            )
                            for r in signal_rows[i:i + BATCH_SIZE]
                        ],
                    )

                copresence_rows = list(copresence_map.values())
                for i in range(0, len(copresence_rows), BATCH_SIZE):
                    cursor.executemany(
                        """
                        INSERT INTO small_engagement_copresence (
                            character_id_a, character_id_b, window_label,
                            co_kill_count, unique_victim_count, unique_system_count,
                            edge_weight, last_event_at, computed_at
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON DUPLICATE KEY UPDATE
                            co_kill_count = VALUES(co_kill_count),
                            unique_victim_count = VALUES(unique_victim_count),
                            unique_system_count = VALUES(unique_system_count),
                            edge_weight = VALUES(edge_weight),
                            last_event_at = VALUES(last_event_at),
                            computed_at = VALUES(computed_at)
                        """,
                        [
                            (
                                int(r["character_id_a"]), int(r["character_id_b"]),
                                f"{lookback_days}d", int(r["co_kill_count"]),
                                int(r["unique_victim_count"]), int(r["unique_system_count"]),
                                float(r["edge_weight"]), str(r["last_event_at"]), computed_at,
                            )
                            for r in copresence_rows[i:i + BATCH_SIZE]
                        ],
                    )
            rows_written = len(score_rows) + len(signal_rows) + len(copresence_rows)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=(
                f"Scored {len(score_rows)} characters "
                f"({len(copresence_map)} copresence edges) "
                f"from {rows_processed} kill participations. "
                f"Tiers: solo ≤{TIER_SOLO_MAX}, gang {TIER_SOLO_MAX+1}–{TIER_GANG_MAX}, battle ≥{TIER_BATTLE_MIN}."
            ),
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "scored_characters": len(score_rows),
                "signal_rows": len(signal_rows),
                "copresence_edges": len(copresence_map),
                "lookback_days": lookback_days,
                "tier_thresholds": {
                    "solo_max": TIER_SOLO_MAX,
                    "gang_max": TIER_GANG_MAX,
                    "battle_min": TIER_BATTLE_MIN,
                },
                "dry_run": dry_run,
            },
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written, meta=result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise
