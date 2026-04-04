"""Lane 2: Small-engagement behavioral scoring.

Scores characters based on behavioral patterns across ALL engagements, not just
large battles.  Focuses on signals that reveal intent rather than statistical
battle-context anomalies:

- Fleet-absence ratio (small kills vs large battles)
- Post-engagement continuation rate
- Kill participation concentration (asymmetric / opportunistic kills)
- Geographic concentration
- Temporal regularity / burst signature
- Companion consistency (who they repeatedly fly with in small groups)
- Cross-side appearances in small engagements
- Asymmetry preference (only show up for easy kills?)
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


# Characters need at least this many kill participations to be scored.
MIN_KILL_PARTICIPATIONS = 3

# Large battle threshold — matches Lane 1 definition.
LARGE_BATTLE_THRESHOLD = 20

# Time window for post-engagement continuation (minutes).
CONTINUATION_WINDOW_MINUTES = 30

# Time window for "same engagement cluster" grouping (seconds).
CLUSTER_WINDOW_SECONDS = 15 * 60

# Lookback days for the scoring window.
LOOKBACK_DAYS = 90

# Behavioral score weights (sum to 1.0).
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
    """Gini coefficient for measuring concentration (0 = uniform, 1 = concentrated)."""
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


# ═══════════════════════════════════════════════════════════════════════════════
# Main pipeline
# ═══════════════════════════════════════════════════════════════════════════════


def run_compute_behavioral_scoring(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute behavioral risk scores for all characters with kill activity."""

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
        # ── Step 1: Load all kill participations ──────────────────────────
        # For each character, gather their kills: timestamp, system, victim,
        # co-attackers, alliance affiliations, and whether it was part of a
        # large battle.

        kill_rows = db.fetch_all(
            """
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
                ke.victim_ship_type_id,
                ke.zkb_total_value,
                ke.zkb_solo,
                ke.zkb_awox,
                ke.battle_id,
                COALESCE(br.participant_count, 0) AS battle_participant_count,
                COALESCE(br.eligible_for_suspicion, 0) AS eligible_for_suspicion
            FROM killmail_attackers ka
            INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
            LEFT JOIN battle_rollups br ON br.battle_id = ke.battle_id
            WHERE ka.character_id > 0
              AND ke.effective_killmail_at >= %s
            ORDER BY ka.character_id, ke.killmail_time
            """,
            (cutoff,),
        )
        rows_processed = len(kill_rows)

        # ── Step 2: Index by character ────────────────────────────────────
        by_character: dict[int, list[dict[str, Any]]] = defaultdict(list)
        # Also build a kill -> attackers index for copresence.
        kill_attackers: dict[int, list[int]] = defaultdict(list)

        for row in kill_rows:
            cid = int(row["character_id"])
            by_character[cid].append(row)
            seq = int(row["sequence_id"])
            kill_attackers[seq].append(cid)

        del kill_rows  # free memory

        # ── Step 3: Also load large-battle participation counts ───────────
        large_battle_counts: dict[int, int] = {}
        lb_rows = db.fetch_all(
            """
            SELECT bp.character_id, COUNT(DISTINCT bp.battle_id) AS cnt
            FROM battle_participants bp
            INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
            WHERE br.eligible_for_suspicion = 1
              AND br.started_at >= %s
            GROUP BY bp.character_id
            """,
            (cutoff,),
        )
        for row in lb_rows:
            large_battle_counts[int(row["character_id"])] = int(row["cnt"])
        del lb_rows

        # ── Step 4: Compute per-character behavioral signals ──────────────
        score_rows: list[dict[str, Any]] = []
        signal_rows: list[dict[str, Any]] = []
        copresence_map: dict[tuple[int, int], dict[str, Any]] = {}

        for character_id, kills in by_character.items():
            total_kill_count = len(kills)
            if total_kill_count < MIN_KILL_PARTICIPATIONS:
                continue

            large_battle_count = large_battle_counts.get(character_id, 0)
            small_kills = [k for k in kills if int(k["battle_participant_count"]) < LARGE_BATTLE_THRESHOLD]
            small_kill_count = len(small_kills)

            # ── Fleet-absence ratio ───────────────────────────────────
            # High = active in small kills, absent from large fleet ops.
            fleet_absence_ratio = _safe_div(
                float(small_kill_count),
                float(small_kill_count + large_battle_count),
            )

            # ── Post-engagement continuation rate ─────────────────────
            # After appearing on a kill, does this character continue
            # participating in nearby activity, or disappear?
            continuation_hits = 0
            continuation_eligible = 0
            sorted_kills = sorted(kills, key=lambda k: str(k["killmail_time"]))
            for i, kill in enumerate(sorted_kills[:-1]):
                t0 = kill["killmail_time"]
                if not t0:
                    continue
                if isinstance(t0, str):
                    try:
                        t0 = datetime.fromisoformat(t0)
                    except (ValueError, TypeError):
                        continue
                continuation_eligible += 1
                for j in range(i + 1, min(i + 10, len(sorted_kills))):
                    t1 = sorted_kills[j]["killmail_time"]
                    if isinstance(t1, str):
                        try:
                            t1 = datetime.fromisoformat(t1)
                        except (ValueError, TypeError):
                            break
                    diff = (t1 - t0).total_seconds()
                    if diff <= CONTINUATION_WINDOW_MINUTES * 60:
                        continuation_hits += 1
                        break
                    if diff > CONTINUATION_WINDOW_MINUTES * 60:
                        break
            post_engagement_continuation_rate = _safe_div(
                float(continuation_hits),
                float(continuation_eligible),
            )

            # ── Kill concentration score ──────────────────────────────
            # Do they only appear on high-value / highly asymmetric kills?
            attacker_counts = []
            for kill in kills:
                seq = int(kill["sequence_id"])
                attacker_counts.append(len(kill_attackers.get(seq, [])))
            avg_attackers_per_kill = statistics.mean(attacker_counts) if attacker_counts else 1.0
            # Higher score = more concentrated on easy/asymmetric kills
            # (many attackers vs few = ganking behavior)
            asymmetric_kills = sum(1 for c in attacker_counts if c >= 5)
            kill_concentration = _safe_div(float(asymmetric_kills), float(total_kill_count))

            # ── Geographic concentration ──────────────────────────────
            # How focused is their activity geographically?
            system_counts: dict[int, int] = defaultdict(int)
            for kill in kills:
                sys_id = int(kill["solar_system_id"] or 0)
                if sys_id > 0:
                    system_counts[sys_id] += 1
            if system_counts:
                geo_gini = _gini(list(system_counts.values()))
            else:
                geo_gini = 0.0

            # ── Temporal regularity ───────────────────────────────────
            # How regular/bursty is their activity?  Bursty = suspicious.
            timestamps = []
            for kill in sorted_kills:
                t = kill["killmail_time"]
                if isinstance(t, str):
                    try:
                        t = datetime.fromisoformat(t)
                    except (ValueError, TypeError):
                        continue
                if t:
                    timestamps.append(t)

            temporal_regularity = 0.0
            if len(timestamps) >= 3:
                # Burstiness index: B = (σ - μ) / (σ + μ) of inter-event intervals
                intervals = [
                    (timestamps[i + 1] - timestamps[i]).total_seconds()
                    for i in range(len(timestamps) - 1)
                    if (timestamps[i + 1] - timestamps[i]).total_seconds() > 0
                ]
                if len(intervals) >= 2:
                    mu = statistics.mean(intervals)
                    sigma = statistics.pstdev(intervals)
                    burstiness = (sigma - mu) / (sigma + mu) if (sigma + mu) > 0 else 0.0
                    # B ranges from -1 (perfectly regular) to +1 (perfectly bursty)
                    # Map to 0..1 where 1 = very bursty (suspicious)
                    temporal_regularity = _bounded((burstiness + 1.0) / 2.0)

            # ── Companion consistency ─────────────────────────────────
            # In small engagements, does this character repeatedly fly
            # with the same small group of people?
            companion_counts: dict[int, int] = defaultdict(int)
            for kill in small_kills:
                seq = int(kill["sequence_id"])
                for companion_id in kill_attackers.get(seq, []):
                    if companion_id != character_id:
                        companion_counts[companion_id] += 1

            companion_consistency = 0.0
            if companion_counts and small_kill_count >= 3:
                # What fraction of small kills share at least one recurring companion?
                recurring = {cid: cnt for cid, cnt in companion_counts.items() if cnt >= 2}
                if recurring:
                    kills_with_recurring = 0
                    for kill in small_kills:
                        seq = int(kill["sequence_id"])
                        attackers = set(kill_attackers.get(seq, []))
                        if attackers.intersection(recurring.keys()):
                            kills_with_recurring += 1
                    companion_consistency = _safe_div(
                        float(kills_with_recurring),
                        float(small_kill_count),
                    )

                    # Build copresence edges for top companions.
                    for comp_id, cnt in sorted(recurring.items(), key=lambda x: -x[1])[:20]:
                        a, b = min(character_id, comp_id), max(character_id, comp_id)
                        key = (a, b)
                        if key not in copresence_map:
                            # Find shared kills for metadata.
                            shared_victims: set[int] = set()
                            shared_systems: set[int] = set()
                            last_event = ""
                            for kill in small_kills:
                                seq = int(kill["sequence_id"])
                                if comp_id in kill_attackers.get(seq, []):
                                    v = int(kill["victim_character_id"] or 0)
                                    if v > 0:
                                        shared_victims.add(v)
                                    s = int(kill["solar_system_id"] or 0)
                                    if s > 0:
                                        shared_systems.add(s)
                                    t = str(kill["killmail_time"] or "")
                                    if t > last_event:
                                        last_event = t
                            copresence_map[key] = {
                                "character_id_a": a,
                                "character_id_b": b,
                                "co_kill_count": cnt,
                                "unique_victim_count": len(shared_victims),
                                "unique_system_count": len(shared_systems),
                                "edge_weight": round(math.log1p(cnt) * (1.0 + 0.3 * len(shared_systems)), 6),
                                "last_event_at": last_event or computed_at,
                            }
                        else:
                            existing = copresence_map[key]
                            existing["co_kill_count"] = max(existing["co_kill_count"], cnt)

            # ── Cross-side rate (small engagements) ───────────────────
            # How often does this character appear on different sides in
            # small kills?  Measured by alliance/corp of victim vs attacker.
            own_alliances: set[int] = set()
            victim_alliances_attacked: set[int] = set()
            for kill in small_kills:
                own_a = int(kill["alliance_id"] or 0)
                if own_a > 0:
                    own_alliances.add(own_a)
                victim_a = int(kill["victim_alliance_id"] or 0)
                if victim_a > 0:
                    victim_alliances_attacked.add(victim_a)
            # Cross-side = attacking your own alliance members.
            cross_side_kills = sum(
                1 for k in small_kills
                if int(k["victim_alliance_id"] or 0) > 0
                and int(k["victim_alliance_id"] or 0) in own_alliances
            )
            cross_side_small_rate = _safe_div(float(cross_side_kills), float(small_kill_count))

            # ── Asymmetry preference ──────────────────────────────────
            # Does this character only show up for extremely one-sided fights?
            asymmetry_scores = []
            for kill in small_kills:
                seq = int(kill["sequence_id"])
                n_attackers = len(kill_attackers.get(seq, []))
                # A solo kill = 1 attacker vs 1 victim = ratio 1.0
                # A 10v1 gank = ratio 10.0
                asymmetry_scores.append(float(n_attackers))
            asymmetry_preference = 0.0
            if asymmetry_scores:
                # Fraction of small kills with >= 5:1 ratio.
                high_asymmetry = sum(1 for a in asymmetry_scores if a >= 5)
                asymmetry_preference = _safe_div(float(high_asymmetry), float(len(asymmetry_scores)))

            # ── Composite score ───────────────────────────────────────
            components = {
                "fleet_absence_ratio": fleet_absence_ratio,
                "post_engagement_continuation_rate": 1.0 - post_engagement_continuation_rate,  # invert: LOW continuation = suspicious
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
                "confidence_tier": _confidence_tier(total_kill_count),
                "total_kill_count": total_kill_count,
                "small_kill_count": small_kill_count,
                "large_battle_count": large_battle_count,
                **{k: round(v, 6) for k, v in components.items()},
            })

            # ── Individual signals ────────────────────────────────────
            signal_defs = [
                ("fleet_absence_ratio", fleet_absence_ratio, f"small kills {small_kill_count}, large battles {large_battle_count}, ratio {fleet_absence_ratio:.3f}"),
                ("post_engagement_continuation", post_engagement_continuation_rate, f"continued within {CONTINUATION_WINDOW_MINUTES}m in {continuation_hits}/{continuation_eligible} cases"),
                ("kill_concentration", kill_concentration, f"{asymmetric_kills}/{total_kill_count} kills with 5+ attackers"),
                ("geographic_concentration", geo_gini, f"Gini {geo_gini:.3f} across {len(system_counts)} systems"),
                ("temporal_burstiness", temporal_regularity, f"burstiness index {temporal_regularity:.3f} from {len(timestamps)} events"),
                ("companion_consistency", companion_consistency, f"{len({c for c, n in companion_counts.items() if n >= 2})} recurring companions in {small_kill_count} small kills"),
                ("cross_side_small", cross_side_small_rate, f"{cross_side_kills}/{small_kill_count} small kills against own alliance"),
                ("asymmetry_preference", asymmetry_preference, f"preference for 5:1+ asymmetric kills: {asymmetry_preference:.3f}"),
            ]

            for sig_key, sig_value, sig_text in signal_defs:
                signal_rows.append({
                    "character_id": character_id,
                    "signal_key": sig_key,
                    "window_label": f"{lookback_days}d",
                    "signal_value": round(sig_value, 6),
                    "confidence_flag": _confidence_tier(total_kill_count),
                    "signal_text": sig_text,
                    "signal_payload_json": json_dumps_safe({
                        "total_kill_count": total_kill_count,
                        "small_kill_count": small_kill_count,
                        "large_battle_count": large_battle_count,
                        "lookback_days": lookback_days,
                    }),
                })

        # ── Step 5: Percentile ranks ──────────────────────────────────
        sorted_scores = sorted(float(r["behavioral_risk_score"]) for r in score_rows)
        for row in score_rows:
            row["percentile_rank"] = round(_percentile(sorted_scores, float(row["behavioral_risk_score"])), 6)

        # ── Step 6: Write results ─────────────────────────────────────
        if not dry_run and score_rows:
            with db.transaction() as (_, cursor):
                cursor.execute("DELETE FROM character_behavioral_scores")
                cursor.execute("DELETE FROM character_behavioral_signals")
                cursor.execute("DELETE FROM small_engagement_copresence WHERE window_label = %s", (f"{lookback_days}d",))

                for i in range(0, len(score_rows), BATCH_SIZE):
                    batch = score_rows[i:i + BATCH_SIZE]
                    cursor.executemany(
                        """
                        INSERT INTO character_behavioral_scores (
                            character_id, behavioral_risk_score, percentile_rank, confidence_tier,
                            total_kill_count, small_kill_count, large_battle_count,
                            fleet_absence_ratio, post_engagement_continuation_rate,
                            kill_concentration_score, geographic_concentration_score,
                            temporal_regularity_score, companion_consistency_score,
                            cross_side_small_rate, asymmetry_preference, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                int(r["character_id"]),
                                float(r["behavioral_risk_score"]),
                                float(r["percentile_rank"]),
                                str(r["confidence_tier"]),
                                int(r["total_kill_count"]),
                                int(r["small_kill_count"]),
                                int(r["large_battle_count"]),
                                float(r["fleet_absence_ratio"]),
                                float(r["post_engagement_continuation_rate"]),
                                float(r["kill_concentration_score"]),
                                float(r["geographic_concentration_score"]),
                                float(r["temporal_regularity_score"]),
                                float(r["companion_consistency_score"]),
                                float(r["cross_side_small_rate"]),
                                float(r["asymmetry_preference"]),
                                computed_at,
                            )
                            for r in batch
                        ],
                    )

                for i in range(0, len(signal_rows), BATCH_SIZE):
                    batch = signal_rows[i:i + BATCH_SIZE]
                    cursor.executemany(
                        """
                        INSERT INTO character_behavioral_signals (
                            character_id, signal_key, window_label, signal_value,
                            confidence_flag, signal_text, signal_payload_json, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            (
                                int(r["character_id"]),
                                str(r["signal_key"]),
                                str(r["window_label"]),
                                float(r["signal_value"]),
                                str(r["confidence_flag"]),
                                str(r["signal_text"]),
                                str(r["signal_payload_json"]),
                                computed_at,
                            )
                            for r in batch
                        ],
                    )

                copresence_rows = list(copresence_map.values())
                for i in range(0, len(copresence_rows), BATCH_SIZE):
                    batch = copresence_rows[i:i + BATCH_SIZE]
                    cursor.executemany(
                        """
                        INSERT INTO small_engagement_copresence (
                            character_id_a, character_id_b, window_label,
                            co_kill_count, unique_victim_count, unique_system_count,
                            edge_weight, last_event_at, computed_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                                int(r["character_id_a"]),
                                int(r["character_id_b"]),
                                f"{lookback_days}d",
                                int(r["co_kill_count"]),
                                int(r["unique_victim_count"]),
                                int(r["unique_system_count"]),
                                float(r["edge_weight"]),
                                str(r["last_event_at"]),
                                computed_at,
                            )
                            for r in batch
                        ],
                    )

            rows_written = len(score_rows) + len(signal_rows) + len(copresence_rows)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=f"Scored {len(score_rows)} characters ({len(copresence_map)} copresence edges) from {rows_processed} kill participations.",
            rows_processed=rows_processed,
            rows_written=0 if dry_run else rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "scored_characters": len(score_rows),
                "signal_rows": len(signal_rows),
                "copresence_edges": len(copresence_map),
                "lookback_days": lookback_days,
                "dry_run": dry_run,
            },
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written, meta=result)
        return result
    except Exception as exc:
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise
