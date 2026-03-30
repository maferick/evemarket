"""Temporal behavior detection — hour/day cadence shift analysis.

Reads ``character_feature_histograms`` (populated by
``compute_character_feature_windows``) and ``killmail_events`` to build
per-character temporal profiles and detect drift between recent and baseline
windows.

Signals emitted into ``character_counterintel_evidence``:
  * ``active_hour_shift``         — peak-hour migration between windows
  * ``weekday_profile_shift``     — weekday distribution divergence
  * ``cadence_burstiness``        — inter-event timing irregularity
  * ``reactivation_after_dormancy`` — activity spike after a quiet period

Statistical methods:
  * Rolling z-score and MAD score (via shared ``_cohort_normalize``)
  * CUSUM drift detection when sample count is sufficient

All output is written to MariaDB.  Neo4j is updated when enabled to tag
characters with temporal-anomaly labels.
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
from ..neo4j import Neo4jClient, Neo4jConfig

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MIN_SAMPLE_TOTAL = 8          # minimum killmail events for any signal
MIN_SAMPLE_DRIFT = 15         # minimum for CUSUM drift detection
DORMANCY_THRESHOLD_DAYS = 30  # gap ≥ this triggers dormancy check
CUSUM_THRESHOLD = 4.0         # cumulative sum alarm threshold
NEO4J_BATCH_SIZE = 500


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d > 0 else default


# ---------------------------------------------------------------------------
# Cohort normalisation (same algorithm as counterintel_pipeline)
# ---------------------------------------------------------------------------

def _cohort_normalize(evidence_rows: list[dict[str, Any]]) -> None:
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
# Histogram helpers
# ---------------------------------------------------------------------------

def _histogram_peak(hist: dict[str, int]) -> int:
    """Return the bin key with the highest count."""
    if not hist:
        return 0
    return int(max(hist, key=lambda k: hist[k]))


def _histogram_total(hist: dict[str, int]) -> int:
    return sum(hist.values())


def _histogram_to_distribution(hist: dict[str, int], bins: int) -> list[float]:
    """Normalise a sparse histogram dict to a fixed-length probability vector."""
    total = _histogram_total(hist)
    if total == 0:
        return [0.0] * bins
    return [hist.get(str(i), 0) / total for i in range(bins)]


def _jensen_shannon_divergence(p: list[float], q: list[float]) -> float:
    """Symmetric divergence between two distributions (0 = identical, 1 = max)."""
    n = len(p)
    m = [(p[i] + q[i]) / 2.0 for i in range(n)]
    kl_pm = sum(p[i] * math.log2(p[i] / m[i]) if p[i] > 0 and m[i] > 0 else 0.0 for i in range(n))
    kl_qm = sum(q[i] * math.log2(q[i] / m[i]) if q[i] > 0 and m[i] > 0 else 0.0 for i in range(n))
    return max(0.0, min(1.0, (kl_pm + kl_qm) / 2.0))


# ---------------------------------------------------------------------------
# Inter-event cadence metrics
# ---------------------------------------------------------------------------

def _compute_cadence_metrics(timestamps: list[datetime]) -> dict[str, float]:
    """Compute burstiness and quiet-period metrics from sorted event timestamps."""
    if len(timestamps) < 2:
        return {"burstiness": 0.0, "max_gap_hours": 0.0, "mean_gap_hours": 0.0, "cv": 0.0}

    gaps_hours = []
    for i in range(1, len(timestamps)):
        delta = (timestamps[i] - timestamps[i - 1]).total_seconds() / 3600.0
        if delta >= 0:
            gaps_hours.append(delta)

    if not gaps_hours:
        return {"burstiness": 0.0, "max_gap_hours": 0.0, "mean_gap_hours": 0.0, "cv": 0.0}

    mean_gap = statistics.mean(gaps_hours)
    std_gap = statistics.pstdev(gaps_hours) if len(gaps_hours) > 1 else 0.0
    cv = _safe_div(std_gap, mean_gap)

    # Burstiness index B = (σ - μ) / (σ + μ), range [-1, 1]
    # B > 0 = bursty, B < 0 = periodic, B ≈ 0 = Poisson
    burstiness = _safe_div(std_gap - mean_gap, std_gap + mean_gap)

    return {
        "burstiness": round(burstiness, 6),
        "max_gap_hours": round(max(gaps_hours), 2),
        "mean_gap_hours": round(mean_gap, 2),
        "cv": round(cv, 6),
    }


# ---------------------------------------------------------------------------
# CUSUM drift detector
# ---------------------------------------------------------------------------

def _cusum_drift(values: list[float], threshold: float = CUSUM_THRESHOLD) -> dict[str, Any]:
    """One-sided CUSUM on a sequence of values.  Returns max cumulative sum and
    whether the alarm threshold was breached."""
    if len(values) < MIN_SAMPLE_DRIFT:
        return {"cusum_max": 0.0, "alarm": False, "sample_count": len(values)}

    mean = statistics.mean(values)
    s_pos = 0.0
    s_neg = 0.0
    max_pos = 0.0
    max_neg = 0.0
    for v in values:
        s_pos = max(0.0, s_pos + (v - mean))
        s_neg = max(0.0, s_neg - (v - mean))
        max_pos = max(max_pos, s_pos)
        max_neg = max(max_neg, s_neg)

    cusum_max = max(max_pos, max_neg)
    return {
        "cusum_max": round(cusum_max, 6),
        "alarm": cusum_max >= threshold,
        "sample_count": len(values),
    }


# ---------------------------------------------------------------------------
# Dormancy / reactivation
# ---------------------------------------------------------------------------

def _detect_reactivation(
    timestamps: list[datetime],
    dormancy_days: int = DORMANCY_THRESHOLD_DAYS,
    recent_days: int = 7,
) -> dict[str, Any]:
    """Check if a character went dormant then recently became active again."""
    if len(timestamps) < 3:
        return {"reactivated": False, "dormancy_hours": 0.0, "recent_burst_count": 0}

    now = datetime.now(UTC)
    recent_cutoff = now - timedelta(days=recent_days)
    dormancy_hours_threshold = dormancy_days * 24.0

    # Find the longest gap before the recent window
    pre_recent = [t for t in timestamps if t < recent_cutoff]
    recent_events = [t for t in timestamps if t >= recent_cutoff]

    if not pre_recent or not recent_events:
        return {"reactivated": False, "dormancy_hours": 0.0, "recent_burst_count": len(recent_events)}

    # Gap between last pre-recent event and first recent event
    last_pre = max(pre_recent)
    first_recent = min(recent_events)
    gap_hours = (first_recent - last_pre).total_seconds() / 3600.0

    return {
        "reactivated": gap_hours >= dormancy_hours_threshold,
        "dormancy_hours": round(gap_hours, 2),
        "recent_burst_count": len(recent_events),
    }


# ---------------------------------------------------------------------------
# Data fetch helpers
# ---------------------------------------------------------------------------

def _fetch_histograms(db: SupplyCoreDb) -> dict[int, dict[str, dict[str, Any]]]:
    """Load all character_feature_histograms rows keyed by character_id → window_label."""
    rows = db.fetch_all(
        """
        SELECT character_id, window_label, hour_histogram, weekday_histogram
        FROM character_feature_histograms
        """
    )
    result: dict[int, dict[str, dict[str, Any]]] = defaultdict(dict)
    for r in rows:
        cid = int(r["character_id"])
        wl = str(r["window_label"])
        hour_raw = r.get("hour_histogram") or "{}"
        weekday_raw = r.get("weekday_histogram") or "{}"
        hour_hist = json.loads(hour_raw) if isinstance(hour_raw, str) else hour_raw
        weekday_hist = json.loads(weekday_raw) if isinstance(weekday_raw, str) else weekday_raw
        result[cid][wl] = {"hour": hour_hist, "weekday": weekday_hist}
    return dict(result)


def _fetch_killmail_timestamps(db: SupplyCoreDb) -> dict[int, list[datetime]]:
    """Fetch per-character sorted killmail timestamps from killmail_events + killmail_attackers.

    Includes both victim events and attacker participation.
    """
    # Victim timestamps
    victim_rows = db.fetch_all(
        """
        SELECT victim_character_id AS character_id, effective_killmail_at AS ts
        FROM killmail_events
        WHERE victim_character_id IS NOT NULL
          AND victim_character_id > 0
          AND effective_killmail_at IS NOT NULL
        """
    )

    # Attacker timestamps (join back to killmail_events for the timestamp)
    attacker_rows = db.fetch_all(
        """
        SELECT ka.character_id, ke.effective_killmail_at AS ts
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        WHERE ka.character_id IS NOT NULL
          AND ka.character_id > 0
          AND ke.effective_killmail_at IS NOT NULL
        """
    )

    result: dict[int, list[datetime]] = defaultdict(list)
    for r in victim_rows:
        cid = int(r["character_id"])
        ts = r["ts"]
        if isinstance(ts, datetime):
            result[cid].append(ts)
    for r in attacker_rows:
        cid = int(r["character_id"])
        ts = r["ts"]
        if isinstance(ts, datetime):
            result[cid].append(ts)

    # Sort per character
    for cid in result:
        result[cid].sort()

    return dict(result)


def _fetch_battle_timestamps(db: SupplyCoreDb) -> dict[int, list[datetime]]:
    """Fetch per-character battle participation timestamps from battle_participants."""
    rows = db.fetch_all(
        """
        SELECT bp.character_id, br.started_at AS ts
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        WHERE bp.character_id > 0
          AND br.started_at IS NOT NULL
        """
    )
    result: dict[int, list[datetime]] = defaultdict(list)
    for r in rows:
        cid = int(r["character_id"])
        ts = r["ts"]
        if isinstance(ts, datetime):
            result[cid].append(ts)
    for cid in result:
        result[cid].sort()
    return dict(result)


# ---------------------------------------------------------------------------
# Neo4j temporal anomaly tagging
# ---------------------------------------------------------------------------

def _neo4j_tag_temporal_anomalies(
    neo4j: Neo4jClient,
    character_signals: dict[int, dict[str, float]],
) -> int:
    """Tag characters in Neo4j with temporal anomaly labels."""
    tagged = 0
    batch: list[dict[str, Any]] = []
    for cid, signals in character_signals.items():
        batch.append({
            "character_id": cid,
            "active_hour_shift": signals.get("active_hour_shift", 0.0),
            "weekday_profile_shift": signals.get("weekday_profile_shift", 0.0),
            "cadence_burstiness": signals.get("cadence_burstiness", 0.0),
            "reactivation_after_dormancy": signals.get("reactivation_after_dormancy", 0.0),
        })
        if len(batch) >= NEO4J_BATCH_SIZE:
            _neo4j_write_batch(neo4j, batch)
            tagged += len(batch)
            batch = []
    if batch:
        _neo4j_write_batch(neo4j, batch)
        tagged += len(batch)
    return tagged


def _neo4j_write_batch(neo4j: Neo4jClient, batch: list[dict[str, Any]]) -> None:
    neo4j.run(
        """
        UNWIND $rows AS row
        MATCH (c:Character {character_id: row.character_id})
        SET c.active_hour_shift = row.active_hour_shift,
            c.weekday_profile_shift = row.weekday_profile_shift,
            c.cadence_burstiness = row.cadence_burstiness,
            c.reactivation_after_dormancy = row.reactivation_after_dormancy,
            c.temporal_anomaly_computed_at = datetime()
        WITH c, row
        WHERE row.active_hour_shift > 0.3
           OR row.weekday_profile_shift > 0.3
           OR row.cadence_burstiness > 0.5
           OR row.reactivation_after_dormancy > 0.5
        SET c:TemporalAnomaly
        """,
        {"rows": batch},
    )


# ---------------------------------------------------------------------------
# Main job entry point
# ---------------------------------------------------------------------------

def run_temporal_behavior_detection(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Compute temporal behavior signals and emit to character_counterintel_evidence."""
    lock_key = "temporal_behavior_detection"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    rows_processed = 0
    rows_written = 0
    computed_at = _now_sql()
    runtime = runtime or {}

    try:
        # ── 1. Load histogram data (already computed by character_feature_windows) ──
        histograms = _fetch_histograms(db)

        # ── 2. Load raw killmail timestamps for cadence analysis ──
        killmail_ts = _fetch_killmail_timestamps(db)

        # ── 3. Load battle participation timestamps ──
        battle_ts = _fetch_battle_timestamps(db)

        # Merge all timestamps per character
        all_characters = set(histograms.keys()) | set(killmail_ts.keys()) | set(battle_ts.keys())
        merged_ts: dict[int, list[datetime]] = {}
        for cid in all_characters:
            combined = killmail_ts.get(cid, []) + battle_ts.get(cid, [])
            combined.sort()
            # Deduplicate within 60s
            deduped: list[datetime] = []
            for ts in combined:
                if not deduped or (ts - deduped[-1]).total_seconds() > 60:
                    deduped.append(ts)
            merged_ts[cid] = deduped

        rows_processed = len(all_characters)
        evidence_rows: list[dict[str, Any]] = []
        neo4j_signals: dict[int, dict[str, float]] = {}

        # ── 4. Compute signals per character ──
        for cid in all_characters:
            char_hist = histograms.get(cid, {})
            char_ts = merged_ts.get(cid, [])
            event_count = len(char_ts)

            # Skip characters with insufficient data
            if event_count < MIN_SAMPLE_TOTAL:
                continue

            signals: dict[str, float] = {}

            # ── 4a. Active hour shift (7d vs 90d or lifetime) ──
            recent_hist = char_hist.get("7d", {})
            baseline_hist = char_hist.get("90d", char_hist.get("lifetime", {}))

            recent_hour = recent_hist.get("hour", {})
            baseline_hour = baseline_hist.get("hour", {})
            recent_hour_total = _histogram_total(recent_hour)
            baseline_hour_total = _histogram_total(baseline_hour)

            if recent_hour_total >= 3 and baseline_hour_total >= MIN_SAMPLE_TOTAL:
                recent_dist = _histogram_to_distribution(recent_hour, 24)
                baseline_dist = _histogram_to_distribution(baseline_hour, 24)
                hour_jsd = _jensen_shannon_divergence(recent_dist, baseline_dist)
                peak_recent = _histogram_peak(recent_hour)
                peak_baseline = _histogram_peak(baseline_hour)
                peak_shift_hours = min(abs(peak_recent - peak_baseline), 24 - abs(peak_recent - peak_baseline))

                # CUSUM on hourly distribution sequence (recent window values)
                recent_hour_values = [recent_hour.get(str(h), 0) for h in range(24)]
                baseline_hour_values = [baseline_hour.get(str(h), 0) for h in range(24)]
                cusum = _cusum_drift(recent_hour_values)

                active_hour_shift = round(hour_jsd, 6)
                signals["active_hour_shift"] = active_hour_shift

                confidence = "high" if event_count >= 30 else ("medium" if event_count >= 15 else "low")
                evidence_rows.append({
                    "character_id": cid,
                    "evidence_key": "active_hour_shift",
                    "window_label": "7d_vs_90d",
                    "evidence_value": active_hour_shift,
                    "evidence_text": f"hour-of-day distribution divergence {active_hour_shift:.3f} (JSD), peak shifted {peak_shift_hours}h ({peak_baseline}:00→{peak_recent}:00)",
                    "evidence_payload_json": json_dumps_safe({
                        "jsd": active_hour_shift,
                        "peak_recent": peak_recent,
                        "peak_baseline": peak_baseline,
                        "peak_shift_hours": peak_shift_hours,
                        "recent_total": recent_hour_total,
                        "baseline_total": baseline_hour_total,
                        "cusum": cusum,
                        "recent_distribution": recent_dist,
                        "baseline_distribution": baseline_dist,
                    }),
                    "confidence_flag": confidence,
                })

            # ── 4b. Weekday profile shift (7d vs 90d or lifetime) ──
            recent_dow = recent_hist.get("weekday", {})
            baseline_dow = baseline_hist.get("weekday", {})
            recent_dow_total = _histogram_total(recent_dow)
            baseline_dow_total = _histogram_total(baseline_dow)

            if recent_dow_total >= 3 and baseline_dow_total >= MIN_SAMPLE_TOTAL:
                recent_dow_dist = _histogram_to_distribution(recent_dow, 7)
                baseline_dow_dist = _histogram_to_distribution(baseline_dow, 7)
                dow_jsd = _jensen_shannon_divergence(recent_dow_dist, baseline_dow_dist)

                # CUSUM on weekday counts
                recent_dow_values = [recent_dow.get(str(d), 0) for d in range(7)]
                cusum_dow = _cusum_drift(recent_dow_values)

                weekday_shift = round(dow_jsd, 6)
                signals["weekday_profile_shift"] = weekday_shift

                confidence = "high" if event_count >= 30 else ("medium" if event_count >= 15 else "low")
                evidence_rows.append({
                    "character_id": cid,
                    "evidence_key": "weekday_profile_shift",
                    "window_label": "7d_vs_90d",
                    "evidence_value": weekday_shift,
                    "evidence_text": f"weekday distribution divergence {weekday_shift:.3f} (JSD) across {recent_dow_total} recent vs {baseline_dow_total} baseline events",
                    "evidence_payload_json": json_dumps_safe({
                        "jsd": weekday_shift,
                        "recent_total": recent_dow_total,
                        "baseline_total": baseline_dow_total,
                        "cusum": cusum_dow,
                        "recent_distribution": recent_dow_dist,
                        "baseline_distribution": baseline_dow_dist,
                    }),
                    "confidence_flag": confidence,
                })

            # ── 4c. Cadence burstiness ──
            cadence = _compute_cadence_metrics(char_ts)
            burstiness = cadence["burstiness"]
            # Normalise burstiness to [0, 1] range: B ∈ [-1, 1] → (B + 1) / 2
            burstiness_norm = round((burstiness + 1.0) / 2.0, 6)
            signals["cadence_burstiness"] = burstiness_norm

            confidence = "high" if event_count >= 30 else ("medium" if event_count >= 15 else "low")

            # CUSUM on inter-event gaps (hourly) for drift detection
            if len(char_ts) >= 2:
                gaps = [(char_ts[i] - char_ts[i - 1]).total_seconds() / 3600.0
                        for i in range(1, len(char_ts))]
                cusum_cadence = _cusum_drift(gaps)
            else:
                cusum_cadence = {"cusum_max": 0.0, "alarm": False, "sample_count": 0}

            evidence_rows.append({
                "character_id": cid,
                "evidence_key": "cadence_burstiness",
                "window_label": "lifetime",
                "evidence_value": burstiness_norm,
                "evidence_text": (
                    f"burstiness index {burstiness:.3f} (raw), "
                    f"max gap {cadence['max_gap_hours']:.1f}h, "
                    f"mean gap {cadence['mean_gap_hours']:.1f}h, "
                    f"CV {cadence['cv']:.2f} across {event_count} events"
                ),
                "evidence_payload_json": json_dumps_safe({
                    "burstiness_raw": burstiness,
                    "burstiness_normalized": burstiness_norm,
                    "max_gap_hours": cadence["max_gap_hours"],
                    "mean_gap_hours": cadence["mean_gap_hours"],
                    "coefficient_of_variation": cadence["cv"],
                    "event_count": event_count,
                    "cusum": cusum_cadence,
                }),
                "confidence_flag": confidence,
            })

            # ── 4d. Reactivation after dormancy ──
            reactivation = _detect_reactivation(char_ts)
            reactivation_score = 0.0
            if reactivation["reactivated"]:
                # Score based on dormancy length and burst intensity
                dormancy_factor = min(1.0, reactivation["dormancy_hours"] / (90 * 24))
                burst_factor = min(1.0, reactivation["recent_burst_count"] / 10.0)
                reactivation_score = round((dormancy_factor * 0.6 + burst_factor * 0.4), 6)

            signals["reactivation_after_dormancy"] = reactivation_score

            if reactivation["reactivated"] or reactivation["dormancy_hours"] >= DORMANCY_THRESHOLD_DAYS * 24 * 0.5:
                confidence = "high" if event_count >= 30 else ("medium" if event_count >= 15 else "low")
                evidence_rows.append({
                    "character_id": cid,
                    "evidence_key": "reactivation_after_dormancy",
                    "window_label": "recent",
                    "evidence_value": reactivation_score,
                    "evidence_text": (
                        f"{'reactivated' if reactivation['reactivated'] else 'partial dormancy'} "
                        f"after {reactivation['dormancy_hours']:.0f}h quiet, "
                        f"{reactivation['recent_burst_count']} recent events"
                    ),
                    "evidence_payload_json": json_dumps_safe({
                        "reactivated": reactivation["reactivated"],
                        "dormancy_hours": reactivation["dormancy_hours"],
                        "dormancy_days": round(reactivation["dormancy_hours"] / 24.0, 1),
                        "recent_burst_count": reactivation["recent_burst_count"],
                        "score": reactivation_score,
                    }),
                    "confidence_flag": confidence,
                })

            neo4j_signals[cid] = signals

        # ── 5. Cohort normalisation ──
        _cohort_normalize(evidence_rows)

        # ── 6. Write to MariaDB ──
        if not dry_run and evidence_rows:
            with db.transaction() as (_, cursor_db):
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
                    rows_written += 1

        # ── 7. Neo4j tagging (optional) ──
        neo4j_tagged = 0
        neo4j_cfg = runtime.get("neo4j") if runtime else None
        if not dry_run and neo4j_cfg and neo4j_signals:
            try:
                neo4j = Neo4jClient(Neo4jConfig(
                    url=neo4j_cfg.get("url", ""),
                    username=neo4j_cfg.get("username", ""),
                    password=neo4j_cfg.get("password", ""),
                    database=neo4j_cfg.get("database", "neo4j"),
                ))
                neo4j_tagged = _neo4j_tag_temporal_anomalies(neo4j, neo4j_signals)
            except Exception:
                # Neo4j is optional — don't fail the job if it's unreachable
                pass

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=(
                f"Temporal behavior detection complete: "
                f"{rows_processed} characters analysed, "
                f"{rows_written} evidence rows written, "
                f"{neo4j_tagged} Neo4j tags."
            ),
            rows_processed=rows_processed,
            rows_written=rows_written,
            duration_ms=duration_ms,
            meta={
                "computed_at": computed_at,
                "characters_analysed": rows_processed,
                "evidence_rows": rows_written,
                "neo4j_tagged": neo4j_tagged,
                "dry_run": dry_run,
            },
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=rows_processed, rows_written=rows_written, meta=result)
        return result

    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(db, job, status="failed", rows_processed=rows_processed, rows_written=rows_written, error_text=str(exc))
        raise
