"""Background worker that drains the character_processing_queue.

Picks characters from the queue and runs only the pipeline stages that are
stale for each character:

  1. Histograms (character_feature_histograms) — from battles + killmails
  2. Counterintel scoring (character_counterintel_scores + evidence)

Stage freshness is tracked via watermarks in ``character_pipeline_status``:
  - ``last_source_event_at`` — latest input timestamp for this character
  - ``histogram_at``, ``counterintel_at`` — when each stage last completed

A stage is stale when ``last_source_event_at > stage_at`` (or stage_at is NULL).
Fresh stages are skipped entirely to reduce compute waste.

Queue ownership boundary:
  - ``enrichment_queue`` → ESI/EveWho data FETCHING (raw source ingestion)
  - ``character_processing_queue`` → downstream COMPUTE (this worker)

The worker is time-budgeted and includes stale-lock recovery for crashed runs.
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

DEFAULT_BATCH_SIZE = 50
DEFAULT_TIME_BUDGET_SECONDS = 55  # leave headroom within a 60s schedule slot
STALE_LOCK_SECONDS = 180  # recover rows locked longer than this
MIN_COUNTERINTEL_BATTLES = 1  # minimum eligible battles to attempt scoring

WINDOW_DEFS: list[tuple[str, timedelta | None]] = [
    ("7d", timedelta(days=7)),
    ("30d", timedelta(days=30)),
    ("90d", timedelta(days=90)),
    ("lifetime", None),
]


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _ensure_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


# ---------------------------------------------------------------------------
# Queue operations
# ---------------------------------------------------------------------------

def enqueue_characters(db: SupplyCoreDb, character_ids: list[int], reason: str = "new_data", priority: float = 0.0) -> int:
    """Insert or update characters into the processing queue.

    Idempotent: re-enqueuing a done/failed character resets it to pending.
    Re-enqueuing a pending/processing character only bumps priority.
    Also updates the source watermark so stage freshness is tracked.

    Returns the number of rows affected.
    """
    if not character_ids:
        return 0
    deduped = list(set(cid for cid in character_ids if cid > 0))
    if not deduped:
        return 0

    BATCH = 500
    affected = 0
    now = _now_sql()
    for offset in range(0, len(deduped), BATCH):
        chunk = deduped[offset:offset + BATCH]

        # 1. Upsert into processing queue
        q_placeholders = ",".join(["(%s, %s, %s, 'pending', UTC_TIMESTAMP())"] * len(chunk))
        q_params: list[Any] = []
        for cid in chunk:
            q_params.extend([cid, reason, round(priority, 4)])
        affected += db.execute(
            f"""
            INSERT INTO character_processing_queue
                (character_id, reason, priority, status, created_at)
            VALUES {q_placeholders}
            ON DUPLICATE KEY UPDATE
                status = IF(status IN ('done','failed'), 'pending', status),
                priority = GREATEST(priority, VALUES(priority)),
                reason = VALUES(reason),
                attempts = IF(status IN ('done','failed'), 0, attempts),
                last_error = IF(status IN ('done','failed'), NULL, last_error),
                updated_at = CURRENT_TIMESTAMP
            """,
            tuple(q_params),
        )

        # 2. Update source watermark so stage freshness is deterministic
        s_placeholders = ",".join(["(%s, %s)"] * len(chunk))
        s_params: list[Any] = []
        for cid in chunk:
            s_params.extend([cid, now])
        db.execute(
            f"""
            INSERT INTO character_pipeline_status (character_id, last_source_event_at)
            VALUES {s_placeholders}
            ON DUPLICATE KEY UPDATE
                last_source_event_at = GREATEST(last_source_event_at, VALUES(last_source_event_at)),
                updated_at = CURRENT_TIMESTAMP
            """,
            tuple(s_params),
        )

    return affected


def _recover_stale_locks(db: SupplyCoreDb) -> int:
    """Reset rows stuck in 'processing' past the stale-lock threshold.

    This handles workers that crashed mid-flight and left rows locked.
    """
    cutoff = (datetime.now(UTC) - timedelta(seconds=STALE_LOCK_SECONDS)).strftime("%Y-%m-%d %H:%M:%S")
    return db.execute(
        """
        UPDATE character_processing_queue
        SET status = IF(attempts >= max_attempts, 'failed', 'pending'),
            locked_at = NULL,
            last_error = CONCAT(COALESCE(last_error, ''), ' [stale-lock recovered]')
        WHERE status = 'processing'
          AND locked_at IS NOT NULL
          AND locked_at < %s
        """,
        (cutoff,),
    )


def _claim_batch(db: SupplyCoreDb, batch_size: int) -> list[dict[str, Any]]:
    """Claim a batch of pending characters for processing.

    Uses SELECT ... FOR UPDATE SKIP LOCKED for safe concurrent access.
    """
    now = _now_sql()
    rows = db.fetch_all(
        """
        SELECT character_id, reason, priority, attempts
        FROM character_processing_queue
        WHERE status = 'pending'
          AND (not_before IS NULL OR not_before <= %s)
          AND attempts < max_attempts
        ORDER BY priority DESC, created_at ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (now, batch_size),
    )
    if not rows:
        return []

    cids = [int(r["character_id"]) for r in rows]
    placeholders = ",".join(["%s"] * len(cids))
    db.execute(
        f"""
        UPDATE character_processing_queue
        SET status = 'processing', locked_at = %s, attempts = attempts + 1
        WHERE character_id IN ({placeholders})
        """,
        tuple([now] + cids),
    )
    return rows


def _mark_done(db: SupplyCoreDb, character_id: int) -> None:
    db.execute(
        """
        UPDATE character_processing_queue
        SET status = 'done', processed_at = UTC_TIMESTAMP(), locked_at = NULL, last_error = NULL
        WHERE character_id = %s
        """,
        (character_id,),
    )


def _mark_failed(db: SupplyCoreDb, character_id: int, error: str) -> None:
    db.execute(
        """
        UPDATE character_processing_queue
        SET status = 'failed', locked_at = NULL, last_error = %s
        WHERE character_id = %s
        """,
        (error[:500], character_id),
    )


# ---------------------------------------------------------------------------
# Pipeline stage: histograms
# ---------------------------------------------------------------------------

def _run_histogram_stage(db: SupplyCoreDb, character_id: int, now_dt: datetime, computed_at: str) -> bool:
    """Compute and upsert feature histograms for a single character.

    Sources: battle_participants + killmail_events + killmail_attackers.
    Returns True if any histogram data was written.
    """
    # Gather all timestamped events for this character
    battle_rows = db.fetch_all(
        """
        SELECT br.started_at, HOUR(br.started_at) AS ev_hour, DAYOFWEEK(br.started_at) AS ev_dow
        FROM battle_participants bp
        INNER JOIN battle_rollups br ON br.battle_id = bp.battle_id
        WHERE bp.character_id = %s AND br.started_at IS NOT NULL
        """,
        (character_id,),
    )
    victim_rows = db.fetch_all(
        """
        SELECT ke.effective_killmail_at AS started_at,
               HOUR(ke.effective_killmail_at) AS ev_hour,
               DAYOFWEEK(ke.effective_killmail_at) AS ev_dow
        FROM killmail_events ke
        WHERE ke.victim_character_id = %s AND ke.effective_killmail_at IS NOT NULL
        """,
        (character_id,),
    )
    attacker_rows = db.fetch_all(
        """
        SELECT ke.effective_killmail_at AS started_at,
               HOUR(ke.effective_killmail_at) AS ev_hour,
               DAYOFWEEK(ke.effective_killmail_at) AS ev_dow
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        WHERE ka.character_id = %s AND ke.effective_killmail_at IS NOT NULL
        """,
        (character_id,),
    )

    # Merge and deduplicate within 60s
    all_events: list[dict[str, Any]] = []
    seen_ts: list[datetime] = []
    for row in battle_rows + victim_rows + attacker_rows:
        ts = _ensure_utc(row["started_at"]) if isinstance(row.get("started_at"), datetime) else None
        if ts is None:
            continue
        dominated = False
        for existing in seen_ts:
            if abs((ts - existing).total_seconds()) <= 60:
                dominated = True
                break
        if not dominated:
            seen_ts.append(ts)
            all_events.append(row)

    if not all_events:
        return False

    # Compute histograms per window
    wrote = False
    for wlabel, wdelta in WINDOW_DEFS:
        if wdelta is not None:
            cutoff = now_dt - wdelta
            window_events = [e for e in all_events if _ensure_utc(e["started_at"]) >= cutoff]
        else:
            window_events = all_events

        hour_hist: dict[int, int] = defaultdict(int)
        dow_hist: dict[int, int] = defaultdict(int)
        for e in window_events:
            hour_hist[int(e.get("ev_hour") or 0)] += 1
            dow_hist[int(e.get("ev_dow") or 1)] += 1

        if not hour_hist and not dow_hist:
            continue

        hour_json = json.dumps({str(k): v for k, v in sorted(hour_hist.items())})
        dow_json = json.dumps({str(k): v for k, v in sorted(dow_hist.items())})
        db.execute(
            """
            INSERT INTO character_feature_histograms
                (character_id, window_label, hour_histogram, weekday_histogram, computed_at)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                hour_histogram = VALUES(hour_histogram),
                weekday_histogram = VALUES(weekday_histogram),
                computed_at = VALUES(computed_at)
            """,
            (character_id, wlabel, hour_json, dow_json, computed_at),
        )
        wrote = True

    return wrote


# ---------------------------------------------------------------------------
# Pipeline stage: counterintel scoring
# ---------------------------------------------------------------------------

def _run_counterintel_stage(db: SupplyCoreDb, character_id: int, computed_at: str) -> bool:
    """Compute counterintel features and scores for a single character.

    Mirrors the logic from compute_character_intelligence_on_demand() in PHP
    but runs in the Python pipeline context.  Returns True if scores were written.
    """
    participations = db.fetch_all(
        """
        SELECT bp.battle_id, bp.side_key, bp.corporation_id, bp.alliance_id,
               COALESCE(cgi.bridge_score, 0) AS bridge_score
        FROM battle_participants bp
        LEFT JOIN character_graph_intelligence cgi ON cgi.character_id = bp.character_id
        WHERE bp.character_id = %s
          AND bp.battle_id IN (
              SELECT br.battle_id FROM battle_rollups br
              WHERE br.eligible_for_suspicion = 1
                AND br.participant_count >= 20
          )
        """,
        (character_id,),
    )

    if not participations:
        return False

    battle_ids = list({str(p["battle_id"]) for p in participations})
    placeholders = ",".join(["%s"] * len(battle_ids))
    over_rows = db.fetch_all(
        f"""
        SELECT battle_id, side_key, anomaly_class, sustain_lift_score
        FROM battle_enemy_overperformance_scores
        WHERE battle_id IN ({placeholders})
        """,
        tuple(battle_ids),
    )

    anomalous_battles: dict[str, bool] = {}
    control_battles: dict[str, bool] = {}
    sustain_by_key: dict[str, float] = {}
    for row in over_rows:
        key = f"{row['battle_id']}|{row['side_key']}"
        sustain_by_key[key] = float(row.get("sustain_lift_score") or 0.0)
        if row["anomaly_class"] == "high_enemy_overperformance":
            anomalous_battles[key] = True
        else:
            control_battles[key] = True

    anomaly_hits = 0
    control_hits = 0
    sustain_lifts: list[float] = []
    bridge_scores: list[float] = []
    anomalous_battle_ids: set[str] = set()

    for row in participations:
        bid = str(row["battle_id"])
        side = str(row.get("side_key") or "unknown")
        key = f"{bid}|{side}"

        if key in anomalous_battles:
            anomaly_hits += 1
            anomalous_battle_ids.add(bid)
        if key in control_battles:
            control_hits += 1
        bridge_scores.append(float(row.get("bridge_score") or 0.0))

        for over in over_rows:
            if str(over["battle_id"]) == bid and str(over["side_key"]) != side:
                sustain_lifts.append(float(over.get("sustain_lift_score") or 0.0))

    anom_denom = max(1, len(anomalous_battles))
    ctrl_denom = max(1, len(control_battles))
    anomalous_rate = anomaly_hits / anom_denom if anom_denom > 0 else 0.0
    control_rate = control_hits / ctrl_denom if ctrl_denom > 0 else 0.0
    presence_delta = anomalous_rate - control_rate
    enemy_sustain_lift = sum(sustain_lifts) / len(sustain_lifts) if sustain_lifts else 0.0
    bridge = sum(bridge_scores) / len(bridge_scores) if bridge_scores else 0.0

    # Org history
    org_row = db.fetch_one(
        """
        SELECT corp_hops_180d, short_tenure_hops_180d
        FROM character_org_history_cache
        WHERE character_id = %s AND source = 'evewho'
          AND (expires_at IS NULL OR expires_at > UTC_TIMESTAMP())
        ORDER BY fetched_at DESC LIMIT 1
        """,
        (character_id,),
    )
    corp_hops = int((org_row or {}).get("corp_hops_180d") or 0)
    short_hops = int((org_row or {}).get("short_tenure_hops_180d") or 0)
    corp_hop_freq = corp_hops / 180.0
    short_tenure_ratio = short_hops / corp_hops if corp_hops > 0 else 0.0

    repeatability = min(1.0, anomaly_hits / 3.0)

    review_score = max(0.0, min(1.0,
        0.24 * anomalous_rate
        + 0.10 * max(0.0, presence_delta)
        + 0.26 * min(1.0, enemy_sustain_lift / 1.5)
        + 0.20 * min(1.0, bridge / 5.0)
        + 0.10 * min(1.0, corp_hop_freq * 10)
        + 0.10 * repeatability
    ))

    confidence = min(1.0, (anomaly_hits + control_hits) / 8.0)

    # Write features
    db.execute(
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
        (
            character_id, anomaly_hits, control_hits,
            len(anomalous_battles), len(control_battles),
            anomalous_rate, control_rate, enemy_sustain_lift,
            enemy_sustain_lift, anomalous_rate, bridge,
            corp_hop_freq, short_tenure_ratio, repeatability, computed_at,
        ),
    )

    # Write scores
    db.execute(
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
        (character_id, review_score, 0.0, confidence, 8, computed_at),
    )

    # Write evidence signals
    presence_payload = json_dumps_safe({
        "anomalous": {"numerator": anomaly_hits, "denominator": len(anomalous_battles), "rate": anomalous_rate},
        "control": {"numerator": control_hits, "denominator": len(control_battles), "rate": control_rate},
        "delta": presence_delta,
    })
    sustain_payload = json_dumps_safe({
        "enemy_same_hull_survival_lift": enemy_sustain_lift,
        "sample_count": len(sustain_lifts),
    })
    graph_payload = json_dumps_safe({
        "co_presence_anomalous_density": anomalous_rate,
        "graph_bridge_score": bridge,
    })
    org_payload = json_dumps_safe({
        "window_days": 180,
        "corp_hops": corp_hops,
        "short_tenure_hops": short_hops,
        "corp_hop_frequency_per_day": corp_hop_freq,
        "short_tenure_ratio": short_tenure_ratio,
    })
    repeat_payload = json_dumps_safe({
        "anomalous_battle_count": len(anomalous_battle_ids),
        "repeatability_score": repeatability,
    })

    evidence_rows = [
        ("anomalous_battle_presence_count", "all_time", float(anomaly_hits),
         f"present in {anomaly_hits}/{len(anomalous_battles)} anomalous large battle-sides", presence_payload),
        ("anomalous_presence_rate", "all_time", anomalous_rate,
         f"anomalous presence rate {anomalous_rate:.3f} vs control {control_rate:.3f}", presence_payload),
        ("presence_rate_delta", "all_time", presence_delta,
         f"presence delta {presence_delta:.3f}", presence_payload),
        ("enemy_sustain_lift", "all_time", enemy_sustain_lift,
         f"enemy sustain lift {enemy_sustain_lift:.3f} when present", sustain_payload),
        ("enemy_same_hull_survival_lift_detail", "all_time", enemy_sustain_lift,
         f"same-hull enemy survival lift {enemy_sustain_lift:.3f} across {len(sustain_lifts)} samples", sustain_payload),
        ("graph_copresence_cluster_proximity", "all_time", min(1.0, bridge / 5.0),
         f"graph bridge {bridge:.3f}, anomalous co-presence density {anomalous_rate:.3f}", graph_payload),
        ("org_history_movement_180d", "180d", corp_hop_freq,
         f"org movement over 180d: {corp_hops} hops, {short_hops} short-tenure, ratio {short_tenure_ratio:.3f}", org_payload),
        ("repeatability_across_battles_windows", "all_time", repeatability,
         f"repeatability {repeatability:.3f}: {len(anomalous_battle_ids)} anomalous battles", repeat_payload),
    ]

    for ek, wl, ev, et, ep in evidence_rows:
        db.execute(
            """
            INSERT INTO character_counterintel_evidence (
                character_id, evidence_key, window_label, evidence_value, evidence_text, evidence_payload_json, computed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                evidence_value = VALUES(evidence_value),
                evidence_text = VALUES(evidence_text),
                evidence_payload_json = VALUES(evidence_payload_json),
                computed_at = VALUES(computed_at)
            """,
            (character_id, ek, wl, ev, et, ep, computed_at),
        )

    return True


# ---------------------------------------------------------------------------
# Pipeline status / freshness tracking
# ---------------------------------------------------------------------------

def _get_stage_freshness(db: SupplyCoreDb, character_id: int) -> dict[str, Any]:
    """Load the pipeline status row for a character.

    Returns a dict with watermark datetimes.  Missing row = everything stale.
    """
    row = db.fetch_one(
        """
        SELECT last_source_event_at, histogram_at, counterintel_at,
               temporal_at, org_history_at, last_fully_processed_at
        FROM character_pipeline_status
        WHERE character_id = %s
        """,
        (character_id,),
    )
    return dict(row) if row else {}


def _is_stage_stale(freshness: dict[str, Any], stage_col: str, upstream_cols: list[str] | None = None) -> bool:
    """A stage is stale if source or any upstream stage is newer than stage output.

    Staleness rules:
      - Stage never ran (stage_at is NULL) → stale
      - last_source_event_at > stage_at → stale (new input data)
      - Any upstream_col > stage_at → stale (upstream rebuilt after this stage)
      - No source data and no upstream changes → not stale
    """
    stage_at = freshness.get(stage_col)
    source_at = freshness.get("last_source_event_at")

    if stage_at is None:
        return True  # never ran

    t = _ensure_utc(stage_at) if isinstance(stage_at, datetime) else None
    if t is None:
        return True

    # Check source watermark
    if source_at is not None:
        s = _ensure_utc(source_at) if isinstance(source_at, datetime) else None
        if s is not None and s > t:
            return True

    # Check upstream stage watermarks (e.g., histogram rebuilt after counterintel).
    # If upstream is NULL, it either never ran or had no data.  Either way,
    # this stage's prior output may still be valid — the stage itself will
    # self-gate on data availability (e.g., counterintel returns False if
    # no eligible battles).  Only treat as stale if upstream ran MORE RECENTLY.
    for ucol in (upstream_cols or []):
        upstream_at = freshness.get(ucol)
        if upstream_at is not None:
            u = _ensure_utc(upstream_at) if isinstance(upstream_at, datetime) else None
            if u is not None and u > t:
                return True

    return False


def _update_stage_watermark(db: SupplyCoreDb, character_id: int, stage: str, computed_at: str) -> None:
    """Update the watermark for a completed stage."""
    at_col = f"{stage}_at"
    error_col = f"{stage}_error"
    # Only set error column if it exists in the table
    has_error = stage in ("histogram", "counterintel")
    if has_error:
        db.execute(
            f"""
            INSERT INTO character_pipeline_status (character_id, {at_col}, {error_col})
            VALUES (%s, %s, NULL)
            ON DUPLICATE KEY UPDATE
                {at_col} = VALUES({at_col}),
                {error_col} = NULL,
                updated_at = CURRENT_TIMESTAMP
            """,
            (character_id, computed_at),
        )
    else:
        db.execute(
            f"""
            INSERT INTO character_pipeline_status (character_id, {at_col})
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE
                {at_col} = VALUES({at_col}),
                updated_at = CURRENT_TIMESTAMP
            """,
            (character_id, computed_at),
        )


def _update_stage_error(db: SupplyCoreDb, character_id: int, stage: str, error: str) -> None:
    """Record a stage error without overwriting the watermark."""
    error_col = f"{stage}_error"
    if stage not in ("histogram", "counterintel"):
        return
    db.execute(
        f"""
        INSERT INTO character_pipeline_status (character_id, {error_col})
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            {error_col} = VALUES({error_col}),
            updated_at = CURRENT_TIMESTAMP
        """,
        (character_id, error[:500]),
    )


def _mark_fully_processed(db: SupplyCoreDb, character_id: int, computed_at: str) -> None:
    db.execute(
        """
        UPDATE character_pipeline_status
        SET last_fully_processed_at = %s
        WHERE character_id = %s
        """,
        (computed_at, character_id),
    )


# ---------------------------------------------------------------------------
# Main worker entry point
# ---------------------------------------------------------------------------

def run_character_pipeline_worker(
    db: SupplyCoreDb,
    runtime: dict[str, Any] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Drain the character_processing_queue through all pipeline stages.

    Stage-aware: only reruns stages whose watermark is older than the source
    watermark.  Fresh stages are skipped to reduce compute waste.

    Includes stale-lock recovery for workers that crashed mid-flight.
    """
    lock_key = "character_pipeline_worker"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    characters_processed = 0
    characters_skipped = 0
    characters_failed = 0
    stages_completed = 0
    stages_skipped = 0
    stale_recovered = 0
    runtime = runtime or {}
    batch_size = max(5, min(200, int(runtime.get("pipeline_worker_batch_size") or DEFAULT_BATCH_SIZE)))
    time_budget = max(10, int(runtime.get("pipeline_worker_time_budget_seconds") or DEFAULT_TIME_BUDGET_SECONDS))
    computed_at = _now_sql()
    now_dt = datetime.now(UTC)

    try:
        # Recover rows stuck in processing from crashed workers
        stale_recovered = _recover_stale_locks(db)

        while (time.perf_counter() - started) < time_budget:
            batch = _claim_batch(db, batch_size)
            if not batch:
                break  # queue drained

            for row in batch:
                if (time.perf_counter() - started) >= time_budget:
                    break

                character_id = int(row["character_id"])
                try:
                    freshness = _get_stage_freshness(db, character_id)
                    ran_any = False

                    # Stage 1: histograms — independent, only needs event data
                    if _is_stage_stale(freshness, "histogram_at"):
                        try:
                            if _run_histogram_stage(db, character_id, now_dt, computed_at):
                                _update_stage_watermark(db, character_id, "histogram", computed_at)
                                # Update in-memory freshness so downstream stages
                                # see the new watermark without a DB round-trip.
                                freshness["histogram_at"] = datetime.strptime(computed_at, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
                                stages_completed += 1
                                ran_any = True
                            else:
                                # No event data — nothing to compute, but don't
                                # mark fresh.  Leave histogram_at as NULL so it
                                # retries when data arrives.
                                stages_skipped += 1
                        except Exception as stage_exc:
                            _update_stage_error(db, character_id, "histogram", str(stage_exc))
                            raise
                    else:
                        stages_skipped += 1

                    # Stage 2: counterintel — needs eligible battle data.
                    # Also stale if histogram was just rebuilt (upstream dependency).
                    # Freshness dict is updated in-memory after stage 1, so the
                    # histogram_at upstream check sees the write from this run.
                    if _is_stage_stale(freshness, "counterintel_at", upstream_cols=["histogram_at"]):
                        try:
                            if _run_counterintel_stage(db, character_id, computed_at):
                                _update_stage_watermark(db, character_id, "counterintel", computed_at)
                                stages_completed += 1
                                ran_any = True
                            else:
                                # No eligible battles — do NOT advance watermark.
                                # Character stays stale for counterintel so it
                                # reruns when qualifying battles arrive later.
                                stages_skipped += 1
                        except Exception as stage_exc:
                            _update_stage_error(db, character_id, "counterintel", str(stage_exc))
                            raise
                    else:
                        stages_skipped += 1

                    # Temporal behavior detection runs as a separate batch job
                    # (temporal_behavior_detection.py) over all characters — we
                    # ensure histogram data is ready for it, but don't run it here.

                    # Only mark fully processed if all runnable stages completed.
                    # If a stage returned False (no data), don't claim completion.
                    if ran_any:
                        _mark_fully_processed(db, character_id, computed_at)
                        characters_processed += 1
                    else:
                        characters_skipped += 1

                    _mark_done(db, character_id)

                except Exception as exc:
                    _mark_failed(db, character_id, str(exc))
                    characters_failed += 1

        # Count remaining backlog
        backlog_row = db.fetch_one(
            "SELECT COUNT(*) AS cnt FROM character_processing_queue WHERE status = 'pending'"
        )
        backlog = int((backlog_row or {}).get("cnt") or 0)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=(
                f"Processed {characters_processed}, skipped {characters_skipped} fresh, "
                f"{characters_failed} failed, {stale_recovered} stale-locks recovered. "
                f"Stages: {stages_completed} ran, {stages_skipped} fresh. "
                f"Backlog: {backlog}."
            ),
            rows_processed=characters_processed + characters_skipped,
            rows_written=stages_completed,
            duration_ms=duration_ms,
            has_more=backlog > 0,
            meta={
                "characters_processed": characters_processed,
                "characters_skipped_fresh": characters_skipped,
                "characters_failed": characters_failed,
                "stale_locks_recovered": stale_recovered,
                "stages_completed": stages_completed,
                "stages_skipped_fresh": stages_skipped,
                "backlog_remaining": backlog,
                "time_budget_seconds": time_budget,
                "computed_at": computed_at,
            },
        ).to_dict()
        finish_job_run(db, job, status="success", rows_processed=characters_processed, rows_written=stages_completed, meta=result)
        return result

    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        finish_job_run(db, job, status="failed", rows_processed=characters_processed, rows_written=stages_completed, error_text=str(exc))
        raise


# ---------------------------------------------------------------------------
# Backfill: enqueue all known characters (idempotent, chunked)
# ---------------------------------------------------------------------------

def backfill_enqueue_all(db: SupplyCoreDb, chunk_size: int = 5000) -> dict[str, int]:
    """Enqueue every character seen in battles, killmails, or org history cache.

    Idempotent and chunked — safe to call repeatedly on large datasets.
    Characters already pending/processing are not disrupted (priority may bump).

    Source watermark is set to UTC_TIMESTAMP() which forces all stages to
    recompute.  This is intentional for backfill: we don't know which stages
    are stale without per-character queries, and the goal is full coverage.
    Once the worker processes them, watermarks will reflect actual completion
    and subsequent runs will be incremental.
    """
    sources = {
        "battle_participants": "SELECT DISTINCT character_id FROM battle_participants WHERE character_id > 0",
        "killmail_victims": "SELECT DISTINCT victim_character_id AS character_id FROM killmail_events WHERE victim_character_id IS NOT NULL AND victim_character_id > 0",
        "killmail_attackers": "SELECT DISTINCT character_id FROM killmail_attackers WHERE character_id IS NOT NULL AND character_id > 0",
        "org_history_cache": "SELECT DISTINCT character_id FROM character_org_history_cache WHERE character_id > 0",
    }
    all_ids: set[int] = set()
    counts: dict[str, int] = {}
    for source_name, query in sources.items():
        rows = db.fetch_all(query)
        ids = [int(r["character_id"]) for r in rows]
        counts[source_name] = len(ids)
        all_ids.update(ids)

    # Enqueue in chunks to avoid pathological single-transaction size
    total_enqueued = 0
    id_list = list(all_ids)
    for offset in range(0, len(id_list), chunk_size):
        chunk = id_list[offset:offset + chunk_size]
        total_enqueued += enqueue_characters(db, chunk, reason="backfill", priority=0.0)

    counts["total_unique"] = len(all_ids)
    counts["enqueued"] = total_enqueued
    return counts
