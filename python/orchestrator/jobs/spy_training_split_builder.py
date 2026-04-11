"""Build labeled training splits for the spy detection platform.

Phase 2 of the spy detection platform. Reads ``analyst_feedback`` rows,
applies the authoritative :mod:`spy_label_policy` mapping, joins to
``character_feature_snapshots`` to confirm features exist, and persists
one ``model_training_splits`` row plus per-character
``model_training_split_members`` rows.

Design notes
------------

* **Label selection is NOT re-implemented here.** Every label comes through
  :func:`spy_label_policy.apply`. Drift would be a bug. Weak labels (from
  heuristic rules) are allowed only in ``split_role='train'`` — this is
  enforced by :func:`spy_label_policy.is_valid_split_role_for_weak_label`.

* **Idempotency via deterministic split_id.** The split_id is a SHA-256
  over the ordered input parameters (feature set, windows, strategy, seed).
  Running the job twice with identical parameters is a no-op — the second
  run hits the ``ON DUPLICATE KEY UPDATE`` path and overwrites with the
  same bytes.

* **Most-recent label wins.** A character may have multiple feedback rows
  over time (e.g. ``needs_review`` then ``true_positive``). The job picks
  the latest row by ``created_at``, then runs it through the policy. This
  keeps training sets in sync with the analyst's current belief.

* **Time-series split by default.** To avoid temporal leakage, training
  characters are those with a qualifying label in ``[train_window_start,
  train_window_end)`` and test characters are those in
  ``[test_window_start, test_window_end)``. Windows may not overlap. If a
  character has feedback in both windows, they go into whichever window
  their most recent feedback falls in (the other one is dropped).

* **Feature join is mandatory.** A character with a label but no snapshot
  is excluded with ``reason='no_feature_snapshot'``. We can't train on a
  character we can't featurize.
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..job_utils import finish_job_run, start_job_run
from ..json_utils import json_dumps_safe
from . import spy_label_policy

# Defaults. Callers may override via ``payload`` — see
# :func:`run_build_spy_training_split` docstring.
DEFAULT_FEATURE_SET = "spy_v1"
DEFAULT_FEATURE_VERSION = "1.0.0"
DEFAULT_STRATEGY = "time_series"
DEFAULT_SEED = 20260411
DEFAULT_TRAIN_WINDOW_DAYS = 60
DEFAULT_TEST_WINDOW_DAYS = 14


def _now_sql() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace(" ", "T")).astimezone(UTC)
        except ValueError:
            return None
    return None


def _fmt_dt(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _compute_split_id(
    feature_set: str,
    feature_version: str,
    strategy: str,
    train_window_start: datetime,
    train_window_end: datetime,
    test_window_start: datetime,
    test_window_end: datetime,
    seed: int,
) -> str:
    """Stable SHA-256 over the ordered inputs.

    The split_id shape is ``spy_v1_<hex12>`` so it's readable in joins but
    still unique across parameter combinations.
    """
    material = json.dumps(
        {
            "feature_set": feature_set,
            "feature_version": feature_version,
            "strategy": strategy,
            "train_window_start": _fmt_dt(train_window_start),
            "train_window_end": _fmt_dt(train_window_end),
            "test_window_start": _fmt_dt(test_window_start),
            "test_window_end": _fmt_dt(test_window_end),
            "seed": seed,
            "policy_version": spy_label_policy.POLICY_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"{feature_set}_{digest[:12]}"


def _resolve_windows(
    payload: dict[str, Any] | None,
) -> tuple[datetime, datetime, datetime, datetime]:
    """Decode train/test windows from the payload or fall back to defaults.

    Default windows: the most recent ``DEFAULT_TEST_WINDOW_DAYS`` are the
    test window; the ``DEFAULT_TRAIN_WINDOW_DAYS`` before that are the
    train window. This is the "time-series holdout" baseline.
    """
    now = datetime.now(UTC)
    payload = payload or {}

    test_end = _parse_dt(payload.get("test_window_end")) or now
    test_days = max(1, int(payload.get("test_window_days") or DEFAULT_TEST_WINDOW_DAYS))
    test_start = _parse_dt(payload.get("test_window_start")) or (test_end - timedelta(days=test_days))

    train_end = _parse_dt(payload.get("train_window_end")) or test_start
    train_days = max(1, int(payload.get("train_window_days") or DEFAULT_TRAIN_WINDOW_DAYS))
    train_start = _parse_dt(payload.get("train_window_start")) or (train_end - timedelta(days=train_days))

    if not (train_start < train_end <= test_start < test_end):
        raise ValueError(
            "Invalid split windows: require train_start < train_end <= test_start < test_end; "
            f"got train=[{_fmt_dt(train_start)}..{_fmt_dt(train_end)}], "
            f"test=[{_fmt_dt(test_start)}..{_fmt_dt(test_end)}]"
        )

    return train_start, train_end, test_start, test_end


def _load_latest_feedback_in_window(
    db: SupplyCoreDb,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    """Return one analyst_feedback row per character — the latest within window.

    SQL picks the most recent row by ``created_at`` per ``character_id``
    whose created_at falls in ``[window_start, window_end)``. Characters
    with no qualifying row are omitted.
    """
    return db.fetch_all(
        """
        SELECT af.character_id, af.label, af.confidence, af.created_at
        FROM analyst_feedback af
        INNER JOIN (
            SELECT character_id, MAX(created_at) AS max_at
            FROM analyst_feedback
            WHERE created_at >= %s AND created_at < %s
            GROUP BY character_id
        ) latest
          ON latest.character_id = af.character_id
         AND latest.max_at = af.created_at
        WHERE af.created_at >= %s AND af.created_at < %s
        ORDER BY af.character_id ASC
        """,
        (_fmt_dt(window_start), _fmt_dt(window_end), _fmt_dt(window_start), _fmt_dt(window_end)),
    )


def _load_snapshot_characters(
    db: SupplyCoreDb,
    feature_set: str,
    feature_version: str,
) -> set[int]:
    """Return the set of character_ids that have at least one snapshot.

    The Phase 6 trainer does a more specific join on window_label and
    computed_at; for split membership all we need is "has at least one
    snapshot for this feature set + version".
    """
    rows = db.fetch_all(
        """
        SELECT DISTINCT character_id
        FROM character_feature_snapshots
        WHERE feature_set = %s AND feature_version = %s
        """,
        (feature_set, feature_version),
    )
    return {int(r["character_id"]) for r in rows if r.get("character_id") is not None}


def _materialize_members(
    feedback_rows: list[dict[str, Any]],
    snapshot_chars: set[int],
    split_role: str,
) -> tuple[list[tuple[Any, ...]], dict[str, int]]:
    """Apply the policy + feature-existence filter and build insert tuples.

    Returns the insert rows and a counter dict for logging.
    """
    rows: list[tuple[Any, ...]] = []
    counters = {
        "considered": 0,
        "excluded_policy": 0,
        "excluded_no_snapshot": 0,
        "excluded_weak_label_role": 0,
        "positive": 0,
        "negative": 0,
    }

    for fb in feedback_rows:
        counters["considered"] += 1
        char_id = int(fb.get("character_id") or 0)
        if char_id <= 0:
            counters["excluded_policy"] += 1
            continue

        decision = spy_label_policy.apply(fb)
        if decision.label is None:
            counters["excluded_policy"] += 1
            continue

        if char_id not in snapshot_chars:
            counters["excluded_no_snapshot"] += 1
            continue

        # Weak-label enforcement. The policy module's label_source is
        # always ``analyst_feedback`` here (we only read the feedback
        # table), but the check is cheap and future-proofs against a
        # future weak-label injection path.
        if decision.label_source != "analyst_feedback":
            if not spy_label_policy.is_valid_split_role_for_weak_label(split_role):
                counters["excluded_weak_label_role"] += 1
                continue

        rows.append(
            (
                char_id,
                split_role,
                decision.label,
                decision.label_source,
                round(float(decision.effective_confidence), 3),
            )
        )
        counters[decision.label] += 1

    return rows, counters


def run_build_spy_training_split(
    db: SupplyCoreDb,
    neo4j_raw: dict[str, Any] | None = None,
    runtime: dict[str, Any] | None = None,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a time-series training split from analyst_feedback.

    Payload (all optional)::

        {
            "feature_set": "spy_v1",
            "feature_version": "1.0.0",
            "split_strategy": "time_series",
            "train_window_start": "2026-02-10T00:00:00",
            "train_window_end":   "2026-04-01T00:00:00",
            "test_window_start":  "2026-04-01T00:00:00",
            "test_window_end":    "2026-04-11T00:00:00",
            "train_window_days": 60,   # used if train_window_start omitted
            "test_window_days":  14,   # used if test_window_start omitted
            "seed": 20260411
        }

    Re-running with identical inputs is a no-op (the split_id hash is
    deterministic).
    """
    lock_key = "build_spy_training_split"
    job = start_job_run(db, lock_key)
    started = time.perf_counter()
    payload = payload or {}

    feature_set = str(payload.get("feature_set") or DEFAULT_FEATURE_SET)
    feature_version = str(payload.get("feature_version") or DEFAULT_FEATURE_VERSION)
    strategy = str(payload.get("split_strategy") or DEFAULT_STRATEGY)
    seed = int(payload.get("seed") or DEFAULT_SEED)

    rows_processed = 0
    rows_written = 0
    try:
        train_start, train_end, test_start, test_end = _resolve_windows(payload)

        split_id = _compute_split_id(
            feature_set=feature_set,
            feature_version=feature_version,
            strategy=strategy,
            train_window_start=train_start,
            train_window_end=train_end,
            test_window_start=test_start,
            test_window_end=test_end,
            seed=seed,
        )

        snapshot_chars = _load_snapshot_characters(db, feature_set, feature_version)
        if not snapshot_chars:
            result = JobResult.success(
                job_key=lock_key,
                summary=(
                    f"No character_feature_snapshots exist for {feature_set} v{feature_version}; "
                    "cannot build a labeled split without features."
                ),
                rows_processed=0,
                rows_written=0,
                duration_ms=int((time.perf_counter() - started) * 1000),
                meta={
                    "split_id": split_id,
                    "feature_set": feature_set,
                    "feature_version": feature_version,
                    "policy_version": spy_label_policy.POLICY_VERSION,
                    "skipped_reason": "no_feature_snapshots",
                },
            ).to_dict()
            finish_job_run(
                db, job, status="success", rows_processed=0, rows_written=0, meta=result,
            )
            return result

        train_feedback = _load_latest_feedback_in_window(db, train_start, train_end)
        test_feedback = _load_latest_feedback_in_window(db, test_start, test_end)

        # If a character has feedback in both windows, they belong to the
        # window where their latest row sits (i.e. test). Drop the train
        # entry so we don't leak.
        test_char_ids = {int(r["character_id"]) for r in test_feedback if r.get("character_id")}
        train_feedback = [r for r in train_feedback if int(r.get("character_id") or 0) not in test_char_ids]

        train_rows, train_counters = _materialize_members(train_feedback, snapshot_chars, "train")
        test_rows, test_counters = _materialize_members(test_feedback, snapshot_chars, "test")

        positive_count = train_counters["positive"] + test_counters["positive"]
        negative_count = train_counters["negative"] + test_counters["negative"]

        notes = json_dumps_safe(
            {
                "policy_version": spy_label_policy.POLICY_VERSION,
                "train_counters": train_counters,
                "test_counters": test_counters,
                "feature_set": feature_set,
                "feature_version": feature_version,
                "snapshot_universe_size": len(snapshot_chars),
            }
        )

        # Upsert the split parent, then refresh its members.
        created_at = _now_sql()
        db.execute(
            """
            INSERT INTO model_training_splits
                (split_id, feature_set, feature_version, split_strategy,
                 train_window_start, train_window_end,
                 test_window_start, test_window_end,
                 positive_count, negative_count, seed, created_at, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                positive_count = VALUES(positive_count),
                negative_count = VALUES(negative_count),
                notes = VALUES(notes)
            """,
            (
                split_id,
                feature_set,
                feature_version,
                strategy,
                _fmt_dt(train_start),
                _fmt_dt(train_end),
                _fmt_dt(test_start),
                _fmt_dt(test_end),
                positive_count,
                negative_count,
                seed,
                created_at,
                notes,
            ),
        )

        # DELETE-then-INSERT members inside a single logical refresh so
        # stale members don't linger after re-runs with different windows
        # that landed on the same split_id (shouldn't happen, but the
        # safety is cheap).
        db.execute(
            "DELETE FROM model_training_split_members WHERE split_id = %s",
            (split_id,),
        )

        member_insert_rows: list[tuple[Any, ...]] = []
        for char_id, split_role, label, label_source, confidence in train_rows + test_rows:
            member_insert_rows.append(
                (split_id, char_id, split_role, label, label_source, confidence)
            )
            rows_processed += 1

        if member_insert_rows:
            db.execute_many(
                """
                INSERT INTO model_training_split_members
                    (split_id, character_id, split_role, label, label_source, label_confidence)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                member_insert_rows,
            )
            rows_written = len(member_insert_rows)

        duration_ms = int((time.perf_counter() - started) * 1000)
        result = JobResult.success(
            job_key=lock_key,
            summary=(
                f"Built split {split_id}: {positive_count} positive, {negative_count} negative "
                f"across train ({len(train_rows)}) + test ({len(test_rows)})."
            ),
            rows_processed=rows_processed,
            rows_written=rows_written,
            duration_ms=duration_ms,
            meta={
                "split_id": split_id,
                "feature_set": feature_set,
                "feature_version": feature_version,
                "split_strategy": strategy,
                "seed": seed,
                "policy_version": spy_label_policy.POLICY_VERSION,
                "train_window": [_fmt_dt(train_start), _fmt_dt(train_end)],
                "test_window": [_fmt_dt(test_start), _fmt_dt(test_end)],
                "positive_count": positive_count,
                "negative_count": negative_count,
                "train_counters": train_counters,
                "test_counters": test_counters,
                "snapshot_universe_size": len(snapshot_chars),
            },
        ).to_dict()
        finish_job_run(
            db,
            job,
            status="success",
            rows_processed=rows_processed,
            rows_written=rows_written,
            meta=result,
        )
        return result
    except Exception as exc:
        finish_job_run(
            db,
            job,
            status="failed",
            rows_processed=rows_processed,
            rows_written=rows_written,
            error_text=str(exc),
        )
        raise
