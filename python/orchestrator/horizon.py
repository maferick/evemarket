"""Incremental horizon / rolling repair window helpers.

This module is the Python mirror of the db_horizon_* helpers in src/db.php
and provides the shared primitives compute/refresh jobs need to safely
transition from full-history recomputation to incremental-only mode.

Core concepts
-------------
- Every dataset carries an explicit ``backfill_complete`` gate on
  ``sync_state``. It is only flipped by admin action (CLI or settings
  UI), never inferred from a single successful run.
- While ``backfill_complete = 0``, jobs keep their current
  full/backfill + incremental behavior — nothing changes.
- Once ``backfill_complete = 1``, jobs may switch to incremental-only
  and read from ``(last_cursor - repair_window_seconds)`` on every
  pass. Because downstream writes are idempotent UPSERTs, re-reading
  the last N hours naturally absorbs late-arriving source rows
  without silent drift.
- A derived ``watermark_event_time`` plus stall tracking feeds a
  freshness report that answers "which datasets are caught up to the
  24h SLA, and are any stalled?".

The helpers are intentionally small, pure, and free of side effects
beyond the ``sync_state`` row they target. They are meant to be called
from inside existing refresh functions without any structural rewrite:

    from orchestrator.horizon import (
        read_from_cursor,
        advance_cursor_with_stall_detection,
        should_use_incremental_only,
    )

    state = horizon_state_get(db, dataset_key)
    if should_use_incremental_only(state):
        read_cursor = read_from_cursor(state, raw_cursor)
    else:
        read_cursor = raw_cursor

    # ... fetch batch, process, write ...

    advance_cursor_with_stall_detection(db, dataset_key, new_cursor)

See docs/OPERATIONS_GUIDE.md for the operational picture and the
rollout plan described in /root/.claude/plans/breezy-toasting-wind.md.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .db import SupplyCoreDb


DEFAULT_HORIZON_SECONDS = 86400
DEFAULT_REPAIR_WINDOW_SECONDS = 86400
STALL_THRESHOLD = 3

_EPOCH_CURSOR = "1970-01-01 00:00:00|0"


# ---------------------------------------------------------------------------
# Cursor parsing (mirrors db_time_series_parse_cursor / db_time_series_cursor_value).
# ---------------------------------------------------------------------------


def parse_cursor(cursor: str | None) -> tuple[str, int]:
    """Split a ``timestamp|id`` cursor. Returns ``('', 0)`` for empty input."""
    if cursor is None:
        return "", 0
    text = str(cursor).strip()
    if not text:
        return "", 0
    parts = text.split("|", 1)
    timestamp = parts[0].strip()
    id_part = parts[1].strip() if len(parts) > 1 else "0"
    try:
        id_value = max(0, int(id_part))
    except (TypeError, ValueError):
        id_value = 0
    return timestamp, id_value


def format_cursor(timestamp: str, id_value: int) -> str:
    """Encode a ``timestamp|id`` cursor string."""
    return f"{(timestamp or '').strip()}|{max(0, int(id_value or 0))}"


# ---------------------------------------------------------------------------
# Horizon state.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HorizonState:
    """Snapshot of a dataset's horizon state.

    Mirrors the columns read by ``db_horizon_state_get()`` in PHP.
    ``last_success_at`` and ``watermark_event_time`` are naive UTC
    datetimes to match the MariaDB ``DATETIME`` storage convention.
    """

    dataset_key: str
    sync_mode: str
    status: str
    last_success_at: datetime | None
    last_cursor: str | None
    watermark_event_time: datetime | None
    backfill_complete: bool
    backfill_proposed_at: datetime | None
    incremental_horizon_seconds: int
    repair_window_seconds: int
    stall_cursor: str | None
    stall_count: int
    auto_approve_blocked: bool = False

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "HorizonState":
        return cls(
            dataset_key=str(row["dataset_key"]),
            sync_mode=str(row.get("sync_mode") or "incremental"),
            status=str(row.get("status") or "idle"),
            last_success_at=_coerce_datetime(row.get("last_success_at")),
            last_cursor=_coerce_str(row.get("last_cursor")),
            watermark_event_time=_coerce_datetime(row.get("watermark_event_time")),
            backfill_complete=bool(int(row.get("backfill_complete") or 0)),
            backfill_proposed_at=_coerce_datetime(row.get("backfill_proposed_at")),
            incremental_horizon_seconds=_coerce_int_default(
                row.get("incremental_horizon_seconds"), DEFAULT_HORIZON_SECONDS
            ),
            repair_window_seconds=_coerce_int_default(
                row.get("repair_window_seconds"), DEFAULT_REPAIR_WINDOW_SECONDS
            ),
            stall_cursor=_coerce_str(row.get("stall_cursor")),
            stall_count=int(row.get("stall_count") or 0),
            auto_approve_blocked=bool(int(row.get("auto_approve_blocked") or 0)),
        )


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    text = str(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _coerce_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _coerce_int_default(value: Any, default: int) -> int:
    """Coerce a row value to int, honoring explicit ``0`` instead of
    falling back to the default.
    """
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def horizon_state_get(db: SupplyCoreDb, dataset_key: str) -> HorizonState | None:
    """Fetch the current horizon state for a dataset.

    Returns ``None`` only if ``sync_state`` has no row for the key.
    """
    row = db.fetch_one(
        """
        SELECT dataset_key,
               sync_mode,
               status,
               last_success_at,
               last_cursor,
               watermark_event_time,
               backfill_complete,
               backfill_proposed_at,
               incremental_horizon_seconds,
               repair_window_seconds,
               stall_cursor,
               stall_count,
               auto_approve_blocked
          FROM sync_state
         WHERE dataset_key = %s
         LIMIT 1
        """,
        (dataset_key,),
    )
    if not row:
        return None
    return HorizonState.from_row(row)


# ---------------------------------------------------------------------------
# Read-path helpers.
# ---------------------------------------------------------------------------


def should_use_incremental_only(state: HorizonState | None) -> bool:
    """Return True when a dataset is cleared to run in incremental-only mode.

    Both the backfill gate and at least one successful run are required.
    """
    if state is None:
        return False
    if not state.backfill_complete:
        return False
    if state.last_success_at is None:
        return False
    return True


def read_from_cursor(state: HorizonState | None, raw_cursor: str | None) -> str:
    """Compute the cursor a refresh run should read from.

    When horizon mode is active, returns a cursor offset back by
    ``repair_window_seconds`` so the last N hours of source data are
    re-scanned on every pass. Writes must be idempotent for this to be
    safe (all current refresh functions use INSERT ... ON DUPLICATE KEY).

    When horizon mode is inactive (or no repair window is set), returns
    the raw cursor unchanged — preserving the existing behavior.
    """
    safe_cursor = (raw_cursor or "").strip()
    if not safe_cursor:
        return _EPOCH_CURSOR

    if not should_use_incremental_only(state):
        return safe_cursor

    assert state is not None  # for type-checkers
    repair_window = max(0, state.repair_window_seconds)
    if repair_window == 0:
        return safe_cursor

    timestamp, _id = parse_cursor(safe_cursor)
    if not timestamp:
        return safe_cursor

    try:
        base = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return safe_cursor

    offset = base - timedelta(seconds=repair_window)
    if offset.year < 1970:
        offset = datetime(1970, 1, 1)

    return format_cursor(offset.strftime("%Y-%m-%d %H:%M:%S"), 0)


# ---------------------------------------------------------------------------
# Write-path helpers.
# ---------------------------------------------------------------------------


def advance_cursor_with_stall_detection(
    db: SupplyCoreDb,
    dataset_key: str,
    new_cursor: str,
    *,
    event_time: str | datetime | None = None,
) -> None:
    """Advance a dataset's forward cursor, honoring monotonicity and
    updating stall tracking.

    - Never moves ``last_cursor`` backward (explicit rewinds must use
      :func:`rewind_cursor`).
    - Updates ``watermark_event_time`` in lockstep with the cursor.
    - Resets ``stall_count`` to 0 when the cursor advances; increments it
      (and leaves ``stall_cursor`` latched) when the cursor is unchanged.
    """
    state = horizon_state_get(db, dataset_key)
    existing_cursor = (state.last_cursor if state is not None else "") or ""
    existing_stall_cursor = (state.stall_cursor if state is not None else "") or ""

    if existing_cursor and new_cursor < existing_cursor:
        return  # monotonic guard

    event_time_str = _coerce_event_time(event_time, new_cursor)

    advanced = new_cursor != existing_cursor
    if advanced:
        stall_cursor = new_cursor
        stall_count_expr = "0"
    else:
        stall_cursor = existing_stall_cursor or existing_cursor
        stall_count_expr = "(stall_count + 1)"

    db.execute(
        f"""
        UPDATE sync_state
           SET last_cursor = %s,
               watermark_event_time = %s,
               stall_cursor = %s,
               stall_count = {stall_count_expr},
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (new_cursor, event_time_str, stall_cursor or None, dataset_key),
    )


def _coerce_event_time(value: str | datetime | None, new_cursor: str) -> str | None:
    if value is None:
        timestamp, _id = parse_cursor(new_cursor)
        return timestamp or None
    if isinstance(value, datetime):
        value = value.replace(tzinfo=None) if value.tzinfo is not None else value
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip()
    return text or None


def rewind_cursor(db: SupplyCoreDb, dataset_key: str, cursor: str) -> None:
    """Manually rewind a dataset's forward cursor.

    Used when out-of-window late data lands (older than
    ``repair_window_seconds``). Bypasses the monotonic guard and clears
    stall tracking so the next run can re-process the affected range.
    """
    timestamp, _id = parse_cursor(cursor)
    event_time_str = timestamp or None
    db.execute(
        """
        UPDATE sync_state
           SET last_cursor = %s,
               watermark_event_time = %s,
               stall_cursor = NULL,
               stall_count = 0,
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (cursor, event_time_str, dataset_key),
    )


def update_watermark_and_stall(
    db: SupplyCoreDb,
    dataset_key: str,
    *,
    new_cursor: str,
    event_time: str | datetime | None,
) -> None:
    """Update ``watermark_event_time`` and stall tracking alongside an
    externally-managed ``last_cursor`` write.

    Unlike :func:`advance_cursor_with_stall_detection`, this helper does
    NOT touch ``last_cursor`` itself -- that's expected to be written by
    the calling job's existing upsert (e.g. battle intelligence uses its
    own integer-sequence-id upsert). The helper only compares cursors
    for equality to detect stalls (equal -> increment, different -> reset),
    so it works for any cursor format including plain integers.

    Call this on successful runs only. Failed runs should leave stall
    tracking alone so the next retry doesn't inherit a bumped counter.
    """
    row = db.fetch_one(
        "SELECT last_cursor, stall_cursor FROM sync_state WHERE dataset_key = %s LIMIT 1",
        (dataset_key,),
    ) or {}
    existing_stall_cursor = _coerce_str(row.get("stall_cursor")) or _coerce_str(row.get("last_cursor")) or ""

    advanced = str(new_cursor) != existing_stall_cursor
    if advanced:
        stall_cursor: str | None = new_cursor
        stall_count_expr = "0"
    else:
        stall_cursor = existing_stall_cursor or None
        stall_count_expr = "(stall_count + 1)"

    event_time_str = _coerce_event_time(event_time, new_cursor) if event_time is not None else None

    db.execute(
        f"""
        UPDATE sync_state
           SET watermark_event_time = COALESCE(%s, watermark_event_time),
               stall_cursor = %s,
               stall_count = {stall_count_expr},
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (event_time_str, stall_cursor, dataset_key),
    )


# ---------------------------------------------------------------------------
# Backfill-complete gate helpers.
# ---------------------------------------------------------------------------


def propose_backfill_complete(
    db: SupplyCoreDb,
    dataset_key: str,
    reason: str,
) -> None:
    """Mark a dataset as a backfill-complete candidate for admin review.

    Does NOT flip ``backfill_complete`` itself — requires approval via
    :func:`approve_backfill_complete` or the ``horizon-approve`` CLI.
    """
    db.execute(
        """
        UPDATE sync_state
           SET backfill_proposed_at = COALESCE(backfill_proposed_at, UTC_TIMESTAMP()),
               backfill_proposed_reason = %s,
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
           AND backfill_complete = 0
        """,
        (reason[:190], dataset_key),
    )


def approve_backfill_complete(db: SupplyCoreDb, dataset_key: str) -> None:
    """Admin approval: flip ``backfill_complete = 1`` and clear proposal."""
    db.execute(
        """
        UPDATE sync_state
           SET backfill_complete = 1,
               backfill_proposed_at = NULL,
               backfill_proposed_reason = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (dataset_key,),
    )


def reject_backfill_complete(db: SupplyCoreDb, dataset_key: str) -> None:
    """Clear a pending proposal without approving it."""
    db.execute(
        """
        UPDATE sync_state
           SET backfill_proposed_at = NULL,
               backfill_proposed_reason = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (dataset_key,),
    )


def reset_backfill_complete(db: SupplyCoreDb, dataset_key: str) -> None:
    """Revert a dataset to full-history mode (``backfill_complete = 0``)."""
    db.execute(
        """
        UPDATE sync_state
           SET backfill_complete = 0,
               backfill_proposed_at = NULL,
               backfill_proposed_reason = NULL,
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (dataset_key,),
    )


def block_auto_approve(db: SupplyCoreDb, dataset_key: str) -> None:
    """Opt a dataset out of the detect_backfill_complete auto-approval loop.

    When set, detect_backfill_complete will still *propose* the dataset
    for backfill_complete (so it still shows up in the admin review
    queue) but will never auto-flip the gate regardless of how long the
    proposal has been soaking. Use for datasets that need a human eye
    on the cutover.
    """
    db.execute(
        """
        UPDATE sync_state
           SET auto_approve_blocked = 1,
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (dataset_key,),
    )


def unblock_auto_approve(db: SupplyCoreDb, dataset_key: str) -> None:
    """Clear a previously-set auto-approval block.

    The dataset becomes eligible for auto-approval again on the next
    detect_backfill_complete run (subject to the usual soak and health
    criteria).
    """
    db.execute(
        """
        UPDATE sync_state
           SET auto_approve_blocked = 0,
               updated_at = CURRENT_TIMESTAMP
         WHERE dataset_key = %s
        """,
        (dataset_key,),
    )


# ---------------------------------------------------------------------------
# Freshness / reporting.
# ---------------------------------------------------------------------------


def freshness_status(
    state: HorizonState | None,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Derive the freshness view of a dataset from its horizon state.

    Returns a dict with:
        - freshness_lag_seconds   (int or None)
        - is_caught_up_24h        (bool)
        - is_stalled              (bool)
        - horizon_status          (str — one of caught_up/catching_up/
                                   backfilling/stopped/stalled)
    """
    if now is None:
        now = datetime.now(UTC).replace(tzinfo=None)
    elif now.tzinfo is not None:
        now = now.astimezone(UTC).replace(tzinfo=None)

    if state is None:
        return {
            "freshness_lag_seconds": None,
            "is_caught_up_24h": False,
            "is_stalled": False,
            "horizon_status": "backfilling",
        }

    lag = (
        int((now - state.watermark_event_time).total_seconds())
        if state.watermark_event_time is not None
        else None
    )
    horizon_seconds = state.incremental_horizon_seconds or DEFAULT_HORIZON_SECONDS
    is_caught_up = lag is not None and lag <= horizon_seconds
    is_stalled = state.stall_count >= STALL_THRESHOLD

    if state.status == "failed":
        horizon_status = "stopped"
    elif is_stalled:
        horizon_status = "stalled"
    elif not state.backfill_complete:
        horizon_status = "backfilling"
    elif is_caught_up:
        horizon_status = "caught_up"
    else:
        horizon_status = "catching_up"

    return {
        "freshness_lag_seconds": lag,
        "is_caught_up_24h": is_caught_up,
        "is_stalled": is_stalled,
        "horizon_status": horizon_status,
    }


def freshness_report(
    db: SupplyCoreDb,
    *,
    dataset_prefix: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Python equivalent of ``db_calculation_freshness_report()``.

    Returns one row per dataset with horizon status plus the latest
    ``sync_runs`` summary, sorted by ``dataset_key``.
    """
    params: list[Any] = []
    prefix_clause = ""
    if dataset_prefix:
        prefix_clause = " WHERE ss.dataset_key LIKE %s"
        params.append(f"{dataset_prefix}%")

    rows = db.fetch_all(
        f"""
        SELECT ss.dataset_key,
               ss.sync_mode,
               ss.status,
               ss.backfill_complete,
               ss.backfill_proposed_at,
               ss.backfill_proposed_reason,
               ss.last_success_at,
               ss.last_cursor,
               ss.watermark_event_time,
               ss.incremental_horizon_seconds,
               ss.repair_window_seconds,
               ss.stall_cursor,
               ss.stall_count,
               ss.auto_approve_blocked,
               latest.run_status   AS last_run_status,
               latest.source_rows  AS last_run_source_rows,
               latest.written_rows AS last_run_written_rows,
               latest.started_at   AS last_run_started_at,
               latest.finished_at  AS last_run_finished_at
          FROM sync_state ss
          LEFT JOIN (
              SELECT sr.dataset_key, sr.run_status, sr.source_rows, sr.written_rows,
                     sr.started_at, sr.finished_at
                FROM sync_runs sr
                INNER JOIN (
                    SELECT dataset_key, MAX(id) AS max_id
                      FROM sync_runs
                     GROUP BY dataset_key
                ) latest_id ON latest_id.dataset_key = sr.dataset_key AND latest_id.max_id = sr.id
          ) latest ON latest.dataset_key = ss.dataset_key
        {prefix_clause}
        ORDER BY ss.dataset_key ASC
        """,
        params,
    )

    result: list[dict[str, Any]] = []
    for row in rows:
        state = HorizonState.from_row(row)
        derived = freshness_status(state, now=now)
        result.append(
            {
                **row,
                "backfill_complete": state.backfill_complete,
                "incremental_horizon_seconds": state.incremental_horizon_seconds,
                "repair_window_seconds": state.repair_window_seconds,
                "stall_count": state.stall_count,
                "auto_approve_blocked": state.auto_approve_blocked,
                **derived,
            }
        )
    return result


def list_pending_proposals(db: SupplyCoreDb) -> list[dict[str, Any]]:
    """Return the pending backfill-complete proposals awaiting admin review."""
    return db.fetch_all(
        """
        SELECT dataset_key,
               sync_mode,
               backfill_proposed_at,
               backfill_proposed_reason,
               last_success_at,
               watermark_event_time,
               last_cursor,
               stall_count,
               auto_approve_blocked
          FROM sync_state
         WHERE backfill_complete = 0
           AND backfill_proposed_at IS NOT NULL
         ORDER BY backfill_proposed_at ASC
        """
    )
