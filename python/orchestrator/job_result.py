"""Canonical job result contract for all SupplyCore processors.

Every job processor — compute, sync, graph, bridge — must ultimately produce
a result that conforms to this shape.  Raw processor dicts are normalized via
``JobResult.from_raw()`` so that all downstream consumers (worker pool,
job runner, PHP finalizer, status UI) see the same fields.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from .json_utils import make_json_safe

# ---------------------------------------------------------------------------
# Schema version — bump when adding/removing top-level fields.
# ---------------------------------------------------------------------------
RESULT_SCHEMA_VERSION = "job_result.v2"

# All valid top-level status values.
VALID_STATUSES = frozenset({"success", "failed", "skipped"})


def _utc_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_int(value: Any, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return minimum


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


@dataclass(slots=True)
class JobResult:
    """Canonical result shape returned by every SupplyCore job processor.

    Fields
    ------
    status : str
        One of ``"success"``, ``"failed"``, or ``"skipped"``.
    summary : str
        Human-readable one-line description of what happened.
    started_at : str
        ISO-8601 UTC timestamp when the job began.
    finished_at : str
        ISO-8601 UTC timestamp when the job ended.
    duration_ms : int
        Wall-clock runtime in milliseconds.
    rows_seen : int
        Total input rows considered (before filtering/skipping).
    rows_processed : int
        Rows that were actually processed (seen minus skipped).
    rows_written : int
        Rows written/upserted to output tables.
    rows_skipped : int
        Rows skipped (seen minus processed).
    rows_failed : int
        Rows that encountered errors during processing.
    batches_completed : int
        Number of batch iterations completed.
    checkpoint_before : str | None
        Cursor/checkpoint value before this run.
    checkpoint_after : str | None
        Cursor/checkpoint value after this run.
    error_text : str | None
        Error description if status is ``"failed"``, else ``None``.
    warnings : list[str]
        Non-fatal warnings produced during execution.
    meta : dict[str, Any]
        Processor-specific metadata.  Must be JSON-serializable.
        Always includes ``schema_version`` and ``job_key``.
    """

    status: str = "success"
    summary: str = ""
    started_at: str = ""
    finished_at: str = ""
    duration_ms: int = 0
    rows_seen: int = 0
    rows_processed: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    rows_failed: int = 0
    batches_completed: int = 0
    checkpoint_before: str | None = None
    checkpoint_after: str | None = None
    has_more: bool = False
    error_text: str | None = None
    warnings: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_raw(cls, raw: dict[str, Any], *, job_key: str = "") -> JobResult:
        """Normalize an arbitrary processor return dict into the canonical shape.

        This is the *single* normalization entry-point that replaces the old
        ``_compute_result_shape`` and ``_graph_result_shape`` helpers.
        """
        safe = make_json_safe(raw)

        status = _safe_str(raw.get("status"), "success")
        if status not in VALID_STATUSES:
            status = "success"

        rows_seen = _safe_int(raw.get("rows_seen") or raw.get("rows_processed"))
        rows_processed = _safe_int(raw.get("rows_processed"))
        rows_written = _safe_int(raw.get("rows_written"))
        rows_skipped = _safe_int(raw.get("rows_skipped") or (rows_seen - rows_processed))
        rows_failed = _safe_int(raw.get("rows_failed"))

        now = _utc_iso()
        started_at = _safe_str(raw.get("started_at"), now)
        finished_at = _safe_str(raw.get("finished_at"), now)
        duration_ms = _safe_int(raw.get("duration_ms"))

        summary = _safe_str(
            raw.get("summary"),
            f"{job_key or 'job'} completed with status {status}.",
        )

        error_text = _safe_str(raw.get("error_text") or raw.get("error"), "") or None

        warnings = [str(w) for w in list(safe.get("warnings") or [])]

        # Build meta — preserve processor-specific keys, inject contract keys.
        raw_meta = dict(safe.get("meta") or {})
        raw_meta.setdefault("job_key", job_key)
        raw_meta.setdefault("schema_version", RESULT_SCHEMA_VERSION)
        if "computed_at" not in raw_meta:
            computed_at = _safe_str(raw.get("computed_at"))
            if computed_at:
                raw_meta["computed_at"] = computed_at

        # Preserve non-canonical root-level fields (e.g. cursor, checksum,
        # run_id) that processors may return.  These are moved into meta so
        # they remain accessible without polluting the canonical envelope.
        _CANONICAL_KEYS = {
            "status", "summary", "started_at", "finished_at", "duration_ms",
            "rows_seen", "rows_processed", "rows_written", "rows_skipped",
            "rows_failed", "batches_completed", "checkpoint_before",
            "checkpoint_after", "has_more", "error_text", "error", "warnings", "meta",
            "computed_at",
        }
        for key, value in safe.items():
            if key not in _CANONICAL_KEYS and key not in raw_meta:
                raw_meta[key] = value

        # has_more may be set at top level or inside meta (graph_pipeline sets it in meta).
        has_more = bool(
            raw.get("has_more")
            or (raw.get("meta") or {}).get("has_more")
        )

        return cls(
            status=status,
            summary=summary,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=duration_ms,
            rows_seen=rows_seen,
            rows_processed=rows_processed,
            rows_written=rows_written,
            rows_skipped=rows_skipped,
            rows_failed=rows_failed,
            batches_completed=_safe_int(raw.get("batches_completed")),
            checkpoint_before=raw.get("checkpoint_before"),
            checkpoint_after=raw.get("checkpoint_after"),
            has_more=has_more,
            error_text=error_text,
            warnings=warnings,
            meta=raw_meta,
        )

    @classmethod
    def success(
        cls,
        *,
        job_key: str,
        summary: str,
        rows_processed: int = 0,
        rows_written: int = 0,
        started_at: str = "",
        finished_at: str = "",
        duration_ms: int = 0,
        meta: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
        has_more: bool = False,
        **kwargs: Any,
    ) -> JobResult:
        """Convenience builder for a successful result."""
        now = _utc_iso()
        return cls(
            status="success",
            summary=summary,
            started_at=started_at or now,
            finished_at=finished_at or now,
            duration_ms=duration_ms,
            rows_seen=kwargs.get("rows_seen", rows_processed),
            rows_processed=rows_processed,
            rows_written=rows_written,
            rows_skipped=kwargs.get("rows_skipped", 0),
            rows_failed=kwargs.get("rows_failed", 0),
            batches_completed=kwargs.get("batches_completed", 0),
            checkpoint_before=kwargs.get("checkpoint_before"),
            checkpoint_after=kwargs.get("checkpoint_after"),
            has_more=has_more,
            error_text=None,
            warnings=warnings or [],
            meta={
                "job_key": job_key,
                "schema_version": RESULT_SCHEMA_VERSION,
                **(meta or {}),
            },
        )

    @classmethod
    def failed(
        cls,
        *,
        job_key: str,
        error: str | Exception,
        started_at: str = "",
        finished_at: str = "",
        duration_ms: int = 0,
        meta: dict[str, Any] | None = None,
    ) -> JobResult:
        """Convenience builder for a failed result."""
        now = _utc_iso()
        error_text = f"{type(error).__name__}: {error}" if isinstance(error, Exception) else str(error)
        return cls(
            status="failed",
            summary=error_text,
            started_at=started_at or now,
            finished_at=finished_at or now,
            duration_ms=duration_ms,
            error_text=error_text,
            warnings=[error_text],
            meta={
                "job_key": job_key,
                "schema_version": RESULT_SCHEMA_VERSION,
                **(meta or {}),
            },
        )

    @classmethod
    def skipped(
        cls,
        *,
        job_key: str,
        reason: str,
        meta: dict[str, Any] | None = None,
    ) -> JobResult:
        """Convenience builder for a skipped result."""
        now = _utc_iso()
        return cls(
            status="skipped",
            summary=reason,
            started_at=now,
            finished_at=now,
            meta={
                "job_key": job_key,
                "schema_version": RESULT_SCHEMA_VERSION,
                "skip_reason": reason,
                **(meta or {}),
            },
        )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict suitable for JSON encoding and DB storage."""
        return make_json_safe({
            "status": self.status,
            "summary": self.summary,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "rows_seen": self.rows_seen,
            "rows_processed": self.rows_processed,
            "rows_written": self.rows_written,
            "rows_skipped": self.rows_skipped,
            "rows_failed": self.rows_failed,
            "batches_completed": self.batches_completed,
            "checkpoint_before": self.checkpoint_before,
            "checkpoint_after": self.checkpoint_after,
            "has_more": self.has_more,
            "error_text": self.error_text,
            "warnings": list(self.warnings),
            "meta": dict(self.meta),
        })

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> list[str]:
        """Return a list of contract violations (empty if valid)."""
        issues: list[str] = []
        if self.status not in VALID_STATUSES:
            issues.append(f"Invalid status: {self.status!r} (expected one of {sorted(VALID_STATUSES)})")
        if not self.summary:
            issues.append("Missing summary")
        if self.status == "failed" and not self.error_text:
            issues.append("Status is 'failed' but error_text is empty")
        if self.rows_processed > self.rows_seen:
            issues.append(f"rows_processed ({self.rows_processed}) > rows_seen ({self.rows_seen})")
        if self.rows_written > self.rows_processed:
            issues.append(f"rows_written ({self.rows_written}) > rows_processed ({self.rows_processed})")
        if self.duration_ms < 0:
            issues.append(f"Negative duration_ms: {self.duration_ms}")
        return issues
