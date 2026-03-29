"""System pressure and ESI freshness calculations for scheduling decisions.

Used by :meth:`~orchestrator.db.SupplyCoreDb.queue_due_recurring_jobs` to
make intelligent scheduling decisions:

- **Pressure state** — how loaded is the worker pool right now?
- **ESI freshness** — has the upstream ESI data actually expired?
- **Error rate** — are we seeing elevated 429s or failures?

These signals let the scheduler defer jobs when the data is still fresh,
reduce urgency under load, and back off when error rates spike.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .db import SupplyCoreDb

logger = logging.getLogger("supplycore.scheduler_pressure")

# -- Job → ESI endpoint mapping -------------------------------------------

#: Maps job_key to the ESI route_template patterns it depends on.
#: Used to check whether upstream ESI data has expired.
ESI_JOB_ENDPOINT_MAP: dict[str, list[str]] = {
    "market_hub_current_sync": ["/latest/markets/%/orders/"],
    "alliance_current_sync": ["/latest/markets/structures/%/"],
    "esi_alliance_history_sync": ["/v2/characters/%/corporationhistory/"],
    "entity_metadata_resolve_sync": ["/latest/universe/names/"],
}


# -- Data classes ----------------------------------------------------------

@dataclass(slots=True)
class PressureState:
    """Current system load pressure."""
    state: str              # "healthy", "loaded", "critical"
    running_jobs: int
    queued_jobs: int
    retry_jobs: int
    recent_dead: int        # Dead jobs in last 10 minutes
    recent_failures: int    # Retries in last 5 minutes

    @property
    def urgency_multiplier(self) -> float:
        """Scale urgency down under load to reduce contention."""
        return {"healthy": 1.0, "loaded": 0.8, "critical": 0.5}.get(self.state, 1.0)


@dataclass(slots=True)
class EsiFreshness:
    """Freshness status for a job's upstream ESI endpoints."""
    job_key: str
    total_endpoints: int
    fresh_endpoints: int    # expires_at still in the future
    expired_endpoints: int
    earliest_expires_seconds: float  # Seconds until earliest expiry (negative = already expired)

    @property
    def all_fresh(self) -> bool:
        """True if ALL upstream endpoints still have valid (unexpired) data."""
        return self.total_endpoints > 0 and self.expired_endpoints == 0

    @property
    def freshness_ratio(self) -> float:
        """0.0 = all expired, 1.0 = all fresh."""
        if self.total_endpoints == 0:
            return 0.0
        return self.fresh_endpoints / self.total_endpoints


@dataclass(slots=True)
class ErrorRate:
    """Recent ESI error rate from rate-limit observations."""
    total_requests: int
    error_count: int        # 429 + 420 + 5xx
    rate_limited_count: int # 429 specifically
    window_minutes: int

    @property
    def error_ratio(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.error_count / self.total_requests

    @property
    def is_elevated(self) -> bool:
        """True if error rate is above 10% or any 429s seen."""
        return self.rate_limited_count > 0 or self.error_ratio > 0.10


# -- Computation functions -------------------------------------------------

def compute_pressure_state(db: SupplyCoreDb) -> PressureState:
    """Compute current system pressure from running/queued/failed job counts."""
    try:
        counts = db.fetch_one(
            """SELECT
                SUM(status = 'running') AS running,
                SUM(status = 'queued') AS queued,
                SUM(status = 'retry') AS retry,
                SUM(status = 'dead' AND last_finished_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)) AS recent_dead,
                SUM(status = 'retry' AND last_finished_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 5 MINUTE)) AS recent_failures
               FROM worker_jobs
               WHERE status IN ('running', 'queued', 'retry', 'dead')"""
        ) or {}
    except Exception as exc:
        logger.warning("Failed to compute pressure state: %s", exc)
        return PressureState(state="healthy", running_jobs=0, queued_jobs=0, retry_jobs=0, recent_dead=0, recent_failures=0)

    running = int(counts.get("running") or 0)
    queued = int(counts.get("queued") or 0)
    retry = int(counts.get("retry") or 0)
    recent_dead = int(counts.get("recent_dead") or 0)
    recent_failures = int(counts.get("recent_failures") or 0)

    if recent_dead > 0 or recent_failures >= 3:
        state = "critical"
    elif running >= 3 or queued >= 5:
        state = "loaded"
    else:
        state = "healthy"

    return PressureState(
        state=state,
        running_jobs=running,
        queued_jobs=queued,
        retry_jobs=retry,
        recent_dead=recent_dead,
        recent_failures=recent_failures,
    )


def compute_esi_freshness(db: SupplyCoreDb, job_key: str) -> EsiFreshness | None:
    """Check ESI endpoint_state for the endpoints this job depends on.

    Returns ``None`` if the job has no known ESI endpoint dependencies.
    """
    patterns = ESI_JOB_ENDPOINT_MAP.get(job_key)
    if not patterns:
        return None

    try:
        # Build WHERE clause for route_template LIKE patterns.
        like_clauses = " OR ".join(["route_template LIKE %s"] * len(patterns))
        rows = db.fetch_all(
            f"""SELECT
                    route_template,
                    expires_at,
                    TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), expires_at) AS expires_in_seconds
                FROM esi_endpoint_state
                WHERE ({like_clauses})
                  AND last_status_code IS NOT NULL""",
            tuple(patterns),
        )
    except Exception as exc:
        logger.warning("Failed to compute ESI freshness for %s: %s", job_key, exc)
        return None

    if not rows:
        # No endpoint state recorded yet — treat as expired (needs fetching).
        return EsiFreshness(
            job_key=job_key,
            total_endpoints=0,
            fresh_endpoints=0,
            expired_endpoints=0,
            earliest_expires_seconds=-1,
        )

    total = len(rows)
    fresh = 0
    earliest_seconds = float("inf")

    for row in rows:
        expires_in = int(row.get("expires_in_seconds") or -1)
        if expires_in > 0:
            fresh += 1
        earliest_seconds = min(earliest_seconds, expires_in)

    return EsiFreshness(
        job_key=job_key,
        total_endpoints=total,
        fresh_endpoints=fresh,
        expired_endpoints=total - fresh,
        earliest_expires_seconds=earliest_seconds if earliest_seconds != float("inf") else -1,
    )


def compute_esi_error_rate(db: SupplyCoreDb, window_minutes: int = 10) -> ErrorRate:
    """Check recent ESI error rate from esi_rate_limit_observations."""
    try:
        row = db.fetch_one(
            """SELECT
                COUNT(*) AS total,
                SUM(status_code IN (429, 420, 500, 502, 503, 504)) AS errors,
                SUM(status_code = 429) AS rate_limited
               FROM esi_rate_limit_observations
               WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s MINUTE)""",
            (window_minutes,),
        ) or {}
    except Exception as exc:
        logger.warning("Failed to compute ESI error rate: %s", exc)
        return ErrorRate(total_requests=0, error_count=0, rate_limited_count=0, window_minutes=window_minutes)

    return ErrorRate(
        total_requests=int(row.get("total") or 0),
        error_count=int(row.get("errors") or 0),
        rate_limited_count=int(row.get("rate_limited") or 0),
        window_minutes=window_minutes,
    )
