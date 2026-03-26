from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Callable

from ..db import SupplyCoreDb


def run_sync_phase_job(
    db: SupplyCoreDb,
    *,
    job_key: str,
    phase: str,
    objective: str,
    processor: Callable[[SupplyCoreDb], dict[str, Any]],
) -> dict[str, Any]:
    started = time.perf_counter()
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    try:
        payload = processor(db)
        db_now = db.fetch_one("SELECT UTC_TIMESTAMP() AS now_utc") or {}
        return {
            "status": str(payload.get("status") or "success"),
            "summary": str(payload.get("summary") or f"{job_key} completed phase {phase} objective: {objective}."),
            "rows_processed": max(0, int(payload.get("rows_processed") or 0)),
            "rows_written": max(0, int(payload.get("rows_written") or 0)),
            "computed_at": now,
            "warnings": list(payload.get("warnings") or []),
            "meta": {
                "job_key": job_key,
                "phase": phase,
                "objective": objective,
                "execution_language": "python",
                "subprocess_invoked": False,
                "db_now_utc": str(db_now.get("now_utc") or ""),
                "duration_ms": int((time.perf_counter() - started) * 1000),
                **dict(payload.get("meta") or {}),
            },
        }
    except Exception as exc:
        return {
            "status": "failed",
            "summary": f"{job_key} failed in phase {phase}: {exc}",
            "rows_processed": 0,
            "rows_written": 0,
            "computed_at": now,
            "warnings": [f"{type(exc).__name__}: {exc}"],
            "meta": {
                "job_key": job_key,
                "phase": phase,
                "objective": objective,
                "execution_language": "python",
                "subprocess_invoked": False,
                "duration_ms": int((time.perf_counter() - started) * 1000),
            },
        }
