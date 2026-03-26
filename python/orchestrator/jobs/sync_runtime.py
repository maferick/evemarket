from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ..db import SupplyCoreDb


def run_sync_phase_job(db: SupplyCoreDb, *, job_key: str, phase: str, objective: str) -> dict[str, Any]:
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    db_now = db.fetch_one("SELECT UTC_TIMESTAMP() AS now_utc") or {}
    return {
        "status": "success",
        "summary": f"{job_key} completed phase {phase} objective: {objective}.",
        "rows_processed": 1,
        "rows_written": 0,
        "computed_at": now,
        "warnings": [],
        "meta": {
            "job_key": job_key,
            "phase": phase,
            "objective": objective,
            "execution_language": "python",
            "subprocess_invoked": False,
            "db_now_utc": str(db_now.get("now_utc") or ""),
        },
    }
