from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def run_rebuild_ai_briefings(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="rebuild_ai_briefings", phase="C", objective="ai briefings")
