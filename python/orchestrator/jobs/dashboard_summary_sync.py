from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def run_dashboard_summary_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="dashboard_summary_sync", phase="B", objective="dashboard summaries")
