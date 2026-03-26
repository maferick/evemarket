from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def run_forecasting_ai_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="forecasting_ai_sync", phase="C", objective="forecasting outputs")
