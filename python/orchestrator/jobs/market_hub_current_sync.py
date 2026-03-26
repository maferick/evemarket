from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def run_market_hub_current_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="market_hub_current_sync", phase="A", objective="market snapshots")
