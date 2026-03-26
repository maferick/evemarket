from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    stats = db.materialize_market_history_from_projection(source_type="alliance_structure")
    warnings: list[str] = []
    if stats["rows_processed"] == 0:
        warnings.append("No alliance_structure projection rows were available for historical materialization.")
    db.upsert_sync_state(dataset_key="alliance.structure.orders.history", status="success", row_count=stats["rows_written"])
    return {
        "rows_processed": stats["rows_processed"],
        "rows_written": stats["rows_written"],
        "warnings": warnings,
        "summary": f"Materialized {stats['rows_written']} alliance_structure historical snapshot rows.",
        "meta": {"dataset_key": "alliance.structure.orders.history"},
    }


def run_alliance_historical_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="alliance_historical_sync", phase="A", objective="alliance history", processor=_processor)
