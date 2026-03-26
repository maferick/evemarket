from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    stats = db.materialize_market_history_from_projection(source_type="market_hub")
    warnings: list[str] = []
    if stats["rows_processed"] == 0:
        warnings.append("No market_hub projection rows were available for historical materialization.")
    db.upsert_sync_state(dataset_key="market_hub.orders.history", status="success", row_count=stats["rows_written"])
    return {
        "rows_processed": stats["rows_processed"],
        "rows_written": stats["rows_written"],
        "warnings": warnings,
        "summary": f"Materialized {stats['rows_written']} market_hub historical snapshot rows.",
        "meta": {"dataset_key": "market_hub.orders.history"},
    }


def run_market_hub_historical_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="market_hub_historical_sync", phase="A", objective="market history", processor=_processor)
