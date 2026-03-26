from __future__ import annotations

from ..db import SupplyCoreDb
from ..esi_market_adapter import EsiMarketAdapter, parse_esi_datetime
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    adapter = EsiMarketAdapter(timeout_seconds=30)
    access_token = db.fetch_latest_esi_access_token()
    structure_ids = db.fetch_alliance_structure_sources(limit=3)
    warnings: list[str] = []
    if not access_token:
        warnings.append("No active ESI OAuth token found; alliance structure orders were not fetched from ESI.")
    rows_processed = 0
    rows_written = 0
    if access_token:
        for structure_id in structure_ids:
            orders: list[dict[str, object]] = []
            try:
                first_page = adapter.fetch_structure_orders(structure_id=structure_id, access_token=access_token, page=1)
                orders.extend(first_page.orders)
                max_pages = min(4, first_page.pages)
                for page in range(2, max_pages + 1):
                    response = adapter.fetch_structure_orders(structure_id=structure_id, access_token=access_token, page=page)
                    orders.extend(response.orders)
            except Exception as exc:
                warnings.append(f"alliance structure {structure_id} fetch failed: {exc}")
                continue
            normalized_orders: list[dict[str, object]] = []
            for order in orders:
                row = dict(order)
                row["issued"] = parse_esi_datetime(row.get("issued"))
                row["expires"] = parse_esi_datetime(row.get("expires"))
                normalized_orders.append(row)
            observed_at = parse_esi_datetime(None)
            ingest_stats = db.replace_market_orders_for_source(
                source_type="alliance_structure",
                source_id=structure_id,
                observed_at=observed_at,
                orders=normalized_orders,
            )
            rows_processed += int(ingest_stats["rows_processed"])
            rows_written += int(ingest_stats["rows_written"])

    stats = db.refresh_market_order_current_projection(source_type="alliance_structure")
    rows_processed += int(stats["rows_processed"])
    rows_written += int(stats["rows_written"])
    if not structure_ids:
        warnings.append("No alliance_structure_metadata sources were available for ESI structure sync.")
    db.upsert_sync_state(
        dataset_key="alliance.structure.orders.current",
        status="success",
        row_count=rows_written,
        cursor=None,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Ingested/projected alliance structure orders ({rows_written} writes across {len(structure_ids)} structures).",
        "meta": {"dataset_key": "alliance.structure.orders.current"},
    }


def run_alliance_current_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="alliance_current_sync", phase="A", objective="alliance snapshots", processor=_processor)
