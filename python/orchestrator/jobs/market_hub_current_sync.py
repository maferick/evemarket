from __future__ import annotations

from ..db import SupplyCoreDb
from ..esi_market_adapter import EsiMarketAdapter, parse_esi_datetime
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    adapter = EsiMarketAdapter(timeout_seconds=30)
    sources = db.fetch_market_hub_sources(limit=4)
    warnings: list[str] = []
    rows_processed = 0
    rows_written = 0
    for source in sources:
        source_id = int(source.get("source_id") or 0)
        region_id = int(source.get("region_id") or 0)
        if source_id <= 0 or region_id <= 0:
            continue
        orders: list[dict[str, object]] = []
        try:
            first_page = adapter.fetch_region_orders(region_id=region_id, order_type="all", page=1)
            orders.extend(first_page.orders)
            max_pages = min(4, first_page.pages)
            for page in range(2, max_pages + 1):
                response = adapter.fetch_region_orders(region_id=region_id, order_type="all", page=page)
                orders.extend(response.orders)
        except Exception as exc:
            warnings.append(f"market_hub source {source_id} region {region_id} fetch failed: {exc}")
            continue

        normalized_orders: list[dict[str, object]] = []
        for order in orders:
            row = dict(order)
            row["issued"] = parse_esi_datetime(row.get("issued"))
            row["expires"] = parse_esi_datetime(row.get("expires"))
            normalized_orders.append(row)
        observed_at = parse_esi_datetime(None)
        ingest_stats = db.replace_market_orders_for_source(
            source_type="market_hub",
            source_id=source_id,
            observed_at=observed_at,
            orders=normalized_orders,
        )
        rows_processed += int(ingest_stats["rows_processed"])
        rows_written += int(ingest_stats["rows_written"])

    stats = db.refresh_market_order_current_projection(source_type="market_hub")
    rows_processed += int(stats["rows_processed"])
    rows_written += int(stats["rows_written"])
    if not sources:
        warnings.append("No configured NPC market-hub sources were found (trading_stations + ref_npc_stations join returned empty).")
    db.upsert_sync_state(
        dataset_key="market_hub.orders.current",
        status="success",
        row_count=rows_written,
        cursor=None,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Ingested/projected market-hub orders ({rows_written} writes across {len(sources)} sources).",
        "meta": {"dataset_key": "market_hub.orders.current"},
    }


def run_market_hub_current_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="market_hub_current_sync", phase="A", objective="market snapshots", processor=_processor)
