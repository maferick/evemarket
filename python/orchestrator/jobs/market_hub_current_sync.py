from __future__ import annotations

from ..db import SupplyCoreDb
from ..esi_market_adapter import EsiMarketAdapter, parse_esi_datetime
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    adapter = EsiMarketAdapter(timeout_seconds=30)
    sources: list[dict[str, object]] = []
    fetch_from_settings = getattr(db, "fetch_market_hub_sources_from_settings", None)
    if callable(fetch_from_settings):
        sources = list(fetch_from_settings(limit=4))
    if not sources:
        legacy_fetch = getattr(db, "fetch_market_hub_sources", None)
        if callable(legacy_fetch):
            sources = list(legacy_fetch(limit=4))
    access_token = db.fetch_latest_esi_access_token()
    warnings: list[str] = []
    rows_processed = 0
    rows_written = 0
    for source in sources:
        source_id = int(source.get("source_id") or 0)
        region_id = int(source.get("region_id") or 0)
        source_kind = str(source.get("source_kind") or "npc_station")
        if source_id <= 0:
            continue
        orders: list[dict[str, object]] = []
        try:
            if source_kind == "structure":
                if not access_token:
                    warnings.append(f"market_hub source {source_id} is a structure, but no active ESI OAuth token is available.")
                    continue
                first_page = adapter.fetch_structure_orders(structure_id=source_id, access_token=access_token, page=1)
            else:
                if region_id <= 0:
                    warnings.append(f"market_hub source {source_id} is missing region metadata.")
                    continue
                first_page = adapter.fetch_region_orders(region_id=region_id, order_type="all", page=1)
            orders.extend(first_page.orders)
            max_pages = min(20, first_page.pages)
            for page in range(2, max_pages + 1):
                if source_kind == "structure":
                    response = adapter.fetch_structure_orders(structure_id=source_id, access_token=access_token, page=page)
                else:
                    response = adapter.fetch_region_orders(region_id=region_id, order_type="all", page=page)
                orders.extend(response.orders)
        except Exception as exc:
            warnings.append(f"market_hub source {source_id} fetch failed: {exc}")
            continue

        if source_kind == "npc_station":
            validated_orders: list[dict[str, object]] = []
            for order in orders:
                if not isinstance(order, dict):
                    raise ValueError(f"market_hub source {source_id} returned a non-object order payload.")
                if int(order.get("location_id") or 0) == source_id:
                    validated_orders.append(order)
            orders = validated_orders

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
        warnings.append("No configured market hub source was found. Save Settings → Trading Stations before running this sync.")
    if rows_processed == 0:
        warnings.append("No market-hub orders were fetched from ESI during this run.")
    hub_ref = db.fetch_app_setting("market_station_id", "")
    dataset_key = f"market.hub.{hub_ref}.orders.current" if hub_ref else "market_hub.orders.current"
    db.upsert_sync_state(
        dataset_key=dataset_key,
        status="success",
        row_count=rows_written,
        cursor=None,
    )
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Ingested/projected market-hub orders ({rows_written} writes across {len(sources)} sources).",
        "meta": {"dataset_key": dataset_key},
    }


def run_market_hub_current_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="market_hub_current_sync", phase="A", objective="market snapshots", processor=_processor)
