from __future__ import annotations

from typing import Any

from ..db import SupplyCoreDb
from ..esi_market_adapter import EsiMarketAdapter, parse_esi_datetime
from .sync_runtime import run_sync_phase_job


def _normalize_source_kind(source_kind: str, source_id: int) -> str:
    normalized = source_kind.strip().lower()
    if normalized == "npc_station":
        return "npc_station"
    if normalized in {"player_structure", "structure"}:
        return "player_structure"
    # Fallback guard for stale/legacy metadata: NPC stations are 64-bit IDs
    # in known low ranges, while Upwell structure IDs are significantly larger.
    return "npc_station" if 0 < source_id < 1_000_000_00000 else "player_structure"


def _processor(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    from ..esi_gateway import build_gateway
    redis_cfg = dict((raw_config or {}).get("redis") or {})
    gateway = build_gateway(db=db, redis_config=redis_cfg) if redis_cfg.get("enabled") else None
    adapter = EsiMarketAdapter(timeout_seconds=30, gateway=gateway)
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
    successful_sources = 0
    failed_sources = 0
    for source in sources:
        source_id = int(source.get("source_id") or 0)
        region_id = int(source.get("region_id") or 0)
        source_kind = _normalize_source_kind(str(source.get("source_kind") or "npc_station"), source_id)
        if source_id <= 0:
            continue
        orders: list[dict[str, object]] = []
        resolved_region_id = region_id
        filter_location_id: int | None = source_id if source_kind == "player_structure" else None
        structure_orders_mode = False

        try:
            if source_kind == "npc_station" and resolved_region_id <= 0:
                fetch_npc_region = getattr(db, "fetch_region_id_for_npc_station", None)
                if callable(fetch_npc_region):
                    resolved_region_id = int(fetch_npc_region(station_id=source_id))

            if source_kind == "player_structure" and resolved_region_id <= 0 and access_token:
                structure_meta = adapter.fetch_structure_metadata(structure_id=source_id, access_token=access_token)
                system_id = int(structure_meta.get("solar_system_id") or 0)
                fetch_region = getattr(db, "fetch_region_id_for_system", None)
                if callable(fetch_region):
                    resolved_region_id = int(fetch_region(system_id=system_id))

            if resolved_region_id > 0:
                first_page = adapter.fetch_region_orders(region_id=resolved_region_id, order_type="all", page=1)
            elif source_kind == "player_structure":
                if not access_token:
                    warnings.append(f"market_hub source {source_id} is a player structure, but no active ESI OAuth token is available.")
                    failed_sources += 1
                    continue
                first_page = adapter.fetch_structure_orders(structure_id=source_id, access_token=access_token, page=1)
                structure_orders_mode = True
            else:
                warnings.append(f"market_hub source {source_id} is an NPC station but region metadata could not be resolved.")
                failed_sources += 1
                continue
            orders.extend(first_page.orders)
            max_pages = min(20, first_page.pages)
            for page in range(2, max_pages + 1):
                if structure_orders_mode:
                    response = adapter.fetch_structure_orders(structure_id=source_id, access_token=access_token, page=page)
                else:
                    response = adapter.fetch_region_orders(region_id=resolved_region_id, order_type="all", page=page)
                orders.extend(response.orders)
        except Exception as exc:
            warnings.append(f"market_hub source {source_id} fetch failed: {exc}")
            failed_sources += 1
            continue

        if filter_location_id is not None:
            validated_orders: list[dict[str, object]] = []
            for order in orders:
                if not isinstance(order, dict):
                    raise ValueError(f"market_hub source {source_id} returned a non-object order payload.")
                if int(order.get("location_id") or 0) == filter_location_id:
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
        successful_sources += 1

    stats = db.refresh_market_order_current_projection(source_type="market_hub")
    rows_processed += int(stats["rows_processed"])
    rows_written += int(stats["rows_written"])
    if not sources:
        warnings.append("No configured market hub source was found. Save Settings → Trading Stations before running this sync.")
    if rows_processed == 0:
        warnings.append("No market-hub orders were fetched from ESI during this run.")
    hub_ref = db.fetch_app_setting("market_station_id", "")
    dataset_key = f"market.hub.{hub_ref}.orders.current" if hub_ref else "market_hub.orders.current"
    sync_state_status = "failed" if (failed_sources > 0 and successful_sources == 0) else "success"
    db.upsert_sync_state(
        dataset_key=dataset_key,
        status=sync_state_status,
        row_count=rows_written,
        cursor=None,
        error_message=warnings[0] if sync_state_status == "failed" and warnings else None,
    )
    return {
        "status": sync_state_status,
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": warnings,
        "summary": f"Ingested/projected market-hub orders ({rows_written} writes across {len(sources)} sources).",
        "meta": {"dataset_key": dataset_key},
    }


def run_market_hub_current_sync(db: SupplyCoreDb, raw_config: dict[str, Any] | None = None) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="market_hub_current_sync", phase="A", objective="market snapshots", processor=lambda d: _processor(d, raw_config))
