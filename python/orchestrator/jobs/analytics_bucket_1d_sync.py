from __future__ import annotations

from ..db import SupplyCoreDb
from ..influx_writer import RollupInfluxBridge
from .sync_runtime import run_sync_phase_job


def _dual_write_stock(db: SupplyCoreDb, bridge: RollupInfluxBridge) -> None:
    if not bridge.enabled:
        return
    rows = db.fetch_all(
        "SELECT bucket_start, source_type, source_id, type_id, sample_count, "
        "stock_units_sum, listing_count_sum, local_stock_units, listing_count "
        "FROM market_item_stock_1d "
        "WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL 14 DAY)"
    )
    for row in rows:
        bridge.enqueue_market_stock(
            bucket_start=row["bucket_start"],
            source_type=row["source_type"],
            source_id=int(row["source_id"]),
            type_id=int(row["type_id"]),
            window="1d",
            fields={
                "sample_count": int(row.get("sample_count") or 0),
                "stock_units_sum": int(row.get("stock_units_sum") or 0),
                "listing_count_sum": int(row.get("listing_count_sum") or 0),
                "local_stock_units": int(row.get("local_stock_units") or 0),
                "listing_count": int(row.get("listing_count") or 0),
            },
        )
    bridge.flush()


def _dual_write_price(db: SupplyCoreDb, bridge: RollupInfluxBridge) -> None:
    if not bridge.enabled:
        return
    rows = db.fetch_all(
        "SELECT bucket_start, source_type, source_id, type_id, sample_count, "
        "listing_count_sum, avg_price_sum, weighted_price_numerator, weighted_price_denominator, "
        "listing_count, min_price, max_price, avg_price, weighted_price "
        "FROM market_item_price_1d "
        "WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL 14 DAY)"
    )
    for row in rows:
        bridge.enqueue_market_price(
            bucket_start=row["bucket_start"],
            source_type=row["source_type"],
            source_id=int(row["source_id"]),
            type_id=int(row["type_id"]),
            window="1d",
            fields={
                "sample_count": int(row.get("sample_count") or 0),
                "listing_count_sum": int(row.get("listing_count_sum") or 0),
                "avg_price_sum": float(row.get("avg_price_sum") or 0),
                "weighted_price_numerator": float(row.get("weighted_price_numerator") or 0),
                "weighted_price_denominator": float(row.get("weighted_price_denominator") or 0),
                "listing_count": int(row.get("listing_count") or 0),
                "min_price": float(row.get("min_price") or 0),
                "max_price": float(row.get("max_price") or 0),
                "avg_price": float(row.get("avg_price") or 0),
                "weighted_price": float(row.get("weighted_price") or 0),
            },
        )
    bridge.flush()


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows_processed = db.fetch_scalar(
        "SELECT COUNT(*) FROM market_orders_history WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 14 DAY)"
    )
    db.execute(
        """INSERT INTO market_item_stock_1d (
                bucket_start, source_type, source_id, type_id, sample_count, stock_units_sum, listing_count_sum, local_stock_units, listing_count
            )
            SELECT
                DATE(observed_at) AS bucket_start,
                source_type,
                source_id,
                type_id,
                COUNT(*) AS sample_count,
                SUM(volume_remain) AS stock_units_sum,
                COUNT(*) AS listing_count_sum,
                SUM(volume_remain) AS local_stock_units,
                COUNT(*) AS listing_count
            FROM market_orders_history
            WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 14 DAY)
            GROUP BY DATE(observed_at), source_type, source_id, type_id
            ON DUPLICATE KEY UPDATE
                sample_count = VALUES(sample_count), stock_units_sum = VALUES(stock_units_sum), listing_count_sum = VALUES(listing_count_sum),
                local_stock_units = VALUES(local_stock_units), listing_count = VALUES(listing_count)"""
    )
    stock_written = db.fetch_scalar(
        "SELECT COUNT(*) FROM market_item_stock_1d WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL 14 DAY)"
    )
    db.execute(
        """INSERT INTO market_item_price_1d (
                bucket_start, source_type, source_id, type_id, sample_count, listing_count_sum, avg_price_sum,
                weighted_price_numerator, weighted_price_denominator, listing_count, min_price, max_price, avg_price, weighted_price
            )
            SELECT
                DATE(observed_at) AS bucket_start,
                source_type,
                source_id,
                type_id,
                COUNT(*) AS sample_count,
                COUNT(*) AS listing_count_sum,
                SUM(price) AS avg_price_sum,
                SUM(price * volume_remain) AS weighted_price_numerator,
                SUM(volume_remain) AS weighted_price_denominator,
                COUNT(*) AS listing_count,
                MIN(price) AS min_price,
                MAX(price) AS max_price,
                AVG(price) AS avg_price,
                SUM(price * volume_remain) / NULLIF(SUM(volume_remain), 0) AS weighted_price
            FROM market_orders_history
            WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 14 DAY)
            GROUP BY DATE(observed_at), source_type, source_id, type_id
            ON DUPLICATE KEY UPDATE
                sample_count = VALUES(sample_count), listing_count_sum = VALUES(listing_count_sum), avg_price_sum = VALUES(avg_price_sum),
                weighted_price_numerator = VALUES(weighted_price_numerator), weighted_price_denominator = VALUES(weighted_price_denominator),
                listing_count = VALUES(listing_count), min_price = VALUES(min_price), max_price = VALUES(max_price),
                avg_price = VALUES(avg_price), weighted_price = VALUES(weighted_price)"""
    )
    price_written = db.fetch_scalar(
        "SELECT COUNT(*) FROM market_item_price_1d WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL 14 DAY)"
    )

    # Dual-write to InfluxDB when enabled — reads back the freshly upserted
    # rollup rows so InfluxDB stays in sync without a separate export pass.
    influx_written = 0
    try:
        from ..config import load_php_runtime_config, resolve_app_root
        from pathlib import Path
        config = load_php_runtime_config(Path(resolve_app_root(__file__)))
        bridge = RollupInfluxBridge.from_config(config)
        if bridge.enabled:
            _dual_write_stock(db, bridge)
            _dual_write_price(db, bridge)
            influx_written = bridge.written_count
    except Exception as exc:
        import sys
        print(f"[analytics_bucket_1d_sync] InfluxDB dual-write failed (non-fatal): {exc}", file=sys.stderr)

    return {
        "rows_processed": rows_processed,
        "rows_written": stock_written + price_written,
        "warnings": [] if rows_processed > 0 else ["No history rows available in the last 14d for daily analytics buckets."],
        "summary": f"Upserted {stock_written} stock and {price_written} price daily bucket rows. InfluxDB: {influx_written} points.",
        "meta": {"window_days": 14, "influx_points_written": influx_written},
    }


def run_analytics_bucket_1d_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="analytics_bucket_1d_sync", phase="B", objective="daily rollups", processor=_processor)
