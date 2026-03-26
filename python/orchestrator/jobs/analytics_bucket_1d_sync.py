from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows_processed = db.fetch_scalar(
        "SELECT COUNT(*) FROM market_orders_history WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 14 DAY)"
    )
    stock_written = db.execute(
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
    price_written = db.execute(
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
    return {
        "rows_processed": rows_processed,
        "rows_written": stock_written + price_written,
        "warnings": [] if rows_processed > 0 else ["No history rows available in the last 14d for daily analytics buckets."],
        "summary": f"Upserted {stock_written} stock and {price_written} price daily bucket rows.",
        "meta": {"window_days": 14},
    }


def run_analytics_bucket_1d_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="analytics_bucket_1d_sync", phase="B", objective="daily rollups", processor=_processor)
