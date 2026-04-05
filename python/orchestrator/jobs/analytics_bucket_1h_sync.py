from __future__ import annotations

from ..db import SupplyCoreDb
from ..influx_writer import RollupInfluxBridge
from .sync_runtime import run_sync_phase_job

# Hourly killmail rollup rebuild window. 72h gives the detector and
# activity-priority page a full 3-day view at hourly resolution even if
# the job skips a couple of cycles.
_KILLMAIL_ROLLUP_BACKFILL_HOURS = 72


def _dual_write_stock(db: SupplyCoreDb, bridge: RollupInfluxBridge) -> None:
    if not bridge.enabled:
        return
    rows = db.fetch_all(
        "SELECT bucket_start, source_type, source_id, type_id, sample_count, "
        "stock_units_sum, listing_count_sum, local_stock_units, listing_count "
        "FROM market_item_stock_1h "
        "WHERE bucket_start >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 48 HOUR)"
    )
    for row in rows:
        # DECIMAL(20,2) columns must be floats in Influx to match the
        # existing legacy-exporter schema. See the daily job for the
        # full explanation.
        bridge.enqueue_market_stock(
            bucket_start=row["bucket_start"],
            source_type=row["source_type"],
            source_id=int(row["source_id"]),
            type_id=int(row["type_id"]),
            window="1h",
            fields={
                "sample_count": int(row.get("sample_count") or 0),
                "stock_units_sum": float(row.get("stock_units_sum") or 0),
                "listing_count_sum": float(row.get("listing_count_sum") or 0),
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
        "FROM market_item_price_1h "
        "WHERE bucket_start >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 48 HOUR)"
    )
    for row in rows:
        bridge.enqueue_market_price(
            bucket_start=row["bucket_start"],
            source_type=row["source_type"],
            source_id=int(row["source_id"]),
            type_id=int(row["type_id"]),
            window="1h",
            fields={
                "sample_count": int(row.get("sample_count") or 0),
                "listing_count_sum": float(row.get("listing_count_sum") or 0),
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


def _rebuild_killmail_item_loss_1h(db: SupplyCoreDb, hours: int) -> int:
    """Rebuild the last ``hours`` of ``killmail_item_loss_1h`` from
    ``killmail_events`` JOIN ``killmail_items``. Idempotent via
    ON DUPLICATE KEY UPDATE.
    """
    db.execute(
        f"""INSERT INTO killmail_item_loss_1h (
                bucket_start, type_id, doctrine_fit_id, doctrine_group_id, hull_type_id,
                loss_count, quantity_lost, victim_count, killmail_count
            )
            SELECT
                DATE_FORMAT(e.effective_killmail_at, '%Y-%m-%d %H:00:00') AS bucket_start,
                i.item_type_id AS type_id,
                NULL AS doctrine_fit_id,
                NULL AS doctrine_group_id,
                e.victim_ship_type_id AS hull_type_id,
                COUNT(*) AS loss_count,
                SUM(COALESCE(i.quantity_destroyed, 0) + COALESCE(i.quantity_dropped, 0)) AS quantity_lost,
                COUNT(DISTINCT COALESCE(e.victim_character_id, e.sequence_id)) AS victim_count,
                COUNT(DISTINCT e.sequence_id) AS killmail_count
            FROM killmail_events e
            INNER JOIN killmail_items i ON i.sequence_id = e.sequence_id
            WHERE e.effective_killmail_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {int(hours)} HOUR)
              AND i.item_type_id IS NOT NULL
            GROUP BY DATE_FORMAT(e.effective_killmail_at, '%Y-%m-%d %H:00:00'),
                     i.item_type_id, e.victim_ship_type_id
            ON DUPLICATE KEY UPDATE
                loss_count    = VALUES(loss_count),
                quantity_lost = VALUES(quantity_lost),
                victim_count  = VALUES(victim_count),
                killmail_count = VALUES(killmail_count),
                updated_at    = CURRENT_TIMESTAMP"""
    )
    return int(db.fetch_scalar(
        f"SELECT COUNT(*) FROM killmail_item_loss_1h "
        f"WHERE bucket_start >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {int(hours)} HOUR)"
    ) or 0)


def _dual_write_killmail_item_loss_1h(db: SupplyCoreDb, bridge: RollupInfluxBridge, hours: int) -> None:
    if not bridge.enabled:
        return
    rows = db.fetch_all(
        f"SELECT bucket_start, type_id, hull_type_id, loss_count, quantity_lost, victim_count, killmail_count "
        f"FROM killmail_item_loss_1h "
        f"WHERE bucket_start >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {int(hours)} HOUR)"
    )
    for row in rows:
        bridge.enqueue_killmail_item_loss(
            bucket_start=row["bucket_start"],
            type_id=int(row.get("type_id") or 0),
            hull_type_id=int(row.get("hull_type_id") or 0),
            window="1h",
            fields={
                "loss_count": int(row.get("loss_count") or 0),
                "quantity_lost": int(row.get("quantity_lost") or 0),
                "victim_count": int(row.get("victim_count") or 0),
                "killmail_count": int(row.get("killmail_count") or 0),
            },
        )
    bridge.flush()


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    rows_processed = db.fetch_scalar(
        "SELECT COUNT(*) FROM market_orders_history WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 48 HOUR)"
    )
    db.execute(
        """INSERT INTO market_item_stock_1h (
                bucket_start, source_type, source_id, type_id, sample_count, stock_units_sum, listing_count_sum, local_stock_units, listing_count
            )
            SELECT
                DATE_FORMAT(observed_at, '%Y-%m-%d %H:00:00') AS bucket_start,
                source_type,
                source_id,
                type_id,
                COUNT(*) AS sample_count,
                SUM(volume_remain) AS stock_units_sum,
                COUNT(*) AS listing_count_sum,
                SUM(volume_remain) AS local_stock_units,
                COUNT(*) AS listing_count
            FROM market_orders_history
            WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 48 HOUR)
            GROUP BY DATE_FORMAT(observed_at, '%Y-%m-%d %H:00:00'), source_type, source_id, type_id
            ON DUPLICATE KEY UPDATE
                sample_count = VALUES(sample_count), stock_units_sum = VALUES(stock_units_sum), listing_count_sum = VALUES(listing_count_sum),
                local_stock_units = VALUES(local_stock_units), listing_count = VALUES(listing_count)"""
    )
    stock_written = db.fetch_scalar(
        "SELECT COUNT(*) FROM market_item_stock_1h WHERE bucket_start >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 48 HOUR)"
    )
    db.execute(
        """INSERT INTO market_item_price_1h (
                bucket_start, source_type, source_id, type_id, sample_count, listing_count_sum, avg_price_sum,
                weighted_price_numerator, weighted_price_denominator, listing_count, min_price, max_price, avg_price, weighted_price
            )
            SELECT
                DATE_FORMAT(observed_at, '%Y-%m-%d %H:00:00') AS bucket_start,
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
            WHERE observed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 48 HOUR)
            GROUP BY DATE_FORMAT(observed_at, '%Y-%m-%d %H:00:00'), source_type, source_id, type_id
            ON DUPLICATE KEY UPDATE
                sample_count = VALUES(sample_count), listing_count_sum = VALUES(listing_count_sum), avg_price_sum = VALUES(avg_price_sum),
                weighted_price_numerator = VALUES(weighted_price_numerator), weighted_price_denominator = VALUES(weighted_price_denominator),
                listing_count = VALUES(listing_count), min_price = VALUES(min_price), max_price = VALUES(max_price),
                avg_price = VALUES(avg_price), weighted_price = VALUES(weighted_price)"""
    )
    price_written = db.fetch_scalar(
        "SELECT COUNT(*) FROM market_item_price_1h WHERE bucket_start >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 48 HOUR)"
    )

    # ── Killmail item loss hourly rollup ──────────────────────────────
    # Restores the orphaned hourly killmail rollup path (see
    # analytics_bucket_1d_sync for the daily version + context).
    item_loss_hourly_written = _rebuild_killmail_item_loss_1h(db, _KILLMAIL_ROLLUP_BACKFILL_HOURS)

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
            _dual_write_killmail_item_loss_1h(db, bridge, _KILLMAIL_ROLLUP_BACKFILL_HOURS)
            influx_written = bridge.written_count
    except Exception as exc:
        import sys
        print(f"[analytics_bucket_1h_sync] InfluxDB dual-write failed (non-fatal): {exc}", file=sys.stderr)

    return {
        "rows_processed": rows_processed,
        "rows_written": stock_written + price_written + item_loss_hourly_written,
        "warnings": [] if rows_processed > 0 else ["No history rows available in the last 48h for hourly analytics buckets."],
        "summary": (
            f"Upserted {stock_written} stock, {price_written} price, "
            f"{item_loss_hourly_written} item-loss hourly bucket rows. "
            f"InfluxDB: {influx_written} points."
        ),
        "meta": {
            "window_hours": 48,
            "killmail_rollup_backfill_hours": _KILLMAIL_ROLLUP_BACKFILL_HOURS,
            "item_loss_hourly_rows": item_loss_hourly_written,
            "influx_points_written": influx_written,
        },
    }


def run_analytics_bucket_1h_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="analytics_bucket_1h_sync", phase="B", objective="hourly rollups", processor=_processor)
