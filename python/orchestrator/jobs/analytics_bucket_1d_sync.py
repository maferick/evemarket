from __future__ import annotations

from ..db import SupplyCoreDb
from ..influx_writer import RollupInfluxBridge
from .sync_runtime import run_sync_phase_job

# How many days of history to rebuild on every run. Idempotent thanks
# to ``ON DUPLICATE KEY UPDATE`` so a larger window is safe; it just
# costs more query time. 30 days is enough to cover the full window
# that the auto-doctrine detector and activity-priority page read.
_KILLMAIL_ROLLUP_BACKFILL_DAYS = 30


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


def _rebuild_killmail_hull_loss_1d(db: SupplyCoreDb, days: int) -> int:
    """Rebuild the last ``days`` of ``killmail_hull_loss_1d`` from
    ``killmail_events``. Idempotent via ON DUPLICATE KEY UPDATE. The
    ``doctrine_fit_id`` / ``doctrine_group_id`` columns are written as
    NULL here and later repopulated by compute_auto_doctrines for the
    hulls that have a single active auto-doctrine.
    """
    db.execute(
        f"""INSERT INTO killmail_hull_loss_1d (
                bucket_start, hull_type_id, doctrine_fit_id, doctrine_group_id,
                loss_count, victim_count, killmail_count
            )
            SELECT
                DATE(e.effective_killmail_at) AS bucket_start,
                e.victim_ship_type_id AS hull_type_id,
                NULL AS doctrine_fit_id,
                NULL AS doctrine_group_id,
                COUNT(*) AS loss_count,
                COUNT(DISTINCT COALESCE(e.victim_character_id, e.sequence_id)) AS victim_count,
                COUNT(DISTINCT e.sequence_id) AS killmail_count
            FROM killmail_events e
            WHERE e.effective_killmail_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {int(days)} DAY)
              AND e.victim_ship_type_id IS NOT NULL
            GROUP BY DATE(e.effective_killmail_at), e.victim_ship_type_id
            ON DUPLICATE KEY UPDATE
                loss_count    = VALUES(loss_count),
                victim_count  = VALUES(victim_count),
                killmail_count = VALUES(killmail_count),
                updated_at    = CURRENT_TIMESTAMP"""
    )
    return int(db.fetch_scalar(
        f"SELECT COUNT(*) FROM killmail_hull_loss_1d "
        f"WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL {int(days)} DAY)"
    ) or 0)


def _rebuild_killmail_item_loss_1d(db: SupplyCoreDb, days: int) -> int:
    """Rebuild the last ``days`` of ``killmail_item_loss_1d`` from
    ``killmail_events`` JOIN ``killmail_items``. Counts every item row
    regardless of slot (cargo / fitted / drone bay) so downstream
    consumers can filter by flag if they only want fitted modules.
    """
    db.execute(
        f"""INSERT INTO killmail_item_loss_1d (
                bucket_start, type_id, doctrine_fit_id, doctrine_group_id, hull_type_id,
                loss_count, quantity_lost, victim_count, killmail_count
            )
            SELECT
                DATE(e.effective_killmail_at) AS bucket_start,
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
            WHERE e.effective_killmail_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {int(days)} DAY)
              AND i.item_type_id IS NOT NULL
            GROUP BY DATE(e.effective_killmail_at), i.item_type_id, e.victim_ship_type_id
            ON DUPLICATE KEY UPDATE
                loss_count    = VALUES(loss_count),
                quantity_lost = VALUES(quantity_lost),
                victim_count  = VALUES(victim_count),
                killmail_count = VALUES(killmail_count),
                updated_at    = CURRENT_TIMESTAMP"""
    )
    return int(db.fetch_scalar(
        f"SELECT COUNT(*) FROM killmail_item_loss_1d "
        f"WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL {int(days)} DAY)"
    ) or 0)


def _dual_write_killmail_hull_loss_1d(db: SupplyCoreDb, bridge: RollupInfluxBridge, days: int) -> None:
    if not bridge.enabled:
        return
    rows = db.fetch_all(
        f"SELECT bucket_start, hull_type_id, loss_count, victim_count, killmail_count "
        f"FROM killmail_hull_loss_1d "
        f"WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL {int(days)} DAY)"
    )
    for row in rows:
        bridge.enqueue_killmail_hull_loss(
            bucket_start=row["bucket_start"],
            hull_type_id=int(row.get("hull_type_id") or 0),
            window="1d",
            fields={
                "loss_count": int(row.get("loss_count") or 0),
                "victim_count": int(row.get("victim_count") or 0),
                "killmail_count": int(row.get("killmail_count") or 0),
            },
        )
    bridge.flush()


def _dual_write_killmail_item_loss_1d(db: SupplyCoreDb, bridge: RollupInfluxBridge, days: int) -> None:
    if not bridge.enabled:
        return
    rows = db.fetch_all(
        f"SELECT bucket_start, type_id, hull_type_id, loss_count, quantity_lost, victim_count, killmail_count "
        f"FROM killmail_item_loss_1d "
        f"WHERE bucket_start >= DATE_SUB(UTC_DATE(), INTERVAL {int(days)} DAY)"
    )
    for row in rows:
        bridge.enqueue_killmail_item_loss(
            bucket_start=row["bucket_start"],
            type_id=int(row.get("type_id") or 0),
            hull_type_id=int(row.get("hull_type_id") or 0),
            window="1d",
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

    # ── Killmail loss rollups ─────────────────────────────────────────
    # The legacy PHP path (db_time_series_refresh_killmail_item_loss)
    # populated these tables, but the Python analytics jobs that
    # replaced the PHP refresh only ported the market half. This block
    # restores the killmail rollups so downstream consumers (auto
    # doctrine detector, activity-priority page, buy-all targeting,
    # Influx export) can read from them again. Idempotent + bounded.
    hull_loss_written = _rebuild_killmail_hull_loss_1d(db, _KILLMAIL_ROLLUP_BACKFILL_DAYS)
    item_loss_written = _rebuild_killmail_item_loss_1d(db, _KILLMAIL_ROLLUP_BACKFILL_DAYS)

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
            _dual_write_killmail_hull_loss_1d(db, bridge, _KILLMAIL_ROLLUP_BACKFILL_DAYS)
            _dual_write_killmail_item_loss_1d(db, bridge, _KILLMAIL_ROLLUP_BACKFILL_DAYS)
            influx_written = bridge.written_count
    except Exception as exc:
        import sys
        print(f"[analytics_bucket_1d_sync] InfluxDB dual-write failed (non-fatal): {exc}", file=sys.stderr)

    return {
        "rows_processed": rows_processed,
        "rows_written": stock_written + price_written + hull_loss_written + item_loss_written,
        "warnings": [] if rows_processed > 0 else ["No history rows available in the last 14d for daily analytics buckets."],
        "summary": (
            f"Upserted {stock_written} stock, {price_written} price, "
            f"{hull_loss_written} hull-loss, {item_loss_written} item-loss daily bucket rows. "
            f"InfluxDB: {influx_written} points."
        ),
        "meta": {
            "window_days": 14,
            "killmail_rollup_backfill_days": _KILLMAIL_ROLLUP_BACKFILL_DAYS,
            "hull_loss_rows": hull_loss_written,
            "item_loss_rows": item_loss_written,
            "influx_points_written": influx_written,
        },
    }


def run_analytics_bucket_1d_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="analytics_bucket_1d_sync", phase="B", objective="daily rollups", processor=_processor)
