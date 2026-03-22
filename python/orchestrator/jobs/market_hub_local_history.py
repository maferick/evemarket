from __future__ import annotations

import gc
import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ..bridge import PhpBridge
from ..db import SupplyCoreDb
from ..worker_runtime import WorkerStats, resident_memory_bytes, utc_now_iso


def _payload_checksum(payload: Any) -> str:
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _market_snapshot_trade_date(observed_at: str, app_timezone: str) -> str:
    raw = observed_at.strip()
    if raw == "":
        return ""

    try:
        captured_at = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        tz = ZoneInfo(app_timezone.strip() or "UTC")
    except Exception:
        tz = UTC
        try:
            captured_at = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        except Exception:
            return ""

    return captured_at.astimezone(tz).strftime("%Y-%m-%d")


def _window_start_observed_at(window_days: int, app_timezone: str) -> str:
    safe_window_days = max(1, min(3650, int(window_days or 1)))
    try:
        tz = ZoneInfo(app_timezone.strip() or "UTC")
    except Exception:
        tz = UTC

    now_local = datetime.now(tz)
    window_start = datetime(
        year=now_local.year,
        month=now_local.month,
        day=now_local.day,
        tzinfo=tz,
    ) - timedelta(days=safe_window_days - 1)

    return window_start.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_spread_percent(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None

    spread_percent = round(float(value), 4)
    return None if abs(spread_percent) > 9999.9999 else spread_percent


def _finalize_snapshot_metric(metric: dict[str, Any], app_timezone: str, source_label: str) -> dict[str, Any] | None:
    type_id = int(metric.get("type_id") or 0)
    source_id = int(metric.get("source_id") or 0)
    observed_at = str(metric.get("observed_at") or "").strip()
    if type_id <= 0 or source_id <= 0 or observed_at == "":
        return None

    sell_price = round(float(metric["best_sell_price"]), 2) if metric.get("best_sell_price") is not None else None
    buy_price = round(float(metric["best_buy_price"]), 2) if metric.get("best_buy_price") is not None else None
    close_price = sell_price if sell_price is not None else buy_price
    if close_price is None:
        return None

    spread_value = None
    spread_percent = None
    if sell_price is not None and buy_price is not None:
        spread_value = round(sell_price - buy_price, 2)
        if buy_price > 0:
            spread_percent = _normalize_spread_percent((spread_value / buy_price) * 100.0)

    return {
        "source": source_label,
        "source_id": source_id,
        "type_id": type_id,
        "trade_date": _market_snapshot_trade_date(observed_at, app_timezone),
        "close_price": close_price,
        "buy_price": buy_price,
        "sell_price": sell_price,
        "spread_value": spread_value,
        "spread_percent": spread_percent,
        "volume": max(0, int(metric.get("total_volume") or 0)),
        "buy_order_count": max(0, int(metric.get("buy_order_count") or 0)),
        "sell_order_count": max(0, int(metric.get("sell_order_count") or 0)),
        "captured_at": observed_at,
    }


def _bucket_seed(snapshot_row: dict[str, Any]) -> dict[str, Any]:
    close_price = float(snapshot_row.get("close_price") or 0.0)
    return {
        "source": str(snapshot_row.get("source") or ""),
        "source_id": max(0, int(snapshot_row.get("source_id") or 0)),
        "type_id": max(0, int(snapshot_row.get("type_id") or 0)),
        "trade_date": str(snapshot_row.get("trade_date") or ""),
        "open_price": close_price,
        "high_price": close_price,
        "low_price": close_price,
        "close_price": close_price,
        "buy_price": snapshot_row.get("buy_price"),
        "sell_price": snapshot_row.get("sell_price"),
        "spread_value": snapshot_row.get("spread_value"),
        "spread_percent": _normalize_spread_percent(snapshot_row.get("spread_percent")),
        "volume": max(0, int(snapshot_row.get("volume") or 0)),
        "buy_order_count": max(0, int(snapshot_row.get("buy_order_count") or 0)),
        "sell_order_count": max(0, int(snapshot_row.get("sell_order_count") or 0)),
        "captured_at": str(snapshot_row.get("captured_at") or ""),
    }


def _bucket_observe(bucket: dict[str, Any], snapshot_row: dict[str, Any]) -> dict[str, Any]:
    close_price = float(snapshot_row.get("close_price") or 0.0)
    captured_at = str(snapshot_row.get("captured_at") or "").strip()
    current_captured_at = str(bucket.get("captured_at") or "").strip()

    bucket["high_price"] = max(float(bucket.get("high_price") or close_price), close_price)
    bucket["low_price"] = min(float(bucket.get("low_price") or close_price), close_price)
    if float(bucket.get("open_price") or 0.0) <= 0.0:
        bucket["open_price"] = close_price

    if current_captured_at == "" or (captured_at != "" and captured_at >= current_captured_at):
        bucket["close_price"] = close_price
        bucket["buy_price"] = snapshot_row.get("buy_price")
        bucket["sell_price"] = snapshot_row.get("sell_price")
        bucket["spread_value"] = snapshot_row.get("spread_value")
        bucket["spread_percent"] = _normalize_spread_percent(snapshot_row.get("spread_percent"))
        bucket["volume"] = max(0, int(snapshot_row.get("volume") or 0))
        bucket["buy_order_count"] = max(0, int(snapshot_row.get("buy_order_count") or 0))
        bucket["sell_order_count"] = max(0, int(snapshot_row.get("sell_order_count") or 0))
        bucket["captured_at"] = captured_at

    return bucket


def _rebuild_daily_rows(snapshot_metrics: list[dict[str, Any]], app_timezone: str, source_label: str) -> dict[str, Any]:
    daily_buckets: dict[str, dict[str, Any]] = {}
    snapshot_count = 0
    trade_dates: set[str] = set()

    for metric in snapshot_metrics:
        snapshot_row = _finalize_snapshot_metric(metric, app_timezone, source_label)
        if snapshot_row is None:
            continue

        trade_date = str(snapshot_row.get("trade_date") or "").strip()
        type_id = int(snapshot_row.get("type_id") or 0)
        source_id = int(snapshot_row.get("source_id") or 0)
        if trade_date == "" or type_id <= 0 or source_id <= 0:
            continue

        snapshot_count += 1
        trade_dates.add(trade_date)
        bucket_key = f"{trade_date}:{type_id}"
        if bucket_key not in daily_buckets:
            daily_buckets[bucket_key] = _bucket_seed(snapshot_row)
            continue

        daily_buckets[bucket_key] = _bucket_observe(daily_buckets[bucket_key], snapshot_row)

    rows = [
        daily_buckets[key]
        for key in sorted(
            daily_buckets.keys(),
            key=lambda value: (value.split(":", 1)[0], int(value.split(":", 1)[1])),
        )
    ]
    return {
        "rows": rows,
        "snapshot_metric_rows": snapshot_count,
        "trade_dates": sorted(trade_dates),
    }


def _emit_progress(context: Any, stats: WorkerStats, stage: str, rows_seen: int, rows_written: int) -> None:
    context.emit(
        "python_worker.batch_progress",
        {
            "schedule_id": context.schedule_id,
            "job_key": context.job_key,
            "stage": stage,
            "batches_completed": stats.progress.batches_completed,
            "rows_processed": rows_seen,
            "rows_written": rows_written,
            "memory_usage_bytes": resident_memory_bytes(),
            "duration_ms": stats.duration_ms(),
        },
    )


def _guard_runtime(context: Any, stats: WorkerStats, stage: str) -> None:
    if stats.duration_ms() > max(30, int(context.timeout_seconds)) * 1000:
        raise TimeoutError(f"Hub snapshot history sync exceeded timeout during {stage}.")

    memory_bytes = resident_memory_bytes()
    if memory_bytes > context.memory_abort_threshold_bytes:
        raise MemoryError(
            f"Hub snapshot history sync exceeded memory threshold during {stage}: {memory_bytes} bytes > {context.memory_abort_threshold_bytes} bytes"
        )


def _fetch_snapshot_summary(db: SupplyCoreDb, source_type: str, source_id: int, start_observed_at: str) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            source_type,
            source_id,
            type_id,
            observed_at,
            best_sell_price,
            best_buy_price,
            total_buy_volume,
            total_sell_volume,
            total_volume,
            buy_order_count,
            sell_order_count
        FROM market_order_snapshots_summary
        WHERE source_type = %s
          AND source_id = %s
          AND observed_at >= %s
        ORDER BY observed_at ASC, type_id ASC
        """,
        (source_type, source_id, start_observed_at),
    )


def _fetch_raw_snapshot_metrics(db: SupplyCoreDb, history_table: str, source_type: str, source_id: int, start_observed_at: str) -> list[dict[str, Any]]:
    sql = f"""
        SELECT
            %s AS source_type,
            snapshots.source_id,
            snapshots.type_id,
            snapshots.observed_at,
            MIN(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.min_price ELSE NULL END) AS best_sell_price,
            MAX(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.max_price ELSE NULL END) AS best_buy_price,
            SUM(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.volume_remain ELSE 0 END) AS total_buy_volume,
            SUM(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.volume_remain ELSE 0 END) AS total_sell_volume,
            SUM(snapshots.volume_remain) AS total_volume,
            SUM(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.order_count ELSE 0 END) AS buy_order_count,
            SUM(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.order_count ELSE 0 END) AS sell_order_count
        FROM (
            SELECT
                moh.source_id,
                moh.type_id,
                moh.is_buy_order,
                MIN(moh.price) AS min_price,
                MAX(moh.price) AS max_price,
                SUM(moh.volume_remain) AS volume_remain,
                COUNT(*) AS order_count,
                moh.observed_at
            FROM {history_table} moh
            WHERE moh.source_type = %s
              AND moh.source_id = %s
              AND moh.observed_at >= %s
            GROUP BY moh.source_id, moh.type_id, moh.is_buy_order, moh.observed_at

            UNION ALL

            SELECT
                moc.source_id,
                moc.type_id,
                moc.is_buy_order,
                MIN(moc.price) AS min_price,
                MAX(moc.price) AS max_price,
                SUM(moc.volume_remain) AS volume_remain,
                COUNT(*) AS order_count,
                moc.observed_at
            FROM market_orders_current moc
            LEFT JOIN (
                SELECT DISTINCT observed_at
                FROM {history_table}
                WHERE source_type = %s
                  AND source_id = %s
                  AND observed_at >= %s
            ) history_observed
              ON history_observed.observed_at = moc.observed_at
            WHERE moc.source_type = %s
              AND moc.source_id = %s
              AND moc.observed_at >= %s
              AND history_observed.observed_at IS NULL
            GROUP BY moc.source_id, moc.type_id, moc.is_buy_order, moc.observed_at
        ) snapshots
        GROUP BY snapshots.source_id, snapshots.type_id, snapshots.observed_at
        ORDER BY snapshots.observed_at ASC, snapshots.type_id ASC
    """
    return db.fetch_all(
        sql,
        (
            source_type,
            source_type,
            source_id,
            start_observed_at,
            source_type,
            source_id,
            start_observed_at,
            source_type,
            source_id,
            start_observed_at,
        ),
    )


def _upsert_snapshot_summary(db: SupplyCoreDb, rows: list[dict[str, Any]], batch_size: int) -> int:
    if not rows:
        return 0

    sql = """
        INSERT INTO market_order_snapshots_summary (
            source_type,
            source_id,
            type_id,
            observed_at,
            best_sell_price,
            best_buy_price,
            total_buy_volume,
            total_sell_volume,
            total_volume,
            buy_order_count,
            sell_order_count
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            best_sell_price = VALUES(best_sell_price),
            best_buy_price = VALUES(best_buy_price),
            total_buy_volume = VALUES(total_buy_volume),
            total_sell_volume = VALUES(total_sell_volume),
            total_volume = VALUES(total_volume),
            buy_order_count = VALUES(buy_order_count),
            sell_order_count = VALUES(sell_order_count),
            updated_at = CURRENT_TIMESTAMP
    """
    written = 0
    with db.cursor() as (connection, cursor):
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            params = [
                (
                    str(row.get("source_type") or ""),
                    int(row.get("source_id") or 0),
                    int(row.get("type_id") or 0),
                    str(row.get("observed_at") or ""),
                    row.get("best_sell_price"),
                    row.get("best_buy_price"),
                    max(0, int(row.get("total_buy_volume") or 0)),
                    max(0, int(row.get("total_sell_volume") or 0)),
                    max(0, int(row.get("total_volume") or 0)),
                    max(0, int(row.get("buy_order_count") or 0)),
                    max(0, int(row.get("sell_order_count") or 0)),
                )
                for row in batch
            ]
            cursor.executemany(sql, params)
            written += max(0, int(cursor.rowcount or 0))
        connection.commit()
    return written


def _upsert_market_source_state(
    db: SupplyCoreDb,
    *,
    source_type: str,
    source_id: int,
    latest_summary_observed_at: str,
    summary_row_count: int,
) -> None:
    db.execute(
        """
        INSERT INTO market_source_snapshot_state (
            source_type,
            source_id,
            latest_summary_observed_at,
            summary_row_count,
            last_synced_at
        ) VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            latest_summary_observed_at = VALUES(latest_summary_observed_at),
            summary_row_count = VALUES(summary_row_count),
            last_synced_at = VALUES(last_synced_at),
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            source_type,
            source_id,
            latest_summary_observed_at,
            summary_row_count,
            latest_summary_observed_at,
        ),
    )


def _upsert_local_history_rows(db: SupplyCoreDb, rows: list[dict[str, Any]], batch_size: int) -> int:
    if not rows:
        return 0

    sql = """
        INSERT INTO market_hub_local_history_daily (
            source,
            source_id,
            type_id,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            buy_price,
            sell_price,
            spread_value,
            spread_percent,
            volume,
            buy_order_count,
            sell_order_count,
            captured_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON DUPLICATE KEY UPDATE
            open_price = VALUES(open_price),
            high_price = VALUES(high_price),
            low_price = VALUES(low_price),
            close_price = VALUES(close_price),
            buy_price = VALUES(buy_price),
            sell_price = VALUES(sell_price),
            spread_value = VALUES(spread_value),
            spread_percent = VALUES(spread_percent),
            volume = VALUES(volume),
            buy_order_count = VALUES(buy_order_count),
            sell_order_count = VALUES(sell_order_count),
            captured_at = VALUES(captured_at),
            updated_at = CURRENT_TIMESTAMP
    """
    written = 0
    with db.cursor() as (connection, cursor):
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            params = [
                (
                    str(row.get("source") or ""),
                    int(row.get("source_id") or 0),
                    int(row.get("type_id") or 0),
                    str(row.get("trade_date") or ""),
                    round(float(row.get("open_price") or 0.0), 2),
                    round(float(row.get("high_price") or 0.0), 2),
                    round(float(row.get("low_price") or 0.0), 2),
                    round(float(row.get("close_price") or 0.0), 2),
                    None if row.get("buy_price") is None else round(float(row["buy_price"]), 2),
                    None if row.get("sell_price") is None else round(float(row["sell_price"]), 2),
                    None if row.get("spread_value") is None else round(float(row["spread_value"]), 2),
                    _normalize_spread_percent(row.get("spread_percent")),
                    max(0, int(row.get("volume") or 0)),
                    max(0, int(row.get("buy_order_count") or 0)),
                    max(0, int(row.get("sell_order_count") or 0)),
                    str(row.get("captured_at") or ""),
                )
                for row in batch
                if str(row.get("source") or "").strip() != ""
                and int(row.get("source_id") or 0) > 0
                and int(row.get("type_id") or 0) > 0
                and str(row.get("trade_date") or "").strip() != ""
                and str(row.get("captured_at") or "").strip() != ""
            ]
            if not params:
                continue
            cursor.executemany(sql, params)
            written += max(0, int(cursor.rowcount or 0))
        connection.commit()
    return written


def _merge_snapshot_rows(summary_rows: list[dict[str, Any]], raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in [*summary_rows, *raw_rows]:
        row_key = ":".join(
            [
                str(row.get("source_type") or ""),
                str(row.get("source_id") or ""),
                str(row.get("type_id") or ""),
                str(row.get("observed_at") or ""),
            ]
        )
        if row_key == ":::":
            continue
        merged[row_key] = {
            "source_type": str(row.get("source_type") or ""),
            "source_id": max(0, int(row.get("source_id") or 0)),
            "type_id": max(0, int(row.get("type_id") or 0)),
            "observed_at": str(row.get("observed_at") or ""),
            "best_sell_price": None if row.get("best_sell_price") is None else float(row["best_sell_price"]),
            "best_buy_price": None if row.get("best_buy_price") is None else float(row["best_buy_price"]),
            "total_buy_volume": max(0, int(row.get("total_buy_volume") or 0)),
            "total_sell_volume": max(0, int(row.get("total_sell_volume") or 0)),
            "total_volume": max(0, int(row.get("total_volume") or 0)),
            "buy_order_count": max(0, int(row.get("buy_order_count") or 0)),
            "sell_order_count": max(0, int(row.get("sell_order_count") or 0)),
        }

    return sorted(merged.values(), key=lambda row: (str(row.get("observed_at") or ""), int(row.get("type_id") or 0)))


def run_market_hub_local_history(context: Any) -> dict[str, Any]:
    bridge = PhpBridge(context.php_binary, context.app_root)
    job_context = dict(bridge.call("market-hub-local-history-context")["context"])
    dataset_key = str(job_context.get("dataset_key") or "").strip()
    run_mode = str(job_context.get("run_mode") or "incremental").strip() or "incremental"
    sync_run = bridge.call(
        "sync-run-start",
        payload={"dataset_key": dataset_key, "run_mode": run_mode},
    )["result"]
    run_id = int(sync_run.get("run_id") or 0)

    stats = WorkerStats()
    db = SupplyCoreDb(context.db_config)
    source_id = int(job_context.get("source_id") or 0)
    source_name = str(job_context.get("source_name") or "Reference market hub")
    app_timezone = str(job_context.get("app_timezone") or "UTC")
    window_days = max(1, min(3650, int(job_context.get("window_days") or 1)))
    history_read_table = str(job_context.get("history_read_table") or "market_orders_history").strip()
    source_label = str(job_context.get("local_history_source") or "market_hub_current_sync")

    try:
        if source_id <= 0:
            result = {
                "status": "success",
                "summary": "Hub snapshot history sync skipped because the configured market hub source could not be resolved.",
                "rows_seen": 0,
                "rows_written": 0,
                "cursor": "source_type:market_hub;state:missing_source_id",
                "checksum": _payload_checksum({"source_type": "market_hub", "source_id": source_id, "status": "missing_source_id"}),
                "warnings": [f"Hub snapshot history sync skipped: could not resolve a valid local source id for {source_name}."],
                "duration_ms": stats.duration_ms(),
                "started_at": stats.started_at_iso,
                "finished_at": utc_now_iso(),
                "meta": {
                    "execution_mode": "python",
                    "source_type": "market_hub",
                    "source_id": source_id,
                    "window_days": window_days,
                    "window_start_observed_at": None,
                    "records_fetched": 0,
                    "records_inserted": 0,
                    "records_updated": 0,
                    "records_skipped": 0,
                    "records_deleted": 0,
                    "history_rows_generated": 0,
                    "snapshot_days_seen": 0,
                    "no_changes": True,
                    "outcome_reason": "missing_source_id",
                }
                | job_context,
                "run_id": run_id,
            }
            bridge.call(
                "sync-run-finish",
                payload={
                    "dataset_key": dataset_key,
                    "run_id": run_id,
                    "run_mode": run_mode,
                    "status": "success",
                    "rows_seen": result["rows_seen"],
                    "rows_written": result["rows_written"],
                    "cursor": result["cursor"],
                    "checksum": result["checksum"],
                },
            )
            return result

        window_start_observed_at = _window_start_observed_at(window_days, app_timezone)
        summary_rows = _fetch_snapshot_summary(db, "market_hub", source_id, window_start_observed_at)
        latest_observed_at = max((str(row.get("observed_at") or "") for row in summary_rows), default="")
        raw_start_observed_at = latest_observed_at if latest_observed_at != "" else window_start_observed_at

        _guard_runtime(context, stats, "snapshot summary load")
        raw_rows = _fetch_raw_snapshot_metrics(db, history_read_table, "market_hub", source_id, raw_start_observed_at)
        summary_rows_written = _upsert_snapshot_summary(db, raw_rows, context.batch_size) if raw_rows else 0
        if raw_rows:
            latest_raw_observed_at = str(raw_rows[-1].get("observed_at") or "").strip()
            if latest_raw_observed_at != "":
                _upsert_market_source_state(
                    db,
                    source_type="market_hub",
                    source_id=source_id,
                    latest_summary_observed_at=latest_raw_observed_at,
                    summary_row_count=len(raw_rows),
                )
        elif latest_observed_at != "":
            _upsert_market_source_state(
                db,
                source_type="market_hub",
                source_id=source_id,
                latest_summary_observed_at=latest_observed_at,
                summary_row_count=len(summary_rows),
            )

        snapshot_metrics = _merge_snapshot_rows(summary_rows, raw_rows) if summary_rows else raw_rows
        rows_seen = len(snapshot_metrics)
        stats.progress.rows_processed = rows_seen
        stats.progress.batches_completed += 1
        _emit_progress(context, stats, "snapshot_metrics", rows_seen, 0)
        _guard_runtime(context, stats, "snapshot metrics build")

        if not snapshot_metrics:
            result = {
                "status": "success",
                "summary": "Hub snapshot history sync found no local raw order snapshots in the configured window.",
                "rows_seen": 0,
                "rows_written": 0,
                "cursor": f"source_type:market_hub;source_id:{source_id};state:awaiting_local_snapshots",
                "checksum": _payload_checksum(
                    {
                        "source_type": "market_hub",
                        "source_id": source_id,
                        "window_days": window_days,
                        "status": "awaiting_local_snapshots",
                    }
                ),
                "warnings": [
                    f"Hub snapshot history sync found no local raw order snapshots in the last {window_days} day(s) for {source_name}. Run the matching current sync first or widen --window-days."
                ],
                "duration_ms": stats.duration_ms(),
                "started_at": stats.started_at_iso,
                "finished_at": utc_now_iso(),
                "meta": {
                    "execution_mode": "python",
                    "source_type": "market_hub",
                    "source_id": source_id,
                    "window_days": window_days,
                    "window_start_observed_at": window_start_observed_at,
                    "records_fetched": 0,
                    "records_inserted": 0,
                    "records_updated": 0,
                    "records_skipped": 0,
                    "records_deleted": 0,
                    "history_rows_generated": 0,
                    "snapshot_days_seen": 0,
                    "snapshot_summary_rows_written": summary_rows_written,
                    "no_changes": True,
                    "outcome_reason": "awaiting_local_snapshots",
                }
                | job_context,
                "run_id": run_id,
            }
            bridge.call(
                "sync-run-finish",
                payload={
                    "dataset_key": dataset_key,
                    "run_id": run_id,
                    "run_mode": run_mode,
                    "status": "success",
                    "rows_seen": result["rows_seen"],
                    "rows_written": result["rows_written"],
                    "cursor": result["cursor"],
                    "checksum": result["checksum"],
                },
            )
            return result

        rebuilt = _rebuild_daily_rows(snapshot_metrics, app_timezone, source_label)
        canonical_rows = list(rebuilt.get("rows") or [])
        trade_dates = list(rebuilt.get("trade_dates") or [])
        history_row_count = len(canonical_rows)
        _guard_runtime(context, stats, "daily rebuild")

        if not canonical_rows:
            result = {
                "status": "success",
                "summary": "Hub snapshot history sync could not derive daily local-history rows from the available snapshots.",
                "rows_seen": rows_seen,
                "rows_written": 0,
                "cursor": f"source_type:market_hub;source_id:{source_id};window_days:{window_days};state:no_canonical_rows",
                "checksum": _payload_checksum(
                    {
                        "source_type": "market_hub",
                        "source_id": source_id,
                        "window_days": window_days,
                        "status": "no_canonical_rows",
                    }
                ),
                "warnings": [f"Hub snapshot history sync could not derive any daily rows from the local raw order snapshots for {source_name}."],
                "duration_ms": stats.duration_ms(),
                "started_at": stats.started_at_iso,
                "finished_at": utc_now_iso(),
                "meta": {
                    "execution_mode": "python",
                    "source_type": "market_hub",
                    "source_id": source_id,
                    "window_days": window_days,
                    "window_start_observed_at": window_start_observed_at,
                    "records_fetched": rows_seen,
                    "records_inserted": 0,
                    "records_updated": 0,
                    "records_skipped": rows_seen,
                    "records_deleted": 0,
                    "history_rows_generated": 0,
                    "snapshot_days_seen": len(trade_dates),
                    "snapshot_summary_rows_written": summary_rows_written,
                    "no_changes": True,
                    "outcome_reason": "no_canonical_rows",
                }
                | job_context,
                "run_id": run_id,
            }
            bridge.call(
                "sync-run-finish",
                payload={
                    "dataset_key": dataset_key,
                    "run_id": run_id,
                    "run_mode": run_mode,
                    "status": "success",
                    "rows_seen": result["rows_seen"],
                    "rows_written": result["rows_written"],
                    "cursor": result["cursor"],
                    "checksum": result["checksum"],
                },
            )
            return result

        rows_written = _upsert_local_history_rows(db, canonical_rows, context.batch_size)
        stats.progress.rows_written = rows_written
        stats.progress.batches_completed += 1
        latest_captured_at = str(canonical_rows[-1].get("captured_at") or "").strip()
        cursor = f"source_type:market_hub;source_id:{source_id};window_days:{window_days};observed_at:{latest_captured_at}"
        checksum = _payload_checksum(canonical_rows)
        _emit_progress(context, stats, "local_history_upsert", rows_seen, rows_written)
        _guard_runtime(context, stats, "local history upsert")

        result = {
            "status": "success",
            "summary": "Hub snapshot history sync rebuilt local-history daily candles in native Python.",
            "rows_seen": rows_seen,
            "rows_written": rows_written,
            "cursor": cursor,
            "checksum": checksum,
            "warnings": [],
            "duration_ms": stats.duration_ms(),
            "started_at": stats.started_at_iso,
            "finished_at": utc_now_iso(),
            "meta": {
                "execution_mode": "python",
                "source_type": "market_hub",
                "source_id": source_id,
                "window_days": window_days,
                "window_start_observed_at": window_start_observed_at,
                "observed_at": latest_captured_at or None,
                "records_fetched": rows_seen,
                "records_inserted": 0,
                "records_updated": rows_written,
                "records_skipped": max(0, history_row_count - rows_written),
                "records_deleted": 0,
                "history_rows_generated": history_row_count,
                "snapshot_summary_rows_written": summary_rows_written,
                "snapshot_days_seen": len(trade_dates),
                "no_changes": rows_written == 0,
                "memory_usage_bytes": resident_memory_bytes(),
            }
            | job_context,
            "run_id": run_id,
        }
        bridge.call(
            "sync-run-finish",
            payload={
                "dataset_key": dataset_key,
                "run_id": run_id,
                "run_mode": run_mode,
                "status": "success",
                "rows_seen": result["rows_seen"],
                "rows_written": result["rows_written"],
                "cursor": result["cursor"],
                "checksum": result["checksum"],
            },
        )
        gc.collect()
        return result
    except Exception as error:
        bridge.call(
            "sync-run-finish",
            payload={
                "dataset_key": dataset_key,
                "run_id": run_id,
                "run_mode": run_mode,
                "status": "failed",
                "error_message": str(error),
            },
        )
        raise
