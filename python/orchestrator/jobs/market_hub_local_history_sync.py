from __future__ import annotations

import hashlib
import json
import math
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _normalize_spread_percent(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    spread_percent = round(float(value), 4)
    return None if abs(spread_percent) > 9999.9999 else spread_percent


def _trade_date(observed_at: str, tz: ZoneInfo) -> str:
    raw = observed_at.strip()
    if raw == "":
        return ""
    try:
        captured = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
    except Exception:
        return ""
    return captured.astimezone(tz).strftime("%Y-%m-%d")


def _window_start(window_days: int, tz: ZoneInfo) -> str:
    now_local = datetime.now(tz)
    start = datetime(now_local.year, now_local.month, now_local.day, tzinfo=tz) - timedelta(days=max(0, window_days - 1))
    return start.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def _finalize_metric(metric: dict[str, Any], tz: ZoneInfo, source_label: str) -> dict[str, Any] | None:
    type_id = int(metric.get("type_id") or 0)
    source_id = int(metric.get("source_id") or 0)
    observed_at = str(metric.get("observed_at") or "").strip()
    if type_id <= 0 or source_id <= 0 or observed_at == "":
        return None
    sell = round(float(metric["best_sell_price"]), 2) if metric.get("best_sell_price") is not None else None
    buy = round(float(metric["best_buy_price"]), 2) if metric.get("best_buy_price") is not None else None
    close = sell if sell is not None else buy
    if close is None:
        return None
    spread_value = round(sell - buy, 2) if sell is not None and buy is not None else None
    spread_pct = _normalize_spread_percent((spread_value / buy) * 100.0) if spread_value is not None and buy and buy > 0 else None
    return {
        "source": source_label, "source_id": source_id, "type_id": type_id,
        "trade_date": _trade_date(observed_at, tz),
        "close_price": close, "buy_price": buy, "sell_price": sell,
        "spread_value": spread_value, "spread_percent": spread_pct,
        "volume": max(0, int(metric.get("total_volume") or 0)),
        "buy_order_count": max(0, int(metric.get("buy_order_count") or 0)),
        "sell_order_count": max(0, int(metric.get("sell_order_count") or 0)),
        "captured_at": observed_at,
    }


def _bucket_seed(row: dict[str, Any]) -> dict[str, Any]:
    close = float(row.get("close_price") or 0.0)
    return {
        "source": str(row.get("source") or ""), "source_id": max(0, int(row.get("source_id") or 0)),
        "type_id": max(0, int(row.get("type_id") or 0)), "trade_date": str(row.get("trade_date") or ""),
        "open_price": close, "high_price": close, "low_price": close, "close_price": close,
        "buy_price": row.get("buy_price"), "sell_price": row.get("sell_price"),
        "spread_value": row.get("spread_value"), "spread_percent": _normalize_spread_percent(row.get("spread_percent")),
        "volume": max(0, int(row.get("volume") or 0)),
        "buy_order_count": max(0, int(row.get("buy_order_count") or 0)),
        "sell_order_count": max(0, int(row.get("sell_order_count") or 0)),
        "captured_at": str(row.get("captured_at") or ""),
    }


def _bucket_observe(bucket: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    close = float(row.get("close_price") or 0.0)
    captured = str(row.get("captured_at") or "").strip()
    current = str(bucket.get("captured_at") or "").strip()
    bucket["high_price"] = max(float(bucket.get("high_price") or close), close)
    bucket["low_price"] = min(float(bucket.get("low_price") or close), close)
    if float(bucket.get("open_price") or 0.0) <= 0.0:
        bucket["open_price"] = close
    if current == "" or (captured != "" and captured >= current):
        bucket["close_price"] = close
        bucket["buy_price"] = row.get("buy_price")
        bucket["sell_price"] = row.get("sell_price")
        bucket["spread_value"] = row.get("spread_value")
        bucket["spread_percent"] = _normalize_spread_percent(row.get("spread_percent"))
        bucket["volume"] = max(0, int(row.get("volume") or 0))
        bucket["buy_order_count"] = max(0, int(row.get("buy_order_count") or 0))
        bucket["sell_order_count"] = max(0, int(row.get("sell_order_count") or 0))
        bucket["captured_at"] = captured
    return bucket


def _rebuild_daily_rows(metrics: list[dict[str, Any]], tz: ZoneInfo, source_label: str) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for metric in metrics:
        row = _finalize_metric(metric, tz, source_label)
        if row is None:
            continue
        td = str(row.get("trade_date") or "").strip()
        tid = int(row.get("type_id") or 0)
        if td == "" or tid <= 0:
            continue
        key = f"{td}:{tid}"
        if key not in buckets:
            buckets[key] = _bucket_seed(row)
        else:
            buckets[key] = _bucket_observe(buckets[key], row)
    return [buckets[k] for k in sorted(buckets.keys(), key=lambda v: (v.split(":", 1)[0], int(v.split(":", 1)[1])))]


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    hub_ref = db.fetch_app_setting("market_station_id", "").strip()
    source_id = int(hub_ref) if hub_ref.isdigit() and int(hub_ref) > 0 else 0
    app_tz_name = db.fetch_app_setting("app_timezone", "UTC").strip() or "UTC"
    window_days = max(1, min(3650, int(db.fetch_app_setting("market_hub_local_history_window_days", "30") or 30)))
    source_label = "market_hub_current_sync"

    try:
        tz = ZoneInfo(app_tz_name)
    except Exception:
        tz = ZoneInfo("UTC")

    if source_id <= 0:
        return {
            "status": "skipped", "rows_processed": 0, "rows_written": 0,
            "warnings": ["Market hub local history requires market_station_id in settings."],
            "summary": "Market hub local history skipped: no configured source.",
        }

    start_observed = _window_start(window_days, tz)

    # Fetch existing snapshot summaries
    summary_rows = db.fetch_all(
        """SELECT source_type, source_id, type_id, observed_at, best_sell_price, best_buy_price,
                  total_buy_volume, total_sell_volume, total_volume, buy_order_count, sell_order_count
           FROM market_order_snapshots_summary
           WHERE source_type = 'market_hub' AND source_id = %s AND observed_at >= %s
           ORDER BY observed_at ASC, type_id ASC""",
        (source_id, start_observed),
    )
    latest_observed = max((str(r.get("observed_at") or "") for r in summary_rows), default="")
    raw_start = latest_observed if latest_observed else start_observed

    # Fetch raw metrics from history + current tables to fill gaps
    raw_rows = db.fetch_all(
        """SELECT 'market_hub' AS source_type, snapshots.source_id, snapshots.type_id, snapshots.observed_at,
                  MIN(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.min_price ELSE NULL END) AS best_sell_price,
                  MAX(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.max_price ELSE NULL END) AS best_buy_price,
                  SUM(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.volume_remain ELSE 0 END) AS total_buy_volume,
                  SUM(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.volume_remain ELSE 0 END) AS total_sell_volume,
                  SUM(snapshots.volume_remain) AS total_volume,
                  SUM(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.order_count ELSE 0 END) AS buy_order_count,
                  SUM(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.order_count ELSE 0 END) AS sell_order_count
           FROM (
               SELECT moh.source_id, moh.type_id, moh.is_buy_order,
                      MIN(moh.price) AS min_price, MAX(moh.price) AS max_price,
                      SUM(moh.volume_remain) AS volume_remain, COUNT(*) AS order_count, moh.observed_at
               FROM market_orders_history moh
               WHERE moh.source_type = 'market_hub' AND moh.source_id = %s AND moh.observed_at >= %s
               GROUP BY moh.source_id, moh.type_id, moh.is_buy_order, moh.observed_at
               UNION ALL
               SELECT moc.source_id, moc.type_id, moc.is_buy_order,
                      MIN(moc.price) AS min_price, MAX(moc.price) AS max_price,
                      SUM(moc.volume_remain) AS volume_remain, COUNT(*) AS order_count, moc.observed_at
               FROM market_orders_current moc
               LEFT JOIN (SELECT DISTINCT observed_at FROM market_orders_history
                          WHERE source_type = 'market_hub' AND source_id = %s AND observed_at >= %s) ho
                   ON ho.observed_at = moc.observed_at
               WHERE moc.source_type = 'market_hub' AND moc.source_id = %s AND moc.observed_at >= %s
                 AND ho.observed_at IS NULL
               GROUP BY moc.source_id, moc.type_id, moc.is_buy_order, moc.observed_at
           ) snapshots
           GROUP BY snapshots.source_id, snapshots.type_id, snapshots.observed_at
           ORDER BY snapshots.observed_at ASC, snapshots.type_id ASC""",
        (source_id, raw_start, source_id, raw_start, source_id, raw_start),
    )

    # Upsert raw rows into snapshot summary table
    summary_written = 0
    if raw_rows:
        sql = """INSERT INTO market_order_snapshots_summary
                    (source_type, source_id, type_id, observed_at, best_sell_price, best_buy_price,
                     total_buy_volume, total_sell_volume, total_volume, buy_order_count, sell_order_count)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                 ON DUPLICATE KEY UPDATE
                    best_sell_price = VALUES(best_sell_price), best_buy_price = VALUES(best_buy_price),
                    total_buy_volume = VALUES(total_buy_volume), total_sell_volume = VALUES(total_sell_volume),
                    total_volume = VALUES(total_volume), buy_order_count = VALUES(buy_order_count),
                    sell_order_count = VALUES(sell_order_count), updated_at = CURRENT_TIMESTAMP"""
        batch_size = 500
        with db.cursor() as (conn, cur):
            for i in range(0, len(raw_rows), batch_size):
                batch = raw_rows[i:i + batch_size]
                params = [
                    (str(r.get("source_type") or ""), int(r.get("source_id") or 0), int(r.get("type_id") or 0),
                     str(r.get("observed_at") or ""), r.get("best_sell_price"), r.get("best_buy_price"),
                     max(0, int(r.get("total_buy_volume") or 0)), max(0, int(r.get("total_sell_volume") or 0)),
                     max(0, int(r.get("total_volume") or 0)), max(0, int(r.get("buy_order_count") or 0)),
                     max(0, int(r.get("sell_order_count") or 0)))
                    for r in batch
                ]
                cur.executemany(sql, params)
                summary_written += max(0, int(cur.rowcount or 0))
            conn.commit()

    # Update source state checkpoint
    latest_raw = str(raw_rows[-1].get("observed_at") or "").strip() if raw_rows else latest_observed
    if latest_raw:
        db.execute(
            """INSERT INTO market_source_snapshot_state
                  (source_type, source_id, latest_summary_observed_at, summary_row_count, last_synced_at)
               VALUES ('market_hub', %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                  latest_summary_observed_at = VALUES(latest_summary_observed_at),
                  summary_row_count = VALUES(summary_row_count),
                  last_synced_at = VALUES(last_synced_at), updated_at = CURRENT_TIMESTAMP""",
            (source_id, latest_raw, len(raw_rows) or len(summary_rows), latest_raw),
        )

    # Merge summary + raw rows, deduplicate by key
    merged: dict[str, dict[str, Any]] = {}
    for row in [*summary_rows, *raw_rows]:
        key = f"{row.get('source_type')}:{row.get('source_id')}:{row.get('type_id')}:{row.get('observed_at')}"
        merged[key] = row
    all_metrics = sorted(merged.values(), key=lambda r: (str(r.get("observed_at") or ""), int(r.get("type_id") or 0)))

    if not all_metrics:
        return {
            "status": "skipped", "rows_processed": 0, "rows_written": 0,
            "warnings": [f"No snapshot data found in last {window_days} day(s) for source {source_id}."],
            "summary": "Market hub local history skipped: no snapshots in window.",
        }

    # Rebuild daily OHLCV candles
    daily_rows = _rebuild_daily_rows(all_metrics, tz, source_label)
    if not daily_rows:
        return {
            "rows_processed": len(all_metrics), "rows_written": 0,
            "summary": f"Market hub local history: {len(all_metrics)} metrics processed but no valid daily rows derived.",
        }

    # Upsert daily candles
    rows_written = 0
    upsert_sql = """INSERT INTO market_hub_local_history_daily
                       (source, source_id, type_id, trade_date, open_price, high_price, low_price, close_price,
                        buy_price, sell_price, spread_value, spread_percent, volume, buy_order_count, sell_order_count, captured_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                       open_price = VALUES(open_price), high_price = VALUES(high_price),
                       low_price = VALUES(low_price), close_price = VALUES(close_price),
                       buy_price = VALUES(buy_price), sell_price = VALUES(sell_price),
                       spread_value = VALUES(spread_value), spread_percent = VALUES(spread_percent),
                       volume = VALUES(volume), buy_order_count = VALUES(buy_order_count),
                       sell_order_count = VALUES(sell_order_count), captured_at = VALUES(captured_at),
                       updated_at = CURRENT_TIMESTAMP"""
    batch_size = 500
    with db.cursor() as (conn, cur):
        for i in range(0, len(daily_rows), batch_size):
            batch = daily_rows[i:i + batch_size]
            params = [
                (str(r.get("source") or ""), int(r.get("source_id") or 0), int(r.get("type_id") or 0),
                 str(r.get("trade_date") or ""),
                 round(float(r.get("open_price") or 0.0), 2), round(float(r.get("high_price") or 0.0), 2),
                 round(float(r.get("low_price") or 0.0), 2), round(float(r.get("close_price") or 0.0), 2),
                 None if r.get("buy_price") is None else round(float(r["buy_price"]), 2),
                 None if r.get("sell_price") is None else round(float(r["sell_price"]), 2),
                 None if r.get("spread_value") is None else round(float(r["spread_value"]), 2),
                 _normalize_spread_percent(r.get("spread_percent")),
                 max(0, int(r.get("volume") or 0)), max(0, int(r.get("buy_order_count") or 0)),
                 max(0, int(r.get("sell_order_count") or 0)), str(r.get("captured_at") or ""))
                for r in batch
                if str(r.get("source") or "").strip() and int(r.get("source_id") or 0) > 0
                and int(r.get("type_id") or 0) > 0 and str(r.get("trade_date") or "").strip()
            ]
            if params:
                cur.executemany(upsert_sql, params)
                rows_written += max(0, int(cur.rowcount or 0))
        conn.commit()

    # Track sync state
    dataset_key = f"market.hub.{hub_ref}.history.local.daily" if hub_ref else "market_hub.history.local.daily"
    db.upsert_sync_state(
        dataset_key=dataset_key,
        status="success",
        row_count=rows_written,
        cursor=latest_raw or None,
    )

    return {
        "rows_processed": len(all_metrics),
        "rows_written": rows_written,
        "summary": f"Market hub local history: {rows_written} daily candles from {len(all_metrics)} snapshot metrics ({window_days}d window).",
        "meta": {
            "source_id": source_id,
            "window_days": window_days,
            "snapshot_summary_rows_written": summary_written,
            "daily_rows_generated": len(daily_rows),
        },
    }


def run_market_hub_local_history_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="market_hub_local_history_sync", phase="B", objective="market hub local history", processor=_processor)
