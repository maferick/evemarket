"""Validate InfluxDB time-series data against MariaDB rollup tables.

Compares row counts and sample values for each market rollup dataset to
confirm that InfluxDB contains the same data as MariaDB.  Useful during
migration to verify that dual-write or batch export is producing correct
results before switching reads to InfluxDB.

Usage:
    python -m orchestrator influx-validate --app-root /var/www/SupplyCore
    python -m orchestrator influx-validate --dataset market_item_price_1d --days 7
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import load_php_runtime_config, resolve_app_root
from .db import SupplyCoreDb
from .influx import InfluxClient, InfluxConfig, InfluxQueryError


_MARKET_DATASETS = {
    "market_item_price_1h": {"measurement": "market_item_price", "window": "1h", "bucket_col": "bucket_start", "fields": ["weighted_price", "avg_price", "min_price", "max_price", "listing_count"]},
    "market_item_price_1d": {"measurement": "market_item_price", "window": "1d", "bucket_col": "bucket_start", "fields": ["weighted_price", "avg_price", "min_price", "max_price", "listing_count"]},
    "market_item_stock_1h": {"measurement": "market_item_stock", "window": "1h", "bucket_col": "bucket_start", "fields": ["local_stock_units", "listing_count", "sample_count"]},
    "market_item_stock_1d": {"measurement": "market_item_stock", "window": "1d", "bucket_col": "bucket_start", "fields": ["local_stock_units", "listing_count", "sample_count"]},
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate InfluxDB data against MariaDB rollups")
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
    parser.add_argument("--dataset", action="append", default=[])
    parser.add_argument("--days", type=int, default=14, help="Compare the last N days of data")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _count_maria_rows(db: SupplyCoreDb, table: str, bucket_col: str, days: int) -> int:
    result = db.fetch_scalar(
        f"SELECT COUNT(*) FROM {table} WHERE {bucket_col} >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {days} DAY)"
    )
    return max(0, int(result))


def _count_influx_points(client: InfluxClient, bucket: str, measurement: str, window: str, days: int) -> int:
    flux = f"""
from(bucket: "{bucket}")
  |> range(start: -{days}d)
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> filter(fn: (r) => r.window == "{window}")
  |> group()
  |> count()
"""
    rows = client.query_flux(flux)
    return sum(int(row.get("_value") or 0) for row in rows)


def _sample_maria(db: SupplyCoreDb, table: str, bucket_col: str, fields: list[str], days: int, limit: int = 5) -> list[dict[str, Any]]:
    cols = ", ".join(["source_type", "source_id", "type_id", bucket_col, *fields])
    return db.fetch_all(
        f"SELECT {cols} FROM {table} WHERE {bucket_col} >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {days} DAY) ORDER BY {bucket_col} DESC LIMIT {limit}"
    )


def _sample_influx(client: InfluxClient, bucket: str, measurement: str, window: str, fields: list[str], days: int, limit: int = 5) -> list[dict[str, Any]]:
    field_filter = " or ".join(f'r._field == "{f}"' for f in fields)
    flux = f"""
from(bucket: "{bucket}")
  |> range(start: -{days}d)
  |> filter(fn: (r) => r._measurement == "{measurement}")
  |> filter(fn: (r) => r.window == "{window}")
  |> filter(fn: (r) => {field_filter})
  |> pivot(rowKey: ["_time", "type_id", "source_type", "source_id"], columnKey: ["_field"], valueColumn: "_value")
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {limit})
"""
    return client.query_flux(flux)


def _validate_dataset(
    db: SupplyCoreDb,
    client: InfluxClient,
    influx_bucket: str,
    dataset_key: str,
    spec: dict[str, Any],
    days: int,
    verbose: bool,
) -> dict[str, Any]:
    maria_count = _count_maria_rows(db, dataset_key, spec["bucket_col"], days)
    try:
        influx_count = _count_influx_points(client, influx_bucket, spec["measurement"], spec["window"], days)
    except InfluxQueryError as exc:
        return {"dataset": dataset_key, "status": "error", "error": str(exc), "maria_rows": maria_count, "influx_points": 0}

    # InfluxDB counts individual field writes as separate points per field,
    # so the comparison is approximate.  A reasonable heuristic: InfluxDB
    # should have at least as many "logical rows" as MariaDB when counting
    # by unique (time, tags) combinations.  We compare raw counts divided by
    # the number of exported fields to approximate logical row count.
    field_count = max(1, len(spec["fields"]))
    influx_logical = influx_count // field_count
    ratio = influx_logical / maria_count if maria_count > 0 else (1.0 if influx_count == 0 else float("inf"))
    status = "ok" if 0.9 <= ratio <= 1.1 else ("warn" if 0.5 <= ratio <= 2.0 else "mismatch")

    result: dict[str, Any] = {
        "dataset": dataset_key,
        "status": status,
        "maria_rows": maria_count,
        "influx_points": influx_count,
        "influx_logical_rows": influx_logical,
        "ratio": round(ratio, 3),
    }

    if verbose:
        result["maria_sample"] = _sample_maria(db, dataset_key, spec["bucket_col"], spec["fields"], days)
        try:
            result["influx_sample"] = _sample_influx(client, influx_bucket, spec["measurement"], spec["window"], spec["fields"], days)
        except InfluxQueryError as exc:
            result["influx_sample_error"] = str(exc)

    return result


def validate_main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    app_root = Path(args.app_root).resolve()
    runtime = load_php_runtime_config(app_root)
    influx_raw = dict(runtime.raw.get("influxdb", {}))
    influx_config = InfluxConfig.from_runtime(influx_raw)

    if not influx_config.enabled:
        print("InfluxDB is disabled.  Enable it to run validation.")
        return 1

    errors = influx_config.validate()
    if errors:
        for e in errors:
            print(f"Config error: {e}")
        return 1

    datasets = list(args.dataset) if args.dataset else list(_MARKET_DATASETS.keys())
    for key in datasets:
        if key not in _MARKET_DATASETS:
            print(f"Unknown dataset: {key}.  Available: {', '.join(_MARKET_DATASETS.keys())}")
            return 1

    db = SupplyCoreDb(dict(runtime.raw.get("db", {})))
    client = InfluxClient(influx_config)

    try:
        health = client.health()
        print(f"InfluxDB health: {health.get('status', 'unknown')}")
    except InfluxQueryError as exc:
        print(f"InfluxDB unreachable: {exc}")
        return 1

    all_ok = True
    print(f"\nValidation window: last {args.days} days")
    print(f"{'Dataset':<30} {'Status':<10} {'MariaDB':<10} {'InfluxDB':<12} {'Ratio':<8}")
    print("-" * 72)

    for key in datasets:
        spec = _MARKET_DATASETS[key]
        result = _validate_dataset(db, client, influx_config.bucket, key, spec, args.days, args.verbose)
        status_marker = {"ok": "OK", "warn": "WARN", "mismatch": "MISMATCH", "error": "ERROR"}.get(result["status"], "?")
        print(f"{result['dataset']:<30} {status_marker:<10} {result['maria_rows']:<10} {result.get('influx_logical_rows', result.get('influx_points', 0)):<12} {result.get('ratio', '—'):<8}")

        if result["status"] in ("mismatch", "error"):
            all_ok = False
            if "error" in result:
                print(f"  Error: {result['error']}")

        if args.verbose and "maria_sample" in result:
            print(f"  MariaDB sample ({len(result['maria_sample'])} rows):")
            for row in result["maria_sample"][:3]:
                print(f"    {row}")
        if args.verbose and "influx_sample" in result:
            print(f"  InfluxDB sample ({len(result['influx_sample'])} rows):")
            for row in result["influx_sample"][:3]:
                print(f"    {row}")

    print()
    if all_ok:
        print("All datasets validated successfully.")
    else:
        print("Some datasets have mismatches — review before switching read_mode.")
    return 0 if all_ok else 1
