from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .config import load_php_runtime_config, resolve_app_root
from .db import SupplyCoreDb
from .influx import InfluxConfig, InfluxWriter, encode_point
from .logging_utils import configure_logging, LoggerAdapter


@dataclass(frozen=True, slots=True)
class RollupDataset:
    key: str
    table: str
    measurement: str
    bucket_column: str
    order_by: tuple[str, ...]
    tags_fn: Callable[[dict[str, Any]], dict[str, Any]]
    fields_fn: Callable[[dict[str, Any]], dict[str, Any]]

    @property
    def dataset_key(self) -> str:
        return f"influx.rollup_export.{self.key}"


def _market_price_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": "1h" if row.get("_window") == "1h" else "1d",
        "source_type": row.get("source_type"),
        "source_id": row.get("source_id"),
        "type_id": row.get("type_id"),
    }


def _market_price_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "sample_count": row.get("sample_count"),
        "listing_count_sum": row.get("listing_count_sum"),
        "avg_price_sum": row.get("avg_price_sum"),
        "weighted_price_numerator": row.get("weighted_price_numerator"),
        "weighted_price_denominator": row.get("weighted_price_denominator"),
        "listing_count": row.get("listing_count"),
        "min_price": row.get("min_price"),
        "max_price": row.get("max_price"),
        "avg_price": row.get("avg_price"),
        "weighted_price": row.get("weighted_price"),
    }


def _market_stock_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": "1h" if row.get("_window") == "1h" else "1d",
        "source_type": row.get("source_type"),
        "source_id": row.get("source_id"),
        "type_id": row.get("type_id"),
    }


def _market_stock_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "sample_count": row.get("sample_count"),
        "stock_units_sum": row.get("stock_units_sum"),
        "listing_count_sum": row.get("listing_count_sum"),
        "local_stock_units": row.get("local_stock_units"),
        "listing_count": row.get("listing_count"),
    }


def _killmail_item_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": row.get("_window"),
        "type_id": row.get("type_id"),
        "doctrine_fit_id": row.get("doctrine_fit_id") or 0,
        "doctrine_group_id": row.get("doctrine_group_id") or 0,
        "hull_type_id": row.get("hull_type_id") or 0,
    }


def _killmail_item_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "loss_count": row.get("loss_count"),
        "quantity_lost": row.get("quantity_lost"),
        "victim_count": row.get("victim_count"),
        "killmail_count": row.get("killmail_count"),
    }


def _killmail_hull_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": row.get("_window"),
        "hull_type_id": row.get("hull_type_id"),
        "doctrine_fit_id": row.get("doctrine_fit_id") or 0,
        "doctrine_group_id": row.get("doctrine_group_id") or 0,
    }


def _killmail_hull_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "loss_count": row.get("loss_count"),
        "victim_count": row.get("victim_count"),
        "killmail_count": row.get("killmail_count"),
    }


def _killmail_doctrine_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": row.get("_window"),
        "doctrine_fit_id": row.get("doctrine_fit_id") or 0,
        "doctrine_group_id": row.get("doctrine_group_id") or 0,
        "hull_type_id": row.get("hull_type_id") or 0,
    }


def _killmail_doctrine_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "loss_count": row.get("loss_count"),
        "quantity_lost": row.get("quantity_lost"),
        "victim_count": row.get("victim_count"),
        "killmail_count": row.get("killmail_count"),
    }


def _doctrine_fit_activity_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": row.get("_window"),
        "fit_id": row.get("fit_id"),
        "doctrine_group_id": row.get("doctrine_group_id") or 0,
        "hull_type_id": row.get("hull_type_id") or 0,
    }


def _doctrine_fit_activity_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "hull_loss_count": row.get("hull_loss_count"),
        "doctrine_item_loss_count": row.get("doctrine_item_loss_count"),
        "complete_fits_available": row.get("complete_fits_available"),
        "target_fits": row.get("target_fits"),
        "fit_gap": row.get("fit_gap"),
        "priority_score": row.get("priority_score"),
        "readiness_state": row.get("readiness_state"),
        "resupply_pressure": row.get("resupply_pressure"),
    }


def _doctrine_group_activity_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": row.get("_window"),
        "group_id": row.get("group_id"),
    }


def _doctrine_group_activity_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "hull_loss_count": row.get("hull_loss_count"),
        "doctrine_item_loss_count": row.get("doctrine_item_loss_count"),
        "complete_fits_available": row.get("complete_fits_available"),
        "target_fits": row.get("target_fits"),
        "fit_gap": row.get("fit_gap"),
        "priority_score": row.get("priority_score"),
        "readiness_state": row.get("readiness_state"),
        "resupply_pressure": row.get("resupply_pressure"),
    }


def _doctrine_stock_pressure_tags(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "window": row.get("_window"),
        "fit_id": row.get("fit_id"),
        "doctrine_group_id": row.get("doctrine_group_id") or 0,
        "bottleneck_type_id": row.get("bottleneck_type_id") or 0,
    }


def _doctrine_stock_pressure_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "complete_fits_available": row.get("complete_fits_available"),
        "target_fits": row.get("target_fits"),
        "fit_gap": row.get("fit_gap"),
        "bottleneck_quantity": row.get("bottleneck_quantity"),
        "readiness_state": row.get("readiness_state"),
        "resupply_pressure": row.get("resupply_pressure"),
    }


DATASETS: tuple[RollupDataset, ...] = (
    RollupDataset("market_item_price_1h", "market_item_price_1h", "market_item_price", "bucket_start", ("updated_at", "bucket_start", "source_type", "source_id", "type_id"), _market_price_tags, _market_price_fields),
    RollupDataset("market_item_price_1d", "market_item_price_1d", "market_item_price", "bucket_start", ("updated_at", "bucket_start", "source_type", "source_id", "type_id"), _market_price_tags, _market_price_fields),
    RollupDataset("market_item_stock_1h", "market_item_stock_1h", "market_item_stock", "bucket_start", ("updated_at", "bucket_start", "source_type", "source_id", "type_id"), _market_stock_tags, _market_stock_fields),
    RollupDataset("market_item_stock_1d", "market_item_stock_1d", "market_item_stock", "bucket_start", ("updated_at", "bucket_start", "source_type", "source_id", "type_id"), _market_stock_tags, _market_stock_fields),
    RollupDataset("killmail_item_loss_1h", "killmail_item_loss_1h", "killmail_item_loss", "bucket_start", ("updated_at", "bucket_start", "type_id", "doctrine_fit_key", "doctrine_group_key", "hull_type_key"), _killmail_item_tags, _killmail_item_fields),
    RollupDataset("killmail_item_loss_1d", "killmail_item_loss_1d", "killmail_item_loss", "bucket_start", ("updated_at", "bucket_start", "type_id", "doctrine_fit_key", "doctrine_group_key", "hull_type_key"), _killmail_item_tags, _killmail_item_fields),
    RollupDataset("killmail_hull_loss_1d", "killmail_hull_loss_1d", "killmail_hull_loss", "bucket_start", ("updated_at", "bucket_start", "hull_type_id", "doctrine_fit_key", "doctrine_group_key"), _killmail_hull_tags, _killmail_hull_fields),
    RollupDataset("killmail_doctrine_activity_1d", "killmail_doctrine_activity_1d", "killmail_doctrine_activity", "bucket_start", ("updated_at", "bucket_start", "doctrine_fit_key", "doctrine_group_key", "hull_type_key"), _killmail_doctrine_tags, _killmail_doctrine_fields),
    RollupDataset("doctrine_fit_activity_1d", "doctrine_fit_activity_1d", "doctrine_fit_activity", "bucket_start", ("updated_at", "bucket_start", "fit_id"), _doctrine_fit_activity_tags, _doctrine_fit_activity_fields),
    RollupDataset("doctrine_group_activity_1d", "doctrine_group_activity_1d", "doctrine_group_activity", "bucket_start", ("updated_at", "bucket_start", "group_id"), _doctrine_group_activity_tags, _doctrine_group_activity_fields),
    RollupDataset("doctrine_fit_stock_pressure_1d", "doctrine_fit_stock_pressure_1d", "doctrine_fit_stock_pressure", "bucket_start", ("updated_at", "bucket_start", "fit_id"), _doctrine_stock_pressure_tags, _doctrine_stock_pressure_fields),
)
DATASET_MAP = {dataset.key: dataset for dataset in DATASETS}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export SupplyCore historical rollups from MariaDB to InfluxDB")
    parser.add_argument("--app-root", default=resolve_app_root(__file__))
    parser.add_argument("--dataset", action="append", default=[])
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=0)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(argv)


def _normalize_datasets(requested: list[str]) -> list[RollupDataset]:
    if requested == []:
        return list(DATASETS)

    normalized: list[RollupDataset] = []
    seen: set[str] = set()
    for key in requested:
        dataset_key = str(key).strip()
        if dataset_key == "":
            continue
        if dataset_key not in DATASET_MAP:
            available = ", ".join(sorted(DATASET_MAP))
            raise ValueError(f"Unknown dataset \"{dataset_key}\". Available datasets: {available}")
        if dataset_key in seen:
            continue
        seen.add(dataset_key)
        normalized.append(DATASET_MAP[dataset_key])
    return normalized


def _sync_state_get(db: SupplyCoreDb, dataset_key: str) -> dict[str, Any] | None:
    return db.fetch_one(
        "SELECT dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message FROM sync_state WHERE dataset_key = %s LIMIT 1",
        (dataset_key,),
    )


def _sync_state_upsert(
    db: SupplyCoreDb,
    dataset_key: str,
    sync_mode: str,
    status: str,
    last_success_at: str | None,
    last_cursor: str | None,
    last_row_count: int,
    last_checksum: str | None,
    last_error_message: str | None,
) -> None:
    db.execute(
        """
        INSERT INTO sync_state (
            dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            sync_mode = VALUES(sync_mode),
            status = VALUES(status),
            last_success_at = VALUES(last_success_at),
            last_cursor = VALUES(last_cursor),
            last_row_count = VALUES(last_row_count),
            last_checksum = VALUES(last_checksum),
            last_error_message = VALUES(last_error_message),
            updated_at = CURRENT_TIMESTAMP
        """,
        (dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message),
    )


def _sync_run_start(db: SupplyCoreDb, dataset_key: str, run_mode: str, cursor_start: str | None) -> int:
    return db.insert(
        "INSERT INTO sync_runs (dataset_key, run_mode, run_status, started_at, cursor_start) VALUES (%s, %s, 'running', UTC_TIMESTAMP(), %s)",
        (dataset_key, run_mode, cursor_start),
    )


def _sync_run_finish(
    db: SupplyCoreDb,
    run_id: int,
    run_status: str,
    source_rows: int,
    written_rows: int,
    cursor_end: str | None,
    error_message: str | None,
) -> None:
    db.execute(
        """
        UPDATE sync_runs
        SET run_status = %s,
            finished_at = UTC_TIMESTAMP(),
            source_rows = %s,
            written_rows = %s,
            cursor_end = %s,
            error_message = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
        """,
        (run_status, source_rows, written_rows, cursor_end, error_message, run_id),
    )


def _iso_cursor(raw: Any) -> str | None:
    if raw in (None, ""):
        return None
    if isinstance(raw, datetime):
        value = raw if raw.tzinfo is not None else raw.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return str(raw)


def _export_query(dataset: RollupDataset, full: bool) -> str:
    columns = ", ".join(dataset.order_by)
    window = "'1h'" if dataset.key.endswith("_1h") else "'1d'"
    if full:
        return f"SELECT *, {window} AS _window FROM {dataset.table} ORDER BY {columns}"
    return f"SELECT *, {window} AS _window FROM {dataset.table} WHERE updated_at >= %s ORDER BY {columns}"


def _lower_bound(state: dict[str, Any] | None, overlap_seconds: int, full: bool) -> str | None:
    if full:
        return None
    cursor_raw = (state or {}).get("last_cursor") or (state or {}).get("last_success_at")
    if cursor_raw in (None, ""):
        return None
    if isinstance(cursor_raw, datetime):
        cursor = cursor_raw if cursor_raw.tzinfo is not None else cursor_raw.replace(tzinfo=timezone.utc)
    else:
        cursor = datetime.strptime(str(cursor_raw), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return (cursor - timedelta(seconds=max(0, overlap_seconds))).strftime("%Y-%m-%d %H:%M:%S")


def _batch_lines(dataset: RollupDataset, rows: list[dict[str, Any]]) -> list[str]:
    return [
        encode_point(dataset.measurement, dataset.tags_fn(row), dataset.fields_fn(row), row[dataset.bucket_column])
        for row in rows
    ]


def _checksum(dataset_key: str, cursor_end: str | None, source_rows: int, written_rows: int) -> str:
    payload = json.dumps(
        {
            "dataset": dataset_key,
            "cursor_end": cursor_end,
            "source_rows": source_rows,
            "written_rows": written_rows,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _export_dataset(
    db: SupplyCoreDb,
    writer: InfluxWriter | None,
    dataset: RollupDataset,
    *,
    batch_size: int,
    overlap_seconds: int,
    full: bool,
    dry_run: bool,
    logger: LoggerAdapter,
) -> dict[str, Any]:
    state = _sync_state_get(db, dataset.dataset_key)
    lower_bound = _lower_bound(state, overlap_seconds, full)
    run_mode = "full" if full else "incremental"
    run_id = _sync_run_start(db, dataset.dataset_key, run_mode, lower_bound)
    _sync_state_upsert(db, dataset.dataset_key, run_mode, "running", state.get("last_success_at") if state else None, lower_bound, 0, state.get("last_checksum") if state else None, None)

    source_rows = 0
    written_rows = 0
    cursor_end: str | None = lower_bound

    logger.info(
        "Starting InfluxDB rollup export dataset.",
        payload={
            "dataset": dataset.key,
            "table": dataset.table,
            "run_mode": run_mode,
            "cursor_start": lower_bound,
            "dry_run": dry_run,
        },
    )

    try:
        query = _export_query(dataset, full or lower_bound is None)
        params: tuple[Any, ...] = () if full or lower_bound is None else (lower_bound,)
        for rows in db.iterate_batches(query, params, batch_size=batch_size):
            source_rows += len(rows)
            lines = _batch_lines(dataset, rows)
            if not dry_run and writer is not None:
                writer.write_lines(lines)
            written_rows += len(lines)
            cursor_end = _iso_cursor(rows[-1].get("updated_at"))
            logger.info(
                "Exported InfluxDB rollup batch.",
                payload={
                    "dataset": dataset.key,
                    "batch_rows": len(rows),
                    "source_rows_total": source_rows,
                    "written_rows_total": written_rows,
                    "cursor_end": cursor_end,
                    "dry_run": dry_run,
                },
            )

        last_success_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        checksum = _checksum(dataset.dataset_key, cursor_end, source_rows, written_rows)
        _sync_run_finish(db, run_id, "success", source_rows, written_rows, cursor_end, None)
        _sync_state_upsert(db, dataset.dataset_key, run_mode, "success", last_success_at, cursor_end, written_rows, checksum, None)
        logger.info(
            "Completed InfluxDB rollup export dataset.",
            payload={
                "dataset": dataset.key,
                "source_rows": source_rows,
                "written_rows": written_rows,
                "cursor_end": cursor_end,
                "dry_run": dry_run,
            },
        )
        return {
            "dataset": dataset.key,
            "run_status": "success",
            "source_rows": source_rows,
            "written_rows": written_rows,
            "cursor_end": cursor_end,
        }
    except Exception as error:
        message = str(error)
        _sync_run_finish(db, run_id, "failed", source_rows, written_rows, cursor_end, message[:500])
        _sync_state_upsert(db, dataset.dataset_key, run_mode, "failed", state.get("last_success_at") if state else None, cursor_end, written_rows, state.get("last_checksum") if state else None, message[:500])
        logger.error(
            "InfluxDB rollup export dataset failed.",
            payload={
                "dataset": dataset.key,
                "source_rows": source_rows,
                "written_rows": written_rows,
                "cursor_end": cursor_end,
                "error": message,
            },
        )
        raise


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    app_root = Path(args.app_root).resolve()
    runtime = load_php_runtime_config(app_root)
    influx_runtime = dict(runtime.raw.get("influxdb", {}))
    influx_config = InfluxConfig.from_runtime(influx_runtime)
    logger = configure_logging(
        logger_name="supplycore.influx_export",
        verbose=bool(args.verbose),
        log_file=Path(str(influx_runtime.get("export_log_file", "storage/logs/influx-rollup-export.log"))).resolve(),
    )

    datasets = _normalize_datasets(list(args.dataset))
    if not influx_config.enabled:
        logger.info("InfluxDB rollup export skipped because the integration is disabled.", payload={"datasets": [dataset.key for dataset in datasets]})
        return 0

    validation_errors = influx_config.validate()
    if validation_errors:
        for error in validation_errors:
            logger.error("InfluxDB configuration validation failed.", payload={"error": error})
        return 1

    db = SupplyCoreDb(dict(runtime.raw.get("db", {})))
    writer = None if args.dry_run else InfluxWriter(influx_config)
    batch_size = max(100, int(args.batch_size or influx_runtime.get("export_batch_size", 1000)))
    overlap_seconds = max(0, int(influx_runtime.get("export_overlap_seconds", 21600)))

    summaries: list[dict[str, Any]] = []
    for dataset in datasets:
        summaries.append(
            _export_dataset(
                db,
                writer,
                dataset,
                batch_size=batch_size,
                overlap_seconds=overlap_seconds,
                full=bool(args.full),
                dry_run=bool(args.dry_run),
                logger=logger,
            )
        )

    logger.info(
        "InfluxDB rollup export completed.",
        payload={
            "dataset_count": len(summaries),
            "datasets": summaries,
            "dry_run": bool(args.dry_run),
        },
    )
    return 0
