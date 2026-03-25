from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .bridge import PhpBridge
from .config import load_php_runtime_config
from .db import SupplyCoreDb
from .json_utils import json_dumps_safe


def _utc_now() -> datetime:
    return datetime.now(tz=UTC)


def _utc_now_iso() -> str:
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(slots=True)
class RebuildStatus:
    run_id: str
    mode: str
    window_days: int
    full_reset: bool
    enable_partitioned_history: bool
    status: str = "starting"
    current_phase: str = "boot"
    dataset: str = "initializing"
    rows_scanned: int = 0
    rows_written: int = 0
    elapsed_seconds: float = 0.0
    started_at: str = field(default_factory=_utc_now_iso)
    updated_at: str = field(default_factory=_utc_now_iso)
    completed_at: str | None = None
    last_progress_update: str | None = None
    error_message: str | None = None
    source_count_total: int = 0
    source_count_completed: int = 0
    steps: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "run_id": self.run_id,
            "mode": self.mode,
            "window_days": self.window_days,
            "full_reset": self.full_reset,
            "enable_partitioned_history": self.enable_partitioned_history,
            "status": self.status,
            "current_phase": self.current_phase,
            "dataset": self.dataset,
            "rows_scanned": self.rows_scanned,
            "rows_written": self.rows_written,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "last_progress_update": self.last_progress_update,
            "error_message": self.error_message,
            "source_count_total": self.source_count_total,
            "source_count_completed": self.source_count_completed,
            "steps": self.steps,
        }
        return payload


class StatusTracker:
    def __init__(self, path: Path, progress_interval_seconds: int) -> None:
        self.path = path
        self.progress_interval_seconds = max(1, progress_interval_seconds)
        self._last_emit_monotonic = 0.0

    def write(self, status: RebuildStatus, *, event: str, force_log: bool = False) -> None:
        now = _utc_now()
        status.updated_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        status.elapsed_seconds = max(0.0, (now - datetime.strptime(status.started_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)).total_seconds())
        payload = status.to_payload()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json_dumps_safe(payload, indent=2) + "\n", encoding="utf-8")
        os.replace(tmp_path, self.path)

        monotonic_now = time.monotonic()
        should_log = force_log or monotonic_now - self._last_emit_monotonic >= self.progress_interval_seconds
        if should_log:
            print(json.dumps({"event": event, **payload}, separators=(",", ":")), flush=True)
            self._last_emit_monotonic = monotonic_now


@dataclass(slots=True)
class CurrentSourceStats:
    source_type: str
    source_id: int
    latest_observed_at: str | None
    order_rows: int
    projection_rows: int


class RollupSourceAccumulator:
    def __init__(self, app_timezone: str, source_type: str, source_id: int) -> None:
        self.app_timezone = app_timezone
        self.source_type = source_type
        self.source_id = source_id
        self.summary_rows_written = 0
        self.rollup_rows_written = {"1h": 0, "1d": 0}
        self.daily_rows_written = 0
        self.local_daily_rows_written = 0
        self.summary_rows_scanned = 0
        self.last_observed_at = ""
        self.daily_buckets: dict[str, dict[str, Any]] = {}
        self.local_daily_buckets: dict[str, dict[str, Any]] = {}
        self.rollup_buckets: dict[str, dict[str, dict[str, Any]]] = {"1h": {}, "1d": {}}

    def observe_summary_row(self, row: dict[str, Any]) -> None:
        normalized = _normalize_summary_row(row, self.source_type)
        if normalized is None:
            return

        self.summary_rows_scanned += 1
        self.last_observed_at = normalized["observed_at"]
        _observe_rollup(self.rollup_buckets["1h"], normalized, resolution="1h")
        _observe_rollup(self.rollup_buckets["1d"], normalized, resolution="1d")
        _observe_daily_bucket(self.daily_buckets, normalized, self.app_timezone, self.source_type)
        if self.source_type == "market_hub":
            _observe_local_daily_bucket(self.local_daily_buckets, normalized, self.app_timezone)


class RebuildRunner:
    def __init__(self, app_root: Path, args: argparse.Namespace) -> None:
        self.app_root = app_root.resolve()
        self.config = load_php_runtime_config(self.app_root)
        self.db = SupplyCoreDb(self.config.raw["db"])
        self.bridge = PhpBridge(self.config.php_binary, self.app_root)
        table_context = self.bridge.call("market-history-tables-context")["context"]
        self.history_read_table = str(table_context.get("history_read_table") or "market_orders_history").strip()
        self.summary_read_table = str(table_context.get("summary_read_table") or "market_order_snapshots_summary").strip()
        self.status = RebuildStatus(
            run_id=str(uuid.uuid4()),
            mode=args.mode,
            window_days=_normalize_window_days(args.window_days),
            full_reset=bool(args.full_reset or args.mode == "full-reset"),
            enable_partitioned_history=bool(args.enable_partitioned_history),
        )
        if args.mode == "full-reset":
            self.status.mode = "rebuild-all-derived"
            self.status.full_reset = True
        self.window = _window_bounds(self.status.window_days, str(self.config.raw["app"].get("timezone", "UTC")))
        self.status_file = Path(self.config.raw["rebuild"]["status_file"]).resolve()
        self.lock_file = Path(self.config.raw["rebuild"]["lock_file"]).resolve()
        self.progress_interval_seconds = int(self.config.raw["rebuild"]["progress_interval_seconds"])
        self.tracker = StatusTracker(self.status_file, self.progress_interval_seconds)
        self.lock_handle: Any | None = None
        self.status.status = "running"

    def acquire_lock(self) -> None:
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.lock_handle = self.lock_file.open("a+")
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"Rebuild lock already held: {self.lock_file}") from exc
        self.lock_handle.seek(0)
        self.lock_handle.truncate(0)
        self.lock_handle.write(f"{os.getpid()}\n")
        self.lock_handle.flush()

    def run(self) -> int:
        self.acquire_lock()
        self.status.current_phase = "boot"
        self.status.dataset = "loading-runtime-config"
        self.tracker.write(self.status, event="rebuild.started", force_log=True)

        try:
            source_rows = self._source_keys()
            self.status.source_count_total = len(source_rows)
            self.status.steps["audit"] = {
                "source_tables": ["market_orders_current", self._history_table_name(), self._summary_table_name(), "market_source_snapshot_state"],
                "target_tables": [
                    "market_order_current_projection",
                    "market_source_snapshot_state",
                    "market_order_snapshots_summary",
                    "market_order_snapshot_rollup_1h",
                    "market_order_snapshot_rollup_1d",
                    "market_history_daily",
                    "market_hub_local_history_daily",
                ],
                "chunked_execution": True,
                "previous_php_cli_was_silent": True,
                "source_count_total": len(source_rows),
                "window": self.window,
            }
            self.tracker.write(self.status, event="rebuild.audit", force_log=True)

            if self.status.enable_partitioned_history:
                self._run_partition_step()

            if self.status.mode in ("rebuild-current-only", "rebuild-all-derived"):
                self._run_current_rebuild(source_rows)

            if self.status.mode in ("rebuild-rollups-only", "rebuild-all-derived"):
                self._run_rollup_rebuild(source_rows)

            self.status.status = "success"
            self.status.current_phase = "complete"
            self.status.dataset = "all"
            self.status.completed_at = _utc_now_iso()
            self.status.last_progress_update = self.status.completed_at
            self.tracker.write(self.status, event="rebuild.completed", force_log=True)
            return 0
        except Exception as exc:
            self.status.status = "failed"
            self.status.current_phase = "failed"
            self.status.error_message = str(exc)
            self.status.completed_at = _utc_now_iso()
            self.status.last_progress_update = self.status.completed_at
            self.tracker.write(self.status, event="rebuild.failed", force_log=True)
            print(json.dumps({"event": "rebuild.error", "error": str(exc), **self.status.to_payload()}, separators=(",", ":")), flush=True)
            return 1

    def _source_keys(self) -> list[dict[str, Any]]:
        return self.db.fetch_all(
            f"""
            SELECT source_type, source_id
            FROM (
                SELECT source_type, source_id FROM market_orders_current
                UNION
                SELECT source_type, source_id FROM {self._history_table_name()}
                UNION
                SELECT source_type, source_id FROM {self._summary_table_name()}
                UNION
                SELECT source_type, source_id FROM market_source_snapshot_state
            ) sources
            WHERE source_type <> ''
              AND source_id > 0
            ORDER BY source_type ASC, source_id ASC
            """
        )

    def _history_table_name(self) -> str:
        return self.history_read_table

    def _summary_table_name(self) -> str:
        return self.summary_read_table

    def _run_partition_step(self) -> None:
        self.status.current_phase = "partitioned_history"
        self.status.dataset = "market_orders_history partitions"
        self.status.last_progress_update = _utc_now_iso()
        self.tracker.write(self.status, event="rebuild.partition.start", force_log=True)
        result = self.bridge.call("rebuild-partitioned-history", payload={"window": self.window})["result"]
        self.status.steps["partitioned_history"] = result
        self.status.last_progress_update = _utc_now_iso()
        self.tracker.write(self.status, event="rebuild.partition.done", force_log=True)

    def _run_current_rebuild(self, source_rows: list[dict[str, Any]]) -> None:
        self.status.current_phase = "current_layers"
        self.status.dataset = "market_orders_current"
        if self.status.full_reset or self.status.mode == "rebuild-current-only":
            self.db.execute("TRUNCATE TABLE market_order_current_projection")
            self.db.execute("TRUNCATE TABLE market_source_snapshot_state")
        current_step: dict[str, Any] = {
            "reset_tables": ["market_order_current_projection", "market_source_snapshot_state"],
            "sources": [],
            "sources_processed": 0,
            "projection_rows_written": 0,
        }
        self.status.steps["current"] = current_step
        for index, source in enumerate(source_rows, start=1):
            source_type = str(source.get("source_type") or "").strip()
            source_id = int(source.get("source_id") or 0)
            if source_type == "" or source_id <= 0:
                continue
            dataset = f"{source_type}:{source_id}"
            self.status.dataset = dataset
            self.tracker.write(self.status, event="rebuild.current.source.start")
            stats = self._rebuild_current_source(source_type, source_id)
            if stats.order_rows <= 0:
                continue
            current_step["sources"].append({
                "source_type": source_type,
                "source_id": source_id,
                "order_rows": stats.order_rows,
                "projection_rows": stats.projection_rows,
                "latest_observed_at": stats.latest_observed_at,
            })
            current_step["sources_processed"] += 1
            current_step["projection_rows_written"] += stats.projection_rows
            self.status.source_count_completed = index
            self.status.rows_scanned += stats.order_rows
            self.status.rows_written += stats.projection_rows
            self.status.last_progress_update = _utc_now_iso()
            self.tracker.write(self.status, event="rebuild.current.source.done", force_log=True)

    def _rebuild_current_source(self, source_type: str, source_id: int) -> CurrentSourceStats:
        latest_row = self.db.fetch_one(
            """
            SELECT observed_at
            FROM market_orders_current
            WHERE source_type = %s AND source_id = %s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            [source_type, source_id],
        )
        latest_observed_at = str((latest_row or {}).get("observed_at") or "").strip()
        if latest_observed_at == "":
            return CurrentSourceStats(source_type, source_id, None, 0, 0)

        orders = self.db.fetch_all(
            """
            SELECT source_type, source_id, type_id, order_id, is_buy_order, price, volume_remain, observed_at
            FROM market_orders_current
            WHERE source_type = %s
              AND source_id = %s
              AND observed_at = %s
            ORDER BY type_id ASC, is_buy_order ASC, price ASC, order_id ASC
            """,
            [source_type, source_id, latest_observed_at],
        )
        projection_rows = _projection_rows_from_orders(orders)
        if projection_rows:
            _bulk_upsert(
                self.db,
                "market_order_current_projection",
                [
                    "source_type",
                    "source_id",
                    "type_id",
                    "observed_at",
                    "best_sell_price",
                    "best_buy_price",
                    "total_sell_volume",
                    "total_buy_volume",
                    "sell_order_count",
                    "buy_order_count",
                    "total_volume",
                ],
                projection_rows,
                [
                    "observed_at",
                    "best_sell_price",
                    "best_buy_price",
                    "total_sell_volume",
                    "total_buy_volume",
                    "sell_order_count",
                    "buy_order_count",
                    "total_volume",
                ],
            )
        distinct_types = len({int(row.get("type_id") or 0) for row in orders if int(row.get("type_id") or 0) > 0})
        _bulk_upsert(
            self.db,
            "market_source_snapshot_state",
            [
                "source_type",
                "source_id",
                "latest_current_observed_at",
                "latest_summary_observed_at",
                "current_order_count",
                "current_distinct_type_count",
                "summary_row_count",
                "last_synced_at",
            ],
            [{
                "source_type": source_type,
                "source_id": source_id,
                "latest_current_observed_at": latest_observed_at,
                "latest_summary_observed_at": None,
                "current_order_count": len(orders),
                "current_distinct_type_count": distinct_types,
                "summary_row_count": 0,
                "last_synced_at": latest_observed_at,
            }],
            [
                "latest_current_observed_at",
                "current_order_count",
                "current_distinct_type_count",
                "summary_row_count",
                "last_synced_at",
            ],
        )
        return CurrentSourceStats(source_type, source_id, latest_observed_at, len(orders), len(projection_rows))

    def _run_rollup_rebuild(self, source_rows: list[dict[str, Any]]) -> None:
        self.status.current_phase = "rollup_layers"
        self.status.dataset = "market_orders_history + market_orders_current"
        if self.status.full_reset:
            tables = [
                "market_order_snapshot_rollup_1h",
                "market_order_snapshot_rollup_1d",
                "market_order_snapshots_summary",
                "market_history_daily",
                "market_hub_local_history_daily",
            ]
            if self._table_exists("market_order_snapshots_summary_p"):
                tables.append("market_order_snapshots_summary_p")
            for table in tables:
                self.db.execute(f"TRUNCATE TABLE {table}")
        rollup_step: dict[str, Any] = {
            "window": self.window,
            "sources": [],
            "sources_processed": 0,
            "summary_rows_written": 0,
            "rollup_rows_written": {"1h": 0, "1d": 0},
            "daily_rows_written": 0,
            "local_daily_rows_written": 0,
        }
        self.status.steps["rollups"] = rollup_step

        for index, source in enumerate(source_rows, start=1):
            source_type = str(source.get("source_type") or "").strip()
            source_id = int(source.get("source_id") or 0)
            if source_type == "" or source_id <= 0:
                continue
            dataset = f"{source_type}:{source_id}"
            self.status.dataset = dataset
            self.tracker.write(self.status, event="rebuild.rollup.source.start")
            source_result = self._rebuild_rollup_source(source_type, source_id)
            if source_result is None:
                continue
            rollup_step["sources"].append(source_result)
            rollup_step["sources_processed"] += 1
            rollup_step["summary_rows_written"] += int(source_result["summary_rows_written"])
            rollup_step["rollup_rows_written"]["1h"] += int(source_result["rollup_rows_written"]["1h"])
            rollup_step["rollup_rows_written"]["1d"] += int(source_result["rollup_rows_written"]["1d"])
            rollup_step["daily_rows_written"] += int(source_result["daily_rows_written"])
            rollup_step["local_daily_rows_written"] += int(source_result["local_daily_rows_written"])
            self.status.source_count_completed = index
            self.status.last_progress_update = _utc_now_iso()
            self.tracker.write(self.status, event="rebuild.rollup.source.done", force_log=True)

        self.status.current_phase = "refresh_materialized_summaries"
        self.status.dataset = "market comparison + doctrine + activity"
        self.tracker.write(self.status, event="rebuild.refresh.start", force_log=True)
        refresh_result = self.bridge.call("refresh-derived-summaries", payload={"reason": "python-rebuild"})["result"]
        rollup_step.update(refresh_result)
        self.status.steps["rollups"] = rollup_step
        self.status.last_progress_update = _utc_now_iso()
        self.tracker.write(self.status, event="rebuild.refresh.done", force_log=True)

    def _rebuild_rollup_source(self, source_type: str, source_id: int) -> dict[str, Any] | None:
        start_date = str(self.window["start_date"])
        end_date = str(self.window["end_date"])
        start_observed_at = str(self.window["start_observed_at"])
        if not self.status.full_reset:
            for table in self._summary_write_tables():
                date_column = "observed_date" if table.endswith("_p") else "DATE(observed_at)"
                self.db.execute(
                    f"DELETE FROM {table} WHERE source_type = %s AND source_id = %s AND {date_column} BETWEEN %s AND %s",
                    [source_type, source_id, start_date, end_date],
                )
            self.db.execute(
                "DELETE FROM market_order_snapshot_rollup_1h WHERE source_type = %s AND source_id = %s AND bucket_start >= %s AND bucket_start < DATE_ADD(%s, INTERVAL 1 DAY)",
                [source_type, source_id, start_date, end_date],
            )
            self.db.execute(
                "DELETE FROM market_order_snapshot_rollup_1d WHERE source_type = %s AND source_id = %s AND bucket_start >= %s AND bucket_start < DATE_ADD(%s, INTERVAL 1 DAY)",
                [source_type, source_id, start_date, end_date],
            )
            self.db.execute(
                "DELETE FROM market_history_daily WHERE source_type = %s AND source_id = %s AND trade_date >= %s AND trade_date <= %s",
                [_normalized_daily_source_type(source_type), source_id, start_date, end_date],
            )
            if source_type == "market_hub":
                self.db.execute(
                    "DELETE FROM market_hub_local_history_daily WHERE source = %s AND source_id = %s AND trade_date >= %s AND trade_date <= %s",
                    ["market_hub_current_sync", source_id, start_date, end_date],
                )

        accumulator = RollupSourceAccumulator(str(self.config.raw["app"].get("timezone", "UTC")), source_type, source_id)
        scanned_before = self.status.rows_scanned
        written_before = self.status.rows_written
        for batch in self.db.iterate_batches(_summary_stream_sql(self._history_table_name()), [
            source_type,
            source_id,
            start_observed_at,
            source_type,
            source_id,
            start_observed_at,
            source_type,
            source_id,
            start_observed_at,
        ], batch_size=500):
            if not batch:
                continue
            normalized_batch = []
            projection_batch = []
            for row in batch:
                normalized = _normalize_summary_row(row, source_type)
                if normalized is None:
                    continue
                normalized_batch.append(normalized)
                projection_batch.append(_summary_row_to_projection_row(normalized))
                accumulator.observe_summary_row(normalized)

            if normalized_batch:
                written = 0
                for table in self._summary_write_tables():
                    summary_rows = normalized_batch if not table.endswith("_p") else [_summary_row_with_observed_date(row) for row in normalized_batch]
                    columns = [
                        "source_type", "source_id", "type_id", "observed_at", "best_sell_price", "best_buy_price",
                        "total_buy_volume", "total_sell_volume", "total_volume", "buy_order_count", "sell_order_count",
                    ]
                    if table.endswith("_p"):
                        columns.append("observed_date")
                    written += _bulk_upsert(
                        self.db,
                        table,
                        columns,
                        summary_rows,
                        [
                            "best_sell_price", "best_buy_price", "total_buy_volume", "total_sell_volume", "total_volume",
                            "buy_order_count", "sell_order_count",
                        ],
                    )
                accumulator.summary_rows_written += len(normalized_batch)
                _bulk_upsert(
                    self.db,
                    "market_order_current_projection",
                    [
                        "source_type", "source_id", "type_id", "observed_at", "best_sell_price", "best_buy_price",
                        "total_sell_volume", "total_buy_volume", "sell_order_count", "buy_order_count", "total_volume",
                    ],
                    projection_batch,
                    [
                        "observed_at", "best_sell_price", "best_buy_price", "total_sell_volume", "total_buy_volume",
                        "sell_order_count", "buy_order_count", "total_volume",
                    ],
                )
                self.status.rows_scanned += len(normalized_batch)
                self.status.rows_written += len(normalized_batch)
                self.status.last_progress_update = _utc_now_iso()
                self.tracker.write(self.status, event="rebuild.rollup.batch")

        if accumulator.summary_rows_scanned <= 0:
            return None

        rollup_1h_rows = list(accumulator.rollup_buckets["1h"].values())
        rollup_1d_rows = list(accumulator.rollup_buckets["1d"].values())
        daily_rows = list(accumulator.daily_buckets.values())
        local_daily_rows = list(accumulator.local_daily_buckets.values())

        accumulator.rollup_rows_written["1h"] = _bulk_upsert(
            self.db,
            "market_order_snapshot_rollup_1h",
            [
                "bucket_start", "source_type", "source_id", "type_id", "sample_count", "first_observed_at", "last_observed_at",
                "best_sell_price_min", "best_sell_price_max", "best_sell_price_sample_count", "best_sell_price_sum", "best_sell_price_last",
                "best_buy_price_min", "best_buy_price_max", "best_buy_price_sample_count", "best_buy_price_sum", "best_buy_price_last",
                "total_buy_volume_sum", "total_sell_volume_sum", "total_volume_sum", "buy_order_count_sum", "sell_order_count_sum",
            ],
            rollup_1h_rows,
            [
                "sample_count", "first_observed_at", "last_observed_at", "best_sell_price_min", "best_sell_price_max",
                "best_sell_price_sample_count", "best_sell_price_sum", "best_sell_price_last", "best_buy_price_min", "best_buy_price_max",
                "best_buy_price_sample_count", "best_buy_price_sum", "best_buy_price_last", "total_buy_volume_sum",
                "total_sell_volume_sum", "total_volume_sum", "buy_order_count_sum", "sell_order_count_sum",
            ],
        )
        accumulator.rollup_rows_written["1d"] = _bulk_upsert(
            self.db,
            "market_order_snapshot_rollup_1d",
            [
                "bucket_start", "source_type", "source_id", "type_id", "sample_count", "first_observed_at", "last_observed_at",
                "best_sell_price_min", "best_sell_price_max", "best_sell_price_sample_count", "best_sell_price_sum", "best_sell_price_last",
                "best_buy_price_min", "best_buy_price_max", "best_buy_price_sample_count", "best_buy_price_sum", "best_buy_price_last",
                "total_buy_volume_sum", "total_sell_volume_sum", "total_volume_sum", "buy_order_count_sum", "sell_order_count_sum",
            ],
            rollup_1d_rows,
            [
                "sample_count", "first_observed_at", "last_observed_at", "best_sell_price_min", "best_sell_price_max",
                "best_sell_price_sample_count", "best_sell_price_sum", "best_sell_price_last", "best_buy_price_min", "best_buy_price_max",
                "best_buy_price_sample_count", "best_buy_price_sum", "best_buy_price_last", "total_buy_volume_sum",
                "total_sell_volume_sum", "total_volume_sum", "buy_order_count_sum", "sell_order_count_sum",
            ],
        )
        accumulator.daily_rows_written = _bulk_upsert(
            self.db,
            "market_history_daily",
            [
                "source_type", "source_id", "type_id", "trade_date", "open_price", "high_price", "low_price", "close_price",
                "average_price", "volume", "order_count", "source_label", "observed_at",
            ],
            daily_rows,
            [
                "open_price", "high_price", "low_price", "close_price", "average_price", "volume", "order_count", "source_label", "observed_at",
            ],
        )
        if local_daily_rows:
            accumulator.local_daily_rows_written = _bulk_upsert(
                self.db,
                "market_hub_local_history_daily",
                [
                    "source", "source_id", "type_id", "trade_date", "open_price", "high_price", "low_price", "close_price",
                    "buy_price", "sell_price", "spread_value", "spread_percent", "volume", "buy_order_count", "sell_order_count", "captured_at",
                ],
                local_daily_rows,
                [
                    "open_price", "high_price", "low_price", "close_price", "buy_price", "sell_price", "spread_value", "spread_percent",
                    "volume", "buy_order_count", "sell_order_count", "captured_at",
                ],
            )
        self.status.rows_written += (
            accumulator.rollup_rows_written["1h"]
            + accumulator.rollup_rows_written["1d"]
            + accumulator.daily_rows_written
            + accumulator.local_daily_rows_written
        )

        existing_state = self.db.fetch_one(
            "SELECT current_order_count, current_distinct_type_count FROM market_source_snapshot_state WHERE source_type = %s AND source_id = %s LIMIT 1",
            [source_type, source_id],
        ) or {}
        current_order_count = int(existing_state.get("current_order_count") or 0)
        current_distinct_type_count = int(existing_state.get("current_distinct_type_count") or 0)
        _bulk_upsert(
            self.db,
            "market_source_snapshot_state",
            [
                "source_type",
                "source_id",
                "latest_current_observed_at",
                "latest_summary_observed_at",
                "current_order_count",
                "current_distinct_type_count",
                "summary_row_count",
                "last_synced_at",
            ],
            [{
                "source_type": source_type,
                "source_id": source_id,
                "latest_current_observed_at": None,
                "latest_summary_observed_at": accumulator.last_observed_at,
                "current_order_count": current_order_count,
                "current_distinct_type_count": current_distinct_type_count,
                "summary_row_count": accumulator.summary_rows_scanned,
                "last_synced_at": accumulator.last_observed_at,
            }],
            [
                "latest_summary_observed_at",
                "current_order_count",
                "current_distinct_type_count",
                "summary_row_count",
                "last_synced_at",
            ],
        )
        source_written_total = self.status.rows_written - written_before
        return {
            "source_type": source_type,
            "source_id": source_id,
            "summary_rows_scanned": accumulator.summary_rows_scanned,
            "summary_rows_written": accumulator.summary_rows_written,
            "rollup_rows_written": accumulator.rollup_rows_written,
            "daily_rows_written": accumulator.daily_rows_written,
            "local_daily_rows_written": accumulator.local_daily_rows_written,
            "latest_summary_observed_at": accumulator.last_observed_at,
            "rows_scanned_delta": self.status.rows_scanned - scanned_before,
            "rows_written_delta": source_written_total,
        }

    def _summary_write_tables(self) -> list[str]:
        tables = ["market_order_snapshots_summary"]
        if self._table_exists("market_order_snapshots_summary_p"):
            tables.append("market_order_snapshots_summary_p")
        return tables

    def _table_exists(self, table_name: str) -> bool:
        row = self.db.fetch_one(
            """
            SELECT COUNT(*) AS table_count
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            [self.config.raw["db"]["database"], table_name],
        )
        return int((row or {}).get("table_count") or 0) > 0


def _normalize_window_days(window_days: int | None) -> int:
    safe = int(window_days or 30)
    return max(1, min(365, safe))


def _window_bounds(window_days: int, app_timezone: str) -> dict[str, str | int]:
    safe = _normalize_window_days(window_days)
    try:
        tz = ZoneInfo(app_timezone or "UTC")
    except Exception:
        tz = UTC
    now_local = datetime.now(tz)
    start_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=tz) - timedelta(days=safe - 1)
    start_utc = start_local.astimezone(UTC)
    return {
        "window_days": safe,
        "start_observed_at": start_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": start_utc.strftime("%Y-%m-%d"),
        "end_date": _utc_now().strftime("%Y-%m-%d"),
    }


def _projection_rows_from_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for order in orders:
        source_type = str(order.get("source_type") or "").strip()
        source_id = int(order.get("source_id") or 0)
        type_id = int(order.get("type_id") or 0)
        observed_at = str(order.get("observed_at") or "").strip()
        if source_type == "" or source_id <= 0 or type_id <= 0 or observed_at == "":
            continue
        key = f"{source_type}:{source_id}:{type_id}:{observed_at}"
        row = rows.setdefault(key, {
            "source_type": source_type,
            "source_id": source_id,
            "type_id": type_id,
            "observed_at": observed_at,
            "best_sell_price": None,
            "best_buy_price": None,
            "total_sell_volume": 0,
            "total_buy_volume": 0,
            "sell_order_count": 0,
            "buy_order_count": 0,
            "total_volume": 0,
        })
        volume = max(0, int(order.get("volume_remain") or 0))
        price = float(order.get("price") or 0)
        if int(order.get("is_buy_order") or 0) == 1:
            row["best_buy_price"] = price if row["best_buy_price"] is None else max(float(row["best_buy_price"]), price)
            row["total_buy_volume"] += volume
            row["buy_order_count"] += 1
        else:
            row["best_sell_price"] = price if row["best_sell_price"] is None else min(float(row["best_sell_price"]), price)
            row["total_sell_volume"] += volume
            row["sell_order_count"] += 1
        row["total_volume"] += volume
    return list(rows.values())


def _summary_stream_sql(history_table: str) -> str:
    return f"""
        SELECT
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


def _normalize_summary_row(row: dict[str, Any], source_type: str) -> dict[str, Any] | None:
    observed_at = str(row.get("observed_at") or "").strip()
    normalized = {
        "source_type": source_type,
        "source_id": int(row.get("source_id") or 0),
        "type_id": int(row.get("type_id") or 0),
        "observed_at": observed_at,
        "best_sell_price": None if row.get("best_sell_price") is None else float(row.get("best_sell_price")),
        "best_buy_price": None if row.get("best_buy_price") is None else float(row.get("best_buy_price")),
        "total_buy_volume": max(0, int(row.get("total_buy_volume") or 0)),
        "total_sell_volume": max(0, int(row.get("total_sell_volume") or 0)),
        "total_volume": max(0, int(row.get("total_volume") or 0)),
        "buy_order_count": max(0, int(row.get("buy_order_count") or 0)),
        "sell_order_count": max(0, int(row.get("sell_order_count") or 0)),
    }
    if normalized["source_id"] <= 0 or normalized["type_id"] <= 0 or observed_at == "":
        return None
    return normalized


def _summary_row_to_projection_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_type": row["source_type"],
        "source_id": row["source_id"],
        "type_id": row["type_id"],
        "observed_at": row["observed_at"],
        "best_sell_price": row["best_sell_price"],
        "best_buy_price": row["best_buy_price"],
        "total_sell_volume": row["total_sell_volume"],
        "total_buy_volume": row["total_buy_volume"],
        "sell_order_count": row["sell_order_count"],
        "buy_order_count": row["buy_order_count"],
        "total_volume": row["total_volume"],
    }


def _summary_row_with_observed_date(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    result["observed_date"] = str(row["observed_at"])[:10]
    return result


def _normalized_daily_source_type(source_type: str) -> str:
    return "market_hub" if source_type == "market_hub" else "alliance_structure"


def _market_snapshot_trade_date(observed_at: str, app_timezone: str) -> str:
    raw = observed_at.strip()
    if raw == "":
        return ""
    try:
        captured_at = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        tz = ZoneInfo(app_timezone or "UTC")
    except Exception:
        return ""
    return captured_at.astimezone(tz).strftime("%Y-%m-%d")


def _observe_daily_bucket(buckets: dict[str, dict[str, Any]], metric: dict[str, Any], app_timezone: str, source_type: str) -> None:
    close_price = metric["best_sell_price"] if metric["best_sell_price"] is not None else metric["best_buy_price"]
    if close_price is None:
        return
    trade_date = _market_snapshot_trade_date(str(metric["observed_at"]), app_timezone)
    if trade_date == "":
        return
    key = f"{trade_date}:{metric['type_id']}"
    row = {
        "source_type": _normalized_daily_source_type(source_type),
        "source_id": metric["source_id"],
        "type_id": metric["type_id"],
        "trade_date": trade_date,
        "open_price": close_price,
        "high_price": close_price,
        "low_price": close_price,
        "close_price": close_price,
        "average_price": close_price,
        "volume": metric["total_volume"],
        "order_count": metric["buy_order_count"] + metric["sell_order_count"],
        "source_label": "supplycore_snapshot",
        "observed_at": metric["observed_at"],
        "_average_sum": close_price,
        "_average_count": 1,
    }
    if key not in buckets:
        buckets[key] = row
        return
    bucket = buckets[key]
    bucket["high_price"] = max(float(bucket["high_price"]), float(close_price))
    bucket["low_price"] = min(float(bucket["low_price"]), float(close_price))
    bucket["_average_sum"] = float(bucket.get("_average_sum") or 0.0) + float(close_price)
    bucket["_average_count"] = int(bucket.get("_average_count") or 0) + 1
    if str(metric["observed_at"]) >= str(bucket.get("observed_at") or ""):
        bucket["close_price"] = close_price
        bucket["volume"] = metric["total_volume"]
        bucket["order_count"] = metric["buy_order_count"] + metric["sell_order_count"]
        bucket["observed_at"] = metric["observed_at"]
    bucket["average_price"] = round(float(bucket["_average_sum"]) / max(1, int(bucket["_average_count"])), 2)
    buckets[key] = bucket


def _observe_local_daily_bucket(buckets: dict[str, dict[str, Any]], metric: dict[str, Any], app_timezone: str) -> None:
    sell_price = None if metric["best_sell_price"] is None else round(float(metric["best_sell_price"]), 2)
    buy_price = None if metric["best_buy_price"] is None else round(float(metric["best_buy_price"]), 2)
    close_price = sell_price if sell_price is not None else buy_price
    if close_price is None:
        return
    trade_date = _market_snapshot_trade_date(str(metric["observed_at"]), app_timezone)
    if trade_date == "":
        return
    spread_value = None
    spread_percent = None
    if sell_price is not None and buy_price is not None:
        spread_value = round(sell_price - buy_price, 2)
        if buy_price > 0:
            spread_percent = round((spread_value / buy_price) * 100.0, 4)
            if abs(spread_percent) > 9999.9999:
                spread_percent = None
    key = f"{trade_date}:{metric['type_id']}"
    row = {
        "source": "market_hub_current_sync",
        "source_id": metric["source_id"],
        "type_id": metric["type_id"],
        "trade_date": trade_date,
        "open_price": close_price,
        "high_price": close_price,
        "low_price": close_price,
        "close_price": close_price,
        "buy_price": buy_price,
        "sell_price": sell_price,
        "spread_value": spread_value,
        "spread_percent": spread_percent,
        "volume": metric["total_volume"],
        "buy_order_count": metric["buy_order_count"],
        "sell_order_count": metric["sell_order_count"],
        "captured_at": metric["observed_at"],
    }
    if key not in buckets:
        buckets[key] = row
        return
    bucket = buckets[key]
    bucket["high_price"] = max(float(bucket["high_price"]), float(close_price))
    bucket["low_price"] = min(float(bucket["low_price"]), float(close_price))
    if str(metric["observed_at"]) >= str(bucket.get("captured_at") or ""):
        bucket["close_price"] = close_price
        bucket["buy_price"] = buy_price
        bucket["sell_price"] = sell_price
        bucket["spread_value"] = spread_value
        bucket["spread_percent"] = spread_percent
        bucket["volume"] = metric["total_volume"]
        bucket["buy_order_count"] = metric["buy_order_count"]
        bucket["sell_order_count"] = metric["sell_order_count"]
        bucket["captured_at"] = metric["observed_at"]
    buckets[key] = bucket


def _observe_rollup(buckets: dict[str, dict[str, Any]], metric: dict[str, Any], *, resolution: str) -> None:
    observed_at = str(metric["observed_at"])
    bucket_start = observed_at[:13] + ":00:00" if resolution == "1h" else observed_at[:10]
    key = f"{bucket_start}:{metric['source_type']}:{metric['source_id']}:{metric['type_id']}"
    if key not in buckets:
        buckets[key] = {
            "bucket_start": bucket_start,
            "source_type": metric["source_type"],
            "source_id": metric["source_id"],
            "type_id": metric["type_id"],
            "sample_count": 0,
            "first_observed_at": observed_at,
            "last_observed_at": observed_at,
            "best_sell_price_min": None,
            "best_sell_price_max": None,
            "best_sell_price_sample_count": 0,
            "best_sell_price_sum": 0.0,
            "best_sell_price_last": None,
            "best_buy_price_min": None,
            "best_buy_price_max": None,
            "best_buy_price_sample_count": 0,
            "best_buy_price_sum": 0.0,
            "best_buy_price_last": None,
            "total_buy_volume_sum": 0,
            "total_sell_volume_sum": 0,
            "total_volume_sum": 0,
            "buy_order_count_sum": 0,
            "sell_order_count_sum": 0,
        }
    bucket = buckets[key]
    bucket["sample_count"] += 1
    if observed_at < str(bucket["first_observed_at"]):
        bucket["first_observed_at"] = observed_at
    if observed_at >= str(bucket["last_observed_at"]):
        bucket["last_observed_at"] = observed_at
        bucket["best_sell_price_last"] = metric["best_sell_price"]
        bucket["best_buy_price_last"] = metric["best_buy_price"]
    if metric["best_sell_price"] is not None:
        price = float(metric["best_sell_price"])
        bucket["best_sell_price_min"] = price if bucket["best_sell_price_min"] is None else min(float(bucket["best_sell_price_min"]), price)
        bucket["best_sell_price_max"] = price if bucket["best_sell_price_max"] is None else max(float(bucket["best_sell_price_max"]), price)
        bucket["best_sell_price_sample_count"] += 1
        bucket["best_sell_price_sum"] = round(float(bucket["best_sell_price_sum"]) + price, 2)
    if metric["best_buy_price"] is not None:
        price = float(metric["best_buy_price"])
        bucket["best_buy_price_min"] = price if bucket["best_buy_price_min"] is None else min(float(bucket["best_buy_price_min"]), price)
        bucket["best_buy_price_max"] = price if bucket["best_buy_price_max"] is None else max(float(bucket["best_buy_price_max"]), price)
        bucket["best_buy_price_sample_count"] += 1
        bucket["best_buy_price_sum"] = round(float(bucket["best_buy_price_sum"]) + price, 2)
    bucket["total_buy_volume_sum"] += int(metric["total_buy_volume"])
    bucket["total_sell_volume_sum"] += int(metric["total_sell_volume"])
    bucket["total_volume_sum"] += int(metric["total_volume"])
    bucket["buy_order_count_sum"] += int(metric["buy_order_count"])
    bucket["sell_order_count_sum"] += int(metric["sell_order_count"])


def _bulk_upsert(db: SupplyCoreDb, table: str, columns: list[str], rows: list[dict[str, Any]], update_columns: list[str], *, batch_size: int = 500) -> int:
    if not rows:
        return 0
    placeholders = "(" + ",".join(["%s"] * len(columns)) + ")"
    update_sql = ", ".join([f"{column}=VALUES({column})" for column in update_columns] + ["updated_at=CURRENT_TIMESTAMP"])
    total = 0
    for offset in range(0, len(rows), batch_size):
        batch = rows[offset:offset + batch_size]
        sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES " + ",".join([placeholders] * len(batch)) + f" ON DUPLICATE KEY UPDATE {update_sql}"
        params: list[Any] = []
        for row in batch:
            for column in columns:
                value = row.get(column)
                if column in {"best_sell_price_sum", "best_buy_price_sum", "average_price", "open_price", "high_price", "low_price", "close_price", "buy_price", "sell_price", "spread_value", "spread_percent", "best_sell_price", "best_buy_price"} and value is not None:
                    params.append(round(float(value), 4 if column == "spread_percent" else 2))
                else:
                    params.append(value)
        db.execute(sql, params)
        total += len(batch)
    return total


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild derived data model with live progress reporting")
    parser.add_argument("--app-root", default=str(Path(__file__).resolve().parents[2]))
    parser.add_argument("--mode", default="rebuild-all-derived", choices=["rebuild-current-only", "rebuild-rollups-only", "rebuild-all-derived", "full-reset"])
    parser.add_argument("--window-days", type=int, default=30)
    parser.add_argument("--full-reset", action="store_true")
    parser.add_argument("--enable-partitioned-history", dest="enable_partitioned_history", action="store_true", default=True)
    parser.add_argument("--disable-partitioned-history", dest="enable_partitioned_history", action="store_false")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    runner = RebuildRunner(Path(args.app_root), args)
    return runner.run()


if __name__ == "__main__":
    raise SystemExit(main())
