from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ..db import SupplyCoreDb
from ..influx import InfluxClient, InfluxConfig, InfluxQueryError
from ..job_result import JobResult


DECIMAL_ZERO = Decimal("0")


def to_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def _historical_mean_by_type(influx_raw: dict[str, Any], type_ids: list[int]) -> dict[int, Decimal]:
    if not bool(influx_raw.get("enabled", False)) or not type_ids:
        return {}

    config = InfluxConfig.from_runtime(influx_raw)
    client = InfluxClient(config)
    in_clause = ",".join(str(type_id) for type_id in sorted(set(type_ids)))
    flux = f'''
from(bucket: "{config.bucket}")
  |> range(start: -14d)
  |> filter(fn: (r) => r._measurement == "market_order_snapshot_rollup_1d")
  |> filter(fn: (r) => r._field == "close_price")
  |> filter(fn: (r) => contains(value: int(v: r.type_id), set: [{in_clause}]))
  |> group(columns: ["type_id"])
  |> mean()
'''
    try:
        rows = client.query_flux(flux)
    except InfluxQueryError:
        return {}

    out: dict[int, Decimal] = {}
    for row in rows:
        type_id = int(row.get("type_id") or 0)
        mean_price = to_decimal(row.get("_value"))
        if type_id > 0 and mean_price > DECIMAL_ZERO:
            out[type_id] = mean_price
    return out


def run_compute_signals(db: SupplyCoreDb, influx_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    """Retired — compute_signals originally materialised enriched signal rows
    out of the old ``buy_all_items`` / ``buy_all_summary`` precompute tables.
    Those tables have been dropped along with the hand-maintained buy-all
    pipeline. The auto buy-all job (``compute_auto_buyall``) now materialises
    its own deterministic list. This function is kept as a no-op so the
    scheduler keeps its slot without erroring on missing tables."""
    _started_at = datetime.now(UTC)
    return {
        "status": "success",
        "summary": "compute_signals retired — buy_all_items dropped.",
        "started_at": _started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "finished_at": _started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_ms": 0,
        "rows_seen": 0,
        "rows_processed": 0,
        "rows_written": 0,
        "meta": {"retired": True},
    }

def _decimal_json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
