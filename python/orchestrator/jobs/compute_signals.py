from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from ..db import SupplyCoreDb
from ..influx import InfluxClient, InfluxConfig, InfluxQueryError


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
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    rows = db.fetch_all(
        """
        SELECT
            bai.type_id,
            MAX(bai.mode_rank_score) AS mode_rank_score,
            MAX(bai.necessity_score) AS necessity_score,
            MAX(bai.profit_score) AS profit_score,
            MAX(CASE WHEN rit.type_name IS NULL OR rit.type_name = '' THEN CONCAT('Type #', bai.type_id) ELSE rit.type_name END) AS type_name,
            MAX(COALESCE(bai.quantity, 0)) AS planner_qty,
            MAX(COALESCE(mh.best_sell_price, mh.best_buy_price, 0)) AS hub_price,
            MAX(COALESCE(asrc.best_sell_price, asrc.best_buy_price, 0)) AS alliance_price,
            MAX(COALESCE(asrc.total_sell_volume, 0)) AS alliance_volume,
            MAX(COALESCE(ids.dependency_score, 0.0)) AS dependency_score,
            MAX(COALESCE(ids.doctrine_count, 0)) AS doctrine_count,
            MAX(COALESCE(ids.fit_count, 0)) AS dependency_fit_count
        FROM buy_all_items bai
        INNER JOIN buy_all_summary bas ON bas.id = bai.summary_id
        LEFT JOIN ref_item_types rit ON rit.type_id = bai.type_id
        LEFT JOIN market_order_snapshots_summary mh ON mh.type_id = bai.type_id AND mh.source_type = 'market_hub'
        LEFT JOIN market_order_snapshots_summary asrc ON asrc.type_id = bai.type_id AND asrc.source_type = 'alliance_structure'
        LEFT JOIN item_dependency_score ids ON ids.type_id = bai.type_id
        WHERE bas.computed_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 24 HOUR)
        GROUP BY bai.type_id
        ORDER BY mode_rank_score DESC
        LIMIT 250
        """
    )

    type_ids = [int(row.get("type_id") or 0) for row in rows if int(row.get("type_id") or 0) > 0]
    historical = _historical_mean_by_type(influx_raw or {}, type_ids)

    created = 0
    with db.transaction() as (_, cursor):
        cursor.execute("DELETE FROM signals WHERE computed_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)")

        for row in rows:
            type_id = int(row.get("type_id") or 0)
            if type_id <= 0:
                continue
            title = str(row.get("type_name") or f"Type #{type_id}")
            hub_price = to_decimal(row.get("hub_price"))
            alliance_volume = int(row.get("alliance_volume") or 0)
            necessity = to_decimal(row.get("necessity_score"))
            dependency_score = to_decimal(row.get("dependency_score"))
            doctrine_count = int(row.get("doctrine_count") or 0)
            dependency_fit_count = int(row.get("dependency_fit_count") or 0)

            signal_type = "market_spike"
            severity = "medium"
            mode_rank_score = to_decimal(row.get("mode_rank_score"))
            signal_text = f"Planner rank {mode_rank_score:.1f} with alliance volume {alliance_volume}."

            hist = to_decimal(historical.get(type_id))
            price_worsening = bool(hist > DECIMAL_ZERO and hub_price > (hist * Decimal("1.10")))
            low_supply = alliance_volume < 20
            high_dependency = dependency_score >= Decimal("30") or doctrine_count >= 3

            if high_dependency and low_supply and price_worsening:
                signal_type = "high_dependency_low_supply"
                severity = "critical"
                signal_text = (
                    f"Dependency score {dependency_score:.1f} across {doctrine_count} doctrines with low alliance volume {alliance_volume}; "
                    f"hub price {hub_price:.2f} is above 14d mean {hist:.2f}."
                )
            elif high_dependency and low_supply:
                signal_type = "critical_shared_dependency"
                severity = "critical"
                signal_text = (
                    f"Shared dependency risk: score {dependency_score:.1f}, doctrines {doctrine_count}, fits {dependency_fit_count}, "
                    f"alliance volume {alliance_volume}."
                )
            elif high_dependency and necessity >= Decimal("60"):
                signal_type = "doctrine_bottleneck"
                severity = "high"
                signal_text = (
                    f"Doctrine bottleneck candidate with dependency score {dependency_score:.1f}, necessity {necessity:.1f}, "
                    f"used by {doctrine_count} doctrines."
                )
            elif hist > DECIMAL_ZERO and hub_price > DECIMAL_ZERO and hub_price <= hist * Decimal("0.9"):
                signal_type = "undervalued_item"
                severity = "high"
                signal_text = f"Hub price {hub_price:.2f} is below 14d mean {hist:.2f}."
            elif alliance_volume < 25 and necessity >= Decimal("55"):
                signal_type = "supply_shortage"
                severity = "critical"
                signal_text = f"Alliance volume {alliance_volume} is low for necessity score {necessity:.1f}."
            elif necessity >= Decimal("70"):
                signal_type = "doctrine_blocking_item"
                severity = "high"
                signal_text = f"Necessity score {necessity:.1f} indicates doctrine blocking risk."

            payload = {
                "hub_price": hub_price,
                "historical_mean": hist if hist > DECIMAL_ZERO else None,
                "alliance_volume": alliance_volume,
                "planner_qty": int(row.get("planner_qty") or 0),
                "mode_rank_score": mode_rank_score,
                "necessity_score": necessity,
                "profit_score": to_decimal(row.get("profit_score")),
                "dependency_score": dependency_score,
                "doctrine_count": doctrine_count,
                "dependency_fit_count": dependency_fit_count,
            }
            signal_key = f"{signal_type}:{type_id}"
            cursor.execute(
                """
                INSERT INTO signals (signal_key, signal_type, severity, type_id, doctrine_fit_id, signal_title, signal_text, signal_payload_json, computed_at)
                VALUES (%s, %s, %s, %s, NULL, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    severity = VALUES(severity),
                    signal_title = VALUES(signal_title),
                    signal_text = VALUES(signal_text),
                    signal_payload_json = VALUES(signal_payload_json),
                    computed_at = VALUES(computed_at)
                """,
                (signal_key, signal_type, severity, type_id, title, signal_text, json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=_decimal_json_default), computed_at),
            )
            created += 1

    return {"computed_at": computed_at, "signals_created": created, "rows_processed": len(rows), "rows_written": created}


def _decimal_json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
