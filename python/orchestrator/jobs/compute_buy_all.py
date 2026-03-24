from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
from typing import Any

from ..db import SupplyCoreDb

DEFAULT_REQUESTS: list[dict[str, Any]] = [
    {"mode": "blended", "sort": "blended_score", "filters": {}},
    {"mode": "doctrine_critical", "sort": "mode_rank_score", "filters": {}},
    {"mode": "opportunity", "sort": "mode_rank_score", "filters": {}},
    {"mode": "seed_backlog", "sort": "necessity_score", "filters": {}},
]

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


def _q2(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _filters_hash(filters: dict[str, Any]) -> str:
    encoded = json.dumps(filters, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _load_market_rows(db: SupplyCoreDb, limit: int = 600) -> list[dict[str, Any]]:
    return db.fetch_all(
        """
        SELECT
            ref.type_id,
            COALESCE(rit.type_name, CONCAT('Type #', ref.type_id)) AS type_name,
            GREATEST(0, LEAST(100, ROUND((COALESCE(ref.total_sell_volume, 0) / 100.0), 0))) AS opportunity_score,
            GREATEST(0, LEAST(100, ROUND((COALESCE(ref.total_sell_volume, 0) - COALESCE(alliance.total_sell_volume, 0)) / 80.0, 0))) AS risk_score,
            GREATEST(0, LEAST(100, ROUND((COALESCE(ref.sell_order_count, 0) / 5.0), 0))) AS volume_score,
            COALESCE(ref.best_sell_price, ref.best_buy_price, 0) AS buy_price,
            COALESCE(alliance.best_sell_price, alliance.best_buy_price, 0) AS sell_price,
            COALESCE(ref.total_sell_volume, 0) AS reference_total_sell_volume,
            COALESCE(alliance.total_sell_volume, 0) AS alliance_total_sell_volume,
            COALESCE(rit.volume, 1.0) AS unit_volume,
            CASE WHEN alliance.type_id IS NULL THEN 1 ELSE 0 END AS missing_in_alliance,
            CASE WHEN COALESCE(alliance.total_sell_volume, 0) < 20 THEN 1 ELSE 0 END AS weak_alliance_stock,
            COALESCE(ids.doctrine_count, 0) AS doctrine_count,
            COALESCE(ids.fit_count, 0) AS dependency_fit_count,
            COALESCE(ids.dependency_score, 0.0) AS dependency_score
        FROM market_order_snapshots_summary ref
        LEFT JOIN market_order_snapshots_summary alliance
            ON alliance.type_id = ref.type_id
           AND alliance.source_type = 'alliance_structure'
           AND alliance.observed_at = (
                SELECT MAX(inner_a.observed_at)
                FROM market_order_snapshots_summary inner_a
                WHERE inner_a.source_type = 'alliance_structure'
                  AND inner_a.source_id = alliance.source_id
           )
        LEFT JOIN ref_item_types rit ON rit.type_id = ref.type_id
        LEFT JOIN item_dependency_score ids ON ids.type_id = ref.type_id
        WHERE ref.source_type = 'market_hub'
          AND ref.observed_at = (
                SELECT MAX(inner_r.observed_at)
                FROM market_order_snapshots_summary inner_r
                WHERE inner_r.source_type = 'market_hub'
                  AND inner_r.source_id = ref.source_id
          )
        ORDER BY opportunity_score DESC, risk_score DESC, ref.type_id ASC
        LIMIT %s
        """,
        (max(50, min(2000, limit)),),
    )


def _ranked_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for row in rows:
        type_id = int(row.get("type_id") or 0)
        if type_id <= 0:
            continue
        buy_price = to_decimal(row.get("buy_price"))
        sell_price = to_decimal(row.get("sell_price"))
        quantity = max(1, int(to_decimal(row.get("reference_total_sell_volume")) * Decimal("0.06")))
        quantity = min(500, quantity)
        dependency_score = to_decimal(row.get("dependency_score"))
        doctrine_count = max(0, int(row.get("doctrine_count") or 0))
        dependency_fit_count = max(0, int(row.get("dependency_fit_count") or 0))
        risk_score = to_decimal(row.get("risk_score"))
        opportunity_score = to_decimal(row.get("opportunity_score"))
        unit_volume = to_decimal(row.get("unit_volume"), default="1")

        dependency_necessity_boost = min(Decimal("30"), dependency_score * Decimal("0.45"))
        doctrine_breadth_boost = min(Decimal("18"), Decimal(doctrine_count) * Decimal("1.8"))
        bottleneck_bonus = Decimal("12") if doctrine_count >= 3 and int(row.get("weak_alliance_stock") or 0) == 1 else DECIMAL_ZERO

        necessity = min(
            Decimal("100"),
            (risk_score * Decimal("0.50"))
            + (Decimal("35") if int(row.get("missing_in_alliance") or 0) == 1 else DECIMAL_ZERO)
            + dependency_necessity_boost
            + doctrine_breadth_boost
            + bottleneck_bonus,
        )
        profit = min(Decimal("100"), max(DECIMAL_ZERO, opportunity_score))
        mode_rank = _q2((necessity * Decimal("0.62")) + (profit * Decimal("0.38")))
        blended_score = _q2(min(Decimal("100"), mode_rank + min(Decimal("15"), dependency_score * Decimal("0.18"))))

        net_profit_per_unit = sell_price - buy_price if buy_price > DECIMAL_ZERO and sell_price > DECIMAL_ZERO else DECIMAL_ZERO
        graph_reason = (
            f"Used by {doctrine_count} doctrine(s), {dependency_fit_count} fit(s), dependency score {_q2(dependency_score):.1f}."
            if dependency_score > DECIMAL_ZERO
            else "No graph dependency enrichment yet."
        )
        total_volume = _q2(unit_volume * Decimal(quantity))
        ranked.append(
            {
                "type_id": type_id,
                "item_name": str(row.get("type_name") or f"Type #{type_id}"),
                "quantity": quantity,
                "final_planner_quantity": quantity,
                "necessity_score": _q2(necessity),
                "profit_score": _q2(profit),
                "mode_rank_score": mode_rank,
                "blended_score": blended_score,
                "final_priority_score": blended_score,
                "buy_price": _q2(buy_price) if buy_price > DECIMAL_ZERO else None,
                "sell_price": _q2(sell_price) if sell_price > DECIMAL_ZERO else None,
                "net_profit_total": _q2(net_profit_per_unit * Decimal(quantity)),
                "is_doctrine_critical": bool(
                    int(row.get("missing_in_alliance") or 0) == 1
                    or int(row.get("weak_alliance_stock") or 0) == 1
                    or doctrine_count >= 3
                ),
                "unit_volume": unit_volume,
                "total_volume": total_volume,
                "missing_in_alliance": bool(int(row.get("missing_in_alliance") or 0) == 1),
                "weak_alliance_stock": bool(int(row.get("weak_alliance_stock") or 0) == 1),
                "valid_doctrine_count": doctrine_count,
                "valid_fits_count": dependency_fit_count,
                "dependency_score": dependency_score.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP),
                "reason_text": graph_reason,
                "reason_theme": "Graph dependency priority" if dependency_score > DECIMAL_ZERO else "Market-only priority",
            }
        )

    ranked.sort(
        key=lambda item: (
            to_decimal(item.get("blended_score")),
            to_decimal(item.get("necessity_score")),
        ),
        reverse=True,
    )
    for idx, item in enumerate(ranked, start=1):
        item["rank_position"] = idx
    return ranked


def run_compute_buy_all(db: SupplyCoreDb, requests: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    planned_requests = requests or DEFAULT_REQUESTS
    market_rows = _load_market_rows(db)
    items = _ranked_items(market_rows)
    created = 0
    rows_written = 0
    rows_processed = len(market_rows)

    for request in planned_requests:
        mode = str(request.get("mode") or "blended")
        sort = str(request.get("sort") or "blended_score")
        filters = dict(request.get("filters") or {})
        filters_hash = _filters_hash(filters)
        page_items = items[:120]

        payload = {
            "request": {"mode": mode, "sort": sort, "page": 1, "filters": filters},
            "summary": {
                "generated_at": datetime.now(UTC).isoformat(),
                "mode": mode,
                "page_count": 1,
                "candidate_count": len(page_items),
                "total_item_types": len(page_items),
                "total_units": sum(int(item.get("quantity") or 0) for item in page_items),
                "total_volume": _q2(sum(to_decimal(item.get("total_volume")) for item in page_items)),
                "total_net_profit": _q2(sum(to_decimal(item.get("net_profit_total")) for item in page_items)),
                "doctrine_critical_count": sum(1 for item in page_items if bool(item.get("is_doctrine_critical"))),
                "top_reason_theme": "Graph dependency priority",
            },
            "items": page_items,
            "pages": [{"number": 1, "items": page_items, "item_count": len(page_items)}],
            "active_page": 1,
            "active_page_data": {"number": 1, "items": page_items, "item_count": len(page_items)},
            "excluded_items": [],
            "freshness": {"generated_at": datetime.now(UTC).isoformat()},
            "price_basis": {
                "buy": "Hub snapshot from market_comparison_snapshot.",
                "sell": "Alliance snapshot from market_comparison_snapshot.",
            },
            "hauling": {"cost_per_m3": Decimal("320"), "page_volume_limit": Decimal("385000"), "page_item_type_limit": 42},
            "mode_options": {},
            "sort_options": {},
        }

        summary_json = json.dumps(payload["summary"], separators=(",", ":"), ensure_ascii=False, default=_decimal_json_default)
        payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=_decimal_json_default)
        with db.transaction() as (_, cursor):
            cursor.execute(
                """
                INSERT INTO buy_all_summary (mode_key, sort_key, filters_hash, summary_json, payload_json, computed_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    summary_json = VALUES(summary_json),
                    payload_json = VALUES(payload_json),
                    computed_at = VALUES(computed_at),
                    updated_at = CURRENT_TIMESTAMP
                """,
                (mode, sort, filters_hash, summary_json, payload_json, computed_at),
            )
            cursor.execute(
                "SELECT id FROM buy_all_summary WHERE mode_key = %s AND sort_key = %s AND filters_hash = %s LIMIT 1",
                (mode, sort, filters_hash),
            )
            summary_row = cursor.fetchone() or {}
            summary_id = int(summary_row.get("id") or 0)
            if summary_id <= 0:
                raise RuntimeError("Failed to resolve summary_id for buy-all precompute.")

            cursor.execute("DELETE FROM buy_all_items WHERE summary_id = %s", (summary_id,))
            for item in page_items:
                cursor.execute(
                    """
                    INSERT INTO buy_all_items (summary_id, page_number, rank_position, type_id, quantity, mode_rank_score, necessity_score, profit_score, item_json, computed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        rank_position = VALUES(rank_position),
                        quantity = VALUES(quantity),
                        mode_rank_score = VALUES(mode_rank_score),
                        necessity_score = VALUES(necessity_score),
                        profit_score = VALUES(profit_score),
                        item_json = VALUES(item_json),
                        computed_at = VALUES(computed_at)
                    """,
                    (
                        summary_id,
                        1,
                        int(item.get("rank_position") or 0),
                        int(item.get("type_id") or 0),
                        int(item.get("quantity") or 0),
                        to_decimal(item.get("mode_rank_score")),
                        to_decimal(item.get("necessity_score")),
                        to_decimal(item.get("profit_score")),
                        json.dumps(item, separators=(",", ":"), ensure_ascii=False, default=_decimal_json_default),
                        computed_at,
                    ),
                )
                rows_written += 1

        created += 1

    return {
        "computed_at": computed_at,
        "requests": created,
        "items_per_request": min(120, len(items)),
        "rows_processed": rows_processed,
        "rows_written": rows_written,
    }


def _decimal_json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
