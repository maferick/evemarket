from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from ..db import SupplyCoreDb

DEFAULT_REQUESTS: list[dict[str, Any]] = [
    {"mode": "blended", "sort": "blended_score", "filters": {}},
    {"mode": "doctrine_critical", "sort": "mode_rank_score", "filters": {}},
    {"mode": "opportunity", "sort": "mode_rank_score", "filters": {}},
    {"mode": "seed_backlog", "sort": "necessity_score", "filters": {}},
]


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
            CASE WHEN COALESCE(alliance.total_sell_volume, 0) < 20 THEN 1 ELSE 0 END AS weak_alliance_stock
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
        buy_price = float(row.get("buy_price") or 0.0)
        sell_price = float(row.get("sell_price") or 0.0)
        quantity = max(1, int((row.get("reference_total_sell_volume") or 0) * 0.06))
        quantity = min(500, quantity)
        necessity = min(100.0, (float(row.get("risk_score") or 0) * 0.55) + (35.0 if int(row.get("missing_in_alliance") or 0) == 1 else 0.0))
        profit = min(100.0, max(0.0, float(row.get("opportunity_score") or 0)))
        mode_rank = round((necessity * 0.55) + (profit * 0.45), 2)
        net_profit_per_unit = sell_price - buy_price if buy_price > 0 and sell_price > 0 else 0.0
        ranked.append(
            {
                "type_id": type_id,
                "item_name": str(row.get("type_name") or f"Type #{type_id}"),
                "quantity": quantity,
                "final_planner_quantity": quantity,
                "necessity_score": round(necessity, 2),
                "profit_score": round(profit, 2),
                "mode_rank_score": mode_rank,
                "blended_score": mode_rank,
                "buy_price": round(buy_price, 2) if buy_price > 0 else None,
                "sell_price": round(sell_price, 2) if sell_price > 0 else None,
                "net_profit_total": round(net_profit_per_unit * quantity, 2),
                "is_doctrine_critical": bool(int(row.get("missing_in_alliance") or 0) == 1 or int(row.get("weak_alliance_stock") or 0) == 1),
                "unit_volume": float(row.get("unit_volume") or 1.0),
                "total_volume": round(float(row.get("unit_volume") or 1.0) * quantity, 2),
                "missing_in_alliance": bool(int(row.get("missing_in_alliance") or 0) == 1),
                "weak_alliance_stock": bool(int(row.get("weak_alliance_stock") or 0) == 1),
            }
        )

    ranked.sort(key=lambda item: (float(item.get("mode_rank_score") or 0.0), float(item.get("necessity_score") or 0.0)), reverse=True)
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
                "total_volume": round(sum(float(item.get("total_volume") or 0.0) for item in page_items), 2),
                "total_net_profit": round(sum(float(item.get("net_profit_total") or 0.0) for item in page_items), 2),
                "doctrine_critical_count": sum(1 for item in page_items if bool(item.get("is_doctrine_critical"))),
                "top_reason_theme": "Precomputed intelligence",
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
            "hauling": {"cost_per_m3": 320.0, "page_volume_limit": 385000.0, "page_item_type_limit": 42},
            "mode_options": {},
            "sort_options": {},
        }

        summary_json = json.dumps(payload["summary"], separators=(",", ":"), ensure_ascii=False)
        payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
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
                        float(item.get("mode_rank_score") or 0.0),
                        float(item.get("necessity_score") or 0.0),
                        float(item.get("profit_score") or 0.0),
                        json.dumps(item, separators=(",", ":"), ensure_ascii=False),
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
