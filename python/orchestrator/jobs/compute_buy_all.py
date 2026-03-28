from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from hashlib import sha256
from typing import Any

from ..db import SupplyCoreDb
from ..item_scope import filter_rows_by_allowed_type_ids, load_allowed_type_ids
from ..job_result import JobResult

_MODE_DEFINITIONS: dict[str, dict[str, Any]] = {
    "doctrine_critical": {
        "label": "Doctrine Critical",
        "description": "Restore blocked doctrine readiness first, even when economics are partial.",
        "default_sort": "necessity",
        "require_doctrine_linked": True,
        "positive_margin_bias": False,
    },
    "seed_backlog": {
        "label": "Seed Backlog",
        "description": "Seed alliance gaps from the hub while keeping haul efficiency visible.",
        "default_sort": "necessity",
        "require_doctrine_linked": False,
        "positive_margin_bias": False,
    },
    "opportunity": {
        "label": "Opportunity / Profit",
        "description": "Prefer positive net imports and repricing candidates with strong contribution after hauling.",
        "default_sort": "net_profit",
        "require_doctrine_linked": False,
        "positive_margin_bias": True,
    },
    "blended": {
        "label": "Blended",
        "description": "Mix doctrine urgency with positive-margin seed and import opportunities.",
        "default_sort": "blended_score",
        "require_doctrine_linked": False,
        "positive_margin_bias": False,
    },
    "custom": {
        "label": "Custom",
        "description": "Operator-selected filters and ranking controls.",
        "default_sort": "blended_score",
        "require_doctrine_linked": False,
        "positive_margin_bias": False,
    },
}

_SORT_OPTIONS: dict[str, str] = {
    "blended_score": "Blended score",
    "necessity": "Necessity",
    "net_profit": "Net profit",
    "doctrine_impact": "Doctrine impact",
    "blocked_fit_impact": "Blocked-fit impact",
    "volume_efficiency": "Volume efficiency",
    "isk_efficiency": "ISK efficiency",
    "margin_percent": "Net margin %",
}


def _php_default_filters(mode: str) -> dict[str, Any]:
    """Build the same default filter dict that PHP ``buy_all_request()`` produces.

    Key insertion order MUST match the PHP source exactly because PHP's
    ``json_encode`` preserves insertion order and the hash is compared."""
    mode_def = _MODE_DEFINITIONS.get(mode, _MODE_DEFINITIONS["blended"])
    return {
        "doctrine_linked_only": bool(mode_def.get("require_doctrine_linked", False)),
        "positive_net_margin_only": bool(mode_def.get("positive_margin_bias", False)),
        "allow_low_margin_doctrine_critical": True,
        "exclude_incomplete_pricing": False,
        "exclude_oversized_low_efficiency": False,
        "min_priority_threshold": 0,
        "min_net_margin_threshold": 0,
        "min_net_profit_threshold": 0,
    }


DEFAULT_REQUESTS: list[dict[str, Any]] = [
    {"mode": mode, "sort": defn["default_sort"]}
    for mode, defn in _MODE_DEFINITIONS.items()
    if mode != "custom"
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
    """Compute the same SHA-256 hash that PHP ``json_encode($filters)`` produces.

    PHP preserves key insertion order — do NOT sort keys here.  The filter
    dict must already have keys in the same order as PHP's ``buy_all_request()``."""
    encoded = json.dumps(filters, separators=(",", ":"), sort_keys=False)
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


def _freshness_card(db: SupplyCoreDb, source_type: str, label: str) -> dict[str, Any]:
    """Build a freshness card for a given market snapshot source type."""
    row = db.fetch_one(
        "SELECT MAX(observed_at) AS latest FROM market_order_snapshots_summary WHERE source_type = %s",
        (source_type,),
    )
    latest = row.get("latest") if row else None
    if latest is None:
        return {"label": "Unknown", "computed_relative": "No data", "computed_at": "Unavailable", "tone": "border-amber-400/20 bg-amber-500/10 text-amber-100"}
    from datetime import datetime as dt
    ts = latest if isinstance(latest, dt) else dt.fromisoformat(str(latest))
    age_seconds = (datetime.now(UTC) - ts.replace(tzinfo=UTC)).total_seconds()
    if age_seconds < 900:
        tone = "border-emerald-400/20 bg-emerald-500/10 text-emerald-100"
        freshness_label = "Fresh"
    elif age_seconds < 3600:
        tone = "border-amber-400/20 bg-amber-500/10 text-amber-100"
        freshness_label = "Recent"
    else:
        tone = "border-rose-400/20 bg-rose-500/10 text-rose-100"
        freshness_label = "Stale"
    minutes = int(age_seconds // 60)
    relative = f"{minutes}m ago" if minutes < 120 else f"{minutes // 60}h ago"
    return {
        "label": freshness_label,
        "computed_relative": relative,
        "computed_at": ts.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "tone": tone,
    }


def _doctrine_freshness(db: SupplyCoreDb) -> dict[str, Any]:
    """Build a freshness card for the doctrine intelligence dataset."""
    row = db.fetch_one(
        "SELECT MAX(updated_at) AS latest FROM intelligence_snapshots WHERE snapshot_key = %s",
        ("doctrine_fit_intelligence",),
    )
    latest = row.get("latest") if row else None
    if latest is None:
        return {"label": "Unknown", "computed_relative": "No data", "computed_at": "Unavailable", "tone": "border-amber-400/20 bg-amber-500/10 text-amber-100"}
    from datetime import datetime as dt
    ts = latest if isinstance(latest, dt) else dt.fromisoformat(str(latest))
    age_seconds = (datetime.now(UTC) - ts.replace(tzinfo=UTC)).total_seconds()
    if age_seconds < 900:
        tone = "border-emerald-400/20 bg-emerald-500/10 text-emerald-100"
        lbl = "Fresh"
    elif age_seconds < 3600:
        tone = "border-amber-400/20 bg-amber-500/10 text-amber-100"
        lbl = "Recent"
    else:
        tone = "border-rose-400/20 bg-rose-500/10 text-rose-100"
        lbl = "Stale"
    minutes = int(age_seconds // 60)
    relative = f"{minutes}m ago" if minutes < 120 else f"{minutes // 60}h ago"
    return {"label": lbl, "computed_relative": relative, "computed_at": ts.strftime("%Y-%m-%d %H:%M:%S UTC"), "tone": tone}


def run_compute_buy_all(db: SupplyCoreDb, requests: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    computed_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    planned_requests = requests or DEFAULT_REQUESTS
    market_rows = _load_market_rows(db)
    allowed_type_ids = load_allowed_type_ids()
    scoped_market_rows, scope_metrics = filter_rows_by_allowed_type_ids(
        market_rows,
        allowed_type_ids,
        type_id_getter=lambda row: int(row.get("type_id") or 0),
    )
    items = _ranked_items(scoped_market_rows)
    created = 0
    rows_written = 0
    rows_processed = len(scoped_market_rows)
    ranked_items_after_scope = len(items)

    # Build freshness cards once for all requests
    hub_freshness = _freshness_card(db, "market_hub", "Hub pricing")
    alliance_freshness = _freshness_card(db, "alliance_structure", "Alliance pricing")
    stock_freshness: dict[str, Any] = {
        "label": hub_freshness.get("label", "Unknown"),
        "computed_relative": hub_freshness.get("computed_relative", "Unknown"),
        "computed_at": hub_freshness.get("computed_at", "Unavailable"),
        "tone": hub_freshness.get("tone", ""),
    }
    doctrine_freshness = _doctrine_freshness(db)

    for request in planned_requests:
        mode = str(request.get("mode") or "blended")
        sort = str(request.get("sort") or "blended_score")
        filters = _php_default_filters(mode)
        filters_hash = _filters_hash(filters)
        page_items = items[:120]

        generated_at_iso = datetime.now(UTC).isoformat()
        total_buy_cost = _q2(sum((
            to_decimal(item.get("buy_price")) * Decimal(item.get("quantity", 0))
            for item in page_items
            if item.get("buy_price") is not None
        ), DECIMAL_ZERO))
        total_sell_value = _q2(sum((
            to_decimal(item.get("sell_price")) * Decimal(item.get("quantity", 0))
            for item in page_items
            if item.get("sell_price") is not None
        ), DECIMAL_ZERO))
        total_volume = _q2(sum((to_decimal(item.get("total_volume")) for item in page_items), DECIMAL_ZERO))
        total_hauling = _q2(total_volume * Decimal("320"))
        total_gross_profit = _q2(total_sell_value - total_buy_cost)
        total_net_profit = _q2(total_gross_profit - total_hauling)

        # Build clipboard text for in-game import
        clipboard_lines = []
        for item in page_items:
            name = str(item.get("item_name") or "")
            qty = int(item.get("quantity") or 0)
            if name and qty > 0:
                clipboard_lines.append(f"{name} {qty}")
        clipboard_text = "\n".join(clipboard_lines)

        mode_label = _MODE_DEFINITIONS.get(mode, {}).get("label", mode.replace("_", " ").title())

        active_page_data = {
            "number": 1,
            "items": page_items,
            "item_count": len(page_items),
            "total_units": sum(int(item.get("quantity") or 0) for item in page_items),
            "total_volume": total_volume,
            "total_buy_cost": total_buy_cost,
            "total_expected_sell_value": total_sell_value,
            "total_hauling_cost": total_hauling,
            "total_gross_profit": total_gross_profit,
            "total_net_profit": total_net_profit,
            "doctrine_critical_count": sum(1 for item in page_items if bool(item.get("is_doctrine_critical"))),
            "necessity_mix_summary": "",
            "clipboard_text": clipboard_text,
        }

        payload = {
            "request": {"mode": mode, "sort": sort, "page": 1, "filters": filters},
            "summary": {
                "generated_at": generated_at_iso,
                "mode": mode,
                "mode_label": mode_label,
                "page_count": 1,
                "candidate_count": len(page_items),
                "total_item_types": len(page_items),
                "total_units": sum(int(item.get("quantity") or 0) for item in page_items),
                "total_volume": total_volume,
                "total_buy_cost": total_buy_cost,
                "total_expected_sell_value": total_sell_value,
                "total_hauling_cost": total_hauling,
                "total_gross_profit": total_gross_profit,
                "total_net_profit": total_net_profit,
                "doctrine_critical_count": sum(1 for item in page_items if bool(item.get("is_doctrine_critical"))),
                "top_reason_theme": "Graph dependency priority" if any(
                    to_decimal(item.get("dependency_score")) > DECIMAL_ZERO for item in page_items
                ) else "Market-only priority",
            },
            "items": page_items,
            "pages": [{"number": 1, "items": page_items, "item_count": len(page_items), "clipboard_text": clipboard_text}],
            "active_page": 1,
            "active_page_data": active_page_data,
            "excluded_items": [],
            "freshness": {
                "generated_at": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "generated_relative": "0s ago",
                "hub_pricing": hub_freshness,
                "alliance_pricing": alliance_freshness,
                "stock": stock_freshness,
                "doctrine": doctrine_freshness,
            },
            "price_basis": {
                "buy": "Hub snapshot from market_order_snapshots_summary.",
                "sell": "Alliance snapshot from market_order_snapshots_summary.",
            },
            "hauling": {"cost_per_m3": Decimal("320"), "page_volume_limit": Decimal("385000"), "page_item_type_limit": 42},
            "mode_options": _MODE_DEFINITIONS,
            "sort_options": _SORT_OPTIONS,
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

    return JobResult.success(
        job_key="compute_buy_all",
        summary=f"Precomputed {created} buy-all request(s) with {rows_written} item rows.",
        rows_processed=rows_processed,
        rows_written=rows_written,
        meta={
            "computed_at": computed_at,
            "requests": created,
            "items_per_request": min(120, len(items)),
            "scope_filter_source": "php_bridge:item-scope-context",
            "scope_allowed_count": scope_metrics["scope_allowed_count"],
            "rows_before_scope": scope_metrics["rows_before_scope"],
            "rows_after_scope": scope_metrics["rows_after_scope"],
            "scope_allowed_type_ids_count": scope_metrics["scope_allowed_count"],
            "scope_market_rows_before_filter": scope_metrics["rows_before_scope"],
            "scope_market_rows_after_filter": scope_metrics["rows_after_scope"],
            "scope_ranked_rows_after_filter": ranked_items_after_scope,
        },
    ).to_dict()


def _decimal_json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
