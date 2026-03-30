from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, TypeVar

from .bridge import PhpBridge
from .config import resolve_app_root

T = TypeVar("T")


def load_allowed_type_ids(*, php_binary: str | None = None, app_root: Path | None = None) -> set[int]:
    """Load authoritative allowed type IDs from PHP settings-driven scope context."""
    resolved_php_binary = php_binary or os.environ.get("SUPPLYCORE_PHP_BINARY", "php")
    resolved_app_root = app_root or Path(resolve_app_root(__file__))
    bridge = PhpBridge(resolved_php_binary, resolved_app_root)
    response = bridge.call("item-scope-context")
    context = dict(response.get("context", {}))

    allowed: set[int] = set()
    for raw_type_id in context.get("allowed_type_ids", []):
        try:
            type_id = int(raw_type_id)
        except (TypeError, ValueError):
            continue
        if type_id > 0:
            allowed.add(type_id)
    return allowed


def filter_rows_by_allowed_type_ids(
    rows: list[T],
    allowed_type_ids: set[int],
    *,
    type_id_getter: Callable[[T], Any],
) -> tuple[list[T], dict[str, int]]:
    """Filter rows by allowed type IDs and return consistency metrics."""
    rows_before_scope = len(rows)
    if not allowed_type_ids:
        return rows, {
            "scope_allowed_count": 0,
            "rows_before_scope": rows_before_scope,
            "rows_after_scope": rows_before_scope,
        }

    filtered_rows: list[T] = []
    for row in rows:
        try:
            type_id = int(type_id_getter(row))
        except (TypeError, ValueError):
            continue
        if type_id in allowed_type_ids:
            filtered_rows.append(row)

    return filtered_rows, {
        "scope_allowed_count": len(allowed_type_ids),
        "rows_before_scope": rows_before_scope,
        "rows_after_scope": len(filtered_rows),
    }
