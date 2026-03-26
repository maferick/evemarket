from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def make_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): make_json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [make_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def json_dumps_safe(value: Any, *, indent: int | None = None, sort_keys: bool = False) -> str:
    return json.dumps(
        make_json_safe(value),
        separators=(",", ":") if indent is None else None,
        ensure_ascii=False,
        indent=indent,
        sort_keys=sort_keys,
    )


def json_loads_safe(value: str | bytes | bytearray, *, fallback: Any = None) -> Any:
    try:
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
        return json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback
