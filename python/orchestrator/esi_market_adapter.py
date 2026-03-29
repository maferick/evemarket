from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .esi_client import EsiClient
from .esi_rate_limiter import shared_limiter

ESI_BASE = "https://esi.evetech.net"
ESI_COMPATIBILITY_DATE = "2025-12-16"


@dataclass(slots=True)
class EsiOrdersResponse:
    orders: list[dict[str, Any]]
    pages: int
    status_code: int


class EsiMarketAdapter:
    def __init__(self, *, timeout_seconds: int = 30) -> None:
        self._client = EsiClient(timeout_seconds=timeout_seconds, limiter=shared_limiter)

    def fetch_region_orders(self, *, region_id: int, order_type: str = "all", page: int = 1) -> EsiOrdersResponse:
        resp = self._client.get(
            f"/latest/markets/{region_id}/orders/",
            params={"order_type": order_type, "page": page},
        )
        if resp.is_not_modified:
            return EsiOrdersResponse(orders=[], pages=1, status_code=304)
        if not resp.ok:
            raise RuntimeError(f"ESI request failed ({resp.status_code}) for region {region_id} orders")
        rows = resp.body if isinstance(resp.body, list) else []
        return EsiOrdersResponse(
            orders=[row for row in rows if isinstance(row, dict)],
            pages=resp.pages,
            status_code=resp.status_code,
        )

    def fetch_structure_orders(self, *, structure_id: int, access_token: str, page: int = 1) -> EsiOrdersResponse:
        resp = self._client.get(
            f"/latest/markets/structures/{structure_id}/",
            params={"datasource": "tranquility", "page": page},
            access_token=access_token,
        )
        if resp.is_not_modified:
            return EsiOrdersResponse(orders=[], pages=1, status_code=304)
        if not resp.ok:
            raise RuntimeError(f"ESI request failed ({resp.status_code}) for structure {structure_id} orders")
        rows = resp.body if isinstance(resp.body, list) else []
        return EsiOrdersResponse(
            orders=[row for row in rows if isinstance(row, dict)],
            pages=resp.pages,
            status_code=resp.status_code,
        )

    def fetch_structure_metadata(self, *, structure_id: int, access_token: str) -> dict[str, Any]:
        resp = self._client.get(
            f"/latest/universe/structures/{structure_id}/",
            params={"datasource": "tranquility"},
            access_token=access_token,
        )
        if not resp.ok:
            raise RuntimeError(f"ESI request failed ({resp.status_code}) for structure {structure_id} metadata")
        return resp.body if isinstance(resp.body, dict) else {}


def parse_esi_datetime(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    return parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
