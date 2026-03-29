from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TYPE_CHECKING

from .esi_client import EsiClient
from .esi_rate_limiter import shared_limiter

if TYPE_CHECKING:
    from .esi_gateway import EsiGateway

ESI_BASE = "https://esi.evetech.net"
ESI_COMPATIBILITY_DATE = "2025-12-16"


@dataclass(slots=True)
class EsiOrdersResponse:
    orders: list[dict[str, Any]]
    pages: int
    status_code: int


class EsiMarketAdapter:
    def __init__(self, *, timeout_seconds: int = 30, gateway: EsiGateway | None = None) -> None:
        self._client = EsiClient(timeout_seconds=timeout_seconds, limiter=shared_limiter)
        self._gateway = gateway

    def fetch_region_orders(self, *, region_id: int, order_type: str = "all", page: int = 1) -> EsiOrdersResponse:
        if self._gateway:
            return self._fetch_region_orders_via_gateway(region_id=region_id, order_type=order_type)
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
        if self._gateway:
            return self._fetch_structure_orders_via_gateway(structure_id=structure_id, access_token=access_token)
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
        if self._gateway:
            resp = self._gateway.get(
                f"/latest/universe/structures/{structure_id}/",
                params={"datasource": "tranquility"},
                access_token=access_token,
                route_template="/latest/universe/structures/{structure_id}/",
            )
            if resp.from_cache or resp.not_modified:
                return {}
            if not (200 <= resp.status_code < 300):
                raise RuntimeError(f"ESI request failed ({resp.status_code}) for structure {structure_id} metadata")
            return resp.body if isinstance(resp.body, dict) else {}

        resp = self._client.get(
            f"/latest/universe/structures/{structure_id}/",
            params={"datasource": "tranquility"},
            access_token=access_token,
        )
        if not resp.ok:
            raise RuntimeError(f"ESI request failed ({resp.status_code}) for structure {structure_id} metadata")
        return resp.body if isinstance(resp.body, dict) else {}

    # -- Gateway-backed paginated fetchers ---------------------------------

    def _fetch_region_orders_via_gateway(self, *, region_id: int, order_type: str) -> EsiOrdersResponse:
        """Fetch all pages via gateway with Last-Modified consistency."""
        responses = self._gateway.get_paginated(
            f"/latest/markets/{region_id}/orders/",
            params={"order_type": order_type},
            route_template="/latest/markets/{region_id}/orders/",
            max_pages=20,
        )
        all_orders: list[dict[str, Any]] = []
        last_status = 200
        total_pages = 1
        for resp in responses:
            if resp.from_cache or resp.not_modified:
                continue
            if resp.body and isinstance(resp.body, list):
                all_orders.extend(row for row in resp.body if isinstance(row, dict))
            last_status = resp.status_code
            total_pages = max(total_pages, resp.pages)
        return EsiOrdersResponse(orders=all_orders, pages=total_pages, status_code=last_status)

    def _fetch_structure_orders_via_gateway(self, *, structure_id: int, access_token: str) -> EsiOrdersResponse:
        """Fetch all pages via gateway with Last-Modified consistency."""
        responses = self._gateway.get_paginated(
            f"/latest/markets/structures/{structure_id}/",
            params={"datasource": "tranquility"},
            access_token=access_token,
            route_template="/latest/markets/structures/{structure_id}/",
            max_pages=20,
        )
        all_orders: list[dict[str, Any]] = []
        last_status = 200
        total_pages = 1
        for resp in responses:
            if resp.from_cache or resp.not_modified:
                continue
            if resp.body and isinstance(resp.body, list):
                all_orders.extend(row for row in resp.body if isinstance(row, dict))
            last_status = resp.status_code
            total_pages = max(total_pages, resp.pages)
        return EsiOrdersResponse(orders=all_orders, pages=total_pages, status_code=last_status)


def parse_esi_datetime(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    return parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
