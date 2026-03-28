from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request

from .http_client import ipv4_opener
from .json_utils import json_loads_safe

ESI_BASE = "https://esi.evetech.net"
ESI_COMPATIBILITY_DATE = "2025-12-16"


@dataclass(slots=True)
class EsiOrdersResponse:
    orders: list[dict[str, Any]]
    pages: int
    status_code: int


class EsiMarketAdapter:
    def __init__(self, *, timeout_seconds: int = 30) -> None:
        self._timeout_seconds = max(5, timeout_seconds)

    def fetch_region_orders(self, *, region_id: int, order_type: str = "all", page: int = 1) -> EsiOrdersResponse:
        query = urlencode({"order_type": order_type, "page": page})
        url = f"{ESI_BASE}/latest/markets/{region_id}/orders/?{query}"
        return self._request_json(url)

    def fetch_structure_orders(self, *, structure_id: int, access_token: str, page: int = 1) -> EsiOrdersResponse:
        query = urlencode({"datasource": "tranquility", "page": page})
        url = f"{ESI_BASE}/latest/markets/structures/{structure_id}/?{query}"
        return self._request_json(url, token=access_token)

    def fetch_structure_metadata(self, *, structure_id: int, access_token: str) -> dict[str, Any]:
        query = urlencode({"datasource": "tranquility"})
        url = f"{ESI_BASE}/latest/universe/structures/{structure_id}/?{query}"
        return self._request_object(url, token=access_token)

    def _request_json(self, url: str, token: str | None = None) -> EsiOrdersResponse:
        request = Request(url, headers=self._request_headers(token), method="GET")
        try:
            with ipv4_opener.open(request, timeout=self._timeout_seconds) as response:
                payload = response.read().decode("utf-8")
                rows = json_loads_safe(payload)
                if not isinstance(rows, list):
                    rows = []
                pages = int(response.headers.get("X-Pages", "1") or "1")
                return EsiOrdersResponse(orders=[row for row in rows if isinstance(row, dict)], pages=max(1, pages), status_code=int(response.status))
        except HTTPError as exc:
            if exc.code == 304:
                return EsiOrdersResponse(orders=[], pages=1, status_code=304)
            raise RuntimeError(f"ESI request failed ({exc.code}) for {url}") from exc

    def _request_object(self, url: str, token: str | None = None) -> dict[str, Any]:
        request = Request(url, headers=self._request_headers(token), method="GET")
        try:
            with ipv4_opener.open(request, timeout=self._timeout_seconds) as response:
                payload = json_loads_safe(response.read().decode("utf-8"))
                if isinstance(payload, dict):
                    return payload
                return {}
        except HTTPError as exc:
            raise RuntimeError(f"ESI request failed ({exc.code}) for {url}") from exc

    def _request_headers(self, token: str | None = None) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "X-Compatibility-Date": ESI_COMPATIBILITY_DATE,
            "X-Tenant": "tranquility",
            "Accept-Language": "en",
            "User-Agent": "SupplyCore-Orchestrator/1.0",
        }
        if token:
            sanitized_token = token.strip()
            if sanitized_token.lower().startswith("bearer "):
                sanitized_token = sanitized_token[7:].strip()
            if sanitized_token:
                headers["Authorization"] = f"Bearer {sanitized_token}"
        return headers


def parse_esi_datetime(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    return parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")
