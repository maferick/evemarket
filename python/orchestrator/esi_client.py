"""Unified ESI HTTP client with centralized rate limiting.

All ESI calls should go through this module so that rate-limit budgets
are tracked in one place and requests are automatically throttled.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request

from .esi_rate_limiter import EsiRateLimiter, shared_limiter, token_cost_for_status
from .http_client import ipv4_opener

logger = logging.getLogger("supplycore.esi_client")

ESI_BASE = "https://esi.evetech.net"
ESI_COMPATIBILITY_DATE = "2025-12-16"
DEFAULT_USER_AGENT = "SupplyCore-Orchestrator/1.0"


@dataclass(slots=True)
class EsiResponse:
    """Wrapper around an ESI HTTP response."""

    status_code: int
    body: Any  # parsed JSON (dict, list, or None)
    headers: dict[str, str]

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    @property
    def is_not_modified(self) -> bool:
        return self.status_code == 304

    @property
    def is_rate_limited(self) -> bool:
        return self.status_code == 429

    @property
    def is_error_limited(self) -> bool:
        return self.status_code == 420

    @property
    def pages(self) -> int:
        """Return X-Pages header value (for paginated endpoints)."""
        try:
            return max(1, int(self.headers.get("X-Pages", "1")))
        except (ValueError, TypeError):
            return 1


class EsiClient:
    """ESI HTTP client with automatic rate limiting.

    All requests go through :meth:`get`, which calls
    ``limiter.acquire()`` before and ``limiter.update_from_response()``
    after every request.  Multiple ``EsiClient`` instances can share the
    same ``EsiRateLimiter`` (the default is the process-wide singleton).

    Usage::

        client = EsiClient(user_agent="MyApp/1.0")
        resp = client.get("/latest/markets/10000002/orders/", params={"order_type": "all"})
        if resp.ok:
            orders = resp.body
    """

    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout_seconds: int = 30,
        limiter: EsiRateLimiter | None = None,
        access_token: str | None = None,
    ) -> None:
        self._user_agent = user_agent
        self._timeout = max(5, timeout_seconds)
        self._limiter = limiter or shared_limiter
        self._default_token = access_token

    def get(
        self,
        path: str,
        *,
        params: dict[str, str | int] | None = None,
        access_token: str | None = None,
        group: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> EsiResponse:
        """Perform a rate-limited GET request to ESI.

        ``path`` should start with ``/`` (e.g. ``/latest/markets/10000002/orders/``).
        """
        url = self._build_url(path, params)
        token = access_token or self._default_token
        headers = self._build_headers(token)
        if extra_headers:
            headers.update(extra_headers)

        # Wait for rate-limit budget before sending.
        self._limiter.acquire(group)

        request = Request(url, headers=headers, method="GET")
        try:
            with ipv4_opener.open(request, timeout=self._timeout) as response:
                raw_body = response.read()
                # Handle gzip
                if response.headers.get("Content-Encoding") == "gzip":
                    import gzip
                    raw_body = gzip.decompress(raw_body)
                body_str = raw_body.decode("utf-8", errors="replace") if isinstance(raw_body, bytes) else raw_body
                resp_headers = {k: v for k, v in response.headers.items()} if hasattr(response.headers, 'items') else {}
                status = int(response.status)

                parsed = self._parse_json(body_str)
                esi_response = EsiResponse(status_code=status, body=parsed, headers=resp_headers)
                self._limiter.update_from_response(status, resp_headers)
                return esi_response

        except HTTPError as exc:
            exc_headers = {}
            if hasattr(exc, "headers") and exc.headers:
                exc_headers = {k: v for k, v in exc.headers.items()} if hasattr(exc.headers, 'items') else {}
            status = int(exc.code)
            self._limiter.update_from_response(status, exc_headers)

            if status == 304:
                return EsiResponse(status_code=304, body=None, headers=exc_headers)

            return EsiResponse(status_code=status, body=None, headers=exc_headers)

        except (OSError, TimeoutError) as exc:
            logger.warning("ESI request failed for %s: %s", url, exc)
            return EsiResponse(status_code=0, body=None, headers={})

    def post(
        self,
        path: str,
        *,
        body: Any = None,
        params: dict[str, str | int] | None = None,
        access_token: str | None = None,
        group: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> EsiResponse:
        """Perform a rate-limited POST request to ESI.

        Used for endpoints like ``/latest/universe/names/`` and
        ``/latest/universe/ids/`` which accept JSON arrays via POST.
        """
        url = self._build_url(path, params)
        token = access_token or self._default_token
        headers = self._build_headers(token)
        headers["Content-Type"] = "application/json"
        if extra_headers:
            headers.update(extra_headers)

        self._limiter.acquire(group)

        encoded_body = json.dumps(body).encode("utf-8") if body is not None else None
        request = Request(url, data=encoded_body, headers=headers, method="POST")
        try:
            with ipv4_opener.open(request, timeout=self._timeout) as response:
                raw_body = response.read()
                if response.headers.get("Content-Encoding") == "gzip":
                    import gzip
                    raw_body = gzip.decompress(raw_body)
                body_str = raw_body.decode("utf-8", errors="replace") if isinstance(raw_body, bytes) else raw_body
                resp_headers = {k: v for k, v in response.headers.items()} if hasattr(response.headers, 'items') else {}
                status = int(response.status)

                parsed = self._parse_json(body_str)
                esi_response = EsiResponse(status_code=status, body=parsed, headers=resp_headers)
                self._limiter.update_from_response(status, resp_headers)
                return esi_response

        except HTTPError as exc:
            exc_headers = {}
            if hasattr(exc, "headers") and exc.headers:
                exc_headers = {k: v for k, v in exc.headers.items()} if hasattr(exc.headers, 'items') else {}
            status = int(exc.code)
            self._limiter.update_from_response(status, exc_headers)
            return EsiResponse(status_code=status, body=None, headers=exc_headers)

        except (OSError, TimeoutError) as exc:
            logger.warning("ESI POST request failed for %s: %s", url, exc)
            return EsiResponse(status_code=0, body=None, headers={})

    def _build_url(self, path: str, params: dict[str, str | int] | None) -> str:
        url = f"{ESI_BASE}{path}"
        if params:
            from urllib.parse import urlencode
            qs = urlencode({k: str(v) for k, v in params.items()})
            separator = "&" if "?" in url else "?"
            url = f"{url}{separator}{qs}"
        return url

    def _build_headers(self, token: str | None) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "X-Compatibility-Date": ESI_COMPATIBILITY_DATE,
            "X-Tenant": "tranquility",
            "Accept-Language": "en",
            "User-Agent": self._user_agent,
        }
        if token:
            sanitized = token.strip()
            if sanitized.lower().startswith("bearer "):
                sanitized = sanitized[7:].strip()
            if sanitized:
                headers["Authorization"] = f"Bearer {sanitized}"
        return headers

    @staticmethod
    def _parse_json(body: str) -> Any:
        if not body.strip():
            return None
        try:
            return json.loads(body)
        except (json.JSONDecodeError, ValueError):
            return None
