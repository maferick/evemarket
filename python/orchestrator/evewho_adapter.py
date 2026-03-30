"""EveWho API adapter — rate limiting, retries, normalization.

All EveWho API access must go through this adapter.  Direct HTTP calls to
evewho.com inside compute jobs are forbidden (AGENTS.md §External Data
Integration).

Rate limit: 10 requests per 30 seconds.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from typing import Any

from .http_client import ipv4_opener

EVEWHO_BASE_URL = "https://evewho.com"
DEFAULT_USER_AGENT = "SupplyCore Intelligence Platform / contact@supplycore.app"
RATE_LIMIT_REQUESTS = 10
RATE_LIMIT_WINDOW_SECONDS = 30
MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = 2.0


class EveWhoAdapter:
    """Adapter for the EveWho public API with built-in rate limiting and retries."""

    def __init__(self, user_agent: str = DEFAULT_USER_AGENT):
        self._user_agent = user_agent
        self._request_timestamps: list[float] = []

    def _rate_limit_wait(self) -> None:
        """Block until a request slot is available within the rate limit window."""
        now = time.monotonic()
        cutoff = now - RATE_LIMIT_WINDOW_SECONDS
        self._request_timestamps = [t for t in self._request_timestamps if t > cutoff]
        if len(self._request_timestamps) >= RATE_LIMIT_REQUESTS:
            oldest = self._request_timestamps[0]
            sleep_for = (oldest + RATE_LIMIT_WINDOW_SECONDS) - now + 0.1
            if sleep_for > 0:
                time.sleep(sleep_for)

    def _record_request(self) -> None:
        self._request_timestamps.append(time.monotonic())

    def _fetch(self, endpoint_path: str, timeout_seconds: int = 20) -> dict[str, Any] | None:
        """Fetch a JSON endpoint with rate limiting and retries."""
        url = f"{EVEWHO_BASE_URL}{endpoint_path}"
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": self._user_agent,
            },
        )

        for attempt in range(MAX_RETRIES + 1):
            self._rate_limit_wait()
            self._record_request()
            try:
                with ipv4_opener.open(request, timeout=timeout_seconds) as response:
                    status = int(getattr(response, "status", response.getcode()))
                    if status != 200:
                        return None
                    body = response.read().decode("utf-8", errors="replace")
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
                    continue
                return None

            try:
                parsed = json.loads(body)
            except json.JSONDecodeError:
                return None
            return parsed if isinstance(parsed, dict) else None

        return None

    def fetch_character(self, character_id: int) -> tuple[str, dict[str, Any] | None]:
        """Fetch full character info + corp history."""
        endpoint = f"/api/character/{character_id}"
        return endpoint, self._fetch(endpoint)

    def fetch_corplist(self, corp_id: int) -> tuple[str, dict[str, Any] | None]:
        """Fetch current member list for a corporation."""
        endpoint = f"/api/corplist/{corp_id}"
        return endpoint, self._fetch(endpoint)

    def fetch_allilist(self, alliance_id: int) -> tuple[str, dict[str, Any] | None]:
        """Fetch all characters in an alliance."""
        endpoint = f"/api/allilist/{alliance_id}"
        return endpoint, self._fetch(endpoint)

    def fetch_corpdeparted(self, corp_id: int) -> tuple[str, dict[str, Any] | None]:
        """Fetch recent leavers for a corporation."""
        endpoint = f"/api/corpdeparted/{corp_id}"
        return endpoint, self._fetch(endpoint)

    def fetch_corpjoined(self, corp_id: int) -> tuple[str, dict[str, Any] | None]:
        """Fetch recent joiners for a corporation."""
        endpoint = f"/api/corpjoined/{corp_id}"
        return endpoint, self._fetch(endpoint)
