"""zKillboard API adapter — rate limiting, retries, normalization.

All zKillboard API access must go through this adapter.  Direct HTTP calls to
zkillboard.com inside compute jobs are forbidden (AGENTS.md §External Data
Integration).

Rate limit: 1 request per second (zKB public API guideline).
"""
from __future__ import annotations

import gzip
import json
import logging
import time
import urllib.error
import urllib.request
from typing import Any

from .http_client import ipv4_opener
from .jobs import run_killmail_r2z2_stream

logger = logging.getLogger("supplycore.zkill_adapter")

ZKB_API_BASE = "https://zkillboard.com/api"
DEFAULT_USER_AGENT = "SupplyCore Intelligence Platform / contact@supplycore.app"
RATE_LIMIT_DELAY = 1.0  # seconds between requests (zKB guideline)
MAX_RETRIES = 2
RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_TIMEOUT = 30


class ZKillAdapter:
    """Adapter for the zKillboard public API with rate limiting and retries."""

    def __init__(
        self,
        user_agent: str = DEFAULT_USER_AGENT,
        rate_limit_delay: float = RATE_LIMIT_DELAY,
    ):
        self._user_agent = user_agent
        self._rate_limit_delay = max(0.5, rate_limit_delay)
        self._last_request_at: float = 0.0

    def _rate_limit_wait(self) -> None:
        """Block until the minimum delay between requests has elapsed."""
        now = time.monotonic()
        elapsed = now - self._last_request_at
        if elapsed < self._rate_limit_delay:
            time.sleep(self._rate_limit_delay - elapsed)

    def _record_request(self) -> None:
        self._last_request_at = time.monotonic()

    def _fetch(
        self,
        url: str,
        timeout_seconds: int = DEFAULT_TIMEOUT,
        accept_gzip: bool = True,
    ) -> tuple[int, str]:
        """HTTP GET with rate limiting and retries.  Returns (status, body)."""
        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": self._user_agent,
        }
        if accept_gzip:
            headers["Accept-Encoding"] = "gzip"

        request = urllib.request.Request(url, headers=headers)

        for attempt in range(MAX_RETRIES + 1):
            self._rate_limit_wait()
            self._record_request()
            try:
                with ipv4_opener.open(request, timeout=timeout_seconds) as response:
                    status = int(getattr(response, "status", response.getcode()))
                    body = response.read()
                    if response.headers.get("Content-Encoding") == "gzip":
                        body = gzip.decompress(body)
                    if isinstance(body, bytes):
                        body = body.decode("utf-8", errors="replace")
                    return status, body
            except urllib.error.HTTPError as error:
                status = int(error.code)
                if status == 429 and attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_SECONDS * (attempt + 1)
                    logger.warning("zKB 429 rate-limited, backing off %.1fs", wait)
                    time.sleep(wait)
                    continue
                return status, ""
            except (urllib.error.URLError, OSError, TimeoutError) as error:
                if attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_SECONDS * (attempt + 1)
                    logger.warning("zKB request failed (%s), retrying in %.1fs", error, wait)
                    time.sleep(wait)
                    continue
                logger.warning("zKB request failed for %s after %d attempts: %s", url, MAX_RETRIES + 1, error)
                return 0, ""

        return 0, ""

    def _parse_json(self, body: str) -> Any:
        if not body.strip():
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    # ── Public API methods ───────────────────────────────────────────────

    def fetch_kill_metadata(self, killmail_id: int) -> dict[str, Any]:
        """Fetch zkb metadata blob for a single killmail.  Returns {} on failure."""
        url = f"{ZKB_API_BASE}/killID/{killmail_id}/"
        status, body = self._fetch(url)
        if status != 200:
            return {}
        data = self._parse_json(body)
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            return data[0].get("zkb") or {}
        return {}

    def fetch_entity_page(
        self,
        entity_type: str,
        entity_id: int,
        year: int,
        month: int,
        page: int,
        endpoint: str = "losses",
    ) -> list[dict[str, Any]]:
        """Fetch one page of losses/kills for an entity+month.  Returns list of dicts."""
        url = f"{ZKB_API_BASE}/{endpoint}/{entity_type}/{entity_id}/year/{year}/month/{month}/page/{page}/"
        status, body = self._fetch(url)
        logger.info("zKB response: status=%d body_len=%d url=%s", status, len(body), url)
        if status in (404, 429) or status != 200:
            return []
        data = self._parse_json(body)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []


# ── Legacy wrapper kept for backward compatibility with R2Z2 stream ──


class ZKillR2Z2Adapter:
    """Stable integration boundary for external zKill ingestion."""

    adapter_key = "zkill_r2z2_adapter"
    job_key = "killmail_r2z2_sync"

    def run_stream_once(self, context: Any) -> dict[str, Any]:
        return run_killmail_r2z2_stream(context)
