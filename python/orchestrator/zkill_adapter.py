"""zKillboard API adapter — rate limiting, retries, normalization.

All zKillboard API access must go through this adapter.  Direct HTTP calls to
zkillboard.com inside compute jobs are forbidden (AGENTS.md §External Data
Integration).

Rate limit: 1 request per second (zKB public API guideline).
"""
from __future__ import annotations

import json
import logging
import subprocess
import time
from typing import Any

from .jobs import run_killmail_r2z2_stream

logger = logging.getLogger("supplycore.zkill_adapter")

ZKB_API_BASE = "https://zkillboard.com/api"
# Contact / identity string sent to zKB via Maintainer header.
# We do NOT override curl's default User-Agent because Cloudflare
# (which fronts zKB) blocks many custom UA strings with 403.
DEFAULT_MAINTAINER = "SupplyCore/1.0 contact@supplycore.app"

# Kept for backwards compatibility with callers that reference it.
DEFAULT_USER_AGENT = DEFAULT_MAINTAINER
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
    ) -> tuple[int, str]:
        """HTTP GET via curl with rate limiting and retries.

        Uses ``curl -4`` to avoid two problems at once:
          1. IPv6 DNS resolves for zkillboard.com but the host has no IPv6
             connectivity — ``-4`` forces IPv4.
          2. Cloudflare (which fronts zKB) blocks custom User-Agent strings
             with 403.  We let curl use its default UA and pass our identity
             via the ``Maintainer`` header instead.
        """
        cmd = [
            "curl", "-4", "-s",
            "-o", "-",           # body to stdout
            "-w", "\n%{http_code}",  # status code on last line
            "--compressed",      # handle gzip/deflate transparently
            "--connect-timeout", str(min(timeout_seconds, 10)),
            "--max-time", str(timeout_seconds),
            "-H", f"Maintainer: {self._user_agent}",
            "-H", "Accept: application/json",
            url,
        ]

        for attempt in range(MAX_RETRIES + 1):
            self._rate_limit_wait()
            self._record_request()
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True,
                    timeout=timeout_seconds + 5,
                )
            except (subprocess.TimeoutExpired, OSError) as error:
                if attempt < MAX_RETRIES:
                    wait = RETRY_BACKOFF_SECONDS * (attempt + 1)
                    logger.warning("zKB request failed (%s), retrying in %.1fs", error, wait)
                    time.sleep(wait)
                    continue
                logger.warning("zKB request failed for %s after %d attempts: %s", url, MAX_RETRIES + 1, error)
                return 0, ""

            # curl writes body then \n{http_code} via -w
            raw = result.stdout
            last_nl = raw.rfind("\n")
            if last_nl == -1:
                return 0, ""
            body = raw[:last_nl]
            status_str = raw[last_nl + 1:].strip()
            status = int(status_str) if status_str.isdigit() else 0

            if status == 429 and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SECONDS * (attempt + 1)
                logger.warning("zKB 429 rate-limited, backing off %.1fs", wait)
                time.sleep(wait)
                continue

            if status == 0 and result.returncode != 0 and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF_SECONDS * (attempt + 1)
                logger.warning(
                    "curl failed (exit=%d) for %s, retrying in %.1fs",
                    result.returncode, url, wait,
                )
                time.sleep(wait)
                continue

            return status, body

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

    def fetch_character_page(
        self,
        character_id: int,
        page: int,
    ) -> list[dict[str, Any]]:
        """Fetch one page of killmails for a character.  Returns list of dicts."""
        url = f"{ZKB_API_BASE}/characterID/{character_id}/page/{page}/"
        status, body = self._fetch(url)
        if status != 200:
            if status not in (0, 429):
                logger.warning(
                    "zKB character page fetch: status=%d url=%s", status, url,
                )
            return []
        data = self._parse_json(body)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        return []

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
