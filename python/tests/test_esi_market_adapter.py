from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch

from orchestrator.esi_market_adapter import EsiMarketAdapter


class _FakeHeaders(dict):
    def get(self, key: str, default: str | None = None) -> str | None:
        return str(super().get(key, default)) if key in self else default

    def items(self):
        return super().items()


class _FakeHttpResponse:
    def __init__(self, body: str = "[]", pages: int = 1, status: int = 200) -> None:
        self._body = body.encode("utf-8")
        self.headers = _FakeHeaders({"X-Pages": str(pages)})
        self.status = status

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_FakeHttpResponse":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> bool:
        return False


class EsiMarketAdapterTests(unittest.TestCase):
    def test_fetch_structure_orders_includes_datasource_query_param(self) -> None:
        captured_url: str | None = None

        def _fake_urlopen(request, timeout: int):
            nonlocal captured_url
            captured_url = request.full_url
            return _FakeHttpResponse()

        adapter = EsiMarketAdapter()
        with patch("orchestrator.esi_client.ipv4_opener") as mock_opener:
            mock_opener.open = _fake_urlopen
            adapter.fetch_structure_orders(structure_id=10203040, access_token="abc123", page=3)

        self.assertIsNotNone(captured_url)
        self.assertIn("datasource=tranquility", str(captured_url))
        self.assertIn("page=3", str(captured_url))

    def test_bearer_prefix_in_token_is_sanitized(self) -> None:
        captured_auth: str | None = None

        def _fake_urlopen(request, timeout: int):
            nonlocal captured_auth
            captured_auth = request.headers.get("Authorization")
            return _FakeHttpResponse()

        adapter = EsiMarketAdapter()
        with patch("orchestrator.esi_client.ipv4_opener") as mock_opener:
            mock_opener.open = _fake_urlopen
            adapter.fetch_structure_orders(structure_id=10203040, access_token="Bearer abc123", page=1)

        self.assertEqual(captured_auth, "Bearer abc123")


if __name__ == "__main__":
    unittest.main()
