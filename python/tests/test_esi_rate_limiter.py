from __future__ import annotations

import time
import unittest

from orchestrator.esi_rate_limiter import (
    EsiRateLimiter,
    _parse_window,
    token_cost_for_status,
)


class TokenCostTests(unittest.TestCase):
    def test_2xx_costs_2(self) -> None:
        self.assertEqual(token_cost_for_status(200), 2)
        self.assertEqual(token_cost_for_status(201), 2)

    def test_3xx_costs_1(self) -> None:
        self.assertEqual(token_cost_for_status(304), 1)

    def test_4xx_costs_5(self) -> None:
        self.assertEqual(token_cost_for_status(400), 5)
        self.assertEqual(token_cost_for_status(403), 5)
        self.assertEqual(token_cost_for_status(404), 5)

    def test_429_costs_0(self) -> None:
        self.assertEqual(token_cost_for_status(429), 0)

    def test_5xx_costs_0(self) -> None:
        self.assertEqual(token_cost_for_status(500), 0)
        self.assertEqual(token_cost_for_status(503), 0)


class ParseWindowTests(unittest.TestCase):
    def test_minutes(self) -> None:
        tokens, seconds = _parse_window("150/15m")
        self.assertEqual(tokens, 150)
        self.assertAlmostEqual(seconds, 900.0)

    def test_hours(self) -> None:
        tokens, seconds = _parse_window("300/1h")
        self.assertEqual(tokens, 300)
        self.assertAlmostEqual(seconds, 3600.0)

    def test_invalid_returns_defaults(self) -> None:
        tokens, seconds = _parse_window("garbage")
        self.assertEqual(tokens, 150)
        self.assertAlmostEqual(seconds, 900.0)


class RateLimiterTests(unittest.TestCase):
    def test_acquire_returns_immediately_when_no_headers_seen(self) -> None:
        limiter = EsiRateLimiter()
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        self.assertLess(elapsed, 0.1)

    def test_update_from_response_tracks_group(self) -> None:
        limiter = EsiRateLimiter()
        limiter.update_from_response(200, {
            "X-Ratelimit-Group": "market",
            "X-Ratelimit-Limit": "150/15m",
            "X-Ratelimit-Remaining": "148",
            "X-Ratelimit-Used": "2",
        })
        stats = limiter.stats
        self.assertIn("market", stats)
        self.assertEqual(stats["market"]["max_tokens"], 150)
        self.assertEqual(stats["market"]["remaining"], 148)
        self.assertTrue(stats["market"]["initialized"])

    def test_acquire_blocks_when_budget_exhausted(self) -> None:
        limiter = EsiRateLimiter(safety_margin=0.0)
        # Use a small window so regeneration wait is short (2 tokens / (100/60s) ≈ 1.2s).
        limiter.update_from_response(200, {
            "X-Ratelimit-Group": "test",
            "X-Ratelimit-Limit": "100/1m",
            "X-Ratelimit-Remaining": "0",
        })
        start = time.monotonic()
        limiter.acquire("test")
        elapsed = time.monotonic() - start
        # Should have waited for token regeneration (~1.2s for 2 tokens).
        self.assertGreater(elapsed, 0.5)
        self.assertLess(elapsed, 5.0)

    def test_retry_after_sets_global_deadline(self) -> None:
        limiter = EsiRateLimiter()
        # 429 with Retry-After but remaining still has budget (429 costs 0 tokens).
        limiter.update_from_response(429, {
            "X-Ratelimit-Group": "market",
            "X-Ratelimit-Limit": "150/15m",
            "X-Ratelimit-Remaining": "100",
            "Retry-After": "1",
        })
        start = time.monotonic()
        limiter.acquire()
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.9)
        self.assertLess(elapsed, 3.0)

    def test_stats_returns_all_groups(self) -> None:
        limiter = EsiRateLimiter()
        limiter.update_from_response(200, {
            "X-Ratelimit-Group": "alpha",
            "X-Ratelimit-Limit": "100/10m",
            "X-Ratelimit-Remaining": "98",
        })
        limiter.update_from_response(200, {
            "X-Ratelimit-Group": "beta",
            "X-Ratelimit-Limit": "50/5m",
            "X-Ratelimit-Remaining": "48",
        })
        stats = limiter.stats
        self.assertEqual(len(stats), 2)
        self.assertIn("alpha", stats)
        self.assertIn("beta", stats)

    def test_none_headers_handled_gracefully(self) -> None:
        limiter = EsiRateLimiter()
        limiter.update_from_response(200, None)
        self.assertEqual(limiter.stats, {})


if __name__ == "__main__":
    unittest.main()
