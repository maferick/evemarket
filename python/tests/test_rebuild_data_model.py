from __future__ import annotations

import unittest

from orchestrator.rebuild_data_model import (
    _observe_daily_bucket,
    _observe_local_daily_bucket,
    _observe_rollup,
    _projection_rows_from_orders,
)


class RebuildDataModelTests(unittest.TestCase):
    def test_projection_rows_from_orders_aggregates_best_prices(self) -> None:
        rows = _projection_rows_from_orders([
            {
                "source_type": "market_hub",
                "source_id": 60003760,
                "type_id": 34,
                "observed_at": "2026-03-23 10:00:00",
                "price": 100.0,
                "volume_remain": 20,
                "is_buy_order": 0,
            },
            {
                "source_type": "market_hub",
                "source_id": 60003760,
                "type_id": 34,
                "observed_at": "2026-03-23 10:00:00",
                "price": 99.0,
                "volume_remain": 10,
                "is_buy_order": 0,
            },
            {
                "source_type": "market_hub",
                "source_id": 60003760,
                "type_id": 34,
                "observed_at": "2026-03-23 10:00:00",
                "price": 98.0,
                "volume_remain": 5,
                "is_buy_order": 1,
            },
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["best_sell_price"], 99.0)
        self.assertEqual(rows[0]["best_buy_price"], 98.0)
        self.assertEqual(rows[0]["total_volume"], 35)

    def test_daily_bucket_tracks_close_and_average(self) -> None:
        buckets: dict[str, dict[str, object]] = {}
        _observe_daily_bucket(buckets, {
            "source_type": "market_hub",
            "source_id": 1,
            "type_id": 34,
            "observed_at": "2026-03-23 01:00:00",
            "best_sell_price": 100.0,
            "best_buy_price": 99.0,
            "total_volume": 10,
            "buy_order_count": 1,
            "sell_order_count": 2,
        }, "UTC", "market_hub")
        _observe_daily_bucket(buckets, {
            "source_type": "market_hub",
            "source_id": 1,
            "type_id": 34,
            "observed_at": "2026-03-23 03:00:00",
            "best_sell_price": 105.0,
            "best_buy_price": 104.0,
            "total_volume": 11,
            "buy_order_count": 2,
            "sell_order_count": 3,
        }, "UTC", "market_hub")
        bucket = next(iter(buckets.values()))
        self.assertEqual(bucket["close_price"], 105.0)
        self.assertEqual(bucket["high_price"], 105.0)
        self.assertEqual(bucket["average_price"], 102.5)

    def test_local_daily_bucket_tracks_latest_snapshot(self) -> None:
        buckets: dict[str, dict[str, object]] = {}
        _observe_local_daily_bucket(buckets, {
            "source_type": "market_hub",
            "source_id": 1,
            "type_id": 34,
            "observed_at": "2026-03-23 01:00:00",
            "best_sell_price": 100.0,
            "best_buy_price": 99.0,
            "total_volume": 10,
            "buy_order_count": 1,
            "sell_order_count": 2,
        }, "UTC")
        _observe_local_daily_bucket(buckets, {
            "source_type": "market_hub",
            "source_id": 1,
            "type_id": 34,
            "observed_at": "2026-03-23 02:00:00",
            "best_sell_price": 101.0,
            "best_buy_price": 98.0,
            "total_volume": 15,
            "buy_order_count": 2,
            "sell_order_count": 4,
        }, "UTC")
        bucket = next(iter(buckets.values()))
        self.assertEqual(bucket["close_price"], 101.0)
        self.assertEqual(bucket["buy_order_count"], 2)
        self.assertEqual(bucket["sell_order_count"], 4)

    def test_rollup_bucket_counts_samples(self) -> None:
        buckets: dict[str, dict[str, object]] = {}
        _observe_rollup(buckets, {
            "source_type": "market_hub",
            "source_id": 1,
            "type_id": 34,
            "observed_at": "2026-03-23 01:15:00",
            "best_sell_price": 100.0,
            "best_buy_price": 99.0,
            "total_buy_volume": 5,
            "total_sell_volume": 6,
            "total_volume": 11,
            "buy_order_count": 2,
            "sell_order_count": 3,
        }, resolution="1h")
        _observe_rollup(buckets, {
            "source_type": "market_hub",
            "source_id": 1,
            "type_id": 34,
            "observed_at": "2026-03-23 01:45:00",
            "best_sell_price": 101.0,
            "best_buy_price": 98.0,
            "total_buy_volume": 7,
            "total_sell_volume": 8,
            "total_volume": 15,
            "buy_order_count": 4,
            "sell_order_count": 5,
        }, resolution="1h")
        bucket = next(iter(buckets.values()))
        self.assertEqual(bucket["sample_count"], 2)
        self.assertEqual(bucket["best_sell_price_last"], 101.0)
        self.assertEqual(bucket["total_volume_sum"], 26)


if __name__ == "__main__":
    unittest.main()
