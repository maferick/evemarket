from __future__ import annotations

import json
import unittest
from decimal import Decimal

from orchestrator.jobs.market_comparison import _evaluate_market_row


class MarketComparisonTests(unittest.TestCase):
    def test_evaluated_row_is_json_serializable(self) -> None:
        row = {
            "type_id": 34,
            "type_name": "Tritanium",
            "alliance_best_sell_price": Decimal("5.12"),
            "alliance_best_buy_price": Decimal("5.02"),
            "alliance_total_sell_volume": 1200,
            "alliance_total_buy_volume": 800,
            "alliance_sell_order_count": 12,
            "alliance_buy_order_count": 8,
            "alliance_last_observed_at": "2026-03-25 01:00:00",
            "reference_best_sell_price": Decimal("4.89"),
            "reference_best_buy_price": Decimal("4.80"),
            "reference_total_sell_volume": 2200,
            "reference_total_buy_volume": 1800,
            "reference_sell_order_count": 22,
            "reference_buy_order_count": 16,
            "reference_last_observed_at": "2026-03-25 01:00:00",
        }
        thresholds = {
            "deviation_percent": 5,
            "min_alliance_sell_volume": 1000,
            "min_alliance_sell_orders": 10,
        }

        evaluated = _evaluate_market_row(row, thresholds)
        encoded = json.dumps(evaluated)

        self.assertIsInstance(evaluated["alliance_best_sell_price"], float)
        self.assertIsInstance(evaluated["reference_best_sell_price"], float)
        self.assertIn('"type_id": 34', encoded)


if __name__ == "__main__":
    unittest.main()
