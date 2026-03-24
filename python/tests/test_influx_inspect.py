from __future__ import annotations

import unittest

from orchestrator.influx_inspect import _pivot_query


class InfluxInspectTests(unittest.TestCase):
    def test_count_query_does_not_aggregate_time_column(self) -> None:
        query = _pivot_query(
            bucket="supplycore_rollups",
            measurement="market_item_price",
            tag_keys=["type_id", "source_id"],
            count_only=True,
        )

        self.assertIn('count(column: "_measurement")', query)
        self.assertNotIn('count(column: "_time")', query)


if __name__ == "__main__":
    unittest.main()
