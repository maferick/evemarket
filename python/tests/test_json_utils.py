from __future__ import annotations

import json
import unittest
from datetime import UTC, date, datetime
from decimal import Decimal

from orchestrator.json_utils import json_dumps_safe


class JsonUtilsTests(unittest.TestCase):
    def test_json_dumps_safe_serializes_datetime_payloads(self) -> None:
        now = datetime(2026, 3, 25, 16, 28, 12, tzinfo=UTC)
        payload = {
            "ts": now,
            "nested": {
                "items": [now],
            },
        }

        decoded = json.loads(json_dumps_safe(payload))

        self.assertEqual(decoded["ts"], now.isoformat())
        self.assertEqual(decoded["nested"]["items"][0], now.isoformat())

    def test_json_dumps_safe_serializes_date_decimal_and_nested_values(self) -> None:
        payload = {
            "as_of": date(2026, 3, 25),
            "price": Decimal("1234.5600"),
            "nested": [
                {"when": datetime(2026, 3, 25, 17, 0, 0, tzinfo=UTC)},
                {"value": Decimal("0.42")},
            ],
        }

        decoded = json.loads(json_dumps_safe(payload))

        self.assertEqual(decoded["as_of"], "2026-03-25")
        self.assertEqual(decoded["price"], "1234.5600")
        self.assertEqual(decoded["nested"][0]["when"], "2026-03-25T17:00:00+00:00")
        self.assertEqual(decoded["nested"][1]["value"], "0.42")


if __name__ == "__main__":
    unittest.main()
