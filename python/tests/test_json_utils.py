from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime

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


if __name__ == "__main__":
    unittest.main()
