from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from orchestrator.jobs import killmail


class FakeBridge:
    def __init__(self, php_binary: str, app_root: Path):
        self.php_binary = php_binary
        self.app_root = app_root
        self.calls: list[tuple[str, dict | None]] = []

    def call(self, action: str, payload: dict | None = None) -> dict:
        self.calls.append((action, payload))
        if action == "killmail-context":
            return {
                "context": {
                    "enabled": True,
                    "sequence_url": "https://example.test/sequence.json",
                    "base_url": "https://example.test/killmails",
                    "user_agent": "test-agent",
                    "poll_sleep_seconds": 10,
                    "max_sequences_per_run": 3,
                    "cursor": "100",
                    "dataset_key": "killmail",
                    "job_key": "killmail_r2z2_sync",
                }
            }
        if action == "sync-run-start":
            return {"result": {"run_id": 1}}
        if action == "process-killmail-batch":
            payloads = list((payload or {}).get("payloads") or [])
            sequence_id = int(payloads[-1].get("sequence_id") or 0) if payloads else 0
            requested_sequence_id = int(payloads[-1].get("requested_sequence_id") or sequence_id) if payloads else 0
            result = self._batch_result_for_payload(payloads[-1] if payloads else {}, requested_sequence_id)
            result["last_processed_sequence"] = requested_sequence_id
            result["meta"] = {
                **dict(result.get("meta") or {}),
                "checkpoint_state": {
                    "checkpoint_updated": True,
                    "cursor": str(requested_sequence_id),
                    "reason": "updated",
                },
                "first_sequence_seen": requested_sequence_id,
                "last_sequence_seen": requested_sequence_id,
            }
            return {"result": result}
        if action == "sync-cursor-upsert":
            cursor = str((payload or {}).get("cursor") or "")
            return {
                "result": {
                    "checkpoint_updated": True,
                    "cursor": cursor,
                    "reason": "updated",
                }
            }
        if action == "sync-run-finish":
            return {"result": {"ok": True}}
        raise AssertionError(f"Unexpected bridge action: {action}")

    @staticmethod
    def _batch_result_for_payload(item: dict, requested_sequence_id: int) -> dict:
        classification = item.get("test_result")
        base = {
            "rows_seen": 1,
            "rows_matched": 0,
            "rows_skipped_existing": 0,
            "rows_filtered_out": 0,
            "rows_write_attempted": 0,
            "rows_written": 0,
            "rows_failed": 0,
            "duplicates": 0,
            "filtered": 0,
            "invalid": 0,
            "reason_for_zero_write": "unknown",
        }
        if classification == "inserted":
            return {
                **base,
                "rows_matched": 1,
                "rows_write_attempted": 1,
                "rows_written": 1,
                "reason_for_zero_write": "",
            }
        if classification == "existing":
            return {
                **base,
                "rows_matched": 1,
                "rows_skipped_existing": 1,
                "duplicates": 1,
                "reason_for_zero_write": "already_existed_locally",
            }
        if classification == "filtered":
            return {
                **base,
                "rows_filtered_out": 1,
                "filtered": 1,
                "reason_for_zero_write": "no_tracked_entity_matches",
            }
        if classification == "empty":
            return {
                **base,
                "invalid": 1,
                "reason_for_zero_write": "unknown",
            }
        return {
            **base,
            "invalid": 1,
            "reason_for_zero_write": "unknown",
        }


class FakeContext:
    def __init__(self) -> None:
        self.php_binary = "php"
        self.app_root = Path("/tmp/supplycore")
        self.timeout_seconds = 60
        self.batch_size = 1000
        self.job_key = "killmail_r2z2_sync"
        self.emitted: list[tuple[str, dict]] = []

    def emit(self, event: str, payload: dict) -> None:
        self.emitted.append((event, payload))


class KillmailStreamTests(unittest.TestCase):
    def setUp(self) -> None:
        self.context = FakeContext()

    def test_http_200_sequences_do_not_raise_and_checkpoint_advances(self) -> None:
        responses = [
            (200, {"esi": {"killmail_id": 1}, "test_result": "filtered"}),
            (200, {"esi": {"killmail_id": 2}, "test_result": "existing"}),
            (200, {"esi": {"killmail_id": 3}, "test_result": "inserted"}),
        ]

        def fake_http_json(url: str, user_agent: str, timeout_seconds: int = 25):
            self.assertEqual(user_agent, "test-agent")
            if url.endswith("/101.json"):
                return responses[0]
            if url.endswith("/102.json"):
                return responses[1]
            if url.endswith("/103.json"):
                return responses[2]
            raise AssertionError(f"Unexpected URL: {url}")

        with patch.object(killmail, "PhpBridge", FakeBridge), patch.object(killmail, "_http_json", side_effect=fake_http_json):
            result = killmail.run_killmail_r2z2_stream(self.context)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["rows_seen"], 3)
        self.assertEqual(result["rows_written"], 1)
        self.assertEqual(result["cursor"], "103")
        self.assertEqual(result["meta"]["rows_filtered_out"], 1)
        self.assertEqual(result["meta"]["rows_skipped_existing"], 1)
        self.assertEqual(result["meta"]["rows_matched"], 2)
        self.assertEqual(result["meta"]["checkpoint_updates"], 3)
        sequence_processed = [payload for event, payload in self.context.emitted if event == "zkill.sequence_processed"]
        self.assertEqual(len(sequence_processed), 3)
        self.assertEqual([item["processing_outcome"] for item in sequence_processed], ["filtered_out", "already_existing", "inserted"])

    def test_empty_http_200_payload_is_valid_non_actionable(self) -> None:
        def fake_http_json(url: str, user_agent: str, timeout_seconds: int = 25):
            if url.endswith("/101.json"):
                return 200, {}
            if url.endswith("/102.json"):
                return 404, {}
            raise AssertionError(f"Unexpected URL: {url}")

        with (
            patch.object(killmail, "PhpBridge", FakeBridge),
            patch.object(killmail, "_http_json", side_effect=fake_http_json),
            patch.object(killmail, "_sleep_with_budget", return_value=False),
        ):
            result = killmail.run_killmail_r2z2_stream(self.context)

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["rows_seen"], 1)
        self.assertEqual(result["rows_written"], 0)
        self.assertEqual(result["cursor"], "101")
        sequence_fetch = [payload for event, payload in self.context.emitted if event == "zkill.sequence_fetch"]
        self.assertEqual(sequence_fetch[0]["payload_classification"], "valid_empty_payload")
        self.assertTrue(sequence_fetch[0]["valid_non_actionable"])
        sequence_processed = [payload for event, payload in self.context.emitted if event == "zkill.sequence_processed"]
        self.assertEqual(sequence_processed[0]["processing_outcome"], "invalid_payload")
        self.assertTrue(sequence_processed[0]["valid_non_actionable"])

    def test_http_200_non_object_payload_raises_precise_error(self) -> None:
        def fake_http_json(url: str, user_agent: str, timeout_seconds: int = 25):
            if url.endswith("/101.json"):
                return 200, []
            raise AssertionError(f"Unexpected URL: {url}")

        with patch.object(killmail, "PhpBridge", FakeBridge), patch.object(killmail, "_http_json", side_effect=fake_http_json):
            with self.assertRaisesRegex(RuntimeError, "HTTP 200 with malformed non-object JSON payload"):
                killmail.run_killmail_r2z2_stream(self.context)


if __name__ == "__main__":
    unittest.main()
