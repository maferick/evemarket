from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
import unittest
from unittest.mock import patch

from orchestrator import processor_registry
from orchestrator.job_runner import PHP_BRIDGED_JOB_KEYS


class ComputeProcessorRoutingTests(unittest.TestCase):
    def test_battle_jobs_have_python_processor_bindings(self) -> None:
        self.assertIn("compute_battle_anomalies", processor_registry.PYTHON_COMPUTE_PROCESSOR_JOB_KEYS)
        self.assertIn("compute_suspicion_scores", processor_registry.PYTHON_COMPUTE_PROCESSOR_JOB_KEYS)
        self.assertIn("compute_counterintel_pipeline", processor_registry.PYTHON_COMPUTE_PROCESSOR_JOB_KEYS)

    def test_battle_jobs_are_not_php_bridged(self) -> None:
        self.assertNotIn("compute_battle_anomalies", PHP_BRIDGED_JOB_KEYS)
        self.assertNotIn("compute_suspicion_scores", PHP_BRIDGED_JOB_KEYS)
        self.assertNotIn("compute_counterintel_pipeline", PHP_BRIDGED_JOB_KEYS)

    def test_battle_anomalies_routes_through_python_processor(self) -> None:
        with (
            patch.object(processor_registry, "run_compute_battle_anomalies", return_value={"status": "success", "rows_processed": 1, "rows_written": 1, "computed_at": "2026-03-25 00:00:00"}) as anomalies,
            patch.object(processor_registry, "battle_runtime", return_value={}) as runtime,
        ):
            result = processor_registry.run_compute_processor("compute_battle_anomalies", db=object(), raw_config={})

        anomalies.assert_called_once()
        runtime.assert_called_once()
        self.assertEqual(result["status"], "success")

    def test_suspicion_scores_routes_through_python_processor(self) -> None:
        with (
            patch.object(processor_registry, "run_compute_suspicion_scores", return_value={"status": "success", "rows_processed": 2, "rows_written": 2, "computed_at": "2026-03-25 00:00:00"}) as suspicion,
            patch.object(processor_registry, "battle_runtime", return_value={}) as runtime,
        ):
            result = processor_registry.run_compute_processor("compute_suspicion_scores", db=object(), raw_config={})

        suspicion.assert_called_once()
        runtime.assert_called_once()
        self.assertEqual(result["status"], "success")

    def test_compute_result_shape_serializes_datetime_and_decimal_metadata(self) -> None:
        shaped = processor_registry._compute_result_shape(  # noqa: SLF001
            {
                "status": "success",
                "rows_processed": 1,
                "rows_written": 1,
                "computed_at": "2026-03-25 00:00:00",
                "warnings": [],
                "sample": {"ts": datetime(2026, 3, 25, tzinfo=UTC), "score": Decimal("0.1250")},
            },
            "compute_suspicion_scores_v2",
        )
        sample = shaped["meta"]["result"]["sample"]
        self.assertEqual(sample["ts"], "2026-03-25T00:00:00+00:00")
        self.assertEqual(sample["score"], "0.1250")


if __name__ == "__main__":
    unittest.main()
