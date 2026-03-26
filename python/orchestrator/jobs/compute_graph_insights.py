from __future__ import annotations

from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from .graph_pipeline import run_compute_graph_insights as run_compute_graph_insights_pipeline


def run_compute_graph_insights(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = run_compute_graph_insights_pipeline(db, neo4j_raw)
    raw.setdefault("meta", {})
    raw["meta"]["execution_mode"] = "python"
    return JobResult.from_raw(raw, job_key="compute_graph_insights").to_dict()
