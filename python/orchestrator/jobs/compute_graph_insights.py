from __future__ import annotations

from typing import Any

from ..db import SupplyCoreDb
from .graph_pipeline import run_compute_graph_insights as run_compute_graph_insights_pipeline


def run_compute_graph_insights(db: SupplyCoreDb, neo4j_raw: dict[str, Any] | None = None) -> dict[str, Any]:
    result = run_compute_graph_insights_pipeline(db, neo4j_raw)
    result.setdefault("meta", {})
    result["meta"]["execution_mode"] = "python"
    return result
