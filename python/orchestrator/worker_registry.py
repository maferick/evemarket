from __future__ import annotations

from typing import Any

# Active Python-native recurring jobs.
WORKER_JOB_DEFINITIONS: dict[str, dict[str, Any]] = {
    "compute_graph_sync": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 300, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_graph_sync_doctrine_dependency": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_graph_sync_battle_intelligence": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_graph_derived_relationships": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_graph_insights": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 300, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_graph_prune": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_graph_topology_metrics": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_behavioral_baselines": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 90, "max_attempts": 4},
    "compute_suspicion_scores_v2": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 90, "max_attempts": 4},
    "compute_buy_all": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_signals": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 900, "timeout_seconds": 300, "memory_limit_mb": 768, "retry_delay_seconds": 60, "max_attempts": 4},
    "compute_battle_rollups": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 600, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 90, "max_attempts": 4},
    "compute_battle_target_metrics": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 600, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 90, "max_attempts": 4},
    "compute_battle_anomalies": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 600, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 90, "max_attempts": 4},
    "compute_battle_actor_features": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 600, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 90, "max_attempts": 4},
    "compute_suspicion_scores": {"workload_class": "compute", "execution_mode": "python", "queue_name": "compute", "priority": "normal", "interval_seconds": 600, "timeout_seconds": 420, "memory_limit_mb": 1024, "retry_delay_seconds": 90, "max_attempts": 4},
}

# Explicitly retired from recurring execution until a Python-native processor exists.
DISABLED_WORKER_JOBS: dict[str, str] = {
    "market_hub_current_sync": "No Python-native processor in worker path.",
    "deal_alerts_sync": "No Python-native processor in worker path.",
    "alliance_current_sync": "No Python-native processor in worker path.",
    "market_comparison_summary_sync": "Current implementation requires PHP bridge context.",
    "dashboard_summary_sync": "No Python-native processor in worker path.",
    "doctrine_intelligence_sync": "No Python-native processor in worker path.",
    "loss_demand_summary_sync": "No Python-native processor in worker path.",
    "activity_priority_summary_sync": "No Python-native processor in worker path.",
    "current_state_refresh_sync": "No Python-native processor in worker path.",
    "market_hub_local_history_sync": "Current implementation requires PHP bridge context.",
    "market_hub_historical_sync": "No Python-native processor in worker path.",
    "alliance_historical_sync": "No Python-native processor in worker path.",
    "analytics_bucket_1h_sync": "No Python-native processor in worker path.",
    "analytics_bucket_1d_sync": "No Python-native processor in worker path.",
    "rebuild_ai_briefings": "No Python-native processor in worker path.",
    "forecasting_ai_sync": "No Python-native processor in worker path.",
    "killmail_r2z2_sync": "Current implementation requires PHP bridge batch handling.",
}
