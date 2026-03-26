# Sync Unification Matrix

This matrix tracks Python sync jobs that now run under the shared `run_sync_phase_job()` contract and return the standardized payload (`sync_result.v1`).

| Job Key | Unified Contract Metadata | Unified Result Payload | Standard Lock/Batch/Checkpoint Metadata | Website Status Surface (via scheduler tables) | Conforms |
|---|---|---|---|---|---|
| `market_hub_current_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `alliance_current_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `market_hub_historical_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `alliance_historical_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `current_state_refresh_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `analytics_bucket_1h_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `analytics_bucket_1d_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `activity_priority_summary_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `dashboard_summary_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `loss_demand_summary_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `doctrine_intelligence_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `deal_alerts_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `rebuild_ai_briefings` | ✅ | ✅ | ✅ | ✅ | ✅ |
| `forecasting_ai_sync` | ✅ | ✅ | ✅ | ✅ | ✅ |

## Notes

- Contracts are defined centrally in `python/orchestrator/jobs/sync_runtime.py` (`SYNC_JOB_CONTRACTS`).
- All sync processors routed through `processor_registry._compute_result_shape()` now preserve the standardized fields.
- Worker CLI now accepts standard operational flags (`--app-root`, `--dry-run`, `--batch-size`, `--max-batches`, `--verbose`) for future operator tooling integration.
