-- Add index to market_order_snapshots_summary to support the compute_signals query.
-- The query filters by (source_type, observed_at) and groups by type_id; all existing
-- indexes lead with (source_type, source_id, ...) which forces a full scan over all
-- source_ids for every type_id join, causing the query to run for hours.

ALTER TABLE market_order_snapshots_summary
    ADD KEY idx_snapshot_summary_source_at_type (source_type, observed_at, type_id);

ALTER TABLE market_order_snapshots_summary_p
    ADD KEY idx_snapshot_summary_source_at_type (source_type, observed_at, type_id);
