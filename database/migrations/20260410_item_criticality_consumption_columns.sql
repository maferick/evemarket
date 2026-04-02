-- Add consumption tracking columns to item_criticality_index for burn-rate
-- driven buy-all quantity predictions.

ALTER TABLE item_criticality_index
    ADD COLUMN consumption_30d       DECIMAL(14,2)  DEFAULT NULL AFTER stock_days_remaining,
    ADD COLUMN avg_daily_consumption DECIMAL(12,4)  DEFAULT NULL AFTER consumption_30d;
