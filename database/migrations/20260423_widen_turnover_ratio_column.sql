-- Widen turnover_ratio_90d from DECIMAL(8,4) to DECIMAL(12,4).
-- The old ceiling of 9999.9999 is too low when a corporation has many
-- historical unique members but very few current members (e.g. 100 000 / 1).
ALTER TABLE shell_corp_indicators
    MODIFY COLUMN turnover_ratio_90d DECIMAL(12,4) NOT NULL DEFAULT 0.0000;
