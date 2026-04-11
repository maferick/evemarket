-- Widen alliance_dossiers behavioral metric columns.
--
-- The previous DECIMAL(8,4) ceiling of 9999.9999 overflowed for very active
-- alliances where kills_per_week exceeds 9999 (avg_engagement_rate) or the
-- kill/loss ratio exceeds 9999 (avg_overperformance, when total_losses is
-- very low relative to total_kills). The compute_alliance_dossiers job then
-- failed with MariaDB error 1264 ("Out of range value for column
-- 'avg_engagement_rate'") and wrote zero rows.
--
-- DECIMAL(14,4) gives a ceiling of 9,999,999,999.9999, which comfortably
-- covers any realistic kills_per_week or kill/loss ratio value we could
-- observe from killmail aggregation.
ALTER TABLE alliance_dossiers
    MODIFY COLUMN avg_engagement_rate DECIMAL(14,4) DEFAULT NULL,
    MODIFY COLUMN avg_token_participation DECIMAL(14,4) DEFAULT NULL,
    MODIFY COLUMN avg_overperformance DECIMAL(14,4) DEFAULT NULL;

-- Keep the downstream opposition_daily_snapshots column aligned with the
-- widened source so compute_opposition_daily_snapshots doesn't have to
-- clamp values at DECIMAL(8,4) precision.
ALTER TABLE opposition_daily_snapshots
    MODIFY COLUMN engagement_rate DECIMAL(14,4) DEFAULT NULL;
