-- Widen engagement_rate column to match source (alliance_dossiers.avg_engagement_rate DECIMAL(8,4)).
-- DECIMAL(5,2) overflows when kills_per_week exceeds 999.99 for active alliances.
ALTER TABLE opposition_daily_snapshots
    MODIFY COLUMN engagement_rate DECIMAL(8,4) DEFAULT NULL;
