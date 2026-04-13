-- One-time fix: unlock theaters within the lookback window so that
-- theater_clustering can re-process them.  theater_analysis previously
-- auto-locked everything >1 hour old; now it respects the clustering
-- lookback window (default 48h).

UPDATE theaters
SET locked_at = NULL
WHERE locked_at IS NOT NULL
  AND end_time >= NOW() - INTERVAL 48 HOUR;
