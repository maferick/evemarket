CREATE TABLE IF NOT EXISTS character_feature_histograms (
    character_id                     BIGINT UNSIGNED NOT NULL,
    window_label                     ENUM('7d','30d','90d','lifetime') NOT NULL,
    hour_histogram                   JSON NOT NULL,
    weekday_histogram                JSON NOT NULL,
    computed_at                      DATETIME NOT NULL,
    PRIMARY KEY (character_id, window_label),
    KEY idx_cfh_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
