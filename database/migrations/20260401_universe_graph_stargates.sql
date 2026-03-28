-- ── Universe Graph: Stargate topology + theater enrichment ──────────────────

-- Stargate connections from EVE SDE
CREATE TABLE IF NOT EXISTS ref_stargates (
    stargate_id      INT UNSIGNED PRIMARY KEY,
    system_id        INT UNSIGNED NOT NULL,
    dest_stargate_id INT UNSIGNED NOT NULL,
    dest_system_id   INT UNSIGNED NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_ref_stargates_system (system_id),
    KEY idx_ref_stargates_dest_system (dest_system_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Theater enrichment columns for graph-derived metrics (idempotent)
SET @_col_exists := (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'theaters' AND COLUMN_NAME = 'max_gate_span'
);
SET @_sql := IF(@_col_exists = 0,
    'ALTER TABLE theaters ADD COLUMN max_gate_span SMALLINT UNSIGNED DEFAULT NULL AFTER anomaly_score',
    'SELECT 1');
PREPARE _stmt FROM @_sql;
EXECUTE _stmt;
DEALLOCATE PREPARE _stmt;

SET @_col_exists := (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'theaters' AND COLUMN_NAME = 'avg_gate_distance'
);
SET @_sql := IF(@_col_exists = 0,
    'ALTER TABLE theaters ADD COLUMN avg_gate_distance DECIMAL(6,2) DEFAULT NULL AFTER max_gate_span',
    'SELECT 1');
PREPARE _stmt FROM @_sql;
EXECUTE _stmt;
DEALLOCATE PREPARE _stmt;

SET @_col_exists := (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'theaters' AND COLUMN_NAME = 'clustering_method'
);
SET @_sql := IF(@_col_exists = 0,
    'ALTER TABLE theaters ADD COLUMN clustering_method VARCHAR(32) DEFAULT ''constellation'' AFTER avg_gate_distance',
    'SELECT 1');
PREPARE _stmt FROM @_sql;
EXECUTE _stmt;
DEALLOCATE PREPARE _stmt;

-- Tuning parameters (changeable without code deploys)
INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('universe_graph_max_gate_distance', '5'),
    ('universe_graph_gate_merge_min_overlap', '0.05'),
    ('universe_graph_ignore_highsec_adjacency', '0'),
    ('universe_graph_highsec_threshold', '0.45')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);

-- Schedule entries for new jobs
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES
    ('graph_universe_sync', 1, 86400, 'python'),
    ('compute_graph_sync_killmail_entities', 1, 300, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;
