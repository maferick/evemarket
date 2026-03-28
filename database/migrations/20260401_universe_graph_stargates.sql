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

-- Theater enrichment columns for graph-derived metrics
ALTER TABLE theaters
    ADD COLUMN max_gate_span SMALLINT UNSIGNED DEFAULT NULL AFTER anomaly_score,
    ADD COLUMN avg_gate_distance DECIMAL(6,2) DEFAULT NULL AFTER max_gate_span,
    ADD COLUMN clustering_method VARCHAR(32) DEFAULT 'constellation' AFTER avg_gate_distance;

-- Tuning parameters (changeable without code deploys)
INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('universe_graph_max_gate_distance', '5'),
    ('universe_graph_gate_merge_min_overlap', '0.05'),
    ('universe_graph_ignore_highsec_adjacency', '0'),
    ('universe_graph_highsec_threshold', '0.45')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);

-- Schedule entries for new jobs
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode, workload_class)
VALUES
    ('graph_universe_sync', 1, 86400, 'python', 'compute'),
    ('compute_graph_sync_killmail_entities', 1, 300, 'python', 'compute')
ON DUPLICATE KEY UPDATE enabled = enabled;
