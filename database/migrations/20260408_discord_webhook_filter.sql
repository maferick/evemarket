-- ---------------------------------------------------------------------------
-- Register discord_webhook_filter job and add Discord webhook setting.
--
-- Scans for interesting events (job failures, deal alerts, battles,
-- sovereignty changes) and sends curated notifications to a Discord webhook.
-- Runs every 600 seconds (10 minutes). Disabled by default.
--
-- Safe to re-run: uses ON DUPLICATE KEY and IF NOT EXISTS guards.
-- ---------------------------------------------------------------------------

-- Schedule registration
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES ('discord_webhook_filter', 0, 600, 'python')
ON DUPLICATE KEY UPDATE enabled = enabled;

-- Discord webhook URL setting (blank = disabled)
INSERT INTO app_settings (setting_key, setting_value)
VALUES ('discord_webhook_url', '')
ON DUPLICATE KEY UPDATE setting_value = setting_value;

-- Dedup tracker table
CREATE TABLE IF NOT EXISTS discord_webhook_sent (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fingerprint     CHAR(64)       NOT NULL,
    event_type      VARCHAR(60)    NOT NULL,
    event_summary   VARCHAR(500)   NOT NULL,
    sent_at         DATETIME       NOT NULL,
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_fingerprint (fingerprint),
    KEY idx_dws_event_type (event_type, sent_at),
    KEY idx_dws_sent_at (sent_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
