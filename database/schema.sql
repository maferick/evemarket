CREATE TABLE IF NOT EXISTS app_settings (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    setting_key VARCHAR(120) NOT NULL UNIQUE,
    setting_value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS trading_stations (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    station_name VARCHAR(190) NOT NULL,
    station_type ENUM('market', 'alliance') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_station_name_type (station_name, station_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alliance_structure_metadata (
    structure_id BIGINT UNSIGNED PRIMARY KEY,
    structure_name VARCHAR(255) DEFAULT NULL,
    last_verified_at DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_last_verified_at (last_verified_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS esi_oauth_tokens (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    character_id BIGINT UNSIGNED NOT NULL,
    character_name VARCHAR(120) NOT NULL,
    owner_hash VARCHAR(120) NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    token_type VARCHAR(20) NOT NULL,
    scopes TEXT NOT NULL,
    expires_at DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_character_id (character_id),
    UNIQUE KEY unique_owner_hash (owner_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS esi_cache_namespaces (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    namespace_key VARCHAR(190) NOT NULL,
    source_system VARCHAR(40) NOT NULL DEFAULT 'esi',
    description VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_namespace_key (namespace_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS esi_cache_entries (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    namespace_key VARCHAR(190) NOT NULL,
    cache_key VARCHAR(190) NOT NULL,
    payload_json LONGTEXT NOT NULL,
    etag VARCHAR(190) DEFAULT NULL,
    fetched_at DATETIME NOT NULL,
    expires_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_namespace_cache_key (namespace_key, cache_key),
    KEY idx_namespace_expires (namespace_key, expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sync_state (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dataset_key VARCHAR(190) NOT NULL,
    sync_mode ENUM('full', 'incremental') NOT NULL DEFAULT 'incremental',
    status ENUM('idle', 'running', 'success', 'failed') NOT NULL DEFAULT 'idle',
    last_success_at DATETIME DEFAULT NULL,
    last_cursor VARCHAR(190) DEFAULT NULL,
    last_row_count INT UNSIGNED NOT NULL DEFAULT 0,
    last_checksum CHAR(64) DEFAULT NULL,
    last_error_message VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_dataset_key (dataset_key),
    KEY idx_status (status),
    KEY idx_last_success_at (last_success_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sync_runs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    dataset_key VARCHAR(190) NOT NULL,
    run_mode ENUM('full', 'incremental') NOT NULL DEFAULT 'incremental',
    run_status ENUM('running', 'success', 'failed') NOT NULL DEFAULT 'running',
    started_at DATETIME NOT NULL,
    finished_at DATETIME DEFAULT NULL,
    source_rows INT UNSIGNED NOT NULL DEFAULT 0,
    written_rows INT UNSIGNED NOT NULL DEFAULT 0,
    cursor_start VARCHAR(190) DEFAULT NULL,
    cursor_end VARCHAR(190) DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_dataset_started (dataset_key, started_at),
    KEY idx_run_status (run_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sync_schedules (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    job_key VARCHAR(190) NOT NULL,
    enabled TINYINT(1) NOT NULL DEFAULT 1,
    interval_seconds INT UNSIGNED NOT NULL,
    offset_seconds INT UNSIGNED NOT NULL DEFAULT 0,
    next_run_at DATETIME DEFAULT NULL,
    last_run_at DATETIME DEFAULT NULL,
    last_status VARCHAR(40) DEFAULT NULL,
    last_error VARCHAR(500) DEFAULT NULL,
    locked_until DATETIME DEFAULT NULL,
    latest_allowed_start_at DATETIME DEFAULT NULL,
    resource_class VARCHAR(20) NOT NULL DEFAULT 'medium',
    resource_class_confidence DECIMAL(6,4) DEFAULT NULL,
    telemetry_sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    learning_mode TINYINT(1) NOT NULL DEFAULT 1,
    allow_parallel TINYINT(1) NOT NULL DEFAULT 1,
    prefers_solo TINYINT(1) NOT NULL DEFAULT 0,
    must_run_alone TINYINT(1) NOT NULL DEFAULT 0,
    preferred_max_parallelism INT UNSIGNED NOT NULL DEFAULT 1,
    last_cpu_percent DECIMAL(8,2) DEFAULT NULL,
    average_cpu_percent DECIMAL(8,2) DEFAULT NULL,
    p95_cpu_percent DECIMAL(8,2) DEFAULT NULL,
    last_memory_peak_bytes BIGINT UNSIGNED DEFAULT NULL,
    average_memory_peak_bytes BIGINT UNSIGNED DEFAULT NULL,
    p95_memory_peak_bytes BIGINT UNSIGNED DEFAULT NULL,
    last_queue_wait_seconds DECIMAL(10,2) DEFAULT NULL,
    last_lock_wait_seconds DECIMAL(10,2) DEFAULT NULL,
    average_lock_wait_seconds DECIMAL(10,2) DEFAULT NULL,
    last_overlap_count INT UNSIGNED NOT NULL DEFAULT 0,
    last_overlapped TINYINT(1) NOT NULL DEFAULT 0,
    recent_timeout_rate DECIMAL(8,4) DEFAULT NULL,
    recent_failure_rate DECIMAL(8,4) DEFAULT NULL,
    current_projected_cpu_percent DECIMAL(8,2) DEFAULT NULL,
    current_projected_memory_bytes BIGINT UNSIGNED DEFAULT NULL,
    current_pressure_state VARCHAR(32) NOT NULL DEFAULT 'healthy',
    last_capacity_reason VARCHAR(500) DEFAULT NULL,
    allow_backfill TINYINT(1) NOT NULL DEFAULT 0,
    backfill_priority VARCHAR(20) NOT NULL DEFAULT 'normal',
    min_backfill_gap_seconds INT UNSIGNED NOT NULL DEFAULT 900,
    max_early_start_seconds INT UNSIGNED NOT NULL DEFAULT 0,
    last_execution_mode VARCHAR(32) NOT NULL DEFAULT 'scheduled',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_job_key (job_key),
    KEY idx_enabled (enabled),
    KEY idx_next_run_at (next_run_at),
    KEY idx_job_key (job_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET @sync_schedules_offset_seconds_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'offset_seconds'
);
SET @sync_schedules_offset_seconds_sql := IF(
    @sync_schedules_offset_seconds_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN offset_seconds INT UNSIGNED NOT NULL DEFAULT 0 AFTER interval_seconds',
    'SELECT 1'
);
PREPARE sync_schedules_offset_seconds_stmt FROM @sync_schedules_offset_seconds_sql;
EXECUTE sync_schedules_offset_seconds_stmt;
DEALLOCATE PREPARE sync_schedules_offset_seconds_stmt;

SET @sync_schedules_interval_minutes_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'interval_minutes'
);
SET @sync_schedules_interval_minutes_sql := IF(
    @sync_schedules_interval_minutes_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN interval_minutes INT UNSIGNED NOT NULL DEFAULT 5 AFTER enabled',
    'SELECT 1'
);
PREPARE sync_schedules_interval_minutes_stmt FROM @sync_schedules_interval_minutes_sql;
EXECUTE sync_schedules_interval_minutes_stmt;
DEALLOCATE PREPARE sync_schedules_interval_minutes_stmt;

SET @sync_schedules_offset_minutes_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'offset_minutes'
);
SET @sync_schedules_offset_minutes_sql := IF(
    @sync_schedules_offset_minutes_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN offset_minutes INT UNSIGNED NOT NULL DEFAULT 0 AFTER offset_seconds',
    'SELECT 1'
);
PREPARE sync_schedules_offset_minutes_stmt FROM @sync_schedules_offset_minutes_sql;
EXECUTE sync_schedules_offset_minutes_stmt;

DEALLOCATE PREPARE sync_schedules_offset_minutes_stmt;

SET @sync_schedules_latest_allowed_start_at_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'latest_allowed_start_at'
);
SET @sync_schedules_latest_allowed_start_at_sql := IF(
    @sync_schedules_latest_allowed_start_at_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN latest_allowed_start_at DATETIME DEFAULT NULL AFTER next_run_at',
    'SELECT 1'
);
PREPARE sync_schedules_latest_allowed_start_at_stmt FROM @sync_schedules_latest_allowed_start_at_sql;
EXECUTE sync_schedules_latest_allowed_start_at_stmt;
DEALLOCATE PREPARE sync_schedules_latest_allowed_start_at_stmt;

SET @sync_schedules_priority_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'priority'
);
SET @sync_schedules_priority_sql := IF(
    @sync_schedules_priority_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN priority VARCHAR(20) NOT NULL DEFAULT ''normal'' AFTER offset_minutes',
    'SELECT 1'
);
PREPARE sync_schedules_priority_stmt FROM @sync_schedules_priority_sql;
EXECUTE sync_schedules_priority_stmt;
DEALLOCATE PREPARE sync_schedules_priority_stmt;

SET @sync_schedules_concurrency_policy_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'concurrency_policy'
);
SET @sync_schedules_concurrency_policy_sql := IF(
    @sync_schedules_concurrency_policy_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN concurrency_policy VARCHAR(40) NOT NULL DEFAULT ''single'' AFTER priority',
    'SELECT 1'
);
PREPARE sync_schedules_concurrency_policy_stmt FROM @sync_schedules_concurrency_policy_sql;
EXECUTE sync_schedules_concurrency_policy_stmt;
DEALLOCATE PREPARE sync_schedules_concurrency_policy_stmt;

SET @sync_schedules_timeout_seconds_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'timeout_seconds'
);
SET @sync_schedules_timeout_seconds_sql := IF(
    @sync_schedules_timeout_seconds_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN timeout_seconds INT UNSIGNED NOT NULL DEFAULT 300 AFTER concurrency_policy',
    'SELECT 1'
);
PREPARE sync_schedules_timeout_seconds_stmt FROM @sync_schedules_timeout_seconds_sql;
EXECUTE sync_schedules_timeout_seconds_stmt;
DEALLOCATE PREPARE sync_schedules_timeout_seconds_stmt;

SET @sync_schedules_last_started_at_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'last_started_at'
);
SET @sync_schedules_last_started_at_sql := IF(
    @sync_schedules_last_started_at_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN last_started_at DATETIME DEFAULT NULL AFTER last_run_at',
    'SELECT 1'
);
PREPARE sync_schedules_last_started_at_stmt FROM @sync_schedules_last_started_at_sql;
EXECUTE sync_schedules_last_started_at_stmt;
DEALLOCATE PREPARE sync_schedules_last_started_at_stmt;

SET @sync_schedules_last_finished_at_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'last_finished_at'
);
SET @sync_schedules_last_finished_at_sql := IF(
    @sync_schedules_last_finished_at_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN last_finished_at DATETIME DEFAULT NULL AFTER last_started_at',
    'SELECT 1'
);
PREPARE sync_schedules_last_finished_at_stmt FROM @sync_schedules_last_finished_at_sql;
EXECUTE sync_schedules_last_finished_at_stmt;
DEALLOCATE PREPARE sync_schedules_last_finished_at_stmt;

SET @sync_schedules_last_duration_seconds_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'last_duration_seconds'
);
SET @sync_schedules_last_duration_seconds_sql := IF(
    @sync_schedules_last_duration_seconds_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN last_duration_seconds DECIMAL(10,2) DEFAULT NULL AFTER last_finished_at',
    'SELECT 1'
);
PREPARE sync_schedules_last_duration_seconds_stmt FROM @sync_schedules_last_duration_seconds_sql;
EXECUTE sync_schedules_last_duration_seconds_stmt;
DEALLOCATE PREPARE sync_schedules_last_duration_seconds_stmt;

SET @sync_schedules_average_duration_seconds_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'average_duration_seconds'
);
SET @sync_schedules_average_duration_seconds_sql := IF(
    @sync_schedules_average_duration_seconds_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN average_duration_seconds DECIMAL(10,2) DEFAULT NULL AFTER last_duration_seconds',
    'SELECT 1'
);
PREPARE sync_schedules_average_duration_seconds_stmt FROM @sync_schedules_average_duration_seconds_sql;
EXECUTE sync_schedules_average_duration_seconds_stmt;
DEALLOCATE PREPARE sync_schedules_average_duration_seconds_stmt;

SET @sync_schedules_p95_duration_seconds_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'p95_duration_seconds'
);
SET @sync_schedules_p95_duration_seconds_sql := IF(
    @sync_schedules_p95_duration_seconds_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN p95_duration_seconds DECIMAL(10,2) DEFAULT NULL AFTER average_duration_seconds',
    'SELECT 1'
);
PREPARE sync_schedules_p95_duration_seconds_stmt FROM @sync_schedules_p95_duration_seconds_sql;
EXECUTE sync_schedules_p95_duration_seconds_stmt;
DEALLOCATE PREPARE sync_schedules_p95_duration_seconds_stmt;

SET @sync_schedules_last_result_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'last_result'
);
SET @sync_schedules_last_result_sql := IF(
    @sync_schedules_last_result_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN last_result VARCHAR(120) DEFAULT NULL AFTER p95_duration_seconds',
    'SELECT 1'
);
PREPARE sync_schedules_last_result_stmt FROM @sync_schedules_last_result_sql;
EXECUTE sync_schedules_last_result_stmt;
DEALLOCATE PREPARE sync_schedules_last_result_stmt;

SET @sync_schedules_next_due_at_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'next_due_at'
);
SET @sync_schedules_next_due_at_sql := IF(
    @sync_schedules_next_due_at_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN next_due_at DATETIME DEFAULT NULL AFTER last_result',
    'SELECT 1'
);
PREPARE sync_schedules_next_due_at_stmt FROM @sync_schedules_next_due_at_sql;
EXECUTE sync_schedules_next_due_at_stmt;
DEALLOCATE PREPARE sync_schedules_next_due_at_stmt;

SET @sync_schedules_current_state_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'current_state'
);
SET @sync_schedules_current_state_sql := IF(
    @sync_schedules_current_state_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN current_state ENUM(''running'', ''waiting'', ''stopped'') NOT NULL DEFAULT ''waiting'' AFTER next_due_at',
    'SELECT 1'
);
PREPARE sync_schedules_current_state_stmt FROM @sync_schedules_current_state_sql;
EXECUTE sync_schedules_current_state_stmt;
DEALLOCATE PREPARE sync_schedules_current_state_stmt;

SET @sync_schedules_tuning_mode_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'tuning_mode'
);
SET @sync_schedules_tuning_mode_sql := IF(
    @sync_schedules_tuning_mode_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN tuning_mode ENUM(''automatic'', ''manual'') NOT NULL DEFAULT ''automatic'' AFTER current_state',
    'SELECT 1'
);
PREPARE sync_schedules_tuning_mode_stmt FROM @sync_schedules_tuning_mode_sql;
EXECUTE sync_schedules_tuning_mode_stmt;
DEALLOCATE PREPARE sync_schedules_tuning_mode_stmt;

SET @sync_schedules_discovered_from_code_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'discovered_from_code'
);
SET @sync_schedules_discovered_from_code_sql := IF(
    @sync_schedules_discovered_from_code_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN discovered_from_code TINYINT(1) NOT NULL DEFAULT 0 AFTER tuning_mode',
    'SELECT 1'
);
PREPARE sync_schedules_discovered_from_code_stmt FROM @sync_schedules_discovered_from_code_sql;
EXECUTE sync_schedules_discovered_from_code_stmt;
DEALLOCATE PREPARE sync_schedules_discovered_from_code_stmt;

SET @sync_schedules_explicitly_configured_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'explicitly_configured'
);
SET @sync_schedules_explicitly_configured_sql := IF(
    @sync_schedules_explicitly_configured_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN explicitly_configured TINYINT(1) NOT NULL DEFAULT 1 AFTER discovered_from_code',
    'SELECT 1'
);
PREPARE sync_schedules_explicitly_configured_stmt FROM @sync_schedules_explicitly_configured_sql;
EXECUTE sync_schedules_explicitly_configured_stmt;
DEALLOCATE PREPARE sync_schedules_explicitly_configured_stmt;

SET @sync_schedules_last_auto_tuned_at_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'last_auto_tuned_at'
);
SET @sync_schedules_last_auto_tuned_at_sql := IF(
    @sync_schedules_last_auto_tuned_at_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN last_auto_tuned_at DATETIME DEFAULT NULL AFTER explicitly_configured',
    'SELECT 1'
);
PREPARE sync_schedules_last_auto_tuned_at_stmt FROM @sync_schedules_last_auto_tuned_at_sql;
EXECUTE sync_schedules_last_auto_tuned_at_stmt;
DEALLOCATE PREPARE sync_schedules_last_auto_tuned_at_stmt;

SET @sync_schedules_last_auto_tune_reason_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'last_auto_tune_reason'
);
SET @sync_schedules_last_auto_tune_reason_sql := IF(
    @sync_schedules_last_auto_tune_reason_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN last_auto_tune_reason VARCHAR(500) DEFAULT NULL AFTER last_auto_tuned_at',
    'SELECT 1'
);
PREPARE sync_schedules_last_auto_tune_reason_stmt FROM @sync_schedules_last_auto_tune_reason_sql;
EXECUTE sync_schedules_last_auto_tune_reason_stmt;
DEALLOCATE PREPARE sync_schedules_last_auto_tune_reason_stmt;

SET @sync_schedules_degraded_until_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'degraded_until'
);
SET @sync_schedules_degraded_until_sql := IF(
    @sync_schedules_degraded_until_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN degraded_until DATETIME DEFAULT NULL AFTER last_auto_tune_reason',
    'SELECT 1'
);
PREPARE sync_schedules_degraded_until_stmt FROM @sync_schedules_degraded_until_sql;
EXECUTE sync_schedules_degraded_until_stmt;
DEALLOCATE PREPARE sync_schedules_degraded_until_stmt;

SET @sync_schedules_failure_streak_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'failure_streak'
);
SET @sync_schedules_failure_streak_sql := IF(
    @sync_schedules_failure_streak_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN failure_streak INT UNSIGNED NOT NULL DEFAULT 0 AFTER degraded_until',
    'SELECT 1'
);
PREPARE sync_schedules_failure_streak_stmt FROM @sync_schedules_failure_streak_sql;
EXECUTE sync_schedules_failure_streak_stmt;
DEALLOCATE PREPARE sync_schedules_failure_streak_stmt;

SET @sync_schedules_lock_conflict_count_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'lock_conflict_count'
);
SET @sync_schedules_lock_conflict_count_sql := IF(
    @sync_schedules_lock_conflict_count_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN lock_conflict_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER failure_streak',
    'SELECT 1'
);
PREPARE sync_schedules_lock_conflict_count_stmt FROM @sync_schedules_lock_conflict_count_sql;
EXECUTE sync_schedules_lock_conflict_count_stmt;
DEALLOCATE PREPARE sync_schedules_lock_conflict_count_stmt;

SET @sync_schedules_timeout_count_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'sync_schedules'
      AND COLUMN_NAME = 'timeout_count'
);
SET @sync_schedules_timeout_count_sql := IF(
    @sync_schedules_timeout_count_exists = 0,
    'ALTER TABLE sync_schedules ADD COLUMN timeout_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER lock_conflict_count',
    'SELECT 1'
);
PREPARE sync_schedules_timeout_count_stmt FROM @sync_schedules_timeout_count_sql;
EXECUTE sync_schedules_timeout_count_stmt;
DEALLOCATE PREPARE sync_schedules_timeout_count_stmt;

UPDATE sync_schedules
SET interval_minutes = GREATEST(1, CEIL(interval_seconds / 60)),
    offset_minutes = FLOOR(offset_seconds / 60),
    next_due_at = COALESCE(next_due_at, next_run_at),
    current_state = CASE
        WHEN enabled = 0 THEN 'stopped'
        WHEN locked_until IS NOT NULL AND locked_until > UTC_TIMESTAMP() THEN 'running'
        ELSE 'waiting'
    END
WHERE interval_minutes IS NULL
   OR offset_minutes IS NULL
   OR next_due_at IS NULL;

CREATE TABLE IF NOT EXISTS scheduler_daemon_state (
    daemon_key VARCHAR(64) NOT NULL PRIMARY KEY,
    owner_token VARCHAR(190) DEFAULT NULL,
    owner_label VARCHAR(190) DEFAULT NULL,
    owner_pid INT UNSIGNED DEFAULT NULL,
    owner_hostname VARCHAR(190) DEFAULT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'stopped',
    loop_state VARCHAR(64) DEFAULT NULL,
    stop_requested TINYINT(1) NOT NULL DEFAULT 0,
    restart_requested TINYINT(1) NOT NULL DEFAULT 0,
    active_dispatch_count INT UNSIGNED NOT NULL DEFAULT 0,
    current_loop_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    current_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
    started_at DATETIME DEFAULT NULL,
    heartbeat_at DATETIME DEFAULT NULL,
    lease_expires_at DATETIME DEFAULT NULL,
    last_dispatch_at DATETIME DEFAULT NULL,
    last_recovery_at DATETIME DEFAULT NULL,
    last_recovery_event VARCHAR(500) DEFAULT NULL,
    last_watchdog_at DATETIME DEFAULT NULL,
    watchdog_status VARCHAR(64) DEFAULT NULL,
    wake_requested_at DATETIME DEFAULT NULL,
    last_exit_at DATETIME DEFAULT NULL,
    last_exit_code INT DEFAULT NULL,
    last_exit_reason VARCHAR(500) DEFAULT NULL,
    metadata_json LONGTEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_scheduler_daemon_lease (lease_expires_at),
    KEY idx_scheduler_daemon_heartbeat (heartbeat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO scheduler_daemon_state (daemon_key, status, loop_state, watchdog_status)
VALUES ('master', 'stopped', 'idle', 'unknown')
ON DUPLICATE KEY UPDATE daemon_key = daemon_key;


CREATE TABLE IF NOT EXISTS scheduler_job_pairing_rules (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    primary_job_key VARCHAR(190) NOT NULL,
    secondary_job_key VARCHAR(190) NOT NULL,
    rule_type VARCHAR(20) NOT NULL,
    source_type VARCHAR(30) NOT NULL DEFAULT 'profiling',
    profiling_run_id BIGINT UNSIGNED DEFAULT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    notes VARCHAR(500) DEFAULT NULL,
    metadata_json LONGTEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_scheduler_pairing_rule (primary_job_key, secondary_job_key, rule_type),
    KEY idx_scheduler_pairing_rules_type_active (rule_type, active),
    KEY idx_scheduler_pairing_rules_run (profiling_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_profiling_runs (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    run_status VARCHAR(30) NOT NULL,
    current_phase VARCHAR(40) NOT NULL,
    execution_mode VARCHAR(30) NOT NULL DEFAULT 'isolated_only',
    started_by VARCHAR(190) NOT NULL,
    scope_json LONGTEXT DEFAULT NULL,
    options_json LONGTEXT DEFAULT NULL,
    selected_job_keys_json LONGTEXT DEFAULT NULL,
    progress_json LONGTEXT DEFAULT NULL,
    recommendations_json LONGTEXT DEFAULT NULL,
    summary_json LONGTEXT DEFAULT NULL,
    failure_message VARCHAR(500) DEFAULT NULL,
    applied_snapshot_id BIGINT UNSIGNED DEFAULT NULL,
    rollback_snapshot_id BIGINT UNSIGNED DEFAULT NULL,
    started_at DATETIME NOT NULL,
    finished_at DATETIME DEFAULT NULL,
    applied_at DATETIME DEFAULT NULL,
    cancelled_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_scheduler_profiling_runs_status (run_status, current_phase),
    KEY idx_scheduler_profiling_runs_started (started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_profiling_samples (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    profiling_run_id BIGINT UNSIGNED NOT NULL,
    schedule_id INT UNSIGNED DEFAULT NULL,
    job_key VARCHAR(190) NOT NULL,
    phase VARCHAR(30) NOT NULL,
    sample_key VARCHAR(190) NOT NULL,
    partner_job_key VARCHAR(190) DEFAULT NULL,
    sample_index INT UNSIGNED NOT NULL DEFAULT 1,
    run_status VARCHAR(20) NOT NULL,
    wall_duration_seconds DECIMAL(10,2) DEFAULT NULL,
    cpu_percent DECIMAL(8,2) DEFAULT NULL,
    memory_peak_bytes BIGINT UNSIGNED DEFAULT NULL,
    lock_wait_seconds DECIMAL(10,2) DEFAULT NULL,
    queue_wait_seconds DECIMAL(10,2) DEFAULT NULL,
    overlap_count INT UNSIGNED NOT NULL DEFAULT 0,
    timed_out TINYINT(1) NOT NULL DEFAULT 0,
    failed TINYINT(1) NOT NULL DEFAULT 0,
    workload_json LONGTEXT DEFAULT NULL,
    result_json LONGTEXT DEFAULT NULL,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_scheduler_profiling_samples_run_phase (profiling_run_id, phase, created_at),
    KEY idx_scheduler_profiling_samples_job (job_key, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_profiling_pairings (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    profiling_run_id BIGINT UNSIGNED NOT NULL,
    primary_job_key VARCHAR(190) NOT NULL,
    secondary_job_key VARCHAR(190) NOT NULL,
    probe_status VARCHAR(20) NOT NULL,
    compatibility VARCHAR(20) NOT NULL DEFAULT 'pending',
    recommended_parallelism INT UNSIGNED NOT NULL DEFAULT 1,
    reason_text VARCHAR(500) DEFAULT NULL,
    metrics_json LONGTEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_scheduler_profiling_pair (profiling_run_id, primary_job_key, secondary_job_key),
    KEY idx_scheduler_profiling_pairings_run (profiling_run_id, compatibility)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_schedule_snapshots (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    profiling_run_id BIGINT UNSIGNED DEFAULT NULL,
    snapshot_label VARCHAR(80) NOT NULL,
    actor VARCHAR(190) NOT NULL,
    reason_text VARCHAR(500) DEFAULT NULL,
    schedule_json LONGTEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_scheduler_schedule_snapshots_run (profiling_run_id, created_at),
    KEY idx_scheduler_schedule_snapshots_label (snapshot_label, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_job_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    job_key VARCHAR(190) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    detail_json LONGTEXT DEFAULT NULL,
    lateness_seconds INT NOT NULL DEFAULT 0,
    duration_seconds DECIMAL(10,2) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_scheduler_job_events_job_created (job_key, created_at),
    KEY idx_scheduler_job_events_type_created (event_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_tuning_actions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    job_key VARCHAR(190) NOT NULL,
    actor VARCHAR(20) NOT NULL DEFAULT 'system',
    action_type VARCHAR(50) NOT NULL,
    reason_text VARCHAR(500) NOT NULL,
    before_json LONGTEXT DEFAULT NULL,
    after_json LONGTEXT DEFAULT NULL,
    metrics_json LONGTEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_scheduler_tuning_actions_job_created (job_key, created_at),
    KEY idx_scheduler_tuning_actions_actor_created (actor, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS scheduler_job_resource_metrics (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    schedule_id INT UNSIGNED DEFAULT NULL,
    job_key VARCHAR(190) NOT NULL,
    run_status VARCHAR(20) NOT NULL,
    wall_duration_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
    queue_wait_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
    lock_wait_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
    overlap_count INT UNSIGNED NOT NULL DEFAULT 0,
    overlapped TINYINT(1) NOT NULL DEFAULT 0,
    cpu_user_seconds DECIMAL(10,4) DEFAULT NULL,
    cpu_system_seconds DECIMAL(10,4) DEFAULT NULL,
    cpu_percent DECIMAL(8,2) DEFAULT NULL,
    memory_start_bytes BIGINT UNSIGNED DEFAULT NULL,
    memory_end_bytes BIGINT UNSIGNED DEFAULT NULL,
    memory_peak_bytes BIGINT UNSIGNED DEFAULT NULL,
    memory_peak_delta_bytes BIGINT UNSIGNED DEFAULT NULL,
    projected_cpu_percent DECIMAL(8,2) DEFAULT NULL,
    projected_memory_bytes BIGINT UNSIGNED DEFAULT NULL,
    pressure_state VARCHAR(32) DEFAULT NULL,
    timed_out TINYINT(1) NOT NULL DEFAULT 0,
    failed TINYINT(1) NOT NULL DEFAULT 0,
    started_at DATETIME DEFAULT NULL,
    finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_scheduler_resource_metrics_job_created (job_key, created_at),
    KEY idx_scheduler_resource_metrics_schedule_created (schedule_id, created_at),
    KEY idx_scheduler_resource_metrics_job_started (job_key, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS scheduler_planner_decisions (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    schedule_id INT UNSIGNED DEFAULT NULL,
    job_key VARCHAR(190) NOT NULL,
    decision_type VARCHAR(40) NOT NULL,
    pressure_state VARCHAR(32) NOT NULL DEFAULT 'healthy',
    reason_text VARCHAR(500) NOT NULL,
    decision_json LONGTEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_scheduler_planner_decisions_job_created (job_key, created_at),
    KEY idx_scheduler_planner_decisions_type_created (decision_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS intelligence_snapshots (
    snapshot_key VARCHAR(190) PRIMARY KEY,
    snapshot_status ENUM('ready', 'updating', 'failed') NOT NULL DEFAULT 'ready',
    payload_json LONGTEXT DEFAULT NULL,
    metadata_json LONGTEXT DEFAULT NULL,
    computed_at DATETIME DEFAULT NULL,
    refresh_started_at DATETIME DEFAULT NULL,
    expires_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_snapshot_status (snapshot_status),
    KEY idx_snapshot_expires_at (expires_at),
    KEY idx_snapshot_computed_at (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS ui_refresh_section_versions (
    section_key VARCHAR(190) PRIMARY KEY,
    version_counter BIGINT UNSIGNED NOT NULL DEFAULT 0,
    fingerprint CHAR(64) DEFAULT NULL,
    snapshot_key VARCHAR(190) DEFAULT NULL,
    domains_json JSON DEFAULT NULL,
    ui_sections_json JSON DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    last_job_key VARCHAR(190) DEFAULT NULL,
    last_status VARCHAR(40) DEFAULT NULL,
    last_event_id BIGINT UNSIGNED DEFAULT NULL,
    last_finished_at DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_ui_refresh_section_versions_event (last_event_id),
    KEY idx_ui_refresh_section_versions_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ui_refresh_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(80) NOT NULL DEFAULT 'job_completed',
    event_key VARCHAR(190) DEFAULT NULL,
    job_key VARCHAR(190) NOT NULL,
    job_status VARCHAR(40) NOT NULL,
    finished_at DATETIME NOT NULL,
    domains_json JSON DEFAULT NULL,
    ui_sections_json JSON DEFAULT NULL,
    section_versions_json JSON DEFAULT NULL,
    payload_json JSON DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_ui_refresh_event_key (event_key),
    KEY idx_ui_refresh_events_job (job_key, finished_at),
    KEY idx_ui_refresh_events_finished (finished_at),
    KEY idx_ui_refresh_events_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_deal_alerts_current (
    alert_key VARCHAR(190) PRIMARY KEY,
    item_type_id INT UNSIGNED NOT NULL,
    source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    source_name VARCHAR(190) NOT NULL,
    percent_band DECIMAL(6,2) NOT NULL,
    current_price DECIMAL(20,2) NOT NULL,
    normal_price DECIMAL(20,2) NOT NULL,
    percent_of_normal DECIMAL(8,4) NOT NULL,
    anomaly_score DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    severity ENUM('critical', 'very_strong', 'strong', 'watch') NOT NULL,
    severity_rank TINYINT UNSIGNED NOT NULL DEFAULT 1,
    quantity_available BIGINT UNSIGNED NOT NULL DEFAULT 0,
    listing_count INT UNSIGNED NOT NULL DEFAULT 0,
    best_order_id BIGINT UNSIGNED DEFAULT NULL,
    baseline_model VARCHAR(80) NOT NULL DEFAULT 'median_weighted_blend',
    baseline_points INT UNSIGNED NOT NULL DEFAULT 0,
    baseline_median_price DECIMAL(20,2) DEFAULT NULL,
    baseline_weighted_price DECIMAL(20,2) DEFAULT NULL,
    observed_at DATETIME NOT NULL,
    detected_at DATETIME NOT NULL,
    last_seen_at DATETIME NOT NULL,
    inactive_at DATETIME DEFAULT NULL,
    freshness_seconds INT UNSIGNED NOT NULL DEFAULT 0,
    status ENUM('active', 'inactive') NOT NULL DEFAULT 'active',
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_market_deal_alerts_status_severity (status, severity_rank, anomaly_score),
    KEY idx_market_deal_alerts_item_source (item_type_id, source_type, source_id),
    KEY idx_market_deal_alerts_last_seen (last_seen_at),
    KEY idx_market_deal_alerts_observed (observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_deal_alert_dismissals (
    alert_key VARCHAR(190) PRIMARY KEY,
    dismissed_severity_rank TINYINT UNSIGNED NOT NULL DEFAULT 1,
    dismissed_at DATETIME NOT NULL,
    dismissed_until DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_market_deal_alert_dismissals_until (dismissed_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_ai_briefings (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    entity_type ENUM('fit', 'group') NOT NULL,
    entity_id INT UNSIGNED NOT NULL,
    fit_id INT UNSIGNED DEFAULT NULL,
    group_id INT UNSIGNED DEFAULT NULL,
    generation_status ENUM('ready', 'fallback', 'failed') NOT NULL DEFAULT 'ready',
    computed_at DATETIME DEFAULT NULL,
    model_name VARCHAR(120) DEFAULT NULL,
    headline VARCHAR(255) DEFAULT NULL,
    summary TEXT DEFAULT NULL,
    action_text TEXT DEFAULT NULL,
    priority_level ENUM('low', 'medium', 'high', 'critical') NOT NULL DEFAULT 'medium',
    source_payload_json LONGTEXT DEFAULT NULL,
    response_json LONGTEXT DEFAULT NULL,
    error_message VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_entity_briefing (entity_type, entity_id),
    KEY idx_entity_status (entity_type, generation_status, priority_level),
    KEY idx_computed_at (computed_at),
    KEY idx_fit_id (fit_id),
    KEY idx_group_id (group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_events (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sequence_id BIGINT UNSIGNED NOT NULL,
    killmail_id BIGINT UNSIGNED NOT NULL,
    killmail_hash VARCHAR(128) NOT NULL,
    uploaded_at DATETIME DEFAULT NULL,
    sequence_updated BIGINT UNSIGNED DEFAULT NULL,
    killmail_time DATETIME DEFAULT NULL,
    solar_system_id INT UNSIGNED DEFAULT NULL,
    region_id INT UNSIGNED DEFAULT NULL,
    victim_character_id BIGINT UNSIGNED DEFAULT NULL,
    victim_corporation_id BIGINT UNSIGNED DEFAULT NULL,
    victim_alliance_id BIGINT UNSIGNED DEFAULT NULL,
    victim_ship_type_id INT UNSIGNED DEFAULT NULL,
    zkb_json LONGTEXT NOT NULL,
    raw_killmail_json LONGTEXT NOT NULL,
    effective_killmail_at DATETIME GENERATED ALWAYS AS (COALESCE(killmail_time, created_at)) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_sequence_id (sequence_id),
    KEY idx_killmail_id (killmail_id),
    KEY idx_uploaded_at (uploaded_at),
    KEY idx_victim_alliance_sequence (victim_alliance_id, sequence_id),
    KEY idx_victim_corporation_sequence (victim_corporation_id, sequence_id),
    KEY idx_victim_alliance_effective (victim_alliance_id, effective_killmail_at),
    KEY idx_victim_corporation_effective (victim_corporation_id, effective_killmail_at),
    KEY idx_victim_ship_type (victim_ship_type_id),
    KEY idx_killmail_effective_ship (effective_killmail_at, victim_ship_type_id),
    KEY idx_killmail_ship_effective (victim_ship_type_id, effective_killmail_at),
    KEY idx_system_region (solar_system_id, region_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_attackers (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sequence_id BIGINT UNSIGNED NOT NULL,
    attacker_index SMALLINT UNSIGNED NOT NULL,
    character_id BIGINT UNSIGNED DEFAULT NULL,
    corporation_id BIGINT UNSIGNED DEFAULT NULL,
    alliance_id BIGINT UNSIGNED DEFAULT NULL,
    ship_type_id INT UNSIGNED DEFAULT NULL,
    weapon_type_id INT UNSIGNED DEFAULT NULL,
    final_blow TINYINT(1) NOT NULL DEFAULT 0,
    security_status DECIMAL(5,2) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_sequence_attacker (sequence_id, attacker_index),
    KEY idx_attacker_alliance (alliance_id),
    KEY idx_attacker_corporation (corporation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_items (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    sequence_id BIGINT UNSIGNED NOT NULL,
    item_index INT UNSIGNED NOT NULL,
    item_type_id INT UNSIGNED NOT NULL,
    item_flag INT DEFAULT NULL,
    quantity_dropped BIGINT DEFAULT NULL,
    quantity_destroyed BIGINT DEFAULT NULL,
    singleton TINYINT DEFAULT NULL,
    item_role ENUM('fitted', 'dropped', 'destroyed', 'other') NOT NULL DEFAULT 'other',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_sequence_item (sequence_id, item_index),
    KEY idx_item_type (item_type_id),
    KEY idx_item_role (item_role)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_tracked_alliances (
    alliance_id BIGINT UNSIGNED PRIMARY KEY,
    label VARCHAR(190) DEFAULT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_tracked_corporations (
    corporation_id BIGINT UNSIGNED PRIMARY KEY,
    label VARCHAR(190) DEFAULT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS entity_metadata_cache (
    entity_type ENUM('alliance', 'corporation', 'character', 'type', 'system', 'region') NOT NULL,
    entity_id BIGINT UNSIGNED NOT NULL,
    entity_name VARCHAR(255) DEFAULT NULL,
    image_url VARCHAR(255) DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    source_system VARCHAR(40) NOT NULL DEFAULT 'cache',
    resolution_status ENUM('pending', 'resolved', 'failed') NOT NULL DEFAULT 'pending',
    expires_at DATETIME DEFAULT NULL,
    last_requested_at DATETIME DEFAULT NULL,
    resolved_at DATETIME DEFAULT NULL,
    last_error_message VARCHAR(255) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_type, entity_id),
    KEY idx_resolution_status (resolution_status, entity_type, updated_at),
    KEY idx_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS market_orders_current (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    order_id BIGINT UNSIGNED NOT NULL,
    is_buy_order TINYINT(1) NOT NULL,
    price DECIMAL(20, 2) NOT NULL,
    volume_remain INT UNSIGNED NOT NULL,
    volume_total INT UNSIGNED NOT NULL,
    min_volume INT UNSIGNED NOT NULL DEFAULT 1,
    `range` VARCHAR(20) NOT NULL,
    duration SMALLINT UNSIGNED NOT NULL,
    issued DATETIME NOT NULL,
    expires DATETIME NOT NULL,
    observed_at DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source_order_current (source_type, source_id, order_id),
    KEY idx_market_orders_current_type_observed (source_type, source_id, type_id, observed_at),
    KEY idx_market_orders_current_observed (source_type, source_id, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_orders_history (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    order_id BIGINT UNSIGNED NOT NULL,
    is_buy_order TINYINT(1) NOT NULL,
    price DECIMAL(20, 2) NOT NULL,
    volume_remain INT UNSIGNED NOT NULL,
    volume_total INT UNSIGNED NOT NULL,
    min_volume INT UNSIGNED NOT NULL DEFAULT 1,
    `range` VARCHAR(20) NOT NULL,
    duration SMALLINT UNSIGNED NOT NULL,
    issued DATETIME NOT NULL,
    expires DATETIME NOT NULL,
    observed_at DATETIME NOT NULL,
    observed_date DATE GENERATED ALWAYS AS (DATE(observed_at)) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source_order_observed (source_type, source_id, order_id, observed_at),
    KEY idx_market_orders_history_type_observed (source_type, source_id, type_id, observed_at),
    KEY idx_market_orders_history_observed (source_type, source_id, observed_at),
    KEY idx_market_orders_history_source_date_type (source_type, source_id, observed_date, type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_order_snapshots_summary (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    observed_at DATETIME NOT NULL,
    observed_date DATE GENERATED ALWAYS AS (DATE(observed_at)) STORED,
    best_sell_price DECIMAL(20, 2) DEFAULT NULL,
    best_buy_price DECIMAL(20, 2) DEFAULT NULL,
    total_buy_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
    total_sell_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
    total_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
    buy_order_count INT UNSIGNED NOT NULL DEFAULT 0,
    sell_order_count INT UNSIGNED NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_market_order_snapshot_summary (source_type, source_id, type_id, observed_at),
    KEY idx_snapshot_summary_source_observed_type (source_type, source_id, observed_at, type_id),
    KEY idx_snapshot_summary_source_type_observed (source_type, source_id, type_id, observed_at),
    KEY idx_snapshot_summary_source_date_type (source_type, source_id, observed_date, type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_history_daily (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(20, 2) NOT NULL,
    high_price DECIMAL(20, 2) NOT NULL,
    low_price DECIMAL(20, 2) NOT NULL,
    close_price DECIMAL(20, 2) NOT NULL,
    average_price DECIMAL(20, 2) DEFAULT NULL,
    volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
    order_count INT UNSIGNED DEFAULT NULL,
    source_label VARCHAR(40) NOT NULL DEFAULT 'esi',
    observed_at DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_market_history_daily (source_type, source_id, type_id, trade_date),
    KEY idx_market_history_daily_type_date (source_type, source_id, type_id, trade_date),
    KEY idx_market_history_daily_observed (source_type, source_id, observed_at),
    KEY idx_market_history_daily_date_type (source_type, source_id, trade_date, type_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_hub_local_history_daily (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    source VARCHAR(40) NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    trade_date DATE NOT NULL,
    open_price DECIMAL(20, 2) NOT NULL,
    high_price DECIMAL(20, 2) NOT NULL,
    low_price DECIMAL(20, 2) NOT NULL,
    close_price DECIMAL(20, 2) NOT NULL,
    buy_price DECIMAL(20, 2) DEFAULT NULL,
    sell_price DECIMAL(20, 2) DEFAULT NULL,
    spread_value DECIMAL(20, 2) DEFAULT NULL,
    spread_percent DECIMAL(20, 4) DEFAULT NULL,
    volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
    buy_order_count INT UNSIGNED NOT NULL DEFAULT 0,
    sell_order_count INT UNSIGNED NOT NULL DEFAULT 0,
    captured_at DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_market_hub_local_history_daily_source_date (source, source_id, trade_date),
    KEY idx_market_hub_local_history_daily_latest_points (source, source_id, type_id, trade_date, id),
    UNIQUE KEY unique_market_hub_local_history_daily (source, source_id, type_id, trade_date),
    KEY idx_market_hub_local_history_daily_type_date (type_id, trade_date),
    KEY idx_market_hub_local_history_daily_source_type_date (source, source_id, type_id, trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS static_data_import_state (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    source_key VARCHAR(120) NOT NULL,
    source_url VARCHAR(500) NOT NULL,
    remote_build_id VARCHAR(190) DEFAULT NULL,
    imported_build_id VARCHAR(190) DEFAULT NULL,
    imported_mode ENUM('full', 'incremental') DEFAULT NULL,
    status ENUM('idle', 'running', 'success', 'failed') NOT NULL DEFAULT 'idle',
    last_checked_at DATETIME DEFAULT NULL,
    last_import_started_at DATETIME DEFAULT NULL,
    last_import_finished_at DATETIME DEFAULT NULL,
    last_error_message VARCHAR(500) DEFAULT NULL,
    metadata_json JSON DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source_key (source_key),
    KEY idx_imported_build_id (imported_build_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_regions (
    region_id INT UNSIGNED PRIMARY KEY,
    region_name VARCHAR(120) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_constellations (
    constellation_id INT UNSIGNED PRIMARY KEY,
    region_id INT UNSIGNED NOT NULL,
    constellation_name VARCHAR(120) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_region_id (region_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_systems (
    system_id INT UNSIGNED PRIMARY KEY,
    constellation_id INT UNSIGNED NOT NULL,
    region_id INT UNSIGNED NOT NULL,
    system_name VARCHAR(120) NOT NULL,
    security DECIMAL(5,3) NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_constellation_id (constellation_id),
    KEY idx_region_id (region_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_npc_stations (
    station_id INT UNSIGNED PRIMARY KEY,
    station_name VARCHAR(190) NOT NULL,
    system_id INT UNSIGNED NOT NULL,
    constellation_id INT UNSIGNED NOT NULL,
    region_id INT UNSIGNED NOT NULL,
    station_type_id INT UNSIGNED DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_system_id (system_id),
    KEY idx_region_id (region_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_market_groups (
    market_group_id INT UNSIGNED PRIMARY KEY,
    parent_group_id INT UNSIGNED DEFAULT NULL,
    market_group_name VARCHAR(190) NOT NULL,
    description TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_parent_group_id (parent_group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_item_categories (
    category_id INT UNSIGNED PRIMARY KEY,
    category_name VARCHAR(190) NOT NULL,
    published TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_published (published)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_item_groups (
    group_id INT UNSIGNED PRIMARY KEY,
    category_id INT UNSIGNED NOT NULL,
    group_name VARCHAR(190) NOT NULL,
    published TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_category_id (category_id),
    KEY idx_published (published)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_meta_groups (
    meta_group_id INT UNSIGNED PRIMARY KEY,
    meta_group_name VARCHAR(120) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ref_item_types (
    type_id INT UNSIGNED PRIMARY KEY,
    category_id INT UNSIGNED NOT NULL,
    group_id INT UNSIGNED NOT NULL,
    market_group_id INT UNSIGNED DEFAULT NULL,
    meta_group_id INT UNSIGNED DEFAULT NULL,
    type_name VARCHAR(255) NOT NULL,
    type_name_normalized VARCHAR(255) GENERATED ALWAYS AS (LOWER(type_name)) STORED,
    description TEXT DEFAULT NULL,
    published TINYINT(1) NOT NULL DEFAULT 0,
    volume DECIMAL(20,6) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_category_id (category_id),
    KEY idx_group_id (group_id),
    KEY idx_market_group_id (market_group_id),
    KEY idx_meta_group_id (meta_group_id),
    KEY idx_type_name_normalized (type_name_normalized),
    KEY idx_published (published)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS item_name_cache (
    normalized_name VARCHAR(190) PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL,
    type_id INT UNSIGNED DEFAULT NULL,
    resolution_source ENUM('cache', 'ref', 'esi', 'missing') NOT NULL DEFAULT 'ref',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_type_id (type_id),
    KEY idx_resolution_source (resolution_source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_groups (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    group_name VARCHAR(190) NOT NULL,
    description TEXT DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_group_name (group_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_fits (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    fit_name VARCHAR(190) NOT NULL,
    ship_name VARCHAR(255) NOT NULL,
    ship_type_id INT UNSIGNED DEFAULT NULL,
    target_fleet_size_override INT UNSIGNED DEFAULT NULL,
    source_type ENUM('html', 'eft', 'buyall', 'manual') NOT NULL DEFAULT 'manual',
    source_format ENUM('eft', 'buyall') NOT NULL,
    source_reference VARCHAR(255) DEFAULT NULL,
    notes TEXT DEFAULT NULL,
    import_body LONGTEXT NOT NULL,
    raw_html LONGTEXT DEFAULT NULL,
    raw_buyall LONGTEXT DEFAULT NULL,
    raw_eft LONGTEXT DEFAULT NULL,
    metadata_json LONGTEXT DEFAULT NULL,
    parse_warnings_json LONGTEXT DEFAULT NULL,
    parse_status ENUM('ready', 'review') NOT NULL DEFAULT 'ready',
    review_status ENUM('clean', 'needs_review', 'reparse_requested') NOT NULL DEFAULT 'clean',
    conflict_state ENUM('none', 'duplicate_name', 'duplicate_items', 'version_conflict', 'source_mismatch') NOT NULL DEFAULT 'none',
    fingerprint_hash CHAR(64) DEFAULT NULL,
    warning_count INT UNSIGNED NOT NULL DEFAULT 0,
    item_count INT UNSIGNED NOT NULL DEFAULT 0,
    unresolved_count INT UNSIGNED NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_doctrine_group_id (doctrine_group_id),
    KEY idx_ship_type_id (ship_type_id),
    KEY idx_source_type (source_type),
    KEY idx_parse_status (parse_status, review_status),
    KEY idx_conflict_state (conflict_state),
    KEY idx_fingerprint_hash (fingerprint_hash),
    CONSTRAINT fk_doctrine_fits_group FOREIGN KEY (doctrine_group_id) REFERENCES doctrine_groups(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_fit_groups (
    doctrine_fit_id INT UNSIGNED NOT NULL,
    doctrine_group_id INT UNSIGNED NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (doctrine_fit_id, doctrine_group_id),
    KEY idx_doctrine_fit_groups_group (doctrine_group_id),
    CONSTRAINT fk_doctrine_fit_groups_fit FOREIGN KEY (doctrine_fit_id) REFERENCES doctrine_fits(id) ON DELETE CASCADE,
    CONSTRAINT fk_doctrine_fit_groups_group FOREIGN KEY (doctrine_group_id) REFERENCES doctrine_groups(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_fit_items (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    doctrine_fit_id INT UNSIGNED NOT NULL,
    line_number SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    slot_category VARCHAR(80) NOT NULL DEFAULT 'Items',
    source_role VARCHAR(80) NOT NULL DEFAULT 'fit',
    item_name VARCHAR(255) NOT NULL,
    type_id INT UNSIGNED DEFAULT NULL,
    quantity INT UNSIGNED NOT NULL DEFAULT 1,
    is_stock_tracked TINYINT(1) NOT NULL DEFAULT 1,
    resolution_source ENUM('cache', 'ref', 'esi', 'missing') NOT NULL DEFAULT 'ref',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_doctrine_fit_id (doctrine_fit_id),
    KEY idx_type_id (type_id),
    KEY idx_slot_category (slot_category),
    KEY idx_source_role (source_role),
    CONSTRAINT fk_doctrine_fit_items_fit FOREIGN KEY (doctrine_fit_id) REFERENCES doctrine_fits(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_fit_snapshots (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fit_id INT UNSIGNED NOT NULL,
    snapshot_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
    target_fits INT UNSIGNED NOT NULL DEFAULT 0,
    fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
    bottleneck_type_id INT UNSIGNED DEFAULT NULL,
    bottleneck_quantity INT NOT NULL DEFAULT 0,
    readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
    resupply_pressure_state VARCHAR(32) NOT NULL DEFAULT 'stable',
    resupply_pressure_code VARCHAR(64) NOT NULL DEFAULT 'stable',
    resupply_pressure_text VARCHAR(255) NOT NULL DEFAULT 'Stable',
    recommendation_code VARCHAR(64) NOT NULL DEFAULT 'observe',
    recommendation_text VARCHAR(255) NOT NULL DEFAULT '',
    loss_24h INT UNSIGNED NOT NULL DEFAULT 0,
    loss_7d INT UNSIGNED NOT NULL DEFAULT 0,
    local_coverage_pct DECIMAL(6,2) NOT NULL DEFAULT 0.00,
    depletion_24h INT NOT NULL DEFAULT 0,
    depletion_7d INT NOT NULL DEFAULT 0,
    total_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    score_loss_pressure DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    score_stock_gap DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    score_depletion DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    score_bottleneck DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    KEY idx_fit_snapshot_time (fit_id, snapshot_time),
    KEY idx_snapshot_time (snapshot_time),
    KEY idx_readiness_state (readiness_state),
    KEY idx_resupply_pressure_state (resupply_pressure_state),
    KEY idx_recommendation_code (recommendation_code),
    CONSTRAINT fk_doctrine_fit_snapshots_fit FOREIGN KEY (fit_id) REFERENCES doctrine_fits(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_activity_snapshots (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    entity_type ENUM('fit', 'group') NOT NULL,
    entity_id INT UNSIGNED NOT NULL,
    entity_name VARCHAR(190) NOT NULL,
    snapshot_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rank_position INT UNSIGNED NOT NULL DEFAULT 0,
    previous_rank_position INT DEFAULT NULL,
    rank_delta INT NOT NULL DEFAULT 0,
    activity_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    activity_level VARCHAR(32) NOT NULL DEFAULT 'low',
    hull_losses_24h INT UNSIGNED NOT NULL DEFAULT 0,
    hull_losses_3d INT UNSIGNED NOT NULL DEFAULT 0,
    hull_losses_7d INT UNSIGNED NOT NULL DEFAULT 0,
    module_losses_24h INT UNSIGNED NOT NULL DEFAULT 0,
    module_losses_3d INT UNSIGNED NOT NULL DEFAULT 0,
    module_losses_7d INT UNSIGNED NOT NULL DEFAULT 0,
    fit_equivalent_losses_24h DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    fit_equivalent_losses_3d DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    fit_equivalent_losses_7d DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
    resupply_pressure_state VARCHAR(32) NOT NULL DEFAULT 'stable',
    resupply_pressure VARCHAR(255) NOT NULL DEFAULT 'Stable',
    readiness_gap_count INT UNSIGNED NOT NULL DEFAULT 0,
    resupply_gap_isk DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    score_components_json LONGTEXT DEFAULT NULL,
    explanation_text VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_doctrine_activity_snapshot (entity_type, entity_id, snapshot_time),
    KEY idx_doctrine_activity_snapshot_time (snapshot_time),
    KEY idx_doctrine_activity_rank (entity_type, rank_position),
    KEY idx_doctrine_activity_score (entity_type, activity_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS item_priority_snapshots (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    type_id INT UNSIGNED NOT NULL,
    item_name VARCHAR(255) NOT NULL,
    snapshot_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    rank_position INT UNSIGNED NOT NULL DEFAULT 0,
    previous_rank_position INT DEFAULT NULL,
    rank_delta INT NOT NULL DEFAULT 0,
    priority_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    priority_band VARCHAR(32) NOT NULL DEFAULT 'watch',
    is_doctrine_linked TINYINT(1) NOT NULL DEFAULT 0,
    linked_doctrine_count INT UNSIGNED NOT NULL DEFAULT 0,
    linked_active_doctrine_count INT UNSIGNED NOT NULL DEFAULT 0,
    local_available_qty INT NOT NULL DEFAULT 0,
    local_sell_orders INT NOT NULL DEFAULT 0,
    local_sell_volume INT NOT NULL DEFAULT 0,
    recent_loss_qty_24h INT UNSIGNED NOT NULL DEFAULT 0,
    recent_loss_qty_3d INT UNSIGNED NOT NULL DEFAULT 0,
    recent_loss_qty_7d INT UNSIGNED NOT NULL DEFAULT 0,
    recent_loss_events_24h INT UNSIGNED NOT NULL DEFAULT 0,
    recent_loss_events_3d INT UNSIGNED NOT NULL DEFAULT 0,
    recent_loss_events_7d INT UNSIGNED NOT NULL DEFAULT 0,
    readiness_gap_fit_count INT UNSIGNED NOT NULL DEFAULT 0,
    bottleneck_fit_count INT UNSIGNED NOT NULL DEFAULT 0,
    depletion_state VARCHAR(32) NOT NULL DEFAULT 'stable',
    score_components_json LONGTEXT DEFAULT NULL,
    linked_doctrines_json LONGTEXT DEFAULT NULL,
    explanation_text VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_item_priority_snapshot (type_id, snapshot_time),
    KEY idx_item_priority_snapshot_time (snapshot_time),
    KEY idx_item_priority_rank (rank_position),
    KEY idx_item_priority_score (priority_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_item_loss_1h (
    bucket_start DATETIME NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    doctrine_fit_id INT UNSIGNED DEFAULT NULL,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    hull_type_id INT UNSIGNED DEFAULT NULL,
    doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
    doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
    hull_type_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED,
    loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    quantity_lost BIGINT UNSIGNED NOT NULL DEFAULT 0,
    victim_count INT UNSIGNED NOT NULL DEFAULT 0,
    killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_killmail_item_loss_1h_dimensions (bucket_start, type_id, doctrine_fit_key, doctrine_group_key, hull_type_key),
    KEY idx_killmail_item_loss_1h_bucket_type (bucket_start, type_id),
    KEY idx_killmail_item_loss_1h_type_bucket (type_id, bucket_start),
    KEY idx_killmail_item_loss_1h_bucket (bucket_start),
    KEY idx_killmail_item_loss_1h_group_bucket (doctrine_group_id, bucket_start),
    KEY idx_killmail_item_loss_1h_fit_bucket (doctrine_fit_id, bucket_start),
    KEY idx_killmail_item_loss_1h_hull_bucket (hull_type_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_item_loss_1d (
    bucket_start DATE NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    doctrine_fit_id INT UNSIGNED DEFAULT NULL,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    hull_type_id INT UNSIGNED DEFAULT NULL,
    doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
    doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
    hull_type_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED,
    loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    quantity_lost BIGINT UNSIGNED NOT NULL DEFAULT 0,
    victim_count INT UNSIGNED NOT NULL DEFAULT 0,
    killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_killmail_item_loss_1d_dimensions (bucket_start, type_id, doctrine_fit_key, doctrine_group_key, hull_type_key),
    KEY idx_killmail_item_loss_1d_bucket_type (bucket_start, type_id),
    KEY idx_killmail_item_loss_1d_type_bucket (type_id, bucket_start),
    KEY idx_killmail_item_loss_1d_bucket (bucket_start),
    KEY idx_killmail_item_loss_1d_group_bucket (doctrine_group_id, bucket_start),
    KEY idx_killmail_item_loss_1d_fit_bucket (doctrine_fit_id, bucket_start),
    KEY idx_killmail_item_loss_1d_hull_bucket (hull_type_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_hull_loss_1d (
    bucket_start DATE NOT NULL,
    hull_type_id INT UNSIGNED NOT NULL,
    doctrine_fit_id INT UNSIGNED DEFAULT NULL,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
    doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
    loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    victim_count INT UNSIGNED NOT NULL DEFAULT 0,
    killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_killmail_hull_loss_1d_dimensions (bucket_start, hull_type_id, doctrine_fit_key, doctrine_group_key),
    KEY idx_killmail_hull_loss_1d_bucket (bucket_start),
    KEY idx_killmail_hull_loss_1d_hull_bucket (hull_type_id, bucket_start),
    KEY idx_killmail_hull_loss_1d_group_bucket (doctrine_group_id, bucket_start),
    KEY idx_killmail_hull_loss_1d_fit_bucket (doctrine_fit_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS killmail_doctrine_activity_1d (
    bucket_start DATE NOT NULL,
    doctrine_fit_id INT UNSIGNED DEFAULT NULL,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    hull_type_id INT UNSIGNED DEFAULT NULL,
    doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
    doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
    hull_type_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED,
    loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    quantity_lost BIGINT UNSIGNED NOT NULL DEFAULT 0,
    victim_count INT UNSIGNED NOT NULL DEFAULT 0,
    killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_killmail_doctrine_activity_1d_dimensions (bucket_start, doctrine_fit_key, doctrine_group_key, hull_type_key),
    KEY idx_killmail_doctrine_activity_1d_bucket_fit (bucket_start, doctrine_fit_id),
    KEY idx_killmail_doctrine_activity_1d_bucket_group (bucket_start, doctrine_group_id),
    KEY idx_killmail_doctrine_activity_1d_bucket (bucket_start),
    KEY idx_killmail_doctrine_activity_1d_group_bucket (doctrine_group_id, bucket_start),
    KEY idx_killmail_doctrine_activity_1d_fit_bucket (doctrine_fit_id, bucket_start),
    KEY idx_killmail_doctrine_activity_1d_hull_bucket (hull_type_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_item_stock_1h (
    bucket_start DATETIME NOT NULL,
    source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    stock_units_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    local_stock_units BIGINT NOT NULL DEFAULT 0,
    listing_count INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, source_type, source_id, type_id),
    KEY idx_market_item_stock_1h_bucket_type (bucket_start, type_id),
    KEY idx_market_item_stock_1h_type_bucket (type_id, bucket_start),
    KEY idx_market_item_stock_1h_bucket (bucket_start),
    KEY idx_market_item_stock_1h_source_bucket (source_type, source_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_item_stock_1d (
    bucket_start DATE NOT NULL,
    source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    stock_units_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    local_stock_units BIGINT NOT NULL DEFAULT 0,
    listing_count INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, source_type, source_id, type_id),
    KEY idx_market_item_stock_1d_bucket_type (bucket_start, type_id),
    KEY idx_market_item_stock_1d_type_bucket (type_id, bucket_start),
    KEY idx_market_item_stock_1d_bucket (bucket_start),
    KEY idx_market_item_stock_1d_source_bucket (source_type, source_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_item_price_1h (
    bucket_start DATETIME NOT NULL,
    source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    avg_price_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    weighted_price_numerator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
    weighted_price_denominator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
    listing_count INT UNSIGNED NOT NULL DEFAULT 0,
    min_price DECIMAL(20, 2) DEFAULT NULL,
    max_price DECIMAL(20, 2) DEFAULT NULL,
    avg_price DECIMAL(20, 2) DEFAULT NULL,
    weighted_price DECIMAL(20, 2) DEFAULT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, source_type, source_id, type_id),
    KEY idx_market_item_price_1h_bucket_type (bucket_start, type_id),
    KEY idx_market_item_price_1h_type_bucket (type_id, bucket_start),
    KEY idx_market_item_price_1h_bucket (bucket_start),
    KEY idx_market_item_price_1h_source_bucket (source_type, source_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS market_item_price_1d (
    bucket_start DATE NOT NULL,
    source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
    source_id BIGINT UNSIGNED NOT NULL,
    type_id INT UNSIGNED NOT NULL,
    sample_count INT UNSIGNED NOT NULL DEFAULT 0,
    listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    avg_price_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
    weighted_price_numerator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
    weighted_price_denominator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
    listing_count INT UNSIGNED NOT NULL DEFAULT 0,
    min_price DECIMAL(20, 2) DEFAULT NULL,
    max_price DECIMAL(20, 2) DEFAULT NULL,
    avg_price DECIMAL(20, 2) DEFAULT NULL,
    weighted_price DECIMAL(20, 2) DEFAULT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, source_type, source_id, type_id),
    KEY idx_market_item_price_1d_bucket_type (bucket_start, type_id),
    KEY idx_market_item_price_1d_type_bucket (type_id, bucket_start),
    KEY idx_market_item_price_1d_bucket (bucket_start),
    KEY idx_market_item_price_1d_source_bucket (source_type, source_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_item_stock_1d (
    bucket_start DATE NOT NULL,
    fit_id INT UNSIGNED NOT NULL,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    type_id INT UNSIGNED NOT NULL,
    required_units INT UNSIGNED NOT NULL DEFAULT 0,
    local_stock_units BIGINT NOT NULL DEFAULT 0,
    complete_fits_supported INT UNSIGNED NOT NULL DEFAULT 0,
    fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, fit_id, type_id),
    KEY idx_doctrine_item_stock_1d_group_bucket (doctrine_group_id, bucket_start),
    KEY idx_doctrine_item_stock_1d_type_bucket (type_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_fit_activity_1d (
    bucket_start DATE NOT NULL,
    fit_id INT UNSIGNED NOT NULL,
    hull_type_id INT UNSIGNED DEFAULT NULL,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    hull_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    doctrine_item_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
    target_fits INT UNSIGNED NOT NULL DEFAULT 0,
    fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
    readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
    resupply_pressure VARCHAR(64) NOT NULL DEFAULT 'stable',
    priority_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, fit_id),
    KEY idx_doctrine_fit_activity_1d_bucket_fit (bucket_start, fit_id),
    KEY idx_doctrine_fit_activity_1d_bucket_group (bucket_start, doctrine_group_id),
    KEY idx_doctrine_fit_activity_1d_bucket (bucket_start),
    KEY idx_doctrine_fit_activity_1d_group_bucket (doctrine_group_id, bucket_start),
    KEY idx_doctrine_fit_activity_1d_hull_bucket (hull_type_id, bucket_start),
    KEY idx_doctrine_fit_activity_1d_priority (priority_score, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_group_activity_1d (
    bucket_start DATE NOT NULL,
    group_id INT UNSIGNED NOT NULL,
    hull_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    doctrine_item_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
    complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
    target_fits INT UNSIGNED NOT NULL DEFAULT 0,
    fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
    readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
    resupply_pressure VARCHAR(64) NOT NULL DEFAULT 'stable',
    priority_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, group_id),
    KEY idx_doctrine_group_activity_1d_bucket_group (bucket_start, group_id),
    KEY idx_doctrine_group_activity_1d_bucket (bucket_start),
    KEY idx_doctrine_group_activity_1d_priority (priority_score, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS doctrine_fit_stock_pressure_1d (
    bucket_start DATE NOT NULL,
    fit_id INT UNSIGNED NOT NULL,
    doctrine_group_id INT UNSIGNED DEFAULT NULL,
    complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
    target_fits INT UNSIGNED NOT NULL DEFAULT 0,
    fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
    readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
    resupply_pressure VARCHAR(64) NOT NULL DEFAULT 'stable',
    bottleneck_type_id INT UNSIGNED DEFAULT NULL,
    bottleneck_quantity INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, fit_id),
    KEY idx_doctrine_fit_stock_pressure_1d_group_bucket (doctrine_group_id, bucket_start),
    KEY idx_doctrine_fit_stock_pressure_1d_bottleneck (bottleneck_type_id, bucket_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO trading_stations (station_name, station_type) VALUES
    ('Rens VI - Moon 8 - Brutor Tribe Treasury', 'market'),
    ('Amarr VIII (Oris) - Emperor Family Academy', 'market'),
    ('Dodixie IX - Moon 20 - Federation Navy Assembly Plant', 'market'),
    ('1DQ1-A Keepstar', 'alliance'),
    ('T5ZI-S Fortizar', 'alliance'),
    ('GE-8JV Sotiyo', 'alliance')
ON DUPLICATE KEY UPDATE station_name = VALUES(station_name);

INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('app_name', 'SupplyCore'),
    ('brand_family_name', 'SupplyCore'),
    ('brand_console_label', 'SupplyCore Console'),
    ('brand_tagline', 'Alliance logistics intelligence platform'),
    ('brand_logo_path', '/assets/branding/supplycore-logo.svg'),
    ('brand_favicon_path', '/assets/branding/supplycore-favicon.svg'),
    ('app_timezone', 'UTC'),
    ('default_currency', 'ISK'),
    ('scheduler_operational_profile', 'medium'),
    ('incremental_updates_enabled', '1'),
    ('incremental_strategy', 'watermark_upsert'),
    ('incremental_delete_policy', 'reconcile'),
    ('incremental_chunk_size', '1000'),
    ('static_data_source_url', 'https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip'),
    ('alliance_current_backfill_start_date', ''),
    ('alliance_history_backfill_start_date', ''),
    ('hub_history_backfill_start_date', ''),
    ('killmail_ingestion_enabled', '0'),
    ('killmail_r2z2_sequence_url', 'https://r2z2.zkillboard.com/ephemeral/sequence.json'),
    ('killmail_r2z2_base_url', 'https://r2z2.zkillboard.com/ephemeral'),
    ('killmail_ingestion_poll_sleep_seconds', '6'),
    ('killmail_ingestion_max_sequences_per_run', '120'),
    ('killmail_demand_prediction_mode', 'baseline'),
    ('analytics_bucket_1h_retention_days', '14'),
    ('analytics_bucket_1d_retention_days', '180'),
    ('analytics_bucket_max_runtime_seconds', '15'),
    ('analytics_bucket_killmail_max_rows_per_run', '1000'),
    ('analytics_bucket_market_max_rows_per_run', '1000'),
    ('analytics_bucket_doctrine_rollup_max_rows_per_run', '500'),
    ('analytics_bucket_cache_ttl_seconds', '300'),
    ('raw_order_snapshot_retention_days', '30'),
    ('market_compare_deviation_percent', '5'),
    ('market_compare_min_alliance_sell_volume', '50'),
    ('market_compare_min_alliance_sell_orders', '3'),
    ('item_scope_mode', 'allow_list'),
    ('item_scope_operational_category_keys', '["ships","modules","rigs","ammo_charges","drones_fighters","fuel_structures","boosters"]'),
    ('item_scope_tier_meta_group_ids', '[1,2]'),
    ('item_scope_noise_filter_keys', '["exclude_commodities_consumer_goods","exclude_civilian_items","exclude_blueprints","exclude_skins","exclude_non_market_mission_items"]'),
    ('item_scope_include_category_ids', '[]'),
    ('item_scope_exclude_category_ids', '[]'),
    ('item_scope_include_group_ids', '[]'),
    ('item_scope_exclude_group_ids', '[]'),
    ('item_scope_include_market_group_ids', '[]'),
    ('item_scope_exclude_market_group_ids', '[]'),
    ('item_scope_include_meta_group_ids', '[]'),
    ('item_scope_exclude_meta_group_ids', '[]'),
    ('item_scope_include_type_ids', '[]'),
    ('item_scope_exclude_type_ids', '[]'),
    ('sync_automation_enabled_since', ''),
    ('esi_enabled', '0'),
    ('esi_client_id', '961316f6177d4a0283fef0bd72fbd224'),
    ('esi_client_secret', 'eat_iasVmhqov40Ud568JVAyctOErv5E6AgV_3S6eiZ'),
    ('esi_callback_url', 'http://192.168.178.47/callback'),
    ('esi_scopes', 'publicData esi-location.read_location.v1 esi-search.search_structures.v1 esi-universe.read_structures.v1 esi-markets.structure_markets.v1'),
    ('ollama_enabled', '0'),
    ('ollama_url', 'http://localhost:11434/api'),
    ('ollama_model', 'qwen2.5:1.5b-instruct'),
    ('ollama_timeout', '20'),
    ('ollama_capability_tier', 'auto'),
    ('doctrine.default_group', 'SupplyCore Doctrine')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);

INSERT INTO doctrine_groups (group_name, description) VALUES
    ('SupplyCore Doctrine', 'Baseline doctrine fits used for gap detection, restock generation, and hauling prep.')
ON DUPLICATE KEY UPDATE description = VALUES(description);

INSERT INTO sync_schedules (
    job_key,
    enabled,
    interval_minutes,
    interval_seconds,
    offset_seconds,
    offset_minutes,
    priority,
    concurrency_policy,
    timeout_seconds,
    next_run_at,
    next_due_at,
    current_state,
    tuning_mode,
    discovered_from_code,
    explicitly_configured,
    last_run_at,
    last_status,
    last_error,
    locked_until
) VALUES
    ('market_hub_current_sync', 1, 8, 480, 0, 0, 'high', 'single', 240, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('alliance_current_sync', 1, 4, 240, 120, 2, 'medium', 'single', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('current_state_refresh_sync', 1, 12, 720, 360, 6, 'medium', 'single', 120, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('market_hub_local_history_sync', 1, 20, 1200, 840, 14, 'normal', 'background', 1800, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('doctrine_intelligence_sync', 1, 15, 900, 480, 8, 'normal', 'single', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('market_comparison_summary_sync', 1, 15, 900, 540, 9, 'normal', 'single', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('loss_demand_summary_sync', 1, 15, 900, 600, 10, 'normal', 'single', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('dashboard_summary_sync', 1, 15, 900, 660, 11, 'normal', 'single', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('rebuild_ai_briefings', 1, 20, 1200, 720, 12, 'normal', 'background', 300, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('killmail_r2z2_sync', 1, 3, 180, 180, 3, 'highest', 'single', 90, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('alliance_historical_sync', 1, 360, 21600, 300, 5, 'normal', 'background', 3600, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('market_hub_historical_sync', 1, 360, 21600, 0, 0, 'normal', 'background', 3600, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('forecasting_ai_sync', 1, 60, 3600, 0, 0, 'normal', 'background', 300, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('deal_alerts_sync', 1, 5, 300, 60, 1, 'high', 'single', 90, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 1, NULL, NULL, NULL, NULL),
    ('activity_priority_summary_sync', 1, 15, 900, 780, 13, 'normal', 'single', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 0, NULL, NULL, NULL, NULL),
    ('analytics_bucket_1h_sync', 1, 15, 900, 900, 15, 'normal', 'single', 180, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 0, NULL, NULL, NULL, NULL),
    ('analytics_bucket_1d_sync', 1, 60, 3600, 960, 16, 'normal', 'single', 240, UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic', 1, 0, NULL, NULL, NULL, NULL)
ON DUPLICATE KEY UPDATE
    enabled = VALUES(enabled),
    interval_minutes = VALUES(interval_minutes),
    interval_seconds = VALUES(interval_seconds),
    offset_seconds = VALUES(offset_seconds),
    offset_minutes = VALUES(offset_minutes),
    priority = VALUES(priority),
    concurrency_policy = VALUES(concurrency_policy),
    timeout_seconds = VALUES(timeout_seconds),
    next_due_at = COALESCE(sync_schedules.next_due_at, VALUES(next_due_at)),
    discovered_from_code = VALUES(discovered_from_code),
    explicitly_configured = VALUES(explicitly_configured);

INSERT INTO esi_cache_namespaces (namespace_key, source_system, description) VALUES
    ('cache.esi.controlTowerResources', 'esi', 'ESI cache namespace mapped to controlTowerResources.jsonl'),
    ('cache.esi.npcStations', 'esi', 'ESI cache namespace mapped to npcStations.jsonl'),
    ('cache.esi.mapMoons', 'esi', 'ESI cache namespace mapped to mapMoons.jsonl'),
    ('cache.esi.dogmaAttributes', 'esi', 'ESI cache namespace mapped to dogmaAttributes.jsonl'),
    ('cache.esi.certificates', 'esi', 'ESI cache namespace mapped to certificates.jsonl'),
    ('cache.esi._sde', 'esi', 'ESI cache namespace mapped to _sde.jsonl'),
    ('cache.esi.stationServices', 'esi', 'ESI cache namespace mapped to stationServices.jsonl'),
    ('cache.esi.categories', 'esi', 'ESI cache namespace mapped to categories.jsonl'),
    ('cache.esi.mapRegions', 'esi', 'ESI cache namespace mapped to mapRegions.jsonl'),
    ('cache.esi.mapConstellations', 'esi', 'ESI cache namespace mapped to mapConstellations.jsonl'),
    ('cache.esi.skins', 'esi', 'ESI cache namespace mapped to skins.jsonl'),
    ('cache.esi.marketGroups', 'esi', 'ESI cache namespace mapped to marketGroups.jsonl'),
    ('cache.esi.skinLicenses', 'esi', 'ESI cache namespace mapped to skinLicenses.jsonl'),
    ('cache.esi.masteries', 'esi', 'ESI cache namespace mapped to masteries.jsonl'),
    ('cache.esi.bloodlines', 'esi', 'ESI cache namespace mapped to bloodlines.jsonl'),
    ('cache.esi.metaGroups', 'esi', 'ESI cache namespace mapped to metaGroups.jsonl'),
    ('cache.esi.mapPlanets', 'esi', 'ESI cache namespace mapped to mapPlanets.jsonl'),
    ('cache.esi.corporationActivities', 'esi', 'ESI cache namespace mapped to corporationActivities.jsonl'),
    ('cache.esi.characterAttributes', 'esi', 'ESI cache namespace mapped to characterAttributes.jsonl'),
    ('cache.esi.blueprints', 'esi', 'ESI cache namespace mapped to blueprints.jsonl'),
    ('cache.esi.mapAsteroidBelts', 'esi', 'ESI cache namespace mapped to mapAsteroidBelts.jsonl'),
    ('cache.esi.skinMaterials', 'esi', 'ESI cache namespace mapped to skinMaterials.jsonl'),
    ('cache.esi.ancestries', 'esi', 'ESI cache namespace mapped to ancestries.jsonl'),
    ('cache.esi.types', 'esi', 'ESI cache namespace mapped to types.jsonl'),
    ('cache.esi.landmarks', 'esi', 'ESI cache namespace mapped to landmarks.jsonl'),
    ('cache.esi.mercenaryTacticalOperations', 'esi', 'ESI cache namespace mapped to mercenaryTacticalOperations.jsonl'),
    ('cache.esi.agentTypes', 'esi', 'ESI cache namespace mapped to agentTypes.jsonl'),
    ('cache.esi.agentsInSpace', 'esi', 'ESI cache namespace mapped to agentsInSpace.jsonl'),
    ('cache.esi.stationOperations', 'esi', 'ESI cache namespace mapped to stationOperations.jsonl'),
    ('cache.esi.typeBonus', 'esi', 'ESI cache namespace mapped to typeBonus.jsonl'),
    ('cache.esi.dogmaEffects', 'esi', 'ESI cache namespace mapped to dogmaEffects.jsonl'),
    ('cache.esi.mapStargates', 'esi', 'ESI cache namespace mapped to mapStargates.jsonl'),
    ('cache.esi.typeDogma', 'esi', 'ESI cache namespace mapped to typeDogma.jsonl'),
    ('cache.esi.dogmaUnits', 'esi', 'ESI cache namespace mapped to dogmaUnits.jsonl'),
    ('cache.esi.cloneGrades', 'esi', 'ESI cache namespace mapped to cloneGrades.jsonl'),
    ('cache.esi.typeMaterials', 'esi', 'ESI cache namespace mapped to typeMaterials.jsonl'),
    ('cache.esi.npcCorporationDivisions', 'esi', 'ESI cache namespace mapped to npcCorporationDivisions.jsonl'),
    ('cache.esi.planetSchematics', 'esi', 'ESI cache namespace mapped to planetSchematics.jsonl'),
    ('cache.esi.icons', 'esi', 'ESI cache namespace mapped to icons.jsonl'),
    ('cache.esi.contrabandTypes', 'esi', 'ESI cache namespace mapped to contrabandTypes.jsonl'),
    ('cache.esi.graphics', 'esi', 'ESI cache namespace mapped to graphics.jsonl'),
    ('cache.esi.mapSolarSystems', 'esi', 'ESI cache namespace mapped to mapSolarSystems.jsonl'),
    ('cache.esi.sovereigntyUpgrades', 'esi', 'ESI cache namespace mapped to sovereigntyUpgrades.jsonl'),
    ('cache.esi.npcCorporations', 'esi', 'ESI cache namespace mapped to npcCorporations.jsonl'),
    ('cache.esi.factions', 'esi', 'ESI cache namespace mapped to factions.jsonl'),
    ('cache.esi.translationLanguages', 'esi', 'ESI cache namespace mapped to translationLanguages.jsonl'),
    ('cache.esi.dbuffCollections', 'esi', 'ESI cache namespace mapped to dbuffCollections.jsonl'),
    ('cache.esi.compressibleTypes', 'esi', 'ESI cache namespace mapped to compressibleTypes.jsonl'),
    ('cache.esi.freelanceJobSchemas', 'esi', 'ESI cache namespace mapped to freelanceJobSchemas.jsonl'),
    ('cache.esi.npcCharacters', 'esi', 'ESI cache namespace mapped to npcCharacters.jsonl'),
    ('cache.esi.races', 'esi', 'ESI cache namespace mapped to races.jsonl'),
    ('cache.esi.mapSecondarySuns', 'esi', 'ESI cache namespace mapped to mapSecondarySuns.jsonl'),
    ('cache.esi.mapStars', 'esi', 'ESI cache namespace mapped to mapStars.jsonl'),
    ('cache.esi.dogmaAttributeCategories', 'esi', 'ESI cache namespace mapped to dogmaAttributeCategories.jsonl'),
    ('cache.esi.groups', 'esi', 'ESI cache namespace mapped to groups.jsonl'),
    ('cache.esi.dynamicItemAttributes', 'esi', 'ESI cache namespace mapped to dynamicItemAttributes.jsonl'),
    ('cache.esi.planetResources', 'esi', 'ESI cache namespace mapped to planetResources.jsonl'),
    ('cache.esi.structures.search', 'esi', 'Cached ESI alliance-structure search results')
ON DUPLICATE KEY UPDATE description = VALUES(description), source_system = VALUES(source_system);
