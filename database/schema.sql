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
    next_run_at DATETIME DEFAULT NULL,
    last_run_at DATETIME DEFAULT NULL,
    last_status VARCHAR(40) DEFAULT NULL,
    last_error VARCHAR(500) DEFAULT NULL,
    locked_until DATETIME DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_job_key (job_key),
    KEY idx_enabled (enabled),
    KEY idx_next_run_at (next_run_at),
    KEY idx_job_key (job_key)
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

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until) VALUES
    ('alliance_current_sync', 1, 60, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('alliance_historical_sync', 1, 1800, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('market_hub_current_sync', 1, 60, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('current_state_refresh_sync', 1, 300, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('market_hub_historical_sync', 1, 1800, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('market_hub_local_history_sync', 1, 300, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('doctrine_intelligence_sync', 1, 300, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('market_comparison_summary_sync', 1, 300, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('loss_demand_summary_sync', 1, 300, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('dashboard_summary_sync', 1, 300, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('rebuild_ai_briefings', 1, 300, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('forecasting_ai_sync', 1, 3600, UTC_TIMESTAMP(), NULL, NULL, NULL, NULL),
    ('killmail_r2z2_sync', 0, 60, NULL, NULL, NULL, NULL, NULL)
ON DUPLICATE KEY UPDATE
    enabled = VALUES(enabled),
    interval_seconds = VALUES(interval_seconds);

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
