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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_source_order_observed (source_type, source_id, order_id, observed_at),
    KEY idx_market_orders_history_type_observed (source_type, source_id, type_id, observed_at),
    KEY idx_market_orders_history_observed (source_type, source_id, observed_at)
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
    KEY idx_market_history_daily_observed (source_type, source_id, observed_at)
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

CREATE TABLE IF NOT EXISTS ref_item_types (
    type_id INT UNSIGNED PRIMARY KEY,
    group_id INT UNSIGNED NOT NULL,
    market_group_id INT UNSIGNED DEFAULT NULL,
    type_name VARCHAR(255) NOT NULL,
    description TEXT DEFAULT NULL,
    published TINYINT(1) NOT NULL DEFAULT 0,
    volume DECIMAL(20,6) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_group_id (group_id),
    KEY idx_market_group_id (market_group_id),
    KEY idx_published (published)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO trading_stations (station_name, station_type) VALUES
    ('Jita IV - Moon 4 - Caldari Navy Assembly Plant', 'market'),
    ('Amarr VIII (Oris) - Emperor Family Academy', 'market'),
    ('Dodixie IX - Moon 20 - Federation Navy Assembly Plant', 'market'),
    ('1DQ1-A Keepstar', 'alliance'),
    ('T5ZI-S Fortizar', 'alliance'),
    ('GE-8JV Sotiyo', 'alliance')
ON DUPLICATE KEY UPDATE station_name = VALUES(station_name);

INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('app_name', 'EveMarket'),
    ('app_timezone', 'UTC'),
    ('default_currency', 'ISK'),
    ('incremental_updates_enabled', '1'),
    ('incremental_strategy', 'watermark_upsert'),
    ('incremental_delete_policy', 'reconcile'),
    ('incremental_chunk_size', '1000'),
    ('static_data_source_url', 'https://www.everef.net/static-dumps/latest/eve-ref-static.sql.gz'),
    ('alliance_current_backfill_start_date', ''),
    ('alliance_history_backfill_start_date', ''),
    ('hub_history_backfill_start_date', ''),
    ('raw_order_snapshot_retention_days', '30'),
    ('sync_automation_enabled_since', ''),
    ('esi_enabled', '0'),
    ('esi_client_id', '961316f6177d4a0283fef0bd72fbd224'),
    ('esi_client_secret', 'eat_iasVmhqov40Ud568JVAyctOErv5E6AgV_3S6eiZ'),
    ('esi_callback_url', 'http://192.168.178.47/callback'),
    ('esi_scopes', 'publicData esi-location.read_location.v1 esi-search.search_structures.v1 esi-universe.read_structures.v1 esi-markets.structure_markets.v1')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);

INSERT INTO sync_schedules (job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until) VALUES
    ('alliance_current_sync', 1, 300, NULL, NULL, NULL, NULL, NULL),
    ('alliance_historical_sync', 1, 1800, NULL, NULL, NULL, NULL, NULL),
    ('market_hub_historical_sync', 1, 1800, NULL, NULL, NULL, NULL, NULL)
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
