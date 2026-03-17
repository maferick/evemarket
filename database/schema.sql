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

INSERT INTO trading_stations (station_name, station_type) VALUES
    ('Jita IV - Moon 4 - Caldari Navy Assembly Plant', 'market'),
    ('Amarr VIII (Oris) - Emperor Family Academy', 'market'),
    ('Dodixie IX - Moon 20 - Federation Navy Assembly Plant', 'market'),
    ('1DQ1-A Keepstar', 'alliance'),
    ('T5ZI-S Fortizar', 'alliance'),
    ('GE-8JV Sotiyo', 'alliance')
ON DUPLICATE KEY UPDATE station_name = VALUES(station_name);

INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('incremental_updates_enabled', '1'),
    ('esi_enabled', '0'),
    ('esi_client_id', '961316f6177d4a0283fef0bd72fbd224'),
    ('esi_client_secret', 'eat_iasVmhqov40Ud568JVAyctOErv5E6AgV_3S6eiZ'),
    ('esi_callback_url', 'http://192.168.178.47/callback'),
    ('esi_scopes', 'publicData esi-location.read_location.v1 esi-universe.read_structures.v1 esi-markets.structure_markets.v1')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);

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
    ('cache.esi.planetResources', 'esi', 'ESI cache namespace mapped to planetResources.jsonl')
ON DUPLICATE KEY UPDATE description = VALUES(description), source_system = VALUES(source_system);
