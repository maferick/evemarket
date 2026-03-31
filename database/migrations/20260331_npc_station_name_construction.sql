-- NPC station name construction support
-- The new official EVE SDE (JSONL format) does not include station names directly.
-- Names must be constructed from: orbit celestial name, NPC corporation name, and station operation name.
-- This migration adds the support tables and extends ref_npc_stations with lookup columns.

-- NPC corporations (for station owner name lookup)
CREATE TABLE IF NOT EXISTS ref_npc_corporations (
    corp_id INT UNSIGNED NOT NULL,
    corp_name VARCHAR(190) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (corp_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Station operations (for operation name suffix lookup)
CREATE TABLE IF NOT EXISTS ref_station_operations (
    operation_id SMALLINT UNSIGNED NOT NULL,
    operation_name VARCHAR(190) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (operation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Celestials: planets and moons extracted from mapSolarSystems SDE data.
-- Used to resolve a station's orbitID to an orbit name (e.g. "Jita IV - Moon 4").
--   celestial_type: 1 = planet, 2 = moon
--   parent_id:      system_id for planets; planet_id for moons
--   celestial_index: planet position in system (for Roman numeral) or moon orbit index
CREATE TABLE IF NOT EXISTS ref_celestials (
    celestial_id INT UNSIGNED NOT NULL,
    celestial_type TINYINT UNSIGNED NOT NULL DEFAULT 1,
    parent_id INT UNSIGNED DEFAULT NULL,
    celestial_index SMALLINT UNSIGNED DEFAULT NULL,
    system_id INT UNSIGNED NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (celestial_id),
    KEY idx_system_id (system_id),
    KEY idx_parent_id (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Extend ref_npc_stations with SDE lookup columns used for name construction
ALTER TABLE ref_npc_stations
    ADD COLUMN IF NOT EXISTS orbit_id INT UNSIGNED DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS owner_id INT UNSIGNED DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS operation_id SMALLINT UNSIGNED DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS use_operation_name TINYINT UNSIGNED NOT NULL DEFAULT 0;
