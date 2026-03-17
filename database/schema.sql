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
    ('esi_enabled', '0')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);
