-- Migration: Add InfluxDB read_mode and write_on_rollup settings
-- These settings control the InfluxDB analytics migration:
--   influxdb.read_mode:      disabled|fallback|preferred|primary
--   influxdb.write_on_rollup: dual-write to InfluxDB during rollup sync jobs
--
-- Existing influxdb.enabled and influxdb.read_enabled settings remain
-- for backward compatibility.  The new read_mode supersedes read_enabled
-- when explicitly set.

INSERT INTO app_settings (setting_key, setting_value, updated_at)
VALUES ('influxdb.read_mode', 'disabled', NOW())
ON DUPLICATE KEY UPDATE updated_at = updated_at;

INSERT INTO app_settings (setting_key, setting_value, updated_at)
VALUES ('influxdb.write_on_rollup', '0', NOW())
ON DUPLICATE KEY UPDATE updated_at = updated_at;
