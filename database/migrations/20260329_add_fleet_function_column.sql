-- Add primary_fleet_function column to character_suspicion_signals
-- Stores the character's most-used fleet function (e.g. tackle, logistics, mainline_dps)
-- Used for role-aware suspicion score adjustments

SET @col_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 'character_suspicion_signals'
      AND COLUMN_NAME = 'primary_fleet_function'
);

SET @sql = IF(@col_exists = 0,
    'ALTER TABLE character_suspicion_signals ADD COLUMN primary_fleet_function VARCHAR(32) DEFAULT NULL AFTER damage_total',
    'SELECT 1'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
