-- Add final-blow attacker alliance/corporation to theater_structure_kills
-- so battle reports can attribute kill ISK credit to the correct side.

ALTER TABLE theater_structure_kills
    ADD COLUMN killer_alliance_id    BIGINT UNSIGNED DEFAULT NULL AFTER side,
    ADD COLUMN killer_corporation_id BIGINT UNSIGNED DEFAULT NULL AFTER killer_alliance_id;
