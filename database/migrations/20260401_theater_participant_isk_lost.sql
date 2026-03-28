-- Add isk_lost column to theater_participants for tracking ISK value of ships lost
-- Previously damage_taken was incorrectly storing ISK values instead of HP damage

ALTER TABLE theater_participants
    ADD COLUMN isk_lost DOUBLE NOT NULL DEFAULT 0 AFTER damage_taken;
