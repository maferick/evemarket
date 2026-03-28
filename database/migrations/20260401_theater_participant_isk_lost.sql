-- Add isk_lost column to theater_participants to separate ISK value of destroyed
-- ships from HP damage taken. Previously damage_taken was storing ISK value;
-- now damage_taken stores actual HP damage and isk_lost stores ISK value.

ALTER TABLE theater_participants
    ADD COLUMN isk_lost DECIMAL(20,2) NOT NULL DEFAULT 0 AFTER damage_taken;
