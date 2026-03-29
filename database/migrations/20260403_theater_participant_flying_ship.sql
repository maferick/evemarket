-- Add flying_ship_type_id to theater_participants.
-- Stores the most commonly flown non-pod ship type across all attacker
-- killmail records, used for display and role classification.

ALTER TABLE theater_participants
    ADD COLUMN flying_ship_type_id INT UNSIGNED DEFAULT NULL AFTER ships_lost_detail;
