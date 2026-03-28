-- Add per-ship-type loss breakdown for theater participants.
-- Stores an array of {ship_type_id, count, isk_lost} objects so the UI can
-- display individual hull types lost (instead of a generic +N counter).

ALTER TABLE theater_participants
    ADD COLUMN ships_lost_detail JSON DEFAULT NULL AFTER ship_type_ids;
