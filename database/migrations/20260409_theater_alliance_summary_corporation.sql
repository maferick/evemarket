-- Add corporation_id to theater_alliance_summary so that corp-only entities
-- (NPC corps / allianceless player corps) get their own row instead of being
-- lumped under alliance_id = 0.

ALTER TABLE theater_alliance_summary
    ADD COLUMN corporation_id BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER alliance_id,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (theater_id, alliance_id, corporation_id);
