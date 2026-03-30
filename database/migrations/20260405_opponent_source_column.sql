-- Add source column to opponent tables to distinguish manually-added vs discovered orgs
-- The iterative graph crawl in evewho_alliance_member_sync discovers new corps/alliances
-- from character history and inserts them with source='discovered'.

ALTER TABLE killmail_opponent_alliances
    ADD COLUMN source ENUM('manual','discovered') NOT NULL DEFAULT 'manual' AFTER is_active;

ALTER TABLE killmail_opponent_corporations
    ADD COLUMN source ENUM('manual','discovered') NOT NULL DEFAULT 'manual' AFTER is_active;
