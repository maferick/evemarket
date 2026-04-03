-- Add source column to distinguish ESI-synced vs manually added contacts.
-- Manual contacts are preserved during ESI sync; ESI contacts are refreshed each run.

ALTER TABLE corp_contacts
    ADD COLUMN source ENUM('esi', 'manual') NOT NULL DEFAULT 'esi' AFTER label_ids,
    ADD KEY idx_source (source);
