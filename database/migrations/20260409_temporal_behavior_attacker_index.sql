-- Index on killmail_attackers.character_id for temporal_behavior_detection.
--
-- The temporal behavior worker joins killmail_attackers → killmail_events to
-- build per-character timestamp lists.  Without an index on character_id the
-- WHERE character_id IS NOT NULL AND character_id > 0 filter requires a full
-- table scan of the (potentially multi-million row) attackers table.

CREATE INDEX IF NOT EXISTS idx_attacker_character
    ON killmail_attackers (character_id);
