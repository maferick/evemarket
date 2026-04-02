-- Add 'untracked' mail_type for killmails that don't match any tracked entity.
-- These are ingested for alliance relationship graph computation and pruned after 90 days.
ALTER TABLE killmail_events
    MODIFY COLUMN mail_type ENUM('kill','loss','opponent_loss','opponent_kill','third_party','untracked') NOT NULL DEFAULT 'loss';

-- Index for efficient retention cleanup of untracked killmails.
CREATE INDEX idx_killmail_events_untracked_cleanup
    ON killmail_events (mail_type, killmail_time)
    COMMENT 'Fast lookup for 90-day untracked killmail retention cleanup';

-- Composite index for alliance relationship computation:
-- The job scans killmail_attackers by sequence_id and needs alliance_id for GROUP_CONCAT.
-- The existing idx_attacker_alliance covers lookups by alliance_id,
-- but the relationship job joins ON sequence_id and filters on alliance_id > 0.
CREATE INDEX idx_ka_sequence_alliance
    ON killmail_attackers (sequence_id, alliance_id)
    COMMENT 'Optimizes alliance relationship graph co-attacker join pattern';

-- Covering index for victim_alliance_id lookups in hostile edge computation.
CREATE INDEX idx_ke_victim_alliance_region
    ON killmail_events (victim_alliance_id, region_id, killmail_time)
    COMMENT 'Optimizes hostile edge and regional ceasefire detection queries';
