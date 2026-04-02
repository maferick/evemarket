-- Alliance Dossiers: Rebase from battle-only to all-killmail data
--
-- Adds killmail-level activity columns alongside existing battle columns.
-- The dossier job now derives geography, fleet composition, behavior, and
-- trends from ALL killmails (including untracked) rather than only clustered
-- battles.  Battle counts are retained as supplementary metrics.

ALTER TABLE alliance_dossiers
    ADD COLUMN total_killmails INT UNSIGNED NOT NULL DEFAULT 0 AFTER recent_battles,
    ADD COLUMN recent_killmails INT UNSIGNED NOT NULL DEFAULT 0 AFTER total_killmails,
    ADD COLUMN total_isk_destroyed DECIMAL(20,2) NOT NULL DEFAULT 0 AFTER recent_killmails,
    ADD COLUMN recent_isk_destroyed DECIMAL(20,2) NOT NULL DEFAULT 0 AFTER total_isk_destroyed,
    ADD COLUMN active_pilots INT UNSIGNED NOT NULL DEFAULT 0 AFTER recent_isk_destroyed,
    ADD COLUMN recent_active_pilots INT UNSIGNED NOT NULL DEFAULT 0 AFTER active_pilots,
    ADD INDEX idx_alliance_dossier_killmails (recent_killmails DESC),
    ADD INDEX idx_alliance_dossier_isk (total_isk_destroyed DESC);

-- Add attacker-side alliance index on killmail_events for efficient
-- "where did alliance X get kills" geographic queries.
-- The existing idx_ke_victim_alliance_region covers victim lookups;
-- this covers attacker-side via the killmail_attackers join.
CREATE INDEX IF NOT EXISTS idx_ka_alliance_sequence
    ON killmail_attackers (alliance_id, sequence_id)
    COMMENT 'Covers alliance-scoped dossier queries joining to killmail_events';
