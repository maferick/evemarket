-- Add per-character battle stats columns to theater_participants.
--
-- kills (existing) is repurposed to mean final_kills (canonical unique kill credit).
-- final_kills mirrors kills for explicitness.
-- contributed_kills = listed-on-killmail involvement count (includes zero-damage).
-- isk_killed = ISK value of killmails where the character got final blow.

ALTER TABLE theater_participants
  ADD COLUMN final_kills INT UNSIGNED NOT NULL DEFAULT 0 AFTER kills,
  ADD COLUMN contributed_kills INT UNSIGNED NOT NULL DEFAULT 0 AFTER final_kills,
  ADD COLUMN isk_killed DECIMAL(20,2) NOT NULL DEFAULT 0 AFTER contributed_kills;

ALTER TABLE theater_alliance_summary
  ADD COLUMN total_contributed_kills INT UNSIGNED NOT NULL DEFAULT 0 AFTER total_kills;
