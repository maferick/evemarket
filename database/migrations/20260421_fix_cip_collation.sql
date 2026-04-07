-- ---------------------------------------------------------------------------
-- Fix collation mismatch between CIP tables and existing schema
--
-- CIP tables were created with utf8mb4_unicode_ci but the rest of the
-- database uses the server default (utf8mb4_general_ci).  This causes
-- "Illegal mix of collations" errors on JOINs.
-- ---------------------------------------------------------------------------

ALTER TABLE intelligence_events CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE intelligence_event_history CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE intelligence_event_notes CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE intelligence_event_digests CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE intelligence_signal_definitions CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE character_intelligence_signals CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE character_intelligence_profiles CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE character_intelligence_labels CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE character_intelligence_profile_history CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE intelligence_calibration_snapshots CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE intelligence_compound_definitions CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE character_intelligence_compound_signals CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE compound_analytics_snapshots CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
ALTER TABLE compound_analyst_outcomes CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
