-- Store a full snapshot of all view data when a theater report is locked,
-- so locked reports can be served without any additional DB queries.
-- Cleared on unlock to revert to live queries.

ALTER TABLE theaters
    ADD COLUMN snapshot_data MEDIUMTEXT DEFAULT NULL AFTER ai_summary_at;
