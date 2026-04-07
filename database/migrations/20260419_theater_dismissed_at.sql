-- Soft-delete for manual/composed theaters.
-- dismissed_at allows removing theaters from the composed strip without
-- destroying the underlying data (battles, participants, summaries).

ALTER TABLE theaters
    ADD COLUMN dismissed_at DATETIME DEFAULT NULL AFTER locked_at;
