-- Killmail overview performance: covering index for final_blow attacker lookup.
--
-- The overview page query resolves the final-blow attacker for each displayed row
-- via a correlated subquery:
--   SELECT MIN(fb.attacker_index) FROM killmail_attackers fb
--   WHERE fb.sequence_id = ? AND fb.final_blow = 1
--
-- Without this index MySQL range-scans unique_sequence_attacker (sequence_id,
-- attacker_index) for every result row and evaluates final_blow per row.
-- The covering index (sequence_id, final_blow, attacker_index) turns each
-- lookup into a single index seek.

CREATE INDEX IF NOT EXISTS idx_attacker_final_blow
    ON killmail_attackers (sequence_id, final_blow, attacker_index);
