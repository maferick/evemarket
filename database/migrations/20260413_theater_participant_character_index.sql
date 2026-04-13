-- Add a stand-alone index on theater_participants(character_id).
--
-- theater_participants has PRIMARY KEY (theater_id, character_id) plus
-- theater_id-prefixed secondary indexes, but no index where character_id
-- is the leading column. Any query that probes by character_id alone
-- (pilot search, pilot profile stats, theater-participant joins in the
-- pilot-lookup page) therefore forced a full-table scan — which pushed
-- /battle-intelligence/pilot-lookup.php over the PHP execution timeout
-- once theater_participants grew past a few hundred thousand rows.
--
-- (character_id, theater_id) is used instead of a single-column index
-- because the PK prefix makes (theater_id, character_id) joins cheap
-- already, while this mirror lets both join directions use an index
-- and covers character_id → role_proxy / entry_time lookups without
-- hitting the table.

ALTER TABLE theater_participants
    ADD INDEX IF NOT EXISTS idx_theater_participants_character (character_id, theater_id);
