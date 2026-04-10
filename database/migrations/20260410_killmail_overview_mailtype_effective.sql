-- Killmail intelligence overview: composite index for mail_type + time ordering.
--
-- db_killmail_overview_page() filters on mail_type ('loss' by default) and
-- orders by effective_killmail_at DESC, sequence_id DESC. No existing index
-- on killmail_events covers that access pattern, so MySQL falls back to a
-- full scan + filesort. During killmail backload this causes the
-- /killmail-intelligence page to time out.
--
-- This composite index covers both the filter and the order-by. sequence_id
-- is already UNIQUE on the table so no further tiebreak column is needed.

CREATE INDEX IF NOT EXISTS idx_killmail_mailtype_effective_seq
    ON killmail_events (mail_type, effective_killmail_at, sequence_id);
