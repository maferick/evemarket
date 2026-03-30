-- Full-history killmail backfill: add 'third_party' mail_type for killmails
-- that don't match any tracked or opponent entity.

ALTER TABLE killmail_events
    MODIFY COLUMN mail_type ENUM('kill','loss','opponent_loss','opponent_kill','third_party') NOT NULL DEFAULT 'loss';
