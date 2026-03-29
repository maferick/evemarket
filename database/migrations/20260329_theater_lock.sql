-- Add locked_at column to theaters for locking battle reports
ALTER TABLE theaters
    ADD COLUMN IF NOT EXISTS locked_at DATETIME DEFAULT NULL AFTER ai_summary_at;
