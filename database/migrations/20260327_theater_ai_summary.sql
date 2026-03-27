-- Theater AI Summary columns
ALTER TABLE theaters
    ADD COLUMN IF NOT EXISTS ai_summary TEXT DEFAULT NULL AFTER anomaly_score,
    ADD COLUMN IF NOT EXISTS ai_headline VARCHAR(255) DEFAULT NULL AFTER ai_summary,
    ADD COLUMN IF NOT EXISTS ai_verdict VARCHAR(32) DEFAULT NULL AFTER ai_headline,
    ADD COLUMN IF NOT EXISTS ai_summary_model VARCHAR(120) DEFAULT NULL AFTER ai_verdict,
    ADD COLUMN IF NOT EXISTS ai_summary_at DATETIME DEFAULT NULL AFTER ai_summary_model;
