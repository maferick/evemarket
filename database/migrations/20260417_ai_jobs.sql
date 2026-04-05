-- AI jobs queue
--
-- Stores asynchronous AI generation work (theater battle summaries,
-- opposition daily briefings, etc.) that used to run inline on the web
-- request thread. The web tier now enqueues a row here and returns
-- immediately; bin/ai_briefing_worker.php (run under systemd) drains the
-- queue, calls the configured AI provider (local/RunPod/Claude/Groq), and
-- writes results back here and into the feature tables.
--
-- MariaDB >= 10.6 is required for SELECT ... FOR UPDATE SKIP LOCKED, which
-- the worker uses to claim jobs without contending across worker slots.

CREATE TABLE IF NOT EXISTS ai_jobs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    -- Feature/use case: 'theater_lock', 'opposition_global',
    -- 'opposition_alliance'. Determines which handler the worker invokes
    -- and which provider override setting it consults.
    feature VARCHAR(64) NOT NULL,
    -- Stable key that identifies the target row the job writes back to
    -- (e.g. theater_id, or 'YYYY-MM-DD' for opposition briefings, or
    -- 'YYYY-MM-DD:<alliance_id>' for per-alliance briefings). Used to
    -- deduplicate queued/running jobs for the same target.
    target_key VARCHAR(191) NOT NULL,
    -- Provider actually resolved at enqueue time ('local'|'runpod'|
    -- 'claude'|'groq'). Captured so the worker uses the provider the
    -- operator picked when the job was submitted, even if global settings
    -- change later.
    provider VARCHAR(32) NOT NULL,
    -- Optional model label captured at enqueue time for audit.
    model VARCHAR(120) NOT NULL DEFAULT '',
    status ENUM('queued','running','done','failed','cancelled')
        NOT NULL DEFAULT 'queued',
    priority TINYINT NOT NULL DEFAULT 0,
    attempts INT UNSIGNED NOT NULL DEFAULT 0,
    max_attempts INT UNSIGNED NOT NULL DEFAULT 2,
    -- Feature-specific input payload (e.g. {"theater_id": "..."} or
    -- {"date":"2026-04-17","alliance_id":12345}). Kept explicit so the
    -- worker is self-sufficient and doesn't re-read mutable state.
    payload_json LONGTEXT NOT NULL,
    -- Final result as returned by the AI provider (optional, primarily
    -- for debugging — the normalized result is written into the feature
    -- table by the handler).
    result_json LONGTEXT NULL,
    last_error TEXT NULL,
    -- Lease for crash-safety: worker claims a row by setting status =
    -- 'running', lease_until = NOW() + N, worker_id = slot tag. If a
    -- worker dies mid-job, another worker (or a janitor sweep) can
    -- reclaim rows whose lease_until is in the past.
    lease_until DATETIME NULL,
    worker_id VARCHAR(64) NULL,
    duration_ms INT UNSIGNED NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,
    started_at DATETIME NULL,
    completed_at DATETIME NULL,
    PRIMARY KEY (id),
    -- Worker claim query: WHERE status='queued' ORDER BY priority DESC, id ASC
    KEY idx_ai_jobs_claim (status, priority, id),
    -- Lease reclaim sweep: WHERE status='running' AND lease_until < NOW()
    KEY idx_ai_jobs_lease (status, lease_until),
    -- Dedup lookup: is there already an active job for this target?
    KEY idx_ai_jobs_target (feature, target_key, status),
    KEY idx_ai_jobs_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
