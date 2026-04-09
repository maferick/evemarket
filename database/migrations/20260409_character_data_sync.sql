-- Character Data Sync — new tables and columns for comprehensive character
-- identity, affiliation, corporation history, and per-character killmail ingestion.
--
-- Part of the "corpus-building" pipeline shift: discover characters inline at
-- killmail ingest, bulk-fetch current affiliations via ESI, persist raw corp
-- history, and expand killmail coverage per character.

-- ── 1. Extend esi_character_queue with queue reason tracking ──────────────────

ALTER TABLE esi_character_queue
    ADD COLUMN IF NOT EXISTS first_queue_reason VARCHAR(40) NOT NULL DEFAULT 'periodic_sync' AFTER fetch_status,
    ADD COLUMN IF NOT EXISTS last_queue_reason  VARCHAR(40) NOT NULL DEFAULT 'periodic_sync' AFTER first_queue_reason;


-- ── 2. Raw corporation history per character (from ESI) ──────────────────────

CREATE TABLE IF NOT EXISTS character_corporation_history (
    character_id    BIGINT UNSIGNED NOT NULL,
    corporation_id  BIGINT UNSIGNED NOT NULL,
    record_id       INT UNSIGNED NOT NULL,
    started_at      DATE NOT NULL,
    ended_at        DATE DEFAULT NULL,
    fetched_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (character_id, record_id),
    KEY idx_char_corp_hist_corp (corporation_id),
    KEY idx_char_corp_hist_dates (started_at, ended_at),
    KEY idx_char_corp_hist_current (character_id, ended_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── 3. Current affiliation + refresh control surface ─────────────────────────

CREATE TABLE IF NOT EXISTS character_current_affiliation (
    character_id             BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    corporation_id           BIGINT UNSIGNED NOT NULL,
    alliance_id              BIGINT UNSIGNED DEFAULT NULL,
    faction_id               BIGINT UNSIGNED DEFAULT NULL,
    -- Refresh tracking
    first_seen_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_killmail_at         DATETIME DEFAULT NULL,
    fetched_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Priority tier: 1=hot (seen <24h), 2=warm (seen <30d), 3=cold (inactive)
    refresh_tier             TINYINT UNSIGNED NOT NULL DEFAULT 3,
    -- Change detection (previous values before last affiliation fetch)
    prev_alliance_id         BIGINT UNSIGNED DEFAULT NULL,
    prev_corporation_id      BIGINT UNSIGNED DEFAULT NULL,
    -- Downstream refresh control
    needs_history_refresh    TINYINT(1) NOT NULL DEFAULT 1,
    needs_killmail_refresh   TINYINT(1) NOT NULL DEFAULT 1,
    last_history_refresh_at  DATETIME DEFAULT NULL,
    last_killmail_refresh_at DATETIME DEFAULT NULL,
    -- Error tracking
    refresh_error_count      TINYINT UNSIGNED NOT NULL DEFAULT 0,
    last_refresh_error       VARCHAR(500) DEFAULT NULL,
    KEY idx_char_affil_alliance (alliance_id),
    KEY idx_char_affil_corp (corporation_id),
    KEY idx_char_affil_refresh (refresh_tier, fetched_at),
    KEY idx_char_affil_killmail (last_killmail_at),
    KEY idx_char_affil_needs_history (needs_history_refresh, refresh_tier),
    KEY idx_char_affil_needs_km (needs_killmail_refresh, refresh_tier)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── 4. Per-character killmail fetch queue ─────────────────────────────────────

CREATE TABLE IF NOT EXISTS character_killmail_queue (
    character_id            BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    priority                DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
    priority_reason         VARCHAR(60) NOT NULL DEFAULT 'discovery',
    status                  ENUM('pending','processing','done','error') NOT NULL DEFAULT 'pending',
    mode                    ENUM('incremental','backfill') NOT NULL DEFAULT 'incremental',
    -- Per-character checkpoint state
    last_page_fetched       INT UNSIGNED NOT NULL DEFAULT 0,
    last_killmail_id_seen   BIGINT UNSIGNED DEFAULT NULL,
    last_killmail_at_seen   DATETIME DEFAULT NULL,
    killmails_found         INT UNSIGNED NOT NULL DEFAULT 0,
    backfill_complete       TINYINT(1) NOT NULL DEFAULT 0,
    -- Timestamps
    queued_at               DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_success_at         DATETIME DEFAULT NULL,
    last_incremental_at     DATETIME DEFAULT NULL,
    last_full_backfill_at   DATETIME DEFAULT NULL,
    processed_at            DATETIME DEFAULT NULL,
    last_error              VARCHAR(500) DEFAULT NULL,
    KEY idx_char_km_queue_drain (status, priority DESC, queued_at ASC),
    KEY idx_char_km_queue_mode (mode, status, backfill_complete)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ── 5. Register new scheduler jobs ───────────────────────────────────────────

INSERT INTO sync_schedules (
    job_key, enabled, interval_minutes, interval_seconds, offset_seconds, offset_minutes,
    priority, concurrency_policy, execution_mode, timeout_seconds,
    next_run_at, next_due_at, current_state, tuning_mode,
    discovered_from_code, explicitly_configured,
    last_run_at, last_status, last_error, locked_until
) VALUES (
    'esi_affiliation_sync', 1, 1, 60, 0, 0,
    'normal', 'single', 'python', 300,
    UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic',
    1, 1,
    NULL, NULL, NULL, NULL
)
ON DUPLICATE KEY UPDATE enabled = 1, interval_minutes = 1, interval_seconds = 60, timeout_seconds = 300;

INSERT INTO sync_schedules (
    job_key, enabled, interval_minutes, interval_seconds, offset_seconds, offset_minutes,
    priority, concurrency_policy, execution_mode, timeout_seconds,
    next_run_at, next_due_at, current_state, tuning_mode,
    discovered_from_code, explicitly_configured,
    last_run_at, last_status, last_error, locked_until
) VALUES (
    'character_killmail_sync', 1, 2, 120, 0, 0,
    'normal', 'single', 'python', 1800,
    UTC_TIMESTAMP(), UTC_TIMESTAMP(), 'waiting', 'automatic',
    1, 1,
    NULL, NULL, NULL, NULL
)
ON DUPLICATE KEY UPDATE enabled = 1, interval_minutes = 2, interval_seconds = 120, timeout_seconds = 1800;
