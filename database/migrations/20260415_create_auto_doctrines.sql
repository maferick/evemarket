-- Automatic, killmail-driven doctrines and buy-all.
--
-- Flow:
--   compute_auto_doctrines.py  reads our losses (mail_type='loss'),
--   clusters fits by Jaccard 0.80, and upserts rows here.
--
--   compute_auto_buyall.py  consumes the active doctrines, projects
--   loss_rate × runway_days into item demand, subtracts alliance
--   structure stock, and prices the remainder via hub snapshots.

-- ── Doctrine registry ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS auto_doctrines (
    id                      INT UNSIGNED NOT NULL AUTO_INCREMENT,
    hull_type_id            INT UNSIGNED NOT NULL,
    fingerprint_hash        CHAR(32)     NOT NULL,
    canonical_name          VARCHAR(191) NOT NULL,
    first_seen_at           DATETIME     NOT NULL,
    last_seen_at            DATETIME     NOT NULL,
    loss_count_window       INT UNSIGNED NOT NULL DEFAULT 0,
    loss_count_total        INT UNSIGNED NOT NULL DEFAULT 0,
    window_days             SMALLINT UNSIGNED NOT NULL DEFAULT 30,
    min_losses_threshold    SMALLINT UNSIGNED NOT NULL DEFAULT 5,
    is_active               TINYINT(1)   NOT NULL DEFAULT 0,
    is_hidden               TINYINT(1)   NOT NULL DEFAULT 0,
    is_pinned               TINYINT(1)   NOT NULL DEFAULT 0,
    runway_days_override    SMALLINT UNSIGNED DEFAULT NULL,
    notes                   VARCHAR(500) DEFAULT NULL,
    created_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_auto_doctrine_identity (hull_type_id, fingerprint_hash),
    KEY idx_auto_doctrine_active_pinned (is_active, is_pinned),
    KEY idx_auto_doctrine_last_seen (last_seen_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Canonical modules per doctrine ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS auto_doctrine_modules (
    doctrine_id             INT UNSIGNED NOT NULL,
    type_id                 INT UNSIGNED NOT NULL,
    flag_category           VARCHAR(16)  NOT NULL,
    quantity                SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    observation_frequency   DECIMAL(5,4) NOT NULL DEFAULT 1.0000,
    PRIMARY KEY (doctrine_id, type_id, flag_category),
    KEY idx_auto_doctrine_modules_type (type_id),
    CONSTRAINT fk_auto_doctrine_modules_doctrine
        FOREIGN KEY (doctrine_id) REFERENCES auto_doctrines(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Per-doctrine daily demand rollup (drives the doctrines list UI) ─────
CREATE TABLE IF NOT EXISTS auto_doctrine_fit_demand_1d (
    bucket_start            DATE         NOT NULL,
    doctrine_id             INT UNSIGNED NOT NULL,
    loss_count              INT UNSIGNED NOT NULL DEFAULT 0,
    daily_loss_rate         DECIMAL(8,3) NOT NULL DEFAULT 0,
    target_fits             INT UNSIGNED NOT NULL DEFAULT 0,
    complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
    fit_gap                 INT UNSIGNED NOT NULL DEFAULT 0,
    priority_score          DECIMAL(8,2) NOT NULL DEFAULT 0,
    updated_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (bucket_start, doctrine_id),
    KEY idx_auto_doctrine_demand_doctrine (doctrine_id, bucket_start),
    CONSTRAINT fk_auto_doctrine_demand_doctrine
        FOREIGN KEY (doctrine_id) REFERENCES auto_doctrines(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Buy-all precompute ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS auto_buyall_summary (
    id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    computed_at             DATETIME     NOT NULL,
    doctrine_count          INT UNSIGNED NOT NULL DEFAULT 0,
    total_items             INT UNSIGNED NOT NULL DEFAULT 0,
    total_isk               DECIMAL(20,2) NOT NULL DEFAULT 0,
    total_volume            DECIMAL(20,2) NOT NULL DEFAULT 0,
    hub_snapshot_at         DATETIME     DEFAULT NULL,
    alliance_snapshot_at    DATETIME     DEFAULT NULL,
    payload_json            LONGTEXT     NOT NULL,
    created_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    KEY idx_auto_buyall_computed (computed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS auto_buyall_items (
    summary_id              BIGINT UNSIGNED NOT NULL,
    type_id                 INT UNSIGNED NOT NULL,
    type_name               VARCHAR(191) NOT NULL,
    demand_qty              INT UNSIGNED NOT NULL DEFAULT 0,
    alliance_stock_qty      INT UNSIGNED NOT NULL DEFAULT 0,
    buy_qty                 INT UNSIGNED NOT NULL DEFAULT 0,
    hub_best_sell           DECIMAL(20,2) DEFAULT NULL,
    alliance_best_sell      DECIMAL(20,2) DEFAULT NULL,
    unit_cost               DECIMAL(20,2) DEFAULT NULL,
    unit_volume             DECIMAL(14,4) DEFAULT NULL,
    line_cost               DECIMAL(20,2) NOT NULL DEFAULT 0,
    line_volume             DECIMAL(20,2) NOT NULL DEFAULT 0,
    contributing_doctrine_ids JSON DEFAULT NULL,
    contributing_fit_count  SMALLINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (summary_id, type_id),
    KEY idx_auto_buyall_items_type (type_id),
    CONSTRAINT fk_auto_buyall_items_summary
        FOREIGN KEY (summary_id) REFERENCES auto_buyall_summary(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── Settings defaults (tunable at runtime) ──────────────────────────────
INSERT INTO app_settings (setting_key, setting_value) VALUES
    ('auto_doctrines.window_days',           '30'),
    ('auto_doctrines.min_losses_threshold',  '5'),
    ('auto_doctrines.default_runway_days',   '14'),
    ('auto_doctrines.jaccard_threshold',     '0.80')
ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value);

-- ── Scheduler registration ──────────────────────────────────────────────
-- Detector: every 15 minutes (cheap — reads 30d killmails).
-- Buy-all: chained on success; also runs hourly as a safety net.
INSERT INTO sync_schedules (job_key, enabled, interval_seconds, execution_mode)
VALUES
    ('compute_auto_doctrines', 1, 900,  'python'),
    ('compute_auto_buyall',    1, 3600, 'python')
ON DUPLICATE KEY UPDATE
    interval_seconds = VALUES(interval_seconds),
    execution_mode   = VALUES(execution_mode);
