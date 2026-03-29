-- ESI Gateway Phase 1: audit and coordination tables
-- Applied by: ESI + Redis + Multi-Store remediation (Phase 1)

-- Durable latest state per ESI endpoint+page.
-- Mirrors the ephemeral Redis metadata cache with a permanent record.
CREATE TABLE IF NOT EXISTS esi_endpoint_state (
    endpoint_key       VARCHAR(255)      NOT NULL,
    method             VARCHAR(10)       NOT NULL DEFAULT 'GET',
    route_template     VARCHAR(255)      NOT NULL DEFAULT '',
    param_signature    VARCHAR(64)       NOT NULL DEFAULT '',
    identity_context   VARCHAR(64)       NOT NULL DEFAULT 'anonymous',
    page_number        SMALLINT UNSIGNED NOT NULL DEFAULT 1,
    etag               VARCHAR(255)      DEFAULT NULL,
    last_modified      VARCHAR(64)       DEFAULT NULL,
    expires_at         DATETIME          DEFAULT NULL,
    last_checked_at    DATETIME          DEFAULT NULL,
    last_success_at    DATETIME          DEFAULT NULL,
    last_status_code   SMALLINT UNSIGNED DEFAULT NULL,
    not_modified_count INT UNSIGNED      NOT NULL DEFAULT 0,
    success_count      INT UNSIGNED      NOT NULL DEFAULT 0,
    error_count        INT UNSIGNED      NOT NULL DEFAULT 0,
    inconsistency_flag TINYINT(1)        NOT NULL DEFAULT 0,
    created_at         TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMP         DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (endpoint_key),
    KEY idx_esi_endpoint_state_expires (expires_at),
    KEY idx_esi_endpoint_state_route   (route_template, page_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Append-only audit log of rate-limit header observations.
CREATE TABLE IF NOT EXISTS esi_rate_limit_observations (
    id                    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    observed_at           DATETIME        NOT NULL,
    ratelimit_group       VARCHAR(64)     NOT NULL,
    identity_context      VARCHAR(64)     NOT NULL DEFAULT 'global',
    x_ratelimit_limit     VARCHAR(32)     DEFAULT NULL,
    x_ratelimit_remaining INT UNSIGNED    DEFAULT NULL,
    x_ratelimit_used      INT UNSIGNED    DEFAULT NULL,
    retry_after_seconds   INT UNSIGNED    DEFAULT NULL,
    status_code           SMALLINT UNSIGNED DEFAULT NULL,
    KEY idx_esi_rate_obs_group_time (ratelimit_group, observed_at),
    KEY idx_esi_rate_obs_time       (observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Pagination consistency events — logged when pages within a single
-- retrieval cycle return mismatched Last-Modified values.
CREATE TABLE IF NOT EXISTS esi_pagination_consistency_events (
    id                             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    endpoint_key                   VARCHAR(255) NOT NULL,
    retrieval_cycle_id             VARCHAR(64)  NOT NULL,
    expected_last_modified         VARCHAR(64)  DEFAULT NULL,
    inconsistent_page_numbers_json JSON         DEFAULT NULL,
    detected_at                    DATETIME     NOT NULL,
    resolution_state               ENUM('detected','retried','accepted','failed')
                                   NOT NULL DEFAULT 'detected',
    retry_count                    TINYINT UNSIGNED NOT NULL DEFAULT 0,
    KEY idx_esi_pag_consistency_endpoint (endpoint_key, detected_at),
    KEY idx_esi_pag_consistency_time     (detected_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
