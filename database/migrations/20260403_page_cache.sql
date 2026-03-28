CREATE TABLE IF NOT EXISTS page_cache (
    cache_key   VARCHAR(120) NOT NULL PRIMARY KEY,
    cache_value LONGTEXT     NOT NULL,
    computed_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_page_cache_expires (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
