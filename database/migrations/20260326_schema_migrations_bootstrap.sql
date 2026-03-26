-- Bootstrap: ensure the schema_migrations tracking table exists.
-- This file is self-referential: once applied it records itself.
CREATE TABLE IF NOT EXISTS schema_migrations (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    file_hash CHAR(64) NOT NULL,
    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_ms INT UNSIGNED NOT NULL DEFAULT 0,
    status ENUM('applied','failed') NOT NULL DEFAULT 'applied',
    error_message TEXT DEFAULT NULL,
    UNIQUE KEY uq_schema_migrations_filename (filename)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
