<?php

declare(strict_types=1);

function config(string $key = null, mixed $default = null): mixed
{
    static $config;

    if ($config === null) {
        $config = require __DIR__ . '/config/app.php';
    }

    if ($key === null) {
        return $config;
    }

    $segments = explode('.', $key);
    $value = $config;

    foreach ($segments as $segment) {
        if (!is_array($value) || !array_key_exists($segment, $value)) {
            return $default;
        }
        $value = $value[$segment];
    }

    return $value;
}

function db(): PDO
{
    static $pdo = null;
    static $connectionError = null;

    if ($pdo instanceof PDO) {
        return $pdo;
    }

    if ($connectionError !== null) {
        throw new RuntimeException($connectionError);
    }

    $dsn = sprintf(
        'mysql:host=%s;port=%d;dbname=%s;charset=%s',
        config('db.host'),
        config('db.port'),
        config('db.database'),
        config('db.charset')
    );

    try {
        $pdo = new PDO(
            $dsn,
            config('db.username'),
            config('db.password'),
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
            ]
        );
    } catch (Throwable $exception) {
        $connectionError = 'Database connection failed: ' . $exception->getMessage();
        throw new RuntimeException($connectionError, 0, $exception);
    }

    return $pdo;
}

function db_connection_status(): array
{
    try {
        db();

        return ['ok' => true, 'message' => null];
    } catch (Throwable $exception) {
        return ['ok' => false, 'message' => $exception->getMessage()];
    }
}

function &db_query_cache_store_ref(): array
{
    static $store = [
        'request' => [],
        'redis_checked' => false,
        'redis_enabled' => false,
    ];

    return $store;
}

function db_query_cache_key(string $sql, array $params = [], string $namespace = 'default'): string
{
    return 'db-query:' . md5($namespace . "\n" . trim($sql) . "\n" . json_encode($params, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE));
}

function db_query_cache_redis_enabled(): bool
{
    $store = &db_query_cache_store_ref();
    if ($store['redis_checked'] === true) {
        return $store['redis_enabled'] === true;
    }

    $store['redis_checked'] = true;
    $store['redis_enabled'] = function_exists('supplycore_redis_enabled') && supplycore_redis_enabled();

    return $store['redis_enabled'] === true;
}

function db_query_cache_clear(): void
{
    $store = &db_query_cache_store_ref();
    $store['request'] = [];
}

function db_select_cached(string $sql, array $params = [], int $ttlSeconds = 60, string $namespace = 'default'): array
{
    $safeTtl = max(30, min(120, $ttlSeconds));
    $cacheKey = db_query_cache_key($sql, $params, $namespace);
    $now = time();
    $store = &db_query_cache_store_ref();

    if (isset($store['request'][$cacheKey]) && (int) ($store['request'][$cacheKey]['expires_at'] ?? 0) >= $now) {
        return (array) ($store['request'][$cacheKey]['value'] ?? []);
    }

    if (db_query_cache_redis_enabled()) {
        $cached = supplycore_redis_get_json($cacheKey);
        if (is_array($cached) && array_key_exists('rows', $cached) && is_array($cached['rows'])) {
            $store['request'][$cacheKey] = [
                'expires_at' => $now + $safeTtl,
                'value' => $cached['rows'],
            ];

            return $cached['rows'];
        }
    }

    $rows = db_select($sql, $params);
    $store['request'][$cacheKey] = [
        'expires_at' => $now + $safeTtl,
        'value' => $rows,
    ];

    if (db_query_cache_redis_enabled()) {
        supplycore_redis_set_json($cacheKey, ['rows' => $rows], $safeTtl);
    }

    return $rows;
}

function db_select_one_cached(string $sql, array $params = [], int $ttlSeconds = 60, string $namespace = 'default'): ?array
{
    $rows = db_select_cached($sql, $params, $ttlSeconds, $namespace);
    $row = $rows[0] ?? null;

    return is_array($row) ? $row : null;
}

function db_select(string $sql, array $params = []): array
{
    $stmt = db()->prepare($sql);
    $stmt->execute($params);

    return $stmt->fetchAll();
}

function db_select_stream(string $sql, array $params, callable $callback): int
{
    $driverOptions = [];
    if (defined('PDO::MYSQL_ATTR_USE_BUFFERED_QUERY')) {
        $driverOptions[PDO::MYSQL_ATTR_USE_BUFFERED_QUERY] = false;
    }

    $stmt = db()->prepare($sql, $driverOptions);
    $stmt->execute($params);
    $stmt->setFetchMode(PDO::FETCH_ASSOC);

    $rowCount = 0;

    try {
        while (($row = $stmt->fetch()) !== false) {
            $rowCount++;
            $callback($row, $rowCount);
        }
    } finally {
        $stmt->closeCursor();
    }

    return $rowCount;
}

function db_select_stream_batches(string $sql, array $params, callable $callback, ?int $batchSize = null): int
{
    $limit = $batchSize ?? db_incremental_chunk_size();
    $limit = max(1, $limit);
    $batch = [];
    $rowCount = 0;

    db_select_stream($sql, $params, static function (array $row) use (&$batch, $limit, $callback, &$rowCount): void {
        $batch[] = $row;
        $rowCount++;

        if (count($batch) < $limit) {
            return;
        }

        $callback($batch, $rowCount);
        $batch = [];
    });

    if ($batch !== []) {
        $callback($batch, $rowCount);
    }

    unset($batch);
    if (function_exists('gc_collect_cycles')) {
        gc_collect_cycles();
    }

    return $rowCount;
}

function db_select_one(string $sql, array $params = []): ?array
{
    $stmt = db()->prepare($sql);
    $stmt->execute($params);
    $result = $stmt->fetch();

    return $result === false ? null : $result;
}

function db_execute(string $sql, array $params = []): bool
{
    db_query_cache_clear();
    $stmt = db()->prepare($sql);

    return $stmt->execute($params);
}

function db_app_settings_upsert_many(array $settings): bool
{
    foreach ($settings as $key => $value) {
        db_execute(
            'INSERT INTO app_settings (setting_key, setting_value) VALUES (?, ?) ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value), updated_at = CURRENT_TIMESTAMP',
            [(string) $key, (string) $value]
        );
    }

    return true;
}

function db_transaction(callable $callback): mixed
{
    db_query_cache_clear();
    $pdo = db();
    $isRootTransaction = !$pdo->inTransaction();
    $savepoint = 'sp_' . bin2hex(random_bytes(8));

    if ($isRootTransaction) {
        $pdo->beginTransaction();
    } else {
        $pdo->exec("SAVEPOINT {$savepoint}");
    }

    try {
        $result = $callback($pdo);

        if ($isRootTransaction) {
            if (!$pdo->inTransaction()) {
                throw new RuntimeException('The active database transaction ended before db_transaction() could commit. Avoid running implicit-commit SQL inside transactional callbacks.');
            }
            $pdo->commit();
        } else {
            if (!$pdo->inTransaction()) {
                throw new RuntimeException('The active database savepoint ended before db_transaction() could release it. Avoid running implicit-commit SQL inside transactional callbacks.');
            }
            $pdo->exec("RELEASE SAVEPOINT {$savepoint}");
        }

        return $result;
    } catch (Throwable $exception) {
        if ($isRootTransaction && $pdo->inTransaction()) {
            $pdo->rollBack();
        } elseif ($pdo->inTransaction()) {
            $pdo->exec("ROLLBACK TO SAVEPOINT {$savepoint}");
        }

        throw $exception;
    }
}

function db_incremental_chunk_size(): int
{
    $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ? LIMIT 1', ['incremental_chunk_size']);
    $size = (int) ($row['setting_value'] ?? 1000);

    return max(100, min(10000, $size));
}

function db_select_chunk(string $sql, array $params = [], ?int $chunkSize = null, int $offset = 0): array
{
    $limit = $chunkSize ?? db_incremental_chunk_size();
    $limit = max(1, $limit);
    $offset = max(0, $offset);

    return db_select(
        sprintf('%s LIMIT %d OFFSET %d', rtrim($sql), $limit, $offset),
        $params
    );
}

function db_select_all_chunked(string $sql, array $params = [], ?int $chunkSize = null): array
{
    $limit = $chunkSize ?? db_incremental_chunk_size();
    $offset = 0;
    $allRows = [];

    do {
        $rows = db_select_chunk($sql, $params, $limit, $offset);
        $allRows = array_merge($allRows, $rows);
        $offset += $limit;
    } while (count($rows) === $limit);

    return $allRows;
}

function db_validate_identifier(string $identifier): string
{
    if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $identifier)) {
        throw new InvalidArgumentException('Invalid SQL identifier: ' . $identifier);
    }

    return sprintf('`%s`', $identifier);
}

function db_bulk_insert_or_upsert(string $table, array $columns, array $rows, array $upsertColumns = [], ?int $chunkSize = null): int
{
    if ($rows === []) {
        return 0;
    }

    $chunk = $chunkSize ?? db_incremental_chunk_size();
    $chunk = max(1, $chunk);
    $quotedTable = db_validate_identifier($table);
    $quotedColumns = array_map('db_validate_identifier', $columns);
    $columnList = implode(', ', $quotedColumns);
    $rowPlaceholders = '(' . implode(', ', array_fill(0, count($columns), '?')) . ')';
    $upsertSql = '';

    if ($upsertColumns !== []) {
        $updates = array_map(
            static fn (string $column): string => sprintf('%1$s = VALUES(%1$s)', db_validate_identifier($column)),
            $upsertColumns
        );
        $upsertSql = ' ON DUPLICATE KEY UPDATE ' . implode(', ', $updates);
    }

    return db_transaction(function () use ($rows, $columns, $chunk, $quotedTable, $columnList, $rowPlaceholders, $upsertSql): int {
        $written = 0;

        foreach (array_chunk($rows, $chunk) as $chunkRows) {
            $placeholders = implode(', ', array_fill(0, count($chunkRows), $rowPlaceholders));
            $sql = sprintf('INSERT INTO %s (%s) VALUES %s%s', $quotedTable, $columnList, $placeholders, $upsertSql);

            $params = [];
            foreach ($chunkRows as $row) {
                foreach ($columns as $column) {
                    $params[] = $row[$column] ?? null;
                }
            }

            $stmt = db()->prepare($sql);
            $stmt->execute($params);
            $written += count($chunkRows);
        }

        return $written;
    });
}


function db_market_history_retention_specs(): array
{
    return [
        'raw' => [
            'setting' => 'market_history_retention_raw_days',
            'legacy_setting' => 'raw_order_snapshot_retention_days',
            'default' => 30,
            'min' => 1,
            'max' => 3650,
        ],
        'hourly' => [
            'setting' => 'market_history_retention_hourly_days',
            'legacy_setting' => 'analytics_bucket_1h_retention_days',
            'default' => 90,
            'min' => 1,
            'max' => 3650,
        ],
        'daily' => [
            'setting' => 'market_history_retention_daily_days',
            'legacy_setting' => 'analytics_bucket_1d_retention_days',
            'default' => 365,
            'min' => 30,
            'max' => 3650,
        ],
    ];
}

function db_market_history_retention_days(string $tier): int
{
    $specs = db_market_history_retention_specs();
    $safeTier = array_key_exists($tier, $specs) ? $tier : 'raw';
    $spec = $specs[$safeTier];
    $setting = (string) $spec['setting'];
    $legacySetting = (string) $spec['legacy_setting'];
    $default = (int) $spec['default'];
    $min = (int) $spec['min'];
    $max = (int) $spec['max'];

    $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ? LIMIT 1', [$setting]);
    if ($row !== null) {
        $value = (int) ($row['setting_value'] ?? $default);

        return max($min, min($max, $value > 0 ? $value : $default));
    }

    return db_setting_int($legacySetting, $default, $min, $max);
}

function db_time_series_bucket_retention_days(string $resolution): int
{
    $default = $resolution === '1h' ? 14 : 180;
    $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ? LIMIT 1', ['analytics_bucket_' . $resolution . '_retention_days']);
    $days = (int) ($row['setting_value'] ?? $default);

    return max($resolution === '1h' ? 1 : 30, min(3650, $days > 0 ? $days : $default));
}

function db_time_series_job_setting_int(string $settingKey, int $default, int $min, int $max): int
{
    $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ? LIMIT 1', [$settingKey]);
    $value = (int) ($row['setting_value'] ?? $default);

    return max($min, min($max, $value > 0 ? $value : $default));
}

function db_time_series_job_max_runtime_seconds(): int
{
    return db_time_series_job_setting_int('analytics_bucket_max_runtime_seconds', 15, 5, 60);
}

function db_time_series_killmail_max_rows_per_run(): int
{
    return db_time_series_job_setting_int('analytics_bucket_killmail_max_rows_per_run', 1000, 100, 5000);
}

function db_time_series_market_max_rows_per_run(): int
{
    return db_time_series_job_setting_int('analytics_bucket_market_max_rows_per_run', 1000, 100, 5000);
}

function db_time_series_doctrine_rollup_max_rows_per_run(): int
{
    return db_time_series_job_setting_int('analytics_bucket_doctrine_rollup_max_rows_per_run', 500, 50, 5000);
}

function db_time_series_cache_ttl_seconds(): int
{
    return db_time_series_job_setting_int('analytics_bucket_cache_ttl_seconds', 300, 60, 3600);
}

function db_table_has_index(string $table, string $indexName): bool
{
    $row = db_select_one(
        'SELECT 1
         FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name = ?
           AND index_name = ?
         LIMIT 1',
        [$table, $indexName]
    );

    return $row !== null;
}

function db_ensure_table_index(string $table, string $indexName, string $definitionSql): void
{
    if (db_table_has_index($table, $indexName)) {
        return;
    }

    db()->exec(sprintf('ALTER TABLE %s ADD %s', db_validate_identifier($table), $definitionSql));
}

function db_killmail_payload_schema_ensure(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db()->exec("CREATE TABLE IF NOT EXISTS killmail_event_payloads (
        sequence_id BIGINT UNSIGNED NOT NULL,
        killmail_id BIGINT UNSIGNED NOT NULL,
        killmail_hash VARCHAR(128) NOT NULL,
        zkb_json LONGTEXT NOT NULL DEFAULT '{}',
        raw_killmail_json LONGTEXT NOT NULL DEFAULT '{}',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (sequence_id),
        KEY idx_killmail_event_payloads_killmail (killmail_id, killmail_hash),
        CONSTRAINT fk_killmail_event_payloads_sequence
            FOREIGN KEY (sequence_id) REFERENCES killmail_events (sequence_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

    if (db_table_has_index('killmail_event_payloads', 'uniq_killmail_event_payloads_killmail')) {
        db()->exec('ALTER TABLE killmail_event_payloads DROP INDEX uniq_killmail_event_payloads_killmail');
    }

    if (!db_table_has_index('killmail_event_payloads', 'idx_killmail_event_payloads_killmail')) {
        db()->exec('ALTER TABLE killmail_event_payloads ADD KEY idx_killmail_event_payloads_killmail (killmail_id, killmail_hash)');
    }

    if (db_table_has_column('killmail_events', 'zkb_json') && db_table_has_column('killmail_events', 'raw_killmail_json')) {
        db_execute(
            "INSERT INTO killmail_event_payloads (sequence_id, killmail_id, killmail_hash, zkb_json, raw_killmail_json)
             SELECT
                e.sequence_id,
                e.killmail_id,
                e.killmail_hash,
               COALESCE(e.zkb_json, '{}'),
               COALESCE(e.raw_killmail_json, '{}')
             FROM killmail_events e
             LEFT JOIN killmail_event_payloads p ON p.sequence_id = e.sequence_id
             WHERE p.sequence_id IS NULL
               AND (e.zkb_json IS NOT NULL OR e.raw_killmail_json IS NOT NULL)"
        );
    }

    db_killmail_overview_schema_ensure();

    $ensured = true;
}

function db_killmail_overview_schema_ensure(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db()->exec("CREATE TABLE IF NOT EXISTS killmail_event_payloads (
        sequence_id BIGINT UNSIGNED NOT NULL,
        killmail_id BIGINT UNSIGNED NOT NULL,
        killmail_hash VARCHAR(128) NOT NULL,
        zkb_json LONGTEXT NOT NULL DEFAULT '{}',
        raw_killmail_json LONGTEXT NOT NULL DEFAULT '{}',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (sequence_id),
        KEY idx_killmail_event_payloads_killmail (killmail_id, killmail_hash),
        CONSTRAINT fk_killmail_event_payloads_sequence
            FOREIGN KEY (sequence_id) REFERENCES killmail_events (sequence_id)
            ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

    db_ensure_table_column('killmail_events', 'zkb_total_value', 'DECIMAL(20,2) DEFAULT NULL AFTER victim_ship_type_id');
    db_ensure_table_column('killmail_events', 'zkb_points', 'INT UNSIGNED DEFAULT NULL AFTER zkb_total_value');
    db_ensure_table_column('killmail_events', 'zkb_npc', 'TINYINT(1) DEFAULT NULL AFTER zkb_points');
    db_ensure_table_column('killmail_events', 'zkb_solo', 'TINYINT(1) DEFAULT NULL AFTER zkb_npc');
    db_ensure_table_column('killmail_events', 'zkb_awox', 'TINYINT(1) DEFAULT NULL AFTER zkb_solo');

    $legacyEventJsonSql = db_table_has_column('killmail_events', 'zkb_json') ? "NULLIF(e.zkb_json, '')" : 'NULL';
    $zkbJsonSql = "COALESCE(NULLIF(p.zkb_json, ''), {$legacyEventJsonSql}, '{}')";
    db_execute(
        "UPDATE killmail_events e
         LEFT JOIN killmail_event_payloads p ON p.sequence_id = e.sequence_id
         SET
            e.zkb_total_value = CASE
                WHEN JSON_VALID({$zkbJsonSql}) AND JSON_EXTRACT({$zkbJsonSql}, '$.totalValue') IS NOT NULL
                    THEN CAST(JSON_UNQUOTE(JSON_EXTRACT({$zkbJsonSql}, '$.totalValue')) AS DECIMAL(20,2))
                ELSE NULL
            END,
            e.zkb_points = CASE
                WHEN JSON_VALID({$zkbJsonSql}) AND JSON_EXTRACT({$zkbJsonSql}, '$.points') IS NOT NULL
                    THEN CAST(JSON_UNQUOTE(JSON_EXTRACT({$zkbJsonSql}, '$.points')) AS UNSIGNED)
                ELSE NULL
            END,
            e.zkb_npc = CASE
                WHEN JSON_VALID({$zkbJsonSql}) AND JSON_EXTRACT({$zkbJsonSql}, '$.npc') IS NOT NULL
                    THEN IF(LOWER(JSON_UNQUOTE(JSON_EXTRACT({$zkbJsonSql}, '$.npc'))) IN ('1', 'true'), 1, 0)
                ELSE NULL
            END,
            e.zkb_solo = CASE
                WHEN JSON_VALID({$zkbJsonSql}) AND JSON_EXTRACT({$zkbJsonSql}, '$.solo') IS NOT NULL
                    THEN IF(LOWER(JSON_UNQUOTE(JSON_EXTRACT({$zkbJsonSql}, '$.solo'))) IN ('1', 'true'), 1, 0)
                ELSE NULL
            END,
            e.zkb_awox = CASE
                WHEN JSON_VALID({$zkbJsonSql}) AND JSON_EXTRACT({$zkbJsonSql}, '$.awox') IS NOT NULL
                    THEN IF(LOWER(JSON_UNQUOTE(JSON_EXTRACT({$zkbJsonSql}, '$.awox'))) IN ('1', 'true'), 1, 0)
                ELSE NULL
            END
         WHERE e.zkb_total_value IS NULL
           AND e.zkb_points IS NULL
           AND e.zkb_npc IS NULL
           AND e.zkb_solo IS NULL
           AND e.zkb_awox IS NULL
           AND (p.sequence_id IS NOT NULL OR {$legacyEventJsonSql} IS NOT NULL)"
    );

    $ensured = true;
}

function db_killmail_event_has_legacy_payload_columns(): bool
{
    return db_table_has_column('killmail_events', 'zkb_json')
        && db_table_has_column('killmail_events', 'raw_killmail_json');
}

function db_killmail_event_payload_upsert(array $event): bool
{
    db_killmail_payload_schema_ensure();

    return db_execute(
        'INSERT INTO killmail_event_payloads (
            sequence_id,
            killmail_id,
            killmail_hash,
            zkb_json,
            raw_killmail_json
        ) VALUES (?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            killmail_id = VALUES(killmail_id),
            killmail_hash = VALUES(killmail_hash),
            zkb_json = VALUES(zkb_json),
            raw_killmail_json = VALUES(raw_killmail_json),
            updated_at = CURRENT_TIMESTAMP',
        [
            (int) ($event['sequence_id'] ?? 0),
            (int) ($event['killmail_id'] ?? 0),
            (string) ($event['killmail_hash'] ?? ''),
            (string) ($event['zkb_json'] ?? '{}'),
            (string) ($event['raw_killmail_json'] ?? '{}'),
        ]
    );
}

function db_killmail_event_payload_by_sequence(int $sequenceId): ?array
{
    db_killmail_payload_schema_ensure();

    if ($sequenceId <= 0) {
        return null;
    }

    return db_select_one(
        'SELECT sequence_id, killmail_id, killmail_hash, zkb_json, raw_killmail_json, created_at, updated_at
         FROM killmail_event_payloads
         WHERE sequence_id = ?
         LIMIT 1',
        [$sequenceId]
    );
}

function db_setting_int(string $settingKey, int $default, int $min, int $max): int
{
    $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ? LIMIT 1', [$settingKey]);
    $value = (int) ($row['setting_value'] ?? $default);

    return max($min, min($max, $value > 0 ? $value : $default));
}

function db_prune_before_datetime(string $table, string $column, string $cutoff, int $limit = 50000): int
{
    $safeLimit = max(100, min(200000, $limit));
    $sql = sprintf(
        'DELETE FROM %s WHERE %s < ? LIMIT %d',
        db_validate_identifier($table),
        db_validate_identifier($column),
        $safeLimit
    );

    db_execute($sql, [$cutoff]);

    return (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
}

function db_table_has_column(string $table, string $columnName): bool
{
    $row = db_select_one(
        'SELECT 1
         FROM information_schema.columns
         WHERE table_schema = DATABASE()
           AND table_name = ?
           AND column_name = ?
         LIMIT 1',
        [$table, $columnName]
    );

    return $row !== null;
}

function db_ensure_table_column(string $table, string $columnName, string $definitionSql): void
{
    if (db_table_has_column($table, $columnName)) {
        return;
    }

    db()->exec(sprintf(
        'ALTER TABLE %s ADD COLUMN %s %s',
        db_validate_identifier($table),
        db_validate_identifier($columnName),
        $definitionSql
    ));
}

function db_table_has_primary_key(string $table): bool
{
    $row = db_select_one(
        'SELECT 1
         FROM information_schema.table_constraints
         WHERE table_schema = DATABASE()
           AND table_name = ?
           AND constraint_type = \'PRIMARY KEY\'
         LIMIT 1',
        [$table]
    );

    return $row !== null;
}

function db_table_column_is_nullable(string $table, string $columnName): ?bool
{
    $row = db_select_one(
        'SELECT is_nullable
         FROM information_schema.columns
         WHERE table_schema = DATABASE()
           AND table_name = ?
           AND column_name = ?
         LIMIT 1',
        [$table, $columnName]
    );

    if ($row === null) {
        return null;
    }

    return strtoupper((string) ($row['is_nullable'] ?? '')) === 'YES';
}

function db_time_series_nullable_dimension_schema_ensure(
    string $table,
    array $nullableColumns,
    array $generatedColumns,
    string $uniqueIndexName,
    string $uniqueIndexDefinition
): void {
    $needsMigration = db_table_has_primary_key($table) || !db_table_has_index($table, $uniqueIndexName);

    foreach (array_keys($generatedColumns) as $columnName) {
        if (!db_table_has_column($table, $columnName)) {
            $needsMigration = true;
            break;
        }
    }

    if (!$needsMigration) {
        foreach (array_keys($nullableColumns) as $columnName) {
            if (db_table_column_is_nullable($table, $columnName) === false) {
                $needsMigration = true;
                break;
            }
        }
    }

    if (!$needsMigration) {
        return;
    }

    if (db_table_has_primary_key($table)) {
        db()->exec('ALTER TABLE ' . db_validate_identifier($table) . ' DROP PRIMARY KEY');
    }

    foreach ($nullableColumns as $columnName => $definitionSql) {
        db()->exec(sprintf(
            'ALTER TABLE %s MODIFY COLUMN %s %s',
            db_validate_identifier($table),
            db_validate_identifier($columnName),
            $definitionSql
        ));
    }

    foreach ($generatedColumns as $columnName => $definitionSql) {
        db_ensure_table_column($table, $columnName, $definitionSql);
    }

    db_ensure_table_index($table, $uniqueIndexName, $uniqueIndexDefinition);
}

function db_time_series_analytics_ensure_schema(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    doctrine_db_ensure_schema();

    $statements = [
        "CREATE TABLE IF NOT EXISTS killmail_item_loss_1h (
            bucket_start DATETIME NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            doctrine_fit_id INT UNSIGNED DEFAULT NULL,
            doctrine_group_id INT UNSIGNED DEFAULT NULL,
            hull_type_id INT UNSIGNED DEFAULT NULL,
            doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
            doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
            hull_type_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED,
            loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            quantity_lost BIGINT UNSIGNED NOT NULL DEFAULT 0,
            victim_count INT UNSIGNED NOT NULL DEFAULT 0,
            killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_killmail_item_loss_1h_dimensions (bucket_start, type_id, doctrine_fit_key, doctrine_group_key, hull_type_key),
            KEY idx_killmail_item_loss_1h_bucket_type (bucket_start, type_id),
            KEY idx_killmail_item_loss_1h_type_bucket (type_id, bucket_start),
            KEY idx_killmail_item_loss_1h_bucket (bucket_start),
            KEY idx_killmail_item_loss_1h_group_bucket (doctrine_group_id, bucket_start),
            KEY idx_killmail_item_loss_1h_fit_bucket (doctrine_fit_id, bucket_start),
            KEY idx_killmail_item_loss_1h_hull_bucket (hull_type_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS killmail_item_loss_1d (
            bucket_start DATE NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            doctrine_fit_id INT UNSIGNED DEFAULT NULL,
            doctrine_group_id INT UNSIGNED DEFAULT NULL,
            hull_type_id INT UNSIGNED DEFAULT NULL,
            doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
            doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
            hull_type_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED,
            loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            quantity_lost BIGINT UNSIGNED NOT NULL DEFAULT 0,
            victim_count INT UNSIGNED NOT NULL DEFAULT 0,
            killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_killmail_item_loss_1d_dimensions (bucket_start, type_id, doctrine_fit_key, doctrine_group_key, hull_type_key),
            KEY idx_killmail_item_loss_1d_bucket_type (bucket_start, type_id),
            KEY idx_killmail_item_loss_1d_type_bucket (type_id, bucket_start),
            KEY idx_killmail_item_loss_1d_bucket (bucket_start),
            KEY idx_killmail_item_loss_1d_group_bucket (doctrine_group_id, bucket_start),
            KEY idx_killmail_item_loss_1d_fit_bucket (doctrine_fit_id, bucket_start),
            KEY idx_killmail_item_loss_1d_hull_bucket (hull_type_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS killmail_hull_loss_1d (
            bucket_start DATE NOT NULL,
            hull_type_id INT UNSIGNED NOT NULL,
            doctrine_fit_id INT UNSIGNED DEFAULT NULL,
            doctrine_group_id INT UNSIGNED DEFAULT NULL,
            doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
            doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
            loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            victim_count INT UNSIGNED NOT NULL DEFAULT 0,
            killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_killmail_hull_loss_1d_dimensions (bucket_start, hull_type_id, doctrine_fit_key, doctrine_group_key),
            KEY idx_killmail_hull_loss_1d_bucket (bucket_start),
            KEY idx_killmail_hull_loss_1d_hull_bucket (hull_type_id, bucket_start),
            KEY idx_killmail_hull_loss_1d_group_bucket (doctrine_group_id, bucket_start),
            KEY idx_killmail_hull_loss_1d_fit_bucket (doctrine_fit_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS killmail_doctrine_activity_1d (
            bucket_start DATE NOT NULL,
            doctrine_fit_id INT UNSIGNED DEFAULT NULL,
            doctrine_group_id INT UNSIGNED DEFAULT NULL,
            hull_type_id INT UNSIGNED DEFAULT NULL,
            doctrine_fit_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED,
            doctrine_group_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED,
            hull_type_key INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED,
            loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            quantity_lost BIGINT UNSIGNED NOT NULL DEFAULT 0,
            victim_count INT UNSIGNED NOT NULL DEFAULT 0,
            killmail_count INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_killmail_doctrine_activity_1d_dimensions (bucket_start, doctrine_fit_key, doctrine_group_key, hull_type_key),
            KEY idx_killmail_doctrine_activity_1d_bucket_fit (bucket_start, doctrine_fit_id),
            KEY idx_killmail_doctrine_activity_1d_bucket_group (bucket_start, doctrine_group_id),
            KEY idx_killmail_doctrine_activity_1d_bucket (bucket_start),
            KEY idx_killmail_doctrine_activity_1d_group_bucket (doctrine_group_id, bucket_start),
            KEY idx_killmail_doctrine_activity_1d_fit_bucket (doctrine_fit_id, bucket_start),
            KEY idx_killmail_doctrine_activity_1d_hull_bucket (hull_type_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS market_item_stock_1h (
            bucket_start DATETIME NOT NULL,
            source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            stock_units_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            local_stock_units BIGINT NOT NULL DEFAULT 0,
            listing_count INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, source_type, source_id, type_id),
            KEY idx_market_item_stock_1h_bucket_type (bucket_start, type_id),
            KEY idx_market_item_stock_1h_type_bucket (type_id, bucket_start),
            KEY idx_market_item_stock_1h_bucket (bucket_start),
            KEY idx_market_item_stock_1h_source_bucket (source_type, source_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS market_item_stock_1d (
            bucket_start DATE NOT NULL,
            source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            stock_units_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            local_stock_units BIGINT NOT NULL DEFAULT 0,
            listing_count INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, source_type, source_id, type_id),
            KEY idx_market_item_stock_1d_bucket_type (bucket_start, type_id),
            KEY idx_market_item_stock_1d_type_bucket (type_id, bucket_start),
            KEY idx_market_item_stock_1d_bucket (bucket_start),
            KEY idx_market_item_stock_1d_source_bucket (source_type, source_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS market_item_price_1h (
            bucket_start DATETIME NOT NULL,
            source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            avg_price_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            weighted_price_numerator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            weighted_price_denominator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            listing_count INT UNSIGNED NOT NULL DEFAULT 0,
            min_price DECIMAL(20, 2) DEFAULT NULL,
            max_price DECIMAL(20, 2) DEFAULT NULL,
            avg_price DECIMAL(20, 2) DEFAULT NULL,
            weighted_price DECIMAL(20, 2) DEFAULT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, source_type, source_id, type_id),
            KEY idx_market_item_price_1h_bucket_type (bucket_start, type_id),
            KEY idx_market_item_price_1h_type_bucket (type_id, bucket_start),
            KEY idx_market_item_price_1h_bucket (bucket_start),
            KEY idx_market_item_price_1h_source_bucket (source_type, source_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS market_item_price_1d (
            bucket_start DATE NOT NULL,
            source_type ENUM('alliance_structure', 'market_hub') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            listing_count_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            avg_price_sum DECIMAL(20, 2) NOT NULL DEFAULT 0.00,
            weighted_price_numerator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            weighted_price_denominator DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            listing_count INT UNSIGNED NOT NULL DEFAULT 0,
            min_price DECIMAL(20, 2) DEFAULT NULL,
            max_price DECIMAL(20, 2) DEFAULT NULL,
            avg_price DECIMAL(20, 2) DEFAULT NULL,
            weighted_price DECIMAL(20, 2) DEFAULT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, source_type, source_id, type_id),
            KEY idx_market_item_price_1d_bucket_type (bucket_start, type_id),
            KEY idx_market_item_price_1d_type_bucket (type_id, bucket_start),
            KEY idx_market_item_price_1d_bucket (bucket_start),
            KEY idx_market_item_price_1d_source_bucket (source_type, source_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS doctrine_item_stock_1d (
            bucket_start DATE NOT NULL,
            fit_id INT UNSIGNED NOT NULL,
            doctrine_group_id INT UNSIGNED DEFAULT NULL,
            type_id INT UNSIGNED NOT NULL,
            required_units INT UNSIGNED NOT NULL DEFAULT 0,
            local_stock_units BIGINT NOT NULL DEFAULT 0,
            complete_fits_supported INT UNSIGNED NOT NULL DEFAULT 0,
            fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, fit_id, type_id),
            KEY idx_doctrine_item_stock_1d_group_bucket (doctrine_group_id, bucket_start),
            KEY idx_doctrine_item_stock_1d_type_bucket (type_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS doctrine_fit_activity_1d (
            bucket_start DATE NOT NULL,
            fit_id INT UNSIGNED NOT NULL,
            hull_type_id INT UNSIGNED DEFAULT NULL,
            doctrine_group_id INT UNSIGNED DEFAULT NULL,
            hull_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            doctrine_item_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
            target_fits INT UNSIGNED NOT NULL DEFAULT 0,
            fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
            readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
            resupply_pressure VARCHAR(64) NOT NULL DEFAULT 'stable',
            priority_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, fit_id),
            KEY idx_doctrine_fit_activity_1d_bucket_fit (bucket_start, fit_id),
            KEY idx_doctrine_fit_activity_1d_bucket_group (bucket_start, doctrine_group_id),
            KEY idx_doctrine_fit_activity_1d_bucket (bucket_start),
            KEY idx_doctrine_fit_activity_1d_group_bucket (doctrine_group_id, bucket_start),
            KEY idx_doctrine_fit_activity_1d_hull_bucket (hull_type_id, bucket_start),
            KEY idx_doctrine_fit_activity_1d_priority (priority_score, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS doctrine_group_activity_1d (
            bucket_start DATE NOT NULL,
            group_id INT UNSIGNED NOT NULL,
            hull_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            doctrine_item_loss_count INT UNSIGNED NOT NULL DEFAULT 0,
            complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
            target_fits INT UNSIGNED NOT NULL DEFAULT 0,
            fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
            readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
            resupply_pressure VARCHAR(64) NOT NULL DEFAULT 'stable',
            priority_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, group_id),
            KEY idx_doctrine_group_activity_1d_bucket_group (bucket_start, group_id),
            KEY idx_doctrine_group_activity_1d_bucket (bucket_start),
            KEY idx_doctrine_group_activity_1d_priority (priority_score, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
        "CREATE TABLE IF NOT EXISTS doctrine_fit_stock_pressure_1d (
            bucket_start DATE NOT NULL,
            fit_id INT UNSIGNED NOT NULL,
            doctrine_group_id INT UNSIGNED DEFAULT NULL,
            complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
            target_fits INT UNSIGNED NOT NULL DEFAULT 0,
            fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
            readiness_state VARCHAR(32) NOT NULL DEFAULT 'unknown',
            resupply_pressure VARCHAR(64) NOT NULL DEFAULT 'stable',
            bottleneck_type_id INT UNSIGNED DEFAULT NULL,
            bottleneck_quantity INT NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, fit_id),
            KEY idx_doctrine_fit_stock_pressure_1d_group_bucket (doctrine_group_id, bucket_start),
            KEY idx_doctrine_fit_stock_pressure_1d_bottleneck (bottleneck_type_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    ];

    foreach ($statements as $sql) {
        db()->exec($sql);
    }

    $columns = [
        ['market_item_stock_1h', 'sample_count', 'INT UNSIGNED NOT NULL DEFAULT 0'],
        ['market_item_stock_1h', 'stock_units_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_stock_1h', 'listing_count_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_stock_1d', 'sample_count', 'INT UNSIGNED NOT NULL DEFAULT 0'],
        ['market_item_stock_1d', 'stock_units_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_stock_1d', 'listing_count_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1h', 'sample_count', 'INT UNSIGNED NOT NULL DEFAULT 0'],
        ['market_item_price_1h', 'listing_count_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1h', 'avg_price_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1h', 'weighted_price_numerator', 'DECIMAL(24, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1h', 'weighted_price_denominator', 'DECIMAL(24, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1d', 'sample_count', 'INT UNSIGNED NOT NULL DEFAULT 0'],
        ['market_item_price_1d', 'listing_count_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1d', 'avg_price_sum', 'DECIMAL(20, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1d', 'weighted_price_numerator', 'DECIMAL(24, 2) NOT NULL DEFAULT 0.00'],
        ['market_item_price_1d', 'weighted_price_denominator', 'DECIMAL(24, 2) NOT NULL DEFAULT 0.00'],
    ];

    foreach ($columns as [$table, $columnName, $definitionSql]) {
        db_ensure_table_column($table, $columnName, $definitionSql);
    }

    db_time_series_nullable_dimension_schema_ensure(
        'killmail_item_loss_1h',
        [
            'doctrine_fit_id' => 'INT UNSIGNED DEFAULT NULL',
            'doctrine_group_id' => 'INT UNSIGNED DEFAULT NULL',
            'hull_type_id' => 'INT UNSIGNED DEFAULT NULL',
        ],
        [
            'doctrine_fit_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED AFTER hull_type_id',
            'doctrine_group_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED AFTER doctrine_fit_key',
            'hull_type_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED AFTER doctrine_group_key',
        ],
        'uniq_killmail_item_loss_1h_dimensions',
        'UNIQUE KEY uniq_killmail_item_loss_1h_dimensions (bucket_start, type_id, doctrine_fit_key, doctrine_group_key, hull_type_key)'
    );
    db_time_series_nullable_dimension_schema_ensure(
        'killmail_item_loss_1d',
        [
            'doctrine_fit_id' => 'INT UNSIGNED DEFAULT NULL',
            'doctrine_group_id' => 'INT UNSIGNED DEFAULT NULL',
            'hull_type_id' => 'INT UNSIGNED DEFAULT NULL',
        ],
        [
            'doctrine_fit_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED AFTER hull_type_id',
            'doctrine_group_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED AFTER doctrine_fit_key',
            'hull_type_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED AFTER doctrine_group_key',
        ],
        'uniq_killmail_item_loss_1d_dimensions',
        'UNIQUE KEY uniq_killmail_item_loss_1d_dimensions (bucket_start, type_id, doctrine_fit_key, doctrine_group_key, hull_type_key)'
    );
    db_time_series_nullable_dimension_schema_ensure(
        'killmail_hull_loss_1d',
        [
            'doctrine_fit_id' => 'INT UNSIGNED DEFAULT NULL',
            'doctrine_group_id' => 'INT UNSIGNED DEFAULT NULL',
        ],
        [
            'doctrine_fit_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED AFTER doctrine_group_id',
            'doctrine_group_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED AFTER doctrine_fit_key',
        ],
        'uniq_killmail_hull_loss_1d_dimensions',
        'UNIQUE KEY uniq_killmail_hull_loss_1d_dimensions (bucket_start, hull_type_id, doctrine_fit_key, doctrine_group_key)'
    );
    db_time_series_nullable_dimension_schema_ensure(
        'killmail_doctrine_activity_1d',
        [
            'doctrine_fit_id' => 'INT UNSIGNED DEFAULT NULL',
            'doctrine_group_id' => 'INT UNSIGNED DEFAULT NULL',
            'hull_type_id' => 'INT UNSIGNED DEFAULT NULL',
        ],
        [
            'doctrine_fit_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_fit_id, 0)) STORED AFTER hull_type_id',
            'doctrine_group_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(doctrine_group_id, 0)) STORED AFTER doctrine_fit_key',
            'hull_type_key' => 'INT UNSIGNED GENERATED ALWAYS AS (COALESCE(hull_type_id, 0)) STORED AFTER doctrine_group_key',
        ],
        'uniq_killmail_doctrine_activity_1d_dimensions',
        'UNIQUE KEY uniq_killmail_doctrine_activity_1d_dimensions (bucket_start, doctrine_fit_key, doctrine_group_key, hull_type_key)'
    );

    if (!db_table_has_column('killmail_events', 'effective_killmail_at')) {
        db()->exec(
            'ALTER TABLE killmail_events
             ADD COLUMN effective_killmail_at DATETIME GENERATED ALWAYS AS (COALESCE(killmail_time, created_at)) STORED'
        );
    }

    $indexes = [
        ['killmail_events', 'idx_victim_alliance_effective', 'INDEX idx_victim_alliance_effective (victim_alliance_id, effective_killmail_at)'],
        ['killmail_events', 'idx_victim_corporation_effective', 'INDEX idx_victim_corporation_effective (victim_corporation_id, effective_killmail_at)'],
        ['killmail_events', 'idx_killmail_effective_ship', 'INDEX idx_killmail_effective_ship (effective_killmail_at, victim_ship_type_id)'],
        ['killmail_events', 'idx_killmail_ship_effective', 'INDEX idx_killmail_ship_effective (victim_ship_type_id, effective_killmail_at)'],
        ['killmail_item_loss_1h', 'idx_killmail_item_loss_1h_bucket_type', 'INDEX idx_killmail_item_loss_1h_bucket_type (bucket_start, type_id)'],
        ['killmail_item_loss_1h', 'idx_killmail_item_loss_1h_bucket', 'INDEX idx_killmail_item_loss_1h_bucket (bucket_start)'],
        ['killmail_item_loss_1d', 'idx_killmail_item_loss_1d_bucket_type', 'INDEX idx_killmail_item_loss_1d_bucket_type (bucket_start, type_id)'],
        ['killmail_item_loss_1d', 'idx_killmail_item_loss_1d_bucket', 'INDEX idx_killmail_item_loss_1d_bucket (bucket_start)'],
        ['killmail_hull_loss_1d', 'idx_killmail_hull_loss_1d_bucket', 'INDEX idx_killmail_hull_loss_1d_bucket (bucket_start)'],
        ['killmail_doctrine_activity_1d', 'idx_killmail_doctrine_activity_1d_bucket_fit', 'INDEX idx_killmail_doctrine_activity_1d_bucket_fit (bucket_start, doctrine_fit_id)'],
        ['killmail_doctrine_activity_1d', 'idx_killmail_doctrine_activity_1d_bucket_group', 'INDEX idx_killmail_doctrine_activity_1d_bucket_group (bucket_start, doctrine_group_id)'],
        ['killmail_doctrine_activity_1d', 'idx_killmail_doctrine_activity_1d_bucket', 'INDEX idx_killmail_doctrine_activity_1d_bucket (bucket_start)'],
        ['market_item_stock_1h', 'idx_market_item_stock_1h_bucket_type', 'INDEX idx_market_item_stock_1h_bucket_type (bucket_start, type_id)'],
        ['market_item_stock_1h', 'idx_market_item_stock_1h_bucket', 'INDEX idx_market_item_stock_1h_bucket (bucket_start)'],
        ['market_item_stock_1d', 'idx_market_item_stock_1d_bucket_type', 'INDEX idx_market_item_stock_1d_bucket_type (bucket_start, type_id)'],
        ['market_item_stock_1d', 'idx_market_item_stock_1d_bucket', 'INDEX idx_market_item_stock_1d_bucket (bucket_start)'],
        ['market_item_price_1h', 'idx_market_item_price_1h_bucket_type', 'INDEX idx_market_item_price_1h_bucket_type (bucket_start, type_id)'],
        ['market_item_price_1h', 'idx_market_item_price_1h_bucket', 'INDEX idx_market_item_price_1h_bucket (bucket_start)'],
        ['market_item_price_1d', 'idx_market_item_price_1d_bucket_type', 'INDEX idx_market_item_price_1d_bucket_type (bucket_start, type_id)'],
        ['market_item_price_1d', 'idx_market_item_price_1d_bucket', 'INDEX idx_market_item_price_1d_bucket (bucket_start)'],
        ['doctrine_fit_activity_1d', 'idx_doctrine_fit_activity_1d_bucket_fit', 'INDEX idx_doctrine_fit_activity_1d_bucket_fit (bucket_start, fit_id)'],
        ['doctrine_fit_activity_1d', 'idx_doctrine_fit_activity_1d_bucket_group', 'INDEX idx_doctrine_fit_activity_1d_bucket_group (bucket_start, doctrine_group_id)'],
        ['doctrine_fit_activity_1d', 'idx_doctrine_fit_activity_1d_bucket', 'INDEX idx_doctrine_fit_activity_1d_bucket (bucket_start)'],
        ['doctrine_group_activity_1d', 'idx_doctrine_group_activity_1d_bucket_group', 'INDEX idx_doctrine_group_activity_1d_bucket_group (bucket_start, group_id)'],
        ['doctrine_group_activity_1d', 'idx_doctrine_group_activity_1d_bucket', 'INDEX idx_doctrine_group_activity_1d_bucket (bucket_start)'],
    ];

    foreach ($indexes as [$table, $indexName, $definitionSql]) {
        db_ensure_table_index($table, $indexName, $definitionSql);
    }

    $ensured = true;
}

function normalize_to_hour_bucket(string $timestamp): string
{
    $unix = strtotime($timestamp);
    if ($unix === false) {
        return gmdate('Y-m-d H:00:00');
    }

    return gmdate('Y-m-d H:00:00', $unix);
}

function normalize_to_day_bucket(string $timestamp): string
{
    $unix = strtotime($timestamp);
    if ($unix === false) {
        return gmdate('Y-m-d 00:00:00');
    }

    return gmdate('Y-m-d 00:00:00', $unix);
}

function db_time_series_hour_bucket_start(string $timestamp): string
{
    return normalize_to_hour_bucket($timestamp);
}

function db_killmail_item_quantity_sql(): string
{
    return "GREATEST(
        COALESCE(i.quantity_destroyed, 0) + COALESCE(i.quantity_dropped, 0),
        CASE
            WHEN COALESCE(i.quantity_destroyed, 0) + COALESCE(i.quantity_dropped, 0) > 0 THEN 0
            WHEN i.item_role IN ('fitted', 'destroyed', 'dropped') THEN 1
            ELSE 0
        END
    )";
}

function db_time_series_dataset_key(string $jobName, string $resolution = '1h'): string
{
    return 'analytics.time_series.' . $jobName . '.' . ($resolution === '1d' ? '1d' : '1h');
}

function db_time_series_parse_cursor(?string $cursor, string $idField = 'id'): array
{
    $parts = explode('|', trim((string) $cursor), 2);

    return [
        'timestamp' => trim($parts[0] ?? ''),
        'id' => max(0, (int) ($parts[1] ?? 0)),
        'id_field' => $idField,
    ];
}

function db_time_series_cursor_value(string $timestamp, int $id): string
{
    return trim($timestamp) . '|' . max(0, $id);
}

function db_time_series_source_batch(
    string $table,
    string $timestampColumn,
    string $idColumn,
    string $cursor,
    int $limit
): array {
    $parsed = db_time_series_parse_cursor($cursor, $idColumn);
    $safeLimit = max(1, $limit);

    return db_select(
        "SELECT {$idColumn} AS source_id, {$timestampColumn} AS source_ts
         FROM {$table}
         WHERE {$timestampColumn} IS NOT NULL
           AND (
                {$timestampColumn} > ?
                OR ({$timestampColumn} = ? AND {$idColumn} > ?)
           )
         ORDER BY {$timestampColumn} ASC, {$idColumn} ASC
         LIMIT {$safeLimit}",
        [$parsed['timestamp'], $parsed['timestamp'], $parsed['id']]
    );
}

function db_time_series_retention_cleanup(string $resolution = '1h', int $maxRowsPerTable = 5000): array
{
    db_time_series_analytics_ensure_schema();

    $safeResolution = $resolution === '1d' ? '1d' : '1h';
    $safeLimit = max(100, min(20000, $maxRowsPerTable));
    $analyticsRetentionDays = db_time_series_bucket_retention_days($safeResolution);
    $analyticsCutoff = $safeResolution === '1d'
        ? gmdate('Y-m-d', strtotime('-' . $analyticsRetentionDays . ' days'))
        : gmdate('Y-m-d H:00:00', strtotime('-' . $analyticsRetentionDays . ' days'));
    $marketTier = $safeResolution === '1d' ? 'daily' : 'hourly';
    $marketCutoff = db_market_history_tier_cutoff($marketTier);
    $marketRetentionDays = db_market_history_retention_days($marketTier);
    $specs = $safeResolution === '1d'
        ? [
            ['table' => 'killmail_item_loss_1d', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
            ['table' => 'killmail_hull_loss_1d', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
            ['table' => 'killmail_doctrine_activity_1d', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
            ['table' => 'market_item_stock_1d', 'cutoff' => $marketCutoff, 'retention_days' => $marketRetentionDays],
            ['table' => 'market_item_price_1d', 'cutoff' => $marketCutoff, 'retention_days' => $marketRetentionDays],
            ['table' => 'doctrine_item_stock_1d', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
            ['table' => 'doctrine_fit_activity_1d', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
            ['table' => 'doctrine_group_activity_1d', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
            ['table' => 'doctrine_fit_stock_pressure_1d', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
        ]
        : [
            ['table' => 'killmail_item_loss_1h', 'cutoff' => $analyticsCutoff, 'retention_days' => $analyticsRetentionDays],
            ['table' => 'market_item_stock_1h', 'cutoff' => $marketCutoff, 'retention_days' => $marketRetentionDays],
            ['table' => 'market_item_price_1h', 'cutoff' => $marketCutoff, 'retention_days' => $marketRetentionDays],
        ];
    $deleted = [];
    $totalDeleted = 0;

    foreach ($specs as $spec) {
        $table = (string) $spec['table'];
        db_execute(
            'DELETE FROM ' . db_validate_identifier($table) . ' WHERE bucket_start < ? LIMIT ' . $safeLimit,
            [(string) $spec['cutoff']]
        );
        $rows = (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
        $deleted[$table] = [
            'rows_deleted' => $rows,
            'cutoff' => (string) $spec['cutoff'],
            'retention_days' => (int) $spec['retention_days'],
        ];
        $totalDeleted += $rows;
    }

    return [
        'rows_seen' => $totalDeleted,
        'rows_written' => $totalDeleted,
        'deleted_rows' => $deleted,
        'cutoff' => $analyticsCutoff,
    ];
}

function db_time_series_refresh_killmail_item_loss(string $resolution = '1h', int $maxKillmailsPerRun = 1000): array
{
    db_time_series_analytics_ensure_schema();

    $startedAt = microtime(true);
    $safeResolution = $resolution === '1d' ? '1d' : '1h';
    $safeLimit = max(1, min(5000, $maxKillmailsPerRun));
    $datasetKey = db_time_series_dataset_key('killmail', $safeResolution);
    $cursorStart = db_sync_cursor_get($datasetKey) ?? '1970-01-01 00:00:00|0';
    $batch = db_time_series_source_batch('killmail_events', 'effective_killmail_at', 'sequence_id', $cursorStart, $safeLimit);

    if ($batch === []) {
        return [
            'rows_seen' => 0,
            'rows_written' => 0,
            'cursor_end' => $cursorStart,
            'last_processed_timestamp' => db_time_series_parse_cursor($cursorStart)['timestamp'],
            'has_more' => false,
            'duration_ms' => (int) round((microtime(true) - $startedAt) * 1000),
        ];
    }

    $sequenceIds = array_map(static fn (array $row): int => (int) ($row['source_id'] ?? 0), $batch);
    $lastRow = $batch[array_key_last($batch)];
    $cursorEnd = db_time_series_cursor_value((string) ($lastRow['source_ts'] ?? ''), (int) ($lastRow['source_id'] ?? 0));
    $placeholders = implode(',', array_fill(0, count($sequenceIds), '?'));
    $quantitySql = db_killmail_item_quantity_sql();
    $itemTable = $safeResolution === '1d' ? 'killmail_item_loss_1d' : 'killmail_item_loss_1h';
    $itemBucketExpr = $safeResolution === '1d'
        ? 'DATE(e.effective_killmail_at)'
        : "DATE_FORMAT(e.effective_killmail_at, '%Y-%m-%d %H:00:00')";

    $result = db_sync_run_with_state($datasetKey, 'incremental', $cursorStart, static function (int $runId) use (
        $safeResolution,
        $sequenceIds,
        $placeholders,
        $quantitySql,
        $itemTable,
        $itemBucketExpr,
        $cursorEnd,
        $batch,
        $startedAt
    ): array {
        db_execute(
            "INSERT INTO {$itemTable} (
                bucket_start,
                type_id,
                doctrine_fit_id,
                doctrine_group_id,
                hull_type_id,
                loss_count,
                quantity_lost,
                victim_count,
                killmail_count
            )
            SELECT
                {$itemBucketExpr} AS bucket_start,
                i.item_type_id AS type_id,
                df.id AS doctrine_fit_id,
                dfg.doctrine_group_id,
                e.victim_ship_type_id AS hull_type_id,
                COUNT(*) AS loss_count,
                SUM({$quantitySql}) AS quantity_lost,
                COUNT(DISTINCT COALESCE(e.victim_character_id, e.sequence_id)) AS victim_count,
                COUNT(DISTINCT e.sequence_id) AS killmail_count
            FROM killmail_events e
            INNER JOIN killmail_items i ON i.sequence_id = e.sequence_id
            LEFT JOIN doctrine_fit_items dfi ON dfi.type_id = i.item_type_id
            LEFT JOIN doctrine_fits df
                ON df.id = dfi.doctrine_fit_id
               AND (df.ship_type_id IS NULL OR df.ship_type_id = e.victim_ship_type_id)
            LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
            WHERE e.sequence_id IN ({$placeholders})
              AND i.item_type_id IS NOT NULL
            GROUP BY {$itemBucketExpr}, i.item_type_id, df.id, dfg.doctrine_group_id, e.victim_ship_type_id
            ON DUPLICATE KEY UPDATE
                loss_count = loss_count + VALUES(loss_count),
                quantity_lost = quantity_lost + VALUES(quantity_lost),
                victim_count = victim_count + VALUES(victim_count),
                killmail_count = killmail_count + VALUES(killmail_count),
                updated_at = CURRENT_TIMESTAMP",
            $sequenceIds
        );
        $rowsWritten = (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();

        if ($safeResolution === '1d') {
            db_execute(
                "INSERT INTO killmail_hull_loss_1d (
                    bucket_start,
                    hull_type_id,
                    doctrine_fit_id,
                    doctrine_group_id,
                    loss_count,
                    victim_count,
                    killmail_count
                )
                SELECT
                    DATE(e.effective_killmail_at) AS bucket_start,
                    e.victim_ship_type_id AS hull_type_id,
                    df.id AS doctrine_fit_id,
                    dfg.doctrine_group_id,
                    COUNT(*) AS loss_count,
                    COUNT(DISTINCT COALESCE(e.victim_character_id, e.sequence_id)) AS victim_count,
                    COUNT(DISTINCT e.sequence_id) AS killmail_count
                FROM killmail_events e
                LEFT JOIN doctrine_fits df ON df.ship_type_id = e.victim_ship_type_id
                LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
                WHERE e.sequence_id IN ({$placeholders})
                  AND e.victim_ship_type_id IS NOT NULL
                GROUP BY DATE(e.effective_killmail_at), e.victim_ship_type_id, df.id, dfg.doctrine_group_id
                ON DUPLICATE KEY UPDATE
                    loss_count = loss_count + VALUES(loss_count),
                    victim_count = victim_count + VALUES(victim_count),
                    killmail_count = killmail_count + VALUES(killmail_count),
                    updated_at = CURRENT_TIMESTAMP",
                $sequenceIds
            );
            $rowsWritten += (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();

            db_execute(
                "INSERT INTO killmail_doctrine_activity_1d (
                    bucket_start,
                    doctrine_fit_id,
                    doctrine_group_id,
                    hull_type_id,
                    loss_count,
                    quantity_lost,
                    victim_count,
                    killmail_count
                )
                SELECT
                    DATE(e.effective_killmail_at) AS bucket_start,
                    df.id AS doctrine_fit_id,
                    dfg.doctrine_group_id,
                    e.victim_ship_type_id AS hull_type_id,
                    COUNT(*) AS loss_count,
                    SUM({$quantitySql}) AS quantity_lost,
                    COUNT(DISTINCT COALESCE(e.victim_character_id, e.sequence_id)) AS victim_count,
                    COUNT(DISTINCT e.sequence_id) AS killmail_count
                FROM killmail_events e
                INNER JOIN killmail_items i ON i.sequence_id = e.sequence_id
                INNER JOIN doctrine_fit_items dfi ON dfi.type_id = i.item_type_id
                INNER JOIN doctrine_fits df
                    ON df.id = dfi.doctrine_fit_id
                   AND (df.ship_type_id IS NULL OR df.ship_type_id = e.victim_ship_type_id)
                LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
                WHERE e.sequence_id IN ({$placeholders})
                GROUP BY DATE(e.effective_killmail_at), df.id, dfg.doctrine_group_id, e.victim_ship_type_id
                ON DUPLICATE KEY UPDATE
                    loss_count = loss_count + VALUES(loss_count),
                    quantity_lost = quantity_lost + VALUES(quantity_lost),
                    victim_count = victim_count + VALUES(victim_count),
                    killmail_count = killmail_count + VALUES(killmail_count),
                    updated_at = CURRENT_TIMESTAMP",
                $sequenceIds
            );
            $rowsWritten += (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
        }

        return [
            'sync_mode' => 'incremental',
            'source_rows' => count($batch),
            'written_rows' => $rowsWritten,
            'cursor_end' => $cursorEnd,
            'checksum' => hash('sha256', json_encode([$safeResolution, $cursorEnd, count($batch), $rowsWritten], JSON_THROW_ON_ERROR)),
            'last_processed_timestamp' => (string) ($batch[array_key_last($batch)]['source_ts'] ?? ''),
            'duration_ms' => (int) round((microtime(true) - $startedAt) * 1000),
        ];
    });

    return $result + [
        'has_more' => count($batch) >= $safeLimit,
    ];
}

function db_time_series_refresh_market_aggregates(string $resolution = '1h', int $maxRowsPerRun = 1000): array
{
    db_time_series_analytics_ensure_schema();

    $startedAt = microtime(true);
    $safeResolution = $resolution === '1d' ? '1d' : '1h';
    $safeLimit = max(1, min(5000, $maxRowsPerRun));
    $datasetKey = db_time_series_dataset_key('market', $safeResolution);
    $cursorStart = db_sync_cursor_get($datasetKey) ?? '1970-01-01 00:00:00|0';
    $summaryTable = db_market_order_snapshots_summary_read_table();
    $quotedSummaryTable = db_validate_identifier($summaryTable);
    $batch = db_time_series_source_batch($summaryTable, 'observed_at', 'id', $cursorStart, $safeLimit);

    if ($batch === []) {
        return [
            'rows_seen' => 0,
            'rows_written' => 0,
            'cursor_end' => $cursorStart,
            'last_processed_timestamp' => db_time_series_parse_cursor($cursorStart)['timestamp'],
            'has_more' => false,
            'duration_ms' => (int) round((microtime(true) - $startedAt) * 1000),
        ];
    }

    $snapshotIds = array_map(static fn (array $row): int => (int) ($row['source_id'] ?? 0), $batch);
    $lastRow = $batch[array_key_last($batch)];
    $cursorEnd = db_time_series_cursor_value((string) ($lastRow['source_ts'] ?? ''), (int) ($lastRow['source_id'] ?? 0));
    $placeholders = implode(',', array_fill(0, count($snapshotIds), '?'));
    $stockTable = $safeResolution === '1d' ? 'market_item_stock_1d' : 'market_item_stock_1h';
    $priceTable = $safeResolution === '1d' ? 'market_item_price_1d' : 'market_item_price_1h';
    $bucketExpr = $safeResolution === '1d'
        ? 'DATE(moss.observed_at)'
        : "DATE_FORMAT(moss.observed_at, '%Y-%m-%d %H:00:00')";

    $result = db_sync_run_with_state($datasetKey, 'incremental', $cursorStart, static function (int $runId) use (
        $snapshotIds,
        $placeholders,
        $stockTable,
        $priceTable,
        $bucketExpr,
        $cursorEnd,
        $batch,
        $safeResolution,
        $startedAt,
        $quotedSummaryTable
    ): array {
        db_execute(
            "INSERT INTO {$stockTable} (
                bucket_start,
                source_type,
                source_id,
                type_id,
                sample_count,
                stock_units_sum,
                listing_count_sum,
                local_stock_units,
                listing_count
            )
            SELECT
                {$bucketExpr} AS bucket_start,
                moss.source_type,
                moss.source_id,
                moss.type_id,
                COUNT(*) AS sample_count,
                SUM(COALESCE(moss.total_sell_volume, 0)) AS stock_units_sum,
                SUM(COALESCE(moss.sell_order_count, 0)) AS listing_count_sum,
                ROUND(AVG(COALESCE(moss.total_sell_volume, 0))) AS local_stock_units,
                ROUND(AVG(COALESCE(moss.sell_order_count, 0))) AS listing_count
            FROM {$quotedSummaryTable} moss
            WHERE moss.id IN ({$placeholders})
            GROUP BY {$bucketExpr}, moss.source_type, moss.source_id, moss.type_id
            ON DUPLICATE KEY UPDATE
                sample_count = sample_count + VALUES(sample_count),
                stock_units_sum = stock_units_sum + VALUES(stock_units_sum),
                listing_count_sum = listing_count_sum + VALUES(listing_count_sum),
                local_stock_units = ROUND((stock_units_sum + VALUES(stock_units_sum)) / NULLIF(sample_count + VALUES(sample_count), 0)),
                listing_count = ROUND((listing_count_sum + VALUES(listing_count_sum)) / NULLIF(sample_count + VALUES(sample_count), 0)),
                updated_at = CURRENT_TIMESTAMP",
            $snapshotIds
        );
        $rowsWritten = (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();

        db_execute(
            "INSERT INTO {$priceTable} (
                bucket_start,
                source_type,
                source_id,
                type_id,
                sample_count,
                listing_count_sum,
                avg_price_sum,
                weighted_price_numerator,
                weighted_price_denominator,
                listing_count,
                min_price,
                max_price,
                avg_price,
                weighted_price
            )
            SELECT
                {$bucketExpr} AS bucket_start,
                moss.source_type,
                moss.source_id,
                moss.type_id,
                COUNT(*) AS sample_count,
                SUM(COALESCE(moss.sell_order_count, 0)) AS listing_count_sum,
                SUM(COALESCE(moss.best_sell_price, 0)) AS avg_price_sum,
                SUM(COALESCE(moss.best_sell_price, 0) * COALESCE(moss.total_sell_volume, 0)) AS weighted_price_numerator,
                SUM(COALESCE(moss.total_sell_volume, 0)) AS weighted_price_denominator,
                ROUND(AVG(COALESCE(moss.sell_order_count, 0))) AS listing_count,
                MIN(moss.best_sell_price) AS min_price,
                MAX(moss.best_sell_price) AS max_price,
                AVG(moss.best_sell_price) AS avg_price,
                CASE
                    WHEN SUM(COALESCE(moss.total_sell_volume, 0)) > 0 THEN SUM(COALESCE(moss.best_sell_price, 0) * COALESCE(moss.total_sell_volume, 0)) / SUM(COALESCE(moss.total_sell_volume, 0))
                    ELSE AVG(moss.best_sell_price)
                END AS weighted_price
            FROM {$quotedSummaryTable} moss
            WHERE moss.id IN ({$placeholders})
            GROUP BY {$bucketExpr}, moss.source_type, moss.source_id, moss.type_id
            ON DUPLICATE KEY UPDATE
                sample_count = sample_count + VALUES(sample_count),
                listing_count_sum = listing_count_sum + VALUES(listing_count_sum),
                avg_price_sum = avg_price_sum + VALUES(avg_price_sum),
                weighted_price_numerator = weighted_price_numerator + VALUES(weighted_price_numerator),
                weighted_price_denominator = weighted_price_denominator + VALUES(weighted_price_denominator),
                listing_count = ROUND((listing_count_sum + VALUES(listing_count_sum)) / NULLIF(sample_count + VALUES(sample_count), 0)),
                min_price = CASE
                    WHEN min_price IS NULL THEN VALUES(min_price)
                    WHEN VALUES(min_price) IS NULL THEN min_price
                    ELSE LEAST(min_price, VALUES(min_price))
                END,
                max_price = CASE
                    WHEN max_price IS NULL THEN VALUES(max_price)
                    WHEN VALUES(max_price) IS NULL THEN max_price
                    ELSE GREATEST(max_price, VALUES(max_price))
                END,
                avg_price = ROUND((avg_price_sum + VALUES(avg_price_sum)) / NULLIF(sample_count + VALUES(sample_count), 0), 2),
                weighted_price = ROUND(
                    CASE
                        WHEN (weighted_price_denominator + VALUES(weighted_price_denominator)) > 0
                            THEN (weighted_price_numerator + VALUES(weighted_price_numerator)) / (weighted_price_denominator + VALUES(weighted_price_denominator))
                        ELSE (avg_price_sum + VALUES(avg_price_sum)) / NULLIF(sample_count + VALUES(sample_count), 0)
                    END,
                    2
                ),
                updated_at = CURRENT_TIMESTAMP",
            $snapshotIds
        );
        $rowsWritten += (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();

        return [
            'sync_mode' => 'incremental',
            'source_rows' => count($batch),
            'written_rows' => $rowsWritten,
            'cursor_end' => $cursorEnd,
            'checksum' => hash('sha256', json_encode([$safeResolution, $cursorEnd, count($batch), $rowsWritten], JSON_THROW_ON_ERROR)),
            'last_processed_timestamp' => (string) ($batch[array_key_last($batch)]['source_ts'] ?? ''),
            'duration_ms' => (int) round((microtime(true) - $startedAt) * 1000),
        ];
    });

    return $result + [
        'has_more' => count($batch) >= $safeLimit,
    ];
}

function db_time_series_refresh_all(string $resolution = '1h'): array
{
    $safeResolution = $resolution === '1d' ? '1d' : '1h';
    $startedAt = microtime(true);
    $maxRuntimeSeconds = db_time_series_job_max_runtime_seconds();
    $killmailResult = db_time_series_refresh_killmail_item_loss($safeResolution, db_time_series_killmail_max_rows_per_run());
    $budgetExceededAfterKillmail = (microtime(true) - $startedAt) >= $maxRuntimeSeconds;
    $marketResult = $budgetExceededAfterKillmail
        ? [
            'rows_seen' => 0,
            'rows_written' => 0,
            'has_more' => true,
            'skipped' => true,
            'skip_reason' => 'max_runtime_reached_before_market_job',
        ]
        : db_time_series_refresh_market_aggregates($safeResolution, db_time_series_market_max_rows_per_run());
    $cleanupResult = db_time_series_retention_cleanup($safeResolution, db_time_series_doctrine_rollup_max_rows_per_run());

    return [
        'resolution' => $safeResolution,
        'rows_seen' => (int) ($killmailResult['source_rows'] ?? $killmailResult['rows_seen'] ?? 0)
            + (int) ($marketResult['source_rows'] ?? $marketResult['rows_seen'] ?? 0),
        'rows_written' => (int) ($killmailResult['written_rows'] ?? $killmailResult['rows_written'] ?? 0)
            + (int) ($marketResult['written_rows'] ?? $marketResult['rows_written'] ?? 0)
            + (int) ($cleanupResult['rows_written'] ?? 0),
        'meta' => [
            'killmail' => $killmailResult,
            'market' => $marketResult,
            'cleanup' => $cleanupResult,
            'max_runtime_seconds' => $maxRuntimeSeconds,
            'killmail_max_rows_per_run' => db_time_series_killmail_max_rows_per_run(),
            'market_max_rows_per_run' => db_time_series_market_max_rows_per_run(),
            'doctrine_rollup_max_rows_per_run' => db_time_series_doctrine_rollup_max_rows_per_run(),
            'has_more_work' => !empty($killmailResult['has_more']) || !empty($marketResult['has_more']),
        ],
    ];
}

function db_time_series_store_doctrine_daily_rollups(array $fitRows, array $groupRows, array $itemsByFitId = [], array $marketByTypeId = []): void
{
    db_time_series_analytics_ensure_schema();

    $bucketStart = gmdate('Y-m-d');
    $fitRows = array_slice($fitRows, 0, db_time_series_doctrine_rollup_max_rows_per_run());
    $groupRows = array_slice($groupRows, 0, db_time_series_doctrine_rollup_max_rows_per_run());
    $fitActivityRows = [];
    $fitPressureRows = [];
    $itemRows = [];

    foreach ($fitRows as $row) {
        $fitId = (int) ($row['entity_id'] ?? $row['id'] ?? 0);
        if ($fitId <= 0) {
            continue;
        }

        $fitActivityRows[] = [
            'bucket_start' => $bucketStart,
            'fit_id' => $fitId,
            'hull_type_id' => isset($row['ship_type_id']) ? (int) $row['ship_type_id'] : null,
            'doctrine_group_id' => isset($row['doctrine_group_id']) ? (int) $row['doctrine_group_id'] : null,
            'hull_loss_count' => (int) ($row['hull_losses_7d'] ?? 0),
            'doctrine_item_loss_count' => (int) ($row['module_losses_7d'] ?? 0),
            'complete_fits_available' => (int) (($row['supply']['complete_fits_available'] ?? 0)),
            'target_fits' => (int) (($row['supply']['recommended_target_fit_count'] ?? 0)),
            'fit_gap' => (int) ($row['readiness_gap_count'] ?? ($row['supply']['gap_to_target_fit_count'] ?? 0)),
            'readiness_state' => (string) ($row['readiness_state'] ?? 'unknown'),
            'resupply_pressure' => (string) ($row['resupply_pressure_state'] ?? 'stable'),
            'priority_score' => (float) ($row['activity_score'] ?? 0.0),
        ];
        $fitPressureRows[] = [
            'bucket_start' => $bucketStart,
            'fit_id' => $fitId,
            'doctrine_group_id' => isset($row['doctrine_group_id']) ? (int) $row['doctrine_group_id'] : null,
            'complete_fits_available' => (int) (($row['supply']['complete_fits_available'] ?? 0)),
            'target_fits' => (int) (($row['supply']['recommended_target_fit_count'] ?? 0)),
            'fit_gap' => (int) ($row['readiness_gap_count'] ?? ($row['supply']['gap_to_target_fit_count'] ?? 0)),
            'readiness_state' => (string) ($row['readiness_state'] ?? 'unknown'),
            'resupply_pressure' => (string) ($row['resupply_pressure_state'] ?? 'stable'),
            'bottleneck_type_id' => isset($row['supply']['bottleneck_type_id']) ? (int) $row['supply']['bottleneck_type_id'] : null,
            'bottleneck_quantity' => (int) (($row['supply']['bottleneck_quantity'] ?? 0)),
        ];

        foreach ((array) ($itemsByFitId[$fitId] ?? []) as $item) {
            $typeId = (int) ($item['type_id'] ?? 0);
            if ($typeId <= 0) {
                continue;
            }

            $required = max(1, (int) ($item['quantity'] ?? 1));
            $localStock = max(0, (int) (($marketByTypeId[$typeId]['alliance_total_sell_volume'] ?? 0)));
            $completeFitsSupported = intdiv($localStock, $required);
            $targetFits = (int) (($row['supply']['recommended_target_fit_count'] ?? 0));
            $itemRows[] = [
                'bucket_start' => $bucketStart,
                'fit_id' => $fitId,
                'doctrine_group_id' => isset($row['doctrine_group_id']) ? (int) $row['doctrine_group_id'] : null,
                'type_id' => $typeId,
                'required_units' => $required,
                'local_stock_units' => $localStock,
                'complete_fits_supported' => $completeFitsSupported,
                'fit_gap' => max(0, $targetFits - $completeFitsSupported),
            ];
        }
    }

    $groupActivityRows = [];
    foreach ($groupRows as $row) {
        $groupId = (int) ($row['entity_id'] ?? $row['id'] ?? 0);
        if ($groupId <= 0) {
            continue;
        }

        $groupActivityRows[] = [
            'bucket_start' => $bucketStart,
            'group_id' => $groupId,
            'hull_loss_count' => (int) ($row['hull_losses_7d'] ?? 0),
            'doctrine_item_loss_count' => (int) ($row['module_losses_7d'] ?? 0),
            'complete_fits_available' => (int) (($row['complete_fits_available'] ?? 0)),
            'target_fits' => (int) (($row['target_fit_count'] ?? 0)),
            'fit_gap' => (int) ($row['readiness_gap_count'] ?? 0),
            'readiness_state' => (string) ($row['readiness_state'] ?? 'unknown'),
            'resupply_pressure' => (string) ($row['resupply_pressure_state'] ?? 'stable'),
            'priority_score' => (float) ($row['activity_score'] ?? 0.0),
        ];
    }

    db_bulk_insert_or_upsert(
        'doctrine_fit_activity_1d',
        ['bucket_start', 'fit_id', 'hull_type_id', 'doctrine_group_id', 'hull_loss_count', 'doctrine_item_loss_count', 'complete_fits_available', 'target_fits', 'fit_gap', 'readiness_state', 'resupply_pressure', 'priority_score'],
        $fitActivityRows,
        ['hull_type_id', 'doctrine_group_id', 'hull_loss_count', 'doctrine_item_loss_count', 'complete_fits_available', 'target_fits', 'fit_gap', 'readiness_state', 'resupply_pressure', 'priority_score']
    );
    db_bulk_insert_or_upsert(
        'doctrine_group_activity_1d',
        ['bucket_start', 'group_id', 'hull_loss_count', 'doctrine_item_loss_count', 'complete_fits_available', 'target_fits', 'fit_gap', 'readiness_state', 'resupply_pressure', 'priority_score'],
        $groupActivityRows,
        ['hull_loss_count', 'doctrine_item_loss_count', 'complete_fits_available', 'target_fits', 'fit_gap', 'readiness_state', 'resupply_pressure', 'priority_score']
    );
    db_bulk_insert_or_upsert(
        'doctrine_fit_stock_pressure_1d',
        ['bucket_start', 'fit_id', 'doctrine_group_id', 'complete_fits_available', 'target_fits', 'fit_gap', 'readiness_state', 'resupply_pressure', 'bottleneck_type_id', 'bottleneck_quantity'],
        $fitPressureRows,
        ['doctrine_group_id', 'complete_fits_available', 'target_fits', 'fit_gap', 'readiness_state', 'resupply_pressure', 'bottleneck_type_id', 'bottleneck_quantity']
    );
    db_bulk_insert_or_upsert(
        'doctrine_item_stock_1d',
        ['bucket_start', 'fit_id', 'doctrine_group_id', 'type_id', 'required_units', 'local_stock_units', 'complete_fits_supported', 'fit_gap'],
        $itemRows,
        ['doctrine_group_id', 'required_units', 'local_stock_units', 'complete_fits_supported', 'fit_gap']
    );
}

function db_killmail_item_loss_window_summaries(array $typeIds, int $hours = 24 * 7): array
{
    db_time_series_analytics_ensure_schema();

    $safeHours = max(1, min(24 * 30, $hours));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));

    return db_select(
        "SELECT
            type_id,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN quantity_lost ELSE 0 END) AS quantity_24h,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 3 DAY) THEN quantity_lost ELSE 0 END) AS quantity_3d,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN quantity_lost ELSE 0 END) AS quantity_7d,
            SUM(quantity_lost) AS quantity_window,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN killmail_count ELSE 0 END) AS losses_24h,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 3 DAY) THEN killmail_count ELSE 0 END) AS losses_3d,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN killmail_count ELSE 0 END) AS losses_7d,
            SUM(killmail_count) AS losses_window,
            MAX(bucket_start) AS latest_loss_at
         FROM killmail_item_loss_1h
         WHERE bucket_start >= (UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)
           AND doctrine_fit_id IS NULL
           AND doctrine_group_id IS NULL
           AND type_id IN ({$placeholders})
         GROUP BY type_id",
        $normalizedTypeIds
    );
}

function db_killmail_hull_loss_window_summaries(array $hullTypeIds, int $hours = 24 * 7): array
{
    db_time_series_analytics_ensure_schema();

    $safeHours = max(1, min(24 * 30, $hours));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $hullTypeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));

    return db_select(
        "SELECT
            hull_type_id AS type_id,
            SUM(CASE WHEN bucket_start >= (UTC_DATE() - INTERVAL 1 DAY) THEN killmail_count ELSE 0 END) AS losses_24h,
            SUM(CASE WHEN bucket_start >= (UTC_DATE() - INTERVAL 3 DAY) THEN killmail_count ELSE 0 END) AS losses_3d,
            SUM(CASE WHEN bucket_start >= (UTC_DATE() - INTERVAL 7 DAY) THEN killmail_count ELSE 0 END) AS losses_7d,
            SUM(killmail_count) AS losses_window,
            MAX(bucket_start) AS latest_loss_at
         FROM killmail_hull_loss_1d
         WHERE bucket_start >= (UTC_DATE() - INTERVAL CEIL({$safeHours} / 24) DAY)
           AND doctrine_fit_id IS NULL
           AND doctrine_group_id IS NULL
           AND hull_type_id IN ({$placeholders})
         GROUP BY hull_type_id",
        $normalizedTypeIds
    );
}

function db_market_item_stock_window_summaries(string $sourceType, int $sourceId, string $startDate, string $endDate, array $typeIds = []): array
{
    db_time_series_analytics_ensure_schema();

    $params = [$sourceType, $sourceId, $startDate, $endDate];
    $typeFilterSql = '';
    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }
        $typeFilterSql = ' AND mis.type_id IN (' . implode(',', array_fill(0, count($normalizedTypeIds), '?')) . ')';
        $params = array_merge($params, $normalizedTypeIds);
    }

    return db_select_cached(
        "SELECT
            mis.bucket_start AS observed_date,
            mis.type_id,
            rit.type_name,
            mis.local_stock_units AS sell_volume,
            0 AS buy_volume,
            mis.listing_count AS sell_order_count,
            0 AS buy_order_count,
            mip.avg_price AS avg_sell_price,
            NULL AS avg_buy_price,
            CONCAT(mis.bucket_start, ' 00:00:00') AS last_observed_at
         FROM market_item_stock_1d mis
         LEFT JOIN market_item_price_1d mip
           ON mip.bucket_start = mis.bucket_start
          AND mip.source_type = mis.source_type
          AND mip.source_id = mis.source_id
          AND mip.type_id = mis.type_id
         LEFT JOIN ref_item_types rit ON rit.type_id = mis.type_id
         WHERE mis.source_type = ?
           AND mis.source_id = ?
           AND mis.bucket_start BETWEEN ? AND ?{$typeFilterSql}
         ORDER BY mis.bucket_start ASC, mis.type_id ASC",
        $params,
        60,
        'market.item-stock.daily'
    );
}

function db_upsert_esi_oauth_token(array $token): bool
{
    return db_execute(
        'INSERT INTO esi_oauth_tokens (
            character_id,
            character_name,
            owner_hash,
            access_token,
            refresh_token,
            token_type,
            scopes,
            expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            character_name = VALUES(character_name),
            owner_hash = VALUES(owner_hash),
            access_token = VALUES(access_token),
            refresh_token = VALUES(refresh_token),
            token_type = VALUES(token_type),
            scopes = VALUES(scopes),
            expires_at = VALUES(expires_at),
            updated_at = CURRENT_TIMESTAMP',
        [
            $token['character_id'],
            $token['character_name'],
            $token['owner_hash'],
            $token['access_token'],
            $token['refresh_token'],
            $token['token_type'],
            $token['scopes'],
            $token['expires_at'],
        ]
    );
}

function db_latest_esi_oauth_token(): ?array
{
    return db_select_one('SELECT * FROM esi_oauth_tokens ORDER BY updated_at DESC, id DESC LIMIT 1');
}

function db_update_esi_oauth_token_refresh(
    int $tokenId,
    string $accessToken,
    string $refreshToken,
    string $tokenType,
    string $scopes,
    string $expiresAt
): bool {
    return db_execute(
        'UPDATE esi_oauth_tokens
         SET access_token = ?,
             refresh_token = ?,
             token_type = ?,
             scopes = ?,
             expires_at = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        [$accessToken, $refreshToken, $tokenType, $scopes, $expiresAt, $tokenId]
    );
}

function db_esi_cache_put(string $namespace, string $cacheKey, string $payloadJson, ?string $etag = null, ?string $expiresAt = null): bool
{
    return db_execute(
        'INSERT INTO esi_cache_entries (namespace_key, cache_key, payload_json, etag, fetched_at, expires_at)
         VALUES (?, ?, ?, ?, UTC_TIMESTAMP(), ?)
         ON DUPLICATE KEY UPDATE
            payload_json = VALUES(payload_json),
            etag = VALUES(etag),
            fetched_at = UTC_TIMESTAMP(),
            expires_at = VALUES(expires_at),
            updated_at = CURRENT_TIMESTAMP',
        [$namespace, $cacheKey, $payloadJson, $etag, $expiresAt]
    );
}

function db_esi_cache_get(string $namespace, string $cacheKey): ?array
{
    return db_select_one(
        'SELECT namespace_key, cache_key, payload_json, etag, fetched_at, expires_at
         FROM esi_cache_entries
         WHERE namespace_key = ? AND cache_key = ?
         LIMIT 1',
        [$namespace, $cacheKey]
    );
}

function db_esi_cache_namespace_keys(): array
{
    $rows = db_select('SELECT namespace_key FROM esi_cache_namespaces ORDER BY namespace_key ASC');

    return array_column($rows, 'namespace_key');
}

function db_sync_state_get(string $datasetKey): ?array
{
    return db_select_one(
        'SELECT dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message, updated_at
         FROM sync_state
         WHERE dataset_key = ?
         LIMIT 1',
        [$datasetKey]
    );
}

function db_sync_state_upsert(
    string $datasetKey,
    string $syncMode,
    string $status,
    ?string $lastSuccessAt,
    ?string $lastCursor,
    int $lastRowCount,
    ?string $lastChecksum,
    ?string $lastErrorMessage
): bool {
    return db_execute(
        'INSERT INTO sync_state (
            dataset_key,
            sync_mode,
            status,
            last_success_at,
            last_cursor,
            last_row_count,
            last_checksum,
            last_error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            sync_mode = VALUES(sync_mode),
            status = VALUES(status),
            last_success_at = VALUES(last_success_at),
            last_cursor = VALUES(last_cursor),
            last_row_count = VALUES(last_row_count),
            last_checksum = VALUES(last_checksum),
            last_error_message = VALUES(last_error_message),
            updated_at = CURRENT_TIMESTAMP',
        [$datasetKey, $syncMode, $status, $lastSuccessAt, $lastCursor, $lastRowCount, $lastChecksum, $lastErrorMessage]
    );
}

function db_sync_cursor_get(string $datasetKey): ?string
{
    $state = db_sync_state_get($datasetKey);

    if ($state === null) {
        return null;
    }

    return $state['last_cursor'] ?: null;
}

function db_sync_cursor_upsert(string $datasetKey, string $syncMode, ?string $cursor): bool
{
    $state = db_sync_state_get($datasetKey);

    return db_sync_state_upsert(
        $datasetKey,
        $syncMode,
        (string) ($state['status'] ?? 'idle'),
        $state['last_success_at'] ?? null,
        $cursor,
        (int) ($state['last_row_count'] ?? 0),
        $state['last_checksum'] ?? null,
        $state['last_error_message'] ?? null
    );
}

function db_sync_run_with_state(
    string $datasetKey,
    string $runMode,
    ?string $cursorStart,
    callable $callback
): mixed {
    return db_transaction(function () use ($datasetKey, $runMode, $cursorStart, $callback): mixed {
        $runId = db_sync_run_start($datasetKey, $runMode, $cursorStart);

        try {
            $result = $callback($runId);
            $sourceRows = (int) ($result['source_rows'] ?? 0);
            $writtenRows = (int) ($result['written_rows'] ?? 0);
            $cursorEnd = isset($result['cursor_end']) ? (string) $result['cursor_end'] : null;

            db_sync_run_finish($runId, 'success', $sourceRows, $writtenRows, $cursorEnd, null);

            if (array_key_exists('sync_mode', $result)) {
                db_sync_state_upsert(
                    $datasetKey,
                    (string) $result['sync_mode'],
                    'success',
                    gmdate('Y-m-d H:i:s'),
                    $cursorEnd,
                    $writtenRows,
                    isset($result['checksum']) ? (string) $result['checksum'] : null,
                    null
                );
            }

            return $result;
        } catch (Throwable $exception) {
            db_sync_run_finish($runId, 'failed', 0, 0, null, mb_substr($exception->getMessage(), 0, 500));
            throw $exception;
        }
    });
}

function db_market_orders_history_legacy_table(): string
{
    return 'market_orders_history';
}

function db_market_orders_history_partitioned_table(): string
{
    return 'market_orders_history_p';
}

function db_market_orders_history_known_tables(): array
{
    return [
        db_market_orders_history_legacy_table(),
        db_market_orders_history_partitioned_table(),
    ];
}

function db_market_orders_history_validate_table(string $table): string
{
    $safeTable = trim($table);
    if (!in_array($safeTable, db_market_orders_history_known_tables(), true)) {
        throw new InvalidArgumentException('Unsupported market orders history table: ' . $table);
    }

    return $safeTable;
}

function db_market_orders_history_cutover_setting_key(string $scope): string
{
    return match ($scope) {
        'read' => 'market_orders_history_read_mode',
        'write' => 'market_orders_history_write_mode',
        default => throw new InvalidArgumentException('Unsupported market orders history cutover scope: ' . $scope),
    };
}

function db_market_orders_history_cutover_allowed_modes(string $scope): array
{
    return match ($scope) {
        'read' => ['legacy', 'partitioned'],
        'write' => ['legacy', 'dual', 'partitioned'],
        default => throw new InvalidArgumentException('Unsupported market orders history cutover scope: ' . $scope),
    };
}

function db_market_orders_history_cutover_mode(string $scope): string
{
    $key = db_market_orders_history_cutover_setting_key($scope);
    $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ? LIMIT 1', [$key]);
    $mode = trim((string) ($row['setting_value'] ?? ''));
    $allowedModes = db_market_orders_history_cutover_allowed_modes($scope);

    if (!in_array($mode, $allowedModes, true)) {
        return 'legacy';
    }

    return $mode;
}

function db_market_orders_history_set_cutover_mode(string $scope, string $mode): bool
{
    $safeMode = trim($mode);
    $allowedModes = db_market_orders_history_cutover_allowed_modes($scope);
    if (!in_array($safeMode, $allowedModes, true)) {
        throw new InvalidArgumentException(sprintf('Unsupported %s cutover mode: %s', $scope, $mode));
    }

    return db_execute(
        'INSERT INTO app_settings (setting_key, setting_value) VALUES (?, ?)
         ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value), updated_at = CURRENT_TIMESTAMP',
        [db_market_orders_history_cutover_setting_key($scope), $safeMode]
    );
}

function db_market_orders_history_read_table(): string
{
    return db_market_orders_history_cutover_mode('read') === 'partitioned'
        ? db_market_orders_history_partitioned_table()
        : db_market_orders_history_legacy_table();
}

function db_market_orders_history_write_tables(): array
{
    return match (db_market_orders_history_cutover_mode('write')) {
        'partitioned' => [db_market_orders_history_partitioned_table()],
        'dual' => [db_market_orders_history_legacy_table(), db_market_orders_history_partitioned_table()],
        default => [db_market_orders_history_legacy_table()],
    };
}

function db_market_orders_history_partitioned_table_sql(): string
{
    return "CREATE TABLE IF NOT EXISTS market_orders_history_p (
"
        . "    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
"
        . "    source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
"
        . "    source_id BIGINT UNSIGNED NOT NULL,
"
        . "    type_id INT UNSIGNED NOT NULL,
"
        . "    order_id BIGINT UNSIGNED NOT NULL,
"
        . "    is_buy_order TINYINT(1) NOT NULL,
"
        . "    price DECIMAL(20, 2) NOT NULL,
"
        . "    volume_remain INT UNSIGNED NOT NULL,
"
        . "    volume_total INT UNSIGNED NOT NULL,
"
        . "    min_volume INT UNSIGNED NOT NULL DEFAULT 1,
"
        . "    `range` VARCHAR(20) NOT NULL,
"
        . "    duration SMALLINT UNSIGNED NOT NULL,
"
        . "    issued DATETIME NOT NULL,
"
        . "    expires DATETIME NOT NULL,
"
        . "    observed_at DATETIME NOT NULL,
"
        . "    observed_date DATE NOT NULL,
"
        . "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
"
        . "    PRIMARY KEY (id, observed_date),
"
        . "    UNIQUE KEY unique_source_order_observed (source_type, source_id, order_id, observed_at, observed_date),
"
        . "    KEY idx_market_orders_history_type_observed (source_type, source_id, type_id, observed_at),
"
        . "    KEY idx_market_orders_history_observed (source_type, source_id, observed_at),
"
        . "    KEY idx_market_orders_history_source_date_type (source_type, source_id, observed_date, type_id),
"
        . "    KEY idx_market_orders_history_observed_at (observed_at)
"
        . ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"
        . "PARTITION BY RANGE COLUMNS(observed_date) (
"
        . "    PARTITION p_bootstrap VALUES LESS THAN ('2026-01-01'),
"
        . "    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
"
        . "    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
"
        . "    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
"
        . "    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
"
        . "    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
"
        . "    PARTITION pmax VALUES LESS THAN (MAXVALUE)
"
        . ")";
}

function db_market_orders_history_partition_name_for_month(DateTimeImmutable $monthStart): string
{
    return 'p' . $monthStart->format('Ym');
}

function db_market_orders_history_partition_boundary_value(DateTimeImmutable $monthStart): string
{
    return $monthStart->modify('+1 month')->format('Y-m-d');
}

function db_monthly_partition_boundaries(string $table): array
{
    $rows = db_select(
        'SELECT partition_name, partition_description, partition_ordinal_position
'
        . 'FROM information_schema.partitions
'
        . 'WHERE table_schema = DATABASE()
'
        . '  AND table_name = ?
'
        . '  AND partition_name IS NOT NULL
'
        . 'ORDER BY partition_ordinal_position ASC',
        [$table]
    );

    $boundaries = [];
    foreach ($rows as $row) {
        $description = trim((string) ($row['partition_description'] ?? ''));
        $description = trim($description, "'");
        $boundaries[] = [
            'partition_name' => (string) ($row['partition_name'] ?? ''),
            'boundary_exclusive' => strtoupper($description) === 'MAXVALUE' ? null : $description,
            'is_maxvalue' => strtoupper($description) === 'MAXVALUE',
            'ordinal_position' => (int) ($row['partition_ordinal_position'] ?? 0),
        ];
    }

    return $boundaries;
}

function db_market_orders_history_partition_boundaries(string $table = 'market_orders_history_p'): array
{
    $safeTable = db_market_orders_history_validate_table($table);

    return db_monthly_partition_boundaries($safeTable);
}


function db_market_orders_history_normalize_observed_date(string $observedAt): string
{
    $safeObservedAt = trim($observedAt);
    if ($safeObservedAt === '') {
        return '';
    }

    $timestamp = strtotime($safeObservedAt);
    if ($timestamp === false) {
        return '';
    }

    return gmdate('Y-m-d', $timestamp);
}

function db_market_orders_history_rows_with_observed_date(array $rows): array
{
    if ($rows === []) {
        return [];
    }

    return array_values(array_map(static function (array $row): array {
        if (trim((string) ($row['observed_date'] ?? '')) !== '') {
            return $row;
        }

        $row['observed_date'] = db_market_orders_history_normalize_observed_date((string) ($row['observed_at'] ?? ''));

        return $row;
    }, $rows));
}

function db_monthly_partition_names_to_drop(array $boundaries, string $cutoffDate): array
{
    $cutoffTimestamp = strtotime($cutoffDate);
    if ($cutoffTimestamp === false) {
        return [];
    }

    $cutoffMonthStart = gmdate('Y-m-01', $cutoffTimestamp);
    $partitions = [];
    foreach ($boundaries as $partition) {
        $partitionName = trim((string) ($partition['partition_name'] ?? ''));
        $boundaryExclusive = trim((string) ($partition['boundary_exclusive'] ?? ''));
        if ($partitionName === '' || $boundaryExclusive === '' || !empty($partition['is_maxvalue'])) {
            continue;
        }

        if ($boundaryExclusive <= $cutoffMonthStart) {
            $partitions[] = $partitionName;
        }
    }

    return $partitions;
}

function db_monthly_partition_frontier_bounds(string $cutoffDateTime): ?array
{
    $timestamp = strtotime($cutoffDateTime);
    if ($timestamp === false) {
        return null;
    }

    $monthStart = gmdate('Y-m-01', $timestamp);
    $monthEndExclusive = gmdate('Y-m-01', strtotime($monthStart . ' +1 month'));

    return [
        'month_start' => $monthStart,
        'month_end_exclusive' => $monthEndExclusive,
    ];
}

function db_drop_table_partitions(string $table, array $partitionNames): array
{
    $safeNames = array_values(array_unique(array_filter(array_map(static function (mixed $partitionName): string {
        $safeName = trim((string) $partitionName);
        if ($safeName === '') {
            return '';
        }

        return db_market_orders_history_validate_partition_name($safeName);
    }, $partitionNames))));

    if ($safeNames === []) {
        return [];
    }

    db()->exec(sprintf(
        'ALTER TABLE %s DROP PARTITION %s',
        db_validate_identifier($table),
        implode(', ', $safeNames)
    ));
    db_query_cache_clear();

    return $safeNames;
}

function db_partition_frontier_prune_before_datetime(
    string $table,
    string $observedColumn,
    string $partitionColumn,
    string $cutoffDateTime,
    int $limit = 50000
): int {
    $bounds = db_monthly_partition_frontier_bounds($cutoffDateTime);
    if ($bounds === null) {
        return 0;
    }

    $safeLimit = max(1, min(50000, $limit));

    return db_execute(
        sprintf(
            'DELETE FROM %s WHERE %s >= ? AND %s < ? AND %s < ? LIMIT %d',
            db_validate_identifier($table),
            db_validate_identifier($partitionColumn),
            db_validate_identifier($partitionColumn),
            db_validate_identifier($observedColumn),
            $safeLimit
        ),
        [
            $bounds['month_start'],
            $bounds['month_end_exclusive'],
            $cutoffDateTime,
        ]
    )
        ? (int) db()->query('SELECT ROW_COUNT()')->fetchColumn()
        : 0;
}

function db_partitioned_table_retention_cleanup(
    string $table,
    string $observedColumn,
    string $partitionColumn,
    string $cutoffDateTime,
    callable $boundariesLoader,
    int $limit = 50000
): array {
    $droppedPartitions = db_drop_table_partitions(
        $table,
        db_monthly_partition_names_to_drop($boundariesLoader($table), $cutoffDateTime)
    );
    $frontierRowsDeleted = db_partition_frontier_prune_before_datetime($table, $observedColumn, $partitionColumn, $cutoffDateTime, $limit);

    return [
        'table' => $table,
        'method' => 'partition_drop+frontier_delete',
        'dropped_partitions' => $droppedPartitions,
        'partitions_dropped' => count($droppedPartitions),
        'rows_deleted' => $frontierRowsDeleted,
    ];
}

function db_market_orders_history_partitioned_schema_ensure(int $futureMonths = 3): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db_execute(db_market_orders_history_partitioned_table_sql());
    db_market_orders_history_ensure_future_monthly_partitions($futureMonths, db_market_orders_history_partitioned_table());

    $ensured = true;
}

function db_market_orders_history_ensure_future_monthly_partitions(int $futureMonths = 3, string $table = 'market_orders_history_p'): array
{
    $safeTable = db_market_orders_history_validate_table($table);
    if ($safeTable !== db_market_orders_history_partitioned_table()) {
        return [];
    }

    db_execute(db_market_orders_history_partitioned_table_sql());

    $safeFutureMonths = max(1, min(24, $futureMonths));
    $existing = db_market_orders_history_partition_boundaries($safeTable);
    $existingNames = array_map(static fn (array $row): string => (string) ($row['partition_name'] ?? ''), $existing);
    $hasMaxPartition = in_array('pmax', $existingNames, true);

    if (!$hasMaxPartition) {
        throw new RuntimeException('Partitioned market orders history table is missing the pmax partition.');
    }

    $monthsToAdd = [];
    $currentMonthStart = new DateTimeImmutable(gmdate('Y-m-01'));
    for ($offset = 0; $offset <= $safeFutureMonths; $offset++) {
        $monthStart = $currentMonthStart->modify(sprintf('+%d month', $offset));
        $partitionName = db_market_orders_history_partition_name_for_month($monthStart);
        if (in_array($partitionName, $existingNames, true)) {
            continue;
        }

        $monthsToAdd[] = [
            'partition_name' => $partitionName,
            'boundary_exclusive' => db_market_orders_history_partition_boundary_value($monthStart),
        ];
    }

    if ($monthsToAdd === []) {
        return [];
    }

    $partitionSql = [];
    foreach ($monthsToAdd as $partition) {
        $partitionSql[] = sprintf(
            "PARTITION %s VALUES LESS THAN ('%s')",
            db_market_orders_history_validate_partition_name((string) $partition['partition_name']),
            (string) $partition['boundary_exclusive']
        );
    }
    $partitionSql[] = 'PARTITION pmax VALUES LESS THAN (MAXVALUE)';

    db()->exec(sprintf(
        'ALTER TABLE %s REORGANIZE PARTITION pmax INTO (%s)',
        db_validate_identifier($safeTable),
        implode(', ', $partitionSql)
    ));
    db_query_cache_clear();

    return $monthsToAdd;
}

function db_market_orders_history_validate_partition_name(string $partitionName): string
{
    $safeName = trim($partitionName);
    if (!preg_match('/^p(?:max|bootstrap|\d{6})$/', $safeName)) {
        throw new InvalidArgumentException('Invalid market orders history partition name: ' . $partitionName);
    }

    return $safeName;
}

function db_market_orders_history_backfill_window(
    string $startDate,
    string $endDate,
    ?int $chunkSize = null,
    ?string $sourceTable = null,
    ?string $targetTable = null
): int {
    $safeStartDate = trim($startDate);
    $safeEndDate = trim($endDate);
    if ($safeStartDate === '' || $safeEndDate === '') {
        return 0;
    }

    $source = db_market_orders_history_validate_table($sourceTable ?? db_market_orders_history_legacy_table());
    $target = db_market_orders_history_validate_table($targetTable ?? db_market_orders_history_partitioned_table());
    if ($source === $target) {
        throw new InvalidArgumentException('Backfill source and target tables must differ.');
    }

    if ($target === db_market_orders_history_partitioned_table()) {
        db_market_orders_history_partitioned_schema_ensure();
        $startMonth = new DateTimeImmutable(substr($safeStartDate, 0, 7) . '-01');
        $endMonth = new DateTimeImmutable(substr($safeEndDate, 0, 7) . '-01');
        $monthsAhead = max(0, (((int) $endMonth->format('Y')) * 12 + (int) $endMonth->format('n')) - (((int) gmdate('Y')) * 12 + (int) gmdate('n')));
        db_market_orders_history_ensure_future_monthly_partitions($monthsAhead + 1, $target);
    }

    $baseColumns = [
        'source_type',
        'source_id',
        'type_id',
        'order_id',
        'is_buy_order',
        'price',
        'volume_remain',
        'volume_total',
        'min_volume',
        'range',
        'duration',
        'issued',
        'expires',
        'observed_at',
    ];

    $safeChunkSize = max(100, min(10000, $chunkSize ?? db_incremental_chunk_size()));
    $offset = 0;
    $written = 0;
    $quotedSource = db_validate_identifier($source);

    do {
        $rows = db_select(
            sprintf(
                'SELECT source_type, source_id, type_id, order_id, is_buy_order, price, volume_remain, volume_total, min_volume, `range`, duration, issued, expires, observed_at
'
                . 'FROM %s
'
                . 'WHERE observed_date BETWEEN ? AND ?
'
                . 'ORDER BY observed_at ASC, source_type ASC, source_id ASC, order_id ASC
'
                . 'LIMIT %d OFFSET %d',
                $quotedSource,
                $safeChunkSize,
                $offset
            ),
            [$safeStartDate, $safeEndDate]
        );

        if ($rows === []) {
            break;
        }

        $targetColumns = $baseColumns;
        if ($target === db_market_orders_history_partitioned_table()) {
            $targetColumns[] = 'observed_date';
            $rows = db_market_orders_history_rows_with_observed_date($rows);
        }

        $written += db_bulk_insert_or_upsert(
            $target,
            $targetColumns,
            $rows,
            [
                'type_id',
                'is_buy_order',
                'price',
                'volume_remain',
                'volume_total',
                'min_volume',
                'range',
                'duration',
                'issued',
                'expires',
            ],
            $safeChunkSize
        );
        $offset += $safeChunkSize;
    } while (count($rows) === $safeChunkSize);

    return $written;
}

function db_market_orders_history_cutover_swap(string $readMode = 'partitioned', string $writeMode = 'dual'): array
{
    $safeReadMode = trim($readMode);
    $safeWriteMode = trim($writeMode);

    if ($safeReadMode === 'partitioned' || $safeWriteMode !== 'legacy') {
        db_market_orders_history_partitioned_schema_ensure();
    }

    db_market_orders_history_set_cutover_mode('write', $safeWriteMode);
    db_market_orders_history_set_cutover_mode('read', $safeReadMode);

    return [
        'read_mode' => db_market_orders_history_cutover_mode('read'),
        'write_mode' => db_market_orders_history_cutover_mode('write'),
        'read_table' => db_market_orders_history_read_table(),
        'write_tables' => db_market_orders_history_write_tables(),
    ];
}

function db_market_orders_history_cutover_rollback_to_legacy(bool $resetReadMode = true): array
{
    db_market_orders_history_set_cutover_mode('write', 'legacy');
    if ($resetReadMode) {
        db_market_orders_history_set_cutover_mode('read', 'legacy');
    }

    return [
        'read_mode' => db_market_orders_history_cutover_mode('read'),
        'write_mode' => db_market_orders_history_cutover_mode('write'),
        'read_table' => db_market_orders_history_read_table(),
        'write_tables' => db_market_orders_history_write_tables(),
    ];
}

function db_market_order_snapshots_summary_legacy_table(): string
{
    return 'market_order_snapshots_summary';
}

function db_market_order_snapshots_summary_partitioned_table(): string
{
    return 'market_order_snapshots_summary_p';
}

function db_market_order_snapshots_summary_known_tables(): array
{
    return [
        db_market_order_snapshots_summary_legacy_table(),
        db_market_order_snapshots_summary_partitioned_table(),
    ];
}

function db_market_order_snapshots_summary_validate_table(string $table): string
{
    $safeTable = trim($table);
    if (!in_array($safeTable, db_market_order_snapshots_summary_known_tables(), true)) {
        throw new InvalidArgumentException('Unsupported market order snapshots summary table: ' . $table);
    }

    return $safeTable;
}

function db_market_order_snapshots_summary_cutover_setting_key(string $scope): string
{
    return match ($scope) {
        'read' => 'market_order_snapshots_summary_read_mode',
        'write' => 'market_order_snapshots_summary_write_mode',
        default => throw new InvalidArgumentException('Unsupported market order snapshots summary cutover scope: ' . $scope),
    };
}

function db_market_order_snapshots_summary_cutover_allowed_modes(string $scope): array
{
    return match ($scope) {
        'read' => ['legacy', 'partitioned'],
        'write' => ['legacy', 'dual', 'partitioned'],
        default => throw new InvalidArgumentException('Unsupported market order snapshots summary cutover scope: ' . $scope),
    };
}

function db_market_order_snapshots_summary_cutover_mode(string $scope): string
{
    $key = db_market_order_snapshots_summary_cutover_setting_key($scope);
    $row = db_select_one('SELECT setting_value FROM app_settings WHERE setting_key = ? LIMIT 1', [$key]);
    $mode = trim((string) ($row['setting_value'] ?? ''));
    $allowedModes = db_market_order_snapshots_summary_cutover_allowed_modes($scope);

    if (!in_array($mode, $allowedModes, true)) {
        return 'legacy';
    }

    return $mode;
}

function db_market_order_snapshots_summary_set_cutover_mode(string $scope, string $mode): bool
{
    $safeMode = trim($mode);
    $allowedModes = db_market_order_snapshots_summary_cutover_allowed_modes($scope);
    if (!in_array($safeMode, $allowedModes, true)) {
        throw new InvalidArgumentException(sprintf('Unsupported %s cutover mode: %s', $scope, $mode));
    }

    return db_execute(
        'INSERT INTO app_settings (setting_key, setting_value) VALUES (?, ?)
         ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value), updated_at = CURRENT_TIMESTAMP',
        [db_market_order_snapshots_summary_cutover_setting_key($scope), $safeMode]
    );
}

function db_market_order_snapshots_summary_read_table(): string
{
    return db_market_order_snapshots_summary_cutover_mode('read') === 'partitioned'
        ? db_market_order_snapshots_summary_partitioned_table()
        : db_market_order_snapshots_summary_legacy_table();
}

function db_market_order_snapshots_summary_write_tables(): array
{
    return match (db_market_order_snapshots_summary_cutover_mode('write')) {
        'partitioned' => [db_market_order_snapshots_summary_partitioned_table()],
        'dual' => [db_market_order_snapshots_summary_legacy_table(), db_market_order_snapshots_summary_partitioned_table()],
        default => [db_market_order_snapshots_summary_legacy_table()],
    };
}

function db_market_order_snapshots_summary_partitioned_table_sql(): string
{
    return "CREATE TABLE IF NOT EXISTS market_order_snapshots_summary_p (
"
        . "    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
"
        . "    source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
"
        . "    source_id BIGINT UNSIGNED NOT NULL,
"
        . "    type_id INT UNSIGNED NOT NULL,
"
        . "    observed_at DATETIME NOT NULL,
"
        . "    observed_date DATE NOT NULL,
"
        . "    best_sell_price DECIMAL(20, 2) DEFAULT NULL,
"
        . "    best_buy_price DECIMAL(20, 2) DEFAULT NULL,
"
        . "    total_buy_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
"
        . "    total_sell_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
"
        . "    total_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
"
        . "    buy_order_count INT UNSIGNED NOT NULL DEFAULT 0,
"
        . "    sell_order_count INT UNSIGNED NOT NULL DEFAULT 0,
"
        . "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
"
        . "    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
"
        . "    PRIMARY KEY (id, observed_date),
"
        . "    UNIQUE KEY unique_market_order_snapshot_summary (source_type, source_id, type_id, observed_at, observed_date),
"
        . "    KEY idx_snapshot_summary_source_observed_type (source_type, source_id, observed_at, type_id),
"
        . "    KEY idx_snapshot_summary_source_type_observed (source_type, source_id, type_id, observed_at),
"
        . "    KEY idx_snapshot_summary_source_date_type (source_type, source_id, observed_date, type_id),
"
        . "    KEY idx_snapshot_summary_observed (observed_at)
"
        . ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"
        . "PARTITION BY RANGE COLUMNS(observed_date) (
"
        . "    PARTITION p_bootstrap VALUES LESS THAN ('2026-01-01'),
"
        . "    PARTITION p202601 VALUES LESS THAN ('2026-02-01'),
"
        . "    PARTITION p202602 VALUES LESS THAN ('2026-03-01'),
"
        . "    PARTITION p202603 VALUES LESS THAN ('2026-04-01'),
"
        . "    PARTITION p202604 VALUES LESS THAN ('2026-05-01'),
"
        . "    PARTITION p202605 VALUES LESS THAN ('2026-06-01'),
"
        . "    PARTITION pmax VALUES LESS THAN (MAXVALUE)
"
        . ")";
}

function db_market_order_snapshots_summary_partition_boundaries(string $table = 'market_order_snapshots_summary_p'): array
{
    $safeTable = db_market_order_snapshots_summary_validate_table($table);

    return db_monthly_partition_boundaries($safeTable);
}

function db_market_order_snapshots_summary_partitioned_schema_ensure(int $futureMonths = 3): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db_execute(db_market_order_snapshots_summary_partitioned_table_sql());
    db_market_order_snapshots_summary_ensure_future_monthly_partitions($futureMonths, db_market_order_snapshots_summary_partitioned_table());

    $ensured = true;
}

function db_market_order_snapshots_summary_ensure_future_monthly_partitions(int $futureMonths = 3, string $table = 'market_order_snapshots_summary_p'): array
{
    $safeTable = db_market_order_snapshots_summary_validate_table($table);
    if ($safeTable !== db_market_order_snapshots_summary_partitioned_table()) {
        return [];
    }

    db_execute(db_market_order_snapshots_summary_partitioned_table_sql());

    $safeFutureMonths = max(1, min(24, $futureMonths));
    $existing = db_market_order_snapshots_summary_partition_boundaries($safeTable);
    $existingNames = array_map(static fn (array $row): string => (string) ($row['partition_name'] ?? ''), $existing);
    if (!in_array('pmax', $existingNames, true)) {
        throw new RuntimeException('Partitioned market order snapshots summary table is missing the pmax partition.');
    }

    $monthsToAdd = [];
    $currentMonthStart = new DateTimeImmutable(gmdate('Y-m-01'));
    for ($offset = 0; $offset <= $safeFutureMonths; $offset++) {
        $monthStart = $currentMonthStart->modify(sprintf('+%d month', $offset));
        $partitionName = db_market_orders_history_partition_name_for_month($monthStart);
        if (in_array($partitionName, $existingNames, true)) {
            continue;
        }

        $monthsToAdd[] = [
            'partition_name' => $partitionName,
            'boundary_exclusive' => db_market_orders_history_partition_boundary_value($monthStart),
        ];
    }

    if ($monthsToAdd === []) {
        return [];
    }

    $partitionSql = [];
    foreach ($monthsToAdd as $partition) {
        $partitionSql[] = sprintf(
            "PARTITION %s VALUES LESS THAN ('%s')",
            db_market_orders_history_validate_partition_name((string) $partition['partition_name']),
            (string) $partition['boundary_exclusive']
        );
    }
    $partitionSql[] = 'PARTITION pmax VALUES LESS THAN (MAXVALUE)';

    db()->exec(sprintf(
        'ALTER TABLE %s REORGANIZE PARTITION pmax INTO (%s)',
        db_validate_identifier($safeTable),
        implode(', ', $partitionSql)
    ));
    db_query_cache_clear();

    return $monthsToAdd;
}

function db_market_order_snapshots_summary_backfill_window(
    string $startDate,
    string $endDate,
    ?int $chunkSize = null,
    ?string $sourceTable = null,
    ?string $targetTable = null
): int {
    $safeStartDate = trim($startDate);
    $safeEndDate = trim($endDate);
    if ($safeStartDate === '' || $safeEndDate === '') {
        return 0;
    }

    $source = db_market_order_snapshots_summary_validate_table($sourceTable ?? db_market_order_snapshots_summary_legacy_table());
    $target = db_market_order_snapshots_summary_validate_table($targetTable ?? db_market_order_snapshots_summary_partitioned_table());
    if ($source === $target) {
        throw new InvalidArgumentException('Backfill source and target tables must differ.');
    }

    if ($target === db_market_order_snapshots_summary_partitioned_table()) {
        db_market_order_snapshots_summary_partitioned_schema_ensure();
        $startMonth = new DateTimeImmutable(substr($safeStartDate, 0, 7) . '-01');
        $endMonth = new DateTimeImmutable(substr($safeEndDate, 0, 7) . '-01');
        $monthsAhead = max(0, (((int) $endMonth->format('Y')) * 12 + (int) $endMonth->format('n')) - (((int) gmdate('Y')) * 12 + (int) gmdate('n')));
        db_market_order_snapshots_summary_ensure_future_monthly_partitions($monthsAhead + 1, $target);
    }

    $baseColumns = [
        'source_type',
        'source_id',
        'type_id',
        'observed_at',
        'best_sell_price',
        'best_buy_price',
        'total_buy_volume',
        'total_sell_volume',
        'total_volume',
        'buy_order_count',
        'sell_order_count',
    ];
    $safeChunkSize = max(100, min(10000, $chunkSize ?? db_incremental_chunk_size()));
    $offset = 0;
    $written = 0;
    $quotedSource = db_validate_identifier($source);

    do {
        $rows = db_select(
            sprintf(
                'SELECT source_type, source_id, type_id, observed_at, best_sell_price, best_buy_price, total_buy_volume, total_sell_volume, total_volume, buy_order_count, sell_order_count
                 FROM %s
                 WHERE observed_date BETWEEN ? AND ?
                 ORDER BY observed_at ASC, source_type ASC, source_id ASC, type_id ASC
                 LIMIT %d OFFSET %d',
                $quotedSource,
                $safeChunkSize,
                $offset
            ),
            [$safeStartDate, $safeEndDate]
        );

        if ($rows === []) {
            break;
        }

        $targetColumns = $baseColumns;
        if ($target === db_market_order_snapshots_summary_partitioned_table()) {
            $targetColumns[] = 'observed_date';
            $rows = db_market_orders_history_rows_with_observed_date($rows);
        }

        $written += db_bulk_insert_or_upsert(
            $target,
            $targetColumns,
            $rows,
            [
                'best_sell_price',
                'best_buy_price',
                'total_buy_volume',
                'total_sell_volume',
                'total_volume',
                'buy_order_count',
                'sell_order_count',
            ],
            $safeChunkSize
        );
        $offset += $safeChunkSize;
    } while (count($rows) === $safeChunkSize);

    return $written;
}

function db_market_order_snapshots_summary_cutover_swap(string $readMode = 'partitioned', string $writeMode = 'dual'): array
{
    $safeReadMode = trim($readMode);
    $safeWriteMode = trim($writeMode);

    if ($safeReadMode === 'partitioned' || $safeWriteMode !== 'legacy') {
        db_market_order_snapshots_summary_partitioned_schema_ensure();
    }

    db_market_order_snapshots_summary_set_cutover_mode('write', $safeWriteMode);
    db_market_order_snapshots_summary_set_cutover_mode('read', $safeReadMode);

    return [
        'read_mode' => db_market_order_snapshots_summary_cutover_mode('read'),
        'write_mode' => db_market_order_snapshots_summary_cutover_mode('write'),
        'read_table' => db_market_order_snapshots_summary_read_table(),
        'write_tables' => db_market_order_snapshots_summary_write_tables(),
    ];
}

function db_market_order_snapshots_summary_cutover_rollback_to_legacy(bool $resetReadMode = true): array
{
    db_market_order_snapshots_summary_set_cutover_mode('write', 'legacy');
    if ($resetReadMode) {
        db_market_order_snapshots_summary_set_cutover_mode('read', 'legacy');
    }

    return [
        'read_mode' => db_market_order_snapshots_summary_cutover_mode('read'),
        'write_mode' => db_market_order_snapshots_summary_cutover_mode('write'),
        'read_table' => db_market_order_snapshots_summary_read_table(),
        'write_tables' => db_market_order_snapshots_summary_write_tables(),
    ];
}

function db_market_order_current_projection_ensure(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db_execute(
        "CREATE TABLE IF NOT EXISTS market_order_current_projection (
            source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            observed_at DATETIME NOT NULL,
            best_sell_price DECIMAL(20, 2) DEFAULT NULL,
            best_buy_price DECIMAL(20, 2) DEFAULT NULL,
            total_sell_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
            total_buy_volume BIGINT UNSIGNED NOT NULL DEFAULT 0,
            sell_order_count INT UNSIGNED NOT NULL DEFAULT 0,
            buy_order_count INT UNSIGNED NOT NULL DEFAULT 0,
            total_volume BIGINT UNSIGNED DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (source_type, source_id, type_id),
            KEY idx_market_order_current_projection_observed (source_type, source_id, observed_at),
            KEY idx_market_order_current_projection_type (type_id, observed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    $ensured = true;
}

function db_market_order_current_projection_normalize_row(array $row): array
{
    return [
        'source_type' => trim((string) ($row['source_type'] ?? '')),
        'source_id' => max(0, (int) ($row['source_id'] ?? 0)),
        'type_id' => max(0, (int) ($row['type_id'] ?? 0)),
        'observed_at' => trim((string) ($row['observed_at'] ?? '')),
        'best_sell_price' => isset($row['best_sell_price']) && $row['best_sell_price'] !== null ? (float) $row['best_sell_price'] : null,
        'best_buy_price' => isset($row['best_buy_price']) && $row['best_buy_price'] !== null ? (float) $row['best_buy_price'] : null,
        'total_sell_volume' => max(0, (int) ($row['total_sell_volume'] ?? 0)),
        'total_buy_volume' => max(0, (int) ($row['total_buy_volume'] ?? 0)),
        'sell_order_count' => max(0, (int) ($row['sell_order_count'] ?? 0)),
        'buy_order_count' => max(0, (int) ($row['buy_order_count'] ?? 0)),
        'total_volume' => array_key_exists('total_volume', $row) && $row['total_volume'] !== null
            ? max(0, (int) $row['total_volume'])
            : null,
    ];
}

function db_market_order_current_projection_rows_from_orders(array $orders): array
{
    $projectionRows = [];

    foreach ($orders as $order) {
        $sourceType = trim((string) ($order['source_type'] ?? ''));
        $sourceId = max(0, (int) ($order['source_id'] ?? 0));
        $typeId = max(0, (int) ($order['type_id'] ?? 0));
        $observedAt = trim((string) ($order['observed_at'] ?? ''));
        if ($sourceType === '' || $sourceId <= 0 || $typeId <= 0 || $observedAt === '') {
            continue;
        }

        $key = implode(':', [$sourceType, $sourceId, $typeId, $observedAt]);
        if (!isset($projectionRows[$key])) {
            $projectionRows[$key] = [
                'source_type' => $sourceType,
                'source_id' => $sourceId,
                'type_id' => $typeId,
                'observed_at' => $observedAt,
                'best_sell_price' => null,
                'best_buy_price' => null,
                'total_sell_volume' => 0,
                'total_buy_volume' => 0,
                'sell_order_count' => 0,
                'buy_order_count' => 0,
                'total_volume' => 0,
            ];
        }

        $volumeRemain = max(0, (int) ($order['volume_remain'] ?? 0));
        $price = isset($order['price']) ? (float) $order['price'] : null;
        $isBuyOrder = (int) ($order['is_buy_order'] ?? 0) === 1;

        if ($isBuyOrder) {
            if ($price !== null) {
                $projectionRows[$key]['best_buy_price'] = $projectionRows[$key]['best_buy_price'] === null
                    ? $price
                    : max((float) $projectionRows[$key]['best_buy_price'], $price);
            }
            $projectionRows[$key]['total_buy_volume'] += $volumeRemain;
            $projectionRows[$key]['buy_order_count']++;
        } else {
            if ($price !== null) {
                $projectionRows[$key]['best_sell_price'] = $projectionRows[$key]['best_sell_price'] === null
                    ? $price
                    : min((float) $projectionRows[$key]['best_sell_price'], $price);
            }
            $projectionRows[$key]['total_sell_volume'] += $volumeRemain;
            $projectionRows[$key]['sell_order_count']++;
        }

        $projectionRows[$key]['total_volume'] += $volumeRemain;
    }

    return array_values($projectionRows);
}

function db_market_order_current_projection_anchor(string $sourceType, int $sourceId): string
{
    $state = db_market_source_snapshot_state_get($sourceType, $sourceId);
    $latestCurrentObservedAt = trim((string) ($state['latest_current_observed_at'] ?? ''));
    $latestSummaryObservedAt = trim((string) ($state['latest_summary_observed_at'] ?? ''));

    if ($latestCurrentObservedAt !== '' && $latestSummaryObservedAt !== '') {
        return strcmp($latestCurrentObservedAt, $latestSummaryObservedAt) >= 0
            ? $latestCurrentObservedAt
            : $latestSummaryObservedAt;
    }

    return $latestCurrentObservedAt !== '' ? $latestCurrentObservedAt : $latestSummaryObservedAt;
}

function db_market_order_current_projection_refresh_snapshots(array $projectionRows, ?int $chunkSize = null): int
{
    db_market_order_current_projection_ensure();
    db_market_source_snapshot_state_ensure();

    if ($projectionRows === []) {
        return 0;
    }

    $snapshots = [];
    foreach ($projectionRows as $row) {
        $normalized = db_market_order_current_projection_normalize_row($row);
        if ($normalized['source_type'] === ''
            || $normalized['source_id'] <= 0
            || $normalized['type_id'] <= 0
            || $normalized['observed_at'] === ''
        ) {
            continue;
        }

        $snapshotKey = implode(':', [$normalized['source_type'], $normalized['source_id'], $normalized['observed_at']]);
        $snapshots[$snapshotKey]['source_type'] = $normalized['source_type'];
        $snapshots[$snapshotKey]['source_id'] = $normalized['source_id'];
        $snapshots[$snapshotKey]['observed_at'] = $normalized['observed_at'];
        $snapshots[$snapshotKey]['rows'][] = $normalized;
    }

    return db_transaction(function () use ($snapshots, $chunkSize): int {
        $written = 0;

        foreach ($snapshots as $snapshot) {
            $sourceType = (string) $snapshot['source_type'];
            $sourceId = (int) $snapshot['source_id'];
            $observedAt = (string) $snapshot['observed_at'];
            $rows = (array) ($snapshot['rows'] ?? []);
            if ($rows === []) {
                continue;
            }

            $anchorObservedAt = db_market_order_current_projection_anchor($sourceType, $sourceId);
            $existingRow = db_select_one(
                'SELECT observed_at
                 FROM market_order_current_projection
                 WHERE source_type = ?
                   AND source_id = ?
                 ORDER BY observed_at DESC, type_id ASC
                 LIMIT 1',
                [$sourceType, $sourceId]
            );
            $existingObservedAt = trim((string) ($existingRow['observed_at'] ?? ''));
            $freshnessFloor = $anchorObservedAt;
            if ($existingObservedAt !== '' && ($freshnessFloor === '' || strcmp($existingObservedAt, $freshnessFloor) > 0)) {
                $freshnessFloor = $existingObservedAt;
            }

            if ($freshnessFloor !== '' && strcmp($observedAt, $freshnessFloor) < 0) {
                continue;
            }

            db_execute(
                'DELETE FROM market_order_current_projection
                 WHERE source_type = ?
                   AND source_id = ?',
                [$sourceType, $sourceId]
            );

            $written += db_bulk_insert_or_upsert(
                'market_order_current_projection',
                [
                    'source_type',
                    'source_id',
                    'type_id',
                    'observed_at',
                    'best_sell_price',
                    'best_buy_price',
                    'total_sell_volume',
                    'total_buy_volume',
                    'sell_order_count',
                    'buy_order_count',
                    'total_volume',
                ],
                $rows,
                [
                    'observed_at',
                    'best_sell_price',
                    'best_buy_price',
                    'total_sell_volume',
                    'total_buy_volume',
                    'sell_order_count',
                    'buy_order_count',
                    'total_volume',
                ],
                $chunkSize
            );
        }

        return $written;
    });
}

function db_market_order_current_projection_latest_rows(string $sourceType, int $sourceId, array $typeIds = []): array
{
    db_market_order_current_projection_ensure();

    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);
    if ($safeSourceType === '' || $safeSourceId <= 0) {
        return [];
    }

    $anchorObservedAt = db_market_order_current_projection_anchor($safeSourceType, $safeSourceId);
    $params = [$safeSourceType, $safeSourceId];
    $observedFilterSql = '';
    if ($anchorObservedAt !== '') {
        $observedFilterSql = ' AND observed_at = ?';
        $params[] = $anchorObservedAt;
    }

    $typeFilterSql = '';
    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $typeFilterSql = ' AND type_id IN (' . implode(', ', array_fill(0, count($normalizedTypeIds), '?')) . ')';
        $params = array_merge($params, $normalizedTypeIds);
    }

    $rows = db_select_cached(
        "SELECT
            source_type,
            source_id,
            type_id,
            observed_at,
            best_sell_price,
            best_buy_price,
            total_sell_volume,
            total_buy_volume,
            sell_order_count,
            buy_order_count,
            total_volume
         FROM market_order_current_projection
         WHERE source_type = ?
           AND source_id = ?{$observedFilterSql}{$typeFilterSql}
         ORDER BY type_id ASC",
        $params,
        60,
        'market.current.projection'
    );

    if ($rows !== []) {
        return $rows;
    }

    return db_select_cached(
        "SELECT
            source_type,
            source_id,
            type_id,
            observed_at,
            best_sell_price,
            best_buy_price,
            total_sell_volume,
            total_buy_volume,
            sell_order_count,
            buy_order_count,
            total_volume
         FROM market_order_current_projection
         WHERE source_type = ?
           AND source_id = ?{$typeFilterSql}
         ORDER BY observed_at DESC, type_id ASC",
        array_merge([$safeSourceType, $safeSourceId], $typeIds !== [] ? array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0))) : []),
        60,
        'market.current.projection'
    );
}

function db_market_orders_current_compact_type_names(array $typeIds): array
{
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $rows = db_select(
        'SELECT type_id, type_name
         FROM ref_item_types
         WHERE type_id IN (' . implode(', ', array_fill(0, count($normalizedTypeIds), '?')) . ')',
        $normalizedTypeIds
    );

    $typeNameMap = [];
    foreach ($rows as $row) {
        $typeNameMap[(int) ($row['type_id'] ?? 0)] = (string) ($row['type_name'] ?? '');
    }

    return $typeNameMap;
}

function db_market_orders_current_compact_projection_rows(string $sourceType, int $sourceId, array $typeIds = []): array
{
    $projectionRows = db_market_order_current_projection_latest_rows($sourceType, $sourceId, $typeIds);
    if ($projectionRows === []) {
        return [];
    }

    $typeNameMap = db_market_orders_current_compact_type_names(array_map(
        static fn (array $row): int => (int) ($row['type_id'] ?? 0),
        $projectionRows
    ));

    return array_map(static function (array $row) use ($typeNameMap): array {
        $typeId = (int) ($row['type_id'] ?? 0);
        $totalSellVolume = max(0, (int) ($row['total_sell_volume'] ?? 0));
        $totalBuyVolume = max(0, (int) ($row['total_buy_volume'] ?? 0));
        $totalVolume = array_key_exists('total_volume', $row) && $row['total_volume'] !== null
            ? max(0, (int) $row['total_volume'])
            : ($totalSellVolume + $totalBuyVolume);
        $observedAt = (string) ($row['observed_at'] ?? '');

        return [
            'source_type' => (string) ($row['source_type'] ?? ''),
            'source_id' => max(0, (int) ($row['source_id'] ?? 0)),
            'type_id' => $typeId,
            'type_name' => $typeNameMap[$typeId] ?? null,
            'best_sell_price' => $row['best_sell_price'] ?? null,
            'best_buy_price' => $row['best_buy_price'] ?? null,
            'total_sell_volume' => $totalSellVolume,
            'total_buy_volume' => $totalBuyVolume,
            'total_volume' => $totalVolume,
            'sell_order_count' => max(0, (int) ($row['sell_order_count'] ?? 0)),
            'buy_order_count' => max(0, (int) ($row['buy_order_count'] ?? 0)),
            'last_observed_at' => $observedAt,
        ];
    }, $projectionRows);
}

function db_market_orders_current_compact_snapshot_rows(string $sourceType, int $sourceId, array $typeIds = []): array
{
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);
    if ($safeSourceType === '' || $safeSourceId <= 0) {
        return [];
    }

    $projectionRows = db_market_orders_current_compact_projection_rows($safeSourceType, $safeSourceId, $typeIds);
    if ($projectionRows !== []) {
        return $projectionRows;
    }

    return db_market_orders_current_source_aggregates($safeSourceType, $safeSourceId, $typeIds);
}

function db_market_orders_current_bulk_upsert(array $orders, ?int $chunkSize = null): int
{
    $written = db_bulk_insert_or_upsert(
        'market_orders_current',
        [
            'source_type',
            'source_id',
            'type_id',
            'order_id',
            'is_buy_order',
            'price',
            'volume_remain',
            'volume_total',
            'min_volume',
            'range',
            'duration',
            'issued',
            'expires',
            'observed_at',
        ],
        $orders,
        [
            'type_id',
            'is_buy_order',
            'price',
            'volume_remain',
            'volume_total',
            'min_volume',
            'range',
            'duration',
            'issued',
            'expires',
            'observed_at',
        ],
        $chunkSize
    );

    db_market_order_current_projection_refresh_snapshots(
        db_market_order_current_projection_rows_from_orders($orders),
        $chunkSize
    );

    return $written;
}

function db_market_orders_history_bulk_insert(array $orders, ?int $chunkSize = null): int
{
    db_market_snapshot_optimization_ensure();

    if ($orders === []) {
        return 0;
    }

    $baseColumns = [
        'source_type',
        'source_id',
        'type_id',
        'order_id',
        'is_buy_order',
        'price',
        'volume_remain',
        'volume_total',
        'min_volume',
        'range',
        'duration',
        'issued',
        'expires',
        'observed_at',
    ];

    $writeTables = db_market_orders_history_write_tables();
    if (in_array(db_market_orders_history_partitioned_table(), $writeTables, true)) {
        db_market_orders_history_partitioned_schema_ensure();

        $latestObservedDate = '';
        foreach ($orders as $order) {
            $observedAt = trim((string) ($order['observed_at'] ?? ''));
            if ($observedAt === '') {
                continue;
            }

            $observedDate = db_market_orders_history_normalize_observed_date($observedAt);
            if ($observedDate === '') {
                continue;
            }

            if ($observedDate > $latestObservedDate) {
                $latestObservedDate = $observedDate;
            }
        }

        if ($latestObservedDate !== '') {
            $windowEndMonth = new DateTimeImmutable(substr($latestObservedDate, 0, 7) . '-01');
            $monthsAhead = max(0, (((int) $windowEndMonth->format('Y')) * 12 + (int) $windowEndMonth->format('n')) - (((int) gmdate('Y')) * 12 + (int) gmdate('n')));
            db_market_orders_history_ensure_future_monthly_partitions($monthsAhead + 1, db_market_orders_history_partitioned_table());
        }
    }

    return db_transaction(function () use ($orders, $baseColumns, $chunkSize, $writeTables): int {
        $written = 0;

        foreach ($writeTables as $table) {
            $tableColumns = $baseColumns;
            $tableRows = $orders;
            if ($table === db_market_orders_history_partitioned_table()) {
                $tableColumns[] = 'observed_date';
                $tableRows = db_market_orders_history_rows_with_observed_date($orders);
            }

            $written = max(
                $written,
                db_bulk_insert_or_upsert(
                    $table,
                    $tableColumns,
                    $tableRows,
                    [
                        'type_id',
                        'is_buy_order',
                        'price',
                        'volume_remain',
                        'volume_total',
                        'min_volume',
                        'range',
                        'duration',
                        'issued',
                        'expires',
                    ],
                    $chunkSize
                )
            );
        }

        return $written;
    });
}

function db_market_snapshot_rollup_table_name(string $resolution): string
{
    return match ($resolution) {
        '1h' => 'market_order_snapshot_rollup_1h',
        '1d' => 'market_order_snapshot_rollup_1d',
        default => throw new InvalidArgumentException('Unsupported market snapshot rollup resolution: ' . $resolution),
    };
}

function db_market_snapshot_rollups_ensure(): void
{
    db_execute(
        "CREATE TABLE IF NOT EXISTS market_order_snapshot_rollup_1h (
            bucket_start DATETIME NOT NULL,
            source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            first_observed_at DATETIME DEFAULT NULL,
            last_observed_at DATETIME DEFAULT NULL,
            best_sell_price_min DECIMAL(20, 2) DEFAULT NULL,
            best_sell_price_max DECIMAL(20, 2) DEFAULT NULL,
            best_sell_price_sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            best_sell_price_sum DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            best_sell_price_last DECIMAL(20, 2) DEFAULT NULL,
            best_buy_price_min DECIMAL(20, 2) DEFAULT NULL,
            best_buy_price_max DECIMAL(20, 2) DEFAULT NULL,
            best_buy_price_sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            best_buy_price_sum DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            best_buy_price_last DECIMAL(20, 2) DEFAULT NULL,
            total_buy_volume_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            total_sell_volume_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            total_volume_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            buy_order_count_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            sell_order_count_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, source_type, source_id, type_id),
            KEY idx_market_order_snapshot_rollup_1h_source_bucket (source_type, source_id, bucket_start),
            KEY idx_market_order_snapshot_rollup_1h_bucket_type (bucket_start, type_id),
            KEY idx_market_order_snapshot_rollup_1h_type_bucket (type_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    db_execute(
        "CREATE TABLE IF NOT EXISTS market_order_snapshot_rollup_1d (
            bucket_start DATE NOT NULL,
            source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            type_id INT UNSIGNED NOT NULL,
            sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            first_observed_at DATETIME DEFAULT NULL,
            last_observed_at DATETIME DEFAULT NULL,
            best_sell_price_min DECIMAL(20, 2) DEFAULT NULL,
            best_sell_price_max DECIMAL(20, 2) DEFAULT NULL,
            best_sell_price_sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            best_sell_price_sum DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            best_sell_price_last DECIMAL(20, 2) DEFAULT NULL,
            best_buy_price_min DECIMAL(20, 2) DEFAULT NULL,
            best_buy_price_max DECIMAL(20, 2) DEFAULT NULL,
            best_buy_price_sample_count INT UNSIGNED NOT NULL DEFAULT 0,
            best_buy_price_sum DECIMAL(24, 2) NOT NULL DEFAULT 0.00,
            best_buy_price_last DECIMAL(20, 2) DEFAULT NULL,
            total_buy_volume_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            total_sell_volume_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            total_volume_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            buy_order_count_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            sell_order_count_sum BIGINT UNSIGNED NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (bucket_start, source_type, source_id, type_id),
            KEY idx_market_order_snapshot_rollup_1d_source_bucket (source_type, source_id, bucket_start),
            KEY idx_market_order_snapshot_rollup_1d_bucket_type (bucket_start, type_id),
            KEY idx_market_order_snapshot_rollup_1d_type_bucket (type_id, bucket_start)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );
}

function db_market_snapshot_rollup_normalize_row(string $resolution, array $row): array
{
    $bucketStart = trim((string) ($row['bucket_start'] ?? ''));
    if ($bucketStart !== '') {
        $bucketStart = $resolution === '1d'
            ? gmdate('Y-m-d', strtotime($bucketStart) ?: time())
            : normalize_to_hour_bucket($bucketStart);
    }

    return [
        'bucket_start' => $bucketStart,
        'source_type' => trim((string) ($row['source_type'] ?? '')),
        'source_id' => max(0, (int) ($row['source_id'] ?? 0)),
        'type_id' => max(0, (int) ($row['type_id'] ?? 0)),
        'sample_count' => max(0, (int) ($row['sample_count'] ?? 0)),
        'first_observed_at' => isset($row['first_observed_at']) && trim((string) $row['first_observed_at']) !== '' ? trim((string) $row['first_observed_at']) : null,
        'last_observed_at' => isset($row['last_observed_at']) && trim((string) $row['last_observed_at']) !== '' ? trim((string) $row['last_observed_at']) : null,
        'best_sell_price_min' => isset($row['best_sell_price_min']) && $row['best_sell_price_min'] !== null ? (float) $row['best_sell_price_min'] : null,
        'best_sell_price_max' => isset($row['best_sell_price_max']) && $row['best_sell_price_max'] !== null ? (float) $row['best_sell_price_max'] : null,
        'best_sell_price_sample_count' => max(0, (int) ($row['best_sell_price_sample_count'] ?? 0)),
        'best_sell_price_sum' => round((float) ($row['best_sell_price_sum'] ?? 0), 2),
        'best_sell_price_last' => isset($row['best_sell_price_last']) && $row['best_sell_price_last'] !== null ? (float) $row['best_sell_price_last'] : null,
        'best_buy_price_min' => isset($row['best_buy_price_min']) && $row['best_buy_price_min'] !== null ? (float) $row['best_buy_price_min'] : null,
        'best_buy_price_max' => isset($row['best_buy_price_max']) && $row['best_buy_price_max'] !== null ? (float) $row['best_buy_price_max'] : null,
        'best_buy_price_sample_count' => max(0, (int) ($row['best_buy_price_sample_count'] ?? 0)),
        'best_buy_price_sum' => round((float) ($row['best_buy_price_sum'] ?? 0), 2),
        'best_buy_price_last' => isset($row['best_buy_price_last']) && $row['best_buy_price_last'] !== null ? (float) $row['best_buy_price_last'] : null,
        'total_buy_volume_sum' => max(0, (int) ($row['total_buy_volume_sum'] ?? 0)),
        'total_sell_volume_sum' => max(0, (int) ($row['total_sell_volume_sum'] ?? 0)),
        'total_volume_sum' => max(0, (int) ($row['total_volume_sum'] ?? 0)),
        'buy_order_count_sum' => max(0, (int) ($row['buy_order_count_sum'] ?? 0)),
        'sell_order_count_sum' => max(0, (int) ($row['sell_order_count_sum'] ?? 0)),
    ];
}

function db_market_snapshot_rollup_rows_from_summary(string $resolution, array $summaryRows): array
{
    db_market_snapshot_rollup_table_name($resolution);

    $rollups = [];

    foreach ($summaryRows as $summaryRow) {
        $normalized = db_market_order_snapshots_summary_normalize_row($summaryRow);
        if ($normalized['source_type'] === ''
            || $normalized['source_id'] <= 0
            || $normalized['type_id'] <= 0
            || $normalized['observed_at'] === ''
        ) {
            continue;
        }

        $bucketStart = $resolution === '1d'
            ? gmdate('Y-m-d', strtotime($normalized['observed_at']) ?: time())
            : normalize_to_hour_bucket($normalized['observed_at']);
        $key = implode(':', [$bucketStart, $normalized['source_type'], $normalized['source_id'], $normalized['type_id']]);

        if (!isset($rollups[$key])) {
            $rollups[$key] = [
                'bucket_start' => $bucketStart,
                'source_type' => $normalized['source_type'],
                'source_id' => $normalized['source_id'],
                'type_id' => $normalized['type_id'],
                'sample_count' => 0,
                'first_observed_at' => $normalized['observed_at'],
                'last_observed_at' => $normalized['observed_at'],
                'best_sell_price_min' => null,
                'best_sell_price_max' => null,
                'best_sell_price_sample_count' => 0,
                'best_sell_price_sum' => 0.0,
                'best_sell_price_last' => null,
                'best_buy_price_min' => null,
                'best_buy_price_max' => null,
                'best_buy_price_sample_count' => 0,
                'best_buy_price_sum' => 0.0,
                'best_buy_price_last' => null,
                'total_buy_volume_sum' => 0,
                'total_sell_volume_sum' => 0,
                'total_volume_sum' => 0,
                'buy_order_count_sum' => 0,
                'sell_order_count_sum' => 0,
            ];
        }

        $rollup = &$rollups[$key];
        $rollup['sample_count']++;
        if (strcmp($normalized['observed_at'], (string) $rollup['first_observed_at']) < 0) {
            $rollup['first_observed_at'] = $normalized['observed_at'];
        }
        if (strcmp($normalized['observed_at'], (string) $rollup['last_observed_at']) >= 0) {
            $rollup['last_observed_at'] = $normalized['observed_at'];
            $rollup['best_sell_price_last'] = $normalized['best_sell_price'];
            $rollup['best_buy_price_last'] = $normalized['best_buy_price'];
        }

        if ($normalized['best_sell_price'] !== null) {
            $rollup['best_sell_price_min'] = $rollup['best_sell_price_min'] === null
                ? $normalized['best_sell_price']
                : min((float) $rollup['best_sell_price_min'], (float) $normalized['best_sell_price']);
            $rollup['best_sell_price_max'] = $rollup['best_sell_price_max'] === null
                ? $normalized['best_sell_price']
                : max((float) $rollup['best_sell_price_max'], (float) $normalized['best_sell_price']);
            $rollup['best_sell_price_sample_count']++;
            $rollup['best_sell_price_sum'] += (float) $normalized['best_sell_price'];
        }

        if ($normalized['best_buy_price'] !== null) {
            $rollup['best_buy_price_min'] = $rollup['best_buy_price_min'] === null
                ? $normalized['best_buy_price']
                : min((float) $rollup['best_buy_price_min'], (float) $normalized['best_buy_price']);
            $rollup['best_buy_price_max'] = $rollup['best_buy_price_max'] === null
                ? $normalized['best_buy_price']
                : max((float) $rollup['best_buy_price_max'], (float) $normalized['best_buy_price']);
            $rollup['best_buy_price_sample_count']++;
            $rollup['best_buy_price_sum'] += (float) $normalized['best_buy_price'];
        }

        $rollup['total_buy_volume_sum'] += $normalized['total_buy_volume'];
        $rollup['total_sell_volume_sum'] += $normalized['total_sell_volume'];
        $rollup['total_volume_sum'] += $normalized['total_volume'];
        $rollup['buy_order_count_sum'] += $normalized['buy_order_count'];
        $rollup['sell_order_count_sum'] += $normalized['sell_order_count'];
        unset($rollup);
    }

    return array_values(array_map(
        static function (array $row): array {
            $row['best_sell_price_sum'] = round((float) $row['best_sell_price_sum'], 2);
            $row['best_buy_price_sum'] = round((float) $row['best_buy_price_sum'], 2);

            return $row;
        },
        $rollups
    ));
}

function db_market_snapshot_rollup_bulk_upsert(string $resolution, array $rows, ?int $chunkSize = null): int
{
    db_market_snapshot_rollups_ensure();

    $table = db_market_snapshot_rollup_table_name($resolution);
    $normalizedRows = [];

    foreach ($rows as $row) {
        $normalized = db_market_snapshot_rollup_normalize_row($resolution, $row);
        if ($normalized['bucket_start'] === ''
            || $normalized['source_type'] === ''
            || $normalized['source_id'] <= 0
            || $normalized['type_id'] <= 0
        ) {
            continue;
        }

        $normalizedRows[] = $normalized;
    }

    return db_bulk_insert_or_upsert(
        $table,
        [
            'bucket_start',
            'source_type',
            'source_id',
            'type_id',
            'sample_count',
            'first_observed_at',
            'last_observed_at',
            'best_sell_price_min',
            'best_sell_price_max',
            'best_sell_price_sample_count',
            'best_sell_price_sum',
            'best_sell_price_last',
            'best_buy_price_min',
            'best_buy_price_max',
            'best_buy_price_sample_count',
            'best_buy_price_sum',
            'best_buy_price_last',
            'total_buy_volume_sum',
            'total_sell_volume_sum',
            'total_volume_sum',
            'buy_order_count_sum',
            'sell_order_count_sum',
        ],
        $normalizedRows,
        [
            'sample_count',
            'first_observed_at',
            'last_observed_at',
            'best_sell_price_min',
            'best_sell_price_max',
            'best_sell_price_sample_count',
            'best_sell_price_sum',
            'best_sell_price_last',
            'best_buy_price_min',
            'best_buy_price_max',
            'best_buy_price_sample_count',
            'best_buy_price_sum',
            'best_buy_price_last',
            'total_buy_volume_sum',
            'total_sell_volume_sum',
            'total_volume_sum',
            'buy_order_count_sum',
            'sell_order_count_sum',
        ],
        $chunkSize
    );
}

function db_market_snapshot_optimization_ensure(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db_ensure_table_index(db_market_orders_history_legacy_table(), 'idx_market_orders_history_observed_at', 'INDEX idx_market_orders_history_observed_at (observed_at)');
    db_market_orders_history_partitioned_schema_ensure();
    db_market_order_snapshots_summary_partitioned_schema_ensure();
    db_ensure_table_index('market_order_snapshots_summary', 'idx_snapshot_summary_observed', 'INDEX idx_snapshot_summary_observed (observed_at)');
    db_ensure_table_index(db_market_order_snapshots_summary_partitioned_table(), 'idx_snapshot_summary_observed', 'INDEX idx_snapshot_summary_observed (observed_at)');
    db_market_snapshot_rollups_ensure();
    db_market_order_current_projection_ensure();

    $ensured = true;
}

function db_market_source_snapshot_state_ensure(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db_market_snapshot_optimization_ensure();

    db_execute(
        "CREATE TABLE IF NOT EXISTS market_source_snapshot_state (
            source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            latest_current_observed_at DATETIME DEFAULT NULL,
            latest_summary_observed_at DATETIME DEFAULT NULL,
            current_order_count INT UNSIGNED NOT NULL DEFAULT 0,
            current_distinct_type_count INT UNSIGNED NOT NULL DEFAULT 0,
            summary_row_count INT UNSIGNED NOT NULL DEFAULT 0,
            last_synced_at DATETIME DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (source_type, source_id),
            KEY idx_market_source_snapshot_state_current (latest_current_observed_at),
            KEY idx_market_source_snapshot_state_summary (latest_summary_observed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    $ensured = true;
}

function db_market_source_snapshot_state_get(string $sourceType, int $sourceId): ?array
{
    db_market_source_snapshot_state_ensure();

    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);
    if ($safeSourceType === '' || $safeSourceId <= 0) {
        return null;
    }

    return db_select_one(
        'SELECT source_type, source_id, latest_current_observed_at, latest_summary_observed_at, current_order_count, current_distinct_type_count, summary_row_count, last_synced_at, created_at, updated_at
         FROM market_source_snapshot_state
         WHERE source_type = ?
           AND source_id = ?
         LIMIT 1',
        [$safeSourceType, $safeSourceId]
    );
}

function db_market_source_snapshot_state_upsert(array $row): bool
{
    db_market_source_snapshot_state_ensure();

    $sourceType = trim((string) ($row['source_type'] ?? ''));
    $sourceId = max(0, (int) ($row['source_id'] ?? 0));
    if ($sourceType === '' || $sourceId <= 0) {
        return false;
    }

    $existing = db_select_one(
        'SELECT current_order_count, current_distinct_type_count, summary_row_count
         FROM market_source_snapshot_state
         WHERE source_type = ?
           AND source_id = ?
         LIMIT 1',
        [$sourceType, $sourceId]
    ) ?? [];
    $latestCurrentObservedAt = isset($row['latest_current_observed_at']) && trim((string) $row['latest_current_observed_at']) !== ''
        ? trim((string) $row['latest_current_observed_at'])
        : null;
    $latestSummaryObservedAt = isset($row['latest_summary_observed_at']) && trim((string) $row['latest_summary_observed_at']) !== ''
        ? trim((string) $row['latest_summary_observed_at'])
        : null;
    $currentOrderCount = array_key_exists('current_order_count', $row)
        ? max(0, (int) $row['current_order_count'])
        : max(0, (int) ($existing['current_order_count'] ?? 0));
    $currentDistinctTypeCount = array_key_exists('current_distinct_type_count', $row)
        ? max(0, (int) $row['current_distinct_type_count'])
        : max(0, (int) ($existing['current_distinct_type_count'] ?? 0));
    $summaryRowCount = array_key_exists('summary_row_count', $row)
        ? max(0, (int) $row['summary_row_count'])
        : max(0, (int) ($existing['summary_row_count'] ?? 0));
    $lastSyncedAt = isset($row['last_synced_at']) && trim((string) $row['last_synced_at']) !== ''
        ? trim((string) $row['last_synced_at'])
        : ($latestCurrentObservedAt ?? $latestSummaryObservedAt);

    return db_execute(
        'INSERT INTO market_source_snapshot_state (
            source_type, source_id, latest_current_observed_at, latest_summary_observed_at, current_order_count, current_distinct_type_count, summary_row_count, last_synced_at
         ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?
         )
         ON DUPLICATE KEY UPDATE
            latest_current_observed_at = COALESCE(VALUES(latest_current_observed_at), latest_current_observed_at),
            latest_summary_observed_at = COALESCE(VALUES(latest_summary_observed_at), latest_summary_observed_at),
            current_order_count = VALUES(current_order_count),
            current_distinct_type_count = VALUES(current_distinct_type_count),
            summary_row_count = VALUES(summary_row_count),
            last_synced_at = COALESCE(VALUES(last_synced_at), last_synced_at),
            updated_at = CURRENT_TIMESTAMP',
        [
            $sourceType,
            $sourceId,
            $latestCurrentObservedAt,
            $latestSummaryObservedAt,
            $currentOrderCount,
            $currentDistinctTypeCount,
            $summaryRowCount,
            $lastSyncedAt,
        ]
    );
}


function db_market_orders_history_prune_before(string $cutoffObservedAt): int
{
    db_market_snapshot_optimization_ensure();

    $deleted = 0;
    foreach (db_market_orders_history_known_tables() as $table) {
        if ($table === db_market_orders_history_partitioned_table()) {
            $deleted += (int) (db_partitioned_table_retention_cleanup(
                $table,
                'observed_at',
                'observed_date',
                $cutoffObservedAt,
                'db_market_orders_history_partition_boundaries'
            )['rows_deleted'] ?? 0);
            continue;
        }

        $deleted += db_prune_before_datetime($table, 'observed_at', $cutoffObservedAt);
    }

    return $deleted;
}

function db_market_order_snapshots_summary_prune_before(string $cutoffObservedAt): int
{
    db_market_snapshot_optimization_ensure();

    $deleted = 0;
    foreach (db_market_order_snapshots_summary_known_tables() as $table) {
        if ($table === db_market_order_snapshots_summary_partitioned_table()) {
            $deleted += (int) (db_partitioned_table_retention_cleanup(
                $table,
                'observed_at',
                'observed_date',
                $cutoffObservedAt,
                'db_market_order_snapshots_summary_partition_boundaries'
            )['rows_deleted'] ?? 0);
            continue;
        }

        $deleted += db_prune_before_datetime($table, 'observed_at', $cutoffObservedAt);
    }

    return $deleted;
}

function db_market_history_tier_cleanup_specs(): array
{
    $rawTables = [];
    foreach (db_market_orders_history_known_tables() as $table) {
        $rawTables[] = ['table' => $table, 'column' => 'observed_at'];
    }
    foreach (db_market_order_snapshots_summary_known_tables() as $table) {
        $rawTables[] = ['table' => $table, 'column' => 'observed_at'];
    }

    return [
        'raw' => $rawTables,
        'hourly' => [
            ['table' => 'market_item_price_1h', 'column' => 'bucket_start'],
            ['table' => 'market_item_stock_1h', 'column' => 'bucket_start'],
        ],
        'daily' => [
            ['table' => 'market_item_price_1d', 'column' => 'bucket_start'],
            ['table' => 'market_item_stock_1d', 'column' => 'bucket_start'],
            ['table' => 'market_history_daily', 'column' => 'trade_date'],
            ['table' => 'market_hub_local_history_daily', 'column' => 'trade_date'],
        ],
    ];
}

function db_market_history_tier_cutoff(string $tier): string
{
    $retentionDays = db_market_history_retention_days($tier);

    return match ($tier) {
        'daily' => gmdate('Y-m-d', strtotime('-' . $retentionDays . ' days')),
        'hourly' => gmdate('Y-m-d H:00:00', strtotime('-' . $retentionDays . ' days')),
        default => gmdate('Y-m-d H:i:s', strtotime('-' . $retentionDays . ' days')),
    };
}

function db_market_history_tier_retention_cleanup(int $limitPerTable = 5000): array
{
    db_market_snapshot_optimization_ensure();
    db_time_series_analytics_ensure_schema();

    $safeLimit = max(100, min(50000, $limitPerTable));
    $deleted = [];
    $rowsWritten = 0;

    foreach (db_market_history_tier_cleanup_specs() as $tier => $tables) {
        $cutoff = db_market_history_tier_cutoff($tier);
        $retentionDays = db_market_history_retention_days($tier);

        foreach ($tables as $spec) {
            $table = (string) $spec['table'];
            $method = 'delete';
            $droppedPartitions = [];
            $rowsDeleted = 0;

            if ($table === db_market_orders_history_partitioned_table()) {
                $cleanup = db_partitioned_table_retention_cleanup(
                    $table,
                    (string) $spec['column'],
                    'observed_date',
                    $cutoff,
                    'db_market_orders_history_partition_boundaries',
                    $safeLimit
                );
                $rowsDeleted = (int) ($cleanup['rows_deleted'] ?? 0);
                $method = (string) ($cleanup['method'] ?? $method);
                $droppedPartitions = array_values((array) ($cleanup['dropped_partitions'] ?? []));
            } elseif ($table === db_market_order_snapshots_summary_partitioned_table()) {
                $cleanup = db_partitioned_table_retention_cleanup(
                    $table,
                    (string) $spec['column'],
                    'observed_date',
                    $cutoff,
                    'db_market_order_snapshots_summary_partition_boundaries',
                    $safeLimit
                );
                $rowsDeleted = (int) ($cleanup['rows_deleted'] ?? 0);
                $method = (string) ($cleanup['method'] ?? $method);
                $droppedPartitions = array_values((array) ($cleanup['dropped_partitions'] ?? []));
            } else {
                $rowsDeleted = db_prune_before_datetime($table, (string) $spec['column'], $cutoff, $safeLimit);
            }

            $deleted[$table] = [
                'tier' => $tier,
                'rows_deleted' => $rowsDeleted,
                'cutoff' => $cutoff,
                'retention_days' => $retentionDays,
                'method' => $method,
                'dropped_partitions' => $droppedPartitions,
            ];
            $rowsWritten += $rowsDeleted;
        }
    }

    return [
        'rows_written' => $rowsWritten,
        'deleted' => $deleted,
    ];
}

function db_table_estimated_rows(string $table): ?int
{
    $row = db_select_one(
        'SELECT table_rows
         FROM information_schema.tables
         WHERE table_schema = DATABASE()
           AND table_name = ?
         LIMIT 1',
        [$table]
    );

    if ($row === null || !isset($row['table_rows'])) {
        return null;
    }

    return max(0, (int) $row['table_rows']);
}

function db_market_history_partition_table_diagnostic(
    string $logicalTable,
    string $partitionedTable,
    string $retentionTier,
    string $readMode,
    string $writeMode,
    array $partitions,
    int $futureMonths = 3
): array {
    $retentionDays = db_market_history_retention_days($retentionTier);
    $cutoff = db_market_history_tier_cutoff($retentionTier);
    $oldestPartition = $partitions[0] ?? null;
    $nonMaxPartitions = array_values(array_filter($partitions, static fn (array $partition): bool => empty($partition['is_maxvalue'])));
    $latestFinitePartition = $nonMaxPartitions === [] ? null : $nonMaxPartitions[array_key_last($nonMaxPartitions)];
    $currentMonthStart = new DateTimeImmutable(gmdate('Y-m-01'));
    $missingFuturePartitions = [];

    $partitionNames = array_map(static fn (array $partition): string => (string) ($partition['partition_name'] ?? ''), $partitions);
    for ($offset = 0; $offset <= max(1, min(24, $futureMonths)); $offset++) {
        $expected = db_market_orders_history_partition_name_for_month($currentMonthStart->modify(sprintf('+%d month', $offset)));
        if (!in_array($expected, $partitionNames, true)) {
            $missingFuturePartitions[] = $expected;
        }
    }

    return [
        'logical_table' => $logicalTable,
        'partitioned_table' => $partitionedTable,
        'read_mode' => $readMode,
        'write_mode' => $writeMode,
        'retention_tier' => $retentionTier,
        'retention_days' => $retentionDays,
        'retention_cutoff' => $cutoff,
        'partition_count' => count($partitions),
        'oldest_partition' => $oldestPartition,
        'newest_partition' => $latestFinitePartition,
        'future_partitions_exist' => $missingFuturePartitions === [],
        'missing_future_partitions' => $missingFuturePartitions,
        'partitions' => $partitions,
        'estimated_rows' => db_table_estimated_rows($partitionedTable),
    ];
}

function db_market_history_partition_diagnostics(): array
{
    db_market_snapshot_optimization_ensure();

    $partitionedTables = [
        db_market_history_partition_table_diagnostic(
            db_market_orders_history_legacy_table(),
            db_market_orders_history_partitioned_table(),
            'raw',
            db_market_orders_history_cutover_mode('read'),
            db_market_orders_history_cutover_mode('write'),
            db_market_orders_history_partition_boundaries(db_market_orders_history_partitioned_table())
        ),
        db_market_history_partition_table_diagnostic(
            db_market_order_snapshots_summary_legacy_table(),
            db_market_order_snapshots_summary_partitioned_table(),
            'raw',
            db_market_order_snapshots_summary_cutover_mode('read'),
            db_market_order_snapshots_summary_cutover_mode('write'),
            db_market_order_snapshots_summary_partition_boundaries(db_market_order_snapshots_summary_partitioned_table())
        ),
    ];

    return [
        'partitioned_tables' => $partitionedTables,
        'evaluation_tables' => [
            [
                'table' => 'market_history_daily',
                'estimated_rows' => db_table_estimated_rows('market_history_daily'),
                'recommended_action' => 'keep_unpartitioned',
                'reason' => 'Daily rollup volume remains modest versus the raw snapshot tier, so partitioning adds more complexity than value right now.',
            ],
            [
                'table' => 'killmail_events',
                'estimated_rows' => db_table_estimated_rows('killmail_events'),
                'recommended_action' => 'keep_unpartitioned',
                'reason' => 'Current table size is small and retention is not driven by short-lived raw windows.',
            ],
        ],
    ];
}

function db_market_orders_current_latest_observed_at(string $sourceType, int $sourceId): string
{
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);

    if ($safeSourceType === '' || $safeSourceId <= 0) {
        return '';
    }

    $state = db_market_source_snapshot_state_get($safeSourceType, $safeSourceId);
    $latestObservedAt = trim((string) ($state['latest_current_observed_at'] ?? ''));
    if ($latestObservedAt !== '') {
        return $latestObservedAt;
    }

    $projectionRows = db_market_order_current_projection_latest_rows($safeSourceType, $safeSourceId);
    $latestObservedAt = trim((string) ($projectionRows[0]['observed_at'] ?? ''));
    if ($latestObservedAt !== '') {
        db_market_source_snapshot_state_upsert([
            'source_type' => $safeSourceType,
            'source_id' => $safeSourceId,
            'latest_current_observed_at' => $latestObservedAt,
            'current_distinct_type_count' => count($projectionRows),
            'last_synced_at' => $latestObservedAt,
        ]);
    }

    return $latestObservedAt;
}

function db_market_orders_current_latest_snapshot_rows(string $sourceType, int $sourceId): array
{
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);

    if ($safeSourceType === '' || $safeSourceId <= 0) {
        return [];
    }

    $latestObservedAt = db_market_orders_current_latest_observed_at($safeSourceType, $safeSourceId);

    if ($latestObservedAt !== '') {
        return db_select(
            'SELECT
                source_type,
                source_id,
                type_id,
                order_id,
                is_buy_order,
                price,
                volume_remain,
                volume_total,
                min_volume,
                `range`,
                duration,
                issued,
                expires,
                observed_at
             FROM market_orders_current
             WHERE source_type = ?
               AND source_id = ?
               AND observed_at = ?
             ORDER BY type_id ASC, is_buy_order ASC, price ASC, order_id ASC',
            [$safeSourceType, $safeSourceId, $latestObservedAt]
        );
    }

    $rows = db_select(
        'SELECT
            source_type,
            source_id,
            type_id,
            order_id,
            is_buy_order,
            price,
            volume_remain,
            volume_total,
            min_volume,
            `range`,
            duration,
            issued,
            expires,
            observed_at
         FROM market_orders_current
         WHERE source_type = ?
           AND source_id = ?
           AND observed_at = (
                SELECT MAX(observed_at)
                FROM market_orders_current
                WHERE source_type = ?
                  AND source_id = ?
           )
         ORDER BY type_id ASC, is_buy_order ASC, price ASC, order_id ASC',
        [$safeSourceType, $safeSourceId, $safeSourceType, $safeSourceId]
    );

    if ($rows !== []) {
        $latestObservedAt = trim((string) ($rows[0]['observed_at'] ?? ''));
        if ($latestObservedAt !== '') {
            db_market_source_snapshot_state_upsert([
                'source_type' => $safeSourceType,
                'source_id' => $safeSourceId,
                'latest_current_observed_at' => $latestObservedAt,
                'current_order_count' => count($rows),
                'current_distinct_type_count' => count(array_unique(array_filter(array_map(
                    static fn (array $row): int => (int) ($row['type_id'] ?? 0),
                    $rows
                )))),
                'last_synced_at' => $latestObservedAt,
            ]);
        }
    }

    return $rows;
}

function db_market_order_snapshots_summary_normalize_row(array $row): array
{
    return [
        'source_type' => trim((string) ($row['source_type'] ?? '')),
        'source_id' => max(0, (int) ($row['source_id'] ?? 0)),
        'type_id' => max(0, (int) ($row['type_id'] ?? 0)),
        'observed_at' => trim((string) ($row['observed_at'] ?? '')),
        'best_sell_price' => isset($row['best_sell_price']) && $row['best_sell_price'] !== null ? (float) $row['best_sell_price'] : null,
        'best_buy_price' => isset($row['best_buy_price']) && $row['best_buy_price'] !== null ? (float) $row['best_buy_price'] : null,
        'total_buy_volume' => max(0, (int) ($row['total_buy_volume'] ?? 0)),
        'total_sell_volume' => max(0, (int) ($row['total_sell_volume'] ?? 0)),
        'total_volume' => max(0, (int) ($row['total_volume'] ?? 0)),
        'buy_order_count' => max(0, (int) ($row['buy_order_count'] ?? 0)),
        'sell_order_count' => max(0, (int) ($row['sell_order_count'] ?? 0)),
    ];
}

function db_market_order_snapshots_summary_bulk_upsert(array $summaryRows, ?int $chunkSize = null): int
{
    $normalizedRows = [];

    foreach ($summaryRows as $row) {
        $normalized = db_market_order_snapshots_summary_normalize_row($row);
        if ($normalized['source_type'] === ''
            || $normalized['source_id'] <= 0
            || $normalized['type_id'] <= 0
            || $normalized['observed_at'] === ''
        ) {
            continue;
        }

        $normalizedRows[] = $normalized;
    }

    if ($normalizedRows === []) {
        return 0;
    }

    $writeTables = db_market_order_snapshots_summary_write_tables();
    if (in_array(db_market_order_snapshots_summary_partitioned_table(), $writeTables, true)) {
        db_market_order_snapshots_summary_partitioned_schema_ensure();
        $maxObservedAt = '';
        foreach ($normalizedRows as $row) {
            $observedAt = (string) ($row['observed_at'] ?? '');
            if ($observedAt !== '' && strcmp($observedAt, $maxObservedAt) > 0) {
                $maxObservedAt = $observedAt;
            }
        }
        if ($maxObservedAt !== '') {
            $maxTimestamp = strtotime($maxObservedAt);
            if ($maxTimestamp !== false) {
                $monthsAhead = max(
                    0,
                    (((int) gmdate('Y', $maxTimestamp)) * 12 + (int) gmdate('n', $maxTimestamp))
                    - (((int) gmdate('Y')) * 12 + (int) gmdate('n'))
                );
                db_market_order_snapshots_summary_ensure_future_monthly_partitions(
                    $monthsAhead + 1,
                    db_market_order_snapshots_summary_partitioned_table()
                );
            }
        }
    }

    $written = 0;
    foreach ($writeTables as $table) {
        $rowsForTable = $table === db_market_order_snapshots_summary_partitioned_table()
            ? db_market_orders_history_rows_with_observed_date($normalizedRows)
            : $normalizedRows;
        $columns = [
            'source_type',
            'source_id',
            'type_id',
            'observed_at',
            'best_sell_price',
            'best_buy_price',
            'total_buy_volume',
            'total_sell_volume',
            'total_volume',
            'buy_order_count',
            'sell_order_count',
        ];
        if ($table === db_market_order_snapshots_summary_partitioned_table()) {
            $columns[] = 'observed_date';
        }

        $written += db_bulk_insert_or_upsert(
            $table,
            $columns,
            $rowsForTable,
            [
                'best_sell_price',
                'best_buy_price',
                'total_buy_volume',
                'total_sell_volume',
                'total_volume',
                'buy_order_count',
                'sell_order_count',
            ],
            $chunkSize
        );
    }

    db_market_order_current_projection_refresh_snapshots($normalizedRows, $chunkSize);

    return $written;
}

function db_market_orders_snapshot_metrics_window_raw(string $sourceType, int $sourceId, string $startObservedAt): array
{
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);
    $safeStartObservedAt = trim($startObservedAt);

    if ($safeSourceType === '' || $safeSourceId <= 0 || $safeStartObservedAt === '') {
        return [];
    }

    $historyTable = db_validate_identifier(db_market_orders_history_read_table());

    $rows = db_select(
        sprintf(
            "SELECT
                snapshots.source_id,
                snapshots.type_id,
                snapshots.observed_at,
                MIN(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.min_price ELSE NULL END) AS best_sell_price,
                MAX(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.max_price ELSE NULL END) AS best_buy_price,
                SUM(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.volume_remain ELSE 0 END) AS total_buy_volume,
                SUM(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.volume_remain ELSE 0 END) AS total_sell_volume,
                SUM(snapshots.volume_remain) AS total_volume,
                SUM(CASE WHEN snapshots.is_buy_order = 1 THEN snapshots.order_count ELSE 0 END) AS buy_order_count,
                SUM(CASE WHEN snapshots.is_buy_order = 0 THEN snapshots.order_count ELSE 0 END) AS sell_order_count
             FROM (
                SELECT
                    moh.source_id,
                    moh.type_id,
                    moh.is_buy_order,
                    MIN(moh.price) AS min_price,
                    MAX(moh.price) AS max_price,
                    SUM(moh.volume_remain) AS volume_remain,
                    COUNT(*) AS order_count,
                    moh.observed_at
                FROM %s moh
                WHERE moh.source_type = ?
                  AND moh.source_id = ?
                  AND moh.observed_at >= ?
                GROUP BY moh.source_id, moh.type_id, moh.is_buy_order, moh.observed_at

                UNION ALL

                SELECT
                    moc.source_id,
                    moc.type_id,
                    moc.is_buy_order,
                    MIN(moc.price) AS min_price,
                    MAX(moc.price) AS max_price,
                    SUM(moc.volume_remain) AS volume_remain,
                    COUNT(*) AS order_count,
                    moc.observed_at
                FROM market_orders_current moc
                LEFT JOIN (
                    SELECT DISTINCT observed_at
                    FROM %s
                    WHERE source_type = ?
                      AND source_id = ?
                      AND observed_at >= ?
                ) history_observed
                  ON history_observed.observed_at = moc.observed_at
                WHERE moc.source_type = ?
                  AND moc.source_id = ?
                  AND moc.observed_at >= ?
                  AND history_observed.observed_at IS NULL
                GROUP BY moc.source_id, moc.type_id, moc.is_buy_order, moc.observed_at
             ) snapshots
             GROUP BY snapshots.source_id, snapshots.type_id, snapshots.observed_at
             ORDER BY snapshots.observed_at ASC, snapshots.type_id ASC",
            $historyTable,
            $historyTable
        ),
        [
            $safeSourceType,
            $safeSourceId,
            $safeStartObservedAt,
            $safeSourceType,
            $safeSourceId,
            $safeStartObservedAt,
            $safeSourceType,
            $safeSourceId,
            $safeStartObservedAt,
        ]
    );

    return array_map(function (array $row) use ($safeSourceType): array {
        return [
            'source_type' => $safeSourceType,
            'source_id' => (int) ($row['source_id'] ?? 0),
            'type_id' => (int) ($row['type_id'] ?? 0),
            'observed_at' => (string) ($row['observed_at'] ?? ''),
            'best_sell_price' => isset($row['best_sell_price']) && $row['best_sell_price'] !== null ? (float) $row['best_sell_price'] : null,
            'best_buy_price' => isset($row['best_buy_price']) && $row['best_buy_price'] !== null ? (float) $row['best_buy_price'] : null,
            'total_buy_volume' => max(0, (int) ($row['total_buy_volume'] ?? 0)),
            'total_sell_volume' => max(0, (int) ($row['total_sell_volume'] ?? 0)),
            'total_volume' => max(0, (int) ($row['total_volume'] ?? 0)),
            'buy_order_count' => max(0, (int) ($row['buy_order_count'] ?? 0)),
            'sell_order_count' => max(0, (int) ($row['sell_order_count'] ?? 0)),
        ];
    }, $rows);
}

function db_market_orders_snapshot_metrics_window(string $sourceType, int $sourceId, string $startObservedAt): array
{
    $result = db_market_orders_snapshot_metrics_window_ensure_summary($sourceType, $sourceId, $startObservedAt);

    return is_array($result['rows'] ?? null) ? $result['rows'] : [];
}

function db_market_orders_snapshot_metrics_window_ensure_summary(string $sourceType, int $sourceId, string $startObservedAt): array
{
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);
    $safeStartObservedAt = trim($startObservedAt);

    if ($safeSourceType === '' || $safeSourceId <= 0 || $safeStartObservedAt === '') {
        return [
            'rows' => [],
            'summary_rows_written' => 0,
            'loaded_from_summary' => false,
        ];
    }

    $summaryTable = db_validate_identifier(db_market_order_snapshots_summary_read_table());
    $summaryRows = db_select_cached(
        "SELECT
            source_type,
            source_id,
            type_id,
            observed_at,
            best_sell_price,
            best_buy_price,
            total_buy_volume,
            total_sell_volume,
            total_volume,
            buy_order_count,
            sell_order_count
         FROM {$summaryTable}
         WHERE source_type = ?
           AND source_id = ?
           AND observed_at >= ?
         ORDER BY observed_at ASC, type_id ASC",
        [$safeSourceType, $safeSourceId, $safeStartObservedAt],
        60,
        'market.snapshot.metrics'
    );

    $normalizedSummaryRows = array_map('db_market_order_snapshots_summary_normalize_row', $summaryRows);
    $stateUpdated = false;
    $latestObservedAt = '';
    foreach ($normalizedSummaryRows as $row) {
        $observedAt = trim((string) ($row['observed_at'] ?? ''));
        if ($observedAt !== '' && strcmp($observedAt, $latestObservedAt) > 0) {
            $latestObservedAt = $observedAt;
        }
    }

    $rawRowsStartObservedAt = $latestObservedAt !== '' ? $latestObservedAt : $safeStartObservedAt;
    $rawRows = db_market_orders_snapshot_metrics_window_raw($safeSourceType, $safeSourceId, $rawRowsStartObservedAt);
    $summaryRowsWritten = $rawRows === [] ? 0 : db_market_order_snapshots_summary_bulk_upsert($rawRows);
    if ($rawRows !== []) {
        $latestRawObservedAt = trim((string) ($rawRows[array_key_last($rawRows)]['observed_at'] ?? ''));
        if ($latestRawObservedAt !== '') {
            db_market_source_snapshot_state_upsert([
                'source_type' => $safeSourceType,
                'source_id' => $safeSourceId,
                'latest_summary_observed_at' => $latestRawObservedAt,
                'summary_row_count' => count($rawRows),
                'last_synced_at' => $latestRawObservedAt,
            ]);
            $stateUpdated = true;
        }
    }

    if (!$stateUpdated && $latestObservedAt !== '') {
        db_market_source_snapshot_state_upsert([
            'source_type' => $safeSourceType,
            'source_id' => $safeSourceId,
            'latest_summary_observed_at' => $latestObservedAt,
            'summary_row_count' => count(array_filter(
                $normalizedSummaryRows,
                static fn (array $row): bool => (string) ($row['observed_at'] ?? '') === $latestObservedAt
            )),
            'last_synced_at' => $latestObservedAt,
        ]);
    }

    if ($normalizedSummaryRows === []) {
        return [
            'rows' => $rawRows,
            'summary_rows_written' => $summaryRowsWritten,
            'loaded_from_summary' => false,
        ];
    }

    $mergedRows = [];
    foreach (array_merge($normalizedSummaryRows, $rawRows) as $row) {
        $rowKey = implode(':', [
            (string) ($row['source_type'] ?? ''),
            (string) ($row['source_id'] ?? ''),
            (string) ($row['type_id'] ?? ''),
            (string) ($row['observed_at'] ?? ''),
        ]);
        if ($rowKey === ':::') {
            continue;
        }

        $mergedRows[$rowKey] = db_market_order_snapshots_summary_normalize_row($row);
    }

    uasort($mergedRows, static function (array $left, array $right): int {
        $observedCompare = strcmp((string) ($left['observed_at'] ?? ''), (string) ($right['observed_at'] ?? ''));
        if ($observedCompare !== 0) {
            return $observedCompare;
        }

        return ((int) ($left['type_id'] ?? 0)) <=> ((int) ($right['type_id'] ?? 0));
    });

    return [
        'rows' => array_values($mergedRows),
        'summary_rows_written' => $summaryRowsWritten,
        'loaded_from_summary' => true,
    ];
}


function db_market_history_daily_bulk_upsert(array $historyRows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert(
        'market_history_daily',
        [
            'source_type',
            'source_id',
            'type_id',
            'trade_date',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'average_price',
            'volume',
            'order_count',
            'source_label',
            'observed_at',
        ],
        $historyRows,
        [
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'average_price',
            'volume',
            'order_count',
            'source_label',
            'observed_at',
        ],
        $chunkSize
    );
}

function db_market_history_daily_insert(array $historyRows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert(
        'market_history_daily',
        [
            'source_type',
            'source_id',
            'type_id',
            'trade_date',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'average_price',
            'volume',
            'order_count',
            'source_label',
            'observed_at',
        ],
        $historyRows,
        [],
        $chunkSize
    );
}

function db_market_hub_local_history_daily_normalize_spread_percent(mixed $value): ?float
{
    if ($value === null || $value === '') {
        return null;
    }

    $spreadPercent = (float) $value;
    if (!is_finite($spreadPercent)) {
        return null;
    }

    $spreadPercent = round($spreadPercent, 4);
    // Keep local-history writes compatible with legacy installs that still
    // have a narrower DECIMAL definition for spread_percent. Extremely large
    // values are usually caused by near-zero buy orders and are not useful for
    // dashboard trend commentary, so drop them instead of failing the sync.
    $maxAbsValue = 9999.9999;
    if (abs($spreadPercent) > $maxAbsValue) {
        return null;
    }

    return $spreadPercent;
}

function db_market_hub_local_history_daily_normalize_row(array $row): array
{
    $normalizeNullableFloat = static function (mixed $value): ?float {
        if ($value === null || $value === '') {
            return null;
        }

        return (float) $value;
    };

    return [
        'source' => trim((string) ($row['source'] ?? '')),
        'source_id' => max(0, (int) ($row['source_id'] ?? 0)),
        'type_id' => max(0, (int) ($row['type_id'] ?? 0)),
        'trade_date' => (string) ($row['trade_date'] ?? ''),
        'open_price' => (float) ($row['open_price'] ?? 0),
        'high_price' => (float) ($row['high_price'] ?? 0),
        'low_price' => (float) ($row['low_price'] ?? 0),
        'close_price' => (float) ($row['close_price'] ?? 0),
        'buy_price' => $normalizeNullableFloat($row['buy_price'] ?? null),
        'sell_price' => $normalizeNullableFloat($row['sell_price'] ?? null),
        'spread_value' => $normalizeNullableFloat($row['spread_value'] ?? null),
        'spread_percent' => db_market_hub_local_history_daily_normalize_spread_percent($row['spread_percent'] ?? null),
        'volume' => max(0, (int) ($row['volume'] ?? 0)),
        'buy_order_count' => max(0, (int) ($row['buy_order_count'] ?? 0)),
        'sell_order_count' => max(0, (int) ($row['sell_order_count'] ?? 0)),
        'captured_at' => (string) ($row['captured_at'] ?? ''),
    ];
}

function db_market_hub_local_history_daily_bulk_upsert(array $historyRows, ?int $chunkSize = null): int
{
    $normalizedRows = [];

    foreach ($historyRows as $row) {
        $normalizedRow = db_market_hub_local_history_daily_normalize_row($row);
        if ($normalizedRow['source'] === ''
            || $normalizedRow['source_id'] <= 0
            || $normalizedRow['type_id'] <= 0
            || $normalizedRow['trade_date'] === ''
            || $normalizedRow['captured_at'] === ''
        ) {
            continue;
        }

        $normalizedRows[] = $normalizedRow;
    }

    return db_bulk_insert_or_upsert(
        'market_hub_local_history_daily',
        [
            'source',
            'source_id',
            'type_id',
            'trade_date',
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'buy_price',
            'sell_price',
            'spread_value',
            'spread_percent',
            'volume',
            'buy_order_count',
            'sell_order_count',
            'captured_at',
        ],
        $normalizedRows,
        [
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'buy_price',
            'sell_price',
            'spread_value',
            'spread_percent',
            'volume',
            'buy_order_count',
            'sell_order_count',
            'captured_at',
        ],
        $chunkSize
    );
}

function db_market_hub_local_history_daily_select_type_ids(string $source, int $sourceId, int $typeLimit = 120): array
{
    $safeTypeLimit = max(1, min($typeLimit, 500));
    $rows = db_select_cached(
        "SELECT ranked.type_id
         FROM (
            SELECT
                type_id,
                MAX(trade_date) AS latest_trade_date,
                SUM(volume) AS total_volume
            FROM market_hub_local_history_daily
            WHERE source = ?
              AND source_id = ?
            GROUP BY type_id
         ) ranked
         ORDER BY ranked.latest_trade_date DESC, ranked.total_volume DESC
         LIMIT {$safeTypeLimit}",
        [$source, $sourceId]
        ,
        60,
        'market.hub.local.types'
    );

    return array_values(array_filter(array_map(static fn (array $row): int => (int) ($row['type_id'] ?? 0), $rows), static fn (int $typeId): bool => $typeId > 0));
}

function db_market_hub_local_history_daily_normalize_result_row(array $row): array
{
    return [
        'type_id' => (int) ($row['type_id'] ?? 0),
        'type_name' => (string) ($row['type_name'] ?? ''),
        'trade_date' => (string) ($row['trade_date'] ?? ''),
        'open_price' => isset($row['open_price']) ? (float) $row['open_price'] : 0.0,
        'high_price' => isset($row['high_price']) ? (float) $row['high_price'] : 0.0,
        'low_price' => isset($row['low_price']) ? (float) $row['low_price'] : 0.0,
        'close_price' => isset($row['close_price']) ? (float) $row['close_price'] : 0.0,
        'buy_price' => isset($row['buy_price']) && $row['buy_price'] !== null ? (float) $row['buy_price'] : null,
        'sell_price' => isset($row['sell_price']) && $row['sell_price'] !== null ? (float) $row['sell_price'] : null,
        'spread_value' => isset($row['spread_value']) && $row['spread_value'] !== null ? (float) $row['spread_value'] : null,
        'spread_percent' => isset($row['spread_percent']) && $row['spread_percent'] !== null ? (float) $row['spread_percent'] : null,
        'volume' => (int) ($row['volume'] ?? 0),
        'buy_order_count' => (int) ($row['buy_order_count'] ?? 0),
        'sell_order_count' => (int) ($row['sell_order_count'] ?? 0),
        'observed_at' => (string) ($row['observed_at'] ?? ''),
    ];
}

function db_market_hub_local_history_daily_by_trade_date(string $source, int $sourceId, string $tradeDate): array
{
    $safeSource = trim($source);
    $safeSourceId = max(0, $sourceId);
    $safeTradeDate = trim($tradeDate);

    if ($safeSource === '' || $safeSourceId <= 0 || $safeTradeDate === '') {
        return [];
    }

    $rows = db_select(
        'SELECT
            type_id,
            trade_date,
            open_price,
            high_price,
            low_price,
            close_price,
            buy_price,
            sell_price,
            spread_value,
            spread_percent,
            volume,
            buy_order_count,
            sell_order_count,
            captured_at AS observed_at
         FROM market_hub_local_history_daily
         WHERE source = ?
           AND source_id = ?
           AND trade_date = ?
         ORDER BY type_id ASC',
        [$safeSource, $safeSourceId, $safeTradeDate]
    );

    return array_map('db_market_hub_local_history_daily_normalize_result_row', $rows);
}

function db_market_hub_local_history_daily_recent_window(string $source, int $sourceId, int $days = 8, int $typeLimit = 120): array
{
    $safeSource = trim($source);
    $safeSourceId = max(0, $sourceId);
    $safeDays = max(1, min($days, 60));

    if ($safeSource === '' || $safeSourceId <= 0) {
        return [];
    }

    $typeIds = db_market_hub_local_history_daily_select_type_ids($safeSource, $safeSourceId, $typeLimit);
    if ($typeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($typeIds), '?'));
    $params = array_merge([$safeSource, $safeSourceId], $typeIds);
    $rows = db_select_cached(
        "SELECT
            mlhd.type_id,
            rit.type_name,
            mlhd.trade_date,
            mlhd.close_price,
            mlhd.buy_price,
            mlhd.sell_price,
            mlhd.spread_value,
            mlhd.spread_percent,
            mlhd.volume,
            mlhd.buy_order_count,
            mlhd.sell_order_count,
            mlhd.captured_at AS observed_at
         FROM market_hub_local_history_daily mlhd
         LEFT JOIN ref_item_types rit ON rit.type_id = mlhd.type_id
         WHERE mlhd.source = ?
           AND mlhd.source_id = ?
           AND mlhd.type_id IN ({$placeholders})
           AND mlhd.trade_date >= DATE_SUB(UTC_DATE(), INTERVAL {$safeDays} DAY)
         ORDER BY mlhd.type_id ASC, mlhd.trade_date DESC, mlhd.id DESC",
        $params,
        60,
        'market.hub.local.recent'
    );

    return array_map('db_market_hub_local_history_daily_normalize_result_row', $rows);
}

function db_mysql_supports_window_functions(): bool
{
    static $supportsWindowFunctions = null;

    if ($supportsWindowFunctions !== null) {
        return $supportsWindowFunctions;
    }

    $versionRow = db_select_one('SELECT VERSION() AS version_string');
    $versionString = strtolower((string) ($versionRow['version_string'] ?? ''));

    if ($versionString === '') {
        $supportsWindowFunctions = false;

        return $supportsWindowFunctions;
    }

    if (str_contains($versionString, 'mariadb')) {
        $normalizedVersion = preg_replace('/[^0-9.].*/', '', $versionString) ?: '0';
        $supportsWindowFunctions = version_compare($normalizedVersion, '10.2.0', '>=');

        return $supportsWindowFunctions;
    }

    $normalizedVersion = preg_replace('/[^0-9.].*/', '', $versionString) ?: '0';
    $supportsWindowFunctions = version_compare($normalizedVersion, '8.0.0', '>=');

    return $supportsWindowFunctions;
}

function db_market_hub_local_history_daily_latest_points_by_type(
    string $source,
    int $sourceId,
    array $typeIds = [],
    int $pointsPerType = 2,
    int $typeLimit = 120
): array {
    $safeSource = trim($source);
    $safeSourceId = max(0, $sourceId);
    $safePointsPerType = max(1, min($pointsPerType, 5));
    $safeTypeLimit = max(1, min($typeLimit, 500));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));

    if ($safeSource === '' || $safeSourceId <= 0) {
        return [];
    }

    if ($normalizedTypeIds === []) {
        $normalizedTypeIds = db_market_hub_local_history_daily_select_type_ids($safeSource, $safeSourceId, $safeTypeLimit);
    }

    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $params = array_merge([$safeSource, $safeSourceId], $normalizedTypeIds);
    $sql = db_mysql_supports_window_functions()
        ? "SELECT
                ranked.type_id,
                ranked.type_name,
                ranked.trade_date,
                ranked.close_price,
                ranked.buy_price,
                ranked.sell_price,
                ranked.spread_value,
                ranked.spread_percent,
                ranked.volume,
                ranked.buy_order_count,
                ranked.sell_order_count,
                ranked.observed_at
           FROM (
                SELECT
                    mlhd.id,
                    mlhd.type_id,
                    rit.type_name,
                    mlhd.trade_date,
                    mlhd.close_price,
                    mlhd.buy_price,
                    mlhd.sell_price,
                    mlhd.spread_value,
                    mlhd.spread_percent,
                    mlhd.volume,
                    mlhd.buy_order_count,
                    mlhd.sell_order_count,
                    mlhd.captured_at AS observed_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY mlhd.source, mlhd.source_id, mlhd.type_id
                        ORDER BY mlhd.trade_date DESC, mlhd.id DESC
                    ) AS row_number
                FROM market_hub_local_history_daily mlhd
                LEFT JOIN ref_item_types rit ON rit.type_id = mlhd.type_id
                WHERE mlhd.source = ?
                  AND mlhd.source_id = ?
                  AND mlhd.type_id IN ({$placeholders})
           ) ranked
           WHERE ranked.row_number <= {$safePointsPerType}
           ORDER BY ranked.type_id ASC, ranked.trade_date DESC, ranked.id DESC"
        : "SELECT
                mlhd.type_id,
                rit.type_name,
                mlhd.trade_date,
                mlhd.close_price,
                mlhd.buy_price,
                mlhd.sell_price,
                mlhd.spread_value,
                mlhd.spread_percent,
                mlhd.volume,
                mlhd.buy_order_count,
                mlhd.sell_order_count,
                mlhd.captured_at AS observed_at
           FROM market_hub_local_history_daily mlhd
           LEFT JOIN ref_item_types rit ON rit.type_id = mlhd.type_id
           WHERE mlhd.source = ?
             AND mlhd.source_id = ?
             AND mlhd.type_id IN ({$placeholders})
             AND (
                  SELECT COUNT(*)
                  FROM market_hub_local_history_daily newer
                  WHERE newer.source = mlhd.source
                    AND newer.source_id = mlhd.source_id
                    AND newer.type_id = mlhd.type_id
                    AND (
                          newer.trade_date > mlhd.trade_date
                          OR (newer.trade_date = mlhd.trade_date AND newer.id > mlhd.id)
                    )
             ) < {$safePointsPerType}
           ORDER BY mlhd.type_id ASC, mlhd.trade_date DESC, mlhd.id DESC";
    $rows = db_select_cached($sql, $params, 60, 'market.hub.local.latest-points');

    return array_map('db_market_hub_local_history_daily_normalize_result_row', $rows);
}

function db_market_hub_local_history_daily_window_by_type_ids(
    string $source,
    int $sourceId,
    array $typeIds,
    int $days = 14
): array {
    $safeSource = trim($source);
    $safeSourceId = max(0, $sourceId);
    $safeDays = max(1, min($days, 90));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));

    if ($safeSource === '' || $safeSourceId <= 0 || $normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $params = array_merge([$safeSource, $safeSourceId], $normalizedTypeIds);
    $rows = db_select_cached(
        "SELECT
            mlhd.type_id,
            rit.type_name,
            mlhd.trade_date,
            mlhd.open_price,
            mlhd.high_price,
            mlhd.low_price,
            mlhd.close_price,
            mlhd.buy_price,
            mlhd.sell_price,
            mlhd.spread_value,
            mlhd.spread_percent,
            mlhd.volume,
            mlhd.buy_order_count,
            mlhd.sell_order_count,
            mlhd.captured_at AS observed_at
         FROM market_hub_local_history_daily mlhd
         LEFT JOIN ref_item_types rit ON rit.type_id = mlhd.type_id
         WHERE mlhd.source = ?
           AND mlhd.source_id = ?
           AND mlhd.type_id IN ({$placeholders})
           AND mlhd.trade_date >= DATE_SUB(UTC_DATE(), INTERVAL {$safeDays} DAY)
         ORDER BY mlhd.trade_date ASC, mlhd.type_id ASC, mlhd.id ASC",
        $params,
        60,
        'market.hub.local.window'
    );

    return array_map('db_market_hub_local_history_daily_normalize_result_row', $rows);
}

function db_market_orders_current_distinct_type_ids(string $sourceType, int $sourceId, int $limit = 500): array
{
    $safeLimit = max(1, min($limit, 5000));
    $rows = db_select_cached(
        "SELECT DISTINCT type_id
         FROM market_orders_current
         WHERE source_type = ? AND source_id = ?
         ORDER BY type_id ASC
         LIMIT {$safeLimit}",
        [$sourceType, $sourceId],
        60,
        'market.current.distinct-types'
    );

    return array_values(array_map(static fn (array $row): int => (int) ($row['type_id'] ?? 0), $rows));
}

function db_market_history_daily_distinct_type_ids(string $sourceType, int $sourceId, int $limit = 500): array
{
    $safeLimit = max(1, min($limit, 5000));
    $rows = db_select_cached(
        "SELECT DISTINCT type_id
         FROM market_history_daily
         WHERE source_type = ? AND source_id = ?
         ORDER BY type_id ASC
         LIMIT {$safeLimit}",
        [$sourceType, $sourceId],
        60,
        'market.history.distinct-types'
    );

    return array_values(array_map(static fn (array $row): int => (int) ($row['type_id'] ?? 0), $rows));
}

function db_market_history_daily_recent_window(string $sourceType, int $sourceId, int $days = 8, int $typeLimit = 120): array
{
    $safeDays = max(1, min($days, 60));
    $safeTypeLimit = max(1, min($typeLimit, 500));

    $typeRows = db_select_cached(
        "SELECT ranked.type_id
         FROM (
            SELECT
                type_id,
                MAX(trade_date) AS latest_trade_date,
                SUM(volume) AS total_volume
            FROM market_history_daily
            WHERE source_type = ?
              AND source_id = ?
            GROUP BY type_id
         ) ranked
         ORDER BY ranked.latest_trade_date DESC, ranked.total_volume DESC
         LIMIT {$safeTypeLimit}",
        [$sourceType, $sourceId]
        ,
        60,
        'market.history.recent-types'
    );

    $typeIds = array_values(array_filter(array_map(static fn (array $row): int => (int) ($row['type_id'] ?? 0), $typeRows), static fn (int $typeId): bool => $typeId > 0));
    if ($typeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($typeIds), '?'));
    $params = array_merge([$sourceType, $sourceId], $typeIds);

    return db_select_cached(
        "SELECT
            mhd.type_id,
            rit.type_name,
            mhd.trade_date,
            mhd.close_price,
            mhd.volume,
            mhd.observed_at
         FROM market_history_daily mhd
         LEFT JOIN ref_item_types rit ON rit.type_id = mhd.type_id
         WHERE mhd.source_type = ?
           AND mhd.source_id = ?
           AND mhd.type_id IN ({$placeholders})
           AND mhd.trade_date >= DATE_SUB(UTC_DATE(), INTERVAL {$safeDays} DAY)
         ORDER BY mhd.type_id ASC, mhd.trade_date DESC",
        $params,
        60,
        'market.history.recent-window'
    );
}

function db_market_history_daily_aggregate_by_date_type_source(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    $params = [$sourceType, $sourceId, $startDate, $endDate];
    $typeFilterSql = '';
    $rawTypeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
        $typeFilterSql = " AND mhd.type_id IN ({$placeholders})";
        $params = array_merge($params, $normalizedTypeIds);
    }

    return db_select_cached(
        "SELECT
            mhd.trade_date,
            mhd.type_id,
            rit.type_name,
            mhd.source_type,
            mhd.source_id,
            AVG(mhd.close_price) AS avg_close_price,
            SUM(mhd.volume) AS total_volume,
            SUM(COALESCE(mhd.order_count, 0)) AS total_order_count,
            MAX(mhd.observed_at) AS last_observed_at
         FROM market_history_daily mhd
         LEFT JOIN ref_item_types rit ON rit.type_id = mhd.type_id
         WHERE mhd.source_type = ?
           AND mhd.source_id = ?
           AND mhd.trade_date BETWEEN ? AND ?{$typeFilterSql}
         GROUP BY mhd.trade_date, mhd.type_id, rit.type_name, mhd.source_type, mhd.source_id
         ORDER BY mhd.trade_date ASC, mhd.type_id ASC",
        $params,
        60,
        'market.history.aggregate'
    );
}

function db_market_history_daily_deviation_series(
    int $allianceStructureId,
    int $hubSourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    $params = [$allianceStructureId, $hubSourceId, $startDate, $endDate];
    $typeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
        $typeFilterSql = " AND a.type_id IN ({$placeholders})";
        $params = array_merge($params, $normalizedTypeIds);
    }

    return db_select_cached(
        "SELECT
            a.trade_date,
            a.type_id,
            rit.type_name,
            a.close_price AS alliance_close_price,
            h.close_price AS hub_close_price,
            CASE
                WHEN h.close_price > 0 THEN ((a.close_price - h.close_price) / h.close_price) * 100
                ELSE NULL
            END AS deviation_percent,
            a.volume AS alliance_volume,
            h.volume AS hub_volume,
            a.order_count AS alliance_order_count,
            h.order_count AS hub_order_count
         FROM market_history_daily a
         INNER JOIN market_history_daily h
             ON h.source_type = 'market_hub'
            AND h.source_id = ?
            AND h.type_id = a.type_id
            AND h.trade_date = a.trade_date
         LEFT JOIN ref_item_types rit ON rit.type_id = a.type_id
         WHERE a.source_type = 'alliance_structure'
           AND a.source_id = ?
           AND a.trade_date BETWEEN ? AND ?{$typeFilterSql}
         ORDER BY a.trade_date ASC, a.type_id ASC",
        [$hubSourceId, $allianceStructureId, $startDate, $endDate, ...array_slice($params, 4)],
        60,
        'market.history.deviation'
    );
}

// UI paths that depend on long-lived market history must read from the
// daily rollups first. Raw fallback stays opt-in so tiered retention can expose
// missing projections instead of silently masking them with short-lived snapshots.
function db_market_orders_history_stock_health_series(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate,
    array $typeIds = [],
    bool $allowRawFallback = false
): array {
    $rows = db_market_item_stock_window_summaries($sourceType, $sourceId, $startDate, $endDate, $typeIds);
    if ($rows !== [] || $allowRawFallback === false) {
        return $rows;
    }

    $params = [$sourceType, $sourceId, $startDate, $endDate];
    $rawTypeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $rawTypeFilterSql = ' AND moh.type_id IN (' . implode(',', array_fill(0, count($normalizedTypeIds), '?')) . ')';
        $params = array_merge($params, $normalizedTypeIds);
    }

    $historyTable = db_validate_identifier(db_market_orders_history_read_table());

    return db_select(
        sprintf(
            "SELECT
                moh.observed_date,
                moh.type_id,
                rit.type_name,
                SUM(CASE WHEN moh.is_buy_order = 0 THEN moh.volume_remain ELSE 0 END) AS sell_volume,
                SUM(CASE WHEN moh.is_buy_order = 1 THEN moh.volume_remain ELSE 0 END) AS buy_volume,
                SUM(CASE WHEN moh.is_buy_order = 0 THEN 1 ELSE 0 END) AS sell_order_count,
                SUM(CASE WHEN moh.is_buy_order = 1 THEN 1 ELSE 0 END) AS buy_order_count,
                AVG(CASE WHEN moh.is_buy_order = 0 THEN moh.price ELSE NULL END) AS avg_sell_price,
                AVG(CASE WHEN moh.is_buy_order = 1 THEN moh.price ELSE NULL END) AS avg_buy_price,
                MAX(moh.observed_at) AS last_observed_at
             FROM %s moh
             LEFT JOIN ref_item_types rit ON rit.type_id = moh.type_id
             WHERE moh.source_type = ?
               AND moh.source_id = ?
               AND moh.observed_date BETWEEN ? AND ?{$rawTypeFilterSql}
             GROUP BY moh.observed_date, moh.type_id, rit.type_name
             ORDER BY moh.observed_date ASC, moh.type_id ASC",
            $historyTable
        ),
        $params
    );
}

function db_market_orders_current_source_aggregates(string $sourceType, int $sourceId, array $typeIds = []): array
{
    $projectionRows = db_market_orders_current_compact_projection_rows($sourceType, $sourceId, $typeIds);
    if ($projectionRows !== []) {
        return $projectionRows;
    }

    $params = [$sourceType, $sourceId];
    $summaryParams = [$sourceType, $sourceId];
    $typeFilterSql = '';
    $rawTypeFilterSql = '';
    $state = db_market_source_snapshot_state_get($sourceType, $sourceId);
    $latestCurrentObservedAt = trim((string) ($state['latest_current_observed_at'] ?? ''));
    $latestSummaryObservedAt = trim((string) ($state['latest_summary_observed_at'] ?? ''));

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $typePlaceholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
        $typeFilterSql = " AND moss.type_id IN ({$typePlaceholders})";
        $rawTypeFilterSql = " AND moc.type_id IN ({$typePlaceholders})";
        $params = array_merge($params, $normalizedTypeIds);
        $summaryParams = array_merge($summaryParams, $normalizedTypeIds);
    }

    // Operational pages should prefer current-state projections first, summary
    // rows second, and raw current snapshots only as a backfill/fallback path
    // while the compact projections are still materializing.
    $summaryTable = db_validate_identifier(db_market_order_snapshots_summary_read_table());
    $rows = [];
    if ($latestSummaryObservedAt !== '') {
        $rows = db_select_cached(
            "SELECT
                moss.type_id,
                rit.type_name,
                moss.best_sell_price,
                moss.best_buy_price,
                moss.total_sell_volume,
                moss.total_buy_volume,
                moss.sell_order_count,
                moss.buy_order_count,
                moss.observed_at AS last_observed_at
             FROM {$summaryTable} moss
             LEFT JOIN ref_item_types rit ON rit.type_id = moss.type_id
             WHERE moss.source_type = ?
               AND moss.source_id = ?
               AND moss.observed_at = ?{$typeFilterSql}
             ORDER BY moss.type_id ASC",
            [$sourceType, $sourceId, $latestSummaryObservedAt, ...array_slice($summaryParams, 2)],
            60,
            'market.snapshot.current-aggregates'
        );
    }

    if ($rows !== []) {
        return $rows;
    }

    $resolvedLatestCurrentObservedAt = $latestCurrentObservedAt !== ''
        ? $latestCurrentObservedAt
        : db_market_orders_current_latest_observed_at($sourceType, $sourceId);

    if ($resolvedLatestCurrentObservedAt !== '') {
        $rows = db_select(
            "SELECT
                moc.type_id,
                rit.type_name,
                MIN(CASE WHEN moc.is_buy_order = 0 AND moc.volume_remain > 0 THEN moc.price END) AS best_sell_price,
                MAX(CASE WHEN moc.is_buy_order = 1 AND moc.volume_remain > 0 THEN moc.price END) AS best_buy_price,
                COALESCE(SUM(CASE WHEN moc.is_buy_order = 0 THEN moc.volume_remain ELSE 0 END), 0) AS total_sell_volume,
                COALESCE(SUM(CASE WHEN moc.is_buy_order = 1 THEN moc.volume_remain ELSE 0 END), 0) AS total_buy_volume,
                COALESCE(SUM(CASE WHEN moc.is_buy_order = 0 THEN 1 ELSE 0 END), 0) AS sell_order_count,
                COALESCE(SUM(CASE WHEN moc.is_buy_order = 1 THEN 1 ELSE 0 END), 0) AS buy_order_count,
                MAX(moc.observed_at) AS last_observed_at
             FROM market_orders_current moc
             LEFT JOIN ref_item_types rit ON rit.type_id = moc.type_id
             WHERE moc.source_type = ?
               AND moc.source_id = ?
               AND moc.observed_at = ?{$rawTypeFilterSql}
             GROUP BY moc.type_id, rit.type_name
             ORDER BY moc.type_id ASC",
            [$sourceType, $sourceId, $resolvedLatestCurrentObservedAt, ...array_slice($params, 2)]
        );
    }

    if ($rows !== []) {
        $observedAt = trim((string) ($rows[0]['last_observed_at'] ?? ''));
        if ($observedAt !== '') {
            db_market_source_snapshot_state_upsert([
                'source_type' => $sourceType,
                'source_id' => $sourceId,
                'latest_current_observed_at' => $observedAt,
                'current_distinct_type_count' => count($rows),
                'last_synced_at' => $observedAt,
            ]);
        }

        return $rows;
    }

    $rows = db_select_cached(
        "SELECT
            moss.type_id,
            rit.type_name,
            moss.best_sell_price,
            moss.best_buy_price,
            moss.total_sell_volume,
            moss.total_buy_volume,
            moss.sell_order_count,
            moss.buy_order_count,
            moss.observed_at AS last_observed_at
         FROM {$summaryTable} moss
         LEFT JOIN ref_item_types rit ON rit.type_id = moss.type_id
         WHERE moss.source_type = ?
           AND moss.source_id = ?
           AND moss.observed_at = (
                SELECT MAX(summary_latest.observed_at)
                FROM {$summaryTable} summary_latest
                WHERE summary_latest.source_type = ?
                  AND summary_latest.source_id = ?
           ){$typeFilterSql}
         ORDER BY moss.type_id ASC",
        [$sourceType, $sourceId, $sourceType, $sourceId, ...array_slice($summaryParams, 2)],
        60,
        'market.snapshot.current-aggregates'
    );

    if ($rows !== []) {
        $observedAt = trim((string) ($rows[0]['last_observed_at'] ?? ''));
        if ($observedAt !== '') {
            db_market_source_snapshot_state_upsert([
                'source_type' => $sourceType,
                'source_id' => $sourceId,
                'latest_summary_observed_at' => $observedAt,
                'summary_row_count' => count($rows),
                'last_synced_at' => $observedAt,
            ]);
        }

        return $rows;
    }

    return db_select(
        "SELECT
            moc.type_id,
            rit.type_name,
            MIN(CASE WHEN moc.is_buy_order = 0 AND moc.volume_remain > 0 THEN moc.price END) AS best_sell_price,
            MAX(CASE WHEN moc.is_buy_order = 1 AND moc.volume_remain > 0 THEN moc.price END) AS best_buy_price,
            COALESCE(SUM(CASE WHEN moc.is_buy_order = 0 THEN moc.volume_remain ELSE 0 END), 0) AS total_sell_volume,
            COALESCE(SUM(CASE WHEN moc.is_buy_order = 1 THEN moc.volume_remain ELSE 0 END), 0) AS total_buy_volume,
            COALESCE(SUM(CASE WHEN moc.is_buy_order = 0 THEN 1 ELSE 0 END), 0) AS sell_order_count,
            COALESCE(SUM(CASE WHEN moc.is_buy_order = 1 THEN 1 ELSE 0 END), 0) AS buy_order_count,
            MAX(moc.observed_at) AS last_observed_at
         FROM market_orders_current moc
         LEFT JOIN ref_item_types rit ON rit.type_id = moc.type_id
         WHERE moc.source_type = ?
           AND moc.source_id = ?
           AND moc.observed_at = (
                SELECT MAX(current_latest.observed_at)
                FROM market_orders_current current_latest
                WHERE current_latest.source_type = ?
                  AND current_latest.source_id = ?
           ){$rawTypeFilterSql}
         GROUP BY moc.type_id, rit.type_name
         ORDER BY moc.type_id ASC",
        [$sourceType, $sourceId, $sourceType, $sourceId, ...array_slice($params, 2)]
    );
}

function db_market_orders_current_alliance_vs_reference_aggregates(int $allianceStructureId, int $referenceSourceId, array $typeIds = []): array
{
    $allianceRows = db_market_orders_current_source_aggregates('alliance_structure', $allianceStructureId, $typeIds);
    $referenceRows = db_market_orders_current_source_aggregates('market_hub', $referenceSourceId, $typeIds);

    $normalized = [];

    foreach ($allianceRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        $normalized[$typeId] = [
            'type_id' => $typeId,
            'type_name' => (string) ($row['type_name'] ?? ''),
            'alliance_best_sell_price' => isset($row['best_sell_price']) ? (float) $row['best_sell_price'] : null,
            'alliance_best_buy_price' => isset($row['best_buy_price']) ? (float) $row['best_buy_price'] : null,
            'alliance_total_sell_volume' => (int) ($row['total_sell_volume'] ?? 0),
            'alliance_total_buy_volume' => (int) ($row['total_buy_volume'] ?? 0),
            'alliance_sell_order_count' => (int) ($row['sell_order_count'] ?? 0),
            'alliance_buy_order_count' => (int) ($row['buy_order_count'] ?? 0),
            'alliance_last_observed_at' => $row['last_observed_at'] ?? null,
            'reference_best_sell_price' => null,
            'reference_best_buy_price' => null,
            'reference_total_sell_volume' => 0,
            'reference_total_buy_volume' => 0,
            'reference_sell_order_count' => 0,
            'reference_buy_order_count' => 0,
            'reference_last_observed_at' => null,
        ];
    }

    foreach ($referenceRows as $row) {
        $typeId = (int) ($row['type_id'] ?? 0);
        if ($typeId <= 0) {
            continue;
        }

        if (!isset($normalized[$typeId])) {
            $normalized[$typeId] = [
                'type_id' => $typeId,
                'type_name' => (string) ($row['type_name'] ?? ''),
                'alliance_best_sell_price' => null,
                'alliance_best_buy_price' => null,
                'alliance_total_sell_volume' => 0,
                'alliance_total_buy_volume' => 0,
                'alliance_sell_order_count' => 0,
                'alliance_buy_order_count' => 0,
                'alliance_last_observed_at' => null,
                'reference_best_sell_price' => null,
                'reference_best_buy_price' => null,
                'reference_total_sell_volume' => 0,
                'reference_total_buy_volume' => 0,
                'reference_sell_order_count' => 0,
                'reference_buy_order_count' => 0,
                'reference_last_observed_at' => null,
            ];
        }

        if (($normalized[$typeId]['type_name'] ?? '') === '' && isset($row['type_name'])) {
            $normalized[$typeId]['type_name'] = (string) $row['type_name'];
        }

        $normalized[$typeId]['reference_best_sell_price'] = isset($row['best_sell_price']) ? (float) $row['best_sell_price'] : null;
        $normalized[$typeId]['reference_best_buy_price'] = isset($row['best_buy_price']) ? (float) $row['best_buy_price'] : null;
        $normalized[$typeId]['reference_total_sell_volume'] = (int) ($row['total_sell_volume'] ?? 0);
        $normalized[$typeId]['reference_total_buy_volume'] = (int) ($row['total_buy_volume'] ?? 0);
        $normalized[$typeId]['reference_sell_order_count'] = (int) ($row['sell_order_count'] ?? 0);
        $normalized[$typeId]['reference_buy_order_count'] = (int) ($row['buy_order_count'] ?? 0);
        $normalized[$typeId]['reference_last_observed_at'] = $row['last_observed_at'] ?? null;
    }

    ksort($normalized);

    return array_values($normalized);
}

function db_sync_run_start(string $datasetKey, string $runMode, ?string $cursorStart): int
{
    db_execute(
        'INSERT INTO sync_runs (dataset_key, run_mode, run_status, started_at, cursor_start)
         VALUES (?, ?, ?, UTC_TIMESTAMP(), ?)',
        [$datasetKey, $runMode, 'running', $cursorStart]
    );

    return (int) db()->lastInsertId();
}

function db_sync_run_finish(
    int $runId,
    string $runStatus,
    int $sourceRows,
    int $writtenRows,
    ?string $cursorEnd,
    ?string $errorMessage
): bool {
    return db_execute(
        'UPDATE sync_runs
         SET run_status = ?,
             finished_at = UTC_TIMESTAMP(),
             source_rows = ?,
             written_rows = ?,
             cursor_end = ?,
             error_message = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?',
        [$runStatus, $sourceRows, $writtenRows, $cursorEnd, $errorMessage, $runId]
    );
}

function db_sync_run_latest_by_dataset(string $datasetKey): ?array
{
    return db_select_one(
        'SELECT id, dataset_key, run_mode, run_status, source_rows, written_rows, error_message, started_at, finished_at
         FROM sync_runs
         WHERE dataset_key = ?
         ORDER BY id DESC
         LIMIT 1',
        [$datasetKey]
    );
}

function db_sync_runs_recent_by_dataset_prefix(string $datasetPrefix, int $limit = 5): array
{
    $safeLimit = max(1, min(50, $limit));

    return db_select(
        'SELECT id, dataset_key, run_mode, run_status, source_rows, written_rows, error_message, started_at, finished_at
         FROM sync_runs
         WHERE dataset_key LIKE ?
         ORDER BY id DESC
         LIMIT ' . $safeLimit,
        [$datasetPrefix . '%']
    );
}

function db_sync_state_by_dataset_prefix(string $datasetPrefix): array
{
    return db_select(
        'SELECT dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message, updated_at
         FROM sync_state
         WHERE dataset_key LIKE ?
         ORDER BY updated_at DESC, dataset_key ASC',
        [$datasetPrefix . '%']
    );
}

function db_trading_station_options(): array
{
    return db_select('SELECT id, station_name, station_type FROM trading_stations ORDER BY station_name ASC');
}

function db_trading_station_by_id(int $stationId, string $stationType): ?array
{
    return db_select_one(
        'SELECT id, station_name, station_type FROM trading_stations WHERE id = ? AND station_type = ? LIMIT 1',
        [$stationId, $stationType]
    );
}

function db_ref_npc_station_id_by_name(string $stationName): ?int
{
    $normalized = trim($stationName);
    if ($normalized === '') {
        return null;
    }

    $row = db_select_one(
        'SELECT station_id
         FROM ref_npc_stations
         WHERE station_name = ?
         LIMIT 1',
        [$normalized]
    );

    $stationId = (int) ($row['station_id'] ?? 0);

    return $stationId > 0 ? $stationId : null;
}

function db_ref_npc_station_by_id(int $stationId): ?array
{
    if ($stationId <= 0) {
        return null;
    }

    return db_select_one(
        'SELECT s.station_id, s.station_name, s.system_id, s.region_id, sys.system_name, s.station_type_id, t.type_name AS station_type_name
         FROM ref_npc_stations s
         LEFT JOIN ref_systems sys ON sys.system_id = s.system_id
         LEFT JOIN ref_item_types t ON t.type_id = s.station_type_id
         WHERE s.station_id = ?
         LIMIT 1',
        [$stationId]
    );
}

function db_ref_npc_station_region_id(int $stationId): ?int
{
    if ($stationId <= 0) {
        return null;
    }

    $row = db_select_one(
        'SELECT region_id
         FROM ref_npc_stations
         WHERE station_id = ?
         LIMIT 1',
        [$stationId]
    );

    $regionId = (int) ($row['region_id'] ?? 0);

    return $regionId > 0 ? $regionId : null;
}

function db_ref_system_region_id(int $systemId): ?int
{
    if ($systemId <= 0) {
        return null;
    }

    $row = db_select_one(
        'SELECT region_id
         FROM ref_systems
         WHERE system_id = ?
         LIMIT 1',
        [$systemId]
    );

    $regionId = (int) ($row['region_id'] ?? 0);

    return $regionId > 0 ? $regionId : null;
}

function db_ref_npc_station_search(string $query, int $limit = 20): array
{
    $normalized = trim($query);
    if ($normalized === '') {
        return [];
    }

    $safeLimit = max(1, min(50, $limit));

    return db_select(
        'SELECT s.station_id, s.station_name, s.system_id, sys.system_name, s.station_type_id, t.type_name AS station_type_name
         FROM ref_npc_stations s
         LEFT JOIN ref_systems sys ON sys.system_id = s.system_id
         LEFT JOIN ref_item_types t ON t.type_id = s.station_type_id
         WHERE s.station_name LIKE ?
         ORDER BY
            CASE WHEN s.station_name LIKE ? THEN 0 ELSE 1 END,
            s.station_name ASC
         LIMIT ' . $safeLimit,
        ['%' . $normalized . '%', $normalized . '%']
    );
}

function db_alliance_structure_metadata_get(int $structureId): ?array
{
    return db_select_one(
        'SELECT structure_id, structure_name, last_verified_at
         FROM alliance_structure_metadata
         WHERE structure_id = ?
         LIMIT 1',
        [$structureId]
    );
}

function db_alliance_structure_metadata_upsert(int $structureId, ?string $structureName, ?string $lastVerifiedAt = null): bool
{
    return db_execute(
        'INSERT INTO alliance_structure_metadata (structure_id, structure_name, last_verified_at)
         VALUES (?, ?, COALESCE(?, UTC_TIMESTAMP()))
         ON DUPLICATE KEY UPDATE
            structure_name = VALUES(structure_name),
            last_verified_at = VALUES(last_verified_at),
            updated_at = CURRENT_TIMESTAMP',
        [$structureId, $structureName, $lastVerifiedAt]
    );
}

function db_esi_structure_search_cache_get(int $characterId, string $query): ?array
{
    $cacheKey = $characterId . ':' . mb_strtolower(trim($query));

    return db_esi_cache_get('cache.esi.structures.search', $cacheKey);
}

function db_esi_structure_search_cache_put(int $characterId, string $query, string $payloadJson, ?string $expiresAt = null): bool
{
    $cacheKey = $characterId . ':' . mb_strtolower(trim($query));

    return db_esi_cache_put('cache.esi.structures.search', $cacheKey, $payloadJson, null, $expiresAt);
}

function db_intelligence_snapshot_get(string $snapshotKey): ?array
{
    return db_select_one(
        'SELECT snapshot_key, snapshot_status, payload_json, metadata_json, computed_at, refresh_started_at, expires_at, created_at, updated_at
         FROM intelligence_snapshots
         WHERE snapshot_key = ?
         LIMIT 1',
        [$snapshotKey]
    );
}

function db_intelligence_snapshots_get_many(array $snapshotKeys): array
{
    $normalized = array_values(array_unique(array_filter(array_map(
        static fn (mixed $snapshotKey): string => trim((string) $snapshotKey),
        $snapshotKeys
    ), static fn (string $snapshotKey): bool => $snapshotKey !== '')));

    if ($normalized === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalized), '?'));

    return db_select(
        "SELECT snapshot_key, snapshot_status, payload_json, metadata_json, computed_at, refresh_started_at, expires_at, created_at, updated_at
         FROM intelligence_snapshots
         WHERE snapshot_key IN ({$placeholders})",
        $normalized
    );
}

function db_market_deal_alerts_ensure_schema(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db()->exec(
        "CREATE TABLE IF NOT EXISTS market_deal_alerts_current (
            alert_key VARCHAR(190) PRIMARY KEY,
            item_type_id INT UNSIGNED NOT NULL,
            source_type ENUM('market_hub', 'alliance_structure') NOT NULL,
            source_id BIGINT UNSIGNED NOT NULL,
            source_name VARCHAR(190) NOT NULL,
            percent_band DECIMAL(6,2) NOT NULL,
            current_price DECIMAL(20,2) NOT NULL,
            normal_price DECIMAL(20,2) NOT NULL,
            percent_of_normal DECIMAL(8,4) NOT NULL,
            anomaly_score DECIMAL(10,2) NOT NULL DEFAULT 0.00,
            severity ENUM('critical', 'very_strong', 'strong', 'watch') NOT NULL,
            severity_rank TINYINT UNSIGNED NOT NULL DEFAULT 1,
            quantity_available BIGINT UNSIGNED NOT NULL DEFAULT 0,
            listing_count INT UNSIGNED NOT NULL DEFAULT 0,
            best_order_id BIGINT UNSIGNED DEFAULT NULL,
            baseline_model VARCHAR(80) NOT NULL DEFAULT 'median_weighted_blend',
            baseline_points INT UNSIGNED NOT NULL DEFAULT 0,
            baseline_median_price DECIMAL(20,2) DEFAULT NULL,
            baseline_weighted_price DECIMAL(20,2) DEFAULT NULL,
            observed_at DATETIME NOT NULL,
            detected_at DATETIME NOT NULL,
            last_seen_at DATETIME NOT NULL,
            inactive_at DATETIME DEFAULT NULL,
            freshness_seconds INT UNSIGNED NOT NULL DEFAULT 0,
            status ENUM('active', 'inactive') NOT NULL DEFAULT 'active',
            metadata_json JSON DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_market_deal_alerts_status_severity (status, severity_rank, anomaly_score),
            KEY idx_market_deal_alerts_item_source (item_type_id, source_type, source_id),
            KEY idx_market_deal_alerts_last_seen (last_seen_at),
            KEY idx_market_deal_alerts_observed (observed_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    db()->exec(
        "CREATE TABLE IF NOT EXISTS market_deal_alert_dismissals (
            alert_key VARCHAR(190) PRIMARY KEY,
            dismissed_severity_rank TINYINT UNSIGNED NOT NULL DEFAULT 1,
            dismissed_at DATETIME NOT NULL,
            dismissed_until DATETIME DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_market_deal_alert_dismissals_until (dismissed_until)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    db()->exec(
        "CREATE TABLE IF NOT EXISTS market_deal_alert_materialization_status (
            snapshot_key VARCHAR(64) PRIMARY KEY,
            last_job_key VARCHAR(190) DEFAULT NULL,
            last_run_started_at DATETIME DEFAULT NULL,
            last_run_finished_at DATETIME DEFAULT NULL,
            last_success_at DATETIME DEFAULT NULL,
            last_materialized_at DATETIME DEFAULT NULL,
            first_materialized_at DATETIME DEFAULT NULL,
            last_attempt_status VARCHAR(40) NOT NULL DEFAULT 'never_ran',
            last_success_status VARCHAR(40) DEFAULT NULL,
            last_reason_zero_output VARCHAR(500) DEFAULT NULL,
            last_failure_reason VARCHAR(500) DEFAULT NULL,
            last_deferred_at DATETIME DEFAULT NULL,
            last_deferred_reason VARCHAR(500) DEFAULT NULL,
            input_row_count INT UNSIGNED NOT NULL DEFAULT 0,
            history_row_count INT UNSIGNED NOT NULL DEFAULT 0,
            candidate_row_count INT UNSIGNED NOT NULL DEFAULT 0,
            output_row_count INT UNSIGNED NOT NULL DEFAULT 0,
            persisted_row_count INT UNSIGNED NOT NULL DEFAULT 0,
            inactive_row_count INT UNSIGNED NOT NULL DEFAULT 0,
            sources_scanned INT UNSIGNED NOT NULL DEFAULT 0,
            last_duration_ms INT UNSIGNED DEFAULT NULL,
            metadata_json JSON DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_market_deal_alert_materialization_attempt (last_attempt_status, last_run_finished_at),
            KEY idx_market_deal_alert_materialization_success (last_success_at),
            KEY idx_market_deal_alert_materialization_write (last_materialized_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    $ensured = true;
}

function db_market_deal_alert_materialization_status_get(string $snapshotKey = 'current'): ?array
{
    db_market_deal_alerts_ensure_schema();

    $safeSnapshotKey = trim($snapshotKey);
    if ($safeSnapshotKey === '') {
        $safeSnapshotKey = 'current';
    }

    $row = db_select_one(
        'SELECT *
         FROM market_deal_alert_materialization_status
         WHERE snapshot_key = ?
         LIMIT 1',
        [$safeSnapshotKey]
    );

    return is_array($row) ? $row : null;
}

function db_market_deal_alert_materialization_status_upsert(array $row): bool
{
    db_market_deal_alerts_ensure_schema();

    $snapshotKey = trim((string) ($row['snapshot_key'] ?? 'current'));
    if ($snapshotKey === '') {
        $snapshotKey = 'current';
    }

    return db_execute(
        'INSERT INTO market_deal_alert_materialization_status (
            snapshot_key,
            last_job_key,
            last_run_started_at,
            last_run_finished_at,
            last_success_at,
            last_materialized_at,
            first_materialized_at,
            last_attempt_status,
            last_success_status,
            last_reason_zero_output,
            last_failure_reason,
            last_deferred_at,
            last_deferred_reason,
            input_row_count,
            history_row_count,
            candidate_row_count,
            output_row_count,
            persisted_row_count,
            inactive_row_count,
            sources_scanned,
            last_duration_ms,
            metadata_json
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON DUPLICATE KEY UPDATE
            last_job_key = VALUES(last_job_key),
            last_run_started_at = COALESCE(VALUES(last_run_started_at), last_run_started_at),
            last_run_finished_at = COALESCE(VALUES(last_run_finished_at), last_run_finished_at),
            last_success_at = COALESCE(VALUES(last_success_at), last_success_at),
            last_materialized_at = COALESCE(VALUES(last_materialized_at), last_materialized_at),
            first_materialized_at = COALESCE(first_materialized_at, VALUES(first_materialized_at)),
            last_attempt_status = VALUES(last_attempt_status),
            last_success_status = COALESCE(VALUES(last_success_status), last_success_status),
            last_reason_zero_output = VALUES(last_reason_zero_output),
            last_failure_reason = VALUES(last_failure_reason),
            last_deferred_at = VALUES(last_deferred_at),
            last_deferred_reason = VALUES(last_deferred_reason),
            input_row_count = VALUES(input_row_count),
            history_row_count = VALUES(history_row_count),
            candidate_row_count = VALUES(candidate_row_count),
            output_row_count = VALUES(output_row_count),
            persisted_row_count = VALUES(persisted_row_count),
            inactive_row_count = VALUES(inactive_row_count),
            sources_scanned = VALUES(sources_scanned),
            last_duration_ms = VALUES(last_duration_ms),
            metadata_json = VALUES(metadata_json),
            updated_at = CURRENT_TIMESTAMP',
        [
            $snapshotKey,
            ($row['last_job_key'] ?? null) !== null ? mb_substr(trim((string) $row['last_job_key']), 0, 190) : null,
            $row['last_run_started_at'] ?? null,
            $row['last_run_finished_at'] ?? null,
            $row['last_success_at'] ?? null,
            $row['last_materialized_at'] ?? null,
            $row['first_materialized_at'] ?? null,
            mb_substr(trim((string) ($row['last_attempt_status'] ?? 'never_ran')), 0, 40),
            ($row['last_success_status'] ?? null) !== null ? mb_substr(trim((string) $row['last_success_status']), 0, 40) : null,
            ($row['last_reason_zero_output'] ?? null) !== null ? mb_substr(trim((string) $row['last_reason_zero_output']), 0, 500) : null,
            ($row['last_failure_reason'] ?? null) !== null ? mb_substr(trim((string) $row['last_failure_reason']), 0, 500) : null,
            $row['last_deferred_at'] ?? null,
            ($row['last_deferred_reason'] ?? null) !== null ? mb_substr(trim((string) $row['last_deferred_reason']), 0, 500) : null,
            max(0, (int) ($row['input_row_count'] ?? 0)),
            max(0, (int) ($row['history_row_count'] ?? 0)),
            max(0, (int) ($row['candidate_row_count'] ?? 0)),
            max(0, (int) ($row['output_row_count'] ?? 0)),
            max(0, (int) ($row['persisted_row_count'] ?? 0)),
            max(0, (int) ($row['inactive_row_count'] ?? 0)),
            max(0, (int) ($row['sources_scanned'] ?? 0)),
            isset($row['last_duration_ms']) ? max(0, (int) $row['last_duration_ms']) : null,
            $row['metadata_json'] ?? null,
        ]
    );
}

function db_market_lowest_sell_orders_by_source(string $sourceType, int $sourceId): array
{
    $safeSourceType = $sourceType === 'alliance_structure' ? 'alliance_structure' : 'market_hub';
    $safeSourceId = max(0, $sourceId);
    if ($safeSourceId <= 0) {
        return [];
    }

    return db_select(
        "SELECT
            picked.type_id,
            rit.type_name,
            picked.order_id,
            picked.price,
            picked.volume_remain,
            picked.observed_at,
            COALESCE(summary.sell_order_count, 0) AS sell_order_count,
            COALESCE(summary.total_sell_volume, 0) AS total_sell_volume
         FROM (
            SELECT
                base.type_id,
                MIN(base.order_id) AS order_id
            FROM market_orders_current base
            INNER JOIN (
                SELECT type_id, MIN(price) AS min_price
                FROM market_orders_current
                WHERE source_type = ?
                  AND source_id = ?
                  AND is_buy_order = 0
                  AND volume_remain > 0
                GROUP BY type_id
            ) best
                ON best.type_id = base.type_id
               AND best.min_price = base.price
            WHERE base.source_type = ?
              AND base.source_id = ?
              AND base.is_buy_order = 0
              AND base.volume_remain > 0
            GROUP BY base.type_id
         ) lowest
         INNER JOIN market_orders_current picked
            ON picked.source_type = ?
           AND picked.source_id = ?
           AND picked.type_id = lowest.type_id
           AND picked.order_id = lowest.order_id
         LEFT JOIN ref_item_types rit ON rit.type_id = picked.type_id
         LEFT JOIN (
            SELECT
                type_id,
                SUM(volume_remain) AS total_sell_volume,
                COUNT(*) AS sell_order_count
            FROM market_orders_current
            WHERE source_type = ?
              AND source_id = ?
              AND is_buy_order = 0
              AND volume_remain > 0
            GROUP BY type_id
         ) summary
            ON summary.type_id = picked.type_id
         ORDER BY picked.price ASC, picked.type_id ASC",
        [
            $safeSourceType,
            $safeSourceId,
            $safeSourceType,
            $safeSourceId,
            $safeSourceType,
            $safeSourceId,
            $safeSourceType,
            $safeSourceId,
        ]
    );
}

function db_market_history_daily_window_by_type_ids(string $sourceType, int $sourceId, array $typeIds, int $days = 14): array
{
    $safeSourceType = $sourceType === 'alliance_structure' ? 'alliance_structure' : 'market_hub';
    $safeSourceId = max(0, $sourceId);
    $safeDays = max(1, min($days, 90));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));

    if ($safeSourceId <= 0 || $normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $params = array_merge([$safeSourceType, $safeSourceId], $normalizedTypeIds);

    return db_select_cached(
        "SELECT
            mhd.type_id,
            mhd.trade_date,
            mhd.close_price,
            mhd.average_price,
            mhd.volume,
            mhd.order_count,
            mhd.observed_at
         FROM market_history_daily mhd
         WHERE mhd.source_type = ?
           AND mhd.source_id = ?
           AND mhd.type_id IN ({$placeholders})
           AND mhd.trade_date >= DATE_SUB(UTC_DATE(), INTERVAL {$safeDays} DAY)
         ORDER BY mhd.type_id ASC, mhd.trade_date DESC",
        $params,
        120,
        'market.deal-alerts.history'
    );
}

function db_market_deal_alerts_upsert_current(array $rows): int
{
    db_market_deal_alerts_ensure_schema();

    if ($rows === []) {
        return 0;
    }

    $written = 0;

    db_transaction(function () use ($rows, &$written): void {
        $sql = 'INSERT INTO market_deal_alerts_current (
                    alert_key,
                    item_type_id,
                    source_type,
                    source_id,
                    source_name,
                    percent_band,
                    current_price,
                    normal_price,
                    percent_of_normal,
                    anomaly_score,
                    severity,
                    severity_rank,
                    quantity_available,
                    listing_count,
                    best_order_id,
                    baseline_model,
                    baseline_points,
                    baseline_median_price,
                    baseline_weighted_price,
                    observed_at,
                    detected_at,
                    last_seen_at,
                    inactive_at,
                    freshness_seconds,
                    status,
                    metadata_json
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON DUPLICATE KEY UPDATE
                    item_type_id = VALUES(item_type_id),
                    source_type = VALUES(source_type),
                    source_id = VALUES(source_id),
                    source_name = VALUES(source_name),
                    percent_band = VALUES(percent_band),
                    current_price = VALUES(current_price),
                    normal_price = VALUES(normal_price),
                    percent_of_normal = VALUES(percent_of_normal),
                    anomaly_score = VALUES(anomaly_score),
                    severity = VALUES(severity),
                    severity_rank = VALUES(severity_rank),
                    quantity_available = VALUES(quantity_available),
                    listing_count = VALUES(listing_count),
                    best_order_id = VALUES(best_order_id),
                    baseline_model = VALUES(baseline_model),
                    baseline_points = VALUES(baseline_points),
                    baseline_median_price = VALUES(baseline_median_price),
                    baseline_weighted_price = VALUES(baseline_weighted_price),
                    observed_at = VALUES(observed_at),
                    detected_at = IF(status = \'inactive\', VALUES(detected_at), detected_at),
                    last_seen_at = VALUES(last_seen_at),
                    inactive_at = NULL,
                    freshness_seconds = VALUES(freshness_seconds),
                    status = VALUES(status),
                    metadata_json = VALUES(metadata_json)';

        foreach ($rows as $row) {
            db_execute($sql, [
                (string) ($row['alert_key'] ?? ''),
                max(0, (int) ($row['item_type_id'] ?? 0)),
                (string) ($row['source_type'] ?? 'market_hub'),
                max(0, (int) ($row['source_id'] ?? 0)),
                mb_substr(trim((string) ($row['source_name'] ?? '')), 0, 190),
                round((float) ($row['percent_band'] ?? 0.0), 2),
                round((float) ($row['current_price'] ?? 0.0), 2),
                round((float) ($row['normal_price'] ?? 0.0), 2),
                round((float) ($row['percent_of_normal'] ?? 0.0), 4),
                round((float) ($row['anomaly_score'] ?? 0.0), 2),
                (string) ($row['severity'] ?? 'watch'),
                max(1, (int) ($row['severity_rank'] ?? 1)),
                max(0, (int) ($row['quantity_available'] ?? 0)),
                max(0, (int) ($row['listing_count'] ?? 0)),
                isset($row['best_order_id']) ? max(0, (int) $row['best_order_id']) : null,
                mb_substr(trim((string) ($row['baseline_model'] ?? 'median_weighted_blend')), 0, 80),
                max(0, (int) ($row['baseline_points'] ?? 0)),
                isset($row['baseline_median_price']) ? round((float) $row['baseline_median_price'], 2) : null,
                isset($row['baseline_weighted_price']) ? round((float) $row['baseline_weighted_price'], 2) : null,
                (string) ($row['observed_at'] ?? gmdate('Y-m-d H:i:s')),
                (string) ($row['detected_at'] ?? gmdate('Y-m-d H:i:s')),
                (string) ($row['last_seen_at'] ?? gmdate('Y-m-d H:i:s')),
                $row['inactive_at'] ?? null,
                max(0, (int) ($row['freshness_seconds'] ?? 0)),
                (string) ($row['status'] ?? 'active'),
                $row['metadata_json'] ?? null,
            ]);
            $written++;
        }
    });

    return $written;
}

function db_market_deal_alerts_mark_missing_inactive(array $activeAlertKeys): int
{
    db_market_deal_alerts_ensure_schema();

    $normalizedKeys = array_values(array_unique(array_filter(array_map(static fn (mixed $value): string => trim((string) $value), $activeAlertKeys), static fn (string $value): bool => $value !== '')));

    if ($normalizedKeys === []) {
        db_execute(
            "UPDATE market_deal_alerts_current
             SET status = 'inactive',
                 inactive_at = UTC_TIMESTAMP()
             WHERE status = 'active'"
        );

        return (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
    }

    $placeholders = implode(',', array_fill(0, count($normalizedKeys), '?'));
    db_execute(
        "UPDATE market_deal_alerts_current
         SET status = 'inactive',
             inactive_at = UTC_TIMESTAMP()
         WHERE status = 'active'
           AND alert_key NOT IN ({$placeholders})",
        $normalizedKeys
    );

    return (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
}

function db_market_deal_alerts_dismiss(string $alertKey, int $severityRank, int $dismissMinutes): bool
{
    db_market_deal_alerts_ensure_schema();

    $safeAlertKey = trim($alertKey);
    if ($safeAlertKey === '') {
        return false;
    }

    return db_execute(
        'INSERT INTO market_deal_alert_dismissals (alert_key, dismissed_severity_rank, dismissed_at, dismissed_until)
         VALUES (?, ?, UTC_TIMESTAMP(), DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? MINUTE))
         ON DUPLICATE KEY UPDATE
            dismissed_severity_rank = GREATEST(dismissed_severity_rank, VALUES(dismissed_severity_rank)),
            dismissed_at = VALUES(dismissed_at),
            dismissed_until = VALUES(dismissed_until)',
        [$safeAlertKey, max(1, min(4, $severityRank)), max(5, min(1440, $dismissMinutes))]
    );
}

function db_market_deal_alerts_active_summary(): array
{
    db_market_deal_alerts_ensure_schema();

    $row = db_select_one(
        "SELECT
            COUNT(*) AS active_count,
            SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_count,
            MAX(last_seen_at) AS last_seen_at
         FROM market_deal_alerts_current
         WHERE status = 'active'"
    );

    return is_array($row) ? $row : ['active_count' => 0, 'critical_count' => 0, 'last_seen_at' => null];
}

function db_market_deal_alerts_list(array $filters = [], int $limit = 25, int $offset = 0): array
{
    db_market_deal_alerts_ensure_schema();

    $safeLimit = max(1, min(100, $limit));
    $safeOffset = max(0, $offset);
    $params = [];
    $where = ["a.status = 'active'"];

    $market = trim((string) ($filters['market'] ?? ''));
    if (in_array($market, ['market_hub', 'alliance_structure'], true)) {
        $where[] = 'a.source_type = ?';
        $params[] = $market;
    }

    $minimumSeverityRank = (int) ($filters['minimum_severity_rank'] ?? 0);
    if ($minimumSeverityRank > 0) {
        $where[] = 'a.severity_rank >= ?';
        $params[] = $minimumSeverityRank;
    }

    $search = trim((string) ($filters['search'] ?? ''));
    if ($search !== '') {
        $numericSearch = preg_replace('/[^0-9]/', '', $search) ?? '';
        if ($numericSearch !== '') {
            $where[] = '(rit.type_name LIKE ? OR CAST(a.item_type_id AS CHAR) LIKE ?)';
            $params[] = '%' . $search . '%';
            $params[] = '%' . $numericSearch . '%';
        } else {
            $where[] = 'rit.type_name LIKE ?';
            $params[] = '%' . $search . '%';
        }
    }

    $whereSql = implode(' AND ', $where);
    $sort = trim((string) ($filters['sort'] ?? 'severity'));
    $orderBy = match ($sort) {
        'price' => 'a.current_price ASC, a.severity_rank DESC, a.anomaly_score DESC, a.item_type_id ASC',
        'freshness' => 'a.last_seen_at DESC, a.severity_rank DESC, a.anomaly_score DESC, a.item_type_id ASC',
        'percent' => 'a.percent_of_normal ASC, a.severity_rank DESC, a.anomaly_score DESC, a.item_type_id ASC',
        default => 'a.severity_rank DESC, a.anomaly_score DESC, a.percent_of_normal ASC, a.last_seen_at DESC, a.item_type_id ASC',
    };

    return db_select(
        "SELECT
            a.*,
            rit.type_name,
            d.dismissed_at,
            d.dismissed_until,
            d.dismissed_severity_rank
         FROM market_deal_alerts_current a
         LEFT JOIN ref_item_types rit ON rit.type_id = a.item_type_id
         LEFT JOIN market_deal_alert_dismissals d ON d.alert_key = a.alert_key
         WHERE {$whereSql}
         ORDER BY {$orderBy}
         LIMIT {$safeLimit} OFFSET {$safeOffset}",
        $params
    );
}

function db_market_deal_alerts_count(array $filters = []): int
{
    db_market_deal_alerts_ensure_schema();

    $params = [];
    $where = ["a.status = 'active'"];

    $market = trim((string) ($filters['market'] ?? ''));
    if (in_array($market, ['market_hub', 'alliance_structure'], true)) {
        $where[] = 'a.source_type = ?';
        $params[] = $market;
    }

    $minimumSeverityRank = (int) ($filters['minimum_severity_rank'] ?? 0);
    if ($minimumSeverityRank > 0) {
        $where[] = 'a.severity_rank >= ?';
        $params[] = $minimumSeverityRank;
    }

    $search = trim((string) ($filters['search'] ?? ''));
    if ($search !== '') {
        $numericSearch = preg_replace('/[^0-9]/', '', $search) ?? '';
        if ($numericSearch !== '') {
            $where[] = '(rit.type_name LIKE ? OR CAST(a.item_type_id AS CHAR) LIKE ?)';
            $params[] = '%' . $search . '%';
            $params[] = '%' . $numericSearch . '%';
        } else {
            $where[] = 'rit.type_name LIKE ?';
            $params[] = '%' . $search . '%';
        }
    }

    $row = db_select_one(
        'SELECT COUNT(*) AS total
         FROM market_deal_alerts_current a
         LEFT JOIN ref_item_types rit ON rit.type_id = a.item_type_id
         WHERE ' . implode(' AND ', $where),
        $params
    );

    return max(0, (int) ($row['total'] ?? 0));
}

function db_market_deal_alerts_popup_rows(int $minimumSeverityRank = 3, int $limit = 3): array
{
    db_market_deal_alerts_ensure_schema();

    $safeMinimumSeverityRank = max(1, min(4, $minimumSeverityRank));
    $safeLimit = max(1, min(5, $limit));

    return db_select(
        "SELECT
            a.*,
            rit.type_name,
            d.dismissed_at,
            d.dismissed_until,
            d.dismissed_severity_rank
         FROM market_deal_alerts_current a
         LEFT JOIN ref_item_types rit ON rit.type_id = a.item_type_id
         LEFT JOIN market_deal_alert_dismissals d ON d.alert_key = a.alert_key
         WHERE a.status = 'active'
           AND a.severity_rank >= ?
           AND (
                d.alert_key IS NULL
                OR d.dismissed_until IS NULL
                OR d.dismissed_until < UTC_TIMESTAMP()
                OR d.dismissed_severity_rank < a.severity_rank
           )
         ORDER BY a.severity_rank DESC, a.anomaly_score DESC, a.percent_of_normal ASC, a.last_seen_at DESC
         LIMIT {$safeLimit}",
        [$safeMinimumSeverityRank]
    );
}

function db_intelligence_snapshot_upsert(
    string $snapshotKey,
    string $status,
    ?string $payloadJson,
    ?string $metadataJson,
    ?string $computedAt,
    ?string $refreshStartedAt,
    ?string $expiresAt
): bool {
    return db_execute(
        'INSERT INTO intelligence_snapshots (snapshot_key, snapshot_status, payload_json, metadata_json, computed_at, refresh_started_at, expires_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            snapshot_status = VALUES(snapshot_status),
            payload_json = VALUES(payload_json),
            metadata_json = VALUES(metadata_json),
            computed_at = VALUES(computed_at),
            refresh_started_at = VALUES(refresh_started_at),
            expires_at = VALUES(expires_at),
            updated_at = CURRENT_TIMESTAMP',
        [$snapshotKey, $status, $payloadJson, $metadataJson, $computedAt, $refreshStartedAt, $expiresAt]
    );
}

function db_intelligence_snapshot_mark_updating(string $snapshotKey, ?string $metadataJson = null, ?string $expiresAt = null): bool
{
    return db_execute(
        'INSERT INTO intelligence_snapshots (snapshot_key, snapshot_status, metadata_json, refresh_started_at, expires_at)
         VALUES (?, ?, ?, UTC_TIMESTAMP(), ?)
         ON DUPLICATE KEY UPDATE
            snapshot_status = VALUES(snapshot_status),
            metadata_json = COALESCE(VALUES(metadata_json), metadata_json),
            refresh_started_at = UTC_TIMESTAMP(),
            expires_at = COALESCE(VALUES(expires_at), expires_at),
            updated_at = CURRENT_TIMESTAMP',
        [$snapshotKey, 'updating', $metadataJson, $expiresAt]
    );
}

function db_ui_refresh_schema_ensure(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db()->exec(
        "CREATE TABLE IF NOT EXISTS ui_refresh_section_versions (
            section_key VARCHAR(190) PRIMARY KEY,
            version_counter BIGINT UNSIGNED NOT NULL DEFAULT 0,
            fingerprint CHAR(64) DEFAULT NULL,
            snapshot_key VARCHAR(190) DEFAULT NULL,
            domains_json JSON DEFAULT NULL,
            ui_sections_json JSON DEFAULT NULL,
            metadata_json JSON DEFAULT NULL,
            last_job_key VARCHAR(190) DEFAULT NULL,
            last_status VARCHAR(40) DEFAULT NULL,
            last_event_id BIGINT UNSIGNED DEFAULT NULL,
            last_finished_at DATETIME DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_ui_refresh_section_versions_event (last_event_id),
            KEY idx_ui_refresh_section_versions_updated (updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    db()->exec(
        "CREATE TABLE IF NOT EXISTS ui_refresh_events (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            event_type VARCHAR(80) NOT NULL DEFAULT 'job_completed',
            event_key VARCHAR(190) DEFAULT NULL,
            job_key VARCHAR(190) NOT NULL,
            job_status VARCHAR(40) NOT NULL,
            finished_at DATETIME NOT NULL,
            domains_json JSON DEFAULT NULL,
            ui_sections_json JSON DEFAULT NULL,
            section_versions_json JSON DEFAULT NULL,
            payload_json JSON DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_ui_refresh_event_key (event_key),
            KEY idx_ui_refresh_events_job (job_key, finished_at),
            KEY idx_ui_refresh_events_finished (finished_at),
            KEY idx_ui_refresh_events_created (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    $ensured = true;
}

function db_ui_refresh_section_version_get(string $sectionKey): ?array
{
    db_ui_refresh_schema_ensure();

    return db_select_one(
        'SELECT section_key, version_counter, fingerprint, snapshot_key, domains_json, ui_sections_json, metadata_json, last_job_key, last_status, last_event_id, last_finished_at, created_at, updated_at
         FROM ui_refresh_section_versions
         WHERE section_key = ?
         LIMIT 1',
        [mb_substr(trim($sectionKey), 0, 190)]
    );
}

function db_ui_refresh_section_versions_get_many(array $sectionKeys): array
{
    db_ui_refresh_schema_ensure();

    $normalized = array_values(array_unique(array_filter(array_map(
        static fn (mixed $sectionKey): string => trim((string) $sectionKey),
        $sectionKeys
    ), static fn (string $sectionKey): bool => $sectionKey !== '')));

    if ($normalized === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalized), '?'));

    return db_select(
        "SELECT section_key, version_counter, fingerprint, snapshot_key, domains_json, ui_sections_json, metadata_json, last_job_key, last_status, last_event_id, last_finished_at, created_at, updated_at
         FROM ui_refresh_section_versions
         WHERE section_key IN ({$placeholders})",
        $normalized
    );
}

function db_ui_refresh_section_version_upsert(array $row): bool
{
    db_ui_refresh_schema_ensure();

    $sectionKey = mb_substr(trim((string) ($row['section_key'] ?? '')), 0, 190);
    if ($sectionKey === '') {
        return false;
    }

    $domainsJson = isset($row['domains_json']) ? (string) $row['domains_json'] : null;
    $uiSectionsJson = isset($row['ui_sections_json']) ? (string) $row['ui_sections_json'] : null;
    $metadataJson = isset($row['metadata_json']) ? (string) $row['metadata_json'] : null;
    $fingerprint = trim((string) ($row['fingerprint'] ?? ''));
    $snapshotKey = trim((string) ($row['snapshot_key'] ?? ''));
    $jobKey = trim((string) ($row['last_job_key'] ?? ''));
    $status = trim((string) ($row['last_status'] ?? ''));

    return db_execute(
        'INSERT INTO ui_refresh_section_versions (
            section_key, version_counter, fingerprint, snapshot_key, domains_json, ui_sections_json, metadata_json, last_job_key, last_status, last_event_id, last_finished_at
         ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
         )
         ON DUPLICATE KEY UPDATE
            version_counter = VALUES(version_counter),
            fingerprint = VALUES(fingerprint),
            snapshot_key = VALUES(snapshot_key),
            domains_json = VALUES(domains_json),
            ui_sections_json = VALUES(ui_sections_json),
            metadata_json = VALUES(metadata_json),
            last_job_key = VALUES(last_job_key),
            last_status = VALUES(last_status),
            last_event_id = VALUES(last_event_id),
            last_finished_at = VALUES(last_finished_at),
            updated_at = CURRENT_TIMESTAMP',
        [
            $sectionKey,
            max(0, (int) ($row['version_counter'] ?? 0)),
            $fingerprint !== '' ? mb_substr($fingerprint, 0, 64) : null,
            $snapshotKey !== '' ? mb_substr($snapshotKey, 0, 190) : null,
            $domainsJson,
            $uiSectionsJson,
            $metadataJson,
            $jobKey !== '' ? mb_substr($jobKey, 0, 190) : null,
            $status !== '' ? mb_substr($status, 0, 40) : null,
            isset($row['last_event_id']) ? max(0, (int) $row['last_event_id']) : null,
            isset($row['last_finished_at']) ? (string) $row['last_finished_at'] : null,
        ]
    );
}

function db_ui_refresh_event_insert(array $row): int
{
    db_ui_refresh_schema_ensure();

    $eventType = trim((string) ($row['event_type'] ?? 'job_completed'));
    $eventKey = trim((string) ($row['event_key'] ?? ''));
    $jobKey = mb_substr(trim((string) ($row['job_key'] ?? 'unknown_job')), 0, 190);
    $jobStatus = trim((string) ($row['job_status'] ?? 'unknown'));
    $finishedAt = (string) ($row['finished_at'] ?? gmdate('Y-m-d H:i:s'));
    $domainsJson = isset($row['domains_json']) ? (string) $row['domains_json'] : null;
    $uiSectionsJson = isset($row['ui_sections_json']) ? (string) $row['ui_sections_json'] : null;
    $sectionVersionsJson = isset($row['section_versions_json']) ? (string) $row['section_versions_json'] : null;
    $payloadJson = isset($row['payload_json']) ? (string) $row['payload_json'] : null;

    db_execute(
        'INSERT INTO ui_refresh_events (event_type, event_key, job_key, job_status, finished_at, domains_json, ui_sections_json, section_versions_json, payload_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            updated_at = CURRENT_TIMESTAMP,
            id = LAST_INSERT_ID(id),
            payload_json = COALESCE(VALUES(payload_json), payload_json),
            section_versions_json = COALESCE(VALUES(section_versions_json), section_versions_json)',
        [
            $eventType !== '' ? mb_substr($eventType, 0, 80) : 'job_completed',
            $eventKey !== '' ? mb_substr($eventKey, 0, 190) : null,
            $jobKey,
            $jobStatus !== '' ? mb_substr($jobStatus, 0, 40) : 'unknown',
            $finishedAt,
            $domainsJson,
            $uiSectionsJson,
            $sectionVersionsJson,
            $payloadJson,
        ]
    );

    return (int) db()->lastInsertId();
}

function db_ui_refresh_events_after(int $afterId = 0, int $limit = 25): array
{
    db_ui_refresh_schema_ensure();

    return db_select(
        'SELECT id, event_type, event_key, job_key, job_status, finished_at, domains_json, ui_sections_json, section_versions_json, payload_json, created_at, updated_at
         FROM ui_refresh_events
         WHERE id > ?
         ORDER BY id ASC
         LIMIT ' . max(1, min(100, $limit)),
        [max(0, $afterId)]
    );
}

function db_ui_refresh_latest_event(): ?array
{
    db_ui_refresh_schema_ensure();

    return db_select_one(
        'SELECT id, event_type, event_key, job_key, job_status, finished_at, domains_json, ui_sections_json, section_versions_json, payload_json, created_at, updated_at
         FROM ui_refresh_events
         ORDER BY id DESC
         LIMIT 1'
    );
}

function db_runner_lock_acquire(string $lockName, int $timeoutSeconds = 0): bool
{
    $row = db_select_one('SELECT GET_LOCK(?, ?) AS lock_acquired', [$lockName, max(0, $timeoutSeconds)]);

    return (int) ($row['lock_acquired'] ?? 0) === 1;
}

function db_runner_lock_release(string $lockName): bool
{
    $row = db_select_one('SELECT RELEASE_LOCK(?) AS lock_released', [$lockName]);

    return (int) ($row['lock_released'] ?? 0) === 1;
}

function db_runner_lock_holder_connection_id(string $lockName): ?int
{
    $row = db_select_one('SELECT IS_USED_LOCK(?) AS connection_id', [$lockName]);
    $connectionId = (int) ($row['connection_id'] ?? 0);

    return $connectionId > 0 ? $connectionId : null;
}

function db_runner_lock_force_release(string $lockName): bool
{
    $connectionId = db_runner_lock_holder_connection_id($lockName);
    if ($connectionId === null) {
        return true;
    }

    try {
        db()->exec('KILL CONNECTION ' . $connectionId);
    } catch (Throwable) {
        return false;
    }

    return db_runner_lock_holder_connection_id($lockName) === null;
}

function db_scheduler_daemon_state_ensure(): void
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_daemon_state (
        daemon_key VARCHAR(64) NOT NULL PRIMARY KEY,
        owner_token VARCHAR(190) DEFAULT NULL,
        owner_label VARCHAR(190) DEFAULT NULL,
        owner_pid INT UNSIGNED DEFAULT NULL,
        owner_hostname VARCHAR(190) DEFAULT NULL,
        status VARCHAR(32) NOT NULL DEFAULT \'stopped\',
        loop_state VARCHAR(64) DEFAULT NULL,
        stop_requested TINYINT(1) NOT NULL DEFAULT 0,
        restart_requested TINYINT(1) NOT NULL DEFAULT 0,
        active_dispatch_count INT UNSIGNED NOT NULL DEFAULT 0,
        current_loop_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
        current_memory_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0,
        started_at DATETIME DEFAULT NULL,
        heartbeat_at DATETIME DEFAULT NULL,
        lease_expires_at DATETIME DEFAULT NULL,
        last_dispatch_at DATETIME DEFAULT NULL,
        last_recovery_at DATETIME DEFAULT NULL,
        last_recovery_event VARCHAR(500) DEFAULT NULL,
        last_watchdog_at DATETIME DEFAULT NULL,
        watchdog_status VARCHAR(64) DEFAULT NULL,
        wake_requested_at DATETIME DEFAULT NULL,
        last_exit_at DATETIME DEFAULT NULL,
        last_exit_code INT DEFAULT NULL,
        last_exit_reason VARCHAR(500) DEFAULT NULL,
        metadata_json LONGTEXT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_scheduler_daemon_lease (lease_expires_at),
        KEY idx_scheduler_daemon_heartbeat (heartbeat_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');

    db_execute(
        'INSERT INTO scheduler_daemon_state (daemon_key, status, loop_state, watchdog_status)
         VALUES (?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE daemon_key = daemon_key',
        ['master', 'stopped', 'idle', 'unknown']
    );
}

function db_scheduler_daemon_state_fetch(string $daemonKey = 'master'): ?array
{
    db_scheduler_daemon_state_ensure();

    $row = db_select_one(
        'SELECT daemon_key, owner_token, owner_label, owner_pid, owner_hostname, status, loop_state, stop_requested, restart_requested, active_dispatch_count, current_loop_count, current_memory_bytes, started_at, heartbeat_at, lease_expires_at, last_dispatch_at, last_recovery_at, last_recovery_event, last_watchdog_at, watchdog_status, wake_requested_at, last_exit_at, last_exit_code, last_exit_reason, metadata_json, created_at, updated_at
         FROM scheduler_daemon_state
         WHERE daemon_key = ?
         LIMIT 1',
        [mb_substr(trim($daemonKey), 0, 64)]
    );

    return $row !== [] ? $row : null;
}

function db_scheduler_daemon_try_claim(string $daemonKey, string $ownerToken, string $ownerLabel, int $ownerPid, string $ownerHostname, int $leaseTtlSeconds, array $metadata = []): bool
{
    db_scheduler_daemon_state_ensure();

    $safeDaemonKey = mb_substr(trim($daemonKey), 0, 64);
    $safeOwnerToken = mb_substr(trim($ownerToken), 0, 190);
    $safeOwnerLabel = mb_substr(trim($ownerLabel), 0, 190);
    $safeHostname = mb_substr(trim($ownerHostname), 0, 190);
    $safeTtl = max(5, min(300, $leaseTtlSeconds));
    $metadataJson = $metadata !== [] ? json_encode($metadata, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) : null;

    $ok = db_execute(
        'INSERT INTO scheduler_daemon_state (
            daemon_key, owner_token, owner_label, owner_pid, owner_hostname, status, loop_state, stop_requested, restart_requested, active_dispatch_count, current_loop_count, current_memory_bytes, started_at, heartbeat_at, lease_expires_at, metadata_json, created_at, updated_at
         ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, 0, UTC_TIMESTAMP(), UTC_TIMESTAMP(), DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND), ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
         )
         ON DUPLICATE KEY UPDATE
            owner_token = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), VALUES(owner_token), owner_token),
            owner_label = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), VALUES(owner_label), owner_label),
            owner_pid = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), VALUES(owner_pid), owner_pid),
            owner_hostname = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), VALUES(owner_hostname), owner_hostname),
            status = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), VALUES(status), status),
            loop_state = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), VALUES(loop_state), loop_state),
            stop_requested = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), 0, stop_requested),
            restart_requested = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), 0, restart_requested),
            active_dispatch_count = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), 0, active_dispatch_count),
            current_loop_count = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), 0, current_loop_count),
            current_memory_bytes = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), 0, current_memory_bytes),
            started_at = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), UTC_TIMESTAMP(), started_at),
            heartbeat_at = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), UTC_TIMESTAMP(), heartbeat_at),
            lease_expires_at = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND), lease_expires_at),
            metadata_json = IF(owner_token IS NULL OR owner_token = VALUES(owner_token) OR lease_expires_at IS NULL OR lease_expires_at <= UTC_TIMESTAMP(), VALUES(metadata_json), metadata_json),
            updated_at = CURRENT_TIMESTAMP',
        [$safeDaemonKey, $safeOwnerToken, $safeOwnerLabel, max(0, $ownerPid), $safeHostname, 'running', 'starting', $safeTtl, $metadataJson, $safeTtl]
    );
    if (!$ok) {
        return false;
    }

    $row = db_scheduler_daemon_state_fetch($safeDaemonKey);

    return (string) ($row['owner_token'] ?? '') === $safeOwnerToken;
}

function db_scheduler_daemon_heartbeat(string $daemonKey, string $ownerToken, array $state = [], int $leaseTtlSeconds = 30): bool
{
    db_scheduler_daemon_state_ensure();

    $safeDaemonKey = mb_substr(trim($daemonKey), 0, 64);
    $safeOwnerToken = mb_substr(trim($ownerToken), 0, 190);
    $safeTtl = max(5, min(300, $leaseTtlSeconds));
    $status = array_key_exists('status', $state) ? mb_substr(trim((string) $state['status']), 0, 32) : null;
    $loopState = array_key_exists('loop_state', $state) ? mb_substr(trim((string) $state['loop_state']), 0, 64) : null;
    $dispatchCount = array_key_exists('active_dispatch_count', $state) ? max(0, (int) $state['active_dispatch_count']) : null;
    $loopCount = array_key_exists('current_loop_count', $state) ? max(0, (int) $state['current_loop_count']) : null;
    $memoryBytes = array_key_exists('current_memory_bytes', $state) ? max(0, (int) $state['current_memory_bytes']) : null;
    $metadataJson = array_key_exists('metadata', $state) && is_array($state['metadata'])
        ? json_encode($state['metadata'], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE)
        : null;
    $touchDispatch = !empty($state['touch_last_dispatch']);
    $touchRecovery = !empty($state['touch_last_recovery']);
    $recoveryEvent = array_key_exists('last_recovery_event', $state) ? mb_substr(trim((string) $state['last_recovery_event']), 0, 500) : null;

    return db_execute(
        'UPDATE scheduler_daemon_state
         SET heartbeat_at = UTC_TIMESTAMP(),
             lease_expires_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND),
             status = COALESCE(?, status),
             loop_state = COALESCE(?, loop_state),
             active_dispatch_count = COALESCE(?, active_dispatch_count),
             current_loop_count = COALESCE(?, current_loop_count),
             current_memory_bytes = COALESCE(?, current_memory_bytes),
             last_dispatch_at = CASE WHEN ? = 1 THEN UTC_TIMESTAMP() ELSE last_dispatch_at END,
             last_recovery_at = CASE WHEN ? = 1 THEN UTC_TIMESTAMP() ELSE last_recovery_at END,
             last_recovery_event = CASE WHEN ? <> \'\' THEN ? ELSE last_recovery_event END,
             metadata_json = COALESCE(?, metadata_json),
             updated_at = CURRENT_TIMESTAMP
         WHERE daemon_key = ?
           AND owner_token = ?
         LIMIT 1',
        [$safeTtl, $status, $loopState, $dispatchCount, $loopCount, $memoryBytes, $touchDispatch ? 1 : 0, $touchRecovery ? 1 : 0, $recoveryEvent ?? '', $recoveryEvent, $metadataJson, $safeDaemonKey, $safeOwnerToken]
    );
}

function db_scheduler_daemon_release(string $daemonKey, string $ownerToken, string $status = 'stopped', ?int $exitCode = null, string $exitReason = ''): bool
{
    db_scheduler_daemon_state_ensure();

    return db_execute(
        'UPDATE scheduler_daemon_state
         SET status = ?,
             loop_state = ?,
             owner_token = NULL,
             owner_label = NULL,
             owner_pid = NULL,
             owner_hostname = NULL,
             active_dispatch_count = 0,
             lease_expires_at = NULL,
             wake_requested_at = NULL,
             stop_requested = 0,
             restart_requested = 0,
             last_exit_at = UTC_TIMESTAMP(),
             last_exit_code = ?,
             last_exit_reason = CASE WHEN ? <> \'\' THEN ? ELSE last_exit_reason END,
             updated_at = CURRENT_TIMESTAMP
         WHERE daemon_key = ?
           AND owner_token = ?
         LIMIT 1',
        [mb_substr(trim($status), 0, 32), 'idle', $exitCode, mb_substr(trim($exitReason), 0, 500), mb_substr(trim($exitReason), 0, 500), mb_substr(trim($daemonKey), 0, 64), mb_substr(trim($ownerToken), 0, 190)]
    );
}

function db_scheduler_daemon_watchdog_touch(string $daemonKey, string $watchdogStatus, array $metadata = []): bool
{
    db_scheduler_daemon_state_ensure();

    $metadataJson = $metadata !== [] ? json_encode($metadata, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) : null;

    return db_execute(
        'UPDATE scheduler_daemon_state
         SET last_watchdog_at = UTC_TIMESTAMP(),
             watchdog_status = ?,
             metadata_json = COALESCE(?, metadata_json),
             updated_at = CURRENT_TIMESTAMP
         WHERE daemon_key = ?
         LIMIT 1',
        [mb_substr(trim($watchdogStatus), 0, 64), $metadataJson, mb_substr(trim($daemonKey), 0, 64)]
    );
}

function db_scheduler_daemon_set_control_flags(string $daemonKey, ?bool $stopRequested = null, ?bool $restartRequested = null): bool
{
    db_scheduler_daemon_state_ensure();

    return db_execute(
        'UPDATE scheduler_daemon_state
         SET stop_requested = COALESCE(?, stop_requested),
             restart_requested = COALESCE(?, restart_requested),
             updated_at = CURRENT_TIMESTAMP
         WHERE daemon_key = ?
         LIMIT 1',
        [$stopRequested === null ? null : ($stopRequested ? 1 : 0), $restartRequested === null ? null : ($restartRequested ? 1 : 0), mb_substr(trim($daemonKey), 0, 64)]
    );
}

function db_scheduler_daemon_request_wake(string $daemonKey = 'master'): bool
{
    db_scheduler_daemon_state_ensure();

    return db_execute(
        'UPDATE scheduler_daemon_state
         SET wake_requested_at = UTC_TIMESTAMP(),
             updated_at = CURRENT_TIMESTAMP
         WHERE daemon_key = ?
         LIMIT 1',
        [mb_substr(trim($daemonKey), 0, 64)]
    );
}

function db_scheduler_daemon_force_reset(string $daemonKey = 'master', string $exitReason = ''): bool
{
    db_scheduler_daemon_state_ensure();

    return db_execute(
        'UPDATE scheduler_daemon_state
         SET status = ?,
             loop_state = ?,
             owner_token = NULL,
             owner_label = NULL,
             owner_pid = NULL,
             owner_hostname = NULL,
             active_dispatch_count = 0,
             stop_requested = 0,
             restart_requested = 0,
             lease_expires_at = NULL,
             heartbeat_at = NULL,
             wake_requested_at = NULL,
             last_exit_at = UTC_TIMESTAMP(),
             last_exit_reason = CASE WHEN ? <> \'\' THEN ? ELSE last_exit_reason END,
             updated_at = CURRENT_TIMESTAMP
         WHERE daemon_key = ?
         LIMIT 1',
        ['stopped', 'idle', mb_substr(trim($exitReason), 0, 500), mb_substr(trim($exitReason), 0, 500), mb_substr(trim($daemonKey), 0, 64)]
    );
}

function db_sync_schedule_registry_tables_ensure(): void
{
    db_execute('CREATE TABLE IF NOT EXISTS sync_schedules (
        id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        job_key VARCHAR(190) NOT NULL,
        enabled TINYINT(1) NOT NULL DEFAULT 1,
        interval_seconds INT UNSIGNED NOT NULL,
        offset_seconds INT UNSIGNED NOT NULL DEFAULT 0,
        next_run_at DATETIME DEFAULT NULL,
        last_run_at DATETIME DEFAULT NULL,
        last_status VARCHAR(40) DEFAULT NULL,
        last_error VARCHAR(500) DEFAULT NULL,
        locked_until DATETIME DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY unique_job_key (job_key),
        KEY idx_enabled (enabled),
        KEY idx_next_run_at (next_run_at),
        KEY idx_job_key (job_key)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');

    db_execute("CREATE TABLE IF NOT EXISTS sync_runs (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        dataset_key VARCHAR(190) NOT NULL,
        run_mode ENUM('full', 'incremental') NOT NULL DEFAULT 'incremental',
        run_status ENUM('running', 'success', 'failed') NOT NULL DEFAULT 'running',
        started_at DATETIME NOT NULL,
        finished_at DATETIME DEFAULT NULL,
        source_rows INT UNSIGNED NOT NULL DEFAULT 0,
        written_rows INT UNSIGNED NOT NULL DEFAULT 0,
        cursor_start VARCHAR(190) DEFAULT NULL,
        cursor_end VARCHAR(190) DEFAULT NULL,
        error_message VARCHAR(500) DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_dataset_started (dataset_key, started_at),
        KEY idx_run_status (run_status),
        KEY idx_sync_runs_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
}

function db_sync_schedule_registry_columns_ensure(): void
{
    db_sync_schedule_registry_tables_ensure();

    db_ensure_table_column('sync_schedules', 'offset_seconds', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'interval_minutes', 'INT UNSIGNED NOT NULL DEFAULT 5');
    db_ensure_table_column('sync_schedules', 'offset_minutes', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'priority', "VARCHAR(20) NOT NULL DEFAULT 'normal'");
    db_ensure_table_column('sync_schedules', 'concurrency_policy', "VARCHAR(40) NOT NULL DEFAULT 'single'");
    db_ensure_table_column('sync_schedules', 'timeout_seconds', 'INT UNSIGNED NOT NULL DEFAULT 300');
    db_ensure_table_column('sync_schedules', 'last_started_at', 'DATETIME DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_finished_at', 'DATETIME DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'latency_sensitive', 'TINYINT(1) NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'user_facing', 'TINYINT(1) NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'consecutive_deferrals', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'last_duration_seconds', 'DECIMAL(10,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'average_duration_seconds', 'DECIMAL(10,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'p95_duration_seconds', 'DECIMAL(10,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_result', 'VARCHAR(120) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'next_due_at', 'DATETIME DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'current_state', "ENUM('running', 'waiting', 'stopped') NOT NULL DEFAULT 'waiting'");
    db_ensure_table_column('sync_schedules', 'tuning_mode', "ENUM('automatic', 'manual') NOT NULL DEFAULT 'automatic'");
    db_ensure_table_column('sync_schedules', 'discovered_from_code', 'TINYINT(1) NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'explicitly_configured', 'TINYINT(1) NOT NULL DEFAULT 1');
    db_ensure_table_column('sync_schedules', 'last_auto_tuned_at', 'DATETIME DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_auto_tune_reason', 'VARCHAR(500) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'degraded_until', 'DATETIME DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'failure_streak', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'lock_conflict_count', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'timeout_count', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'resource_class', "VARCHAR(20) NOT NULL DEFAULT 'learning'");
    db_ensure_table_column('sync_schedules', 'resource_class_confidence', 'DECIMAL(6,4) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'telemetry_sample_count', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'learning_mode', 'TINYINT(1) NOT NULL DEFAULT 1');
    db_ensure_table_column('sync_schedules', 'allow_parallel', 'TINYINT(1) NOT NULL DEFAULT 1');
    db_ensure_table_column('sync_schedules', 'prefers_solo', 'TINYINT(1) NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'must_run_alone', 'TINYINT(1) NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'preferred_max_parallelism', 'INT UNSIGNED NOT NULL DEFAULT 1');
    db_ensure_table_column('sync_schedules', 'latest_allowed_start_at', 'DATETIME DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_cpu_percent', 'DECIMAL(8,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'average_cpu_percent', 'DECIMAL(8,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'p95_cpu_percent', 'DECIMAL(8,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_memory_peak_bytes', 'BIGINT UNSIGNED DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'average_memory_peak_bytes', 'BIGINT UNSIGNED DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'p95_memory_peak_bytes', 'BIGINT UNSIGNED DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_queue_wait_seconds', 'DECIMAL(10,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_lock_wait_seconds', 'DECIMAL(10,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'average_lock_wait_seconds', 'DECIMAL(10,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'last_overlap_count', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'last_overlapped', 'TINYINT(1) NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'recent_timeout_rate', 'DECIMAL(8,4) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'recent_failure_rate', 'DECIMAL(8,4) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'current_projected_cpu_percent', 'DECIMAL(8,2) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'current_projected_memory_bytes', 'BIGINT UNSIGNED DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'current_pressure_state', "VARCHAR(32) NOT NULL DEFAULT 'healthy'");
    db_ensure_table_column('sync_schedules', 'last_capacity_reason', 'VARCHAR(500) DEFAULT NULL');
    db_ensure_table_column('sync_schedules', 'allow_backfill', 'TINYINT(1) NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'backfill_priority', "VARCHAR(20) NOT NULL DEFAULT 'normal'");
    db_ensure_table_column('sync_schedules', 'min_backfill_gap_seconds', 'INT UNSIGNED NOT NULL DEFAULT 900');
    db_ensure_table_column('sync_schedules', 'max_early_start_seconds', 'INT UNSIGNED NOT NULL DEFAULT 0');
    db_ensure_table_column('sync_schedules', 'execution_mode', "VARCHAR(16) NOT NULL DEFAULT 'php'");
    db_ensure_table_column('sync_schedules', 'last_execution_mode', "VARCHAR(32) NOT NULL DEFAULT 'scheduled'");

    db_execute(
        "UPDATE sync_schedules
         SET interval_minutes = GREATEST(1, COALESCE(interval_minutes, CEIL(interval_seconds / 60), 5)),
             offset_minutes = GREATEST(0, COALESCE(offset_minutes, FLOOR(offset_seconds / 60), 0)),
             priority = COALESCE(NULLIF(priority, ''), 'normal'),
             concurrency_policy = COALESCE(NULLIF(concurrency_policy, ''), 'single'),
             timeout_seconds = GREATEST(30, COALESCE(timeout_seconds, 300)),
             next_due_at = COALESCE(next_due_at, next_run_at),
             latest_allowed_start_at = COALESCE(latest_allowed_start_at, DATE_ADD(COALESCE(next_due_at, next_run_at, UTC_TIMESTAMP()), INTERVAL GREATEST(1, interval_minutes) MINUTE)),
             resource_class = COALESCE(NULLIF(resource_class, ''), 'learning'),
             current_pressure_state = COALESCE(NULLIF(current_pressure_state, ''), 'healthy'),
             execution_mode = CASE
                WHEN LOWER(COALESCE(NULLIF(execution_mode, ''), 'php')) = 'python' THEN 'python'
                ELSE 'php'
             END,
             allow_parallel = IFNULL(allow_parallel, 1),
             prefers_solo = IFNULL(prefers_solo, 0),
             must_run_alone = IFNULL(must_run_alone, 0),
             preferred_max_parallelism = GREATEST(1, IFNULL(preferred_max_parallelism, 1)),
             learning_mode = IFNULL(learning_mode, 1),
             current_state = CASE
                WHEN enabled = 0 THEN 'stopped'
                WHEN degraded_until IS NOT NULL AND degraded_until > UTC_TIMESTAMP() THEN 'stopped'
                WHEN failure_streak >= 3 THEN 'stopped'
                WHEN locked_until IS NOT NULL AND locked_until > UTC_TIMESTAMP() THEN 'running'
                ELSE 'waiting'
             END
         WHERE 1 = 1"
    );

    db_ensure_table_index('sync_schedules', 'idx_sync_schedules_due_lookup', 'INDEX idx_sync_schedules_due_lookup (enabled, current_state, next_due_at, locked_until, id)');
    db_ensure_table_index('sync_schedules', 'idx_sync_schedules_backfill_lookup', 'INDEX idx_sync_schedules_backfill_lookup (enabled, allow_backfill, current_state, degraded_until, locked_until, next_due_at, last_finished_at, id)');
    db_ensure_table_index('sync_schedules', 'idx_sync_schedules_running_lookup', 'INDEX idx_sync_schedules_running_lookup (enabled, current_state, locked_until, last_started_at, id)');
    db_ensure_table_index('sync_runs', 'idx_sync_runs_created_at', 'INDEX idx_sync_runs_created_at (created_at)');
}

function db_sync_schedule_select_columns_sql(): string
{
    db_sync_schedule_registry_columns_ensure();

    return 'id, job_key, enabled, interval_minutes, interval_seconds, offset_seconds, offset_minutes, priority, concurrency_policy, timeout_seconds, next_run_at, next_due_at, latest_allowed_start_at, last_run_at, last_started_at, last_finished_at, latency_sensitive, user_facing, consecutive_deferrals, last_duration_seconds, average_duration_seconds, p95_duration_seconds, last_status, last_result, last_error, current_state, tuning_mode, discovered_from_code, explicitly_configured, last_auto_tuned_at, last_auto_tune_reason, degraded_until, failure_streak, lock_conflict_count, timeout_count, resource_class, resource_class_confidence, telemetry_sample_count, learning_mode, allow_parallel, prefers_solo, must_run_alone, preferred_max_parallelism, last_cpu_percent, average_cpu_percent, p95_cpu_percent, last_memory_peak_bytes, average_memory_peak_bytes, p95_memory_peak_bytes, last_queue_wait_seconds, last_lock_wait_seconds, average_lock_wait_seconds, last_overlap_count, last_overlapped, recent_timeout_rate, recent_failure_rate, current_projected_cpu_percent, current_projected_memory_bytes, current_pressure_state, last_capacity_reason, allow_backfill, backfill_priority, min_backfill_gap_seconds, max_early_start_seconds, execution_mode, last_execution_mode, locked_until, created_at, updated_at';
}

function db_sync_schedule_priority_rank_sql(string $column = 'priority'): string
{
    return "CASE " . $column . " WHEN 'highest' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END";
}

function db_sync_schedule_fetch_due_jobs(int $limit = 20): array
{
    db_sync_schedule_registry_columns_ensure();

    $safeLimit = max(1, min(200, $limit));
    $columns = db_sync_schedule_select_columns_sql();
    $priorityRank = db_sync_schedule_priority_rank_sql();

    return db_select(
        'SELECT ' . $columns . '
         FROM sync_schedules
         WHERE enabled = 1
           AND next_due_at IS NOT NULL
           AND next_due_at <= UTC_TIMESTAMP()
           AND (locked_until IS NULL OR locked_until <= UTC_TIMESTAMP())
           AND current_state <> ?
         ORDER BY ' . $priorityRank . ' ASC, next_due_at ASC, id ASC
         LIMIT ' . $safeLimit,
        ['stopped']
    );
}

function db_sync_schedule_fetch_backfill_candidates(int $limit = 20): array
{
    db_sync_schedule_registry_columns_ensure();

    $safeLimit = max(1, min(200, $limit));
    $columns = db_sync_schedule_select_columns_sql();
    $priorityRank = db_sync_schedule_priority_rank_sql('backfill_priority');

    return db_select(
        'SELECT ' . $columns . '
         FROM sync_schedules
         WHERE enabled = 1
           AND allow_backfill = 1
           AND current_state <> ?
           AND (locked_until IS NULL OR locked_until <= UTC_TIMESTAMP())
           AND (degraded_until IS NULL OR degraded_until <= UTC_TIMESTAMP())
           AND (next_due_at IS NULL OR next_due_at > UTC_TIMESTAMP())
           AND (
                last_finished_at IS NULL
                OR TIMESTAMPDIFF(SECOND, last_finished_at, UTC_TIMESTAMP()) >= GREATEST(60, min_backfill_gap_seconds)
           )
           AND (
                max_early_start_seconds <= 0
                OR next_due_at IS NULL
                OR TIMESTAMPDIFF(SECOND, UTC_TIMESTAMP(), next_due_at) <= max_early_start_seconds
           )
         ORDER BY ' . $priorityRank . ' ASC,
                  CASE WHEN last_finished_at IS NULL THEN 0 ELSE 1 END ASC,
                  last_finished_at ASC,
                  next_due_at ASC,
                  id ASC
         LIMIT ' . $safeLimit,
        ['stopped']
    );
}

function db_sync_schedule_fetch_locked_due_jobs(int $limit = 50): array
{
    db_sync_schedule_registry_columns_ensure();

    $safeLimit = max(1, min(200, $limit));
    $columns = db_sync_schedule_select_columns_sql();

    return db_select(
        'SELECT ' . $columns . '
         FROM sync_schedules
         WHERE enabled = 1
           AND next_due_at IS NOT NULL
           AND next_due_at <= UTC_TIMESTAMP()
           AND locked_until IS NOT NULL
           AND locked_until > UTC_TIMESTAMP()
         ORDER BY next_due_at ASC, id ASC
         LIMIT ' . $safeLimit
    );
}

function db_sync_schedule_claim_job(int $scheduleId, int $lockTtlSeconds = 300): ?array
{
    db_sync_schedule_registry_columns_ensure();
    db_scheduler_job_current_status_ensure();

    $safeLockTtl = max(30, min(7200, $lockTtlSeconds));

    $stmt = db()->prepare(
        'UPDATE sync_schedules
         SET locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND),
             last_status = ?,
             last_error = NULL,
             current_state = ?,
             last_execution_mode = \'scheduled\',
             last_started_at = UTC_TIMESTAMP(),
             latest_allowed_start_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL GREATEST(1, interval_minutes) MINUTE),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
           AND enabled = 1
           AND next_due_at IS NOT NULL
           AND next_due_at <= UTC_TIMESTAMP()
           AND (locked_until IS NULL OR locked_until <= UTC_TIMESTAMP())
         LIMIT 1'
    );

    $stmt->execute([$safeLockTtl, 'running', 'running', $scheduleId]);
    if ($stmt->rowCount() !== 1) {
        return null;
    }

    $claimed = db_sync_schedule_fetch_by_id($scheduleId);
    if (is_array($claimed)) {
        $jobKey = trim((string) ($claimed['job_key'] ?? ''));
        if ($jobKey !== '') {
            db_scheduler_job_current_status_upsert($jobKey, [
                'latest_status' => 'running',
                'latest_event_type' => 'claim',
                'last_started_at' => $claimed['last_started_at'] ?? gmdate('Y-m-d H:i:s'),
                'current_pressure_state' => $claimed['current_pressure_state'] ?? 'healthy',
                'last_pressure_state' => $claimed['current_pressure_state'] ?? null,
                'last_event_at' => $claimed['last_started_at'] ?? gmdate('Y-m-d H:i:s'),
            ]);
        }
    }

    return $claimed;
}

function db_sync_schedule_next_run_at_for_values(int $intervalSeconds, int $offsetSeconds, ?int $nowUnix = null): string
{
    $intervalMinutes = max(1, (int) ceil($intervalSeconds / 60));
    $offsetMinutes = max(0, (int) floor($offsetSeconds / 60));

    return db_sync_schedule_next_due_at_for_minutes($intervalMinutes, $offsetMinutes, $nowUnix);
}

function db_sync_schedule_next_due_at_for_minutes(int $intervalMinutes, int $offsetMinutes, ?int $nowUnix = null): string
{
    $safeInterval = max(1, min(1440, $intervalMinutes));
    $safeOffset = max(0, min(1439, $offsetMinutes));
    $referenceUnix = $nowUnix ?? time();
    $referenceMinute = (int) floor($referenceUnix / 60);

    $elapsed = (($referenceMinute - $safeOffset) % $safeInterval + $safeInterval) % $safeInterval;
    $wait = $elapsed === 0 ? $safeInterval : ($safeInterval - $elapsed);
    $nextMinute = $referenceMinute + $wait;

    return gmdate('Y-m-d H:i:s', $nextMinute * 60);
}

function db_sync_schedule_next_run_at_by_id(int $scheduleId, ?int $nowUnix = null): ?string
{
    return db_sync_schedule_next_due_at_by_id($scheduleId, $nowUnix);
}

function db_sync_schedule_next_due_at_by_id(int $scheduleId, ?int $nowUnix = null): ?string
{
    if ($scheduleId <= 0) {
        return null;
    }

    $row = db_select_one(
        'SELECT interval_minutes, offset_minutes
         FROM sync_schedules
         WHERE id = ?
         LIMIT 1',
        [$scheduleId]
    );

    if ($row === null) {
        return null;
    }

    return db_sync_schedule_next_due_at_for_minutes(
        (int) ($row['interval_minutes'] ?? 5),
        (int) ($row['offset_minutes'] ?? 0),
        $nowUnix
    );
}

function db_sync_schedule_mark_success(int $scheduleId, ?array $runtime = null): bool
{
    db_sync_schedule_registry_columns_ensure();
    db_scheduler_job_current_status_ensure();

    $schedule = db_sync_schedule_fetch_by_id($scheduleId);
    $jobKey = trim((string) ($schedule['job_key'] ?? ''));

    $nextDueAt = db_sync_schedule_next_due_at_by_id($scheduleId);
    if ($nextDueAt === null) {
        return false;
    }

    $lastDuration = $runtime !== null ? (float) ($runtime['last_duration_seconds'] ?? 0.0) : null;
    $averageDuration = $runtime !== null ? (float) ($runtime['average_duration_seconds'] ?? 0.0) : null;
    $p95Duration = $runtime !== null ? (float) ($runtime['p95_duration_seconds'] ?? 0.0) : null;
    $lastResult = $runtime !== null ? mb_substr(trim((string) ($runtime['last_result'] ?? 'success')), 0, 120) : 'success';

    $updated = db_execute(
        'UPDATE sync_schedules
         SET last_run_at = UTC_TIMESTAMP(),
             last_finished_at = UTC_TIMESTAMP(),
             last_status = ?,
             last_result = ?,
             last_error = NULL,
             next_run_at = ?,
             next_due_at = ?,
             latest_allowed_start_at = DATE_ADD(?, INTERVAL GREATEST(1, interval_minutes) MINUTE),
             current_state = CASE WHEN enabled = 1 THEN ? ELSE ? END,
             last_execution_mode = COALESCE(NULLIF(last_execution_mode, \'\'), \'scheduled\'),
             last_duration_seconds = ?,
             average_duration_seconds = ?,
             p95_duration_seconds = ?,
             locked_until = NULL,
             consecutive_deferrals = 0,
             failure_streak = 0,
             degraded_until = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        ['success', $lastResult, $nextDueAt, $nextDueAt, $nextDueAt, 'waiting', 'stopped', $lastDuration, $averageDuration, $p95Duration, $scheduleId]
    );

    if ($updated && $jobKey !== '') {
        $finishedAt = gmdate('Y-m-d H:i:s');
        $isSkipLike = in_array($lastResult, ['skipped', 'skipped_no_change', 'skipped_within_freshness_window'], true);
        db_scheduler_job_current_status_upsert($jobKey, [
            'latest_status' => $lastResult,
            'latest_event_type' => 'completion',
            'last_started_at' => $schedule['last_started_at'] ?? null,
            'last_finished_at' => $finishedAt,
            'last_success_at' => $finishedAt,
            'last_failure_at' => $isSkipLike ? ($runtime['last_failure_at'] ?? null) : null,
            'last_failure_message' => null,
            'current_pressure_state' => $runtime['current_pressure_state'] ?? ($schedule['current_pressure_state'] ?? 'healthy'),
            'last_pressure_state' => $runtime['current_pressure_state'] ?? ($schedule['current_pressure_state'] ?? null),
            'recent_timeout_count' => 0,
            'recent_lock_conflict_count' => 0,
            'recent_deferral_count' => 0,
            'recent_skip_count' => $isSkipLike ? max(1, (int) (($runtime['recent_skip_count'] ?? 0))) : 0,
            'change_aware' => $runtime['change_aware'] ?? null,
            'dependencies_json' => $runtime['dependencies_json'] ?? null,
            'last_change_detection_json' => $runtime['last_change_detection_json'] ?? null,
            'last_execution_context_json' => $runtime['last_execution_context_json'] ?? null,
            'last_no_change_skip_at' => $runtime['last_no_change_skip_at'] ?? null,
            'last_no_change_reason' => $runtime['last_no_change_reason'] ?? null,
            'last_event_at' => $finishedAt,
        ]);
    }

    return $updated;
}

function db_sync_schedule_mark_failure(int $scheduleId, string $errorMessage, ?array $runtime = null): bool
{
    db_sync_schedule_registry_columns_ensure();
    db_scheduler_job_current_status_ensure();

    $schedule = db_sync_schedule_fetch_by_id($scheduleId);
    $jobKey = trim((string) ($schedule['job_key'] ?? ''));

    $nextDueAt = db_sync_schedule_next_due_at_by_id($scheduleId);
    if ($nextDueAt === null) {
        return false;
    }

    $message = mb_substr($errorMessage, 0, 500);
    $lastDuration = $runtime !== null ? (float) ($runtime['last_duration_seconds'] ?? 0.0) : null;
    $averageDuration = $runtime !== null ? (float) ($runtime['average_duration_seconds'] ?? 0.0) : null;
    $p95Duration = $runtime !== null ? (float) ($runtime['p95_duration_seconds'] ?? 0.0) : null;
    $lastResult = $runtime !== null ? mb_substr(trim((string) ($runtime['last_result'] ?? 'failed')), 0, 120) : 'failed';

    $updated = db_execute(
        'UPDATE sync_schedules
         SET last_run_at = UTC_TIMESTAMP(),
             last_finished_at = UTC_TIMESTAMP(),
             last_status = ?,
             last_result = ?,
             last_error = ?,
             next_run_at = ?,
             next_due_at = ?,
             latest_allowed_start_at = DATE_ADD(?, INTERVAL GREATEST(1, interval_minutes) MINUTE),
             current_state = CASE WHEN enabled = 1 THEN ? ELSE ? END,
             last_execution_mode = COALESCE(NULLIF(last_execution_mode, \'\'), \'scheduled\'),
             last_duration_seconds = ?,
             average_duration_seconds = ?,
             p95_duration_seconds = ?,
             locked_until = NULL,
             consecutive_deferrals = 0,
             failure_streak = failure_streak + 1,
             timeout_count = timeout_count + CASE WHEN ? = 1 THEN 1 ELSE 0 END,
             degraded_until = CASE WHEN failure_streak + 1 >= 3 THEN DATE_ADD(UTC_TIMESTAMP(), INTERVAL 30 MINUTE) ELSE degraded_until END,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        ['failed', $lastResult, $message, $nextDueAt, $nextDueAt, $nextDueAt, 'waiting', 'stopped', $lastDuration, $averageDuration, $p95Duration, !empty($runtime['timeout']) ? 1 : 0, $scheduleId]
    );

    if ($updated && $jobKey !== '') {
        $finishedAt = gmdate('Y-m-d H:i:s');
        $current = db_scheduler_job_current_status_fetch_map([$jobKey])[$jobKey] ?? [];
        db_scheduler_job_current_status_upsert($jobKey, [
            'latest_status' => 'failed',
            'latest_event_type' => !empty($runtime['timeout']) ? 'timeout' : 'failure',
            'last_started_at' => $schedule['last_started_at'] ?? null,
            'last_finished_at' => $finishedAt,
            'last_failure_at' => $finishedAt,
            'last_failure_message' => $message,
            'current_pressure_state' => $runtime['current_pressure_state'] ?? ($schedule['current_pressure_state'] ?? 'healthy'),
            'last_pressure_state' => $runtime['current_pressure_state'] ?? ($schedule['current_pressure_state'] ?? null),
            'recent_timeout_count' => max(0, (int) ($current['recent_timeout_count'] ?? 0)) + (!empty($runtime['timeout']) ? 1 : 0),
            'recent_lock_conflict_count' => max(0, (int) ($current['recent_lock_conflict_count'] ?? 0)),
            'recent_deferral_count' => max(0, (int) ($current['recent_deferral_count'] ?? 0)),
            'recent_skip_count' => 0,
            'change_aware' => $runtime['change_aware'] ?? null,
            'dependencies_json' => $runtime['dependencies_json'] ?? null,
            'last_change_detection_json' => $runtime['last_change_detection_json'] ?? null,
            'last_execution_context_json' => $runtime['last_execution_context_json'] ?? null,
            'last_event_at' => $finishedAt,
        ]);
    }

    return $updated;
}

function db_sync_schedule_fetch_by_job_keys(array $jobKeys): array
{
    db_sync_schedule_registry_columns_ensure();

    $keys = array_values(array_filter(array_map(static fn (mixed $jobKey): string => trim((string) $jobKey), $jobKeys), static fn (string $jobKey): bool => $jobKey !== ''));
    if ($keys === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($keys), '?'));
    $columns = db_sync_schedule_select_columns_sql();

    return db_select(
        "SELECT $columns
         FROM sync_schedules
         WHERE job_key IN ($placeholders)",
        $keys
    );
}

function db_sync_schedule_fetch_all(): array
{
    db_sync_schedule_registry_columns_ensure();

    $columns = db_sync_schedule_select_columns_sql();
    $priorityRank = db_sync_schedule_priority_rank_sql();

    return db_select(
        'SELECT ' . $columns . '
         FROM sync_schedules
         ORDER BY explicitly_configured DESC, ' . $priorityRank . ' ASC, job_key ASC'
    );
}

function db_sync_schedule_fetch_by_id(int $scheduleId): ?array
{
    db_sync_schedule_registry_columns_ensure();

    if ($scheduleId <= 0) {
        return null;
    }

    $columns = db_sync_schedule_select_columns_sql();
    $row = db_select_one(
        'SELECT ' . $columns . '
         FROM sync_schedules
         WHERE id = ?
         LIMIT 1',
        [$scheduleId]
    );

    return $row !== [] ? $row : null;
}

function db_sync_schedule_running_job_keys(array $jobKeys): array
{
    db_sync_schedule_registry_columns_ensure();

    $keys = array_values(array_filter(array_map(static fn (mixed $jobKey): string => trim((string) $jobKey), $jobKeys), static fn (string $jobKey): bool => $jobKey !== ''));
    if ($keys === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($keys), '?'));
    $rows = db_select(
        "SELECT job_key
         FROM sync_schedules
         WHERE job_key IN ($placeholders)
           AND enabled = 1
           AND current_state = 'running'
           AND locked_until IS NOT NULL
           AND locked_until > UTC_TIMESTAMP()
         ORDER BY job_key ASC",
        $keys
    );

    return array_values(array_filter(array_map(
        static fn (array $row): string => trim((string) ($row['job_key'] ?? '')),
        $rows
    ), static fn (string $jobKey): bool => $jobKey !== ''));
}


function db_sync_schedule_fetch_running_jobs(?array $excludeScheduleIds = null): array
{
    db_sync_schedule_registry_columns_ensure();

    $columns = db_sync_schedule_select_columns_sql();
    $params = [];
    $sql = 'SELECT ' . $columns . '
'
        . 'FROM sync_schedules
'
        . 'WHERE enabled = 1
'
        . '  AND current_state = \'running\'
'
        . '  AND locked_until IS NOT NULL
'
        . '  AND locked_until > UTC_TIMESTAMP()';

    $ids = array_values(array_filter(array_map('intval', $excludeScheduleIds ?? []), static fn (int $id): bool => $id > 0));
    if ($ids !== []) {
        $sql .= '
  AND id NOT IN (' . implode(',', array_fill(0, count($ids), '?')) . ')';
        $params = $ids;
    }

    $sql .= '
ORDER BY last_started_at ASC, id ASC';

    return db_select($sql, $params);
}

function db_sync_schedule_release_claim(int $scheduleId, ?string $nextDueAt = null, string $reason = ''): bool
{
    db_sync_schedule_registry_columns_ensure();

    if ($scheduleId <= 0) {
        return false;
    }

    $resolvedNextDueAt = $nextDueAt;
    if ($resolvedNextDueAt === null || trim($resolvedNextDueAt) === '') {
        $resolvedNextDueAt = gmdate('Y-m-d H:i:s', time() + 60);
    }

    return db_execute(
        'UPDATE sync_schedules
         SET locked_until = NULL,
             current_state = CASE WHEN enabled = 1 THEN \'waiting\' ELSE \'stopped\' END,
             next_run_at = CASE WHEN enabled = 1 THEN ? ELSE NULL END,
             next_due_at = CASE WHEN enabled = 1 THEN ? ELSE NULL END,
             latest_allowed_start_at = CASE WHEN enabled = 1 THEN DATE_ADD(?, INTERVAL GREATEST(1, interval_minutes) MINUTE) ELSE NULL END,
             last_result = CASE WHEN ? <> \'\' THEN ? ELSE last_result END,
             last_capacity_reason = CASE WHEN ? <> \'\' THEN ? ELSE last_capacity_reason END,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        [$resolvedNextDueAt, $resolvedNextDueAt, $resolvedNextDueAt, $reason, mb_substr($reason, 0, 120), $reason, mb_substr($reason, 0, 500), $scheduleId]
    );
}

function db_sync_schedule_ensure_job(string $jobKey, int $enabled, int $intervalSeconds, int $offsetSeconds = 0, array $options = []): bool
{
    db_sync_schedule_registry_columns_ensure();

    $normalizedKey = trim($jobKey);
    if ($normalizedKey === '') {
        return false;
    }

    $intervalMinutes = max(1, min(1440, (int) ($options['interval_minutes'] ?? (int) ceil($intervalSeconds / 60))));
    $safeIntervalSeconds = max(60, $intervalMinutes * 60);
    $offsetMinutes = max(0, min(1439, (int) ($options['offset_minutes'] ?? (int) floor($offsetSeconds / 60))));
    $safeOffsetSeconds = $offsetMinutes * 60;
    $priority = mb_substr(trim((string) ($options['priority'] ?? 'normal')), 0, 20);
    $concurrencyPolicy = mb_substr(trim((string) ($options['concurrency_policy'] ?? 'single')), 0, 40);
    $timeoutSeconds = max(30, min(7200, (int) ($options['timeout_seconds'] ?? 300)));
    $allowBackfill = !empty($options['allow_backfill']) ? 1 : 0;
    $backfillPriority = mb_substr(trim((string) ($options['backfill_priority'] ?? $priority)), 0, 20);
    $minBackfillGapSeconds = max(60, min(86400, (int) ($options['min_backfill_gap_seconds'] ?? 900)));
    $maxEarlyStartSeconds = max(0, min(86400, (int) ($options['max_early_start_seconds'] ?? 0)));
    $latencySensitive = !empty($options['latency_sensitive']) ? 1 : 0;
    $userFacing = !empty($options['user_facing']) ? 1 : 0;
    $executionMode = strtolower(trim((string) ($options['execution_mode'] ?? 'php'))) === 'python' ? 'python' : 'php';
    $tuningMode = ($options['tuning_mode'] ?? 'automatic') === 'manual' ? 'manual' : 'automatic';
    $discoveredFromCode = !empty($options['discovered_from_code']) ? 1 : 0;
    $explicitlyConfigured = array_key_exists('explicitly_configured', $options) && !$options['explicitly_configured'] ? 0 : 1;
    $nextDueAt = $enabled === 1 ? db_sync_schedule_next_due_at_for_minutes($intervalMinutes, $offsetMinutes) : null;
    $currentState = $enabled === 1 ? 'waiting' : 'stopped';

    return db_execute(
        'INSERT INTO sync_schedules (
            job_key,
            enabled,
            interval_minutes,
            interval_seconds,
            offset_seconds,
            offset_minutes,
            priority,
            concurrency_policy,
            timeout_seconds,
            allow_backfill,
            backfill_priority,
            min_backfill_gap_seconds,
            max_early_start_seconds,
            latency_sensitive,
            user_facing,
            execution_mode,
            next_run_at,
            next_due_at,
            current_state,
            tuning_mode,
            discovered_from_code,
            explicitly_configured,
            last_run_at,
            last_status,
            last_error,
            locked_until
         ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL
         )
         ON DUPLICATE KEY UPDATE
            enabled = sync_schedules.enabled,
            discovered_from_code = GREATEST(sync_schedules.discovered_from_code, VALUES(discovered_from_code)),
            explicitly_configured = GREATEST(sync_schedules.explicitly_configured, VALUES(explicitly_configured)),
            interval_minutes = COALESCE(sync_schedules.interval_minutes, VALUES(interval_minutes)),
            interval_seconds = COALESCE(sync_schedules.interval_seconds, VALUES(interval_seconds)),
            offset_seconds = COALESCE(sync_schedules.offset_seconds, VALUES(offset_seconds)),
            offset_minutes = COALESCE(sync_schedules.offset_minutes, VALUES(offset_minutes)),
            priority = COALESCE(NULLIF(sync_schedules.priority, \'\'), VALUES(priority)),
            concurrency_policy = COALESCE(NULLIF(sync_schedules.concurrency_policy, \'\'), VALUES(concurrency_policy)),
            timeout_seconds = COALESCE(sync_schedules.timeout_seconds, VALUES(timeout_seconds)),
            allow_backfill = COALESCE(sync_schedules.allow_backfill, VALUES(allow_backfill)),
            backfill_priority = COALESCE(NULLIF(sync_schedules.backfill_priority, \'\'), VALUES(backfill_priority)),
            min_backfill_gap_seconds = COALESCE(sync_schedules.min_backfill_gap_seconds, VALUES(min_backfill_gap_seconds)),
            max_early_start_seconds = COALESCE(sync_schedules.max_early_start_seconds, VALUES(max_early_start_seconds)),
            latency_sensitive = GREATEST(sync_schedules.latency_sensitive, VALUES(latency_sensitive)),
            user_facing = GREATEST(sync_schedules.user_facing, VALUES(user_facing)),
            execution_mode = COALESCE(NULLIF(sync_schedules.execution_mode, \'\'), VALUES(execution_mode)),
            tuning_mode = COALESCE(sync_schedules.tuning_mode, VALUES(tuning_mode)),
            next_run_at = CASE
                WHEN sync_schedules.enabled = 1 AND sync_schedules.next_run_at IS NULL THEN VALUES(next_run_at)
                ELSE sync_schedules.next_run_at
            END,
            next_due_at = CASE
                WHEN sync_schedules.enabled = 1 AND sync_schedules.next_due_at IS NULL THEN VALUES(next_due_at)
                ELSE sync_schedules.next_due_at
            END,
            current_state = CASE
                WHEN sync_schedules.enabled = 0 THEN \'stopped\'
                WHEN sync_schedules.current_state = \'running\' THEN sync_schedules.current_state
                ELSE VALUES(current_state)
            END,
            updated_at = CURRENT_TIMESTAMP',
        [$normalizedKey, $enabled, $intervalMinutes, $safeIntervalSeconds, $safeOffsetSeconds, $offsetMinutes, $priority, $concurrencyPolicy, $timeoutSeconds, $allowBackfill, $backfillPriority, $minBackfillGapSeconds, $maxEarlyStartSeconds, $latencySensitive, $userFacing, $executionMode, $nextDueAt, $nextDueAt, $currentState, $tuningMode, $discoveredFromCode, $explicitlyConfigured]
    );
}

function db_sync_schedule_upsert(string $jobKey, int $enabled, int $intervalSeconds, ?int $offsetSeconds = null, array $options = []): bool
{
    db_sync_schedule_registry_columns_ensure();

    $normalizedKey = trim($jobKey);
    if ($normalizedKey === '') {
        return false;
    }

    $existing = db_select_one(
        'SELECT offset_minutes, priority, concurrency_policy, timeout_seconds, tuning_mode, discovered_from_code, explicitly_configured, allow_backfill, backfill_priority, min_backfill_gap_seconds, max_early_start_seconds, latency_sensitive, user_facing, execution_mode
         FROM sync_schedules
         WHERE job_key = ?
         LIMIT 1',
        [$normalizedKey]
    ) ?? [];

    $intervalMinutes = max(1, min(1440, (int) ($options['interval_minutes'] ?? (int) ceil($intervalSeconds / 60))));
    $safeIntervalSeconds = max(60, $intervalMinutes * 60);
    $resolvedOffsetSeconds = $offsetSeconds ?? ((int) ($existing['offset_minutes'] ?? 0) * 60);
    $offsetMinutes = max(0, min(1439, (int) ($options['offset_minutes'] ?? (int) floor($resolvedOffsetSeconds / 60))));
    $safeOffsetSeconds = $offsetMinutes * 60;
    $priority = mb_substr(trim((string) ($options['priority'] ?? ($existing['priority'] ?? 'normal'))), 0, 20);
    $concurrencyPolicy = mb_substr(trim((string) ($options['concurrency_policy'] ?? ($existing['concurrency_policy'] ?? 'single'))), 0, 40);
    $timeoutSeconds = max(30, min(7200, (int) ($options['timeout_seconds'] ?? ($existing['timeout_seconds'] ?? 300))));
    $allowBackfill = !empty($options['allow_backfill'] ?? $existing['allow_backfill'] ?? 0) ? 1 : 0;
    $backfillPriority = mb_substr(trim((string) ($options['backfill_priority'] ?? ($existing['backfill_priority'] ?? $priority))), 0, 20);
    $minBackfillGapSeconds = max(60, min(86400, (int) ($options['min_backfill_gap_seconds'] ?? ($existing['min_backfill_gap_seconds'] ?? 900))));
    $maxEarlyStartSeconds = max(0, min(86400, (int) ($options['max_early_start_seconds'] ?? ($existing['max_early_start_seconds'] ?? 0))));
    $latencySensitive = !empty($options['latency_sensitive'] ?? $existing['latency_sensitive'] ?? 0) ? 1 : 0;
    $userFacing = !empty($options['user_facing'] ?? $existing['user_facing'] ?? 0) ? 1 : 0;
    $executionMode = strtolower(trim((string) ($options['execution_mode'] ?? ($existing['execution_mode'] ?? 'php')))) === 'python' ? 'python' : 'php';
    $tuningMode = ($options['tuning_mode'] ?? ($existing['tuning_mode'] ?? 'automatic')) === 'manual' ? 'manual' : 'automatic';
    $discoveredFromCode = !empty($options['discovered_from_code'] ?? $existing['discovered_from_code'] ?? 0) ? 1 : 0;
    $explicitlyConfigured = array_key_exists('explicitly_configured', $options)
        ? ($options['explicitly_configured'] ? 1 : 0)
        : (int) ($existing['explicitly_configured'] ?? 1);
    $nextDueAt = $enabled === 1 ? db_sync_schedule_next_due_at_for_minutes($intervalMinutes, $offsetMinutes) : null;
    $currentState = $enabled === 1 ? 'waiting' : 'stopped';

    return db_execute(
        'INSERT INTO sync_schedules (
            job_key,
            enabled,
            interval_minutes,
            interval_seconds,
            offset_seconds,
            offset_minutes,
            priority,
            concurrency_policy,
            timeout_seconds,
            allow_backfill,
            backfill_priority,
            min_backfill_gap_seconds,
            max_early_start_seconds,
            latency_sensitive,
            user_facing,
            execution_mode,
            next_run_at,
            next_due_at,
            current_state,
            tuning_mode,
            discovered_from_code,
            explicitly_configured,
            last_run_at,
            last_status,
            last_error,
            locked_until
         ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL
         )
         ON DUPLICATE KEY UPDATE
            enabled = VALUES(enabled),
            interval_minutes = VALUES(interval_minutes),
            interval_seconds = VALUES(interval_seconds),
            offset_seconds = VALUES(offset_seconds),
            offset_minutes = VALUES(offset_minutes),
            priority = VALUES(priority),
            concurrency_policy = VALUES(concurrency_policy),
            timeout_seconds = VALUES(timeout_seconds),
            allow_backfill = VALUES(allow_backfill),
            backfill_priority = VALUES(backfill_priority),
            min_backfill_gap_seconds = VALUES(min_backfill_gap_seconds),
            max_early_start_seconds = VALUES(max_early_start_seconds),
            latency_sensitive = VALUES(latency_sensitive),
            user_facing = VALUES(user_facing),
            execution_mode = VALUES(execution_mode),
            tuning_mode = VALUES(tuning_mode),
            discovered_from_code = VALUES(discovered_from_code),
            explicitly_configured = VALUES(explicitly_configured),
            next_run_at = CASE WHEN VALUES(enabled) = 1 THEN COALESCE(sync_schedules.next_run_at, VALUES(next_run_at)) ELSE NULL END,
            next_due_at = CASE WHEN VALUES(enabled) = 1 THEN COALESCE(sync_schedules.next_due_at, VALUES(next_due_at)) ELSE NULL END,
            current_state = CASE
                WHEN VALUES(enabled) = 0 THEN \'stopped\'
                WHEN sync_schedules.current_state = \'running\' THEN sync_schedules.current_state
                ELSE VALUES(current_state)
            END,
            locked_until = CASE WHEN VALUES(enabled) = 1 THEN sync_schedules.locked_until ELSE NULL END,
            updated_at = CURRENT_TIMESTAMP',
        [$normalizedKey, $enabled, $intervalMinutes, $safeIntervalSeconds, $safeOffsetSeconds, $offsetMinutes, $priority, $concurrencyPolicy, $timeoutSeconds, $allowBackfill, $backfillPriority, $minBackfillGapSeconds, $maxEarlyStartSeconds, $latencySensitive, $userFacing, $executionMode, $nextDueAt, $nextDueAt, $currentState, $tuningMode, $discoveredFromCode, $explicitlyConfigured]
    );
}

function db_sync_schedule_set_next_due_at(int $scheduleId, string $nextDueAt, string $reason = ''): bool
{
    db_sync_schedule_registry_columns_ensure();
    $result = mb_substr($reason, 0, 120);

    return db_execute(
        'UPDATE sync_schedules
         SET next_run_at = ?,
             next_due_at = ?,
             last_result = CASE WHEN ? <> \'\' THEN ? ELSE last_result END,
             consecutive_deferrals = CASE
                WHEN ? <> \'\' AND (LOWER(?) LIKE \'deferred%\' OR LOWER(?) LIKE \'%deferred%\') THEN consecutive_deferrals + 1
                ELSE consecutive_deferrals
             END,
             current_state = CASE WHEN enabled = 1 THEN \'waiting\' ELSE \'stopped\' END,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        [$nextDueAt, $nextDueAt, $result, $result, $result, $result, $result, $scheduleId]
    );
}

function db_sync_schedule_force_due_all_enabled(): int
{
    db_sync_schedule_registry_columns_ensure();

    $stmt = db()->prepare(
        'UPDATE sync_schedules
         SET next_run_at = UTC_TIMESTAMP(),
             next_due_at = UTC_TIMESTAMP(),
             latest_allowed_start_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL GREATEST(1, interval_minutes) MINUTE),
             locked_until = NULL,
             current_state = \'waiting\',
             updated_at = CURRENT_TIMESTAMP
         WHERE enabled = 1'
    );

    $stmt->execute();

    return (int) $stmt->rowCount();
}

function db_sync_schedule_force_due_by_job_keys(array $jobKeys): int
{
    db_sync_schedule_registry_columns_ensure();

    $normalized = [];

    foreach ($jobKeys as $jobKey) {
        $trimmed = trim((string) $jobKey);
        if ($trimmed === '') {
            continue;
        }

        $normalized[$trimmed] = $trimmed;
    }

    $keys = array_values($normalized);

    if ($keys === []) {
        return 0;
    }

    $placeholders = implode(', ', array_fill(0, count($keys), '?'));
    $sql = "UPDATE sync_schedules
            SET next_run_at = UTC_TIMESTAMP(),
                next_due_at = UTC_TIMESTAMP(),
                latest_allowed_start_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL GREATEST(1, interval_minutes) MINUTE),
                locked_until = NULL,
                current_state = 'waiting',
                updated_at = CURRENT_TIMESTAMP
            WHERE enabled = 1
              AND job_key IN ($placeholders)";

    $stmt = db()->prepare($sql);
    $stmt->execute($keys);

    return (int) $stmt->rowCount();
}

function db_sync_schedule_retry_by_job_key(string $jobKey): bool
{
    db_sync_schedule_registry_columns_ensure();

    $normalizedKey = trim($jobKey);
    if ($normalizedKey === '') {
        return false;
    }

    return db_execute(
        'UPDATE sync_schedules
         SET next_run_at = CASE WHEN enabled = 1 THEN UTC_TIMESTAMP() ELSE next_run_at END,
             next_due_at = CASE WHEN enabled = 1 THEN UTC_TIMESTAMP() ELSE next_due_at END,
             latest_allowed_start_at = CASE
                WHEN enabled = 1 THEN DATE_ADD(UTC_TIMESTAMP(), INTERVAL GREATEST(1, interval_minutes) MINUTE)
                ELSE latest_allowed_start_at
             END,
             locked_until = NULL,
             current_state = CASE WHEN enabled = 1 THEN \'waiting\' ELSE \'stopped\' END,
             failure_streak = 0,
             degraded_until = NULL,
             consecutive_deferrals = 0,
             last_status = CASE WHEN enabled = 1 THEN \'queued\' ELSE last_status END,
             last_result = CASE WHEN enabled = 1 THEN \'queued\' ELSE last_result END,
             last_error = CASE WHEN enabled = 1 THEN NULL ELSE last_error END,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_key = ?
         LIMIT 1',
        [$normalizedKey]
    );
}

function db_sync_schedule_stop_for_investigation_by_job_key(string $jobKey, string $reason, int $holdMinutes = 240): bool
{
    db_sync_schedule_registry_columns_ensure();

    $normalizedKey = trim($jobKey);
    if ($normalizedKey === '') {
        return false;
    }

    $message = mb_substr(trim($reason), 0, 500);
    if ($message === '') {
        $message = 'Stopped by operator for investigation.';
    }

    $safeHoldMinutes = max(5, min(1440, $holdMinutes));

    return db_execute(
        'UPDATE sync_schedules
         SET locked_until = NULL,
             current_state = CASE WHEN enabled = 1 THEN \'stopped\' ELSE \'stopped\' END,
             degraded_until = CASE
                WHEN enabled = 1 THEN DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? MINUTE)
                ELSE degraded_until
             END,
             last_status = CASE WHEN enabled = 1 THEN \'stopped\' ELSE last_status END,
             last_result = CASE WHEN enabled = 1 THEN \'investigating\' ELSE last_result END,
             last_error = CASE WHEN enabled = 1 THEN ? ELSE last_error END,
             updated_at = CURRENT_TIMESTAMP
         WHERE job_key = ?
         LIMIT 1',
        [$safeHoldMinutes, $message, $normalizedKey]
    );
}

function db_sync_schedule_apply_adjustment(int $scheduleId, array $changes): bool
{
    db_sync_schedule_registry_columns_ensure();

    $row = db_sync_schedule_fetch_by_id($scheduleId);
    if ($row === null) {
        return false;
    }

    $enabled = (int) ($row['enabled'] ?? 1);
    $intervalMinutes = max(1, min(1440, (int) ($changes['interval_minutes'] ?? $row['interval_minutes'] ?? 5)));
    $offsetMinutes = max(0, min(1439, (int) ($changes['offset_minutes'] ?? $row['offset_minutes'] ?? 0)));
    $timeoutSeconds = max(30, min(7200, (int) ($changes['timeout_seconds'] ?? $row['timeout_seconds'] ?? 300)));
    $preferredMaxParallelism = max(1, min(6, (int) ($changes['preferred_max_parallelism'] ?? $row['preferred_max_parallelism'] ?? 1)));
    $priority = mb_substr(trim((string) ($changes['priority'] ?? $row['priority'] ?? 'normal')), 0, 20);
    $tuningMode = ($changes['tuning_mode'] ?? $row['tuning_mode'] ?? 'automatic') === 'manual' ? 'manual' : 'automatic';
    $reason = mb_substr(trim((string) ($changes['last_auto_tune_reason'] ?? $row['last_auto_tune_reason'] ?? '')), 0, 500);
    $nextDueAt = $enabled === 1 ? db_sync_schedule_next_due_at_for_minutes($intervalMinutes, $offsetMinutes) : null;
    $currentState = $enabled === 1
        ? (($row['current_state'] ?? '') === 'running' ? 'running' : 'waiting')
        : 'stopped';

    return db_execute(
        'UPDATE sync_schedules
         SET interval_minutes = ?,
             interval_seconds = ?,
             offset_minutes = ?,
             offset_seconds = ?,
             timeout_seconds = ?,
             priority = ?,
             tuning_mode = ?,
             preferred_max_parallelism = ?,
             last_auto_tuned_at = CASE WHEN ? <> \'\' THEN UTC_TIMESTAMP() ELSE last_auto_tuned_at END,
             last_auto_tune_reason = CASE WHEN ? <> \'\' THEN ? ELSE last_auto_tune_reason END,
             next_run_at = CASE WHEN enabled = 1 AND current_state <> \'running\' THEN ? ELSE next_run_at END,
             next_due_at = CASE WHEN enabled = 1 AND current_state <> \'running\' THEN ? ELSE next_due_at END,
             current_state = ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        [$intervalMinutes, $intervalMinutes * 60, $offsetMinutes, $offsetMinutes * 60, $timeoutSeconds, $priority, $tuningMode, $preferredMaxParallelism, $reason, $reason, $reason, $nextDueAt, $nextDueAt, $currentState, $scheduleId]
    );
}


function db_sync_schedule_claim_job_forced(int $scheduleId, int $lockTtlSeconds = 300): ?array
{
    db_sync_schedule_registry_columns_ensure();

    $safeLockTtl = max(30, min(7200, $lockTtlSeconds));
    $stmt = db()->prepare(
        'UPDATE sync_schedules
         SET locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND),
             last_status = ?,
             last_error = NULL,
             current_state = ?,
             last_execution_mode = \'idle_backfill\',
             last_started_at = UTC_TIMESTAMP(),
             latest_allowed_start_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL GREATEST(1, interval_minutes) MINUTE),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
           AND enabled = 1
           AND (locked_until IS NULL OR locked_until <= UTC_TIMESTAMP())
           AND current_state <> ?
         LIMIT 1'
    );

    $stmt->execute([$safeLockTtl, 'running', 'running', $scheduleId, 'running']);
    if ($stmt->rowCount() !== 1) {
        return null;
    }

    return db_sync_schedule_fetch_by_id($scheduleId);
}

function db_scheduler_pairing_rules_ensure(): void
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_job_pairing_rules (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        primary_job_key VARCHAR(190) NOT NULL,
        secondary_job_key VARCHAR(190) NOT NULL,
        rule_type VARCHAR(20) NOT NULL,
        source_type VARCHAR(30) NOT NULL DEFAULT "profiling",
        profiling_run_id BIGINT UNSIGNED DEFAULT NULL,
        active TINYINT(1) NOT NULL DEFAULT 1,
        notes VARCHAR(500) DEFAULT NULL,
        metadata_json LONGTEXT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_scheduler_pairing_rule (primary_job_key, secondary_job_key, rule_type),
        KEY idx_scheduler_pairing_rules_type_active (rule_type, active),
        KEY idx_scheduler_pairing_rules_run (profiling_run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
}

function db_scheduler_pairing_rules_fetch_active(): array
{
    db_scheduler_pairing_rules_ensure();

    return db_select(
        'SELECT id, primary_job_key, secondary_job_key, rule_type, source_type, profiling_run_id, active, notes, metadata_json, created_at, updated_at
         FROM scheduler_job_pairing_rules
         WHERE active = 1
         ORDER BY primary_job_key ASC, secondary_job_key ASC, rule_type ASC'
    );
}

function db_scheduler_pairing_rules_replace_from_profiling_run(int $profilingRunId, array $rules, string $actorNote = ''): bool
{
    db_scheduler_pairing_rules_ensure();
    $safeRunId = max(0, $profilingRunId);

    return db_transaction(static function () use ($safeRunId, $rules, $actorNote): bool {
        db_execute('UPDATE scheduler_job_pairing_rules SET active = 0, updated_at = CURRENT_TIMESTAMP WHERE source_type = ?', ['profiling']);
        foreach ($rules as $rule) {
            $primary = mb_substr(trim((string) ($rule['primary_job_key'] ?? '')), 0, 190);
            $secondary = mb_substr(trim((string) ($rule['secondary_job_key'] ?? '')), 0, 190);
            $type = mb_substr(trim((string) ($rule['rule_type'] ?? 'avoid')), 0, 20);
            if ($primary === '' || $secondary === '' || $primary === $secondary) {
                continue;
            }
            if (strcmp($primary, $secondary) > 0) {
                $primaryTmp = $primary;
                $primary = $secondary;
                $secondary = $primaryTmp;
            }
            db_execute(
                'INSERT INTO scheduler_job_pairing_rules (primary_job_key, secondary_job_key, rule_type, source_type, profiling_run_id, active, notes, metadata_json)
                 VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                 ON DUPLICATE KEY UPDATE
                    source_type = VALUES(source_type),
                    profiling_run_id = VALUES(profiling_run_id),
                    active = VALUES(active),
                    notes = VALUES(notes),
                    metadata_json = VALUES(metadata_json),
                    updated_at = CURRENT_TIMESTAMP',
                [
                    $primary,
                    $secondary,
                    $type,
                    'profiling',
                    $safeRunId > 0 ? $safeRunId : null,
                    mb_substr(trim((string) (($rule['notes'] ?? '') ?: $actorNote)), 0, 500),
                    !empty($rule['metadata']) ? json_encode($rule['metadata'], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) : null,
                ]
            );
        }
        return true;
    });
}

function db_scheduler_profiling_tables_ensure(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db_execute('CREATE TABLE IF NOT EXISTS scheduler_profiling_runs (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        run_status VARCHAR(30) NOT NULL,
        current_phase VARCHAR(40) NOT NULL,
        execution_mode VARCHAR(30) NOT NULL DEFAULT "isolated_only",
        started_by VARCHAR(190) NOT NULL,
        scope_json LONGTEXT DEFAULT NULL,
        options_json LONGTEXT DEFAULT NULL,
        selected_job_keys_json LONGTEXT DEFAULT NULL,
        progress_json LONGTEXT DEFAULT NULL,
        recommendations_json LONGTEXT DEFAULT NULL,
        summary_json LONGTEXT DEFAULT NULL,
        failure_message VARCHAR(500) DEFAULT NULL,
        applied_snapshot_id BIGINT UNSIGNED DEFAULT NULL,
        rollback_snapshot_id BIGINT UNSIGNED DEFAULT NULL,
        started_at DATETIME NOT NULL,
        finished_at DATETIME DEFAULT NULL,
        applied_at DATETIME DEFAULT NULL,
        cancelled_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_scheduler_profiling_runs_status (run_status, current_phase),
        KEY idx_scheduler_profiling_runs_started (started_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');

    db_execute('CREATE TABLE IF NOT EXISTS scheduler_profiling_samples (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        profiling_run_id BIGINT UNSIGNED NOT NULL,
        schedule_id INT UNSIGNED DEFAULT NULL,
        job_key VARCHAR(190) NOT NULL,
        phase VARCHAR(30) NOT NULL,
        sample_key VARCHAR(190) NOT NULL,
        partner_job_key VARCHAR(190) DEFAULT NULL,
        sample_index INT UNSIGNED NOT NULL DEFAULT 1,
        run_status VARCHAR(20) NOT NULL,
        wall_duration_seconds DECIMAL(10,2) DEFAULT NULL,
        cpu_percent DECIMAL(8,2) DEFAULT NULL,
        memory_peak_bytes BIGINT UNSIGNED DEFAULT NULL,
        lock_wait_seconds DECIMAL(10,2) DEFAULT NULL,
        queue_wait_seconds DECIMAL(10,2) DEFAULT NULL,
        overlap_count INT UNSIGNED NOT NULL DEFAULT 0,
        timed_out TINYINT(1) NOT NULL DEFAULT 0,
        failed TINYINT(1) NOT NULL DEFAULT 0,
        workload_json LONGTEXT DEFAULT NULL,
        result_json LONGTEXT DEFAULT NULL,
        started_at DATETIME DEFAULT NULL,
        finished_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_profiling_samples_run_phase (profiling_run_id, phase, created_at),
        KEY idx_scheduler_profiling_samples_job (job_key, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');

    db_execute('CREATE TABLE IF NOT EXISTS scheduler_profiling_pairings (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        profiling_run_id BIGINT UNSIGNED NOT NULL,
        primary_job_key VARCHAR(190) NOT NULL,
        secondary_job_key VARCHAR(190) NOT NULL,
        probe_status VARCHAR(20) NOT NULL,
        compatibility VARCHAR(20) NOT NULL DEFAULT "pending",
        recommended_parallelism INT UNSIGNED NOT NULL DEFAULT 1,
        reason_text VARCHAR(500) DEFAULT NULL,
        metrics_json LONGTEXT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_scheduler_profiling_pair (profiling_run_id, primary_job_key, secondary_job_key),
        KEY idx_scheduler_profiling_pairings_run (profiling_run_id, compatibility)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');

    db_execute('CREATE TABLE IF NOT EXISTS scheduler_schedule_snapshots (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        profiling_run_id BIGINT UNSIGNED DEFAULT NULL,
        snapshot_label VARCHAR(80) NOT NULL,
        actor VARCHAR(190) NOT NULL,
        reason_text VARCHAR(500) DEFAULT NULL,
        schedule_json LONGTEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_schedule_snapshots_run (profiling_run_id, created_at),
        KEY idx_scheduler_schedule_snapshots_label (snapshot_label, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');

    $ensured = true;
}

function db_scheduler_profiling_active_run(): ?array
{
    db_scheduler_profiling_tables_ensure();

    return db_select_one(
        'SELECT id, run_status, current_phase, execution_mode, started_by, scope_json, options_json, selected_job_keys_json, progress_json, recommendations_json, summary_json, failure_message, applied_snapshot_id, rollback_snapshot_id, started_at, finished_at, applied_at, cancelled_at, created_at, updated_at
         FROM scheduler_profiling_runs
         WHERE run_status IN ("requested", "waiting", "isolated", "compatibility", "awaiting_review")
         ORDER BY id DESC
         LIMIT 1'
    );
}

function db_scheduler_profiling_latest_runs(int $limit = 10): array
{
    db_scheduler_profiling_tables_ensure();
    $safeLimit = max(1, min(50, $limit));

    return db_select(
        'SELECT id, run_status, current_phase, execution_mode, started_by, scope_json, options_json, selected_job_keys_json, progress_json, recommendations_json, summary_json, failure_message, applied_snapshot_id, rollback_snapshot_id, started_at, finished_at, applied_at, cancelled_at, created_at, updated_at
         FROM scheduler_profiling_runs
         ORDER BY id DESC
         LIMIT ' . $safeLimit
    );
}

function db_scheduler_profiling_run_fetch(int $runId): ?array
{
    db_scheduler_profiling_tables_ensure();
    return db_select_one(
        'SELECT id, run_status, current_phase, execution_mode, started_by, scope_json, options_json, selected_job_keys_json, progress_json, recommendations_json, summary_json, failure_message, applied_snapshot_id, rollback_snapshot_id, started_at, finished_at, applied_at, cancelled_at, created_at, updated_at
         FROM scheduler_profiling_runs
         WHERE id = ?
         LIMIT 1',
        [$runId]
    );
}

function db_scheduler_profiling_run_insert(array $row): int
{
    db_scheduler_profiling_tables_ensure();
    db_execute(
        'INSERT INTO scheduler_profiling_runs (run_status, current_phase, execution_mode, started_by, scope_json, options_json, selected_job_keys_json, progress_json, recommendations_json, summary_json, failure_message, applied_snapshot_id, rollback_snapshot_id, started_at, finished_at, applied_at, cancelled_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            mb_substr(trim((string) ($row['run_status'] ?? 'requested')), 0, 30),
            mb_substr(trim((string) ($row['current_phase'] ?? 'requested')), 0, 40),
            mb_substr(trim((string) ($row['execution_mode'] ?? 'isolated_only')), 0, 30),
            mb_substr(trim((string) ($row['started_by'] ?? 'scheduler-admin')), 0, 190),
            !empty($row['scope_json']) ? (string) $row['scope_json'] : null,
            !empty($row['options_json']) ? (string) $row['options_json'] : null,
            !empty($row['selected_job_keys_json']) ? (string) $row['selected_job_keys_json'] : null,
            !empty($row['progress_json']) ? (string) $row['progress_json'] : null,
            !empty($row['recommendations_json']) ? (string) $row['recommendations_json'] : null,
            !empty($row['summary_json']) ? (string) $row['summary_json'] : null,
            isset($row['failure_message']) ? mb_substr(trim((string) $row['failure_message']), 0, 500) : null,
            isset($row['applied_snapshot_id']) ? max(0, (int) $row['applied_snapshot_id']) ?: null : null,
            isset($row['rollback_snapshot_id']) ? max(0, (int) $row['rollback_snapshot_id']) ?: null : null,
            $row['started_at'] ?? gmdate('Y-m-d H:i:s'),
            $row['finished_at'] ?? null,
            $row['applied_at'] ?? null,
            $row['cancelled_at'] ?? null,
        ]
    );

    return (int) db()->lastInsertId();
}

function db_scheduler_profiling_run_update(int $runId, array $changes): bool
{
    db_scheduler_profiling_tables_ensure();
    if ($runId <= 0 || $changes === []) {
        return false;
    }

    $sets = [];
    $params = [];
    $map = [
        'run_status' => ['run_status', 30],
        'current_phase' => ['current_phase', 40],
        'execution_mode' => ['execution_mode', 30],
        'started_by' => ['started_by', 190],
        'failure_message' => ['failure_message', 500],
    ];
    foreach ($map as $key => [$column, $length]) {
        if (array_key_exists($key, $changes)) {
            $sets[] = $column . ' = ?';
            $params[] = $changes[$key] === null ? null : mb_substr(trim((string) $changes[$key]), 0, $length);
        }
    }
    foreach (['scope_json','options_json','selected_job_keys_json','progress_json','recommendations_json','summary_json','started_at','finished_at','applied_at','cancelled_at'] as $column) {
        if (array_key_exists($column, $changes)) {
            $sets[] = $column . ' = ?';
            $params[] = $changes[$column];
        }
    }
    foreach (['applied_snapshot_id','rollback_snapshot_id'] as $column) {
        if (array_key_exists($column, $changes)) {
            $sets[] = $column . ' = ?';
            $params[] = $changes[$column] !== null ? max(0, (int) $changes[$column]) ?: null : null;
        }
    }
    if ($sets === []) {
        return false;
    }
    $sets[] = 'updated_at = CURRENT_TIMESTAMP';
    $params[] = $runId;

    return db_execute('UPDATE scheduler_profiling_runs SET ' . implode(', ', $sets) . ' WHERE id = ? LIMIT 1', $params);
}

function db_scheduler_profiling_sample_insert(array $row): bool
{
    db_scheduler_profiling_tables_ensure();
    return db_execute(
        'INSERT INTO scheduler_profiling_samples (profiling_run_id, schedule_id, job_key, phase, sample_key, partner_job_key, sample_index, run_status, wall_duration_seconds, cpu_percent, memory_peak_bytes, lock_wait_seconds, queue_wait_seconds, overlap_count, timed_out, failed, workload_json, result_json, started_at, finished_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            max(1, (int) ($row['profiling_run_id'] ?? 0)),
            isset($row['schedule_id']) ? max(0, (int) $row['schedule_id']) ?: null : null,
            mb_substr(trim((string) ($row['job_key'] ?? '')), 0, 190),
            mb_substr(trim((string) ($row['phase'] ?? 'isolated')), 0, 30),
            mb_substr(trim((string) ($row['sample_key'] ?? 'sample')), 0, 190),
            isset($row['partner_job_key']) ? mb_substr(trim((string) $row['partner_job_key']), 0, 190) : null,
            max(1, (int) ($row['sample_index'] ?? 1)),
            mb_substr(trim((string) ($row['run_status'] ?? 'unknown')), 0, 20),
            isset($row['wall_duration_seconds']) ? round((float) $row['wall_duration_seconds'], 2) : null,
            isset($row['cpu_percent']) ? round((float) $row['cpu_percent'], 2) : null,
            isset($row['memory_peak_bytes']) ? max(0, (int) $row['memory_peak_bytes']) : null,
            isset($row['lock_wait_seconds']) ? round((float) $row['lock_wait_seconds'], 2) : null,
            isset($row['queue_wait_seconds']) ? round((float) $row['queue_wait_seconds'], 2) : null,
            max(0, (int) ($row['overlap_count'] ?? 0)),
            !empty($row['timed_out']) ? 1 : 0,
            !empty($row['failed']) ? 1 : 0,
            !empty($row['workload_json']) ? (string) $row['workload_json'] : null,
            !empty($row['result_json']) ? (string) $row['result_json'] : null,
            $row['started_at'] ?? null,
            $row['finished_at'] ?? null,
        ]
    );
}

function db_scheduler_profiling_samples_fetch(int $runId, ?string $phase = null): array
{
    db_scheduler_profiling_tables_ensure();
    $params = [max(1, $runId)];
    $sql = 'SELECT id, profiling_run_id, schedule_id, job_key, phase, sample_key, partner_job_key, sample_index, run_status, wall_duration_seconds, cpu_percent, memory_peak_bytes, lock_wait_seconds, queue_wait_seconds, overlap_count, timed_out, failed, workload_json, result_json, started_at, finished_at, created_at
            FROM scheduler_profiling_samples
            WHERE profiling_run_id = ?';
    if ($phase !== null && $phase !== '') {
        $sql .= ' AND phase = ?';
        $params[] = mb_substr(trim($phase), 0, 30);
    }
    $sql .= ' ORDER BY created_at ASC, id ASC';
    return db_select($sql, $params);
}

function db_scheduler_profiling_pairing_upsert(array $row): bool
{
    db_scheduler_profiling_tables_ensure();
    $primary = mb_substr(trim((string) ($row['primary_job_key'] ?? '')), 0, 190);
    $secondary = mb_substr(trim((string) ($row['secondary_job_key'] ?? '')), 0, 190);
    if ($primary === '' || $secondary === '') {
        return false;
    }
    if (strcmp($primary, $secondary) > 0) {
        $tmp = $primary;
        $primary = $secondary;
        $secondary = $tmp;
    }

    return db_execute(
        'INSERT INTO scheduler_profiling_pairings (profiling_run_id, primary_job_key, secondary_job_key, probe_status, compatibility, recommended_parallelism, reason_text, metrics_json)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            probe_status = VALUES(probe_status),
            compatibility = VALUES(compatibility),
            recommended_parallelism = VALUES(recommended_parallelism),
            reason_text = VALUES(reason_text),
            metrics_json = VALUES(metrics_json)',
        [
            max(1, (int) ($row['profiling_run_id'] ?? 0)),
            $primary,
            $secondary,
            mb_substr(trim((string) ($row['probe_status'] ?? 'planned')), 0, 20),
            mb_substr(trim((string) ($row['compatibility'] ?? 'pending')), 0, 20),
            max(1, min(6, (int) ($row['recommended_parallelism'] ?? 1))),
            isset($row['reason_text']) ? mb_substr(trim((string) $row['reason_text']), 0, 500) : null,
            !empty($row['metrics_json']) ? (string) $row['metrics_json'] : null,
        ]
    );
}

function db_scheduler_profiling_pairings_fetch(int $runId): array
{
    db_scheduler_profiling_tables_ensure();
    return db_select(
        'SELECT id, profiling_run_id, primary_job_key, secondary_job_key, probe_status, compatibility, recommended_parallelism, reason_text, metrics_json, created_at
         FROM scheduler_profiling_pairings
         WHERE profiling_run_id = ?
         ORDER BY primary_job_key ASC, secondary_job_key ASC',
        [max(1, $runId)]
    );
}

function db_scheduler_schedule_snapshot_insert(?int $profilingRunId, string $label, string $actor, string $reason, array $rows): int
{
    db_scheduler_profiling_tables_ensure();
    db_execute(
        'INSERT INTO scheduler_schedule_snapshots (profiling_run_id, snapshot_label, actor, reason_text, schedule_json)
         VALUES (?, ?, ?, ?, ?)',
        [
            $profilingRunId !== null && $profilingRunId > 0 ? $profilingRunId : null,
            mb_substr(trim($label), 0, 80),
            mb_substr(trim($actor), 0, 190),
            mb_substr(trim($reason), 0, 500),
            json_encode(array_values($rows), JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
        ]
    );

    return (int) db()->lastInsertId();
}

function db_scheduler_schedule_snapshot_fetch(int $snapshotId): ?array
{
    db_scheduler_profiling_tables_ensure();
    return db_select_one(
        'SELECT id, profiling_run_id, snapshot_label, actor, reason_text, schedule_json, created_at
         FROM scheduler_schedule_snapshots
         WHERE id = ?
         LIMIT 1',
        [$snapshotId]
    );
}

function db_scheduler_schedule_snapshots_recent(int $limit = 12): array
{
    db_scheduler_profiling_tables_ensure();
    $safeLimit = max(1, min(50, $limit));
    return db_select(
        'SELECT id, profiling_run_id, snapshot_label, actor, reason_text, schedule_json, created_at
         FROM scheduler_schedule_snapshots
         ORDER BY id DESC
         LIMIT ' . $safeLimit
    );
}

function db_sync_schedule_reset_locks(string $errorMessage): int
{
    db_sync_schedule_registry_columns_ensure();

    $message = mb_substr(trim($errorMessage), 0, 500);
    if ($message === '') {
        $message = 'Scheduler locks were reset manually.';
    }

    $stmt = db()->prepare(
        'UPDATE sync_schedules
         SET locked_until = NULL,
             last_status = CASE
                WHEN current_state = \'running\' OR locked_until IS NOT NULL THEN \'failed\'
                ELSE last_status
             END,
             last_result = CASE
                WHEN current_state = \'running\' OR locked_until IS NOT NULL THEN \'reset\'
                ELSE last_result
             END,
             last_error = CASE
                WHEN current_state = \'running\' OR locked_until IS NOT NULL THEN ?
                ELSE last_error
             END,
             next_run_at = CASE
                WHEN enabled = 1 AND (current_state = \'running\' OR locked_until IS NOT NULL) THEN UTC_TIMESTAMP()
                WHEN enabled = 0 THEN NULL
                ELSE next_run_at
             END,
             next_due_at = CASE
                WHEN enabled = 1 AND (current_state = \'running\' OR locked_until IS NOT NULL) THEN UTC_TIMESTAMP()
                WHEN enabled = 0 THEN NULL
                ELSE next_due_at
             END,
             current_state = CASE WHEN enabled = 1 THEN \'waiting\' ELSE \'stopped\' END,
             updated_at = CURRENT_TIMESTAMP
         WHERE current_state = \'running\'
            OR locked_until IS NOT NULL'
    );

    $stmt->execute([$message]);

    return (int) $stmt->rowCount();
}

function db_sync_schedule_recover_stale_running_jobs(string $errorMessage, int $staleGraceSeconds = 300): int
{
    db_sync_schedule_registry_columns_ensure();

    $message = mb_substr(trim($errorMessage), 0, 500);
    if ($message === '') {
        $message = 'Recovered stale scheduler job state after daemon restart.';
    }

    $safeGrace = max(60, min(7200, $staleGraceSeconds));
    $stmt = db()->prepare(
        'UPDATE sync_schedules
         SET locked_until = NULL,
             last_status = \'failed\',
             last_result = \'recovered\',
             last_error = ?,
             next_run_at = CASE WHEN enabled = 1 THEN UTC_TIMESTAMP() ELSE NULL END,
             next_due_at = CASE WHEN enabled = 1 THEN UTC_TIMESTAMP() ELSE NULL END,
             latest_allowed_start_at = CASE
                WHEN enabled = 1 THEN DATE_ADD(UTC_TIMESTAMP(), INTERVAL GREATEST(1, interval_minutes) MINUTE)
                ELSE NULL
             END,
             current_state = CASE WHEN enabled = 1 THEN \'waiting\' ELSE \'stopped\' END,
             updated_at = CURRENT_TIMESTAMP
         WHERE enabled = 1
           AND (
                (locked_until IS NOT NULL AND locked_until <= UTC_TIMESTAMP())
                OR (
                    current_state = \'running\'
                    AND last_started_at IS NOT NULL
                    AND last_started_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL GREATEST(timeout_seconds + ?, 120) SECOND)
                )
           )'
    );
    $stmt->execute([$message, $safeGrace]);

    return (int) $stmt->rowCount();
}

function db_sync_schedule_next_due_snapshot(): ?array
{
    db_sync_schedule_registry_columns_ensure();

    $row = db_select_one(
        'SELECT id, job_key, next_due_at
         FROM sync_schedules
         WHERE enabled = 1
           AND next_due_at IS NOT NULL
           AND current_state <> ?
         ORDER BY next_due_at ASC, id ASC
         LIMIT 1',
        ['stopped']
    );

    return $row !== [] ? $row : null;
}

function db_scheduler_job_current_status_ensure(): void
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_job_current_status (
        job_key VARCHAR(190) PRIMARY KEY,
        dataset_key VARCHAR(190) DEFAULT NULL,
        latest_status VARCHAR(40) NOT NULL DEFAULT \'unknown\',
        latest_event_type VARCHAR(50) DEFAULT NULL,
        last_started_at DATETIME DEFAULT NULL,
        last_finished_at DATETIME DEFAULT NULL,
        last_success_at DATETIME DEFAULT NULL,
        last_failure_at DATETIME DEFAULT NULL,
        last_failure_message VARCHAR(500) DEFAULT NULL,
        current_pressure_state VARCHAR(32) NOT NULL DEFAULT \'healthy\',
        last_pressure_state VARCHAR(32) DEFAULT NULL,
        recent_timeout_count INT UNSIGNED NOT NULL DEFAULT 0,
        recent_lock_conflict_count INT UNSIGNED NOT NULL DEFAULT 0,
        recent_deferral_count INT UNSIGNED NOT NULL DEFAULT 0,
        recent_skip_count INT UNSIGNED NOT NULL DEFAULT 0,
        change_aware TINYINT(1) NOT NULL DEFAULT 0,
        dependencies_json LONGTEXT DEFAULT NULL,
        last_change_detection_json LONGTEXT DEFAULT NULL,
        last_execution_context_json LONGTEXT DEFAULT NULL,
        last_no_change_skip_at DATETIME DEFAULT NULL,
        last_no_change_reason VARCHAR(500) DEFAULT NULL,
        last_resource_metrics_summary_json LONGTEXT DEFAULT NULL,
        last_planner_decision_type VARCHAR(40) DEFAULT NULL,
        last_planner_reason_text VARCHAR(500) DEFAULT NULL,
        last_planner_decided_at DATETIME DEFAULT NULL,
        last_event_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_scheduler_job_current_status_dataset (dataset_key),
        KEY idx_scheduler_job_current_status_status (latest_status, current_pressure_state),
        KEY idx_scheduler_job_current_status_event (last_event_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    db_ensure_table_column('scheduler_job_current_status', 'change_aware', 'TINYINT(1) NOT NULL DEFAULT 0 AFTER recent_skip_count');
    db_ensure_table_column('scheduler_job_current_status', 'dependencies_json', 'LONGTEXT DEFAULT NULL AFTER change_aware');
    db_ensure_table_column('scheduler_job_current_status', 'last_change_detection_json', 'LONGTEXT DEFAULT NULL AFTER dependencies_json');
    db_ensure_table_column('scheduler_job_current_status', 'last_execution_context_json', 'LONGTEXT DEFAULT NULL AFTER last_change_detection_json');
    db_ensure_table_column('scheduler_job_current_status', 'last_no_change_skip_at', 'DATETIME DEFAULT NULL AFTER last_execution_context_json');
    db_ensure_table_column('scheduler_job_current_status', 'last_no_change_reason', 'VARCHAR(500) DEFAULT NULL AFTER last_no_change_skip_at');
}

function db_scheduler_job_current_status_upsert(string $jobKey, array $status): bool
{
    db_scheduler_job_current_status_ensure();

    $normalizedJobKey = mb_substr(trim($jobKey), 0, 190);
    if ($normalizedJobKey === '') {
        return false;
    }
    $current = db_select(
        'SELECT change_aware
         FROM scheduler_job_current_status
         WHERE job_key = ?
         LIMIT 1',
        [$normalizedJobKey]
    )[0] ?? [];

    $datasetKey = array_key_exists('dataset_key', $status)
        ? (($status['dataset_key'] !== null && trim((string) $status['dataset_key']) !== '') ? mb_substr(trim((string) $status['dataset_key']), 0, 190) : null)
        : ('scheduler.job.' . $normalizedJobKey);
    $latestStatus = mb_substr(trim((string) ($status['latest_status'] ?? 'unknown')), 0, 40);
    $latestEventType = array_key_exists('latest_event_type', $status)
        ? (($status['latest_event_type'] !== null && trim((string) $status['latest_event_type']) !== '') ? mb_substr(trim((string) $status['latest_event_type']), 0, 50) : null)
        : null;
    $currentPressureState = mb_substr(trim((string) ($status['current_pressure_state'] ?? 'healthy')), 0, 32);
    $lastPressureState = array_key_exists('last_pressure_state', $status)
        ? (($status['last_pressure_state'] !== null && trim((string) $status['last_pressure_state']) !== '') ? mb_substr(trim((string) $status['last_pressure_state']), 0, 32) : null)
        : null;
    $lastFailureMessage = array_key_exists('last_failure_message', $status)
        ? (($status['last_failure_message'] !== null && trim((string) $status['last_failure_message']) !== '') ? mb_substr(trim((string) $status['last_failure_message']), 0, 500) : null)
        : null;
    $lastPlannerDecisionType = array_key_exists('last_planner_decision_type', $status)
        ? (($status['last_planner_decision_type'] !== null && trim((string) $status['last_planner_decision_type']) !== '') ? mb_substr(trim((string) $status['last_planner_decision_type']), 0, 40) : null)
        : null;
    $lastPlannerReasonText = array_key_exists('last_planner_reason_text', $status)
        ? (($status['last_planner_reason_text'] !== null && trim((string) $status['last_planner_reason_text']) !== '') ? mb_substr(trim((string) $status['last_planner_reason_text']), 0, 500) : null)
        : null;
    $resourceSummaryJson = null;
    if (array_key_exists('last_resource_metrics_summary_json', $status) && $status['last_resource_metrics_summary_json'] !== null) {
        $resourceSummaryJson = is_string($status['last_resource_metrics_summary_json'])
            ? $status['last_resource_metrics_summary_json']
            : json_encode($status['last_resource_metrics_summary_json'], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    }
    $dependenciesJson = null;
    if (array_key_exists('dependencies_json', $status) && $status['dependencies_json'] !== null) {
        $dependenciesJson = is_string($status['dependencies_json'])
            ? $status['dependencies_json']
            : json_encode($status['dependencies_json'], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    }
    $changeDetectionJson = null;
    if (array_key_exists('last_change_detection_json', $status) && $status['last_change_detection_json'] !== null) {
        $changeDetectionJson = is_string($status['last_change_detection_json'])
            ? $status['last_change_detection_json']
            : json_encode($status['last_change_detection_json'], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    }
    $executionContextJson = null;
    if (array_key_exists('last_execution_context_json', $status) && $status['last_execution_context_json'] !== null) {
        $executionContextJson = is_string($status['last_execution_context_json'])
            ? $status['last_execution_context_json']
            : json_encode($status['last_execution_context_json'], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    }
    $changeAware = array_key_exists('change_aware', $status) && $status['change_aware'] !== null
        ? (!empty($status['change_aware']) ? 1 : 0)
        : (!empty($current['change_aware']) ? 1 : 0);

    return db_execute(
        'INSERT INTO scheduler_job_current_status (
            job_key,
            dataset_key,
            latest_status,
            latest_event_type,
            last_started_at,
            last_finished_at,
            last_success_at,
            last_failure_at,
            last_failure_message,
            current_pressure_state,
            last_pressure_state,
            recent_timeout_count,
            recent_lock_conflict_count,
            recent_deferral_count,
            recent_skip_count,
            change_aware,
            dependencies_json,
            last_change_detection_json,
            last_execution_context_json,
            last_no_change_skip_at,
            last_no_change_reason,
            last_resource_metrics_summary_json,
            last_planner_decision_type,
            last_planner_reason_text,
            last_planner_decided_at,
            last_event_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            dataset_key = COALESCE(VALUES(dataset_key), dataset_key),
            latest_status = VALUES(latest_status),
            latest_event_type = COALESCE(VALUES(latest_event_type), latest_event_type),
            last_started_at = COALESCE(VALUES(last_started_at), last_started_at),
            last_finished_at = COALESCE(VALUES(last_finished_at), last_finished_at),
            last_success_at = COALESCE(VALUES(last_success_at), last_success_at),
            last_failure_at = COALESCE(VALUES(last_failure_at), last_failure_at),
            last_failure_message = CASE
                WHEN VALUES(last_failure_message) IS NULL AND VALUES(last_failure_at) IS NULL THEN last_failure_message
                ELSE VALUES(last_failure_message)
            END,
            current_pressure_state = COALESCE(VALUES(current_pressure_state), current_pressure_state),
            last_pressure_state = COALESCE(VALUES(last_pressure_state), last_pressure_state),
            recent_timeout_count = VALUES(recent_timeout_count),
            recent_lock_conflict_count = VALUES(recent_lock_conflict_count),
            recent_deferral_count = VALUES(recent_deferral_count),
            recent_skip_count = VALUES(recent_skip_count),
            change_aware = VALUES(change_aware),
            dependencies_json = COALESCE(VALUES(dependencies_json), dependencies_json),
            last_change_detection_json = COALESCE(VALUES(last_change_detection_json), last_change_detection_json),
            last_execution_context_json = COALESCE(VALUES(last_execution_context_json), last_execution_context_json),
            last_no_change_skip_at = COALESCE(VALUES(last_no_change_skip_at), last_no_change_skip_at),
            last_no_change_reason = CASE
                WHEN VALUES(last_no_change_reason) IS NULL AND VALUES(last_no_change_skip_at) IS NULL THEN last_no_change_reason
                ELSE VALUES(last_no_change_reason)
            END,
            last_resource_metrics_summary_json = COALESCE(VALUES(last_resource_metrics_summary_json), last_resource_metrics_summary_json),
            last_planner_decision_type = COALESCE(VALUES(last_planner_decision_type), last_planner_decision_type),
            last_planner_reason_text = COALESCE(VALUES(last_planner_reason_text), last_planner_reason_text),
            last_planner_decided_at = COALESCE(VALUES(last_planner_decided_at), last_planner_decided_at),
            last_event_at = COALESCE(VALUES(last_event_at), last_event_at),
            updated_at = CURRENT_TIMESTAMP',
        [
            $normalizedJobKey,
            $datasetKey,
            $latestStatus,
            $latestEventType,
            $status['last_started_at'] ?? null,
            $status['last_finished_at'] ?? null,
            $status['last_success_at'] ?? null,
            $status['last_failure_at'] ?? null,
            $lastFailureMessage,
            $currentPressureState,
            $lastPressureState,
            max(0, (int) ($status['recent_timeout_count'] ?? 0)),
            max(0, (int) ($status['recent_lock_conflict_count'] ?? 0)),
            max(0, (int) ($status['recent_deferral_count'] ?? 0)),
            max(0, (int) ($status['recent_skip_count'] ?? 0)),
            $changeAware,
            $dependenciesJson,
            $changeDetectionJson,
            $executionContextJson,
            $status['last_no_change_skip_at'] ?? null,
            array_key_exists('last_no_change_reason', $status) && $status['last_no_change_reason'] !== null
                ? mb_substr(trim((string) $status['last_no_change_reason']), 0, 500)
                : null,
            $resourceSummaryJson,
            $lastPlannerDecisionType,
            $lastPlannerReasonText,
            $status['last_planner_decided_at'] ?? null,
            $status['last_event_at'] ?? null,
        ]
    );
}

function db_scheduler_job_current_status_fetch_map(array $jobKeys = []): array
{
    db_scheduler_job_current_status_ensure();

    $params = [];
    $sql = 'SELECT job_key, dataset_key, latest_status, latest_event_type, last_started_at, last_finished_at, last_success_at, last_failure_at, last_failure_message, current_pressure_state, last_pressure_state, recent_timeout_count, recent_lock_conflict_count, recent_deferral_count, recent_skip_count, change_aware, dependencies_json, last_change_detection_json, last_execution_context_json, last_no_change_skip_at, last_no_change_reason, last_resource_metrics_summary_json, last_planner_decision_type, last_planner_reason_text, last_planner_decided_at, last_event_at, created_at, updated_at
            FROM scheduler_job_current_status';

    $normalizedKeys = array_values(array_filter(array_map(static fn (mixed $jobKey): string => trim((string) $jobKey), $jobKeys), static fn (string $jobKey): bool => $jobKey !== ''));
    if ($normalizedKeys !== []) {
        $sql .= ' WHERE job_key IN (' . implode(',', array_fill(0, count($normalizedKeys), '?')) . ')';
        $params = $normalizedKeys;
    }

    $rows = db_select($sql, $params);
    $map = [];
    foreach ($rows as $row) {
        $currentJobKey = trim((string) ($row['job_key'] ?? ''));
        if ($currentJobKey === '') {
            continue;
        }

        $row['last_resource_metrics_summary'] = null;
        $summaryJson = $row['last_resource_metrics_summary_json'] ?? null;
        if (is_string($summaryJson) && trim($summaryJson) !== '') {
            $decoded = json_decode($summaryJson, true);
            if (is_array($decoded)) {
                $row['last_resource_metrics_summary'] = $decoded;
            }
        }
        foreach ([
            'dependencies_json' => 'dependencies',
            'last_change_detection_json' => 'last_change_detection',
            'last_execution_context_json' => 'last_execution_context',
        ] as $jsonColumn => $decodedColumn) {
            $row[$decodedColumn] = null;
            $jsonValue = $row[$jsonColumn] ?? null;
            if (is_string($jsonValue) && trim($jsonValue) !== '') {
                $decoded = json_decode($jsonValue, true);
                if (is_array($decoded)) {
                    $row[$decodedColumn] = $decoded;
                }
            }
        }

        $map[$currentJobKey] = $row;
    }

    return $map;
}

function db_scheduler_job_event_insert(string $jobKey, string $eventType, array $detail = [], int $latenessSeconds = 0, ?float $durationSeconds = null): bool
{
    db_scheduler_job_current_status_ensure();
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_job_events (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        job_key VARCHAR(190) NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        detail_json LONGTEXT DEFAULT NULL,
        lateness_seconds INT NOT NULL DEFAULT 0,
        duration_seconds DECIMAL(10,2) DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_job_events_job_created (job_key, created_at),
        KEY idx_scheduler_job_events_type_created (event_type, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    db_ensure_table_index('scheduler_job_events', 'idx_scheduler_job_events_created', 'INDEX idx_scheduler_job_events_created (created_at)');

    $inserted = db_execute(
        'INSERT INTO scheduler_job_events (job_key, event_type, detail_json, lateness_seconds, duration_seconds)
         VALUES (?, ?, ?, ?, ?)',
        [mb_substr(trim($jobKey), 0, 190), mb_substr(trim($eventType), 0, 50), $detail !== [] ? json_encode($detail, JSON_UNESCAPED_SLASHES) : null, $latenessSeconds, $durationSeconds]
    );

    if (!$inserted) {
        return false;
    }

    $jobKey = mb_substr(trim($jobKey), 0, 190);
    $eventType = mb_substr(trim($eventType), 0, 50);
    $current = db_scheduler_job_current_status_fetch_map([$jobKey])[$jobKey] ?? [];
    $now = gmdate('Y-m-d H:i:s');
    $latestStatus = match ($eventType) {
        'started' => 'running',
        'finished' => (string) (($detail['status'] ?? 'success') === 'success' ? 'success' : ($detail['status'] ?? 'finished')),
        'timeout', 'failure' => 'failed',
        'skipped', 'skipped_no_change', 'skipped_within_freshness_window', 'deferred_capacity', 'deferred_memory', 'deferred_cpu' => $eventType,
        'lock_conflict', 'lock_skipped' => 'blocked',
        'deferred_pressure', 'idle_backfill_deferred_capacity', 'idle_backfill_deferred_memory', 'idle_backfill_deferred_cpu' => $eventType,
        default => (string) ($current['latest_status'] ?? 'unknown'),
    };

    db_scheduler_job_current_status_upsert($jobKey, [
        'latest_status' => $latestStatus,
        'latest_event_type' => $eventType,
        'last_started_at' => $eventType === 'started' ? $now : ($current['last_started_at'] ?? null),
        'last_finished_at' => in_array($eventType, ['finished', 'skipped', 'skipped_no_change', 'skipped_within_freshness_window', 'timeout', 'failure'], true) ? $now : ($current['last_finished_at'] ?? null),
        'last_failure_at' => in_array($eventType, ['timeout', 'failure'], true) ? $now : ($current['last_failure_at'] ?? null),
        'last_failure_message' => in_array($eventType, ['timeout', 'failure'], true) ? ($detail['error'] ?? $current['last_failure_message'] ?? null) : ($current['last_failure_message'] ?? null),
        'current_pressure_state' => $detail['pressure_state'] ?? ($current['current_pressure_state'] ?? 'healthy'),
        'last_pressure_state' => $detail['pressure_state'] ?? ($current['last_pressure_state'] ?? null),
        'recent_timeout_count' => max(0, (int) ($current['recent_timeout_count'] ?? 0)) + ($eventType === 'timeout' ? 1 : 0),
        'recent_lock_conflict_count' => max(0, (int) ($current['recent_lock_conflict_count'] ?? 0)) + (in_array($eventType, ['lock_conflict', 'lock_skipped'], true) ? 1 : 0),
        'recent_deferral_count' => max(0, (int) ($current['recent_deferral_count'] ?? 0)) + (str_contains($eventType, 'deferred') ? 1 : 0),
        'recent_skip_count' => max(0, (int) ($current['recent_skip_count'] ?? 0)) + (in_array($eventType, ['skipped', 'skipped_no_change', 'skipped_within_freshness_window'], true) ? 1 : 0),
        'change_aware' => !empty($detail['change_aware']) || !empty($current['change_aware']),
        'dependencies_json' => $detail['dependencies'] ?? ($current['dependencies'] ?? null),
        'last_change_detection_json' => $detail['change_detection'] ?? ($current['last_change_detection'] ?? null),
        'last_execution_context_json' => $detail['execution_context'] ?? ($current['last_execution_context'] ?? null),
        'last_no_change_skip_at' => in_array($eventType, ['skipped_no_change', 'skipped_within_freshness_window'], true) ? $now : ($current['last_no_change_skip_at'] ?? null),
        'last_no_change_reason' => in_array($eventType, ['skipped_no_change', 'skipped_within_freshness_window'], true)
            ? ($detail['skip_reason'] ?? $current['last_no_change_reason'] ?? null)
            : ($current['last_no_change_reason'] ?? null),
        'last_event_at' => $now,
    ]);

    return true;
}

function db_scheduler_job_events_recent_summary(int $windowMinutes = 120): array
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_job_events (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        job_key VARCHAR(190) NOT NULL,
        event_type VARCHAR(50) NOT NULL,
        detail_json LONGTEXT DEFAULT NULL,
        lateness_seconds INT NOT NULL DEFAULT 0,
        duration_seconds DECIMAL(10,2) DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_job_events_job_created (job_key, created_at),
        KEY idx_scheduler_job_events_type_created (event_type, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    db_ensure_table_index('scheduler_job_events', 'idx_scheduler_job_events_created', 'INDEX idx_scheduler_job_events_created (created_at)');

    $safeWindow = max(5, min(1440, $windowMinutes));
    $rows = db_select(
        'SELECT job_key, event_type, COUNT(*) AS event_count, MAX(created_at) AS last_seen_at
         FROM scheduler_job_events
         WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ? MINUTE)
         GROUP BY job_key, event_type',
        [$safeWindow]
    );

    $summary = [];
    foreach ($rows as $row) {
        $jobKey = (string) ($row['job_key'] ?? '');
        $eventType = (string) ($row['event_type'] ?? '');
        if ($jobKey === '' || $eventType === '') {
            continue;
        }

        $summary[$jobKey][$eventType] = [
            'count' => (int) ($row['event_count'] ?? 0),
            'last_seen_at' => $row['last_seen_at'] ?? null,
        ];
    }

    return $summary;
}

function db_scheduler_tuning_action_log(string $jobKey, string $actor, string $actionType, string $reasonText, array $before = [], array $after = [], array $metrics = []): bool
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_tuning_actions (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        job_key VARCHAR(190) NOT NULL,
        actor VARCHAR(20) NOT NULL DEFAULT \'system\',
        action_type VARCHAR(50) NOT NULL,
        reason_text VARCHAR(500) NOT NULL,
        before_json LONGTEXT DEFAULT NULL,
        after_json LONGTEXT DEFAULT NULL,
        metrics_json LONGTEXT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_tuning_actions_job_created (job_key, created_at),
        KEY idx_scheduler_tuning_actions_actor_created (actor, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    db_ensure_table_index('scheduler_tuning_actions', 'idx_scheduler_tuning_actions_created', 'INDEX idx_scheduler_tuning_actions_created (created_at)');

    return db_execute(
        'INSERT INTO scheduler_tuning_actions (job_key, actor, action_type, reason_text, before_json, after_json, metrics_json)
         VALUES (?, ?, ?, ?, ?, ?, ?)',
        [
            mb_substr(trim($jobKey), 0, 190),
            mb_substr(trim($actor), 0, 20),
            mb_substr(trim($actionType), 0, 50),
            mb_substr(trim($reasonText), 0, 500),
            $before !== [] ? json_encode($before, JSON_UNESCAPED_SLASHES) : null,
            $after !== [] ? json_encode($after, JSON_UNESCAPED_SLASHES) : null,
            $metrics !== [] ? json_encode($metrics, JSON_UNESCAPED_SLASHES) : null,
        ]
    );
}

function db_scheduler_tuning_actions_recent(int $limit = 20): array
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_tuning_actions (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        job_key VARCHAR(190) NOT NULL,
        actor VARCHAR(20) NOT NULL DEFAULT \'system\',
        action_type VARCHAR(50) NOT NULL,
        reason_text VARCHAR(500) NOT NULL,
        before_json LONGTEXT DEFAULT NULL,
        after_json LONGTEXT DEFAULT NULL,
        metrics_json LONGTEXT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_tuning_actions_job_created (job_key, created_at),
        KEY idx_scheduler_tuning_actions_actor_created (actor, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    db_ensure_table_index('scheduler_tuning_actions', 'idx_scheduler_tuning_actions_created', 'INDEX idx_scheduler_tuning_actions_created (created_at)');

    $safeLimit = max(1, min(100, $limit));
    return db_select(
        'SELECT id, job_key, actor, action_type, reason_text, before_json, after_json, metrics_json, created_at
         FROM scheduler_tuning_actions
         ORDER BY created_at DESC, id DESC
         LIMIT ' . $safeLimit
    );
}


function db_scheduler_resource_metrics_ensure(): void
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_job_resource_metrics (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        schedule_id INT UNSIGNED DEFAULT NULL,
        job_key VARCHAR(190) NOT NULL,
        run_status VARCHAR(20) NOT NULL,
        wall_duration_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
        queue_wait_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
        lock_wait_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
        overlap_count INT UNSIGNED NOT NULL DEFAULT 0,
        overlapped TINYINT(1) NOT NULL DEFAULT 0,
        cpu_user_seconds DECIMAL(10,4) DEFAULT NULL,
        cpu_system_seconds DECIMAL(10,4) DEFAULT NULL,
        cpu_percent DECIMAL(8,2) DEFAULT NULL,
        memory_start_bytes BIGINT UNSIGNED DEFAULT NULL,
        memory_end_bytes BIGINT UNSIGNED DEFAULT NULL,
        memory_peak_bytes BIGINT UNSIGNED DEFAULT NULL,
        memory_peak_delta_bytes BIGINT UNSIGNED DEFAULT NULL,
        projected_cpu_percent DECIMAL(8,2) DEFAULT NULL,
        projected_memory_bytes BIGINT UNSIGNED DEFAULT NULL,
        pressure_state VARCHAR(32) DEFAULT NULL,
        timed_out TINYINT(1) NOT NULL DEFAULT 0,
        failed TINYINT(1) NOT NULL DEFAULT 0,
        started_at DATETIME DEFAULT NULL,
        finished_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_resource_metrics_job_created (job_key, created_at),
        KEY idx_scheduler_resource_metrics_schedule_created (schedule_id, created_at),
        KEY idx_scheduler_resource_metrics_job_started (job_key, started_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    db_ensure_table_index('scheduler_job_resource_metrics', 'idx_scheduler_resource_metrics_created', 'INDEX idx_scheduler_resource_metrics_created (created_at)');
}

function db_scheduler_resource_metric_insert(array $row): bool
{
    db_scheduler_resource_metrics_ensure();
    db_scheduler_job_current_status_ensure();

    $inserted = db_execute(
        'INSERT INTO scheduler_job_resource_metrics (
            schedule_id,
            job_key,
            run_status,
            wall_duration_seconds,
            queue_wait_seconds,
            lock_wait_seconds,
            overlap_count,
            overlapped,
            cpu_user_seconds,
            cpu_system_seconds,
            cpu_percent,
            memory_start_bytes,
            memory_end_bytes,
            memory_peak_bytes,
            memory_peak_delta_bytes,
            projected_cpu_percent,
            projected_memory_bytes,
            pressure_state,
            timed_out,
            failed,
            started_at,
            finished_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            isset($row['schedule_id']) ? max(0, (int) $row['schedule_id']) ?: null : null,
            mb_substr(trim((string) ($row['job_key'] ?? '')), 0, 190),
            mb_substr(trim((string) ($row['run_status'] ?? 'unknown')), 0, 20),
            round((float) ($row['wall_duration_seconds'] ?? 0.0), 2),
            round((float) ($row['queue_wait_seconds'] ?? 0.0), 2),
            round((float) ($row['lock_wait_seconds'] ?? 0.0), 2),
            max(0, (int) ($row['overlap_count'] ?? 0)),
            !empty($row['overlapped']) ? 1 : 0,
            isset($row['cpu_user_seconds']) ? round((float) $row['cpu_user_seconds'], 4) : null,
            isset($row['cpu_system_seconds']) ? round((float) $row['cpu_system_seconds'], 4) : null,
            isset($row['cpu_percent']) ? round((float) $row['cpu_percent'], 2) : null,
            isset($row['memory_start_bytes']) ? max(0, (int) $row['memory_start_bytes']) : null,
            isset($row['memory_end_bytes']) ? max(0, (int) $row['memory_end_bytes']) : null,
            isset($row['memory_peak_bytes']) ? max(0, (int) $row['memory_peak_bytes']) : null,
            isset($row['memory_peak_delta_bytes']) ? max(0, (int) $row['memory_peak_delta_bytes']) : null,
            isset($row['projected_cpu_percent']) ? round((float) $row['projected_cpu_percent'], 2) : null,
            isset($row['projected_memory_bytes']) ? max(0, (int) $row['projected_memory_bytes']) : null,
            isset($row['pressure_state']) ? mb_substr(trim((string) $row['pressure_state']), 0, 32) : null,
            !empty($row['timed_out']) ? 1 : 0,
            !empty($row['failed']) ? 1 : 0,
            $row['started_at'] ?? null,
            $row['finished_at'] ?? null,
        ]
    );

    if (!$inserted) {
        return false;
    }

    $jobKey = mb_substr(trim((string) ($row['job_key'] ?? '')), 0, 190);
    if ($jobKey === '') {
        return true;
    }

    db_scheduler_job_current_status_upsert($jobKey, [
        'latest_status' => mb_substr(trim((string) ($row['run_status'] ?? 'unknown')), 0, 40),
        'current_pressure_state' => $row['pressure_state'] ?? 'healthy',
        'last_pressure_state' => $row['pressure_state'] ?? null,
        'recent_timeout_count' => !empty($row['timed_out']) ? 1 : 0,
        'last_resource_metrics_summary_json' => [
            'run_status' => (string) ($row['run_status'] ?? 'unknown'),
            'wall_duration_seconds' => round((float) ($row['wall_duration_seconds'] ?? 0.0), 2),
            'queue_wait_seconds' => round((float) ($row['queue_wait_seconds'] ?? 0.0), 2),
            'lock_wait_seconds' => round((float) ($row['lock_wait_seconds'] ?? 0.0), 2),
            'overlap_count' => max(0, (int) ($row['overlap_count'] ?? 0)),
            'cpu_percent' => isset($row['cpu_percent']) ? round((float) $row['cpu_percent'], 2) : null,
            'memory_peak_bytes' => isset($row['memory_peak_bytes']) ? max(0, (int) $row['memory_peak_bytes']) : null,
            'memory_peak_delta_bytes' => isset($row['memory_peak_delta_bytes']) ? max(0, (int) $row['memory_peak_delta_bytes']) : null,
            'projected_cpu_percent' => isset($row['projected_cpu_percent']) ? round((float) $row['projected_cpu_percent'], 2) : null,
            'projected_memory_bytes' => isset($row['projected_memory_bytes']) ? max(0, (int) $row['projected_memory_bytes']) : null,
            'timed_out' => !empty($row['timed_out']),
            'failed' => !empty($row['failed']),
            'started_at' => $row['started_at'] ?? null,
            'finished_at' => $row['finished_at'] ?? null,
        ],
        'last_event_at' => $row['finished_at'] ?? $row['started_at'] ?? gmdate('Y-m-d H:i:s'),
    ]);

    return true;
}

function db_scheduler_resource_metrics_recent(string $jobKey, int $limit = 40): array
{
    db_scheduler_resource_metrics_ensure();

    $safeLimit = max(1, min(200, $limit));
    return db_select(
        'SELECT id, schedule_id, job_key, run_status, wall_duration_seconds, queue_wait_seconds, lock_wait_seconds, overlap_count, overlapped, cpu_user_seconds, cpu_system_seconds, cpu_percent, memory_start_bytes, memory_end_bytes, memory_peak_bytes, memory_peak_delta_bytes, projected_cpu_percent, projected_memory_bytes, pressure_state, timed_out, failed, started_at, finished_at, created_at
         FROM scheduler_job_resource_metrics
         WHERE job_key = ?
         ORDER BY created_at DESC, id DESC
         LIMIT ' . $safeLimit,
        [mb_substr(trim($jobKey), 0, 190)]
    );
}

function db_scheduler_resource_metrics_recent_all(int $limit = 20): array
{
    db_scheduler_resource_metrics_ensure();

    $safeLimit = max(1, min(100, $limit));
    return db_select(
        'SELECT id, schedule_id, job_key, run_status, wall_duration_seconds, queue_wait_seconds, lock_wait_seconds, overlap_count, overlapped, cpu_percent, memory_peak_bytes, projected_cpu_percent, projected_memory_bytes, pressure_state, timed_out, failed, started_at, finished_at, created_at
         FROM scheduler_job_resource_metrics
         ORDER BY created_at DESC, id DESC
         LIMIT ' . $safeLimit
    );
}

function db_control_plane_retention_cleanup(int $limitPerTable = 5000): array
{
    db_sync_schedule_registry_columns_ensure();
    db_scheduler_job_events_recent_summary(5);
    db_scheduler_resource_metrics_ensure();
    db_scheduler_planner_decisions_ensure();
    db_scheduler_tuning_actions_recent(1);
    db_ui_refresh_schema_ensure();

    $safeLimit = max(100, min(50000, $limitPerTable));
    $specs = [
        ['table' => 'sync_runs', 'column' => 'created_at', 'setting' => 'sync_run_retention_days', 'default' => 30, 'min' => 7, 'max' => 3650],
        ['table' => 'scheduler_job_events', 'column' => 'created_at', 'setting' => 'scheduler_event_retention_days', 'default' => 14, 'min' => 1, 'max' => 3650],
        ['table' => 'scheduler_job_resource_metrics', 'column' => 'created_at', 'setting' => 'scheduler_metric_retention_days', 'default' => 30, 'min' => 7, 'max' => 3650],
        ['table' => 'scheduler_planner_decisions', 'column' => 'created_at', 'setting' => 'scheduler_planner_retention_days', 'default' => 30, 'min' => 7, 'max' => 3650],
        ['table' => 'scheduler_tuning_actions', 'column' => 'created_at', 'setting' => 'scheduler_tuning_retention_days', 'default' => 30, 'min' => 7, 'max' => 3650],
        ['table' => 'ui_refresh_events', 'column' => 'created_at', 'setting' => 'ui_refresh_event_retention_days', 'default' => 14, 'min' => 1, 'max' => 3650],
    ];

    $deleted = [];
    $rowsWritten = 0;

    foreach ($specs as $spec) {
        $retentionDays = db_setting_int((string) $spec['setting'], (int) $spec['default'], (int) $spec['min'], (int) $spec['max']);
        $cutoff = gmdate('Y-m-d H:i:s', strtotime('-' . $retentionDays . ' days'));
        $rowsDeleted = db_prune_before_datetime((string) $spec['table'], (string) $spec['column'], $cutoff, $safeLimit);
        $deleted[(string) $spec['table']] = [
            'rows_deleted' => $rowsDeleted,
            'cutoff' => $cutoff,
            'retention_days' => $retentionDays,
        ];
        $rowsWritten += $rowsDeleted;
    }

    return [
        'rows_written' => $rowsWritten,
        'deleted' => $deleted,
    ];
}


function db_sync_schedule_update_resource_profile(int $scheduleId, array $profile): bool
{
    db_sync_schedule_registry_columns_ensure();

    if ($scheduleId <= 0) {
        return false;
    }

    return db_execute(
        'UPDATE sync_schedules
         SET resource_class = ?,
             resource_class_confidence = ?,
             telemetry_sample_count = ?,
             learning_mode = ?,
             allow_parallel = ?,
             prefers_solo = ?,
             must_run_alone = ?,
             preferred_max_parallelism = ?,
             last_cpu_percent = ?,
             average_cpu_percent = ?,
             p95_cpu_percent = ?,
             last_memory_peak_bytes = ?,
             average_memory_peak_bytes = ?,
             p95_memory_peak_bytes = ?,
             last_queue_wait_seconds = ?,
             last_lock_wait_seconds = ?,
             average_lock_wait_seconds = ?,
             recent_timeout_rate = ?,
             recent_failure_rate = ?,
             last_overlap_count = ?,
             last_overlapped = ?,
             current_projected_cpu_percent = ?,
             current_projected_memory_bytes = ?,
             last_capacity_reason = CASE WHEN ? <> \'\' THEN ? ELSE last_capacity_reason END,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        [
            mb_substr(trim((string) ($profile['resource_class'] ?? 'learning')), 0, 20),
            isset($profile['resource_class_confidence']) ? round((float) $profile['resource_class_confidence'], 4) : null,
            max(0, (int) ($profile['telemetry_sample_count'] ?? 0)),
            !empty($profile['learning_mode']) ? 1 : 0,
            !empty($profile['allow_parallel']) ? 1 : 0,
            !empty($profile['prefers_solo']) ? 1 : 0,
            !empty($profile['must_run_alone']) ? 1 : 0,
            max(1, min(6, (int) ($profile['preferred_max_parallelism'] ?? 1))),
            isset($profile['last_cpu_percent']) ? round((float) $profile['last_cpu_percent'], 2) : null,
            isset($profile['average_cpu_percent']) ? round((float) $profile['average_cpu_percent'], 2) : null,
            isset($profile['p95_cpu_percent']) ? round((float) $profile['p95_cpu_percent'], 2) : null,
            isset($profile['last_memory_peak_bytes']) ? max(0, (int) $profile['last_memory_peak_bytes']) : null,
            isset($profile['average_memory_peak_bytes']) ? max(0, (int) round((float) $profile['average_memory_peak_bytes'])) : null,
            isset($profile['p95_memory_peak_bytes']) ? max(0, (int) round((float) $profile['p95_memory_peak_bytes'])) : null,
            isset($profile['last_queue_wait_seconds']) ? round((float) $profile['last_queue_wait_seconds'], 2) : null,
            isset($profile['last_lock_wait_seconds']) ? round((float) $profile['last_lock_wait_seconds'], 2) : null,
            isset($profile['average_lock_wait_seconds']) ? round((float) $profile['average_lock_wait_seconds'], 2) : null,
            isset($profile['recent_timeout_rate']) ? round((float) $profile['recent_timeout_rate'], 4) : null,
            isset($profile['recent_failure_rate']) ? round((float) $profile['recent_failure_rate'], 4) : null,
            max(0, (int) ($profile['last_overlap_count'] ?? 0)),
            !empty($profile['last_overlapped']) ? 1 : 0,
            isset($profile['current_projected_cpu_percent']) ? round((float) $profile['current_projected_cpu_percent'], 2) : null,
            isset($profile['current_projected_memory_bytes']) ? max(0, (int) $profile['current_projected_memory_bytes']) : null,
            mb_substr(trim((string) ($profile['last_capacity_reason'] ?? '')), 0, 500),
            mb_substr(trim((string) ($profile['last_capacity_reason'] ?? '')), 0, 500),
            $scheduleId,
        ]
    );
}

function db_sync_schedule_mark_planner_state(int $scheduleId, array $state): bool
{
    db_sync_schedule_registry_columns_ensure();
    db_scheduler_job_current_status_ensure();

    if ($scheduleId <= 0) {
        return false;
    }

    $schedule = db_sync_schedule_fetch_by_id($scheduleId);
    $updated = db_execute(
        'UPDATE sync_schedules
         SET current_projected_cpu_percent = ?,
             current_projected_memory_bytes = ?,
             current_pressure_state = ?,
             latest_allowed_start_at = COALESCE(?, latest_allowed_start_at),
             must_run_alone = ?,
             prefers_solo = ?,
             allow_parallel = ?,
             last_capacity_reason = CASE WHEN ? <> \'\' THEN ? ELSE last_capacity_reason END,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        [
            isset($state['current_projected_cpu_percent']) ? round((float) $state['current_projected_cpu_percent'], 2) : null,
            isset($state['current_projected_memory_bytes']) ? max(0, (int) $state['current_projected_memory_bytes']) : null,
            mb_substr(trim((string) ($state['current_pressure_state'] ?? 'healthy')), 0, 32),
            $state['latest_allowed_start_at'] ?? null,
            !empty($state['must_run_alone']) ? 1 : 0,
            !empty($state['prefers_solo']) ? 1 : 0,
            !empty($state['allow_parallel']) ? 1 : 0,
            mb_substr(trim((string) ($state['last_capacity_reason'] ?? '')), 0, 500),
            mb_substr(trim((string) ($state['last_capacity_reason'] ?? '')), 0, 500),
            $scheduleId,
        ]
    );

    $jobKey = trim((string) ($schedule['job_key'] ?? ''));
    if ($updated && $jobKey !== '') {
        db_scheduler_job_current_status_upsert($jobKey, [
            'latest_status' => (string) ($state['latest_status'] ?? ($schedule['last_result'] ?? $schedule['last_status'] ?? 'planned')),
            'current_pressure_state' => $state['current_pressure_state'] ?? ($schedule['current_pressure_state'] ?? 'healthy'),
            'last_pressure_state' => $state['current_pressure_state'] ?? ($schedule['current_pressure_state'] ?? null),
            'last_event_at' => gmdate('Y-m-d H:i:s'),
        ]);
    }

    return $updated;
}

function db_scheduler_planner_decisions_ensure(): void
{
    db_execute('CREATE TABLE IF NOT EXISTS scheduler_planner_decisions (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        schedule_id INT UNSIGNED DEFAULT NULL,
        job_key VARCHAR(190) NOT NULL,
        decision_type VARCHAR(40) NOT NULL,
        pressure_state VARCHAR(32) NOT NULL DEFAULT "healthy",
        reason_text VARCHAR(500) NOT NULL,
        decision_json LONGTEXT DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        KEY idx_scheduler_planner_decisions_job_created (job_key, created_at),
        KEY idx_scheduler_planner_decisions_type_created (decision_type, created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4');
    db_ensure_table_index('scheduler_planner_decisions', 'idx_scheduler_planner_decisions_created', 'INDEX idx_scheduler_planner_decisions_created (created_at)');
}

function db_scheduler_planner_decision_insert(?int $scheduleId, string $jobKey, string $decisionType, string $pressureState, string $reasonText, array $decision = []): bool
{
    db_scheduler_planner_decisions_ensure();
    db_scheduler_job_current_status_ensure();

    $inserted = db_execute(
        'INSERT INTO scheduler_planner_decisions (schedule_id, job_key, decision_type, pressure_state, reason_text, decision_json)
         VALUES (?, ?, ?, ?, ?, ?)',
        [
            $scheduleId !== null && $scheduleId > 0 ? $scheduleId : null,
            mb_substr(trim($jobKey), 0, 190),
            mb_substr(trim($decisionType), 0, 40),
            mb_substr(trim($pressureState), 0, 32),
            mb_substr(trim($reasonText), 0, 500),
            $decision !== [] ? json_encode($decision, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) : null,
        ]
    );

    if (!$inserted) {
        return false;
    }

    $normalizedJobKey = mb_substr(trim($jobKey), 0, 190);
    $current = db_scheduler_job_current_status_fetch_map([$normalizedJobKey])[$normalizedJobKey] ?? [];
    $decisionType = mb_substr(trim($decisionType), 0, 40);
    $latestStatus = str_contains($decisionType, 'deferred') ? $decisionType : (($decisionType === 'allowed' || $decisionType === 'idle_backfill_allowed') ? 'planned' : ((string) ($current['latest_status'] ?? 'planned')));
    db_scheduler_job_current_status_upsert($normalizedJobKey, [
        'latest_status' => $latestStatus,
        'current_pressure_state' => mb_substr(trim($pressureState), 0, 32),
        'last_pressure_state' => mb_substr(trim($pressureState), 0, 32),
        'recent_timeout_count' => max(0, (int) ($current['recent_timeout_count'] ?? 0)),
        'recent_lock_conflict_count' => max(0, (int) ($current['recent_lock_conflict_count'] ?? 0)),
        'recent_deferral_count' => max(0, (int) ($current['recent_deferral_count'] ?? 0)) + (str_contains($decisionType, 'deferred') ? 1 : 0),
        'recent_skip_count' => max(0, (int) ($current['recent_skip_count'] ?? 0)),
        'last_planner_decision_type' => $decisionType,
        'last_planner_reason_text' => mb_substr(trim($reasonText), 0, 500),
        'last_planner_decided_at' => gmdate('Y-m-d H:i:s'),
        'last_event_at' => gmdate('Y-m-d H:i:s'),
    ]);

    return true;
}

function db_scheduler_planner_decisions_recent(int $limit = 30): array
{
    db_scheduler_planner_decisions_ensure();

    $safeLimit = max(1, min(200, $limit));
    return db_select(
        'SELECT id, schedule_id, job_key, decision_type, pressure_state, reason_text, decision_json, created_at
         FROM scheduler_planner_decisions
         ORDER BY created_at DESC, id DESC
         LIMIT ' . $safeLimit
    );
}

function db_sync_schedule_recent_run_durations(string $jobKey, int $limit = 20): array
{
    $safeLimit = max(1, min(100, $limit));
    $datasetKey = 'scheduler.job.' . trim($jobKey);

    $rows = db_select(
        'SELECT TIMESTAMPDIFF(SECOND, started_at, finished_at) AS duration_seconds,
                started_at,
                finished_at,
                run_status
         FROM sync_runs
         WHERE dataset_key = ?
           AND finished_at IS NOT NULL
         ORDER BY started_at DESC
         LIMIT ' . $safeLimit,
        [$datasetKey]
    );

    return array_values(array_map(static function (array $row): array {
        return [
            'duration_seconds' => max(0.0, (float) ($row['duration_seconds'] ?? 0)),
            'started_at' => $row['started_at'] ?? null,
            'finished_at' => $row['finished_at'] ?? null,
            'run_status' => $row['run_status'] ?? null,
        ];
    }, $rows));
}

function db_sync_schedule_recent_job_run_stats(string $jobKey, int $limit = 20): array
{
    $runs = db_sync_schedule_recent_run_durations($jobKey, $limit);
    $durations = array_values(array_filter(array_map(static fn (array $run): float => (float) ($run['duration_seconds'] ?? 0.0), $runs), static fn (float $duration): bool => $duration >= 0.0));
    if ($durations === []) {
        return [
            'count' => 0,
            'average_duration_seconds' => null,
            'p95_duration_seconds' => null,
        ];
    }

    sort($durations);
    $count = count($durations);
    $average = array_sum($durations) / $count;
    $index = (int) max(0, ceil($count * 0.95) - 1);
    $p95 = $durations[$index] ?? $durations[$count - 1];

    return [
        'count' => $count,
        'average_duration_seconds' => round($average, 2),
        'p95_duration_seconds' => round((float) $p95, 2),
    ];
}

function db_sync_schedule_busiest_offsets(int $limit = 12): array
{
    db_sync_schedule_registry_columns_ensure();

    $safeLimit = max(1, min(60, $limit));
    return db_select(
        "SELECT offset_minutes, COUNT(*) AS job_count,
                GROUP_CONCAT(job_key ORDER BY job_key SEPARATOR ', ') AS job_keys
         FROM sync_schedules
         WHERE enabled = 1
         GROUP BY offset_minutes
         ORDER BY job_count DESC, offset_minutes ASC
         LIMIT " . $safeLimit
    );
}

function db_sync_schedule_recent_outcomes_summary(int $windowMinutes = 180): array
{
    $safeWindow = max(5, min(1440, $windowMinutes));
    $rows = db_select(
        "SELECT dataset_key,
                SUM(CASE WHEN run_status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
                SUM(CASE WHEN run_status = 'success' THEN 1 ELSE 0 END) AS successful_runs,
                COUNT(*) AS total_runs
         FROM sync_runs
         WHERE dataset_key LIKE 'scheduler.job.%'
           AND started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ? MINUTE)
         GROUP BY dataset_key",
        [$safeWindow]
    );

    $summary = [];
    foreach ($rows as $row) {
        $datasetKey = trim((string) ($row['dataset_key'] ?? ''));
        if ($datasetKey === '') {
            continue;
        }

        $jobKey = preg_replace('/^scheduler\.job\./', '', $datasetKey) ?? $datasetKey;
        $summary[$jobKey] = [
            'failed_runs' => (int) ($row['failed_runs'] ?? 0),
            'successful_runs' => (int) ($row['successful_runs'] ?? 0),
            'total_runs' => (int) ($row['total_runs'] ?? 0),
        ];
    }

    return $summary;
}

function db_static_data_import_state_get(string $sourceKey): ?array
{
    return db_select_one(
        'SELECT id, source_key, source_url, remote_build_id, imported_build_id, imported_mode, status, last_checked_at, last_import_started_at, last_import_finished_at, last_error_message, metadata_json
         FROM static_data_import_state
         WHERE source_key = ?
         LIMIT 1',
        [$sourceKey]
    );
}

function db_static_data_import_state_upsert(
    string $sourceKey,
    string $sourceUrl,
    ?string $remoteBuildId,
    ?string $importedBuildId,
    ?string $importedMode,
    string $status,
    ?string $lastCheckedAt,
    ?string $lastImportStartedAt,
    ?string $lastImportFinishedAt,
    ?string $lastErrorMessage,
    ?string $metadataJson
): bool {
    $safeStatus = in_array($status, ['idle', 'running', 'success', 'failed'], true) ? $status : 'idle';
    $safeMode = in_array((string) $importedMode, ['full', 'incremental'], true) ? $importedMode : null;

    return db_execute(
        'INSERT INTO static_data_import_state (
            source_key,
            source_url,
            remote_build_id,
            imported_build_id,
            imported_mode,
            status,
            last_checked_at,
            last_import_started_at,
            last_import_finished_at,
            last_error_message,
            metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            source_url = VALUES(source_url),
            remote_build_id = VALUES(remote_build_id),
            imported_build_id = VALUES(imported_build_id),
            imported_mode = VALUES(imported_mode),
            status = VALUES(status),
            last_checked_at = VALUES(last_checked_at),
            last_import_started_at = VALUES(last_import_started_at),
            last_import_finished_at = VALUES(last_import_finished_at),
            last_error_message = VALUES(last_error_message),
            metadata_json = VALUES(metadata_json),
            updated_at = CURRENT_TIMESTAMP',
        [
            $sourceKey,
            $sourceUrl,
            $remoteBuildId,
            $importedBuildId,
            $safeMode,
            $safeStatus,
            $lastCheckedAt,
            $lastImportStartedAt,
            $lastImportFinishedAt,
            $lastErrorMessage !== null ? mb_substr($lastErrorMessage, 0, 500) : null,
            $metadataJson,
        ]
    );
}

function db_reference_data_truncate_all(): void
{
    item_scope_db_ensure_schema();
    db_execute('TRUNCATE TABLE ref_item_types');
    db_execute('TRUNCATE TABLE ref_meta_groups');
    db_execute('TRUNCATE TABLE ref_item_groups');
    db_execute('TRUNCATE TABLE ref_item_categories');
    db_execute('TRUNCATE TABLE ref_market_groups');
    db_execute('TRUNCATE TABLE ref_npc_stations');
    db_execute('TRUNCATE TABLE ref_systems');
    db_execute('TRUNCATE TABLE ref_constellations');
    db_execute('TRUNCATE TABLE ref_regions');
}

function db_ref_regions_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_regions', ['region_id', 'region_name'], $rows, ['region_name'], $chunkSize);
}

function db_ref_constellations_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_constellations', ['constellation_id', 'region_id', 'constellation_name'], $rows, ['region_id', 'constellation_name'], $chunkSize);
}

function db_ref_systems_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_systems', ['system_id', 'constellation_id', 'region_id', 'system_name', 'security'], $rows, ['constellation_id', 'region_id', 'system_name', 'security'], $chunkSize);
}

function db_ref_npc_stations_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_npc_stations', ['station_id', 'station_name', 'system_id', 'constellation_id', 'region_id', 'station_type_id'], $rows, ['station_name', 'system_id', 'constellation_id', 'region_id', 'station_type_id'], $chunkSize);
}

function db_ref_market_groups_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_market_groups', ['market_group_id', 'parent_group_id', 'market_group_name', 'description'], $rows, ['parent_group_id', 'market_group_name', 'description'], $chunkSize);
}

function db_ref_item_categories_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    item_scope_db_ensure_schema();

    return db_bulk_insert_or_upsert('ref_item_categories', ['category_id', 'category_name', 'published'], $rows, ['category_name', 'published'], $chunkSize);
}

function db_ref_item_groups_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    item_scope_db_ensure_schema();

    return db_bulk_insert_or_upsert('ref_item_groups', ['group_id', 'category_id', 'group_name', 'published'], $rows, ['category_id', 'group_name', 'published'], $chunkSize);
}

function db_ref_meta_groups_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    item_scope_db_ensure_schema();

    return db_bulk_insert_or_upsert('ref_meta_groups', ['meta_group_id', 'meta_group_name'], $rows, ['meta_group_name'], $chunkSize);
}

function db_ref_item_types_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    item_scope_db_ensure_schema();

    return db_bulk_insert_or_upsert('ref_item_types', ['type_id', 'category_id', 'group_id', 'market_group_id', 'meta_group_id', 'type_name', 'description', 'published', 'volume'], $rows, ['category_id', 'group_id', 'market_group_id', 'meta_group_id', 'type_name', 'description', 'published', 'volume'], $chunkSize);
}

function db_ref_item_types_by_ids(array $typeIds): array
{
    item_scope_db_ensure_schema();

    $ids = array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $typeIds), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($ids), '?'));

    return db_select(
        "SELECT type_id, type_name, category_id, group_id, market_group_id, meta_group_id, description, published, volume
         FROM ref_item_types
         WHERE type_id IN ($placeholders)",
        $ids
    );
}

function db_ref_systems_by_ids(array $systemIds): array
{
    $ids = array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $systemIds), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($ids), '?'));

    return db_select(
        "SELECT system_id, constellation_id, region_id, system_name, security
         FROM ref_systems
         WHERE system_id IN ($placeholders)",
        $ids
    );
}

function db_ref_regions_by_ids(array $regionIds): array
{
    $ids = array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $regionIds), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($ids), '?'));

    return db_select(
        "SELECT region_id, region_name
         FROM ref_regions
         WHERE region_id IN ($placeholders)",
        $ids
    );
}

function db_entity_metadata_cache_get_many(string $entityType, array $entityIds): array
{
    $type = trim($entityType);
    $ids = array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $entityIds), static fn (int $id): bool => $id > 0)));
    if ($type === '' || $ids === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($ids), '?'));

    return db_select(
        "SELECT entity_type, entity_id, entity_name, image_url, metadata_json, source_system, resolution_status, expires_at, last_requested_at, resolved_at, last_error_message, updated_at
         FROM entity_metadata_cache
         WHERE entity_type = ?
           AND entity_id IN ($placeholders)",
        array_merge([$type], $ids)
    );
}

function db_entity_metadata_cache_upsert(array $rows): int
{
    $normalizedRows = [];

    foreach ($rows as $row) {
        $entityType = trim((string) ($row['entity_type'] ?? ''));
        $entityId = (int) ($row['entity_id'] ?? 0);
        if ($entityType === '' || $entityId <= 0) {
            continue;
        }

        $normalizedRows[] = [
            'entity_type' => $entityType,
            'entity_id' => $entityId,
            'entity_name' => ($name = trim((string) ($row['entity_name'] ?? ''))) !== '' ? $name : null,
            'image_url' => ($imageUrl = trim((string) ($row['image_url'] ?? ''))) !== '' ? $imageUrl : null,
            'metadata_json' => $row['metadata_json'] ?? null,
            'source_system' => ($source = trim((string) ($row['source_system'] ?? 'cache'))) !== '' ? $source : 'cache',
            'resolution_status' => in_array((string) ($row['resolution_status'] ?? 'pending'), ['pending', 'resolved', 'failed'], true) ? (string) $row['resolution_status'] : 'pending',
            'expires_at' => $row['expires_at'] ?? null,
            'last_requested_at' => $row['last_requested_at'] ?? gmdate('Y-m-d H:i:s'),
            'resolved_at' => $row['resolved_at'] ?? (((string) ($row['resolution_status'] ?? 'pending')) === 'resolved' ? gmdate('Y-m-d H:i:s') : null),
            'last_error_message' => isset($row['last_error_message']) ? mb_substr(trim((string) $row['last_error_message']), 0, 255) : null,
        ];
    }

    if ($normalizedRows === []) {
        return 0;
    }

    return db_bulk_insert_or_upsert(
        'entity_metadata_cache',
        [
            'entity_type',
            'entity_id',
            'entity_name',
            'image_url',
            'metadata_json',
            'source_system',
            'resolution_status',
            'expires_at',
            'last_requested_at',
            'resolved_at',
            'last_error_message',
        ],
        $normalizedRows,
        [
            'entity_name',
            'image_url',
            'metadata_json',
            'source_system',
            'resolution_status',
            'expires_at',
            'last_requested_at',
            'resolved_at',
            'last_error_message',
        ]
    );
}

function db_entity_metadata_cache_mark_pending(string $entityType, array $entityIds, ?string $errorMessage = null): int
{
    $type = trim($entityType);
    $ids = array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $entityIds), static fn (int $id): bool => $id > 0)));
    if ($type === '' || $ids === []) {
        return 0;
    }

    $rows = [];
    $requestedAt = gmdate('Y-m-d H:i:s');
    foreach ($ids as $id) {
        $rows[] = [
            'entity_type' => $type,
            'entity_id' => $id,
            'entity_name' => null,
            'image_url' => null,
            'metadata_json' => null,
            'source_system' => 'queue',
            'resolution_status' => 'pending',
            'expires_at' => null,
            'last_requested_at' => $requestedAt,
            'resolved_at' => null,
            'last_error_message' => $errorMessage,
        ];
    }

    return db_entity_metadata_cache_upsert($rows);
}

function db_killmail_event_upsert(array $event): bool
{
    db_killmail_payload_schema_ensure();

    return db_transaction(static function () use ($event): bool {
        $hasLegacyPayloadColumns = db_killmail_event_has_legacy_payload_columns();
        $columns = [
            'sequence_id',
            'killmail_id',
            'killmail_hash',
            'uploaded_at',
            'sequence_updated',
            'killmail_time',
            'solar_system_id',
            'region_id',
            'victim_character_id',
            'victim_corporation_id',
            'victim_alliance_id',
            'victim_ship_type_id',
            'zkb_total_value',
            'zkb_points',
            'zkb_npc',
            'zkb_solo',
            'zkb_awox',
        ];
        $values = [
            (int) ($event['sequence_id'] ?? 0),
            (int) ($event['killmail_id'] ?? 0),
            (string) ($event['killmail_hash'] ?? ''),
            $event['uploaded_at'] ?? null,
            isset($event['sequence_updated']) ? (int) $event['sequence_updated'] : null,
            $event['killmail_time'] ?? null,
            isset($event['solar_system_id']) ? (int) $event['solar_system_id'] : null,
            isset($event['region_id']) ? (int) $event['region_id'] : null,
            isset($event['victim_character_id']) ? (int) $event['victim_character_id'] : null,
            isset($event['victim_corporation_id']) ? (int) $event['victim_corporation_id'] : null,
            isset($event['victim_alliance_id']) ? (int) $event['victim_alliance_id'] : null,
            isset($event['victim_ship_type_id']) ? (int) $event['victim_ship_type_id'] : null,
            isset($event['zkb_total_value']) ? (float) $event['zkb_total_value'] : null,
            isset($event['zkb_points']) ? (int) $event['zkb_points'] : null,
            array_key_exists('zkb_npc', $event) ? (int) ((bool) $event['zkb_npc']) : null,
            array_key_exists('zkb_solo', $event) ? (int) ((bool) $event['zkb_solo']) : null,
            array_key_exists('zkb_awox', $event) ? (int) ((bool) $event['zkb_awox']) : null,
        ];
        $updates = [
            'killmail_id = VALUES(killmail_id)',
            'killmail_hash = VALUES(killmail_hash)',
            'uploaded_at = VALUES(uploaded_at)',
            'sequence_updated = VALUES(sequence_updated)',
            'killmail_time = VALUES(killmail_time)',
            'solar_system_id = VALUES(solar_system_id)',
            'region_id = VALUES(region_id)',
            'victim_character_id = VALUES(victim_character_id)',
            'victim_corporation_id = VALUES(victim_corporation_id)',
            'victim_alliance_id = VALUES(victim_alliance_id)',
            'victim_ship_type_id = VALUES(victim_ship_type_id)',
            'zkb_total_value = VALUES(zkb_total_value)',
            'zkb_points = VALUES(zkb_points)',
            'zkb_npc = VALUES(zkb_npc)',
            'zkb_solo = VALUES(zkb_solo)',
            'zkb_awox = VALUES(zkb_awox)',
        ];

        if ($hasLegacyPayloadColumns) {
            $columns[] = 'zkb_json';
            $columns[] = 'raw_killmail_json';
            $values[] = (string) ($event['zkb_json'] ?? '{}');
            $values[] = (string) ($event['raw_killmail_json'] ?? '{}');
            $updates[] = 'zkb_json = VALUES(zkb_json)';
            $updates[] = 'raw_killmail_json = VALUES(raw_killmail_json)';
        }

        $placeholders = implode(', ', array_fill(0, count($columns), '?'));
        $eventWritten = db_execute(
            sprintf(
                'INSERT INTO killmail_events (%s) VALUES (%s)
                ON DUPLICATE KEY UPDATE
                    %s,
                    updated_at = CURRENT_TIMESTAMP',
                implode(",\n                ", $columns),
                $placeholders,
                implode(",\n                    ", $updates)
            ),
            $values
        );
        $payloadWritten = db_killmail_event_payload_upsert($event);

        return $eventWritten && $payloadWritten;
    });
}

function db_killmail_event_exists(int $sequenceId, int $killmailId, string $killmailHash): bool
{
    $row = db_select_one(
        'SELECT sequence_id
           FROM killmail_events
          WHERE sequence_id = ?
          LIMIT 1',
        [$sequenceId]
    );

    return $row !== null;
}

function db_killmail_attackers_replace(int $sequenceId, array $rows): int
{
    return db_transaction(static function () use ($sequenceId, $rows): int {
        db_execute('DELETE FROM killmail_attackers WHERE sequence_id = ?', [$sequenceId]);
        if ($rows === []) {
            return 0;
        }

        $written = 0;
        foreach ($rows as $row) {
            $ok = db_execute(
                'INSERT INTO killmail_attackers (
                    sequence_id,
                    attacker_index,
                    character_id,
                    corporation_id,
                    alliance_id,
                    ship_type_id,
                    weapon_type_id,
                    final_blow,
                    security_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    character_id = VALUES(character_id),
                    corporation_id = VALUES(corporation_id),
                    alliance_id = VALUES(alliance_id),
                    ship_type_id = VALUES(ship_type_id),
                    weapon_type_id = VALUES(weapon_type_id),
                    final_blow = VALUES(final_blow),
                    security_status = VALUES(security_status)',
                [
                    $sequenceId,
                    (int) ($row['attacker_index'] ?? 0),
                    isset($row['character_id']) ? (int) $row['character_id'] : null,
                    isset($row['corporation_id']) ? (int) $row['corporation_id'] : null,
                    isset($row['alliance_id']) ? (int) $row['alliance_id'] : null,
                    isset($row['ship_type_id']) ? (int) $row['ship_type_id'] : null,
                    isset($row['weapon_type_id']) ? (int) $row['weapon_type_id'] : null,
                    (int) (($row['final_blow'] ?? false) ? 1 : 0),
                    isset($row['security_status']) ? (float) $row['security_status'] : null,
                ]
            );
            if ($ok) {
                $written++;
            }
        }

        return $written;
    });
}

function db_killmail_items_replace(int $sequenceId, array $rows): int
{
    return db_transaction(static function () use ($sequenceId, $rows): int {
        db_execute('DELETE FROM killmail_items WHERE sequence_id = ?', [$sequenceId]);
        if ($rows === []) {
            return 0;
        }

        $written = 0;
        foreach ($rows as $row) {
            $ok = db_execute(
                'INSERT INTO killmail_items (
                    sequence_id,
                    item_index,
                    item_type_id,
                    item_flag,
                    quantity_dropped,
                    quantity_destroyed,
                    singleton,
                    item_role
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    item_type_id = VALUES(item_type_id),
                    item_flag = VALUES(item_flag),
                    quantity_dropped = VALUES(quantity_dropped),
                    quantity_destroyed = VALUES(quantity_destroyed),
                    singleton = VALUES(singleton),
                    item_role = VALUES(item_role)',
                [
                    $sequenceId,
                    (int) ($row['item_index'] ?? 0),
                    (int) ($row['item_type_id'] ?? 0),
                    isset($row['item_flag']) ? (int) $row['item_flag'] : null,
                    isset($row['quantity_dropped']) ? (int) $row['quantity_dropped'] : null,
                    isset($row['quantity_destroyed']) ? (int) $row['quantity_destroyed'] : null,
                    isset($row['singleton']) ? (int) $row['singleton'] : null,
                    (string) ($row['item_role'] ?? 'other'),
                ]
            );
            if ($ok) {
                $written++;
            }
        }

        return $written;
    });
}

function db_killmail_tracked_alliances_replace(array $rows): bool
{
    return db_transaction(static function () use ($rows): bool {
        db_execute('CREATE TABLE IF NOT EXISTS killmail_tracked_alliances (
            alliance_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            label VARCHAR(255) DEFAULT NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci');
        db_execute('DELETE FROM killmail_tracked_alliances');

        foreach ($rows as $row) {
            db_execute(
                'INSERT INTO killmail_tracked_alliances (alliance_id, label, is_active) VALUES (?, ?, 1)
                 ON DUPLICATE KEY UPDATE label = VALUES(label), is_active = 1, updated_at = CURRENT_TIMESTAMP',
                [(int) ($row['alliance_id'] ?? 0), $row['label'] ?? null]
            );
        }

        return true;
    });
}

function db_killmail_tracked_corporations_replace(array $rows): bool
{
    return db_transaction(static function () use ($rows): bool {
        db_execute('CREATE TABLE IF NOT EXISTS killmail_tracked_corporations (
            corporation_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
            label VARCHAR(255) DEFAULT NULL,
            is_active TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci');
        db_execute('DELETE FROM killmail_tracked_corporations');

        foreach ($rows as $row) {
            db_execute(
                'INSERT INTO killmail_tracked_corporations (corporation_id, label, is_active) VALUES (?, ?, 1)
                 ON DUPLICATE KEY UPDATE label = VALUES(label), is_active = 1, updated_at = CURRENT_TIMESTAMP',
                [(int) ($row['corporation_id'] ?? 0), $row['label'] ?? null]
            );
        }

        return true;
    });
}

function db_killmail_tracked_alliances_active(): array
{
    return db_select('SELECT alliance_id, label FROM killmail_tracked_alliances WHERE is_active = 1 ORDER BY alliance_id ASC');
}

function db_killmail_tracked_corporations_active(): array
{
    return db_select('SELECT corporation_id, label FROM killmail_tracked_corporations WHERE is_active = 1 ORDER BY corporation_id ASC');
}

function db_sync_schedule_delete_by_job_keys(array $jobKeys): int
{
    $safeJobKeys = array_values(array_filter(array_map(static fn (mixed $jobKey): string => trim((string) $jobKey), $jobKeys), static fn (string $jobKey): bool => $jobKey !== ''));
    if ($safeJobKeys === []) {
        return 0;
    }

    db_sync_schedule_registry_columns_ensure();
    $placeholders = implode(',', array_fill(0, count($safeJobKeys), '?'));
    $stmt = db()->prepare("DELETE FROM sync_schedules WHERE job_key IN ({$placeholders})");
    $stmt->execute($safeJobKeys);
    db_query_cache_clear();

    return (int) $stmt->rowCount();
}

function db_killmail_tracked_match_sql(string $eventAlias = 'e'): string
{
    $eventAlias = preg_replace('/[^a-zA-Z0-9_]/', '', $eventAlias) ?: 'e';

    return "(
        {$eventAlias}.victim_alliance_id IN (
            SELECT ta.alliance_id
            FROM killmail_tracked_alliances ta
            WHERE ta.is_active = 1
        )
        OR {$eventAlias}.victim_corporation_id IN (
            SELECT tc.corporation_id
            FROM killmail_tracked_corporations tc
            WHERE tc.is_active = 1
        )
    )";
}

function db_killmail_tracked_matches_sql(?string $effectiveAfterSql = null, ?string $sequenceAfterSql = null): string
{
    $effectiveFilterSql = $effectiveAfterSql !== null
        ? " AND e.effective_killmail_at >= {$effectiveAfterSql}"
        : '';
    $sequenceFilterSql = $sequenceAfterSql !== null
        ? " AND e.sequence_id >= {$sequenceAfterSql}"
        : '';

    return "(
        SELECT
            matched.sequence_id,
            MAX(matched.matches_victim_alliance) AS matches_victim_alliance,
            MAX(matched.matches_victim_corporation) AS matches_victim_corporation,
            MAX(matched.matches_attacker_alliance) AS matches_attacker_alliance,
            MAX(matched.matches_attacker_corporation) AS matches_attacker_corporation
        FROM (
            SELECT
                e.sequence_id,
                1 AS matches_victim_alliance,
                0 AS matches_victim_corporation,
                0 AS matches_attacker_alliance,
                0 AS matches_attacker_corporation
            FROM killmail_tracked_alliances ta
            INNER JOIN killmail_events e
                ON e.victim_alliance_id = ta.alliance_id{$effectiveFilterSql}{$sequenceFilterSql}
            WHERE ta.is_active = 1

            UNION ALL

            SELECT
                e.sequence_id,
                0 AS matches_victim_alliance,
                1 AS matches_victim_corporation,
                0 AS matches_attacker_alliance,
                0 AS matches_attacker_corporation
            FROM killmail_tracked_corporations tc
            INNER JOIN killmail_events e
                ON e.victim_corporation_id = tc.corporation_id{$effectiveFilterSql}{$sequenceFilterSql}
            WHERE tc.is_active = 1
        ) matched
        GROUP BY matched.sequence_id
    )";
}

function db_killmail_ingestion_status(): array
{
    $state = db_sync_state_get('killmail.r2z2.stream');
    $latest = db_select_one('SELECT MAX(sequence_id) AS max_sequence_id, MAX(uploaded_at) AS max_uploaded_at, MAX(created_at) AS max_created_at FROM killmail_events');
    $latestRun = db_sync_run_latest_by_dataset('killmail.r2z2.stream');
    $trackedCounts = db_select_one(
        'SELECT
            (SELECT COUNT(*) FROM killmail_tracked_alliances WHERE is_active = 1) AS tracked_alliance_count,
            (SELECT COUNT(*) FROM killmail_tracked_corporations WHERE is_active = 1) AS tracked_corporation_count'
    );

    return [
        'state' => $state,
        'max_sequence_id' => isset($latest['max_sequence_id']) ? (int) $latest['max_sequence_id'] : null,
        'max_uploaded_at' => $latest['max_uploaded_at'] ?? null,
        'max_created_at' => $latest['max_created_at'] ?? null,
        'latest_run' => $latestRun,
        'tracked_alliance_count' => (int) ($trackedCounts['tracked_alliance_count'] ?? 0),
        'tracked_corporation_count' => (int) ($trackedCounts['tracked_corporation_count'] ?? 0),
    ];
}

function db_killmail_filtered_recent(int $limit = 50): array
{
    $limit = max(1, min(500, $limit));
    $trackedMatchesSql = db_killmail_tracked_matches_sql();

    return db_select(
        "SELECT e.sequence_id, e.killmail_id, e.killmail_hash, e.uploaded_at, e.killmail_time,
                e.victim_alliance_id, e.victim_corporation_id, e.victim_ship_type_id, e.solar_system_id, e.region_id
         FROM {$trackedMatchesSql} tracked
         INNER JOIN killmail_events e ON e.sequence_id = tracked.sequence_id
         ORDER BY e.sequence_id DESC
         LIMIT {$limit}"
    );
}

function db_killmail_overview_summary(int $recentHours = 24): array
{
    $safeRecentHours = max(1, min(24 * 30, $recentHours));
    $trackedMatchesSql = db_killmail_tracked_matches_sql();
    $summary = db_select_one(
        "SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN e.created_at >= (UTC_TIMESTAMP() - INTERVAL {$safeRecentHours} HOUR) THEN 1 ELSE 0 END) AS recent_count,
            MAX(e.sequence_id) AS max_sequence_id,
            MAX(e.created_at) AS last_ingested_at,
            MAX(e.uploaded_at) AS latest_uploaded_at
         FROM killmail_events e"
    ) ?? [];
    $trackedCountRow = db_select_one("SELECT COUNT(*) AS tracked_match_count FROM {$trackedMatchesSql} tracked") ?? [];
    $summary['tracked_match_count'] = (int) ($trackedCountRow['tracked_match_count'] ?? 0);

    return $summary;
}

function db_killmail_overview_filter_options(): array
{
    $alliances = db_select(
        "SELECT DISTINCT
            e.victim_alliance_id AS entity_id,
            COALESCE(NULLIF(ta.label, ''), CONCAT('Alliance #', e.victim_alliance_id)) AS entity_label
         FROM killmail_events e
         LEFT JOIN killmail_tracked_alliances ta
           ON ta.alliance_id = e.victim_alliance_id
          AND ta.is_active = 1
         WHERE e.victim_alliance_id IS NOT NULL
           AND e.victim_alliance_id > 0
         ORDER BY entity_label ASC"
    );

    $corporations = db_select(
        "SELECT DISTINCT
            e.victim_corporation_id AS entity_id,
            COALESCE(NULLIF(tc.label, ''), CONCAT('Corporation #', e.victim_corporation_id)) AS entity_label
         FROM killmail_events e
         LEFT JOIN killmail_tracked_corporations tc
           ON tc.corporation_id = e.victim_corporation_id
          AND tc.is_active = 1
         WHERE e.victim_corporation_id IS NOT NULL
           AND e.victim_corporation_id > 0
         ORDER BY entity_label ASC"
    );

    return [
        'alliances' => $alliances,
        'corporations' => $corporations,
    ];
}

function db_killmail_overview_row(int $sequenceId): ?array
{
    if ($sequenceId <= 0) {
        return null;
    }

    $matchSql = db_killmail_tracked_match_sql('e');
    $trackedMatchesSql = db_killmail_tracked_matches_sql();

    return db_select_one(
        "SELECT
            e.sequence_id,
            e.killmail_id,
            e.killmail_hash,
            e.uploaded_at,
            e.sequence_updated,
            e.killmail_time,
            e.solar_system_id,
            e.region_id,
            e.victim_character_id,
            e.victim_corporation_id,
            e.victim_alliance_id,
            e.victim_ship_type_id,
            e.zkb_total_value,
            e.zkb_points,
            e.zkb_npc,
            e.zkb_solo,
            e.zkb_awox,
            e.created_at,
            e.updated_at,
            COALESCE(NULLIF(victim_tc.label, ''), '') AS victim_corporation_label,
            COALESCE(NULLIF(victim_ta.label, ''), '') AS victim_alliance_label,
            COALESCE(NULLIF(ship.type_name, ''), '') AS ship_type_name,
            COALESCE(NULLIF(system_ref.system_name, ''), '') AS system_name,
            COALESCE(NULLIF(region_ref.region_name, ''), '') AS region_name,
            CASE WHEN victim_ta.alliance_id IS NULL THEN 0 ELSE 1 END AS matches_victim_alliance,
            CASE WHEN victim_tc.corporation_id IS NULL THEN 0 ELSE 1 END AS matches_victim_corporation,
            COALESCE(tracked.matches_attacker_alliance, 0) AS matches_attacker_alliance,
            COALESCE(tracked.matches_attacker_corporation, 0) AS matches_attacker_corporation,
            CASE WHEN {$matchSql} THEN 1 ELSE 0 END AS matched_tracked
         FROM killmail_events e
         LEFT JOIN {$trackedMatchesSql} tracked
           ON tracked.sequence_id = e.sequence_id
         LEFT JOIN ref_item_types ship ON ship.type_id = e.victim_ship_type_id
         LEFT JOIN ref_systems system_ref ON system_ref.system_id = e.solar_system_id
         LEFT JOIN ref_regions region_ref ON region_ref.region_id = e.region_id
         LEFT JOIN killmail_tracked_alliances victim_ta
           ON victim_ta.alliance_id = e.victim_alliance_id
          AND victim_ta.is_active = 1
         LEFT JOIN killmail_tracked_corporations victim_tc
           ON victim_tc.corporation_id = e.victim_corporation_id
          AND victim_tc.is_active = 1
         WHERE e.sequence_id = ?
         LIMIT 1",
        [$sequenceId]
    );
}

function db_killmail_detail(int $sequenceId): ?array
{
    db_killmail_payload_schema_ensure();

    if ($sequenceId <= 0) {
        return null;
    }

    $overview = db_killmail_overview_row($sequenceId);
    if (!is_array($overview)) {
        return null;
    }

    $payload = db_killmail_event_payload_by_sequence($sequenceId) ?? [];
    $overview['zkb_json'] = (string) ($payload['zkb_json'] ?? '{}');
    $overview['raw_killmail_json'] = (string) ($payload['raw_killmail_json'] ?? '{}');

    return $overview;
}

function db_killmail_attackers_by_sequence(int $sequenceId): array
{
    if ($sequenceId <= 0) {
        return [];
    }

    return db_select(
        "SELECT
            a.sequence_id,
            a.attacker_index,
            a.character_id,
            a.corporation_id,
            a.alliance_id,
            a.ship_type_id,
            a.weapon_type_id,
            a.final_blow,
            a.security_status,
            COALESCE(NULLIF(ship.type_name, ''), '') AS ship_type_name,
            COALESCE(NULLIF(weapon.type_name, ''), '') AS weapon_type_name
         FROM killmail_attackers a
         LEFT JOIN ref_item_types ship ON ship.type_id = a.ship_type_id
         LEFT JOIN ref_item_types weapon ON weapon.type_id = a.weapon_type_id
         WHERE a.sequence_id = ?
         ORDER BY a.final_blow DESC, a.attacker_index ASC",
        [$sequenceId]
    );
}

function db_killmail_items_by_sequence(int $sequenceId): array
{
    if ($sequenceId <= 0) {
        return [];
    }

    return db_select(
        "SELECT
            i.sequence_id,
            i.item_index,
            i.item_type_id,
            i.item_flag,
            i.quantity_dropped,
            i.quantity_destroyed,
            i.singleton,
            i.item_role,
            i.created_at,
            COALESCE(NULLIF(t.type_name, ''), '') AS item_type_name
         FROM killmail_items i
         LEFT JOIN ref_item_types t ON t.type_id = i.item_type_id
         WHERE i.sequence_id = ?
         ORDER BY FIELD(i.item_role, 'dropped', 'destroyed', 'fitted', 'other'), i.item_index ASC",
        [$sequenceId]
    );
}

function db_killmail_overview_page(array $filters = []): array
{
    db_killmail_overview_schema_ensure();

    $page = max(1, (int) ($filters['page'] ?? 1));
    $pageSize = max(1, min(100, (int) ($filters['page_size'] ?? 25)));
    $offset = ($page - 1) * $pageSize;
    $allianceId = max(0, (int) ($filters['alliance_id'] ?? 0));
    $corporationId = max(0, (int) ($filters['corporation_id'] ?? 0));
    $trackedOnly = (bool) ($filters['tracked_only'] ?? false);
    $search = trim((string) ($filters['search'] ?? ''));
    $trackedMatchesSql = db_killmail_tracked_matches_sql();
    $trackedJoinType = $trackedOnly ? 'INNER JOIN' : 'LEFT JOIN';

    $fromSql = " FROM killmail_events e
        {$trackedJoinType} {$trackedMatchesSql} tracked
          ON tracked.sequence_id = e.sequence_id
        LEFT JOIN killmail_attackers final_blow_attacker
          ON final_blow_attacker.sequence_id = e.sequence_id
         AND final_blow_attacker.attacker_index = (
             SELECT MIN(fb.attacker_index)
               FROM killmail_attackers fb
              WHERE fb.sequence_id = e.sequence_id
                AND fb.final_blow = 1
         )
        LEFT JOIN ref_item_types ship ON ship.type_id = e.victim_ship_type_id
        LEFT JOIN ref_systems system_ref ON system_ref.system_id = e.solar_system_id
        LEFT JOIN ref_regions region_ref ON region_ref.region_id = e.region_id
        LEFT JOIN killmail_tracked_alliances victim_ta
          ON victim_ta.alliance_id = e.victim_alliance_id
         AND victim_ta.is_active = 1
        LEFT JOIN killmail_tracked_corporations victim_tc
          ON victim_tc.corporation_id = e.victim_corporation_id
         AND victim_tc.is_active = 1";

    $conditions = [];
    $params = [];

    if ($allianceId > 0) {
        $conditions[] = 'e.victim_alliance_id = ?';
        $params[] = $allianceId;
    }

    if ($corporationId > 0) {
        $conditions[] = 'e.victim_corporation_id = ?';
        $params[] = $corporationId;
    }

    if ($trackedOnly) {
        $conditions[] = 'tracked.sequence_id IS NOT NULL';
    }

    if ($search !== '') {
        $needle = '%' . $search . '%';
        $conditions[] = '(
            CAST(e.sequence_id AS CHAR) LIKE ?
            OR CAST(e.killmail_id AS CHAR) LIKE ?
            OR COALESCE(victim_ta.label, \'\') LIKE ?
            OR COALESCE(victim_tc.label, \'\') LIKE ?
            OR COALESCE(ship.type_name, \'\') LIKE ?
            OR COALESCE(system_ref.system_name, \'\') LIKE ?
            OR COALESCE(region_ref.region_name, \'\') LIKE ?
        )';
        array_push($params, $needle, $needle, $needle, $needle, $needle, $needle, $needle);
    }

    $whereSql = $conditions === [] ? '' : (' WHERE ' . implode(' AND ', $conditions));

    $countRow = db_select_one(
        'SELECT COUNT(*) AS total_items' . $fromSql . $whereSql,
        $params
    );
    $totalItems = (int) ($countRow['total_items'] ?? 0);
    $totalPages = max(1, (int) ceil($totalItems / $pageSize));
    $page = min($page, $totalPages);
    $offset = ($page - 1) * $pageSize;

    $rows = db_select(
        "SELECT
            e.sequence_id,
            e.killmail_id,
            e.killmail_hash,
            e.killmail_time,
            e.uploaded_at,
            e.created_at,
            e.victim_corporation_id,
            e.victim_alliance_id,
            e.victim_ship_type_id,
            e.solar_system_id,
            e.region_id,
            final_blow_attacker.character_id AS final_blow_character_id,
            final_blow_attacker.corporation_id AS final_blow_corporation_id,
            final_blow_attacker.alliance_id AS final_blow_alliance_id,
            final_blow_attacker.ship_type_id AS final_blow_ship_type_id,
            final_blow_attacker.weapon_type_id AS final_blow_weapon_type_id,
            e.zkb_total_value,
            e.zkb_points,
            e.zkb_npc,
            e.zkb_solo,
            e.zkb_awox,
            COALESCE(NULLIF(victim_tc.label, ''), CONCAT('Corporation #', e.victim_corporation_id)) AS victim_corporation_label,
            COALESCE(NULLIF(victim_ta.label, ''), CONCAT('Alliance #', e.victim_alliance_id)) AS victim_alliance_label,
            COALESCE(NULLIF(ship.type_name, ''), CONCAT('Type #', e.victim_ship_type_id)) AS ship_type_name,
            COALESCE(NULLIF(system_ref.system_name, ''), CONCAT('System #', e.solar_system_id)) AS system_name,
            COALESCE(NULLIF(region_ref.region_name, ''), CONCAT('Region #', e.region_id)) AS region_name,
            COALESCE(tracked.matches_victim_alliance, 0) AS matches_victim_alliance,
            COALESCE(tracked.matches_victim_corporation, 0) AS matches_victim_corporation,
            COALESCE(tracked.matches_attacker_alliance, 0) AS matches_attacker_alliance,
            COALESCE(tracked.matches_attacker_corporation, 0) AS matches_attacker_corporation,
            CASE WHEN tracked.sequence_id IS NULL THEN 0 ELSE 1 END AS matched_tracked
            {$fromSql}
            {$whereSql}
         ORDER BY e.sequence_id DESC
         LIMIT {$pageSize} OFFSET {$offset}",
        $params
    );

    return [
        'rows' => $rows,
        'page' => $page,
        'page_size' => $pageSize,
        'total_items' => $totalItems,
        'total_pages' => $totalPages,
        'showing_from' => $totalItems > 0 ? $offset + 1 : 0,
        'showing_to' => min($offset + $pageSize, $totalItems),
    ];
}

function db_killmail_tracked_recent_hull_losses(array $hullTypeIds, int $hours = 24 * 7): array
{
    $safeHours = max(1, min(24 * 30, $hours));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $hullTypeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $trackedMatchesSql = db_killmail_tracked_matches_sql("(UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)");

    return db_select(
        "SELECT
            e.victim_ship_type_id AS type_id,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN 1 ELSE 0 END) AS losses_24h,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN 1 ELSE 0 END) AS losses_7d,
            COUNT(*) AS losses_window,
            MAX(e.effective_killmail_at) AS latest_loss_at
         FROM {$trackedMatchesSql} tracked
         INNER JOIN killmail_events e ON e.sequence_id = tracked.sequence_id
         WHERE e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)
           AND e.victim_ship_type_id IN ({$placeholders})
         GROUP BY e.victim_ship_type_id",
        $normalizedTypeIds
    );
}

function db_killmail_tracked_recent_item_losses(array $typeIds, int $hours = 24 * 7): array
{
    $rows = db_killmail_item_loss_window_summaries($typeIds, $hours);
    if ($rows !== []) {
        return array_map(static function (array $row): array {
            unset($row['quantity_3d'], $row['losses_3d']);

            return $row;
        }, $rows);
    }

    $safeHours = max(1, min(24 * 30, $hours));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $trackedMatchesSql = db_killmail_tracked_matches_sql("(UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)");
    $quantitySql = db_killmail_item_quantity_sql();

    return db_select(
        "SELECT
            i.item_type_id AS type_id,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN {$quantitySql} ELSE 0 END) AS quantity_24h,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN {$quantitySql} ELSE 0 END) AS quantity_7d,
            SUM({$quantitySql}) AS quantity_window,
            COUNT(DISTINCT CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN e.sequence_id END) AS losses_24h,
            COUNT(DISTINCT CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN e.sequence_id END) AS losses_7d,
            COUNT(DISTINCT e.sequence_id) AS losses_window,
            MAX(e.effective_killmail_at) AS latest_loss_at
         FROM {$trackedMatchesSql} tracked
         INNER JOIN killmail_events e ON e.sequence_id = tracked.sequence_id
         INNER JOIN killmail_items i ON i.sequence_id = e.sequence_id
         WHERE e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)
           AND i.item_type_id IN ({$placeholders})
         GROUP BY i.item_type_id",
        $normalizedTypeIds
    );
}

function db_ref_item_types_by_names(array $names): array
{
    item_scope_db_ensure_schema();

    $safeNames = array_values(array_unique(array_filter(array_map(static fn (mixed $name): string => trim((string) $name), $names), static fn (string $name): bool => $name !== '')));
    if ($safeNames === []) {
        return [];
    }

    $normalizedNames = array_map(static fn (string $name): string => mb_strtolower($name), $safeNames);
    $placeholders = implode(',', array_fill(0, count($normalizedNames), '?'));

    return db_select(
        "SELECT type_id, type_name, category_id, group_id, market_group_id, meta_group_id, description, published, volume
         FROM ref_item_types
         WHERE type_name_normalized IN ($placeholders)",
        $normalizedNames
    );
}

function db_ref_item_scope_metadata_by_ids(array $typeIds): array
{
    item_scope_db_ensure_schema();

    $ids = array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $typeIds), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($ids), '?'));

    return db_select(
        "SELECT
            rit.type_id,
            rit.type_name,
            COALESCE(rit.category_id, rig.category_id) AS category_id,
            rit.group_id,
            rig.group_name,
            ric.category_name,
            rit.market_group_id,
            rmg.market_group_name,
            rmg.parent_group_id,
            rit.meta_group_id,
            rmeta.meta_group_name,
            rit.published,
            rit.volume
         FROM ref_item_types rit
         LEFT JOIN ref_item_groups rig ON rig.group_id = rit.group_id
         LEFT JOIN ref_item_categories ric ON ric.category_id = COALESCE(rit.category_id, rig.category_id)
         LEFT JOIN ref_market_groups rmg ON rmg.market_group_id = rit.market_group_id
         LEFT JOIN ref_meta_groups rmeta ON rmeta.meta_group_id = rit.meta_group_id
         WHERE rit.type_id IN ($placeholders)",
        $ids
    );
}

function db_ref_item_scope_catalog(): array
{
    item_scope_db_ensure_schema();

    return [
        'categories' => db_select(
            'SELECT
                ric.category_id,
                ric.category_name,
                ric.published,
                COUNT(DISTINCT rit.type_id) AS type_count
             FROM ref_item_categories ric
             LEFT JOIN ref_item_groups rig ON rig.category_id = ric.category_id
             LEFT JOIN ref_item_types rit ON rit.group_id = rig.group_id AND rit.published = 1
             GROUP BY ric.category_id, ric.category_name, ric.published
             ORDER BY ric.category_name ASC'
        ),
        'groups' => db_select(
            'SELECT
                rig.group_id,
                rig.group_name,
                rig.category_id,
                COALESCE(ric.category_name, CONCAT("Category #", rig.category_id)) AS category_name,
                rig.published,
                COUNT(DISTINCT rit.type_id) AS type_count
             FROM ref_item_groups rig
             LEFT JOIN ref_item_categories ric ON ric.category_id = rig.category_id
             LEFT JOIN ref_item_types rit ON rit.group_id = rig.group_id AND rit.published = 1
             GROUP BY rig.group_id, rig.group_name, rig.category_id, ric.category_name, rig.published
             ORDER BY rig.group_name ASC'
        ),
        'market_groups' => db_select(
            'SELECT
                rmg.market_group_id,
                rmg.parent_group_id,
                rmg.market_group_name,
                COUNT(DISTINCT rit.type_id) AS type_count
             FROM ref_market_groups rmg
             LEFT JOIN ref_item_types rit ON rit.market_group_id = rmg.market_group_id AND rit.published = 1
             GROUP BY rmg.market_group_id, rmg.parent_group_id, rmg.market_group_name
             ORDER BY rmg.market_group_name ASC'
        ),
        'meta_groups' => db_select(
            'SELECT
                rmeta.meta_group_id,
                rmeta.meta_group_name,
                COUNT(DISTINCT rit.type_id) AS type_count
             FROM ref_meta_groups rmeta
             LEFT JOIN ref_item_types rit ON rit.meta_group_id = rmeta.meta_group_id AND rit.published = 1
             GROUP BY rmeta.meta_group_id, rmeta.meta_group_name
             ORDER BY rmeta.meta_group_id ASC'
        ),
    ];
}

function db_ref_item_type_ids_published(): array
{
    item_scope_db_ensure_schema();

    return array_values(array_map(
        static fn (array $row): int => (int) ($row['type_id'] ?? 0),
        db_select('SELECT type_id FROM ref_item_types WHERE published = 1 ORDER BY type_id ASC')
    ));
}

function db_item_name_cache_get_many(array $normalizedNames): array
{
    $safeNames = array_values(array_unique(array_filter(array_map(static fn (mixed $name): string => trim((string) $name), $normalizedNames), static fn (string $name): bool => $name !== '')));
    if ($safeNames === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($safeNames), '?'));

    return db_select(
        "SELECT normalized_name, item_name, type_id, resolution_source, created_at, updated_at
         FROM item_name_cache
         WHERE normalized_name IN ($placeholders)",
        $safeNames
    );
}

function db_item_name_cache_upsert(string $normalizedName, string $itemName, ?int $typeId, string $resolutionSource): bool
{
    return db_execute(
        'INSERT INTO item_name_cache (normalized_name, item_name, type_id, resolution_source)
         VALUES (?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            item_name = VALUES(item_name),
            type_id = VALUES(type_id),
            resolution_source = VALUES(resolution_source),
            updated_at = CURRENT_TIMESTAMP',
        [$normalizedName, $itemName, $typeId, $resolutionSource]
    );
}

function db_table_exists(string $tableName): bool
{
    $row = db_select_one(
        'SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1',
        [$tableName]
    );

    return $row !== null;
}

function db_column_exists(string $tableName, string $columnName): bool
{
    $row = db_select_one(
        'SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1',
        [$tableName, $columnName]
    );

    return $row !== null;
}

function db_index_exists(string $tableName, string $indexName): bool
{
    $row = db_select_one(
        'SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1',
        [$tableName, $indexName]
    );

    return $row !== null;
}

function db_foreign_key_delete_rule(string $constraintName): ?string
{
    $row = db_select_one(
        'SELECT DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = ? LIMIT 1',
        [$constraintName]
    );

    return $row !== null ? strtoupper((string) ($row['DELETE_RULE'] ?? '')) : null;
}

function item_scope_db_ensure_schema(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    db()->exec(
        'CREATE TABLE IF NOT EXISTS ref_item_categories (
            category_id INT UNSIGNED PRIMARY KEY,
            category_name VARCHAR(190) NOT NULL,
            published TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_published (published)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );

    db()->exec(
        'CREATE TABLE IF NOT EXISTS ref_item_groups (
            group_id INT UNSIGNED PRIMARY KEY,
            category_id INT UNSIGNED NOT NULL,
            group_name VARCHAR(190) NOT NULL,
            published TINYINT(1) NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            KEY idx_category_id (category_id),
            KEY idx_published (published)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );

    db()->exec(
        'CREATE TABLE IF NOT EXISTS ref_meta_groups (
            meta_group_id INT UNSIGNED PRIMARY KEY,
            meta_group_name VARCHAR(120) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );

    if (db_table_exists('ref_item_types') && !db_column_exists('ref_item_types', 'category_id')) {
        db()->exec('ALTER TABLE ref_item_types ADD COLUMN category_id INT UNSIGNED DEFAULT NULL AFTER type_id');
        db()->exec('UPDATE ref_item_types rit INNER JOIN ref_item_groups rig ON rig.group_id = rit.group_id SET rit.category_id = rig.category_id WHERE rit.category_id IS NULL');
        db()->exec('ALTER TABLE ref_item_types MODIFY COLUMN category_id INT UNSIGNED NOT NULL');
        db()->exec('ALTER TABLE ref_item_types ADD KEY idx_category_id (category_id)');
    }

    if (db_table_exists('ref_item_types') && !db_column_exists('ref_item_types', 'meta_group_id')) {
        db()->exec('ALTER TABLE ref_item_types ADD COLUMN meta_group_id INT UNSIGNED DEFAULT NULL AFTER market_group_id');
        db()->exec('ALTER TABLE ref_item_types ADD KEY idx_meta_group_id (meta_group_id)');
    }

    if (db_table_exists('ref_item_types') && !db_column_exists('ref_item_types', 'type_name_normalized')) {
        db()->exec('ALTER TABLE ref_item_types ADD COLUMN type_name_normalized VARCHAR(255) GENERATED ALWAYS AS (LOWER(type_name)) STORED AFTER type_name');
    }

    if (db_table_exists('ref_item_types') && !db_index_exists('ref_item_types', 'idx_type_name_normalized')) {
        db()->exec('ALTER TABLE ref_item_types ADD KEY idx_type_name_normalized (type_name_normalized)');
    }

    $ensured = true;
}

function doctrine_db_ensure_schema(): void
{
    static $ensured = false;

    if ($ensured) {
        return;
    }

    if (db_table_exists('doctrine_fits') && db_column_exists('doctrine_fits', 'doctrine_group_id')) {
        $column = db_select_one(
            'SELECT IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1',
            ['doctrine_fits', 'doctrine_group_id']
        );
        if (strtoupper((string) ($column['IS_NULLABLE'] ?? 'NO')) !== 'YES') {
            db()->exec('ALTER TABLE doctrine_fits MODIFY doctrine_group_id INT UNSIGNED DEFAULT NULL');
        }

        $deleteRule = db_foreign_key_delete_rule('fk_doctrine_fits_group');
        if ($deleteRule !== null && $deleteRule !== 'SET NULL') {
            db()->exec('ALTER TABLE doctrine_fits DROP FOREIGN KEY fk_doctrine_fits_group');
            db()->exec('ALTER TABLE doctrine_fits ADD CONSTRAINT fk_doctrine_fits_group FOREIGN KEY (doctrine_group_id) REFERENCES doctrine_groups(id) ON DELETE SET NULL');
        }
    }

    db()->exec(
        "CREATE TABLE IF NOT EXISTS doctrine_fit_groups (
            doctrine_fit_id INT UNSIGNED NOT NULL,
            doctrine_group_id INT UNSIGNED NOT NULL,
            membership_role ENUM('primary', 'support', 'reference') NOT NULL DEFAULT 'support',
            primary_fit_id INT UNSIGNED GENERATED ALWAYS AS (CASE WHEN membership_role = 'primary' THEN doctrine_fit_id ELSE NULL END) STORED,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (doctrine_fit_id, doctrine_group_id),
            UNIQUE KEY uniq_doctrine_fit_groups_primary_fit (primary_fit_id),
            KEY idx_doctrine_fit_groups_group (doctrine_group_id),
            KEY idx_doctrine_fit_groups_role (membership_role),
            CONSTRAINT fk_doctrine_fit_groups_fit FOREIGN KEY (doctrine_fit_id) REFERENCES doctrine_fits(id) ON DELETE CASCADE,
            CONSTRAINT fk_doctrine_fit_groups_group FOREIGN KEY (doctrine_group_id) REFERENCES doctrine_groups(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    );

    if (db_table_exists('doctrine_fit_groups') && !db_column_exists('doctrine_fit_groups', 'membership_role')) {
        db()->exec("ALTER TABLE doctrine_fit_groups ADD COLUMN membership_role ENUM('primary', 'support', 'reference') NOT NULL DEFAULT 'support' AFTER doctrine_group_id");
    }

    if (db_table_exists('doctrine_fit_groups') && !db_column_exists('doctrine_fit_groups', 'primary_fit_id')) {
        db()->exec("ALTER TABLE doctrine_fit_groups ADD COLUMN primary_fit_id INT UNSIGNED GENERATED ALWAYS AS (CASE WHEN membership_role = 'primary' THEN doctrine_fit_id ELSE NULL END) STORED AFTER membership_role");
    }

    if (db_table_exists('doctrine_fit_groups') && !db_index_exists('doctrine_fit_groups', 'uniq_doctrine_fit_groups_primary_fit')) {
        db()->exec('ALTER TABLE doctrine_fit_groups ADD UNIQUE KEY uniq_doctrine_fit_groups_primary_fit (primary_fit_id)');
    }

    if (db_table_exists('doctrine_fit_groups') && !db_index_exists('doctrine_fit_groups', 'idx_doctrine_fit_groups_role')) {
        db()->exec('ALTER TABLE doctrine_fit_groups ADD KEY idx_doctrine_fit_groups_role (membership_role)');
    }

    db()->exec(
        "INSERT IGNORE INTO doctrine_fit_groups (doctrine_fit_id, doctrine_group_id, membership_role)
         SELECT id, doctrine_group_id, 'primary'
         FROM doctrine_fits
         WHERE doctrine_group_id IS NOT NULL"
    );

    if (db_table_exists('doctrine_fit_groups')) {
        db_execute(
            "UPDATE doctrine_fit_groups dfg
             INNER JOIN doctrine_fits df ON df.id = dfg.doctrine_fit_id
             SET dfg.membership_role = CASE
                 WHEN df.doctrine_group_id IS NOT NULL AND df.doctrine_group_id = dfg.doctrine_group_id THEN 'primary'
                 ELSE 'support'
             END
             WHERE df.doctrine_group_id IS NOT NULL"
        );

        db_execute(
            "UPDATE doctrine_fit_groups dfg
             INNER JOIN (
                SELECT doctrine_fit_id, MIN(doctrine_group_id) AS only_group_id
                FROM doctrine_fit_groups
                GROUP BY doctrine_fit_id
                HAVING COUNT(*) = 1
             ) singles ON singles.doctrine_fit_id = dfg.doctrine_fit_id AND singles.only_group_id = dfg.doctrine_group_id
             LEFT JOIN doctrine_fits df ON df.id = dfg.doctrine_fit_id
             SET dfg.membership_role = 'primary'
             WHERE (df.doctrine_group_id IS NULL OR df.doctrine_group_id = 0)"
        );
    }

    db()->exec(
        'CREATE TABLE IF NOT EXISTS doctrine_fit_snapshots (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            fit_id INT UNSIGNED NOT NULL,
            snapshot_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            complete_fits_available INT UNSIGNED NOT NULL DEFAULT 0,
            target_fits INT UNSIGNED NOT NULL DEFAULT 0,
            fit_gap INT UNSIGNED NOT NULL DEFAULT 0,
            bottleneck_type_id INT UNSIGNED DEFAULT NULL,
            bottleneck_quantity INT NOT NULL DEFAULT 0,
            readiness_state VARCHAR(32) NOT NULL DEFAULT "unknown",
            resupply_pressure_state VARCHAR(32) NOT NULL DEFAULT "stable",
            resupply_pressure_code VARCHAR(64) NOT NULL DEFAULT "stable",
            resupply_pressure_text VARCHAR(255) NOT NULL DEFAULT "Stable",
            recommendation_code VARCHAR(64) NOT NULL DEFAULT "observe",
            recommendation_text VARCHAR(255) NOT NULL DEFAULT "",
            loss_24h INT UNSIGNED NOT NULL DEFAULT 0,
            loss_7d INT UNSIGNED NOT NULL DEFAULT 0,
            local_coverage_pct DECIMAL(6,2) NOT NULL DEFAULT 0.00,
            depletion_24h INT NOT NULL DEFAULT 0,
            depletion_7d INT NOT NULL DEFAULT 0,
            total_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            score_loss_pressure DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            score_stock_gap DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            score_depletion DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            score_bottleneck DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            KEY idx_fit_snapshot_time (fit_id, snapshot_time),
            KEY idx_snapshot_time (snapshot_time),
            KEY idx_readiness_state (readiness_state),
            KEY idx_resupply_pressure_state (resupply_pressure_state),
            KEY idx_recommendation_code (recommendation_code),
            CONSTRAINT fk_doctrine_fit_snapshots_fit FOREIGN KEY (fit_id) REFERENCES doctrine_fits(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );

    db()->exec(
        'CREATE TABLE IF NOT EXISTS doctrine_activity_snapshots (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            entity_type ENUM(\'fit\', \'group\') NOT NULL,
            entity_id INT UNSIGNED NOT NULL,
            entity_name VARCHAR(190) NOT NULL,
            snapshot_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            rank_position INT UNSIGNED NOT NULL DEFAULT 0,
            previous_rank_position INT DEFAULT NULL,
            rank_delta INT NOT NULL DEFAULT 0,
            activity_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            activity_level VARCHAR(32) NOT NULL DEFAULT "low",
            hull_losses_24h INT UNSIGNED NOT NULL DEFAULT 0,
            hull_losses_3d INT UNSIGNED NOT NULL DEFAULT 0,
            hull_losses_7d INT UNSIGNED NOT NULL DEFAULT 0,
            module_losses_24h INT UNSIGNED NOT NULL DEFAULT 0,
            module_losses_3d INT UNSIGNED NOT NULL DEFAULT 0,
            module_losses_7d INT UNSIGNED NOT NULL DEFAULT 0,
            fit_equivalent_losses_24h DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            fit_equivalent_losses_3d DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            fit_equivalent_losses_7d DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            readiness_state VARCHAR(32) NOT NULL DEFAULT "unknown",
            resupply_pressure_state VARCHAR(32) NOT NULL DEFAULT "stable",
            resupply_pressure VARCHAR(255) NOT NULL DEFAULT "Stable",
            readiness_gap_count INT UNSIGNED NOT NULL DEFAULT 0,
            resupply_gap_isk DECIMAL(16,2) NOT NULL DEFAULT 0.00,
            score_components_json LONGTEXT DEFAULT NULL,
            explanation_text VARCHAR(500) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_doctrine_activity_snapshot (entity_type, entity_id, snapshot_time),
            KEY idx_doctrine_activity_snapshot_time (snapshot_time),
            KEY idx_doctrine_activity_rank (entity_type, rank_position),
            KEY idx_doctrine_activity_score (entity_type, activity_score)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );

    db()->exec(
        'CREATE TABLE IF NOT EXISTS item_priority_snapshots (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            type_id INT UNSIGNED NOT NULL,
            item_name VARCHAR(255) NOT NULL,
            snapshot_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            rank_position INT UNSIGNED NOT NULL DEFAULT 0,
            previous_rank_position INT DEFAULT NULL,
            rank_delta INT NOT NULL DEFAULT 0,
            priority_score DECIMAL(8,2) NOT NULL DEFAULT 0.00,
            priority_band VARCHAR(32) NOT NULL DEFAULT "watch",
            is_doctrine_linked TINYINT(1) NOT NULL DEFAULT 0,
            linked_doctrine_count INT UNSIGNED NOT NULL DEFAULT 0,
            linked_active_doctrine_count INT UNSIGNED NOT NULL DEFAULT 0,
            local_available_qty INT NOT NULL DEFAULT 0,
            local_sell_orders INT NOT NULL DEFAULT 0,
            local_sell_volume INT NOT NULL DEFAULT 0,
            recent_loss_qty_24h INT UNSIGNED NOT NULL DEFAULT 0,
            recent_loss_qty_3d INT UNSIGNED NOT NULL DEFAULT 0,
            recent_loss_qty_7d INT UNSIGNED NOT NULL DEFAULT 0,
            recent_loss_events_24h INT UNSIGNED NOT NULL DEFAULT 0,
            recent_loss_events_3d INT UNSIGNED NOT NULL DEFAULT 0,
            recent_loss_events_7d INT UNSIGNED NOT NULL DEFAULT 0,
            readiness_gap_fit_count INT UNSIGNED NOT NULL DEFAULT 0,
            bottleneck_fit_count INT UNSIGNED NOT NULL DEFAULT 0,
            depletion_state VARCHAR(32) NOT NULL DEFAULT "stable",
            score_components_json LONGTEXT DEFAULT NULL,
            linked_doctrines_json LONGTEXT DEFAULT NULL,
            explanation_text VARCHAR(500) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_item_priority_snapshot (type_id, snapshot_time),
            KEY idx_item_priority_snapshot_time (snapshot_time),
            KEY idx_item_priority_rank (rank_position),
            KEY idx_item_priority_score (priority_score)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );

    if (db_table_exists('doctrine_fit_snapshots') && !db_column_exists('doctrine_fit_snapshots', 'resupply_pressure_state')) {
        db()->exec('ALTER TABLE doctrine_fit_snapshots ADD COLUMN resupply_pressure_state VARCHAR(32) NOT NULL DEFAULT "stable" AFTER readiness_state');
        db()->exec('ALTER TABLE doctrine_fit_snapshots ADD COLUMN resupply_pressure_code VARCHAR(64) NOT NULL DEFAULT "stable" AFTER resupply_pressure_state');
        db()->exec('ALTER TABLE doctrine_fit_snapshots ADD COLUMN resupply_pressure_text VARCHAR(255) NOT NULL DEFAULT "Stable" AFTER resupply_pressure_code');
        db()->exec('ALTER TABLE doctrine_fit_snapshots ADD KEY idx_resupply_pressure_state (resupply_pressure_state)');
    }

    if (db_table_exists('doctrine_fit_items') && !db_column_exists('doctrine_fit_items', 'is_stock_tracked')) {
        db()->exec('ALTER TABLE doctrine_fit_items ADD COLUMN is_stock_tracked TINYINT(1) NOT NULL DEFAULT 1 AFTER quantity');
    }

    $fitColumnDefinitions = [
        'target_fleet_size_override' => 'ALTER TABLE doctrine_fits ADD COLUMN target_fleet_size_override INT UNSIGNED DEFAULT NULL AFTER ship_type_id',
        'source_type' => "ALTER TABLE doctrine_fits ADD COLUMN source_type ENUM('html', 'eft', 'buyall', 'manual') NOT NULL DEFAULT 'manual' AFTER ship_type_id",
        'source_reference' => 'ALTER TABLE doctrine_fits ADD COLUMN source_reference VARCHAR(255) DEFAULT NULL AFTER source_format',
        'notes' => 'ALTER TABLE doctrine_fits ADD COLUMN notes TEXT DEFAULT NULL AFTER source_reference',
        'raw_html' => 'ALTER TABLE doctrine_fits ADD COLUMN raw_html LONGTEXT DEFAULT NULL AFTER import_body',
        'raw_buyall' => 'ALTER TABLE doctrine_fits ADD COLUMN raw_buyall LONGTEXT DEFAULT NULL AFTER raw_html',
        'raw_eft' => 'ALTER TABLE doctrine_fits ADD COLUMN raw_eft LONGTEXT DEFAULT NULL AFTER raw_buyall',
        'metadata_json' => 'ALTER TABLE doctrine_fits ADD COLUMN metadata_json LONGTEXT DEFAULT NULL AFTER raw_eft',
        'parse_warnings_json' => 'ALTER TABLE doctrine_fits ADD COLUMN parse_warnings_json LONGTEXT DEFAULT NULL AFTER metadata_json',
        'parse_status' => "ALTER TABLE doctrine_fits ADD COLUMN parse_status ENUM('ready', 'review') NOT NULL DEFAULT 'ready' AFTER parse_warnings_json",
        'review_status' => "ALTER TABLE doctrine_fits ADD COLUMN review_status ENUM('clean', 'needs_review', 'reparse_requested') NOT NULL DEFAULT 'clean' AFTER parse_status",
        'conflict_state' => "ALTER TABLE doctrine_fits ADD COLUMN conflict_state ENUM('none', 'duplicate_name', 'duplicate_items', 'version_conflict', 'source_mismatch') NOT NULL DEFAULT 'none' AFTER review_status",
        'fingerprint_hash' => 'ALTER TABLE doctrine_fits ADD COLUMN fingerprint_hash CHAR(64) DEFAULT NULL AFTER conflict_state',
        'warning_count' => 'ALTER TABLE doctrine_fits ADD COLUMN warning_count INT UNSIGNED NOT NULL DEFAULT 0 AFTER fingerprint_hash',
    ];

    foreach ($fitColumnDefinitions as $column => $sql) {
        if (db_table_exists('doctrine_fits') && !db_column_exists('doctrine_fits', $column)) {
            db()->exec($sql);
        }
    }

    if (db_table_exists('doctrine_fits') && !db_index_exists('doctrine_fits', 'idx_source_type')) {
        db()->exec('ALTER TABLE doctrine_fits ADD KEY idx_source_type (source_type)');
    }

    if (db_table_exists('doctrine_fits') && !db_index_exists('doctrine_fits', 'idx_parse_status')) {
        db()->exec('ALTER TABLE doctrine_fits ADD KEY idx_parse_status (parse_status, review_status)');
    }

    if (db_table_exists('doctrine_fits') && !db_index_exists('doctrine_fits', 'idx_conflict_state')) {
        db()->exec('ALTER TABLE doctrine_fits ADD KEY idx_conflict_state (conflict_state)');
    }

    if (db_table_exists('doctrine_fits') && !db_index_exists('doctrine_fits', 'idx_fingerprint_hash')) {
        db()->exec('ALTER TABLE doctrine_fits ADD KEY idx_fingerprint_hash (fingerprint_hash)');
    }

    if (db_table_exists('doctrine_fit_items') && !db_column_exists('doctrine_fit_items', 'source_role')) {
        db()->exec("ALTER TABLE doctrine_fit_items ADD COLUMN source_role VARCHAR(80) NOT NULL DEFAULT 'fit' AFTER slot_category");
    }

    if (db_table_exists('doctrine_fit_items') && !db_index_exists('doctrine_fit_items', 'idx_source_role')) {
        db()->exec('ALTER TABLE doctrine_fit_items ADD KEY idx_source_role (source_role)');
    }

    $ensured = true;
}

function db_doctrine_group_membership_fit_count(int $groupId): int
{
    doctrine_db_ensure_schema();

    $row = db_select_one(
        'SELECT COUNT(DISTINCT doctrine_fit_id) AS fit_count FROM doctrine_fit_groups WHERE doctrine_group_id = ?',
        [$groupId]
    );

    return (int) ($row['fit_count'] ?? 0);
}

function db_doctrine_groups_all(): array
{
    doctrine_db_ensure_schema();

    return db_select(
        'SELECT
            dg.id,
            dg.group_name,
            dg.description,
            dg.created_at,
            dg.updated_at,
            COUNT(DISTINCT dfg.doctrine_fit_id) AS fit_count,
            COALESCE(SUM(COALESCE(df.item_count, 0)), 0) AS item_count,
            MAX(df.updated_at) AS last_fit_updated_at
         FROM doctrine_groups dg
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_group_id = dg.id
         LEFT JOIN doctrine_fits df ON df.id = dfg.doctrine_fit_id
         GROUP BY dg.id, dg.group_name, dg.description, dg.created_at, dg.updated_at
         ORDER BY dg.group_name ASC'
    );
}

function db_doctrine_group_by_id(int $groupId): ?array
{
    doctrine_db_ensure_schema();

    if ($groupId <= 0) {
        return null;
    }

    return db_select_one(
        'SELECT
            dg.id,
            dg.group_name,
            dg.description,
            dg.created_at,
            dg.updated_at,
            COUNT(DISTINCT dfg.doctrine_fit_id) AS fit_count,
            COALESCE(SUM(COALESCE(df.item_count, 0)), 0) AS item_count,
            MAX(df.updated_at) AS last_fit_updated_at
         FROM doctrine_groups dg
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_group_id = dg.id
         LEFT JOIN doctrine_fits df ON df.id = dfg.doctrine_fit_id
         WHERE dg.id = ?
         GROUP BY dg.id, dg.group_name, dg.description, dg.created_at, dg.updated_at
         LIMIT 1',
        [$groupId]
    );
}

function db_doctrine_group_create(string $groupName, ?string $description = null): int
{
    doctrine_db_ensure_schema();

    db_execute(
        'INSERT INTO doctrine_groups (group_name, description) VALUES (?, ?)
         ON DUPLICATE KEY UPDATE
            description = COALESCE(VALUES(description), description),
            id = LAST_INSERT_ID(id),
            updated_at = CURRENT_TIMESTAMP',
        [$groupName, $description]
    );

    return (int) db()->lastInsertId();
}

function db_doctrine_group_update(int $groupId, string $groupName, ?string $description = null): bool
{
    doctrine_db_ensure_schema();

    if ($groupId <= 0) {
        return false;
    }

    return db_execute(
        'UPDATE doctrine_groups SET group_name = ?, description = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
        [$groupName, $description, $groupId]
    );
}

function db_doctrine_group_delete(int $groupId): array
{
    doctrine_db_ensure_schema();

    if ($groupId <= 0) {
        return ['deleted' => false, 'removed_memberships' => 0, 'orphaned_fits' => 0];
    }

    return db_transaction(static function () use ($groupId): array {
        $memberships = db_select(
            'SELECT doctrine_fit_id FROM doctrine_fit_groups WHERE doctrine_group_id = ?',
            [$groupId]
        );
        $fitIds = array_values(array_unique(array_map(static fn (array $row): int => (int) ($row['doctrine_fit_id'] ?? 0), $memberships)));
        $orphanedFits = 0;

        foreach ($fitIds as $fitId) {
            $other = db_select_one(
                'SELECT doctrine_group_id FROM doctrine_fit_groups WHERE doctrine_fit_id = ? AND doctrine_group_id <> ? ORDER BY doctrine_group_id ASC LIMIT 1',
                [$fitId, $groupId]
            );
            if ($other === null) {
                $orphanedFits++;
            }
        }

        db_execute('DELETE FROM doctrine_fit_groups WHERE doctrine_group_id = ?', [$groupId]);
        db_execute('DELETE FROM doctrine_groups WHERE id = ? LIMIT 1', [$groupId]);

        foreach ($fitIds as $fitId) {
            $next = db_select_one(
                "SELECT doctrine_group_id
                 FROM doctrine_fit_groups
                 WHERE doctrine_fit_id = ? AND membership_role = 'primary'
                 LIMIT 1",
                [$fitId]
            );
            db_execute(
                'UPDATE doctrine_fits SET doctrine_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
                [$next !== null ? (int) ($next['doctrine_group_id'] ?? 0) : null, $fitId]
            );
        }

        return [
            'deleted' => true,
            'removed_memberships' => count($memberships),
            'orphaned_fits' => $orphanedFits,
        ];
    });
}

function db_doctrine_membership_role(string $role): string
{
    return in_array($role, ['primary', 'support', 'reference'], true) ? $role : 'support';
}

function db_doctrine_fit_memberships_normalize(array $memberships): array
{
    $normalized = [];

    foreach ($memberships as $key => $membership) {
        if (is_array($membership)) {
            $groupId = (int) ($membership['group_id'] ?? $membership['doctrine_group_id'] ?? $membership['id'] ?? 0);
            $role = db_doctrine_membership_role((string) ($membership['membership_role'] ?? 'support'));
        } else {
            $groupId = (int) $membership;
            $role = 'support';
            if ($key === 0) {
                $role = 'primary';
            }
        }

        if ($groupId <= 0) {
            continue;
        }

        $normalized[$groupId] = [
            'doctrine_fit_id' => 0,
            'doctrine_group_id' => $groupId,
            'membership_role' => $role,
        ];
    }

    $primaryCount = 0;
    foreach ($normalized as $membership) {
        if (($membership['membership_role'] ?? 'support') === 'primary') {
            $primaryCount++;
        }
    }

    if ($primaryCount > 1) {
        $firstPrimaryGroupId = 0;
        foreach ($normalized as $groupId => $membership) {
            if (($membership['membership_role'] ?? 'support') !== 'primary') {
                continue;
            }
            if ($firstPrimaryGroupId === 0) {
                $firstPrimaryGroupId = (int) $groupId;
                continue;
            }
            $normalized[$groupId]['membership_role'] = 'support';
        }
    }

    return array_values($normalized);
}

function db_doctrine_fit_membership_rows(int $fitId): array
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return [];
    }

    return db_select(
        "SELECT dg.id, dg.group_name, dfg.membership_role,
                CASE WHEN dfg.membership_role = 'primary' THEN 1 ELSE 0 END AS is_primary
         FROM doctrine_fit_groups dfg
         INNER JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         WHERE dfg.doctrine_fit_id = ?
         ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END ASC, dg.group_name ASC",
        [$fitId]
    );
}

function db_doctrine_fit_group_ids(int $fitId): array
{
    return array_values(array_map(static fn (array $row): int => (int) ($row['id'] ?? 0), db_doctrine_fit_membership_rows($fitId)));
}

function db_doctrine_fits_all(): array
{
    doctrine_db_ensure_schema();

    return db_select(
        "SELECT
            df.id,
            df.doctrine_group_id,
            df.fit_name,
            df.ship_name,
            df.ship_type_id,
            df.target_fleet_size_override,
            df.source_type,
            df.source_format,
            df.source_reference,
            df.notes,
            df.import_body,
            df.parse_status,
            df.review_status,
            df.conflict_state,
            df.fingerprint_hash,
            df.warning_count,
            df.item_count,
            df.unresolved_count,
            df.created_at,
            df.updated_at,
            MAX(CASE WHEN dfg.membership_role = 'primary' THEN dfg.doctrine_group_id END) AS primary_group_id,
            GROUP_CONCAT(DISTINCT dfg.doctrine_group_id ORDER BY dfg.doctrine_group_id SEPARATOR ',') AS group_ids_csv,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR '||') AS group_names_csv,
            GROUP_CONCAT(DISTINCT CONCAT(dfg.doctrine_group_id, ':', dfg.membership_role) ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END, dfg.doctrine_group_id SEPARATOR ',') AS membership_roles_csv,
            GROUP_CONCAT(DISTINCT CONCAT(dg.group_name, ':', dfg.membership_role) ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END, dg.group_name SEPARATOR '||') AS membership_names_csv
         FROM doctrine_fits df
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
         LEFT JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.target_fleet_size_override, df.source_type, df.source_format, df.source_reference, df.notes, df.import_body, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at
         ORDER BY df.updated_at DESC, df.fit_name ASC"
    );
}

function db_doctrine_fits_by_group(int $groupId): array
{
    doctrine_db_ensure_schema();

    if ($groupId <= 0) {
        return [];
    }

    return db_select(
        "SELECT
            df.id,
            df.doctrine_group_id,
            df.fit_name,
            df.ship_name,
            df.ship_type_id,
            df.target_fleet_size_override,
            df.source_type,
            df.source_format,
            df.source_reference,
            df.notes,
            df.import_body,
            df.parse_status,
            df.review_status,
            df.conflict_state,
            df.fingerprint_hash,
            df.warning_count,
            df.item_count,
            df.unresolved_count,
            df.created_at,
            df.updated_at,
            dfg.membership_role,
            COALESCE(SUM(dfi.quantity), 0) AS required_quantity,
            COUNT(dfi.id) AS fit_line_count
         FROM doctrine_fit_groups dfg
         INNER JOIN doctrine_fits df ON df.id = dfg.doctrine_fit_id
         LEFT JOIN doctrine_fit_items dfi ON dfi.doctrine_fit_id = df.id
         WHERE dfg.doctrine_group_id = ?
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.target_fleet_size_override, df.source_type, df.source_format, df.source_reference, df.notes, df.import_body, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at, dfg.membership_role
         ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END ASC, df.updated_at DESC, df.fit_name ASC",
        [$groupId]
    );
}

function db_doctrine_ungrouped_fits(): array
{
    doctrine_db_ensure_schema();

    return db_select(
        "SELECT
            df.*,
            GROUP_CONCAT(DISTINCT CONCAT(dfg.doctrine_group_id, ':', dfg.membership_role) ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END, dfg.doctrine_group_id SEPARATOR ',') AS membership_roles_csv,
            GROUP_CONCAT(DISTINCT CONCAT(dg.group_name, ':', dfg.membership_role) ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END, dg.group_name SEPARATOR '||') AS membership_names_csv,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR '||') AS group_names_csv
         FROM doctrine_fits df
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
         LEFT JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.target_fleet_size_override, df.source_type, df.source_format, df.source_reference, df.notes, df.import_body, df.raw_html, df.raw_buyall, df.raw_eft, df.metadata_json, df.parse_warnings_json, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at
         HAVING SUM(CASE WHEN dfg.membership_role = 'primary' THEN 1 ELSE 0 END) = 0
         ORDER BY df.updated_at DESC, df.fit_name ASC"
    );
}

function db_doctrine_fit_by_id(int $fitId): ?array
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return null;
    }

    return db_select_one(
        "SELECT
            df.*,
            pg.group_name AS primary_group_name,
            MAX(CASE WHEN dfg.membership_role = 'primary' THEN dfg.doctrine_group_id END) AS primary_group_id,
            GROUP_CONCAT(DISTINCT dfg.doctrine_group_id ORDER BY dfg.doctrine_group_id SEPARATOR ',') AS group_ids_csv,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR '||') AS group_names_csv,
            GROUP_CONCAT(DISTINCT CONCAT(dfg.doctrine_group_id, ':', dfg.membership_role) ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END, dfg.doctrine_group_id SEPARATOR ',') AS membership_roles_csv,
            GROUP_CONCAT(DISTINCT CONCAT(dg.group_name, ':', dfg.membership_role) ORDER BY CASE dfg.membership_role WHEN 'primary' THEN 0 WHEN 'support' THEN 1 ELSE 2 END, dg.group_name SEPARATOR '||') AS membership_names_csv
         FROM doctrine_fits df
         LEFT JOIN doctrine_groups pg ON pg.id = df.doctrine_group_id
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
         LEFT JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         WHERE df.id = ?
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.target_fleet_size_override, df.source_type, df.source_format, df.source_reference, df.notes, df.import_body, df.raw_html, df.raw_buyall, df.raw_eft, df.metadata_json, df.parse_warnings_json, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at, pg.group_name
         LIMIT 1",
        [$fitId]
    );
}

function db_doctrine_fit_items_by_fit(int $fitId): array
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return [];
    }

    return db_select(
        'SELECT
            dfi.id,
            dfi.doctrine_fit_id,
            dfi.line_number,
            dfi.slot_category,
            dfi.source_role,
            dfi.item_name,
            dfi.type_id,
            dfi.quantity,
            dfi.is_stock_tracked,
            dfi.resolution_source,
            rit.type_name
         FROM doctrine_fit_items dfi
         LEFT JOIN ref_item_types rit ON rit.type_id = dfi.type_id
         WHERE dfi.doctrine_fit_id = ?
         ORDER BY dfi.line_number ASC, dfi.id ASC',
        [$fitId]
    );
}

function db_doctrine_fit_items_by_fit_ids(array $fitIds): array
{
    doctrine_db_ensure_schema();

    $fitIds = array_values(array_filter(array_map('intval', $fitIds), static fn (int $id): bool => $id > 0));
    if ($fitIds === []) {
        return [];
    }

    $placeholders = implode(', ', array_fill(0, count($fitIds), '?'));

    return db_select(
        "SELECT
            dfi.id,
            dfi.doctrine_fit_id,
            dfi.line_number,
            dfi.slot_category,
            dfi.source_role,
            dfi.item_name,
            dfi.type_id,
            dfi.quantity,
            dfi.is_stock_tracked,
            dfi.resolution_source,
            rit.type_name
         FROM doctrine_fit_items dfi
         LEFT JOIN ref_item_types rit ON rit.type_id = dfi.type_id
         WHERE dfi.doctrine_fit_id IN ({$placeholders})
         ORDER BY dfi.doctrine_fit_id ASC, dfi.line_number ASC, dfi.id ASC",
        $fitIds
    );
}

function db_doctrine_fit_latest_snapshots(array $fitIds = []): array
{
    doctrine_db_ensure_schema();

    $params = [];
    $fitFilterSql = '';
    if ($fitIds !== []) {
        $fitIds = array_values(array_unique(array_filter(array_map('intval', $fitIds), static fn (int $id): bool => $id > 0)));
        if ($fitIds === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($fitIds), '?'));
        $fitFilterSql = " AND dfs.fit_id IN ({$placeholders})";
        $params = $fitIds;
    }

    return db_select(
        "SELECT dfs.*
         FROM doctrine_fit_snapshots dfs
         INNER JOIN (
            SELECT fit_id, MAX(id) AS latest_id
            FROM doctrine_fit_snapshots
            GROUP BY fit_id
         ) latest ON latest.latest_id = dfs.id
         WHERE 1 = 1{$fitFilterSql}
         ORDER BY dfs.snapshot_time DESC, dfs.id DESC",
        $params
    );
}

function db_doctrine_fit_snapshot_history(int $fitId, int $limit = 20): array
{
    doctrine_db_ensure_schema();

    $safeFitId = max(0, $fitId);
    $safeLimit = max(1, min($limit, 90));
    if ($safeFitId <= 0) {
        return [];
    }

    return db_select(
        "SELECT *
         FROM doctrine_fit_snapshots
         WHERE fit_id = ?
         ORDER BY snapshot_time DESC, id DESC
         LIMIT {$safeLimit}",
        [$safeFitId]
    );
}

function db_doctrine_fit_snapshot_insert(array $row): int
{
    doctrine_db_ensure_schema();

    db_execute(
        'INSERT INTO doctrine_fit_snapshots (
            fit_id,
            snapshot_time,
            complete_fits_available,
            target_fits,
            fit_gap,
            bottleneck_type_id,
            bottleneck_quantity,
            readiness_state,
            resupply_pressure_state,
            resupply_pressure_code,
            resupply_pressure_text,
            recommendation_code,
            recommendation_text,
            loss_24h,
            loss_7d,
            local_coverage_pct,
            depletion_24h,
            depletion_7d,
            total_score,
            score_loss_pressure,
            score_stock_gap,
            score_depletion,
            score_bottleneck
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [
            (int) ($row['fit_id'] ?? 0),
            (string) ($row['snapshot_time'] ?? gmdate('Y-m-d H:i:s')),
            max(0, (int) ($row['complete_fits_available'] ?? 0)),
            max(0, (int) ($row['target_fits'] ?? 0)),
            max(0, (int) ($row['fit_gap'] ?? 0)),
            isset($row['bottleneck_type_id']) && (int) $row['bottleneck_type_id'] > 0 ? (int) $row['bottleneck_type_id'] : null,
            (int) ($row['bottleneck_quantity'] ?? 0),
            mb_substr(trim((string) ($row['readiness_state'] ?? 'unknown')), 0, 32),
            mb_substr(trim((string) ($row['resupply_pressure_state'] ?? 'stable')), 0, 32),
            mb_substr(trim((string) ($row['resupply_pressure_code'] ?? 'stable')), 0, 64),
            mb_substr(trim((string) ($row['resupply_pressure_text'] ?? 'Stable')), 0, 255),
            mb_substr(trim((string) ($row['recommendation_code'] ?? 'observe')), 0, 64),
            mb_substr(trim((string) ($row['recommendation_text'] ?? '')), 0, 255),
            max(0, (int) ($row['loss_24h'] ?? 0)),
            max(0, (int) ($row['loss_7d'] ?? 0)),
            round((float) ($row['local_coverage_pct'] ?? 0.0), 2),
            (int) ($row['depletion_24h'] ?? 0),
            (int) ($row['depletion_7d'] ?? 0),
            round((float) ($row['total_score'] ?? 0.0), 2),
            round((float) ($row['score_loss_pressure'] ?? 0.0), 2),
            round((float) ($row['score_stock_gap'] ?? 0.0), 2),
            round((float) ($row['score_depletion'] ?? 0.0), 2),
            round((float) ($row['score_bottleneck'] ?? 0.0), 2),
        ]
    );

    return (int) db()->lastInsertId();
}

function db_doctrine_ai_briefing_get(string $entityType, int $entityId): ?array
{
    $normalizedType = in_array($entityType, ['fit', 'group'], true) ? $entityType : '';
    $safeEntityId = max(0, $entityId);
    if ($normalizedType === '' || $safeEntityId <= 0) {
        return null;
    }

    return db_select_one(
        'SELECT *
         FROM doctrine_ai_briefings
         WHERE entity_type = ? AND entity_id = ?
         LIMIT 1',
        [$normalizedType, $safeEntityId]
    );
}

function db_doctrine_ai_briefings_get_many(array $targets): array
{
    $normalized = [];

    foreach ($targets as $target) {
        if (!is_array($target)) {
            continue;
        }

        $entityType = in_array((string) ($target['entity_type'] ?? ''), ['fit', 'group'], true)
            ? (string) $target['entity_type']
            : '';
        $entityId = max(0, (int) ($target['entity_id'] ?? 0));
        if ($entityType === '' || $entityId <= 0) {
            continue;
        }

        $normalized[$entityType . ':' . $entityId] = [$entityType, $entityId];
    }

    if ($normalized === []) {
        return [];
    }

    $clauses = [];
    $params = [];
    foreach (array_values($normalized) as [$entityType, $entityId]) {
        $clauses[] = '(entity_type = ? AND entity_id = ?)';
        $params[] = $entityType;
        $params[] = $entityId;
    }

    return db_select(
        'SELECT *
         FROM doctrine_ai_briefings
         WHERE ' . implode(' OR ', $clauses),
        $params
    );
}

function db_doctrine_ai_briefings_top(int $limit = 6): array
{
    $safeLimit = max(1, min(20, $limit));

    return db_select(
        "SELECT *
         FROM doctrine_ai_briefings
         WHERE generation_status IN ('ready', 'fallback')
         ORDER BY
            FIELD(priority_level, 'critical', 'high', 'medium', 'low'),
            computed_at DESC,
            updated_at DESC
         LIMIT {$safeLimit}"
    );
}

function db_doctrine_ai_briefing_upsert(array $row): bool
{
    $entityType = in_array((string) ($row['entity_type'] ?? ''), ['fit', 'group'], true)
        ? (string) $row['entity_type']
        : '';
    $entityId = max(0, (int) ($row['entity_id'] ?? 0));
    if ($entityType === '' || $entityId <= 0) {
        return false;
    }

    $fitId = $entityType === 'fit' ? $entityId : null;
    $groupId = $entityType === 'group' ? $entityId : null;

    return db_execute(
        'INSERT INTO doctrine_ai_briefings (
            entity_type,
            entity_id,
            fit_id,
            group_id,
            generation_status,
            computed_at,
            model_name,
            headline,
            summary,
            action_text,
            priority_level,
            source_payload_json,
            response_json,
            error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            fit_id = VALUES(fit_id),
            group_id = VALUES(group_id),
            generation_status = VALUES(generation_status),
            computed_at = VALUES(computed_at),
            model_name = VALUES(model_name),
            headline = VALUES(headline),
            summary = VALUES(summary),
            action_text = VALUES(action_text),
            priority_level = VALUES(priority_level),
            source_payload_json = VALUES(source_payload_json),
            response_json = VALUES(response_json),
            error_message = VALUES(error_message),
            updated_at = CURRENT_TIMESTAMP',
        [
            $entityType,
            $entityId,
            $fitId,
            $groupId,
            mb_substr(trim((string) ($row['generation_status'] ?? 'ready')), 0, 32),
            (string) ($row['computed_at'] ?? gmdate('Y-m-d H:i:s')),
            ($row['model_name'] ?? null) !== null ? mb_substr(trim((string) $row['model_name']), 0, 120) : null,
            ($row['headline'] ?? null) !== null ? mb_substr(trim((string) $row['headline']), 0, 255) : null,
            ($row['summary'] ?? null) !== null ? trim((string) $row['summary']) : null,
            ($row['action_text'] ?? null) !== null ? trim((string) $row['action_text']) : null,
            in_array((string) ($row['priority_level'] ?? 'medium'), ['low', 'medium', 'high', 'critical'], true)
                ? (string) $row['priority_level']
                : 'medium',
            ($row['source_payload_json'] ?? null) !== null ? (string) $row['source_payload_json'] : null,
            ($row['response_json'] ?? null) !== null ? (string) $row['response_json'] : null,
            ($row['error_message'] ?? null) !== null ? mb_substr(trim((string) $row['error_message']), 0, 500) : null,
        ]
    );
}

function db_doctrine_fit_replace_items(int $fitId, array $items): void
{
    db_execute('DELETE FROM doctrine_fit_items WHERE doctrine_fit_id = ?', [$fitId]);

    if ($items === []) {
        return;
    }

    $rows = [];
    foreach ($items as $item) {
        $rows[] = [
            'doctrine_fit_id' => $fitId,
            'line_number' => (int) ($item['line_number'] ?? 0),
            'slot_category' => (string) ($item['slot_category'] ?? 'Items'),
            'source_role' => (string) ($item['source_role'] ?? 'fit'),
            'item_name' => (string) ($item['item_name'] ?? ''),
            'type_id' => isset($item['type_id']) ? (int) $item['type_id'] : null,
            'quantity' => max(1, (int) ($item['quantity'] ?? 1)),
            'is_stock_tracked' => !array_key_exists('is_stock_tracked', $item) || (bool) $item['is_stock_tracked'] ? 1 : 0,
            'resolution_source' => (string) ($item['resolution_source'] ?? 'ref'),
        ];
    }

    db_bulk_insert_or_upsert(
        'doctrine_fit_items',
        ['doctrine_fit_id', 'line_number', 'slot_category', 'source_role', 'item_name', 'type_id', 'quantity', 'is_stock_tracked', 'resolution_source'],
        $rows
    );
}

function db_doctrine_fit_sync_item_totals(int $fitId, int $itemCount, int $unresolvedCount): bool
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return false;
    }

    return db_execute(
        'UPDATE doctrine_fits SET item_count = ?, unresolved_count = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
        [max(0, $itemCount), max(0, $unresolvedCount), $fitId]
    );
}

function db_doctrine_fit_replace_groups(int $fitId, array $memberships): void
{
    $memberships = db_doctrine_fit_memberships_normalize($memberships);
    db_execute('DELETE FROM doctrine_fit_groups WHERE doctrine_fit_id = ?', [$fitId]);

    if ($memberships !== []) {
        $rows = [];
        foreach ($memberships as $membership) {
            $rows[] = [
                'doctrine_fit_id' => $fitId,
                'doctrine_group_id' => (int) ($membership['doctrine_group_id'] ?? 0),
                'membership_role' => db_doctrine_membership_role((string) ($membership['membership_role'] ?? 'support')),
            ];
        }
        db_bulk_insert_or_upsert('doctrine_fit_groups', ['doctrine_fit_id', 'doctrine_group_id', 'membership_role'], $rows);
    }

    $primaryGroupId = null;
    foreach ($memberships as $membership) {
        if (($membership['membership_role'] ?? 'support') === 'primary') {
            $primaryGroupId = (int) ($membership['doctrine_group_id'] ?? 0);
            break;
        }
    }

    db_execute(
        'UPDATE doctrine_fits SET doctrine_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
        [$primaryGroupId > 0 ? $primaryGroupId : null, $fitId]
    );
}

function db_doctrine_fit_create(array $fit, array $items, array $memberships = []): int
{
    doctrine_db_ensure_schema();

    return db_transaction(function () use ($fit, $items, $memberships): int {
        $memberships = db_doctrine_fit_memberships_normalize($memberships);
        $primaryGroupId = null;
        foreach ($memberships as $membership) {
            if (($membership['membership_role'] ?? 'support') === 'primary') {
                $primaryGroupId = (int) ($membership['doctrine_group_id'] ?? 0);
                break;
            }
        }
        db_execute(
            'INSERT INTO doctrine_fits (
                doctrine_group_id,
                fit_name,
                ship_name,
                ship_type_id,
                target_fleet_size_override,
                source_type,
                source_format,
                source_reference,
                notes,
                import_body,
                raw_html,
                raw_buyall,
                raw_eft,
                metadata_json,
                parse_warnings_json,
                parse_status,
                review_status,
                conflict_state,
                fingerprint_hash,
                warning_count,
                item_count,
                unresolved_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                $primaryGroupId > 0 ? $primaryGroupId : (isset($fit['doctrine_group_id']) ? (int) $fit['doctrine_group_id'] : null),
                (string) ($fit['fit_name'] ?? ''),
                (string) ($fit['ship_name'] ?? ''),
                isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null,
                isset($fit['target_fleet_size_override']) && (int) ($fit['target_fleet_size_override'] ?? 0) > 0 ? (int) $fit['target_fleet_size_override'] : null,
                (string) ($fit['source_type'] ?? 'manual'),
                (string) ($fit['source_format'] ?? 'buyall'),
                ($fit['source_reference'] ?? null) !== null ? (string) $fit['source_reference'] : null,
                ($fit['notes'] ?? null) !== null ? (string) $fit['notes'] : null,
                (string) ($fit['import_body'] ?? ''),
                ($fit['raw_html'] ?? null) !== null ? (string) $fit['raw_html'] : null,
                ($fit['raw_buyall'] ?? null) !== null ? (string) $fit['raw_buyall'] : null,
                ($fit['raw_eft'] ?? null) !== null ? (string) $fit['raw_eft'] : null,
                ($fit['metadata_json'] ?? null) !== null ? (string) $fit['metadata_json'] : null,
                ($fit['parse_warnings_json'] ?? null) !== null ? (string) $fit['parse_warnings_json'] : null,
                (string) ($fit['parse_status'] ?? 'ready'),
                (string) ($fit['review_status'] ?? 'clean'),
                (string) ($fit['conflict_state'] ?? 'none'),
                ($fit['fingerprint_hash'] ?? null) !== null ? (string) $fit['fingerprint_hash'] : null,
                max(0, (int) ($fit['warning_count'] ?? 0)),
                (int) ($fit['item_count'] ?? 0),
                (int) ($fit['unresolved_count'] ?? 0),
            ]
        );

        $fitId = (int) db()->lastInsertId();
        db_doctrine_fit_replace_items($fitId, $items);
        db_doctrine_fit_replace_groups($fitId, $memberships);

        return $fitId;
    });
}

function db_doctrine_fit_update(int $fitId, array $fit, array $items, array $memberships = []): bool
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return false;
    }

    return db_transaction(function () use ($fitId, $fit, $items, $memberships): bool {
        $memberships = db_doctrine_fit_memberships_normalize($memberships);
        $primaryGroupId = null;
        foreach ($memberships as $membership) {
            if (($membership['membership_role'] ?? 'support') === 'primary') {
                $primaryGroupId = (int) ($membership['doctrine_group_id'] ?? 0);
                break;
            }
        }
        db_execute(
            'UPDATE doctrine_fits
             SET doctrine_group_id = ?, fit_name = ?, ship_name = ?, ship_type_id = ?, target_fleet_size_override = ?, source_type = ?, source_format = ?, source_reference = ?, notes = ?, import_body = ?, raw_html = ?, raw_buyall = ?, raw_eft = ?, metadata_json = ?, parse_warnings_json = ?, parse_status = ?, review_status = ?, conflict_state = ?, fingerprint_hash = ?, warning_count = ?, item_count = ?, unresolved_count = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ? LIMIT 1',
            [
                $primaryGroupId > 0 ? $primaryGroupId : null,
                (string) ($fit['fit_name'] ?? ''),
                (string) ($fit['ship_name'] ?? ''),
                isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null,
                isset($fit['target_fleet_size_override']) && (int) ($fit['target_fleet_size_override'] ?? 0) > 0 ? (int) $fit['target_fleet_size_override'] : null,
                (string) ($fit['source_type'] ?? 'manual'),
                (string) ($fit['source_format'] ?? 'buyall'),
                ($fit['source_reference'] ?? null) !== null ? (string) $fit['source_reference'] : null,
                ($fit['notes'] ?? null) !== null ? (string) $fit['notes'] : null,
                (string) ($fit['import_body'] ?? ''),
                ($fit['raw_html'] ?? null) !== null ? (string) $fit['raw_html'] : null,
                ($fit['raw_buyall'] ?? null) !== null ? (string) $fit['raw_buyall'] : null,
                ($fit['raw_eft'] ?? null) !== null ? (string) $fit['raw_eft'] : null,
                ($fit['metadata_json'] ?? null) !== null ? (string) $fit['metadata_json'] : null,
                ($fit['parse_warnings_json'] ?? null) !== null ? (string) $fit['parse_warnings_json'] : null,
                (string) ($fit['parse_status'] ?? 'ready'),
                (string) ($fit['review_status'] ?? 'clean'),
                (string) ($fit['conflict_state'] ?? 'none'),
                ($fit['fingerprint_hash'] ?? null) !== null ? (string) $fit['fingerprint_hash'] : null,
                max(0, (int) ($fit['warning_count'] ?? 0)),
                (int) ($fit['item_count'] ?? 0),
                (int) ($fit['unresolved_count'] ?? 0),
                $fitId,
            ]
        );

        db_doctrine_fit_replace_items($fitId, $items);
        db_doctrine_fit_replace_groups($fitId, $memberships);

        return true;
    });
}

function db_doctrine_fit_delete(int $fitId): bool
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return false;
    }

    return db_transaction(static function () use ($fitId): bool {
        db_execute('DELETE FROM doctrine_fit_groups WHERE doctrine_fit_id = ?', [$fitId]);
        db_execute('DELETE FROM doctrine_fit_items WHERE doctrine_fit_id = ?', [$fitId]);

        return db_execute('DELETE FROM doctrine_fits WHERE id = ? LIMIT 1', [$fitId]);
    });
}

function db_doctrine_fits_change_signal(): array
{
    doctrine_db_ensure_schema();

    $row = db_select(
        'SELECT COUNT(*) AS fit_count,
                MAX(updated_at) AS last_updated_at,
                COALESCE(SUM(item_count), 0) AS total_item_count,
                COALESCE(SUM(unresolved_count), 0) AS unresolved_count,
                COALESCE(SUM(CRC32(COALESCE(fingerprint_hash, CONCAT(id, ":", updated_at)))), 0) AS fingerprint_crc_sum
         FROM doctrine_fits'
    )[0] ?? [];

    return [
        'fit_count' => (int) ($row['fit_count'] ?? 0),
        'last_updated_at' => $row['last_updated_at'] ?? null,
        'total_item_count' => (int) ($row['total_item_count'] ?? 0),
        'unresolved_count' => (int) ($row['unresolved_count'] ?? 0),
        'fingerprint_crc_sum' => (string) ($row['fingerprint_crc_sum'] ?? '0'),
    ];
}

function db_doctrine_fit_conflicts(string $fitName, ?int $shipTypeId = null, string $shipName = '', ?string $fingerprintHash = null, int $excludeFitId = 0): array
{
    doctrine_db_ensure_schema();

    $conditions = ['df.id <> ?'];
    $params = [$excludeFitId];

    $name = trim($fitName);
    $shipName = trim($shipName);
    $fingerprintHash = trim((string) $fingerprintHash);

    $conflictParts = [];
    if ($name !== '' && ($shipTypeId ?? 0) > 0) {
        $conflictParts[] = '(df.fit_name = ? AND df.ship_type_id = ?)';
        $params[] = $name;
        $params[] = $shipTypeId;
    } elseif ($name !== '' && $shipName !== '') {
        $conflictParts[] = '(df.fit_name = ? AND df.ship_name = ?)';
        $params[] = $name;
        $params[] = $shipName;
    }

    if ($fingerprintHash !== '') {
        if (($shipTypeId ?? 0) > 0) {
            $conflictParts[] = '(df.fingerprint_hash = ? AND df.ship_type_id = ?)';
            $params[] = $fingerprintHash;
            $params[] = $shipTypeId;
        } elseif ($shipName !== '') {
            $conflictParts[] = '(df.fingerprint_hash = ? AND df.ship_name = ?)';
            $params[] = $fingerprintHash;
            $params[] = $shipName;
        } else {
            $conflictParts[] = 'df.fingerprint_hash = ?';
            $params[] = $fingerprintHash;
        }
    }

    if ($conflictParts === []) {
        return [];
    }

    $conditions[] = '(' . implode(' OR ', $conflictParts) . ')';

    return db_select(
        'SELECT
            df.id,
            df.fit_name,
            df.ship_name,
            df.ship_type_id,
            df.source_type,
            df.source_format,
            df.source_reference,
            df.parse_status,
            df.review_status,
            df.conflict_state,
            df.fingerprint_hash,
            df.updated_at,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR "||") AS group_names_csv
         FROM doctrine_fits df
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
         LEFT JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         WHERE ' . implode(' AND ', $conditions) . '
         GROUP BY df.id, df.fit_name, df.ship_name, df.ship_type_id, df.source_type, df.source_format, df.source_reference, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.updated_at
         ORDER BY df.updated_at DESC, df.id DESC',
        $params
    );
}

function db_doctrine_fit_overview(array $filters = [], string $sort = 'updated_desc'): array
{
    doctrine_db_ensure_schema();

    $joins = [
        'LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id',
        'LEFT JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id',
        'LEFT JOIN doctrine_fit_items dfi ON dfi.doctrine_fit_id = df.id',
    ];
    $where = ['1 = 1'];
    $params = [];

    $search = trim((string) ($filters['search'] ?? ''));
    if ($search !== '') {
        $where[] = '(df.fit_name LIKE ? OR df.ship_name LIKE ? OR df.notes LIKE ? OR df.source_reference LIKE ?)';
        $like = '%' . $search . '%';
        array_push($params, $like, $like, $like, $like);
    }

    $groupId = (int) ($filters['group_id'] ?? 0);
    if ($groupId > 0) {
        $where[] = 'EXISTS (SELECT 1 FROM doctrine_fit_groups x WHERE x.doctrine_fit_id = df.id AND x.doctrine_group_id = ?)';
        $params[] = $groupId;
    }

    $sourceType = trim((string) ($filters['source_type'] ?? ''));
    if ($sourceType !== '') {
        $where[] = 'df.source_type = ?';
        $params[] = $sourceType;
    }

    $parseStatus = trim((string) ($filters['parse_status'] ?? ''));
    if ($parseStatus !== '') {
        $where[] = 'df.parse_status = ?';
        $params[] = $parseStatus;
    }

    $reviewStatus = trim((string) ($filters['review_status'] ?? ''));
    if ($reviewStatus !== '') {
        $where[] = 'df.review_status = ?';
        $params[] = $reviewStatus;
    }

    $conflictState = trim((string) ($filters['conflict_state'] ?? ''));
    if ($conflictState !== '') {
        if ($conflictState === 'has_conflict') {
            $where[] = "df.conflict_state <> 'none'";
        } else {
            $where[] = 'df.conflict_state = ?';
            $params[] = $conflictState;
        }
    }

    if (!empty($filters['unresolved_only'])) {
        $where[] = 'df.unresolved_count > 0';
    }

    $hullSearch = trim((string) ($filters['hull'] ?? ''));
    if ($hullSearch !== '') {
        $where[] = 'df.ship_name LIKE ?';
        $params[] = '%' . $hullSearch . '%';
    }

    $sortSql = match ($sort) {
        'fit_name_asc' => 'df.fit_name ASC, df.updated_at DESC',
        'fit_name_desc' => 'df.fit_name DESC, df.updated_at DESC',
        'hull_asc' => 'df.ship_name ASC, df.fit_name ASC',
        'groups_desc' => 'group_count DESC, df.updated_at DESC',
        'items_desc' => 'df.item_count DESC, df.updated_at DESC',
        'warnings_desc' => 'df.warning_count DESC, df.unresolved_count DESC, df.updated_at DESC',
        'status_asc' => 'df.parse_status ASC, df.review_status ASC, df.updated_at DESC',
        default => 'df.updated_at DESC, df.fit_name ASC',
    };

    return db_select(
        'SELECT
            df.id,
            df.doctrine_group_id,
            df.fit_name,
            df.ship_name,
            df.ship_type_id,
            df.source_type,
            df.source_format,
            df.source_reference,
            df.notes,
            df.parse_status,
            df.review_status,
            df.conflict_state,
            df.fingerprint_hash,
            df.warning_count,
            df.item_count,
            df.unresolved_count,
            df.created_at,
            df.updated_at,
            MAX(CASE WHEN dfg.membership_role = \'primary\' THEN dfg.doctrine_group_id END) AS primary_group_id,
            COUNT(DISTINCT dfg.doctrine_group_id) AS group_count,
            COUNT(DISTINCT dfi.id) AS normalized_item_rows,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR "||") AS group_names_csv,
            GROUP_CONCAT(DISTINCT CONCAT(dfg.doctrine_group_id, \':\', dfg.membership_role) ORDER BY CASE dfg.membership_role WHEN \'primary\' THEN 0 WHEN \'support\' THEN 1 ELSE 2 END, dfg.doctrine_group_id SEPARATOR \',\') AS membership_roles_csv
         FROM doctrine_fits df
         ' . implode(' ', $joins) . '
         WHERE ' . implode(' AND ', $where) . '
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.source_type, df.source_format, df.source_reference, df.notes, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at
         ORDER BY ' . $sortSql
        ,
        $params
    );
}

function db_doctrine_fit_bulk_delete(array $fitIds): int
{
    doctrine_db_ensure_schema();

    $fitIds = array_values(array_unique(array_filter(array_map('intval', $fitIds), static fn (int $id): bool => $id > 0)));
    if ($fitIds === []) {
        return 0;
    }

    return db_transaction(static function () use ($fitIds): int {
        $placeholders = implode(', ', array_fill(0, count($fitIds), '?'));
        db_execute("DELETE FROM doctrine_fit_groups WHERE doctrine_fit_id IN ({$placeholders})", $fitIds);
        db_execute("DELETE FROM doctrine_fit_items WHERE doctrine_fit_id IN ({$placeholders})", $fitIds);
        db_execute("DELETE FROM doctrine_fits WHERE id IN ({$placeholders})", $fitIds);

        return count($fitIds);
    });
}

function db_doctrine_fit_bulk_assign_groups(array $fitIds, array $groupIds, bool $replaceExisting = false): int
{
    doctrine_db_ensure_schema();

    $fitIds = array_values(array_unique(array_filter(array_map('intval', $fitIds), static fn (int $id): bool => $id > 0)));
    $groupIds = array_values(array_unique(array_filter(array_map('intval', $groupIds), static fn (int $id): bool => $id > 0)));

    if ($fitIds === [] || $groupIds === []) {
        return 0;
    }

    return db_transaction(static function () use ($fitIds, $groupIds, $replaceExisting): int {
        $fitPlaceholders = implode(', ', array_fill(0, count($fitIds), '?'));

        if ($replaceExisting) {
            db_execute("DELETE FROM doctrine_fit_groups WHERE doctrine_fit_id IN ({$fitPlaceholders})", $fitIds);
        }

        $rows = [];
        foreach ($fitIds as $fitId) {
            $existingMemberships = $replaceExisting ? [] : db_doctrine_fit_membership_rows($fitId);
            $hasPrimary = false;
            foreach ($existingMemberships as $membership) {
                if (($membership['membership_role'] ?? 'support') === 'primary') {
                    $hasPrimary = true;
                    break;
                }
            }

            foreach (array_values($groupIds) as $index => $groupId) {
                $role = ($index === 0 && !$hasPrimary) ? 'primary' : 'support';
                $rows[] = [
                    'doctrine_fit_id' => $fitId,
                    'doctrine_group_id' => $groupId,
                    'membership_role' => $role,
                ];
                if ($role === 'primary') {
                    $hasPrimary = true;
                }
            }
        }

        db_bulk_insert_or_upsert('doctrine_fit_groups', ['doctrine_fit_id', 'doctrine_group_id', 'membership_role'], $rows);

        foreach ($fitIds as $fitId) {
            $primary = db_select_one(
                "SELECT doctrine_group_id
                 FROM doctrine_fit_groups
                 WHERE doctrine_fit_id = ? AND membership_role = 'primary'
                 LIMIT 1",
                [$fitId]
            );
            db_execute(
                'UPDATE doctrine_fits SET doctrine_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
                [$primary !== null ? (int) ($primary['doctrine_group_id'] ?? 0) : null, $fitId]
            );
        }

        return count($fitIds);
    });
}

function db_doctrine_fit_bulk_remove_groups(array $fitIds, array $groupIds): int
{
    doctrine_db_ensure_schema();

    $fitIds = array_values(array_unique(array_filter(array_map('intval', $fitIds), static fn (int $id): bool => $id > 0)));
    $groupIds = array_values(array_unique(array_filter(array_map('intval', $groupIds), static fn (int $id): bool => $id > 0)));

    if ($fitIds === [] || $groupIds === []) {
        return 0;
    }

    return db_transaction(static function () use ($fitIds, $groupIds): int {
        $fitPlaceholders = implode(', ', array_fill(0, count($fitIds), '?'));
        $groupPlaceholders = implode(', ', array_fill(0, count($groupIds), '?'));
        $params = array_merge($fitIds, $groupIds);

        db_execute(
            "DELETE FROM doctrine_fit_groups
             WHERE doctrine_fit_id IN ({$fitPlaceholders})
               AND doctrine_group_id IN ({$groupPlaceholders})",
            $params
        );

        foreach ($fitIds as $fitId) {
            $primary = db_select_one(
                "SELECT doctrine_group_id
                 FROM doctrine_fit_groups
                 WHERE doctrine_fit_id = ? AND membership_role = 'primary'
                 LIMIT 1",
                [$fitId]
            );
            db_execute(
                'UPDATE doctrine_fits SET doctrine_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
                [$primary !== null ? (int) ($primary['doctrine_group_id'] ?? 0) : null, $fitId]
            );
        }

        return count($fitIds);
    });
}

function db_doctrine_fit_bulk_update_metadata(array $fitIds, array $changes): int
{
    doctrine_db_ensure_schema();

    $fitIds = array_values(array_unique(array_filter(array_map('intval', $fitIds), static fn (int $id): bool => $id > 0)));
    if ($fitIds === []) {
        return 0;
    }

    $sets = [];
    $params = [];

    foreach (['notes', 'parse_status', 'review_status', 'conflict_state', 'source_reference'] as $column) {
        if (!array_key_exists($column, $changes)) {
            continue;
        }

        $sets[] = $column . ' = ?';
        $params[] = $changes[$column];
    }

    if ($sets === []) {
        return 0;
    }

    $placeholders = implode(', ', array_fill(0, count($fitIds), '?'));
    $params = array_merge($params, $fitIds);

    db_execute(
        'UPDATE doctrine_fits SET ' . implode(', ', $sets) . ', updated_at = CURRENT_TIMESTAMP WHERE id IN (' . $placeholders . ')',
        $params
    );

    return count($fitIds);
}

function db_doctrine_group_suggestions_for_fit(string $fitName, ?int $shipTypeId = null, int $excludeFitId = 0): array
{
    doctrine_db_ensure_schema();

    return db_select(
        'SELECT DISTINCT dg.id, dg.group_name
         FROM doctrine_groups dg
         INNER JOIN doctrine_fit_groups dfg ON dfg.doctrine_group_id = dg.id
         INNER JOIN doctrine_fits df ON df.id = dfg.doctrine_fit_id
         WHERE df.id <> ?
           AND ((? <> "" AND df.fit_name = ?) OR (? > 0 AND df.ship_type_id = ?))
         ORDER BY dg.group_name ASC',
        [$excludeFitId, trim($fitName), trim($fitName), $shipTypeId ?? 0, $shipTypeId ?? 0]
    );
}


function db_killmail_tracked_recent_hull_activity_windows(array $hullTypeIds, int $hours = 24 * 7): array
{
    $rows = db_killmail_hull_loss_window_summaries($hullTypeIds, $hours);
    if ($rows !== []) {
        return $rows;
    }

    $safeHours = max(24, min(24 * 30, $hours));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $hullTypeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $trackedMatchesSql = db_killmail_tracked_matches_sql("(UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)");

    return db_select(
        "SELECT
            e.victim_ship_type_id AS type_id,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN 1 ELSE 0 END) AS losses_24h,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 3 DAY) THEN 1 ELSE 0 END) AS losses_3d,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN 1 ELSE 0 END) AS losses_7d,
            COUNT(*) AS losses_window,
            MAX(e.effective_killmail_at) AS latest_loss_at
         FROM {$trackedMatchesSql} tracked
         INNER JOIN killmail_events e ON e.sequence_id = tracked.sequence_id
         WHERE e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)
           AND e.victim_ship_type_id IN ({$placeholders})
         GROUP BY e.victim_ship_type_id",
        $normalizedTypeIds
    );
}

function db_killmail_tracked_recent_item_activity_windows(array $typeIds, int $hours = 24 * 7): array
{
    $rows = db_killmail_item_loss_window_summaries($typeIds, $hours);
    if ($rows !== []) {
        return $rows;
    }

    $safeHours = max(24, min(24 * 30, $hours));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $trackedMatchesSql = db_killmail_tracked_matches_sql("(UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)");
    $quantitySql = db_killmail_item_quantity_sql();

    return db_select(
        "SELECT
            i.item_type_id AS type_id,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN {$quantitySql} ELSE 0 END) AS quantity_24h,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 3 DAY) THEN {$quantitySql} ELSE 0 END) AS quantity_3d,
            SUM(CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN {$quantitySql} ELSE 0 END) AS quantity_7d,
            SUM({$quantitySql}) AS quantity_window,
            COUNT(DISTINCT CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN e.sequence_id END) AS losses_24h,
            COUNT(DISTINCT CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 3 DAY) THEN e.sequence_id END) AS losses_3d,
            COUNT(DISTINCT CASE WHEN e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN e.sequence_id END) AS losses_7d,
            COUNT(DISTINCT e.sequence_id) AS losses_window,
            MAX(e.effective_killmail_at) AS latest_loss_at
         FROM {$trackedMatchesSql} tracked
         INNER JOIN killmail_events e ON e.sequence_id = tracked.sequence_id
         INNER JOIN killmail_items i ON i.sequence_id = e.sequence_id
         WHERE e.effective_killmail_at >= (UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)
           AND i.item_type_id IN ({$placeholders})
         GROUP BY i.item_type_id",
        $normalizedTypeIds
    );
}

function db_doctrine_activity_latest_snapshots(string $entityType, array $entityIds): array
{
    doctrine_db_ensure_schema();

    $safeEntityType = trim($entityType);
    $normalizedIds = array_values(array_unique(array_filter(array_map('intval', $entityIds), static fn (int $id): bool => $id > 0)));
    if (!in_array($safeEntityType, ['fit', 'group'], true) || $normalizedIds === []) {
        return [];
    }

    $placeholders = implode(', ', array_fill(0, count($normalizedIds), '?'));
    $params = array_merge([$safeEntityType], $normalizedIds, [$safeEntityType], $normalizedIds);

    return db_select(
        "SELECT s.*
         FROM doctrine_activity_snapshots s
         INNER JOIN (
            SELECT entity_id, MAX(snapshot_time) AS snapshot_time
            FROM doctrine_activity_snapshots
            WHERE entity_type = ?
              AND entity_id IN ({$placeholders})
            GROUP BY entity_id
         ) latest
           ON latest.entity_id = s.entity_id
          AND latest.snapshot_time = s.snapshot_time
         WHERE s.entity_type = ?
           AND s.entity_id IN ({$placeholders})",
        $params
    );
}

function db_item_priority_latest_snapshots(array $typeIds): array
{
    doctrine_db_ensure_schema();

    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(', ', array_fill(0, count($normalizedTypeIds), '?'));

    return db_select(
        "SELECT s.*
         FROM item_priority_snapshots s
         INNER JOIN (
            SELECT type_id, MAX(snapshot_time) AS snapshot_time
            FROM item_priority_snapshots
            WHERE type_id IN ({$placeholders})
            GROUP BY type_id
         ) latest
           ON latest.type_id = s.type_id
          AND latest.snapshot_time = s.snapshot_time
         WHERE s.type_id IN ({$placeholders})",
        array_merge($normalizedTypeIds, $normalizedTypeIds)
    );
}

function db_doctrine_activity_snapshot_bulk_insert(array $rows, ?int $chunkSize = null): int
{
    doctrine_db_ensure_schema();

    return db_bulk_insert_or_upsert(
        'doctrine_activity_snapshots',
        [
            'entity_type',
            'entity_id',
            'entity_name',
            'snapshot_time',
            'rank_position',
            'previous_rank_position',
            'rank_delta',
            'activity_score',
            'activity_level',
            'hull_losses_24h',
            'hull_losses_3d',
            'hull_losses_7d',
            'module_losses_24h',
            'module_losses_3d',
            'module_losses_7d',
            'fit_equivalent_losses_24h',
            'fit_equivalent_losses_3d',
            'fit_equivalent_losses_7d',
            'readiness_state',
            'resupply_pressure_state',
            'resupply_pressure',
            'readiness_gap_count',
            'resupply_gap_isk',
            'score_components_json',
            'explanation_text',
        ],
        $rows,
        [
            'entity_name',
            'rank_position',
            'previous_rank_position',
            'rank_delta',
            'activity_score',
            'activity_level',
            'hull_losses_24h',
            'hull_losses_3d',
            'hull_losses_7d',
            'module_losses_24h',
            'module_losses_3d',
            'module_losses_7d',
            'fit_equivalent_losses_24h',
            'fit_equivalent_losses_3d',
            'fit_equivalent_losses_7d',
            'readiness_state',
            'resupply_pressure_state',
            'resupply_pressure',
            'readiness_gap_count',
            'resupply_gap_isk',
            'score_components_json',
            'explanation_text',
        ],
        $chunkSize
    );
}

function db_item_priority_snapshot_bulk_insert(array $rows, ?int $chunkSize = null): int
{
    doctrine_db_ensure_schema();

    return db_bulk_insert_or_upsert(
        'item_priority_snapshots',
        [
            'type_id',
            'item_name',
            'snapshot_time',
            'rank_position',
            'previous_rank_position',
            'rank_delta',
            'priority_score',
            'priority_band',
            'is_doctrine_linked',
            'linked_doctrine_count',
            'linked_active_doctrine_count',
            'local_available_qty',
            'local_sell_orders',
            'local_sell_volume',
            'recent_loss_qty_24h',
            'recent_loss_qty_3d',
            'recent_loss_qty_7d',
            'recent_loss_events_24h',
            'recent_loss_events_3d',
            'recent_loss_events_7d',
            'readiness_gap_fit_count',
            'bottleneck_fit_count',
            'depletion_state',
            'score_components_json',
            'linked_doctrines_json',
            'explanation_text',
        ],
        $rows,
        [
            'item_name',
            'rank_position',
            'previous_rank_position',
            'rank_delta',
            'priority_score',
            'priority_band',
            'is_doctrine_linked',
            'linked_doctrine_count',
            'linked_active_doctrine_count',
            'local_available_qty',
            'local_sell_orders',
            'local_sell_volume',
            'recent_loss_qty_24h',
            'recent_loss_qty_3d',
            'recent_loss_qty_7d',
            'recent_loss_events_24h',
            'recent_loss_events_3d',
            'recent_loss_events_7d',
            'readiness_gap_fit_count',
            'bottleneck_fit_count',
            'depletion_state',
            'score_components_json',
            'linked_doctrines_json',
            'explanation_text',
        ],
        $chunkSize
    );
}

function db_worker_jobs_ensure_schema(): void
{
    db_execute("CREATE TABLE IF NOT EXISTS worker_jobs (
        id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        job_key VARCHAR(190) NOT NULL,
        queue_name VARCHAR(40) NOT NULL DEFAULT 'default',
        workload_class ENUM('sync', 'compute', 'stream') NOT NULL DEFAULT 'sync',
        execution_mode ENUM('python', 'php') NOT NULL DEFAULT 'python',
        priority VARCHAR(20) NOT NULL DEFAULT 'normal',
        status ENUM('queued', 'running', 'retry', 'completed', 'failed', 'dead') NOT NULL DEFAULT 'queued',
        unique_key VARCHAR(190) DEFAULT NULL,
        payload_json LONGTEXT DEFAULT NULL,
        available_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        locked_at DATETIME DEFAULT NULL,
        lock_expires_at DATETIME DEFAULT NULL,
        locked_by VARCHAR(190) DEFAULT NULL,
        heartbeat_at DATETIME DEFAULT NULL,
        attempts INT UNSIGNED NOT NULL DEFAULT 0,
        max_attempts INT UNSIGNED NOT NULL DEFAULT 5,
        timeout_seconds INT UNSIGNED NOT NULL DEFAULT 300,
        retry_delay_seconds INT UNSIGNED NOT NULL DEFAULT 30,
        memory_limit_mb INT UNSIGNED NOT NULL DEFAULT 512,
        last_error VARCHAR(500) DEFAULT NULL,
        last_result_json LONGTEXT DEFAULT NULL,
        last_started_at DATETIME DEFAULT NULL,
        last_finished_at DATETIME DEFAULT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_worker_job_unique_key (unique_key),
        KEY idx_worker_jobs_fetch (queue_name, workload_class, status, available_at, priority, id),
        KEY idx_worker_jobs_lock (status, lock_expires_at, locked_by),
        KEY idx_worker_jobs_job_key (job_key, status, available_at),
        KEY idx_worker_jobs_created (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
}

function db_worker_job_definitions(): array
{
    static $definitions = null;
    if (is_array($definitions)) {
        return $definitions;
    }

    $definitions = function_exists('worker_job_registry_definitions') ? worker_job_registry_definitions() : [];

    return $definitions;
}

function db_worker_job_definition(string $jobKey): array
{
    $definitions = db_worker_job_definitions();

    return is_array($definitions[$jobKey] ?? null) ? $definitions[$jobKey] : [];
}

function db_worker_job_queue_due_recurring_jobs(array $jobKeys = []): array
{
    db_worker_jobs_ensure_schema();

    $definitions = db_worker_job_definitions();
    if ($jobKeys !== []) {
        $definitions = array_intersect_key($definitions, array_fill_keys(array_map('strval', $jobKeys), true));
    }

    $queued = 0;
    $skipped = 0;
    $now = gmdate('Y-m-d H:i:s');

    foreach ($definitions as $jobKey => $definition) {
        $existing = db_select_one(
            'SELECT id FROM worker_jobs WHERE job_key = ? AND status IN (\'queued\', \'running\', \'retry\') LIMIT 1',
            [$jobKey]
        );
        if (is_array($existing)) {
            $skipped++;
            continue;
        }

        $intervalSeconds = max(60, (int) ($definition['interval_seconds'] ?? 300));
        $lastFinished = db_select_one(
            'SELECT MAX(last_finished_at) AS last_finished_at FROM worker_jobs WHERE job_key = ?',
            [$jobKey]
        );
        $lastFinishedAt = trim((string) ($lastFinished['last_finished_at'] ?? ''));
        if ($lastFinishedAt !== '') {
            $nextEligibleAt = gmdate('Y-m-d H:i:s', strtotime($lastFinishedAt . ' UTC') + $intervalSeconds);
            if ($nextEligibleAt > $now) {
                $skipped++;
                continue;
            }
        }

        $payload = [
            'recurring' => true,
            'interval_seconds' => $intervalSeconds,
            'queued_at' => gmdate(DATE_ATOM),
        ];
        $uniqueKey = 'recurring:' . $jobKey;
        db_execute(
            'INSERT INTO worker_jobs (
                job_key, queue_name, workload_class, execution_mode, priority, status, unique_key,
                payload_json, available_at, max_attempts, timeout_seconds, retry_delay_seconds, memory_limit_mb
             ) VALUES (?, ?, ?, ?, ?, \'queued\', ?, ?, UTC_TIMESTAMP(), ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                queue_name = VALUES(queue_name),
                workload_class = VALUES(workload_class),
                execution_mode = VALUES(execution_mode),
                priority = VALUES(priority),
                payload_json = VALUES(payload_json),
                available_at = CASE
                    WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN VALUES(available_at)
                    ELSE worker_jobs.available_at
                END,
                status = CASE
                    WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN \'queued\'
                    ELSE worker_jobs.status
                END,
                last_error = CASE
                    WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN NULL
                    ELSE worker_jobs.last_error
                END,
                locked_at = CASE WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN NULL ELSE worker_jobs.locked_at END,
                lock_expires_at = CASE WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN NULL ELSE worker_jobs.lock_expires_at END,
                locked_by = CASE WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN NULL ELSE worker_jobs.locked_by END,
                heartbeat_at = CASE WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN NULL ELSE worker_jobs.heartbeat_at END,
                attempts = CASE WHEN worker_jobs.status IN (\'completed\', \'failed\', \'dead\') THEN 0 ELSE worker_jobs.attempts END,
                updated_at = CURRENT_TIMESTAMP',
            [
                $jobKey,
                (string) ($definition['queue_name'] ?? 'default'),
                (string) ($definition['workload_class'] ?? 'sync'),
                strtolower((string) ($definition['execution_mode'] ?? 'python')) === 'php' ? 'php' : 'python',
                (string) ($definition['priority'] ?? 'normal'),
                $uniqueKey,
                json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
                max(1, (int) ($definition['max_attempts'] ?? 5)),
                max(30, (int) ($definition['timeout_seconds'] ?? 300)),
                max(5, (int) ($definition['retry_delay_seconds'] ?? 30)),
                max(128, (int) ($definition['memory_limit_mb'] ?? 512)),
            ]
        );
        $queued++;
    }

    return ['queued' => $queued, 'skipped' => $skipped, 'job_count' => count($definitions)];
}

function db_worker_job_force_available_by_job_keys(array $jobKeys): int
{
    db_worker_jobs_ensure_schema();

    $definitions = db_worker_job_definitions();
    $normalizedKeys = array_values(array_unique(array_filter(
        array_map(static fn ($value): string => trim((string) $value), $jobKeys),
        static fn (string $jobKey): bool => $jobKey !== '' && isset($definitions[$jobKey])
    )));
    if ($normalizedKeys === []) {
        return 0;
    }

    $forced = 0;
    foreach ($normalizedKeys as $jobKey) {
        $definition = (array) ($definitions[$jobKey] ?? []);
        $payload = [
            'recurring' => true,
            'interval_seconds' => max(60, (int) ($definition['interval_seconds'] ?? 300)),
            'forced_at' => gmdate(DATE_ATOM),
        ];
        $uniqueKey = 'recurring:' . $jobKey;
        db_execute(
            'INSERT INTO worker_jobs (
                job_key, queue_name, workload_class, execution_mode, priority, status, unique_key,
                payload_json, available_at, max_attempts, timeout_seconds, retry_delay_seconds, memory_limit_mb
             ) VALUES (?, ?, ?, ?, ?, \'queued\', ?, ?, UTC_TIMESTAMP(), ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE
                queue_name = VALUES(queue_name),
                workload_class = VALUES(workload_class),
                execution_mode = VALUES(execution_mode),
                priority = VALUES(priority),
                payload_json = VALUES(payload_json),
                available_at = CASE
                    WHEN worker_jobs.status = \'running\' THEN worker_jobs.available_at
                    ELSE UTC_TIMESTAMP()
                END,
                status = CASE
                    WHEN worker_jobs.status = \'running\' THEN worker_jobs.status
                    ELSE \'queued\'
                END,
                last_error = CASE WHEN worker_jobs.status = \'running\' THEN worker_jobs.last_error ELSE NULL END,
                locked_at = CASE WHEN worker_jobs.status = \'running\' THEN worker_jobs.locked_at ELSE NULL END,
                lock_expires_at = CASE WHEN worker_jobs.status = \'running\' THEN worker_jobs.lock_expires_at ELSE NULL END,
                locked_by = CASE WHEN worker_jobs.status = \'running\' THEN worker_jobs.locked_by ELSE NULL END,
                heartbeat_at = CASE WHEN worker_jobs.status = \'running\' THEN worker_jobs.heartbeat_at ELSE NULL END,
                attempts = CASE WHEN worker_jobs.status = \'running\' THEN worker_jobs.attempts ELSE 0 END,
                updated_at = CURRENT_TIMESTAMP',
            [
                $jobKey,
                (string) ($definition['queue_name'] ?? 'default'),
                (string) ($definition['workload_class'] ?? 'sync'),
                strtolower((string) ($definition['execution_mode'] ?? 'python')) === 'php' ? 'php' : 'python',
                (string) ($definition['priority'] ?? 'normal'),
                $uniqueKey,
                json_encode($payload, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
                max(1, (int) ($definition['max_attempts'] ?? 5)),
                max(30, (int) ($definition['timeout_seconds'] ?? 300)),
                max(5, (int) ($definition['retry_delay_seconds'] ?? 30)),
                max(128, (int) ($definition['memory_limit_mb'] ?? 512)),
            ]
        );
        $forced++;
    }

    return $forced;
}

function db_worker_job_release_expired_claims(): int
{
    db_worker_jobs_ensure_schema();
    $stmt = db()->prepare(
        "UPDATE worker_jobs
         SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'retry' END,
             available_at = CASE
                WHEN attempts >= max_attempts THEN available_at
                ELSE DATE_ADD(UTC_TIMESTAMP(), INTERVAL retry_delay_seconds SECOND)
             END,
             locked_at = NULL,
             lock_expires_at = NULL,
             locked_by = NULL,
             heartbeat_at = NULL,
             last_error = COALESCE(last_error, 'Worker lease expired before completion.'),
             last_finished_at = UTC_TIMESTAMP(),
             updated_at = CURRENT_TIMESTAMP
         WHERE status = 'running'
           AND lock_expires_at IS NOT NULL
           AND lock_expires_at < UTC_TIMESTAMP()"
    );
    $stmt->execute();

    return (int) $stmt->rowCount();
}

function db_worker_job_claim_next(string $workerId, array $queues = [], array $workloadClasses = [], ?int $leaseSeconds = null): ?array
{
    db_worker_jobs_ensure_schema();
    db_worker_job_release_expired_claims();

    $safeWorkerId = mb_substr(trim($workerId), 0, 190);
    if ($safeWorkerId === '') {
        return null;
    }

    $leaseTtl = max(30, min(3600, (int) ($leaseSeconds ?? 300)));
    $conditions = ["status IN ('queued', 'retry')", 'available_at <= UTC_TIMESTAMP()'];
    $params = [];

    $queueValues = array_values(array_filter(array_map(static fn ($value): string => trim((string) $value), $queues), static fn (string $value): bool => $value !== ''));
    if ($queueValues !== []) {
        $conditions[] = 'queue_name IN (' . implode(',', array_fill(0, count($queueValues), '?')) . ')';
        array_push($params, ...$queueValues);
    }

    $classValues = array_values(array_filter(array_map(static fn ($value): string => trim((string) $value), $workloadClasses), static fn (string $value): bool => $value !== ''));
    if ($classValues !== []) {
        $conditions[] = 'workload_class IN (' . implode(',', array_fill(0, count($classValues), '?')) . ')';
        array_push($params, ...$classValues);
    }

    $sql = "SELECT id
            FROM worker_jobs
            WHERE " . implode(' AND ', $conditions) . "
            ORDER BY
                CASE priority
                    WHEN 'highest' THEN 400
                    WHEN 'high' THEN 300
                    WHEN 'medium' THEN 200
                    WHEN 'normal' THEN 100
                    ELSE 50
                END DESC,
                available_at ASC,
                id ASC
            LIMIT 1";
    $candidate = db_select_one($sql, $params);
    if (!is_array($candidate)) {
        return null;
    }

    $jobId = (int) ($candidate['id'] ?? 0);
    if ($jobId <= 0) {
        return null;
    }

    $claimed = db_execute(
        'UPDATE worker_jobs
         SET status = \'running\',
             attempts = attempts + 1,
             locked_at = UTC_TIMESTAMP(),
             lock_expires_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND),
             heartbeat_at = UTC_TIMESTAMP(),
             locked_by = ?,
             last_started_at = UTC_TIMESTAMP(),
             last_error = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
           AND status IN (\'queued\', \'retry\')
         LIMIT 1',
        [$leaseTtl, $safeWorkerId, $jobId]
    );

    if (!$claimed) {
        return null;
    }

    return db_select_one('SELECT * FROM worker_jobs WHERE id = ? LIMIT 1', [$jobId]);
}

function db_worker_job_heartbeat(int $jobId, string $workerId, ?int $leaseSeconds = null): bool
{
    db_worker_jobs_ensure_schema();
    if ($jobId <= 0) {
        return false;
    }

    $leaseTtl = max(30, min(3600, (int) ($leaseSeconds ?? 300)));

    return db_execute(
        'UPDATE worker_jobs
         SET heartbeat_at = UTC_TIMESTAMP(),
             lock_expires_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
           AND status = \'running\'
           AND locked_by = ?
         LIMIT 1',
        [$leaseTtl, $jobId, mb_substr(trim($workerId), 0, 190)]
    );
}

function db_worker_job_complete(int $jobId, string $workerId, array $result = []): ?array
{
    db_worker_jobs_ensure_schema();
    if ($jobId <= 0) {
        return null;
    }

    $status = trim((string) ($result['status'] ?? 'completed'));
    $safeStatus = in_array($status, ['completed', 'success', 'skipped'], true) ? 'completed' : 'completed';
    $summary = mb_substr(trim((string) ($result['summary'] ?? 'Worker job completed.')), 0, 500);

    db_execute(
        'UPDATE worker_jobs
         SET status = ?,
             locked_at = NULL,
             lock_expires_at = NULL,
             locked_by = NULL,
             heartbeat_at = NULL,
             last_error = NULL,
             last_result_json = ?,
             last_finished_at = UTC_TIMESTAMP(),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
           AND status = \'running\'
           AND locked_by = ?
         LIMIT 1',
        [$safeStatus, json_encode($result + ['summary' => $summary], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE), $jobId, mb_substr(trim($workerId), 0, 190)]
    );

    return db_select_one('SELECT * FROM worker_jobs WHERE id = ? LIMIT 1', [$jobId]);
}

function db_worker_job_retry(int $jobId, string $workerId, string $error, ?int $retryDelaySeconds = null, array $result = []): ?array
{
    db_worker_jobs_ensure_schema();
    if ($jobId <= 0) {
        return null;
    }

    $row = db_select_one('SELECT attempts, max_attempts, retry_delay_seconds FROM worker_jobs WHERE id = ? LIMIT 1', [$jobId]);
    if (!is_array($row)) {
        return null;
    }

    $attempts = (int) ($row['attempts'] ?? 0);
    $maxAttempts = max(1, (int) ($row['max_attempts'] ?? 1));
    $nextDelay = max(5, (int) ($retryDelaySeconds ?? (int) ($row['retry_delay_seconds'] ?? 30)));
    $nextStatus = $attempts >= $maxAttempts ? 'dead' : 'retry';

    db_execute(
        'UPDATE worker_jobs
         SET status = ?,
             available_at = CASE WHEN ? = \'retry\' THEN DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND) ELSE available_at END,
             locked_at = NULL,
             lock_expires_at = NULL,
             locked_by = NULL,
             heartbeat_at = NULL,
             last_error = ?,
             last_result_json = ?,
             last_finished_at = UTC_TIMESTAMP(),
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
           AND locked_by = ?
         LIMIT 1',
        [
            $nextStatus,
            $nextStatus,
            $nextDelay,
            mb_substr(trim($error), 0, 500),
            json_encode($result + ['error' => $error], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE),
            $jobId,
            mb_substr(trim($workerId), 0, 190),
        ]
    );

    return db_select_one('SELECT * FROM worker_jobs WHERE id = ? LIMIT 1', [$jobId]);
}
