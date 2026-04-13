<?php

declare(strict_types=1);

function config(string $key = null, mixed $default = null): mixed
{
    $config = supplycore_runtime_config();

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

function supplycore_base_config(): array
{
    static $config = null;
    if ($config !== null) {
        return $config;
    }

    $loaded = require __DIR__ . '/config/app.php';
    $config = is_array($loaded) ? $loaded : [];

    $localPath = __DIR__ . '/config/local.php';
    if (is_file($localPath)) {
        try {
            $local = (static fn(string $p): mixed => require $p)($localPath);
            if (is_array($local)) {
                $config = array_replace_recursive($config, $local);
            }
        } catch (\Throwable $e) {
            error_log('supplycore_base_config: failed to load local.php: ' . $e->getMessage());
        }
    }

    return $config;
}

function supplycore_base_config_value(string $key, mixed $default = null): mixed
{
    $segments = explode('.', $key);
    $value = supplycore_base_config();

    foreach ($segments as $segment) {
        if (!is_array($value) || !array_key_exists($segment, $value)) {
            return $default;
        }

        $value = $value[$segment];
    }

    return $value;
}

function supplycore_runtime_settings_registry(): array
{
    static $registry = null;
    if ($registry !== null) {
        return $registry;
    }

    $loaded = require __DIR__ . '/config/runtime_settings.php';
    $registry = is_array($loaded) ? $loaded : [];

    return $registry;
}

function supplycore_cast_runtime_config_value(mixed $value, string $type): mixed
{
    return match ($type) {
        'bool' => in_array(strtolower(trim((string) $value)), ['1', 'true', 'yes', 'on'], true),
        'int' => (int) $value,
        'float' => (float) $value,
        default => (string) $value,
    };
}

function &supplycore_runtime_config_cache_ref(): array
{
    static $cache = [
        'db_overrides_loaded' => false,
        'db_overrides' => [],
        'runtime_loaded' => false,
        'runtime' => [],
    ];

    return $cache;
}

function supplycore_runtime_config_cache_clear(): void
{
    $cache = &supplycore_runtime_config_cache_ref();
    $cache['db_overrides_loaded'] = false;
    $cache['db_overrides'] = [];
    $cache['runtime_loaded'] = false;
    $cache['runtime'] = [];
}

function supplycore_runtime_db_overrides(): array
{
    $cache = &supplycore_runtime_config_cache_ref();
    if ($cache['db_overrides_loaded'] === true) {
        return (array) $cache['db_overrides'];
    }

    $overrides = [];
    $registry = supplycore_runtime_settings_registry();

    try {
        $rows = db_app_settings_all();
    } catch (Throwable) {
        return $overrides;
    }

    foreach ($registry as $section => $sectionSpec) {
        $fields = (array) ($sectionSpec['fields'] ?? []);
        foreach ($fields as $path => $fieldSpec) {
            if (($fieldSpec['database_backed'] ?? false) !== true) {
                continue;
            }

            if (!array_key_exists($path, $rows)) {
                continue;
            }

            $leafKey = (string) preg_replace('/^[^.]+\./', '', (string) $path);
            $overrides[$section][$leafKey] = supplycore_cast_runtime_config_value(
                $rows[$path],
                (string) ($fieldSpec['type'] ?? 'string')
            );
        }
    }

    $cache['db_overrides_loaded'] = true;
    $cache['db_overrides'] = $overrides;

    return (array) $cache['db_overrides'];
}

function supplycore_runtime_config(bool $refresh = false): array
{
    if ($refresh) {
        supplycore_runtime_config_cache_clear();
    }
    $cache = &supplycore_runtime_config_cache_ref();
    if ($cache['runtime_loaded'] === true) {
        return (array) $cache['runtime'];
    }

    $base = supplycore_base_config();
    $cache['runtime'] = array_replace_recursive($base, supplycore_runtime_db_overrides());
    $cache['runtime_loaded'] = true;

    return (array) $cache['runtime'];
}

function supplycore_runtime_config_refresh(): void
{
    supplycore_runtime_config(true);
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
        (string) supplycore_base_config_value('db.host', '127.0.0.1'),
        (int) supplycore_base_config_value('db.port', 3306),
        (string) supplycore_base_config_value('db.database', ''),
        (string) supplycore_base_config_value('db.charset', 'utf8mb4')
    );

    try {
        $socket = trim((string) supplycore_base_config_value('db.socket', ''));
        if ($socket !== '') {
            $dsn = sprintf(
                'mysql:unix_socket=%s;dbname=%s;charset=%s',
                $socket,
                (string) supplycore_base_config_value('db.database', ''),
                (string) supplycore_base_config_value('db.charset', 'utf8mb4')
            );
        }

        $pdo = new PDO(
            $dsn,
            (string) supplycore_base_config_value('db.username', ''),
            (string) supplycore_base_config_value('db.password', ''),
            [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
                PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => true,
                PDO::ATTR_STRINGIFY_FETCHES => false,
            ]
        );

        // Session-level optimizer tuning: use histogram statistics for better
        // cardinality estimation (requires ANALYZE TABLE ... PERSISTENT FOR ALL).
        // Level 4 = use histogram-based selectivity for all conditions.
        $pdo->exec('SET SESSION optimizer_use_condition_selectivity = 4');

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

function &db_performance_metrics_ref(): array
{
    static $metrics = [
        'query_count' => 0,
        'query_time_ms' => 0.0,
        'queries' => [],
        'calls' => [],
    ];

    return $metrics;
}

function db_performance_track_query(string $sql, array $params, float $durationMs): void
{
    $metrics = &db_performance_metrics_ref();
    $metrics['query_count']++;
    $metrics['query_time_ms'] += max(0.0, $durationMs);

    $fingerprint = md5(trim($sql));
    if (!isset($metrics['queries'][$fingerprint])) {
        $metrics['queries'][$fingerprint] = [
            'fingerprint' => $fingerprint,
            'sql_preview' => mb_substr(preg_replace('/\s+/', ' ', trim($sql)) ?? '', 0, 160),
            'count' => 0,
            'total_ms' => 0.0,
            'param_count_total' => 0,
        ];
    }

    $metrics['queries'][$fingerprint]['count']++;
    $metrics['queries'][$fingerprint]['total_ms'] += max(0.0, $durationMs);
    $metrics['queries'][$fingerprint]['param_count_total'] += count($params);
}

function db_performance_track_call(string $name, float $durationMs, array $context = []): void
{
    $metrics = &db_performance_metrics_ref();
    if (!isset($metrics['calls'][$name])) {
        $metrics['calls'][$name] = [
            'count' => 0,
            'total_ms' => 0.0,
            'context' => [],
        ];
    }

    $metrics['calls'][$name]['count']++;
    $metrics['calls'][$name]['total_ms'] += max(0.0, $durationMs);

    if ($context !== []) {
        foreach ($context as $key => $value) {
            if (!is_scalar($value)) {
                continue;
            }
            if (!isset($metrics['calls'][$name]['context'][$key])) {
                $metrics['calls'][$name]['context'][$key] = [];
            }
            $contextKey = (string) $value;
            $metrics['calls'][$name]['context'][$key][$contextKey] = (int) (($metrics['calls'][$name]['context'][$key][$contextKey] ?? 0) + 1);
        }
    }
}

function db_performance_snapshot(): array
{
    $metrics = db_performance_metrics_ref();
    $querySummaries = array_values($metrics['queries']);
    usort($querySummaries, static fn (array $a, array $b): int => ($b['count'] <=> $a['count']) ?: ((int) round(($b['total_ms'] ?? 0.0) * 1000) <=> (int) round(($a['total_ms'] ?? 0.0) * 1000)));
    $querySummaries = array_slice($querySummaries, 0, 10);

    $callSummaries = [];
    foreach ((array) $metrics['calls'] as $name => $callStats) {
        $context = [];
        foreach ((array) ($callStats['context'] ?? []) as $key => $counts) {
            arsort($counts);
            $context[$key] = array_slice($counts, 0, 6, true);
        }
        $callSummaries[$name] = [
            'count' => (int) ($callStats['count'] ?? 0),
            'total_ms' => round((float) ($callStats['total_ms'] ?? 0.0), 3),
            'top_context' => $context,
        ];
    }

    return [
        'query_count' => (int) ($metrics['query_count'] ?? 0),
        'query_time_ms' => round((float) ($metrics['query_time_ms'] ?? 0.0), 3),
        'top_queries' => $querySummaries,
        'calls' => $callSummaries,
    ];
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

function &db_app_settings_cache_ref(): array
{
    static $cache = [
        'loaded' => false,
        'values' => [],
    ];

    return $cache;
}

function db_app_settings_cache_clear(): void
{
    $cache = &db_app_settings_cache_ref();
    $cache['loaded'] = false;
    $cache['values'] = [];
    supplycore_runtime_config_cache_clear();
}

function db_app_settings_all(): array
{
    $cache = &db_app_settings_cache_ref();
    if ($cache['loaded'] === true) {
        return (array) $cache['values'];
    }

    $rows = db_select('SELECT setting_key, setting_value FROM app_settings');
    $settings = [];
    foreach ($rows as $row) {
        $key = trim((string) ($row['setting_key'] ?? ''));
        if ($key === '') {
            continue;
        }
        $settings[$key] = (string) ($row['setting_value'] ?? '');
    }

    $cache['loaded'] = true;
    $cache['values'] = $settings;

    return $settings;
}

function db_app_setting_get(string $settingKey, mixed $default = null): mixed
{
    $settings = db_app_settings_all();

    return $settings[$settingKey] ?? $default;
}

function db_app_settings_by_prefix(string $prefix): array
{
    $settings = db_app_settings_all();
    $filtered = [];
    foreach ($settings as $key => $value) {
        if (str_starts_with((string) $key, $prefix)) {
            $filtered[$key] = $value;
        }
    }

    return $filtered;
}

function db_app_setting_set(string $settingKey, string $value): bool
{
    return db_app_settings_upsert_many([$settingKey => $value]);
}

function db_quote_identifier(string $identifier): string
{
    if (!preg_match('/^[A-Za-z0-9_]+$/', $identifier)) {
        throw new InvalidArgumentException('Invalid SQL identifier.');
    }

    return '`' . str_replace('`', '``', $identifier) . '`';
}

function db_table_names(): array
{
    $rows = db_select('SHOW TABLES');
    $tables = [];
    foreach ($rows as $row) {
        $name = (string) array_values($row)[0];
        if ($name === '') {
            continue;
        }
        $tables[] = $name;
    }

    sort($tables, SORT_NATURAL | SORT_FLAG_CASE);

    return $tables;
}

function db_table_columns(string $tableName): array
{
    if (!db_table_exists($tableName)) {
        return [];
    }

    $rows = db_select('SHOW COLUMNS FROM ' . db_quote_identifier($tableName));
    $columns = [];
    foreach ($rows as $row) {
        $name = trim((string) ($row['Field'] ?? ''));
        if ($name === '') {
            continue;
        }
        $columns[] = $name;
    }

    return $columns;
}

function db_table_export_rows(string $tableName): array
{
    if (!db_table_exists($tableName)) {
        return [];
    }

    return db_select('SELECT * FROM ' . db_quote_identifier($tableName));
}

function db_tables_export_payload(array $tables): array
{
    $export = [];
    foreach ($tables as $tableName) {
        $safeTable = trim((string) $tableName);
        if ($safeTable === '' || !db_table_exists($safeTable)) {
            continue;
        }

        $rows = db_table_export_rows($safeTable);
        $export[$safeTable] = [
            'columns' => db_table_columns($safeTable),
            'row_count' => count($rows),
            'rows' => $rows,
        ];
    }

    return $export;
}

function db_tables_replace_from_payload(array $tablesPayload): array
{
    $tableNames = array_keys($tablesPayload);
    $summary = [];

    if ($tableNames === []) {
        return $summary;
    }

    db_transaction(static function () use ($tableNames, $tablesPayload, &$summary): void {
        db_execute('SET FOREIGN_KEY_CHECKS=0');

        try {
            foreach ($tableNames as $tableName) {
                $safeTable = trim((string) $tableName);
                if ($safeTable === '' || !db_table_exists($safeTable)) {
                    continue;
                }

                $tableQuoted = db_quote_identifier($safeTable);
                db_execute('DELETE FROM ' . $tableQuoted);

                $tableRows = (array) ($tablesPayload[$safeTable]['rows'] ?? []);
                $columns = db_table_columns($safeTable);
                if ($tableRows === [] || $columns === []) {
                    $summary[$safeTable] = ['deleted' => true, 'inserted' => 0];
                    continue;
                }

                $quotedColumns = implode(', ', array_map(
                    static fn (string $column): string => db_quote_identifier($column),
                    $columns
                ));
                $placeholders = implode(', ', array_fill(0, count($columns), '?'));
                $sql = 'INSERT INTO ' . $tableQuoted . ' (' . $quotedColumns . ') VALUES (' . $placeholders . ')';
                $inserted = 0;

                foreach ($tableRows as $row) {
                    $values = [];
                    foreach ($columns as $column) {
                        $values[] = $row[$column] ?? null;
                    }
                    db_execute($sql, $values);
                    $inserted++;
                }

                $summary[$safeTable] = ['deleted' => true, 'inserted' => $inserted];
            }
        } finally {
            db_execute('SET FOREIGN_KEY_CHECKS=1');
        }
    });

    return $summary;
}

function db_runtime_schema_checks_enabled(): bool
{
    static $enabled = null;
    if ($enabled !== null) {
        return $enabled;
    }

    $raw = strtolower(trim((string) ($_ENV['SUPPLYCORE_RUNTIME_SCHEMA_CHECKS'] ?? getenv('SUPPLYCORE_RUNTIME_SCHEMA_CHECKS') ?: '0')));
    $enabled = in_array($raw, ['1', 'true', 'yes', 'on'], true);

    return $enabled;
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
    $startedAt = microtime(true);
    $stmt = db()->prepare($sql);
    $stmt->execute($params);
    $durationMs = (microtime(true) - $startedAt) * 1000.0;
    db_performance_track_query($sql, $params, $durationMs);

    return $stmt->fetchAll();
}

function db_select_stream(string $sql, array $params, callable $callback): int
{
    // Keep the connection's default buffered-query behavior so callbacks can
    // safely execute follow-up queries while iterating the result set.
    $stmt = db()->prepare($sql);
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
    $startedAt = microtime(true);
    $stmt = db()->prepare($sql);
    $stmt->execute($params);
    $durationMs = (microtime(true) - $startedAt) * 1000.0;
    db_performance_track_query($sql, $params, $durationMs);
    $result = $stmt->fetch();
    $stmt->closeCursor();

    return $result === false ? null : $result;
}

function db_fetch_single_value(string $sql, array $params = []): mixed
{
    $row = db_select_one($sql, $params);
    if (!is_array($row) || $row === []) {
        return null;
    }

    return reset($row);
}

function db_execute(string $sql, array $params = []): bool
{
    db_query_cache_clear();
    db_app_settings_cache_clear();
    $startedAt = microtime(true);
    $stmt = db()->prepare($sql);
    $result = $stmt->execute($params);
    $durationMs = (microtime(true) - $startedAt) * 1000.0;
    db_performance_track_query($sql, $params, $durationMs);

    return $result;
}

function db_app_settings_upsert_many(array $settings): bool
{
    db_app_settings_cache_clear();
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
    $size = (int) db_app_setting_get('incremental_chunk_size', 1000);

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

    $value = (int) db_app_setting_get($setting, $default);
    if ($value > 0) {

        return max($min, min($max, $value > 0 ? $value : $default));
    }

    return db_setting_int($legacySetting, $default, $min, $max);
}

function db_time_series_bucket_retention_days(string $resolution): int
{
    $default = $resolution === '1h' ? 14 : 180;
    $days = (int) db_app_setting_get('analytics_bucket_' . $resolution . '_retention_days', $default);

    return max($resolution === '1h' ? 1 : 30, min(3650, $days > 0 ? $days : $default));
}

function db_time_series_job_setting_int(string $settingKey, int $default, int $min, int $max): int
{
    $value = (int) db_app_setting_get($settingKey, $default);

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
    if (!db_runtime_schema_checks_enabled()) {
        return true;
    }

    static $cache = [];
    $key = $table . '.' . $indexName;
    if (isset($cache[$key])) {
        return $cache[$key];
    }

    $row = db_select_one(
        'SELECT 1
         FROM information_schema.statistics
         WHERE table_schema = DATABASE()
           AND table_name = ?
           AND index_name = ?
         LIMIT 1',
        [$table, $indexName]
    );

    $exists = $row !== null;
    $cache[$key] = $exists;

    return $exists;
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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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
    db_ensure_table_column('killmail_events', 'victim_damage_taken', 'BIGINT UNSIGNED DEFAULT NULL AFTER victim_ship_type_id');
    db_ensure_table_column('killmail_events', 'battle_id', 'CHAR(64) DEFAULT NULL AFTER victim_damage_taken');
    db_ensure_table_column('killmail_events', 'zkb_fitted_value', 'DECIMAL(20,2) DEFAULT NULL AFTER zkb_total_value');
    db_ensure_table_column('killmail_events', 'zkb_dropped_value', 'DECIMAL(20,2) DEFAULT NULL AFTER zkb_fitted_value');
    db_ensure_table_column('killmail_events', 'zkb_destroyed_value', 'DECIMAL(20,2) DEFAULT NULL AFTER zkb_dropped_value');
    db_ensure_table_column('killmail_events', 'zkb_points', 'INT UNSIGNED DEFAULT NULL AFTER zkb_total_value');
    db_ensure_table_column('killmail_events', 'zkb_npc', 'TINYINT(1) DEFAULT NULL AFTER zkb_points');
    db_ensure_table_column('killmail_events', 'zkb_solo', 'TINYINT(1) DEFAULT NULL AFTER zkb_npc');
    db_ensure_table_column('killmail_events', 'zkb_awox', 'TINYINT(1) DEFAULT NULL AFTER zkb_solo');
    db_ensure_table_index('killmail_events', 'idx_killmail_natural_sequence', 'INDEX idx_killmail_natural_sequence (killmail_id, killmail_hash, sequence_id)');
    db_ensure_table_index('killmail_events', 'idx_killmail_events_battle', 'INDEX idx_killmail_events_battle (battle_id, effective_killmail_at)');
    db_killmail_identity_dedupe_enforce();
    db_ensure_table_index('killmail_events', 'uniq_killmail_identity', 'UNIQUE KEY uniq_killmail_identity (killmail_id, killmail_hash)');
    db_ensure_table_index('killmail_attackers', 'idx_attacker_final_blow', 'INDEX idx_attacker_final_blow (sequence_id, final_blow, attacker_index)');

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

function db_killmail_latest_sequences_sql(): string
{
    return "(
        SELECT
            MAX(e.sequence_id) AS sequence_id,
            CONCAT(CAST(e.killmail_id AS CHAR), ':', e.killmail_hash) AS esi_killmail_key
        FROM killmail_events e
        GROUP BY e.killmail_id, e.killmail_hash
    )";
}

function db_killmail_duplicate_identities(int $limit = 100): array
{
    $safeLimit = max(1, min(1000, $limit));

    return db_select(
        "SELECT
            e.killmail_id,
            e.killmail_hash,
            CONCAT(CAST(e.killmail_id AS CHAR), ':', e.killmail_hash) AS esi_killmail_key,
            COUNT(*) AS duplicate_count,
            MAX(e.sequence_id) AS latest_sequence_id,
            MIN(e.sequence_id) AS earliest_sequence_id,
            GROUP_CONCAT(e.sequence_id ORDER BY e.sequence_id DESC SEPARATOR ',') AS sequence_ids
         FROM killmail_events e
         GROUP BY e.killmail_id, e.killmail_hash
         HAVING COUNT(*) > 1
         ORDER BY duplicate_count DESC, latest_sequence_id DESC
         LIMIT {$safeLimit}"
    );
}

function db_killmail_identity_dedupe_enforce(): void
{
    $duplicates = db_killmail_duplicate_identities(1000);
    if ($duplicates === []) {
        return;
    }

    foreach ($duplicates as $duplicate) {
        $killmailId = (int) ($duplicate['killmail_id'] ?? 0);
        $killmailHash = (string) ($duplicate['killmail_hash'] ?? '');
        if ($killmailId <= 0 || $killmailHash === '') {
            continue;
        }

        $rows = db_select(
            'SELECT sequence_id
             FROM killmail_events
             WHERE killmail_id = ?
               AND killmail_hash = ?
             ORDER BY sequence_id DESC
             LIMIT 1000',
            [$killmailId, $killmailHash]
        );
        if (count($rows) <= 1) {
            continue;
        }

        $sequenceIds = array_map(static fn (array $row): int => (int) ($row['sequence_id'] ?? 0), $rows);
        $sequenceIds = array_values(array_filter($sequenceIds, static fn (int $id): bool => $id > 0));
        if (count($sequenceIds) <= 1) {
            continue;
        }

        $keepSequenceId = (int) array_shift($sequenceIds);
        if ($keepSequenceId <= 0 || $sequenceIds === []) {
            continue;
        }

        $placeholders = implode(',', array_fill(0, count($sequenceIds), '?'));
        $params = $sequenceIds;

        // Delete child rows first, then parent — order matters for referential integrity
        $childTables = ['killmail_attackers', 'killmail_items', 'killmail_event_payloads'];
        foreach ($childTables as $childTable) {
            db_execute("DELETE FROM {$childTable} WHERE sequence_id IN ({$placeholders})", $params);
        }
        db_execute("DELETE FROM killmail_events WHERE sequence_id IN ({$placeholders})", $params);
    }
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
    $value = (int) db_app_setting_get($settingKey, $default);

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
    static $cache = [];
    $key = $table . '.' . $columnName;
    if (isset($cache[$key])) {
        return $cache[$key];
    }

    $row = db_select_one(
        'SELECT 1
         FROM information_schema.columns
         WHERE table_schema = DATABASE()
           AND table_name = ?
           AND column_name = ?
         LIMIT 1',
        [$table, $columnName]
    );

    $exists = $row !== null;
    $cache[$key] = $exists;

    return $exists;
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
    static $cache = [];
    if (isset($cache[$table])) {
        return $cache[$table];
    }

    $row = db_select_one(
        'SELECT 1
         FROM information_schema.table_constraints
         WHERE table_schema = DATABASE()
           AND table_name = ?
           AND constraint_type = \'PRIMARY KEY\'
         LIMIT 1',
        [$table]
    );

    $exists = $row !== null;
    $cache[$table] = $exists;

    return $exists;
}

function db_table_column_is_nullable(string $table, string $columnName): ?bool
{
    static $cache = [];
    $key = $table . '.' . $columnName;
    if (array_key_exists($key, $cache)) {
        return $cache[$key];
    }

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
        $cache[$key] = null;
        return null;
    }

    $result = strtoupper((string) ($row['is_nullable'] ?? '')) === 'YES';
    $cache[$key] = $result;

    return $result;
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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
    static $ensured = false;

    if ($ensured) {
        return;
    }

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
        ['market_item_stock_1h', 'idx_market_item_stock_1h_bucket_type', 'INDEX idx_market_item_stock_1h_bucket_type (bucket_start, type_id)'],
        ['market_item_stock_1h', 'idx_market_item_stock_1h_bucket', 'INDEX idx_market_item_stock_1h_bucket (bucket_start)'],
        ['market_item_stock_1d', 'idx_market_item_stock_1d_bucket_type', 'INDEX idx_market_item_stock_1d_bucket_type (bucket_start, type_id)'],
        ['market_item_stock_1d', 'idx_market_item_stock_1d_bucket', 'INDEX idx_market_item_stock_1d_bucket (bucket_start)'],
        ['market_item_price_1h', 'idx_market_item_price_1h_bucket_type', 'INDEX idx_market_item_price_1h_bucket_type (bucket_start, type_id)'],
        ['market_item_price_1h', 'idx_market_item_price_1h_bucket', 'INDEX idx_market_item_price_1h_bucket (bucket_start)'],
        ['market_item_price_1d', 'idx_market_item_price_1d_bucket_type', 'INDEX idx_market_item_price_1d_bucket_type (bucket_start, type_id)'],
        ['market_item_price_1d', 'idx_market_item_price_1d_bucket', 'INDEX idx_market_item_price_1d_bucket (bucket_start)'],
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
            ['table' => 'market_item_stock_1d', 'cutoff' => $marketCutoff, 'retention_days' => $marketRetentionDays],
            ['table' => 'market_item_price_1d', 'cutoff' => $marketCutoff, 'retention_days' => $marketRetentionDays],
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
        // Empty batch still feeds the horizon stall counter: if the cursor
        // never advances we want operators to see it flagged in the
        // freshness report.  The helper is a no-op when sync_state has no
        // row yet, so first-ever runs are safe.
        db_horizon_advance_cursor($datasetKey, $cursorStart);

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
        // Insert per-item loss rollups. The `doctrine_fit_id` / `doctrine_group_id`
        // columns are left NULL here — compute_auto_doctrines.py repopulates
        // `doctrine_fit_id` with auto_doctrine ids on each run.
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
                NULL AS doctrine_fit_id,
                NULL AS doctrine_group_id,
                e.victim_ship_type_id AS hull_type_id,
                COUNT(*) AS loss_count,
                SUM({$quantitySql}) AS quantity_lost,
                COUNT(DISTINCT COALESCE(e.victim_character_id, e.sequence_id)) AS victim_count,
                COUNT(DISTINCT e.sequence_id) AS killmail_count
            FROM killmail_events e
            INNER JOIN killmail_items i ON i.sequence_id = e.sequence_id
            WHERE e.sequence_id IN ({$placeholders})
              AND i.item_type_id IS NOT NULL
            GROUP BY {$itemBucketExpr}, i.item_type_id, e.victim_ship_type_id
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
                    NULL AS doctrine_fit_id,
                    NULL AS doctrine_group_id,
                    COUNT(*) AS loss_count,
                    COUNT(DISTINCT COALESCE(e.victim_character_id, e.sequence_id)) AS victim_count,
                    COUNT(DISTINCT e.sequence_id) AS killmail_count
                FROM killmail_events e
                WHERE e.sequence_id IN ({$placeholders})
                  AND e.victim_ship_type_id IS NOT NULL
                GROUP BY DATE(e.effective_killmail_at), e.victim_ship_type_id
                ON DUPLICATE KEY UPDATE
                    loss_count = loss_count + VALUES(loss_count),
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

    // Advance the horizon watermark and stall tracking alongside the
    // forward cursor.  Note: we do NOT apply db_horizon_read_from_cursor
    // here because the INSERT ... ON DUPLICATE KEY UPDATE above uses
    // accumulative expressions (loss_count = loss_count + VALUES(...))
    // that would double-count on re-read.  Late-arriving killmails are
    // already handled correctly by the existing design: new rows get
    // new sequence_ids above the cursor and bucket_start derives from
    // effective_killmail_at (event time), so they naturally accumulate
    // into the correct historical bucket.
    db_horizon_advance_cursor($datasetKey, (string) ($result['cursor_end'] ?? $cursorStart));

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
        // Empty batch still feeds the horizon stall counter so operators
        // can see stalled market aggregates in the freshness report.
        db_horizon_advance_cursor($datasetKey, $cursorStart);

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

    // Advance the horizon watermark and stall tracking.  As with
    // killmail_item_loss above, we deliberately do NOT apply the
    // rolling repair window here: the market stock / price UPSERTs
    // are accumulative (sample_count = sample_count + VALUES(...),
    // weighted_price_numerator = weighted_price_numerator + VALUES(...))
    // so re-reading the tail would double-count the averages and
    // weighted prices.  Late market observations still land in the
    // correct bucket via their new sequence id and event-time bucket
    // expression.
    db_horizon_advance_cursor($datasetKey, (string) ($result['cursor_end'] ?? $cursorStart));

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
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 14 DAY) THEN quantity_lost ELSE 0 END) AS quantity_14d,
            SUM(quantity_lost) AS quantity_window,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 24 HOUR) THEN killmail_count ELSE 0 END) AS losses_24h,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 3 DAY) THEN killmail_count ELSE 0 END) AS losses_3d,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 7 DAY) THEN killmail_count ELSE 0 END) AS losses_7d,
            SUM(CASE WHEN bucket_start >= (UTC_TIMESTAMP() - INTERVAL 14 DAY) THEN killmail_count ELSE 0 END) AS losses_14d,
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

    $readMode = db_influx_read_mode();

    // In 'primary' mode, skip MariaDB entirely for unfiltered queries.
    if ($typeIds === [] && $readMode === 'primary') {
        return db_market_item_stock_window_summaries_influx($sourceType, $sourceId, $startDate, $endDate);
    }

    // In 'preferred' mode, try InfluxDB first for unfiltered queries.
    if ($typeIds === [] && $readMode === 'preferred') {
        $influxRows = db_market_item_stock_window_summaries_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

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

    $mariaRows = db_select_cached(
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

    // In 'fallback' mode, try InfluxDB when MariaDB returned nothing.
    if ($mariaRows === [] && $typeIds === [] && $readMode === 'fallback') {
        $influxRows = db_market_item_stock_window_summaries_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

    return $mariaRows;
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
    return db_select_one(
        'SELECT id, character_id, character_name, owner_hash, access_token, refresh_token,
                token_type, scopes, expires_at, updated_at
         FROM esi_oauth_tokens ORDER BY updated_at DESC, id DESC LIMIT 1'
    );
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

function db_sync_states_get_many(array $datasetKeys): array
{
    $normalized = array_values(array_unique(array_filter(array_map(
        static fn (mixed $key): string => trim((string) $key),
        $datasetKeys
    ), static fn (string $key): bool => $key !== '')));
    if ($normalized === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($normalized), '?'));

    return db_select(
        "SELECT dataset_key, sync_mode, status, last_success_at, last_cursor, last_row_count, last_checksum, last_error_message, updated_at
         FROM sync_state
         WHERE dataset_key IN ({$placeholders})",
        $normalized
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

// ---------------------------------------------------------------------------
// Incremental horizon / rolling repair window.
//
// These helpers implement the per-dataset progress model documented in
// docs/OPERATIONS_GUIDE.md (see "Incremental horizon"). The core idea is:
//
//   1. Each dataset gets an explicit backfill_complete gate (flipped by
//      admin, never inferred).
//   2. Once backfill_complete = 1, refresh jobs may switch from
//      full/backfill + incremental mode to incremental-only, reading
//      from (last_cursor - repair_window_seconds) on every pass so
//      late-arriving source rows are absorbed without widening the
//      scan window.
//   3. A derived watermark_event_time plus stall tracking lets the
//      freshness report answer "which datasets are caught up to 24h
//      and which are behind / stalled?" cheaply.
//
// Columns live on sync_state; this block just provides the typed
// accessors used by both PHP callers and the Python orchestrator
// (which mirrors these helpers in python/orchestrator/horizon.py).
// ---------------------------------------------------------------------------

function db_sync_state_horizon_columns_ensure(): void
{
    static $done = false;
    if ($done) {
        return;
    }
    $done = true;

    db_ensure_table_column('sync_state', 'watermark_event_time', 'DATETIME DEFAULT NULL AFTER last_cursor');
    db_ensure_table_column('sync_state', 'backfill_complete', 'TINYINT(1) NOT NULL DEFAULT 0 AFTER watermark_event_time');
    db_ensure_table_column('sync_state', 'backfill_proposed_at', 'DATETIME DEFAULT NULL AFTER backfill_complete');
    db_ensure_table_column('sync_state', 'backfill_proposed_reason', 'VARCHAR(190) DEFAULT NULL AFTER backfill_proposed_at');
    db_ensure_table_column('sync_state', 'incremental_horizon_seconds', 'INT UNSIGNED NOT NULL DEFAULT 86400 AFTER backfill_proposed_reason');
    db_ensure_table_column('sync_state', 'repair_window_seconds', 'INT UNSIGNED NOT NULL DEFAULT 86400 AFTER incremental_horizon_seconds');
    db_ensure_table_column('sync_state', 'stall_cursor', 'VARCHAR(190) DEFAULT NULL AFTER repair_window_seconds');
    db_ensure_table_column('sync_state', 'stall_count', 'INT UNSIGNED NOT NULL DEFAULT 0 AFTER stall_cursor');
    db_ensure_table_column('sync_state', 'auto_approve_blocked', 'TINYINT(1) NOT NULL DEFAULT 0 AFTER stall_count');
}

/**
 * Fetch the full horizon state for a dataset. Returns null only if the
 * dataset_key has no sync_state row at all.
 */
function db_horizon_state_get(string $datasetKey): ?array
{
    db_sync_state_horizon_columns_ensure();

    $row = db_select_one(
        'SELECT dataset_key,
                sync_mode,
                status,
                last_success_at,
                last_cursor,
                watermark_event_time,
                backfill_complete,
                backfill_proposed_at,
                backfill_proposed_reason,
                incremental_horizon_seconds,
                repair_window_seconds,
                stall_cursor,
                stall_count,
                auto_approve_blocked,
                updated_at
         FROM sync_state
         WHERE dataset_key = ?
         LIMIT 1',
        [$datasetKey]
    );

    if ($row === null) {
        return null;
    }

    $row['backfill_complete'] = (int) $row['backfill_complete'] === 1;
    $row['incremental_horizon_seconds'] = (int) $row['incremental_horizon_seconds'];
    $row['repair_window_seconds'] = (int) $row['repair_window_seconds'];
    $row['stall_count'] = (int) $row['stall_count'];
    $row['auto_approve_blocked'] = (int) $row['auto_approve_blocked'] === 1;

    return $row;
}

/**
 * Return true when a dataset is allowed to run in incremental-only mode
 * (backfill_complete gate is set and the dataset has successfully run at
 * least once).
 */
function db_horizon_should_use_incremental_only(string $datasetKey): bool
{
    $state = db_horizon_state_get($datasetKey);
    if ($state === null) {
        return false;
    }
    if (empty($state['backfill_complete'])) {
        return false;
    }
    if (($state['last_success_at'] ?? null) === null) {
        return false;
    }

    return true;
}

/**
 * Compute the "timestamp|id" cursor a refresh run should read from, given
 * the dataset's current forward cursor and configured repair window.
 *
 * When backfill is incomplete or no repair window is configured, returns
 * the raw cursor unchanged (preserves current behavior).
 */
function db_horizon_read_from_cursor(string $datasetKey, ?string $cursor): string
{
    $safeCursor = (string) ($cursor ?? '');
    if ($safeCursor === '') {
        return '1970-01-01 00:00:00|0';
    }

    $state = db_horizon_state_get($datasetKey);
    if ($state === null || empty($state['backfill_complete'])) {
        return $safeCursor;
    }

    $repairWindow = (int) ($state['repair_window_seconds'] ?? 0);
    if ($repairWindow <= 0) {
        return $safeCursor;
    }

    $parsed = db_time_series_parse_cursor($safeCursor);
    $timestamp = (string) ($parsed['timestamp'] ?? '');
    if ($timestamp === '') {
        return $safeCursor;
    }

    $baseEpoch = strtotime($timestamp . ' UTC');
    if ($baseEpoch === false) {
        return $safeCursor;
    }

    $offsetTimestamp = gmdate('Y-m-d H:i:s', max(0, $baseEpoch - $repairWindow));

    return db_time_series_cursor_value($offsetTimestamp, 0);
}

/**
 * Advance the forward cursor with monotonic guarantees and stall detection.
 *
 * $newCursor   "timestamp|id" string emitted by the refresh run.
 * $eventTime   Optional override for watermark_event_time. If null, it is
 *              derived from $newCursor.
 *
 * Behavior:
 *   - Never moves last_cursor backward (explicit rewinds go through
 *     db_horizon_rewind_cursor()).
 *   - Updates watermark_event_time in lockstep.
 *   - If the cursor failed to advance (equal to previous value), increments
 *     stall_count; otherwise resets stall_count and latches stall_cursor
 *     to the new value.
 */
function db_horizon_advance_cursor(string $datasetKey, string $newCursor, ?string $eventTime = null): void
{
    db_sync_state_horizon_columns_ensure();

    $state = db_horizon_state_get($datasetKey);
    $existingCursor = (string) ($state['last_cursor'] ?? '');
    $existingStallCursor = (string) ($state['stall_cursor'] ?? '');

    if ($existingCursor !== '' && strcmp($newCursor, $existingCursor) < 0) {
        return;
    }

    if ($eventTime === null) {
        $parsed = db_time_series_parse_cursor($newCursor);
        $eventTime = $parsed['timestamp'] !== '' ? (string) $parsed['timestamp'] : null;
    }

    $advanced = $newCursor !== $existingCursor;
    $stallCursor = $advanced ? $newCursor : ($existingStallCursor !== '' ? $existingStallCursor : $existingCursor);
    $stallCountExpr = $advanced ? '0' : '(stall_count + 1)';

    db_execute(
        'UPDATE sync_state
            SET last_cursor = ?,
                watermark_event_time = ?,
                stall_cursor = ?,
                stall_count = ' . $stallCountExpr . ',
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?',
        [$newCursor, $eventTime, $stallCursor, $datasetKey]
    );
}

/**
 * Mark a dataset as a backfill-complete candidate. Does NOT flip
 * backfill_complete itself — requires admin approval via
 * db_horizon_approve_backfill_complete().
 */
function db_horizon_propose_backfill_complete(string $datasetKey, string $reason): bool
{
    db_sync_state_horizon_columns_ensure();

    return db_execute(
        'UPDATE sync_state
            SET backfill_proposed_at = IFNULL(backfill_proposed_at, UTC_TIMESTAMP()),
                backfill_proposed_reason = ?,
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?
            AND backfill_complete = 0',
        [mb_substr($reason, 0, 190), $datasetKey]
    );
}

/**
 * Flip backfill_complete = 1 (admin approval). Clears the pending proposal.
 */
function db_horizon_approve_backfill_complete(string $datasetKey): bool
{
    db_sync_state_horizon_columns_ensure();

    return db_execute(
        'UPDATE sync_state
            SET backfill_complete = 1,
                backfill_proposed_at = NULL,
                backfill_proposed_reason = NULL,
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?',
        [$datasetKey]
    );
}

/**
 * Clear a pending proposal without approving it.
 */
function db_horizon_reject_backfill_complete(string $datasetKey): bool
{
    db_sync_state_horizon_columns_ensure();

    return db_execute(
        'UPDATE sync_state
            SET backfill_proposed_at = NULL,
                backfill_proposed_reason = NULL,
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?',
        [$datasetKey]
    );
}

/**
 * Revert a dataset to full-history mode (backfill_complete = 0). The next
 * refresh run resumes its original full/backfill + incremental behavior.
 */
function db_horizon_reset_backfill_complete(string $datasetKey): bool
{
    db_sync_state_horizon_columns_ensure();

    return db_execute(
        'UPDATE sync_state
            SET backfill_complete = 0,
                backfill_proposed_at = NULL,
                backfill_proposed_reason = NULL,
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?',
        [$datasetKey]
    );
}

/**
 * Opt a dataset out of the detect_backfill_complete auto-approval loop.
 *
 * When set, detect_backfill_complete will still *propose* the dataset for
 * backfill_complete (so it shows up in the admin review queue) but will
 * never auto-flip the gate regardless of how long the proposal has been
 * soaking. Useful for datasets that need a human eye on the cutover.
 */
function db_horizon_block_auto_approve(string $datasetKey): bool
{
    db_sync_state_horizon_columns_ensure();

    return db_execute(
        'UPDATE sync_state
            SET auto_approve_blocked = 1,
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?',
        [$datasetKey]
    );
}

/**
 * Clear a previously-set auto-approval block. The dataset becomes eligible
 * for auto-approval again on the next detect_backfill_complete run (subject
 * to the usual soak and health criteria).
 */
function db_horizon_unblock_auto_approve(string $datasetKey): bool
{
    db_sync_state_horizon_columns_ensure();

    return db_execute(
        'UPDATE sync_state
            SET auto_approve_blocked = 0,
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?',
        [$datasetKey]
    );
}

/**
 * Manually rewind a dataset's forward cursor. Used when out-of-window late
 * data lands (bigger than repair_window_seconds) and we need to re-process
 * a range of source events. Bypasses the monotonic guard.
 */
function db_horizon_rewind_cursor(string $datasetKey, string $cursor): bool
{
    db_sync_state_horizon_columns_ensure();

    $parsed = db_time_series_parse_cursor($cursor);
    $eventTime = $parsed['timestamp'] !== '' ? (string) $parsed['timestamp'] : null;

    return db_execute(
        'UPDATE sync_state
            SET last_cursor = ?,
                watermark_event_time = ?,
                stall_cursor = NULL,
                stall_count = 0,
                updated_at = CURRENT_TIMESTAMP
          WHERE dataset_key = ?',
        [$cursor, $eventTime, $datasetKey]
    );
}

/**
 * Report per-dataset freshness and horizon status, joined with the latest
 * sync_runs row for each dataset.
 *
 * Returns rows with keys: dataset_key, sync_mode, status, backfill_complete,
 * backfill_proposed_at, backfill_proposed_reason, last_success_at,
 * watermark_event_time, last_cursor, incremental_horizon_seconds,
 * repair_window_seconds, stall_count, freshness_lag_seconds,
 * is_caught_up_24h, is_stalled, last_run_*, horizon_status.
 *
 * horizon_status ∈ { caught_up, catching_up, backfilling, stopped, stalled }.
 */
function db_calculation_freshness_report(?string $datasetPrefix = null): array
{
    db_sync_state_horizon_columns_ensure();

    $params = [];
    $prefixClause = '';
    if ($datasetPrefix !== null && $datasetPrefix !== '') {
        $prefixClause = ' WHERE ss.dataset_key LIKE ?';
        $params[] = $datasetPrefix . '%';
    }

    $sql = "
        SELECT
            ss.dataset_key,
            ss.sync_mode,
            ss.status,
            ss.backfill_complete,
            ss.backfill_proposed_at,
            ss.backfill_proposed_reason,
            ss.last_success_at,
            ss.watermark_event_time,
            ss.last_cursor,
            ss.incremental_horizon_seconds,
            ss.repair_window_seconds,
            ss.stall_count,
            ss.auto_approve_blocked,
            CASE
                WHEN ss.watermark_event_time IS NULL THEN NULL
                ELSE TIMESTAMPDIFF(SECOND, ss.watermark_event_time, UTC_TIMESTAMP())
            END AS freshness_lag_seconds,
            latest.run_status AS last_run_status,
            latest.source_rows AS last_run_source_rows,
            latest.written_rows AS last_run_written_rows,
            latest.started_at AS last_run_started_at,
            latest.finished_at AS last_run_finished_at
        FROM sync_state ss
        LEFT JOIN (
            SELECT sr.dataset_key, sr.run_status, sr.source_rows, sr.written_rows,
                   sr.started_at, sr.finished_at
            FROM sync_runs sr
            INNER JOIN (
                SELECT dataset_key, MAX(id) AS max_id
                FROM sync_runs
                GROUP BY dataset_key
            ) latest_id ON latest_id.dataset_key = sr.dataset_key AND latest_id.max_id = sr.id
        ) latest ON latest.dataset_key = ss.dataset_key
        {$prefixClause}
        ORDER BY ss.dataset_key ASC
    ";

    $rows = db_select($sql, $params);

    foreach ($rows as &$row) {
        $row['backfill_complete'] = (int) $row['backfill_complete'] === 1;
        $row['incremental_horizon_seconds'] = (int) $row['incremental_horizon_seconds'];
        $row['repair_window_seconds'] = (int) $row['repair_window_seconds'];
        $row['stall_count'] = (int) $row['stall_count'];
        $row['auto_approve_blocked'] = (int) $row['auto_approve_blocked'] === 1;
        $row['freshness_lag_seconds'] = $row['freshness_lag_seconds'] === null
            ? null
            : (int) $row['freshness_lag_seconds'];

        $lag = $row['freshness_lag_seconds'];
        $horizonSeconds = $row['incremental_horizon_seconds'];
        $row['is_caught_up_24h'] = $lag !== null && $lag <= $horizonSeconds;
        $row['is_stalled'] = $row['stall_count'] >= 3;

        if ((string) $row['status'] === 'failed') {
            $row['horizon_status'] = 'stopped';
        } elseif ($row['is_stalled']) {
            $row['horizon_status'] = 'stalled';
        } elseif (!$row['backfill_complete']) {
            $row['horizon_status'] = 'backfilling';
        } elseif ($row['is_caught_up_24h']) {
            $row['horizon_status'] = 'caught_up';
        } else {
            $row['horizon_status'] = 'catching_up';
        }
    }
    unset($row);

    return $rows;
}

/**
 * List all datasets with a pending backfill_complete proposal awaiting
 * admin review.
 */
function db_horizon_list_pending_proposals(): array
{
    db_sync_state_horizon_columns_ensure();

    $rows = db_select(
        'SELECT dataset_key,
                sync_mode,
                backfill_proposed_at,
                backfill_proposed_reason,
                last_success_at,
                watermark_event_time,
                last_cursor,
                stall_count,
                auto_approve_blocked
         FROM sync_state
         WHERE backfill_complete = 0
           AND backfill_proposed_at IS NOT NULL
         ORDER BY backfill_proposed_at ASC'
    );

    foreach ($rows as &$row) {
        $row['stall_count'] = (int) $row['stall_count'];
        $row['auto_approve_blocked'] = (int) $row['auto_approve_blocked'] === 1;
    }
    unset($row);

    return $rows;
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
    $mode = trim((string) db_app_setting_get($key, ''));
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

function db_observed_date_expression(string $table, string $observedAtColumn = 'observed_at'): string
{
    $safeTable = trim($table);
    $safeObservedAtColumn = trim($observedAtColumn);
    if ($safeTable === '' || $safeObservedAtColumn === '') {
        throw new InvalidArgumentException('Table and observed-at column are required.');
    }

    if (db_table_has_column($safeTable, 'observed_date')) {
        return db_validate_identifier('observed_date');
    }

    return 'DATE(' . db_validate_identifier($safeObservedAtColumn) . ')';
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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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
    $observedDateExpression = db_observed_date_expression($source);

    do {
        $rows = db_select(
            sprintf(
                'SELECT source_type, source_id, type_id, order_id, is_buy_order, price, volume_remain, volume_total, min_volume, `range`, duration, issued, expires, observed_at
'
                . 'FROM %s
'
                . 'WHERE %s BETWEEN ? AND ?
'
                . 'ORDER BY observed_at ASC, source_type ASC, source_id ASC, order_id ASC
'
                . 'LIMIT %d OFFSET %d',
                $quotedSource,
                $observedDateExpression,
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
    $mode = trim((string) db_app_setting_get($key, ''));
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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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
    $observedDateExpression = db_observed_date_expression($source);

    do {
        $rows = db_select(
            sprintf(
                'SELECT source_type, source_id, type_id, observed_at, best_sell_price, best_buy_price, total_buy_volume, total_sell_volume, total_volume, buy_order_count, sell_order_count
                 FROM %s
                 WHERE %s BETWEEN ? AND ?
                 ORDER BY observed_at ASC, source_type ASC, source_id ASC, type_id ASC
                 LIMIT %d OFFSET %d',
                $quotedSource,
                $observedDateExpression,
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

function db_market_rebuild_source_keys(): array
{
    db_market_snapshot_optimization_ensure();

    $historyTable = db_validate_identifier(db_market_orders_history_read_table());
    $summaryTable = db_validate_identifier(db_market_order_snapshots_summary_read_table());

    return db_select(
        "SELECT source_type, source_id
         FROM (
            SELECT source_type, source_id FROM market_orders_current
            UNION
            SELECT source_type, source_id FROM {$historyTable}
            UNION
            SELECT source_type, source_id FROM {$summaryTable}
            UNION
            SELECT source_type, source_id FROM market_source_snapshot_state
         ) sources
         WHERE source_type <> ''
           AND source_id > 0
         ORDER BY source_type ASC, source_id ASC"
    );
}

function db_truncate_tables(array $tables): int
{
    $truncated = 0;

    foreach ($tables as $table) {
        db_execute('TRUNCATE TABLE ' . db_validate_identifier((string) $table));
        $truncated++;
    }

    return $truncated;
}

function db_market_order_snapshots_summary_delete_window(string $sourceType, int $sourceId, string $startDate, string $endDate): int
{
    $deleted = 0;

    foreach (db_market_order_snapshots_summary_known_tables() as $table) {
        db_execute(
            'DELETE FROM ' . db_validate_identifier($table) . '
             WHERE source_type = ?
               AND source_id = ?
               AND ' . db_observed_date_expression($table) . ' BETWEEN ? AND ?',
            [$sourceType, $sourceId, $startDate, $endDate]
        );
        $deleted += (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
    }

    return $deleted;
}

function db_market_snapshot_rollup_delete_window(string $resolution, string $sourceType, int $sourceId, string $startDate, string $endDate): int
{
    $table = db_validate_identifier(db_market_snapshot_rollup_table_name($resolution));
    $startValue = $resolution === '1d' ? $startDate : ($startDate . ' 00:00:00');
    $endValue = $resolution === '1d' ? $endDate : ($endDate . ' 23:59:59');

    db_execute(
        "DELETE FROM {$table}
         WHERE source_type = ?
           AND source_id = ?
           AND bucket_start BETWEEN ? AND ?",
        [$sourceType, $sourceId, $startValue, $endValue]
    );

    return (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
}

function db_market_history_daily_delete_window(string $sourceType, int $sourceId, string $startDate, string $endDate): int
{
    db_execute(
        'DELETE FROM market_history_daily
         WHERE source_type = ?
           AND source_id = ?
           AND trade_date BETWEEN ? AND ?',
        [$sourceType, $sourceId, $startDate, $endDate]
    );

    return (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
}

function db_market_hub_local_history_daily_delete_window(string $source, int $sourceId, string $startDate, string $endDate): int
{
    db_execute(
        'DELETE FROM market_hub_local_history_daily
         WHERE source = ?
           AND source_id = ?
           AND trade_date BETWEEN ? AND ?',
        [$source, $sourceId, $startDate, $endDate]
    );

    return (int) db()->query('SELECT ROW_COUNT()')->fetchColumn();
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

function db_market_hub_local_history_daily_latest_captured_at(string $source, int $sourceId): ?string
{
    $safeSource = trim($source);
    $safeSourceId = max(0, $sourceId);
    if ($safeSource === '' || $safeSourceId <= 0) {
        return null;
    }

    $row = db_fetch_one(
        'SELECT MAX(captured_at) AS latest_captured_at
           FROM market_hub_local_history_daily
          WHERE source = :source
            AND source_id = :source_id',
        [
            'source' => $safeSource,
            'source_id' => $safeSourceId,
        ]
    );

    $latestCapturedAt = is_array($row) ? trim((string) ($row['latest_captured_at'] ?? '')) : '';

    return $latestCapturedAt !== '' ? $latestCapturedAt : null;
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

function db_influx_read_enabled(): bool
{
    if ((bool) config('influxdb.enabled', false) && (bool) config('influxdb.read_enabled', false)) {
        return true;
    }

    $readMode = db_influx_read_mode();
    return $readMode === 'preferred' || $readMode === 'primary';
}

/**
 * Return the active InfluxDB read mode.
 *
 * Modes:
 *   disabled   – never read from InfluxDB (default / legacy).
 *   fallback   – try InfluxDB only when MariaDB returns empty.
 *   preferred  – try InfluxDB first; fall back to MariaDB on empty/error.
 *   primary    – use InfluxDB exclusively; MariaDB is not queried for
 *                time-series analytics at all.
 *
 * The mode is resolved from ``influxdb.read_mode`` (new) with backward
 * compatibility for the older boolean ``influxdb.read_enabled`` flag.
 */
function db_influx_read_mode(): string
{
    static $resolved = null;
    if ($resolved !== null) {
        return $resolved;
    }

    if (!(bool) config('influxdb.enabled', false)) {
        $resolved = 'disabled';
        return $resolved;
    }

    $explicit = trim((string) config('influxdb.read_mode', ''));
    if (in_array($explicit, ['disabled', 'fallback', 'preferred', 'primary'], true)) {
        $resolved = $explicit;
        return $resolved;
    }

    // Backward compatibility: honour the legacy boolean flag.
    if ((bool) config('influxdb.read_enabled', false)) {
        $resolved = 'preferred';
        return $resolved;
    }

    $resolved = 'disabled';
    return $resolved;
}

function db_influx_query_rows(string $flux): array
{
    if (!db_influx_read_enabled()) {
        return [];
    }

    if (!function_exists('curl_init')) {
        return [];
    }

    $url = rtrim((string) config('influxdb.url', 'http://127.0.0.1:8086'), '/');
    $org = trim((string) config('influxdb.org', ''));
    $token = trim((string) config('influxdb.token', ''));
    $timeoutSeconds = max(3, (int) config('influxdb.timeout_seconds', 15));
    if ($url === '' || $org === '' || $token === '') {
        return [];
    }

    $endpoint = $url . '/api/v2/query?org=' . rawurlencode($org);
    $headers = [
        'Authorization: Token ' . $token,
        'Content-Type: application/vnd.flux',
        'Accept: application/csv',
    ];

    $handle = curl_init($endpoint);
    curl_setopt_array($handle, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $flux,
        CURLOPT_HTTPHEADER => $headers,
        CURLOPT_CONNECTTIMEOUT => min(5, $timeoutSeconds),
        CURLOPT_TIMEOUT => $timeoutSeconds,
    ]);

    $body = curl_exec($handle);
    $statusCode = (int) curl_getinfo($handle, CURLINFO_HTTP_CODE);
    $curlError = curl_error($handle);
    curl_close($handle);

    if ($body === false || $statusCode < 200 || $statusCode >= 300) {
        return [];
    }
    if ($curlError !== '') {
        return [];
    }

    $lines = preg_split('/\r\n|\n|\r/', (string) $body) ?: [];
    $rows = [];
    $headersRow = null;
    foreach ($lines as $line) {
        $trimmed = trim((string) $line);
        if ($trimmed === '' || str_starts_with($trimmed, '#')) {
            continue;
        }

        $columns = str_getcsv($line);
        if (!is_array($headersRow)) {
            $headersRow = $columns;
            continue;
        }

        if (count($columns) !== count($headersRow)) {
            continue;
        }

        $row = [];
        foreach ($headersRow as $index => $header) {
            $key = trim((string) $header);
            if ($key === '') {
                continue;
            }
            $row[$key] = $columns[$index];
        }
        if ($row !== []) {
            $rows[] = $row;
        }
    }

    return $rows;
}

function db_market_history_daily_recent_window_influx(string $sourceType, int $sourceId, int $days = 8, int $typeLimit = 120): array
{
    if (!db_influx_read_enabled()) {
        return [];
    }

    $safeDays = max(1, min($days, 60));
    $safeTypeLimit = max(1, min($typeLimit, 500));
    $bucket = trim((string) config('influxdb.bucket', 'supplycore_rollups'));
    if ($bucket === '' || $sourceId <= 0) {
        return [];
    }

    $quotedBucket = addslashes($bucket);
    $quotedSourceType = addslashes($sourceType);
    $safeSourceId = (string) $sourceId;

    $priceFlux = <<<FLUX
from(bucket: "{$quotedBucket}")
  |> range(start: -{$safeDays}d)
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$quotedSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "weighted_price")
  |> keep(columns: ["_time", "type_id", "_value"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {$safeTypeLimit}00)
FLUX;

    $stockFlux = <<<FLUX
from(bucket: "{$quotedBucket}")
  |> range(start: -{$safeDays}d)
  |> filter(fn: (r) => r._measurement == "market_item_stock")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$quotedSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "local_stock_units")
  |> keep(columns: ["_time", "type_id", "_value"])
  |> sort(columns: ["_time"], desc: true)
  |> limit(n: {$safeTypeLimit}00)
FLUX;

    $priceRows = db_influx_query_rows($priceFlux);
    if ($priceRows === []) {
        return [];
    }
    $stockRows = db_influx_query_rows($stockFlux);

    $volumeByKey = [];
    foreach ($stockRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $volumeByKey[$tradeDate . ':' . $typeId] = max(0, (int) round((float) ($row['_value'] ?? 0)));
    }

    $rows = [];
    foreach ($priceRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $key = $tradeDate . ':' . $typeId;
        if (isset($rows[$key])) {
            continue;
        }
        $rows[$key] = [
            'type_id' => $typeId,
            'trade_date' => $tradeDate,
            'close_price' => max(0.0, (float) ($row['_value'] ?? 0)),
            'volume' => max(0, (int) ($volumeByKey[$key] ?? 0)),
            'observed_at' => gmdate('Y-m-d H:i:s', strtotime($time) ?: 0),
        ];
    }

    if ($rows === []) {
        return [];
    }

    $typeNamesById = [];
    foreach (db_ref_item_types_by_ids(array_map(static fn (array $row): int => (int) $row['type_id'], array_values($rows))) as $typeRow) {
        $typeId = max(0, (int) ($typeRow['type_id'] ?? 0));
        if ($typeId <= 0) {
            continue;
        }
        $typeNamesById[$typeId] = (string) ($typeRow['type_name'] ?? '');
    }

    $normalizedRows = array_map(static function (array $row) use ($typeNamesById): array {
        $typeId = (int) ($row['type_id'] ?? 0);
        $row['type_name'] = (string) ($typeNamesById[$typeId] ?? '');

        return $row;
    }, array_values($rows));

    usort($normalizedRows, static fn (array $left, array $right): int => strcmp((string) ($right['trade_date'] ?? ''), (string) ($left['trade_date'] ?? '')) ?: ((int) ($left['type_id'] ?? 0) <=> (int) ($right['type_id'] ?? 0)));

    return array_slice($normalizedRows, 0, $safeTypeLimit * $safeDays);
}

/**
 * InfluxDB-backed daily aggregate by date/type/source.
 * Returns the same row shape as db_market_history_daily_aggregate_by_date_type_source().
 */
function db_market_history_daily_aggregate_influx(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate
): array {
    if (!db_influx_read_enabled() || $sourceId <= 0) {
        return [];
    }

    $bucket = trim((string) config('influxdb.bucket', 'supplycore_rollups'));
    if ($bucket === '') {
        return [];
    }

    $qBucket = addslashes($bucket);
    $qSourceType = addslashes($sourceType);
    $safeSourceId = (string) $sourceId;
    $rangeStart = $startDate . 'T00:00:00Z';
    $rangeStop = date('Y-m-d', strtotime($endDate . ' +1 day')) . 'T00:00:00Z';

    $priceFlux = <<<FLUX
from(bucket: "{$qBucket}")
  |> range(start: {$rangeStart}, stop: {$rangeStop})
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$qSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "weighted_price" or r._field == "listing_count")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "type_id", "weighted_price", "listing_count"])
FLUX;

    $stockFlux = <<<FLUX
from(bucket: "{$qBucket}")
  |> range(start: {$rangeStart}, stop: {$rangeStop})
  |> filter(fn: (r) => r._measurement == "market_item_stock")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$qSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "local_stock_units" or r._field == "listing_count")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "type_id", "local_stock_units", "listing_count"])
FLUX;

    $priceRows = db_influx_query_rows($priceFlux);
    if ($priceRows === []) {
        return [];
    }
    $stockRows = db_influx_query_rows($stockFlux);

    $stockByKey = [];
    foreach ($stockRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $stockByKey[$tradeDate . ':' . $typeId] = [
            'volume' => max(0, (int) round((float) ($row['local_stock_units'] ?? 0))),
            'order_count' => max(0, (int) round((float) ($row['listing_count'] ?? 0))),
        ];
    }

    $typeIds = [];
    $results = [];
    foreach ($priceRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $key = $tradeDate . ':' . $typeId;
        if (isset($results[$key])) {
            continue;
        }
        $typeIds[$typeId] = $typeId;
        $stock = $stockByKey[$key] ?? ['volume' => 0, 'order_count' => 0];
        $results[$key] = [
            'trade_date' => $tradeDate,
            'type_id' => $typeId,
            'type_name' => '',
            'source_type' => $sourceType,
            'source_id' => $sourceId,
            'avg_close_price' => max(0.0, (float) ($row['weighted_price'] ?? 0)),
            'total_volume' => $stock['volume'],
            'total_order_count' => $stock['order_count'],
            'last_observed_at' => gmdate('Y-m-d H:i:s', strtotime($time) ?: 0),
        ];
    }

    if ($results === []) {
        return [];
    }

    $typeNamesById = [];
    foreach (db_ref_item_types_by_ids(array_values($typeIds)) as $typeRow) {
        $tid = max(0, (int) ($typeRow['type_id'] ?? 0));
        if ($tid > 0) {
            $typeNamesById[$tid] = (string) ($typeRow['type_name'] ?? '');
        }
    }

    $rows = array_values($results);
    foreach ($rows as &$r) {
        $r['type_name'] = $typeNamesById[(int) $r['type_id']] ?? '';
    }
    unset($r);

    usort($rows, static fn (array $a, array $b): int => strcmp($a['trade_date'], $b['trade_date']) ?: ($a['type_id'] <=> $b['type_id']));

    return $rows;
}

/**
 * InfluxDB-backed stock health series.
 * Returns the same row shape as db_market_orders_history_stock_health_series().
 */
function db_market_history_stock_health_influx(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate
): array {
    if (!db_influx_read_enabled() || $sourceId <= 0) {
        return [];
    }

    $bucket = trim((string) config('influxdb.bucket', 'supplycore_rollups'));
    if ($bucket === '') {
        return [];
    }

    $qBucket = addslashes($bucket);
    $qSourceType = addslashes($sourceType);
    $safeSourceId = (string) $sourceId;
    $rangeStart = $startDate . 'T00:00:00Z';
    $rangeStop = date('Y-m-d', strtotime($endDate . ' +1 day')) . 'T00:00:00Z';

    $stockFlux = <<<FLUX
from(bucket: "{$qBucket}")
  |> range(start: {$rangeStart}, stop: {$rangeStop})
  |> filter(fn: (r) => r._measurement == "market_item_stock")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$qSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "local_stock_units" or r._field == "listing_count")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "type_id", "local_stock_units", "listing_count"])
FLUX;

    $priceFlux = <<<FLUX
from(bucket: "{$qBucket}")
  |> range(start: {$rangeStart}, stop: {$rangeStop})
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$qSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "avg_price")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "type_id", "avg_price"])
FLUX;

    $influxRows = db_influx_query_rows($stockFlux);
    if ($influxRows === []) {
        return [];
    }
    $influxPriceRows = db_influx_query_rows($priceFlux);

    $priceByKey = [];
    foreach ($influxPriceRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate !== '1970-01-01') {
            $priceByKey[$tradeDate . ':' . $typeId] = max(0.0, (float) ($row['avg_price'] ?? 0));
        }
    }

    $typeIds = [];
    $results = [];
    foreach ($influxRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $key = $tradeDate . ':' . $typeId;
        if (isset($results[$key])) {
            continue;
        }
        $typeIds[$typeId] = $typeId;
        $results[$key] = [
            'observed_date' => $tradeDate,
            'type_id' => $typeId,
            'type_name' => '',
            'sell_volume' => max(0, (int) round((float) ($row['local_stock_units'] ?? 0))),
            'buy_volume' => 0,
            'sell_order_count' => max(0, (int) round((float) ($row['listing_count'] ?? 0))),
            'buy_order_count' => 0,
            'avg_sell_price' => isset($priceByKey[$key]) ? $priceByKey[$key] : null,
            'avg_buy_price' => null,
            'last_observed_at' => gmdate('Y-m-d H:i:s', strtotime($time) ?: 0),
        ];
    }

    if ($results === []) {
        return [];
    }

    $typeNamesById = [];
    foreach (db_ref_item_types_by_ids(array_values($typeIds)) as $typeRow) {
        $tid = max(0, (int) ($typeRow['type_id'] ?? 0));
        if ($tid > 0) {
            $typeNamesById[$tid] = (string) ($typeRow['type_name'] ?? '');
        }
    }

    $rows = array_values($results);
    foreach ($rows as &$r) {
        $r['type_name'] = $typeNamesById[(int) $r['type_id']] ?? '';
    }
    unset($r);

    usort($rows, static fn (array $a, array $b): int => strcmp($a['observed_date'], $b['observed_date']) ?: ($a['type_id'] <=> $b['type_id']));

    return $rows;
}

/**
 * InfluxDB-backed stock window summaries with price data.
 * Returns the same row shape as db_market_item_stock_window_summaries().
 */
function db_market_item_stock_window_summaries_influx(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate
): array {
    if (!db_influx_read_enabled() || $sourceId <= 0) {
        return [];
    }

    $bucket = trim((string) config('influxdb.bucket', 'supplycore_rollups'));
    if ($bucket === '') {
        return [];
    }

    $qBucket = addslashes($bucket);
    $qSourceType = addslashes($sourceType);
    $safeSourceId = (string) $sourceId;
    $rangeStart = $startDate . 'T00:00:00Z';
    $rangeStop = date('Y-m-d', strtotime($endDate . ' +1 day')) . 'T00:00:00Z';

    $stockFlux = <<<FLUX
from(bucket: "{$qBucket}")
  |> range(start: {$rangeStart}, stop: {$rangeStop})
  |> filter(fn: (r) => r._measurement == "market_item_stock")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$qSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "local_stock_units" or r._field == "listing_count")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "type_id", "local_stock_units", "listing_count"])
FLUX;

    $priceFlux = <<<FLUX
from(bucket: "{$qBucket}")
  |> range(start: {$rangeStart}, stop: {$rangeStop})
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$qSourceType}")
  |> filter(fn: (r) => r.source_id == "{$safeSourceId}")
  |> filter(fn: (r) => r._field == "avg_price")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "type_id", "avg_price"])
FLUX;

    $stockRows = db_influx_query_rows($stockFlux);
    if ($stockRows === []) {
        return [];
    }
    $priceRows = db_influx_query_rows($priceFlux);

    $priceByKey = [];
    foreach ($priceRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $priceByKey[$tradeDate . ':' . $typeId] = max(0.0, (float) ($row['avg_price'] ?? 0));
    }

    $typeIds = [];
    $results = [];
    foreach ($stockRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $key = $tradeDate . ':' . $typeId;
        if (isset($results[$key])) {
            continue;
        }
        $typeIds[$typeId] = $typeId;
        $results[$key] = [
            'observed_date' => $tradeDate,
            'type_id' => $typeId,
            'type_name' => '',
            'sell_volume' => max(0, (int) round((float) ($row['local_stock_units'] ?? 0))),
            'buy_volume' => 0,
            'sell_order_count' => max(0, (int) round((float) ($row['listing_count'] ?? 0))),
            'buy_order_count' => 0,
            'avg_sell_price' => isset($priceByKey[$key]) ? $priceByKey[$key] : null,
            'avg_buy_price' => null,
            'last_observed_at' => $tradeDate . ' 00:00:00',
        ];
    }

    if ($results === []) {
        return [];
    }

    $typeNamesById = [];
    foreach (db_ref_item_types_by_ids(array_values($typeIds)) as $typeRow) {
        $tid = max(0, (int) ($typeRow['type_id'] ?? 0));
        if ($tid > 0) {
            $typeNamesById[$tid] = (string) ($typeRow['type_name'] ?? '');
        }
    }

    $rows = array_values($results);
    foreach ($rows as &$r) {
        $r['type_name'] = $typeNamesById[(int) $r['type_id']] ?? '';
    }
    unset($r);

    usort($rows, static fn (array $a, array $b): int => strcmp($a['observed_date'], $b['observed_date']) ?: ($a['type_id'] <=> $b['type_id']));

    return $rows;
}

/**
 * InfluxDB-backed deviation series (alliance vs hub prices).
 * Returns the same row shape as db_market_history_daily_deviation_series().
 */
function db_market_history_deviation_influx(
    int $allianceStructureId,
    int $hubSourceId,
    string $startDate,
    string $endDate
): array {
    if (!db_influx_read_enabled() || $allianceStructureId <= 0 || $hubSourceId <= 0) {
        return [];
    }

    $bucket = trim((string) config('influxdb.bucket', 'supplycore_rollups'));
    if ($bucket === '') {
        return [];
    }

    $qBucket = addslashes($bucket);
    $safeAllianceId = (string) $allianceStructureId;
    $safeHubId = (string) $hubSourceId;
    $rangeStart = $startDate . 'T00:00:00Z';
    $rangeStop = date('Y-m-d', strtotime($endDate . ' +1 day')) . 'T00:00:00Z';

    $buildFlux = static function (string $sourceType, string $sourceId) use ($qBucket, $rangeStart, $rangeStop): string {
        return <<<FLUX
from(bucket: "{$qBucket}")
  |> range(start: {$rangeStart}, stop: {$rangeStop})
  |> filter(fn: (r) => r._measurement == "market_item_price")
  |> filter(fn: (r) => r.window == "1d")
  |> filter(fn: (r) => r.source_type == "{$sourceType}")
  |> filter(fn: (r) => r.source_id == "{$sourceId}")
  |> filter(fn: (r) => r._field == "weighted_price" or r._field == "listing_count")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "type_id", "weighted_price", "listing_count"])
FLUX;
    };

    $allianceRows = db_influx_query_rows($buildFlux('alliance_structure', $safeAllianceId));
    if ($allianceRows === []) {
        return [];
    }
    $hubRows = db_influx_query_rows($buildFlux('market_hub', $safeHubId));

    $hubByKey = [];
    foreach ($hubRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $hubByKey[$tradeDate . ':' . $typeId] = [
            'price' => max(0.0, (float) ($row['weighted_price'] ?? 0)),
            'order_count' => max(0, (int) round((float) ($row['listing_count'] ?? 0))),
        ];
    }

    if ($hubByKey === []) {
        return [];
    }

    $typeIds = [];
    $results = [];
    foreach ($allianceRows as $row) {
        $typeId = max(0, (int) ($row['type_id'] ?? 0));
        $time = trim((string) ($row['_time'] ?? ''));
        if ($typeId <= 0 || $time === '') {
            continue;
        }
        $tradeDate = gmdate('Y-m-d', strtotime($time) ?: 0);
        if ($tradeDate === '1970-01-01') {
            continue;
        }
        $key = $tradeDate . ':' . $typeId;
        if (isset($results[$key]) || !isset($hubByKey[$key])) {
            continue;
        }
        $alliancePrice = max(0.0, (float) ($row['weighted_price'] ?? 0));
        $hubPrice = $hubByKey[$key]['price'];
        $deviationPercent = $hubPrice > 0 ? (($alliancePrice - $hubPrice) / $hubPrice) * 100 : null;

        $typeIds[$typeId] = $typeId;
        $results[$key] = [
            'trade_date' => $tradeDate,
            'type_id' => $typeId,
            'type_name' => '',
            'alliance_close_price' => $alliancePrice,
            'hub_close_price' => $hubPrice,
            'deviation_percent' => $deviationPercent,
            'alliance_volume' => 0,
            'hub_volume' => 0,
            'alliance_order_count' => max(0, (int) round((float) ($row['listing_count'] ?? 0))),
            'hub_order_count' => $hubByKey[$key]['order_count'],
        ];
    }

    if ($results === []) {
        return [];
    }

    $typeNamesById = [];
    foreach (db_ref_item_types_by_ids(array_values($typeIds)) as $typeRow) {
        $tid = max(0, (int) ($typeRow['type_id'] ?? 0));
        if ($tid > 0) {
            $typeNamesById[$tid] = (string) ($typeRow['type_name'] ?? '');
        }
    }

    $rows = array_values($results);
    foreach ($rows as &$r) {
        $r['type_name'] = $typeNamesById[(int) $r['type_id']] ?? '';
    }
    unset($r);

    usort($rows, static fn (array $a, array $b): int => strcmp($a['trade_date'], $b['trade_date']) ?: ($a['type_id'] <=> $b['type_id']));

    return $rows;
}

function db_market_history_daily_aggregate_by_date_type_source(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    $readMode = db_influx_read_mode();

    // In 'primary' mode, InfluxDB is the only source for time-series reads.
    if ($typeIds === [] && $readMode === 'primary') {
        return db_market_history_daily_aggregate_influx($sourceType, $sourceId, $startDate, $endDate);
    }

    // In 'preferred' mode, try InfluxDB first; fall back to MariaDB on empty.
    if ($typeIds === [] && $readMode === 'preferred') {
        $influxRows = db_market_history_daily_aggregate_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

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

    $mariaRows = db_select_cached(
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
         GROUP BY mhd.trade_date, mhd.type_id
         ORDER BY mhd.trade_date ASC, mhd.type_id ASC",
        $params,
        60,
        'market.history.aggregate'
    );

    // In 'fallback' mode, try InfluxDB only when MariaDB returned nothing.
    if ($mariaRows === [] && $typeIds === [] && $readMode === 'fallback') {
        $influxRows = db_market_history_daily_aggregate_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

    // Final backstop: market_history_daily is only populated by the manual
    // rebuild_data_model job, so recurring installs often find it empty even
    // when analytics_bucket_1d_sync is healthy. Fall back to the 1d bucket
    // rollup tables so Alliance Trends / Module History can still render.
    if ($mariaRows === []) {
        return db_market_history_daily_aggregate_from_buckets($sourceType, $sourceId, $startDate, $endDate, $typeIds);
    }

    return $mariaRows;
}

function db_market_history_daily_aggregate_from_buckets(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    if ($sourceId <= 0) {
        return [];
    }

    $params = [$sourceType, $sourceId, $startDate, $endDate];
    $typeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }
        $typeFilterSql = ' AND p.type_id IN (' . implode(',', array_fill(0, count($normalizedTypeIds), '?')) . ')';
        $params = array_merge($params, $normalizedTypeIds);
    }

    return db_select(
        "SELECT
            p.bucket_start AS trade_date,
            p.type_id,
            rit.type_name,
            p.source_type,
            p.source_id,
            COALESCE(p.weighted_price, p.avg_price) AS avg_close_price,
            COALESCE(s.local_stock_units, 0) AS total_volume,
            COALESCE(p.listing_count, s.listing_count, 0) AS total_order_count,
            p.updated_at AS last_observed_at
         FROM market_item_price_1d p
         LEFT JOIN market_item_stock_1d s
             ON s.source_type = p.source_type
            AND s.source_id = p.source_id
            AND s.type_id = p.type_id
            AND s.bucket_start = p.bucket_start
         LEFT JOIN ref_item_types rit ON rit.type_id = p.type_id
         WHERE p.source_type = ?
           AND p.source_id = ?
           AND p.bucket_start BETWEEN ? AND ?{$typeFilterSql}
         ORDER BY p.bucket_start ASC, p.type_id ASC",
        $params
    );
}

function db_market_history_daily_deviation_series(
    int $allianceStructureId,
    int $hubSourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    $readMode = db_influx_read_mode();

    if ($typeIds === [] && $readMode === 'primary') {
        return db_market_history_deviation_influx($allianceStructureId, $hubSourceId, $startDate, $endDate);
    }

    if ($typeIds === [] && $readMode === 'preferred') {
        $influxRows = db_market_history_deviation_influx($allianceStructureId, $hubSourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

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

    $mariaRows = db_select_cached(
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

    if ($mariaRows === [] && $typeIds === [] && $readMode === 'fallback') {
        $influxRows = db_market_history_deviation_influx($allianceStructureId, $hubSourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

    // Final backstop: derive deviation from the 1d price bucket tables when
    // market_history_daily is empty (only written by the manual rebuild job).
    if ($mariaRows === []) {
        return db_market_history_daily_deviation_from_buckets($allianceStructureId, $hubSourceId, $startDate, $endDate, $typeIds);
    }

    return $mariaRows;
}

function db_market_history_daily_deviation_from_buckets(
    int $allianceStructureId,
    int $hubSourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    if ($allianceStructureId <= 0 || $hubSourceId <= 0) {
        return [];
    }

    $params = [$hubSourceId, $allianceStructureId, $startDate, $endDate];
    $typeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }
        $typeFilterSql = ' AND a.type_id IN (' . implode(',', array_fill(0, count($normalizedTypeIds), '?')) . ')';
        $params = array_merge($params, $normalizedTypeIds);
    }

    return db_select(
        "SELECT
            a.bucket_start AS trade_date,
            a.type_id,
            rit.type_name,
            COALESCE(a.weighted_price, a.avg_price) AS alliance_close_price,
            COALESCE(h.weighted_price, h.avg_price) AS hub_close_price,
            CASE
                WHEN COALESCE(h.weighted_price, h.avg_price) > 0
                    THEN ((COALESCE(a.weighted_price, a.avg_price) - COALESCE(h.weighted_price, h.avg_price)) / COALESCE(h.weighted_price, h.avg_price)) * 100
                ELSE NULL
            END AS deviation_percent,
            NULL AS alliance_volume,
            NULL AS hub_volume,
            a.listing_count AS alliance_order_count,
            h.listing_count AS hub_order_count
         FROM market_item_price_1d a
         INNER JOIN market_item_price_1d h
             ON h.source_type = 'market_hub'
            AND h.source_id = ?
            AND h.type_id = a.type_id
            AND h.bucket_start = a.bucket_start
         LEFT JOIN ref_item_types rit ON rit.type_id = a.type_id
         WHERE a.source_type = 'alliance_structure'
           AND a.source_id = ?
           AND a.bucket_start BETWEEN ? AND ?{$typeFilterSql}
         ORDER BY a.bucket_start ASC, a.type_id ASC",
        $params
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
    $readMode = db_influx_read_mode();

    // In 'primary' mode, InfluxDB is the only source.
    if ($typeIds === [] && $readMode === 'primary') {
        return db_market_history_stock_health_influx($sourceType, $sourceId, $startDate, $endDate);
    }

    // In 'preferred' mode, try InfluxDB first.
    if ($typeIds === [] && ($readMode === 'preferred')) {
        $influxRows = db_market_history_stock_health_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

    $rows = db_market_item_stock_window_summaries($sourceType, $sourceId, $startDate, $endDate, $typeIds);

    // In 'fallback' mode, try InfluxDB when MariaDB rollups are empty.
    if ($rows === [] && $typeIds === [] && $readMode === 'fallback') {
        $influxRows = db_market_history_stock_health_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

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

function db_sync_runs_reap_stale(int $staleGraceSeconds = 300): int
{
    $safeGrace = max(60, min(7200, $staleGraceSeconds));
    $stmt = db()->prepare(
        "UPDATE sync_runs
         SET run_status = 'failed',
             finished_at = UTC_TIMESTAMP(),
             error_message = 'Reaped: run exceeded timeout while still marked as running (worker likely crashed).',
             updated_at = CURRENT_TIMESTAMP
         WHERE run_status = 'running'
           AND started_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL ? SECOND)"
    );
    $stmt->execute([$safeGrace]);

    return (int) $stmt->rowCount();
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

function db_sync_runs_get_latest_many(array $datasetKeys): array
{
    $normalized = array_values(array_unique(array_filter(array_map(
        static fn (mixed $key): string => trim((string) $key),
        $datasetKeys
    ), static fn (string $key): bool => $key !== '')));
    if ($normalized === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($normalized), '?'));

    return db_select(
        "SELECT sr.id, sr.dataset_key, sr.run_mode, sr.run_status, sr.source_rows, sr.written_rows, sr.error_message, sr.started_at, sr.finished_at
         FROM sync_runs sr
         INNER JOIN (
             SELECT dataset_key, MAX(id) AS max_id
             FROM sync_runs
             WHERE dataset_key IN ({$placeholders})
             GROUP BY dataset_key
         ) latest ON latest.dataset_key = sr.dataset_key AND latest.max_id = sr.id",
        $normalized
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
        'SELECT COALESCE(rs.region_id, ns.region_id) AS region_id
         FROM ref_npc_stations ns
         LEFT JOIN ref_systems rs ON rs.system_id = ns.system_id
         WHERE ns.station_id = ?
         LIMIT 1',
        [$stationId]
    );

    $regionId = (int) ($row['region_id'] ?? 0);

    return ($regionId > 10000000 && $regionId < 11000000) ? $regionId : null;
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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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

    // Single-scan CTE with window functions replaces 3 separate scans of market_orders_current
    return db_select(
        "WITH sell_orders AS (
            SELECT
                type_id,
                order_id,
                price,
                volume_remain,
                observed_at,
                ROW_NUMBER() OVER (PARTITION BY type_id ORDER BY price ASC, order_id ASC) AS rn,
                SUM(volume_remain) OVER (PARTITION BY type_id) AS total_sell_volume,
                COUNT(*) OVER (PARTITION BY type_id) AS sell_order_count
            FROM market_orders_current
            WHERE source_type = ?
              AND source_id = ?
              AND is_buy_order = 0
              AND volume_remain > 0
         )
         SELECT
            so.type_id,
            rit.type_name,
            so.order_id,
            so.price,
            so.volume_remain,
            so.observed_at,
            so.sell_order_count,
            so.total_sell_volume
         FROM sell_orders so
         LEFT JOIN ref_item_types rit ON rit.type_id = so.type_id
         WHERE so.rn = 1
         ORDER BY so.price ASC, so.type_id ASC",
        [
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
        if ($numericSearch !== '' && $numericSearch !== '0') {
            // Sargable numeric range instead of CAST(item_type_id AS CHAR) LIKE
            $lowerBound = (int) $numericSearch;
            $upperBound = (int) ($numericSearch . str_repeat('9', max(0, 15 - strlen($numericSearch))));
            $where[] = '(rit.type_name LIKE ? OR a.item_type_id BETWEEN ? AND ?)';
            $params[] = '%' . $search . '%';
            $params[] = $lowerBound;
            $params[] = $upperBound;
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
        if ($numericSearch !== '' && $numericSearch !== '0') {
            // Sargable numeric range instead of CAST(item_type_id AS CHAR) LIKE
            $lowerBound = (int) $numericSearch;
            $upperBound = (int) ($numericSearch . str_repeat('9', max(0, 15 - strlen($numericSearch))));
            $where[] = '(rit.type_name LIKE ? OR a.item_type_id BETWEEN ? AND ?)';
            $params[] = '%' . $search . '%';
            $params[] = $lowerBound;
            $params[] = $upperBound;
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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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
    static $done = false;
    if ($done) {
        return;
    }
    $done = true;

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
    static $done = false;
    if ($done) {
        return;
    }
    $done = true;

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
    db_ensure_table_column('sync_schedules', 'execution_mode', "VARCHAR(16) NOT NULL DEFAULT 'python'");
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
                WHEN LOWER(COALESCE(NULLIF(execution_mode, ''), 'python')) = 'php' THEN 'php'
                ELSE 'python'
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
    db_ensure_table_column('sync_runs', 'summary', 'VARCHAR(500) DEFAULT NULL');
    db_ensure_table_column('scheduler_job_current_status', 'last_run_summary', 'VARCHAR(500) DEFAULT NULL');

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
                OR last_finished_at <= DATE_SUB(UTC_TIMESTAMP(), INTERVAL GREATEST(60, min_backfill_gap_seconds) SECOND)
           )
           AND (
                max_early_start_seconds <= 0
                OR next_due_at IS NULL
                OR next_due_at <= DATE_ADD(UTC_TIMESTAMP(), INTERVAL max_early_start_seconds SECOND)
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
        // Preserve last_success_at for skip-like results so the change-aware
        // freshness ceiling (requires_forced_periodic_refresh) eventually
        // triggers.  Previously skips reset last_success_at to now(), which
        // prevented the forced-refresh safety net from ever firing — jobs
        // with unchanging upstream data would be skipped indefinitely.
        $currentStatus = db_scheduler_job_current_status_fetch_map([$jobKey])[$jobKey] ?? [];
        $lastSuccessAtValue = $isSkipLike
            ? ($currentStatus['last_success_at'] ?? $finishedAt)
            : $finishedAt;
        db_scheduler_job_current_status_upsert($jobKey, [
            'latest_status' => $lastResult,
            'latest_event_type' => 'completion',
            'last_started_at' => $schedule['last_started_at'] ?? null,
            'last_finished_at' => $finishedAt,
            'last_success_at' => $lastSuccessAtValue,
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
            'last_warnings_json' => $runtime['last_warnings_json'] ?? null,
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
    $executionMode = strtolower(trim((string) ($options['execution_mode'] ?? 'python'))) === 'php' ? 'php' : 'python';
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
            execution_mode = VALUES(execution_mode),
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
    $executionMode = strtolower(trim((string) ($options['execution_mode'] ?? ($existing['execution_mode'] ?? 'python')))) === 'php' ? 'php' : 'python';
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
    static $done = false;
    if ($done) {
        return;
    }
    $done = true;

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
    db_ensure_table_column('scheduler_job_current_status', 'last_warnings_json', 'LONGTEXT DEFAULT NULL AFTER last_no_change_reason');
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
    $lastWarningsJson = null;
    if (array_key_exists('last_warnings_json', $status) && $status['last_warnings_json'] !== null) {
        $lastWarningsJson = is_string($status['last_warnings_json'])
            ? $status['last_warnings_json']
            : json_encode($status['last_warnings_json'], JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
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
            last_warnings_json,
            last_resource_metrics_summary_json,
            last_planner_decision_type,
            last_planner_reason_text,
            last_planner_decided_at,
            last_event_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            last_warnings_json = VALUES(last_warnings_json),
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
            $lastWarningsJson,
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
    $sql = 'SELECT job_key, dataset_key, latest_status, latest_event_type, last_started_at, last_finished_at, last_success_at, last_failure_at, last_failure_message, current_pressure_state, last_pressure_state, recent_timeout_count, recent_lock_conflict_count, recent_deferral_count, recent_skip_count, change_aware, dependencies_json, last_change_detection_json, last_execution_context_json, last_no_change_skip_at, last_no_change_reason, last_warnings_json, last_resource_metrics_summary_json, last_planner_decision_type, last_planner_reason_text, last_planner_decided_at, last_event_at, created_at, updated_at
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
        $row['last_warnings'] = [];
        $warningsJson = $row['last_warnings_json'] ?? null;
        if (is_string($warningsJson) && trim($warningsJson) !== '') {
            $decodedWarnings = json_decode($warningsJson, true);
            if (is_array($decodedWarnings)) {
                $row['last_warnings'] = $decodedWarnings;
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
    static $done = false;
    if ($done) {
        return;
    }
    $done = true;

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
    // Truncate name-construction support tables (best-effort; may not exist on older installs).
    try {
        db_execute('TRUNCATE TABLE ref_celestials');
        db_execute('TRUNCATE TABLE ref_npc_corporations');
        db_execute('TRUNCATE TABLE ref_station_operations');
    } catch (Throwable) {
    }
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
    return db_bulk_insert_or_upsert(
        'ref_npc_stations',
        ['station_id', 'station_name', 'system_id', 'constellation_id', 'region_id', 'station_type_id', 'orbit_id', 'owner_id', 'operation_id', 'use_operation_name'],
        $rows,
        ['station_name', 'system_id', 'constellation_id', 'region_id', 'station_type_id', 'orbit_id', 'owner_id', 'operation_id', 'use_operation_name'],
        $chunkSize
    );
}

function db_ref_npc_corporations_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_npc_corporations', ['corp_id', 'corp_name'], $rows, ['corp_name'], $chunkSize);
}

function db_ref_station_operations_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_station_operations', ['operation_id', 'operation_name'], $rows, ['operation_name'], $chunkSize);
}

function db_ref_celestials_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_celestials', ['celestial_id', 'celestial_type', 'parent_id', 'celestial_index', 'system_id'], $rows, ['celestial_type', 'parent_id', 'celestial_index', 'system_id'], $chunkSize);
}

function db_ref_stargates_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_stargates', ['stargate_id', 'system_id', 'dest_stargate_id', 'dest_system_id'], $rows, ['system_id', 'dest_stargate_id', 'dest_system_id'], $chunkSize);
}

/**
 * Fetch all systems in the given constellation(s) with their internal gate connections.
 * Returns ['nodes' => [...], 'edges' => [...]] in the same format as db_threat_corridor_graph_subgraph().
 */
function db_constellation_graph(array $constellationIds): array
{
    $constellationIds = array_values(array_unique(array_filter(array_map('intval', $constellationIds), static fn(int $id): bool => $id > 0)));
    if ($constellationIds === []) {
        return ['nodes' => [], 'edges' => []];
    }

    $ph = implode(',', array_fill(0, count($constellationIds), '?'));
    $systems = db_select(
        "SELECT rs.system_id, rs.system_name, rs.security, rs.constellation_id,
                rc.constellation_name
         FROM ref_systems rs
         LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
         WHERE rs.constellation_id IN ({$ph})",
        $constellationIds
    );

    if ($systems === []) {
        return ['nodes' => [], 'edges' => []];
    }

    $systemIds = array_map(static fn(array $r): int => (int) $r['system_id'], $systems);
    $sysPh = implode(',', array_fill(0, count($systemIds), '?'));
    $edges = db_select(
        "SELECT system_id, dest_system_id
         FROM ref_stargates
         WHERE system_id IN ({$sysPh})
           AND dest_system_id IN ({$sysPh})",
        array_merge($systemIds, $systemIds)
    );

    $edgePairs = [];
    foreach ($edges as $r) {
        $a = (int) $r['system_id'];
        $b = (int) $r['dest_system_id'];
        if ($a > 0 && $b > 0 && $a !== $b) {
            $left = min($a, $b);
            $right = max($a, $b);
            $edgePairs[$left . ':' . $right] = [$left, $right];
        }
    }

    return [
        'nodes' => $systems,
        'edges' => array_values($edgePairs),
    ];
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

/**
 * Fetch entity IDs from entity_metadata_cache that need resolution:
 * - resolution_status = 'pending'
 * - resolution_status = 'failed' and last attempt was over $retryAfterMinutes ago
 * - resolution_status = 'resolved' but expires_at has passed (stale)
 *
 * Returns ['alliance' => [id, ...], 'corporation' => [...], 'character' => [...]]
 */
function db_entity_metadata_cache_pending(int $limit = 500, int $retryAfterMinutes = 30): array
{
    $now = gmdate('Y-m-d H:i:s');
    $retryThreshold = gmdate('Y-m-d H:i:s', strtotime("-{$retryAfterMinutes} minutes"));

    $rows = db_query(
        "SELECT entity_type, entity_id
         FROM entity_metadata_cache
         WHERE (
             resolution_status = 'pending'
             OR (resolution_status = 'failed' AND updated_at < ?)
             OR (resolution_status = 'resolved' AND expires_at IS NOT NULL AND expires_at < ?)
         )
         AND entity_type IN ('alliance', 'corporation', 'character')
         ORDER BY
             CASE resolution_status
                 WHEN 'pending' THEN 0
                 WHEN 'failed' THEN 1
                 ELSE 2
             END ASC,
             updated_at ASC
         LIMIT ?",
        [$retryThreshold, $now, $limit]
    );

    $result = ['alliance' => [], 'corporation' => [], 'character' => []];
    foreach ($rows as $row) {
        $type = (string) $row['entity_type'];
        $id = (int) $row['entity_id'];
        if ($id > 0 && isset($result[$type])) {
            $result[$type][] = $id;
        }
    }

    return $result;
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
            'victim_damage_taken',
            'battle_id',
            'mail_type',
            'zkb_total_value',
            'zkb_fitted_value',
            'zkb_dropped_value',
            'zkb_destroyed_value',
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
            isset($event['victim_damage_taken']) ? (int) $event['victim_damage_taken'] : null,
            isset($event['battle_id']) ? (string) $event['battle_id'] : null,
            (string) ($event['mail_type'] ?? 'loss'),
            isset($event['zkb_total_value']) ? (float) $event['zkb_total_value'] : null,
            isset($event['zkb_fitted_value']) ? (float) $event['zkb_fitted_value'] : null,
            isset($event['zkb_dropped_value']) ? (float) $event['zkb_dropped_value'] : null,
            isset($event['zkb_destroyed_value']) ? (float) $event['zkb_destroyed_value'] : null,
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
            'victim_damage_taken = VALUES(victim_damage_taken)',
            'battle_id = VALUES(battle_id)',
            'mail_type = VALUES(mail_type)',
            'zkb_total_value = VALUES(zkb_total_value)',
            'zkb_fitted_value = VALUES(zkb_fitted_value)',
            'zkb_dropped_value = VALUES(zkb_dropped_value)',
            'zkb_destroyed_value = VALUES(zkb_destroyed_value)',
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
             OR (killmail_id = ? AND killmail_hash = ?)
          LIMIT 1',
        [$sequenceId, $killmailId, $killmailHash]
    );

    return $row !== null;
}

/**
 * Given a list of killmail IDs, return the subset that already exist in killmail_events.
 *
 * @param int[] $killmailIds
 * @return int[]
 */
function db_killmail_ids_existing(array $killmailIds): array
{
    $killmailIds = array_values(array_unique(array_filter(array_map('intval', $killmailIds), static fn (int $id): bool => $id > 0)));
    if ($killmailIds === []) {
        return [];
    }

    $existing = [];
    foreach (array_chunk($killmailIds, 500) as $chunk) {
        $placeholders = implode(',', array_fill(0, count($chunk), '?'));
        $rows = db_select("SELECT DISTINCT killmail_id FROM killmail_events WHERE killmail_id IN ({$placeholders})", $chunk);
        foreach ($rows as $row) {
            $existing[] = (int) $row['killmail_id'];
        }
    }

    return $existing;
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
                    damage_done,
                    final_blow,
                    security_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    character_id = VALUES(character_id),
                    corporation_id = VALUES(corporation_id),
                    alliance_id = VALUES(alliance_id),
                    ship_type_id = VALUES(ship_type_id),
                    weapon_type_id = VALUES(weapon_type_id),
                    damage_done = VALUES(damage_done),
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
                    isset($row['damage_done']) ? (int) $row['damage_done'] : null,
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

function db_killmail_tracked_alliances_active(): array
{
    static $cache = null;
    if ($cache !== null) {
        return $cache;
    }
    $contacts = db_corp_contacts_by_standing();
    $ids = array_values(array_unique(array_filter(array_map('intval', $contacts['friendly_alliance_ids'] ?? []), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return $cache = [];
    }
    $names = db_entity_metadata_cache_get_many('alliance', $ids);
    $nameMap = [];
    foreach ($names as $n) {
        $nameMap[(int) $n['entity_id']] = (string) $n['entity_name'];
    }
    $rows = [];
    foreach ($ids as $id) {
        $rows[] = ['alliance_id' => $id, 'label' => $nameMap[$id] ?? null];
    }
    return $cache = $rows;
}

function db_killmail_tracked_corporations_active(): array
{
    static $cache = null;
    if ($cache !== null) {
        return $cache;
    }
    $contacts = db_corp_contacts_by_standing();
    $ids = array_values(array_unique(array_filter(array_map('intval', $contacts['friendly_corporation_ids'] ?? []), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return $cache = [];
    }
    $names = db_entity_metadata_cache_get_many('corporation', $ids);
    $nameMap = [];
    foreach ($names as $n) {
        $nameMap[(int) $n['entity_id']] = (string) $n['entity_name'];
    }
    $rows = [];
    foreach ($ids as $id) {
        $rows[] = ['corporation_id' => $id, 'label' => $nameMap[$id] ?? null];
    }
    return $cache = $rows;
}

function db_killmail_opponent_alliances_active(): array
{
    static $cache = null;
    if ($cache !== null) {
        return $cache;
    }
    $contacts = db_corp_contacts_by_standing();
    $ids = array_values(array_unique(array_filter(array_map('intval', $contacts['hostile_alliance_ids'] ?? []), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return $cache = [];
    }
    $names = db_entity_metadata_cache_get_many('alliance', $ids);
    $nameMap = [];
    foreach ($names as $n) {
        $nameMap[(int) $n['entity_id']] = (string) $n['entity_name'];
    }
    $rows = [];
    foreach ($ids as $id) {
        $rows[] = ['alliance_id' => $id, 'label' => $nameMap[$id] ?? null];
    }
    return $cache = $rows;
}

function db_killmail_opponent_corporations_active(): array
{
    static $cache = null;
    if ($cache !== null) {
        return $cache;
    }
    $contacts = db_corp_contacts_by_standing();
    $ids = array_values(array_unique(array_filter(array_map('intval', $contacts['hostile_corporation_ids'] ?? []), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        return $cache = [];
    }
    $names = db_entity_metadata_cache_get_many('corporation', $ids);
    $nameMap = [];
    foreach ($names as $n) {
        $nameMap[(int) $n['entity_id']] = (string) $n['entity_name'];
    }
    $rows = [];
    foreach ($ids as $id) {
        $rows[] = ['corporation_id' => $id, 'label' => $nameMap[$id] ?? null];
    }
    return $cache = $rows;
}

// ── Corporation standings queries ────────────────────────────────────────────

/**
 * Load all corporation standings (from ESI /corporations/{id}/standings).
 * Returns standings for all tracked corporations, grouped by corporation_id.
 */
function db_corp_standings_all(): array
{
    try {
        return db_select(
            'SELECT corporation_id, from_id, from_type, standing, fetched_at
             FROM corp_standings
             ORDER BY corporation_id ASC, from_type ASC, standing DESC'
        );
    } catch (Throwable) {
        return [];
    }
}

/**
 * Load corporation standings for a specific corporation.
 */
function db_corp_standings_for(int $corporationId): array
{
    try {
        return db_select(
            'SELECT from_id, from_type, standing, fetched_at
             FROM corp_standings
             WHERE corporation_id = ?
             ORDER BY from_type ASC, standing DESC',
            [$corporationId]
        );
    } catch (Throwable) {
        return [];
    }
}

/**
 * Load faction standings for tracked corporations.
 * These are the most useful for understanding political alignment.
 */
function db_corp_standings_factions(): array
{
    try {
        return db_select(
            "SELECT cs.corporation_id, cs.from_id, cs.standing, cs.fetched_at,
                    COALESCE(emc.entity_name, CONCAT('Faction #', cs.from_id)) AS faction_name
             FROM corp_standings cs
             LEFT JOIN entity_metadata_cache emc
                ON emc.entity_id = cs.from_id AND emc.entity_type = 'faction'
             WHERE cs.from_type = 'faction'
             ORDER BY cs.corporation_id ASC, cs.standing DESC"
        );
    } catch (Throwable) {
        return [];
    }
}

/**
 * Detect standing discrepancies for a theater.
 *
 * Compares how alliances are classified (friendly/opponent) vs how they
 * actually behave in a theater (who they kill/get killed by).
 * A "betrayal" is when a friendly alliance kills more friendlies than opponents,
 * or when an opponent alliance helps friendlies more than it hurts them.
 *
 * @param array $allianceSummary  The theater_alliance_summary rows
 * @param array $killmails        Raw killmail data for cross-reference
 * @param callable $classifyFn    The $classifyAlliance closure
 * @return array Map of alliance_id/corporation_id → discrepancy info
 */
function compute_standing_discrepancies(array $allianceSummary, callable $classifyFn): array
{
    $discrepancies = [];

    foreach ($allianceSummary as $a) {
        $aid = (int) ($a['alliance_id'] ?? 0);
        $corpId = (int) ($a['corporation_id'] ?? 0);
        $side = $classifyFn($aid, $corpId);
        $kills = (int) ($a['total_kills'] ?? 0);
        $losses = (int) ($a['total_losses'] ?? 0);
        $iskKilled = (float) ($a['total_isk_killed'] ?? 0);
        $iskLost = (float) ($a['total_isk_lost'] ?? 0);

        // Skip entities with no significant activity
        if ($kills + $losses < 2) {
            continue;
        }

        // We can't determine who they killed from the summary alone,
        // so we flag based on the relationship graph data if available.
        $key = $aid > 0 ? "a:{$aid}" : "c:{$corpId}";
        $discrepancies[$key] = [
            'alliance_id' => $aid,
            'corporation_id' => $corpId,
            'configured_side' => $side,
            'kills' => $kills,
            'losses' => $losses,
            'isk_killed' => $iskKilled,
            'isk_lost' => $iskLost,
            'discrepancy_type' => null,
        ];
    }

    return $discrepancies;
}

/**
 * Detect betrayals by analyzing cross-side kill relationships within a theater.
 *
 * Examines theater participants to find alliances classified as 'friendly'
 * that have hostile interactions against other friendlies, or 'opponent'
 * alliances cooperating with friendlies.
 *
 * @param int $theaterId
 * @return array Alliance/corp IDs with betrayal indicators
 */
function db_theater_standing_discrepancies(string $theaterId): array
{
    try {
        // Find cases where a "friendly" entity killed another "friendly" entity
        // by looking at killmail attacker/victim alliance pairs within the theater.
        // Exclude AOE weapons (smartbombs group=141, bombs group=1015) — splash
        // damage is not intentional hostile action and does not affect standings.
        return db_select(
            "SELECT
                ka.alliance_id AS attacker_alliance_id,
                ka.corporation_id AS attacker_corporation_id,
                ke.victim_alliance_id,
                ke.victim_corporation_id,
                COUNT(DISTINCT ke.killmail_id) AS cross_kills,
                SUM(COALESCE(ke.zkb_total_value, 0)) AS cross_isk
             FROM theater_battles tb
             JOIN killmail_events ke ON ke.battle_id = tb.battle_id
             JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id
             WHERE tb.theater_id = ?
               AND ka.alliance_id > 0
               AND ke.victim_alliance_id > 0
               AND ka.alliance_id != ke.victim_alliance_id
               AND (ka.weapon_type_id IS NULL OR ka.weapon_type_id NOT IN (
                   SELECT rit.type_id FROM ref_item_types rit WHERE rit.group_id IN (141, 1015)
               ))
             GROUP BY ka.alliance_id, ka.corporation_id,
                      ke.victim_alliance_id, ke.victim_corporation_id
             HAVING cross_kills >= 1
             ORDER BY cross_kills DESC",
            [$theaterId]
        );
    } catch (Throwable) {
        return [];
    }
}

// ── Corporation contacts queries (player diplomatic standings) ───────────────

/**
 * Load all player contacts from corp_contacts.
 * These are the in-game diplomatic standings toward player entities.
 * Positive standing = blue (friendly), negative = red (hostile).
 */
function db_corp_contacts_all(): array
{
    try {
        return db_select(
            "SELECT corporation_id, contact_id, contact_type, standing, label_ids, COALESCE(source, 'esi') AS source, fetched_at
             FROM corp_contacts
             ORDER BY source ASC, contact_type ASC, standing DESC"
        );
    } catch (Throwable) {
        // Fallback: source column may not exist yet (migration pending).
        try {
            return array_map(
                static fn (array $row): array => array_merge($row, ['source' => 'esi']),
                db_select(
                    "SELECT corporation_id, contact_id, contact_type, standing, label_ids, 'esi' AS source, fetched_at
                     FROM corp_contacts
                     ORDER BY contact_type ASC, standing DESC"
                )
            );
        } catch (Throwable) {
            return [];
        }
    }
}

/**
 * Load player contacts classified by standing polarity.
 * Returns a structured array with alliance_ids and corporation_ids
 * grouped as friendly (standing > 0) or hostile (standing < 0).
 *
 * This is the primary function used for side classification from in-game standings.
 *
 * @return array{friendly_alliance_ids: int[], friendly_corporation_ids: int[],
 *               hostile_alliance_ids: int[], hostile_corporation_ids: int[]}
 */
function db_corp_contacts_by_standing(): array
{
    $result = [
        'friendly_alliance_ids' => [],
        'friendly_corporation_ids' => [],
        'hostile_alliance_ids' => [],
        'hostile_corporation_ids' => [],
    ];

    try {
        $contacts = db_select(
            "SELECT contact_id, contact_type, standing
             FROM corp_contacts
             WHERE contact_type IN ('alliance', 'corporation')
               AND standing != 0
             ORDER BY ABS(standing) DESC"
        );
    } catch (Throwable) {
        return $result;
    }

    foreach ($contacts as $c) {
        $contactId = (int) ($c['contact_id'] ?? 0);
        $contactType = (string) ($c['contact_type'] ?? '');
        $standing = (float) ($c['standing'] ?? 0);

        if ($contactId <= 0) {
            continue;
        }

        if ($standing > 0) {
            if ($contactType === 'alliance') {
                $result['friendly_alliance_ids'][] = $contactId;
            } elseif ($contactType === 'corporation') {
                $result['friendly_corporation_ids'][] = $contactId;
            }
        } elseif ($standing < 0) {
            if ($contactType === 'alliance') {
                $result['hostile_alliance_ids'][] = $contactId;
            } elseif ($contactType === 'corporation') {
                $result['hostile_corporation_ids'][] = $contactId;
            }
        }
    }

    return $result;
}

/**
 * Get the in-game standing for a specific alliance or corporation
 * from corp_contacts. Returns the standing value or null if not found.
 */
function db_corp_contact_standing(int $entityId, string $entityType = 'alliance'): ?float
{
    try {
        $row = db_select_one(
            'SELECT standing FROM corp_contacts
             WHERE contact_id = ? AND contact_type = ?
             LIMIT 1',
            [$entityId, $entityType]
        );
        return $row !== null ? (float) $row['standing'] : null;
    } catch (Throwable) {
        return null;
    }
}

/**
 * Ensure the source column exists on corp_contacts (handles pre-migration tables).
 */
function db_corp_contacts_ensure_source_column(): void
{
    try {
        db_execute(
            "ALTER TABLE corp_contacts
                ADD COLUMN source ENUM('esi', 'manual') NOT NULL DEFAULT 'esi' AFTER label_ids,
                ADD KEY idx_source (source)"
        );
    } catch (Throwable) {
        // Column already exists — nothing to do.
    }
}

/**
 * Replace all manual corp contacts with the given list.
 * Each entry: ['contact_id' => int, 'contact_type' => string, 'standing' => float]
 */
function db_corp_contacts_manual_replace(array $contacts): void
{
    // Ensure the source column exists before writing manual contacts.
    db_corp_contacts_ensure_source_column();

    db_execute('DELETE FROM corp_contacts WHERE source = ?', ['manual']);

    foreach ($contacts as $c) {
        $contactId = (int) ($c['contact_id'] ?? 0);
        $contactType = (string) ($c['contact_type'] ?? '');
        $standing = (float) ($c['standing'] ?? 0);

        if ($contactId <= 0 || !in_array($contactType, ['alliance', 'corporation'], true) || $standing == 0) {
            continue;
        }

        db_execute(
            "INSERT INTO corp_contacts (corporation_id, contact_id, contact_type, standing, source, fetched_at)
             VALUES (0, ?, ?, ?, 'manual', UTC_TIMESTAMP())
             ON DUPLICATE KEY UPDATE
                standing = VALUES(standing),
                source = 'manual',
                updated_at = CURRENT_TIMESTAMP",
            [$contactId, $contactType, $standing]
        );
    }
}

/**
 * Fetch all manually added corp contacts.
 */
function db_corp_contacts_manual(): array
{
    try {
        return db_select(
            "SELECT contact_id, contact_type, standing
             FROM corp_contacts
             WHERE source = 'manual'
             ORDER BY standing DESC, contact_type ASC"
        );
    } catch (Throwable) {
        return [];
    }
}

// ── Economic Warfare queries ─────────────────────────────────────────────────

function db_economic_warfare_scores(array $filters = [], int $limit = 100, int $offset = 0): array
{
    $where = ['1 = 1'];
    $params = [];

    if (isset($filters['min_score']) && (float) $filters['min_score'] > 0) {
        $where[] = 'ew.economic_warfare_score >= ?';
        $params[] = (float) $filters['min_score'];
    }
    if (isset($filters['group_id']) && (int) $filters['group_id'] > 0) {
        $where[] = 'ew.group_id = ?';
        $params[] = (int) $filters['group_id'];
    }
    if (isset($filters['meta_group_id']) && (int) $filters['meta_group_id'] > 0) {
        $where[] = 'ew.meta_group_id = ?';
        $params[] = (int) $filters['meta_group_id'];
    }
    if (isset($filters['hostile_alliance_id']) && (int) $filters['hostile_alliance_id'] > 0) {
        $where[] = 'ew.type_id IN (
            SELECT hfm.item_type_id FROM hostile_fit_family_modules hfm
            INNER JOIN hostile_fit_families hff ON hff.id = hfm.family_id
            WHERE JSON_CONTAINS(hff.alliance_ids_json, ?)
        )';
        $params[] = json_encode((int) $filters['hostile_alliance_id']);
    }

    $whereSql = implode(' AND ', $where);
    $params[] = max(1, min(500, $limit));
    $params[] = max(0, $offset);

    return db_select(
        "SELECT ew.* FROM economic_warfare_scores ew
         WHERE {$whereSql}
         ORDER BY ew.economic_warfare_score DESC
         LIMIT ? OFFSET ?",
        $params
    );
}

function db_economic_warfare_summary(): array
{
    $row = db_select_one(
        "SELECT
            COUNT(*) AS modules_scored,
            SUM(hostile_family_count) AS total_family_refs,
            MAX(economic_warfare_score) AS max_score,
            AVG(economic_warfare_score) AS avg_score,
            AVG(replacement_friction_score) AS avg_replacement_friction,
            MAX(computed_at) AS computed_at
         FROM economic_warfare_scores
         WHERE economic_warfare_score > 0"
    );
    return is_array($row) ? $row : [];
}

function db_hostile_fit_families(array $filters = [], int $limit = 100, int $offset = 0): array
{
    $where = ['1 = 1'];
    $params = [];

    if (isset($filters['hull_type_id']) && (int) $filters['hull_type_id'] > 0) {
        $where[] = 'hff.hull_type_id = ?';
        $params[] = (int) $filters['hull_type_id'];
    }
    if (isset($filters['hostile_alliance_id']) && (int) $filters['hostile_alliance_id'] > 0) {
        $where[] = 'JSON_CONTAINS(hff.alliance_ids_json, ?)';
        $params[] = json_encode((int) $filters['hostile_alliance_id']);
    }
    if (isset($filters['min_confidence']) && (float) $filters['min_confidence'] > 0) {
        $where[] = 'hff.confidence >= ?';
        $params[] = (float) $filters['min_confidence'];
    }

    $whereSql = implode(' AND ', $where);
    $params[] = max(1, min(500, $limit));
    $params[] = max(0, $offset);

    return db_select(
        "SELECT hff.*, rit.type_name AS hull_name
         FROM hostile_fit_families hff
         LEFT JOIN ref_item_types rit ON rit.type_id = hff.hull_type_id
         WHERE {$whereSql}
         ORDER BY hff.observation_count DESC
         LIMIT ? OFFSET ?",
        $params
    );
}

function db_hostile_fit_family_modules(int $familyId): array
{
    return db_select(
        "SELECT hfm.*, rit.type_name
         FROM hostile_fit_family_modules hfm
         LEFT JOIN ref_item_types rit ON rit.type_id = hfm.item_type_id
         WHERE hfm.family_id = ?
         ORDER BY hfm.is_core DESC, hfm.frequency DESC",
        [$familyId]
    );
}

function db_economic_warfare_module_drilldown(int $typeId): array
{
    $families = db_select(
        "SELECT hff.id, hff.hull_type_id, hff.observation_count, hff.confidence,
                hff.alliance_ids_json, rit.type_name AS hull_name,
                hfm.frequency, hfm.is_core, hfm.flag_category
         FROM hostile_fit_family_modules hfm
         INNER JOIN hostile_fit_families hff ON hff.id = hfm.family_id
         LEFT JOIN ref_item_types rit ON rit.type_id = hff.hull_type_id
         WHERE hfm.item_type_id = ?
         ORDER BY hff.observation_count DESC
         LIMIT 50",
        [$typeId]
    );

    $score = db_select_one(
        "SELECT * FROM economic_warfare_scores WHERE type_id = ? LIMIT 1",
        [$typeId]
    );

    $substitutes = [];
    if (is_array($score) && (int) ($score['group_id'] ?? 0) > 0) {
        $substitutes = db_select(
            "SELECT rit.type_id, rit.type_name, rit.meta_group_id,
                    ew.economic_warfare_score, ew.fit_constraint_score
             FROM ref_item_types rit
             LEFT JOIN economic_warfare_scores ew ON ew.type_id = rit.type_id
             WHERE rit.group_id = ? AND rit.type_id != ? AND rit.published = 1
             ORDER BY ew.economic_warfare_score DESC
             LIMIT 20",
            [(int) $score['group_id'], $typeId]
        );
    }

    return [
        'score' => $score,
        'families' => $families,
        'substitutes' => $substitutes,
    ];
}

function db_economic_warfare_hostile_alliances(): array
{
    // Single query with LEFT JOIN replaces N+1 pattern (one query per alliance_id)
    $rows = db_select(
        "SELECT DISTINCT jt.alliance_id,
                COALESCE(emc.entity_name, CONCAT('Alliance #', jt.alliance_id)) AS label
         FROM hostile_fit_families hff,
              JSON_TABLE(hff.alliance_ids_json, '\$[*]' COLUMNS (alliance_id BIGINT PATH '\$')) jt
         LEFT JOIN entity_metadata_cache emc
           ON emc.entity_type = 'alliance' AND emc.entity_id = jt.alliance_id
         WHERE jt.alliance_id > 0
         ORDER BY jt.alliance_id ASC
         LIMIT 200"
    );

    $result = [];
    foreach ($rows as $row) {
        $aid = (int) ($row['alliance_id'] ?? 0);
        if ($aid <= 0) {
            continue;
        }
        $result[] = [
            'alliance_id' => $aid,
            'label' => (string) ($row['label'] ?? 'Alliance #' . $aid),
        ];
    }

    return $result;
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
            SELECT cc.contact_id
            FROM corp_contacts cc
            WHERE cc.contact_type = 'alliance' AND cc.standing > 0
        )
        OR {$eventAlias}.victim_corporation_id IN (
            SELECT cc.contact_id
            FROM corp_contacts cc
            WHERE cc.contact_type = 'corporation' AND cc.standing > 0
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
            FROM corp_contacts cc
            INNER JOIN killmail_events e
                ON e.victim_alliance_id = cc.contact_id{$effectiveFilterSql}{$sequenceFilterSql}
            WHERE cc.contact_type = 'alliance' AND cc.standing > 0

            UNION ALL

            SELECT
                e.sequence_id,
                0 AS matches_victim_alliance,
                1 AS matches_victim_corporation,
                0 AS matches_attacker_alliance,
                0 AS matches_attacker_corporation
            FROM corp_contacts cc
            INNER JOIN killmail_events e
                ON e.victim_corporation_id = cc.contact_id{$effectiveFilterSql}{$sequenceFilterSql}
            WHERE cc.contact_type = 'corporation' AND cc.standing > 0
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
        "SELECT
            (SELECT COUNT(DISTINCT contact_id) FROM corp_contacts WHERE contact_type = 'alliance' AND standing > 0) AS tracked_alliance_count,
            (SELECT COUNT(DISTINCT contact_id) FROM corp_contacts WHERE contact_type = 'corporation' AND standing > 0) AS tracked_corporation_count"
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
    $latestSequencesSql = db_killmail_latest_sequences_sql();

    return db_select(
        "SELECT e.sequence_id, e.killmail_id, e.killmail_hash, e.uploaded_at, e.killmail_time,
                e.victim_alliance_id, e.victim_corporation_id, e.victim_ship_type_id, e.solar_system_id, e.region_id
         FROM {$trackedMatchesSql} tracked
         INNER JOIN {$latestSequencesSql} latest ON latest.sequence_id = tracked.sequence_id
         INNER JOIN killmail_events e ON e.sequence_id = tracked.sequence_id
         ORDER BY e.effective_killmail_at DESC, e.sequence_id DESC, e.killmail_id DESC
         LIMIT {$limit}"
    );
}

function db_killmail_overview_summary(int $recentHours = 24, string $startDate = '2024-01-01 00:00:00'): array
{
    $safeRecentHours = max(1, min(24 * 30, $recentHours));

    // The uniq_killmail_identity (killmail_id, killmail_hash) unique key is
    // enforced on write by db_killmail_event_upsert() via ON DUPLICATE KEY
    // UPDATE, so there is never more than one row per identity. Scanning
    // killmail_events directly is safe and orders of magnitude faster than
    // wrapping it in a GROUP BY derived table (db_killmail_latest_sequences_sql).
    //
    // `recent_count` powers the "Recent Ingestion — Stored in the last N hours"
    // card on the overview dashboard, so it must filter on the row-insertion
    // timestamp (`created_at`), NOT `effective_killmail_at`. The latter is a
    // generated column `COALESCE(killmail_time, created_at)` that resolves to
    // the in-game kill time, so every backfill write (EveRef tarball imports,
    // character_killmail_sync, killmail_history_backfill, ...) lands with an
    // old `effective_killmail_at` and was invisible to this counter even
    // though the row had just been stored. The end result was a permanent
    // "Recent Ingestion: 0" display whenever the R2Z2 live stream happened
    // to be quiet, despite the total count and sync freshness both moving.
    $summary = db_select_one(
        "SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN e.created_at >= (UTC_TIMESTAMP() - INTERVAL {$safeRecentHours} HOUR) THEN 1 ELSE 0 END) AS recent_count,
            MAX(e.sequence_id) AS max_sequence_id,
            MAX(e.created_at) AS last_ingested_at,
            MAX(e.uploaded_at) AS latest_uploaded_at
         FROM killmail_events e
         WHERE e.effective_killmail_at >= ?",
        [$startDate]
    ) ?? [];

    // Tracked match count: killmails whose victim is in corp_contacts with
    // standing > 0. Semi-join against the tiny corp_contacts table instead
    // of the expensive UNION+GROUP BY derived table.
    $trackedCountRow = db_select_one(
        "SELECT COUNT(*) AS tracked_match_count
         FROM killmail_events e
         WHERE e.effective_killmail_at >= ?
           AND (
             e.victim_alliance_id IN (
                 SELECT contact_id FROM corp_contacts
                 WHERE contact_type = 'alliance' AND standing > 0
             )
             OR e.victim_corporation_id IN (
                 SELECT contact_id FROM corp_contacts
                 WHERE contact_type = 'corporation' AND standing > 0
             )
           )",
        [$startDate]
    ) ?? [];
    $summary['tracked_match_count'] = (int) ($trackedCountRow['tracked_match_count'] ?? 0);
    $summary['start_date'] = $startDate;

    // Breakdown by mail_type so the UI can show what we're actually
    // storing (tracked losses/kills vs. opponent vs. third_party vs.
    // untracked).  Untracked rows are pruned after 90 days by the
    // killmail_untracked_retention job, so their count reflects the
    // rolling window not the all-time total.
    $mailTypeRows = db_select(
        "SELECT e.mail_type, COUNT(*) AS mail_count
         FROM killmail_events e
         WHERE e.effective_killmail_at >= ?
         GROUP BY e.mail_type",
        [$startDate]
    ) ?? [];
    $mailTypeCounts = [
        'loss' => 0,
        'kill' => 0,
        'opponent_loss' => 0,
        'opponent_kill' => 0,
        'third_party' => 0,
        'untracked' => 0,
    ];
    foreach ($mailTypeRows as $mailTypeRow) {
        $type = (string) ($mailTypeRow['mail_type'] ?? '');
        if ($type !== '' && array_key_exists($type, $mailTypeCounts)) {
            $mailTypeCounts[$type] = (int) ($mailTypeRow['mail_count'] ?? 0);
        }
    }
    $summary['mail_type_counts'] = $mailTypeCounts;

    return $summary;
}

function db_killmail_overview_filter_options(string $startDate = '2024-01-01 00:00:00'): array
{
    // See db_killmail_overview_summary() for why we scan killmail_events
    // directly instead of wrapping it in a latest-sequences derived table.
    $alliances = db_select(
        "SELECT DISTINCT
            e.victim_alliance_id AS entity_id,
            COALESCE(emc.entity_name, CONCAT('Alliance #', e.victim_alliance_id)) AS entity_label
         FROM killmail_events e
         LEFT JOIN entity_metadata_cache emc
           ON emc.entity_type = 'alliance' AND emc.entity_id = e.victim_alliance_id
         WHERE e.victim_alliance_id IS NOT NULL
           AND e.victim_alliance_id > 0
           AND e.effective_killmail_at >= ?
         ORDER BY entity_label ASC
         LIMIT 200",
        [$startDate]
    );

    $corporations = db_select(
        "SELECT DISTINCT
            e.victim_corporation_id AS entity_id,
            COALESCE(emc.entity_name, CONCAT('Corporation #', e.victim_corporation_id)) AS entity_label
         FROM killmail_events e
         LEFT JOIN entity_metadata_cache emc
           ON emc.entity_type = 'corporation' AND emc.entity_id = e.victim_corporation_id
         WHERE e.victim_corporation_id IS NOT NULL
           AND e.victim_corporation_id > 0
           AND e.effective_killmail_at >= ?
         ORDER BY entity_label ASC
         LIMIT 200",
        [$startDate]
    );

    return [
        'alliances' => $alliances,
        'corporations' => $corporations,
    ];
}

function db_killmail_overview_monthly_histogram(string $startDate = '2024-01-01'): array
{
    // See db_killmail_overview_summary() for why we scan killmail_events
    // directly. With idx_killmail_mailtype_effective_seq this becomes a
    // range scan that is covered end-to-end by the index.
    return db_select(
        "SELECT DATE_FORMAT(e.effective_killmail_at, '%Y-%m') AS bucket_month,
                COUNT(*) AS loss_count
         FROM killmail_events e
         WHERE e.mail_type = 'loss'
           AND e.effective_killmail_at >= ?
         GROUP BY DATE_FORMAT(e.effective_killmail_at, '%Y-%m')
         ORDER BY bucket_month ASC",
        [$startDate . ' 00:00:00']
    );
}

function db_killmail_esi_coverage_snapshot(string $startDate = '2024-01-01'): array
{
    $startAt = $startDate . ' 00:00:00';

    // Reusable participant subquery (victim + attacker character IDs from
    // killmails effective on or after the start date).  Every coverage metric
    // joins against this so all numbers share the same denominator.
    $participantSubquery = "(
        SELECT ke.victim_character_id AS character_id
        FROM killmail_events ke
        WHERE ke.victim_character_id IS NOT NULL
          AND ke.victim_character_id > 0
          AND ke.effective_killmail_at >= ?
        UNION
        SELECT ka.character_id AS character_id
        FROM killmail_attackers ka
        INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
        WHERE ka.character_id IS NOT NULL
          AND ka.character_id > 0
          AND ke.effective_killmail_at >= ?
    ) participant";

    $participantCount = (int) db_fetch_single_value(
        "SELECT COUNT(DISTINCT participant.character_id) FROM {$participantSubquery}",
        [$startAt, $startAt]
    );

    // esi_character_queue coverage with fetch_status breakdown.
    $queueStatusRows = db_select(
        "SELECT q.fetch_status, COUNT(*) AS n
         FROM esi_character_queue q
         INNER JOIN {$participantSubquery} ON participant.character_id = q.character_id
         GROUP BY q.fetch_status",
        [$startAt, $startAt]
    );
    $queueStatusCounts = ['pending' => 0, 'done' => 0, 'error' => 0];
    foreach ($queueStatusRows as $row) {
        $status = (string) ($row['fetch_status'] ?? '');
        if (isset($queueStatusCounts[$status])) {
            $queueStatusCounts[$status] = (int) $row['n'];
        }
    }
    $queuedCount = array_sum($queueStatusCounts);

    $affiliationCount = (int) db_fetch_single_value(
        "SELECT COUNT(*)
         FROM character_current_affiliation a
         INNER JOIN {$participantSubquery} ON participant.character_id = a.character_id",
        [$startAt, $startAt]
    );

    // "History processed" = sync job actually ran for this character, even if
    // they had zero alliance periods to store.  This is the real indicator of
    // pipeline health, distinct from the row-count-based metric below.
    $historyRefreshDoneCount = (int) db_fetch_single_value(
        "SELECT COUNT(*)
         FROM character_current_affiliation a
         INNER JOIN {$participantSubquery} ON participant.character_id = a.character_id
         WHERE a.last_history_refresh_at IS NOT NULL",
        [$startAt, $startAt]
    );

    // "Has at least one alliance history row" — naturally lower than the
    // refresh count because many characters have never joined an alliance.
    $allianceHistoryCount = (int) db_fetch_single_value(
        "SELECT COUNT(DISTINCT h.character_id)
         FROM character_alliance_history h
         INNER JOIN {$participantSubquery} ON participant.character_id = h.character_id",
        [$startAt, $startAt]
    );

    // Per-character killmail backfill queue: do participants have an entry,
    // and how many have actually been drained to completion?
    $killmailQueueTotal = (int) db_fetch_single_value(
        "SELECT COUNT(*)
         FROM character_killmail_queue ckq
         INNER JOIN {$participantSubquery} ON participant.character_id = ckq.character_id",
        [$startAt, $startAt]
    );

    $killmailQueueStatusRows = db_select(
        "SELECT ckq.status, COUNT(*) AS n
         FROM character_killmail_queue ckq
         INNER JOIN {$participantSubquery} ON participant.character_id = ckq.character_id
         GROUP BY ckq.status",
        [$startAt, $startAt]
    );
    $killmailQueueStatusCounts = ['pending' => 0, 'processing' => 0, 'done' => 0, 'error' => 0];
    foreach ($killmailQueueStatusRows as $row) {
        $status = (string) ($row['status'] ?? '');
        if (isset($killmailQueueStatusCounts[$status])) {
            $killmailQueueStatusCounts[$status] = (int) $row['n'];
        }
    }

    $killmailBackfillCompleteCount = (int) db_fetch_single_value(
        "SELECT COUNT(*)
         FROM character_killmail_queue ckq
         INNER JOIN {$participantSubquery} ON participant.character_id = ckq.character_id
         WHERE ckq.backfill_complete = 1",
        [$startAt, $startAt]
    );

    $missingAffiliation = max(0, $participantCount - $affiliationCount);
    $missingAllianceHistory = max(0, $participantCount - $allianceHistoryCount);
    $missingHistoryRefresh = max(0, $participantCount - $historyRefreshDoneCount);
    $missingKillmailQueue = max(0, $participantCount - $killmailQueueTotal);

    return [
        'start_date' => $startDate,
        'participant_characters' => $participantCount,
        // ESI queue coverage
        'queued_in_esi_character_queue' => $queuedCount,
        'esi_queue_pending' => $queueStatusCounts['pending'],
        'esi_queue_done' => $queueStatusCounts['done'],
        'esi_queue_error' => $queueStatusCounts['error'],
        // Affiliation + alliance history
        'with_current_affiliation' => $affiliationCount,
        'with_alliance_history_row' => $allianceHistoryCount,
        'with_alliance_history' => $allianceHistoryCount, // legacy alias
        'history_refresh_completed' => $historyRefreshDoneCount,
        // Per-character killmail backfill queue
        'enrolled_for_killmail_backfill' => $killmailQueueTotal,
        'killmail_backfill_pending' => $killmailQueueStatusCounts['pending'],
        'killmail_backfill_processing' => $killmailQueueStatusCounts['processing'],
        'killmail_backfill_done' => $killmailQueueStatusCounts['done'],
        'killmail_backfill_error' => $killmailQueueStatusCounts['error'],
        'killmail_backfill_complete_flag' => $killmailBackfillCompleteCount,
        // Gap metrics
        'missing_current_affiliation' => $missingAffiliation,
        'missing_alliance_history' => $missingAllianceHistory,
        'missing_history_refresh' => $missingHistoryRefresh,
        'missing_killmail_backfill_enrollment' => $missingKillmailQueue,
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
            CONCAT(CAST(e.killmail_id AS CHAR), ':', e.killmail_hash) AS esi_killmail_key,
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
            '' AS victim_corporation_label,
            '' AS victim_alliance_label,
            COALESCE(NULLIF(ship.type_name, ''), '') AS ship_type_name,
            COALESCE(NULLIF(system_ref.system_name, ''), '') AS system_name,
            COALESCE(NULLIF(region_ref.region_name, ''), '') AS region_name,
            CASE WHEN victim_ta.contact_id IS NULL THEN 0 ELSE 1 END AS matches_victim_alliance,
            CASE WHEN victim_tc.contact_id IS NULL THEN 0 ELSE 1 END AS matches_victim_corporation,
            COALESCE(tracked.matches_attacker_alliance, 0) AS matches_attacker_alliance,
            COALESCE(tracked.matches_attacker_corporation, 0) AS matches_attacker_corporation,
            CASE WHEN {$matchSql} THEN 1 ELSE 0 END AS matched_tracked
         FROM killmail_events e
         LEFT JOIN {$trackedMatchesSql} tracked
           ON tracked.sequence_id = e.sequence_id
         LEFT JOIN ref_item_types ship ON ship.type_id = e.victim_ship_type_id
         LEFT JOIN ref_systems system_ref ON system_ref.system_id = e.solar_system_id
         LEFT JOIN ref_regions region_ref ON region_ref.region_id = e.region_id
         LEFT JOIN corp_contacts victim_ta
           ON victim_ta.contact_id = e.victim_alliance_id
          AND victim_ta.contact_type = 'alliance' AND victim_ta.standing > 0
         LEFT JOIN corp_contacts victim_tc
           ON victim_tc.contact_id = e.victim_corporation_id
          AND victim_tc.contact_type = 'corporation' AND victim_tc.standing > 0
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
    $mailType = in_array((string) ($filters['mail_type'] ?? 'loss'), ['kill', 'loss', ''], true) ? (string) ($filters['mail_type'] ?? 'loss') : 'loss';
    $search = trim((string) ($filters['search'] ?? ''));
    $startDate = trim((string) ($filters['start_date'] ?? '2024-01-01 00:00:00'));

    // Keyset pagination support (no-search path only). Without a cursor this
    // still runs as "keyset mode at the head" — LIMIT pageSize+1 with no extra
    // WHERE clause — so the head view also gets a next-cursor for the Next
    // button. When a cursor IS provided, pagination is driven by
    // (effective_killmail_at, sequence_id) instead of LIMIT/OFFSET so deep-page
    // navigation never scans skipped rows.
    //   direction='next'  → rows strictly older than the cursor
    //   direction='prev'  → rows strictly newer than the cursor (then reversed)
    // $skipCount lets callers opt out of the 1.5M-row COUNT(*) when they plan
    // to fetch the total asynchronously (see public/api/killmail-intelligence/count.php).
    $cursor = trim((string) ($filters['cursor'] ?? ''));
    $direction = (string) ($filters['direction'] ?? 'next');
    if ($direction !== 'prev') {
        $direction = 'next';
    }
    $skipCount = (bool) ($filters['skip_count'] ?? false);
    $useKeyset = $search === '';
    [$cursorAt, $cursorSeq] = db_killmail_overview_parse_cursor($cursor);
    $hasCursor = $cursorAt !== null && $cursorSeq > 0;
    if (!$hasCursor) {
        // Keyset still active for fetch-limit and cursor generation, but the
        // WHERE clause below only fires when $hasCursor is true.
        $cursorAt = null;
        $cursorSeq = 0;
    }

    // --- Base predicates (apply to both COUNT and row fetch) ---
    //
    // This function used to wrap killmail_events in a GROUP BY derived table
    // (db_killmail_latest_sequences_sql) and also build a UNION+GROUP BY
    // derived table for tracked-match lookup (db_killmail_tracked_matches_sql)
    // as a side join. Both were full-table operations that got built on every
    // query even when they could not help the result. With the
    // uniq_killmail_identity unique key enforced at ingest, the latest-
    // sequence dedupe is redundant; and tracked-match state can be computed
    // per-row with EXISTS against the tiny corp_contacts table.
    $baseConditions = [];
    $baseParams = [];

    if ($allianceId > 0) {
        $baseConditions[] = 'e.victim_alliance_id = ?';
        $baseParams[] = $allianceId;
    }
    if ($corporationId > 0) {
        $baseConditions[] = 'e.victim_corporation_id = ?';
        $baseParams[] = $corporationId;
    }
    if ($mailType !== '') {
        $baseConditions[] = 'e.mail_type = ?';
        $baseParams[] = $mailType;
    }
    $baseConditions[] = 'e.effective_killmail_at >= ?';
    $baseParams[] = $startDate;

    if ($trackedOnly) {
        // Semi-join against corp_contacts instead of joining the big
        // UNION+GROUP BY derived table. corp_contacts is small and indexed
        // on (contact_type, contact_id), so the optimizer can hash it.
        $baseConditions[] = '(
            e.victim_alliance_id IN (
                SELECT contact_id FROM corp_contacts
                WHERE contact_type = \'alliance\' AND standing > 0
            )
            OR e.victim_corporation_id IN (
                SELECT contact_id FROM corp_contacts
                WHERE contact_type = \'corporation\' AND standing > 0
            )
        )';
    }

    // Display-side joins needed only when the row SELECT references them,
    // or when the search filter needs to reach into their columns.
    $displayJoinsSql = "
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
        LEFT JOIN entity_metadata_cache emc_va
          ON emc_va.entity_type = 'alliance' AND emc_va.entity_id = e.victim_alliance_id
        LEFT JOIN entity_metadata_cache emc_vc
          ON emc_vc.entity_type = 'corporation' AND emc_vc.entity_id = e.victim_corporation_id";

    // Search predicates reference the display joins (entity names, ship/
    // system/region labels), so their presence forces those joins into the
    // COUNT path too. When there is no search term the count query can be
    // a plain range scan on idx_killmail_mailtype_effective_seq.
    $searchConditions = [];
    $searchParams = [];
    if ($search !== '') {
        $needle = '%' . $search . '%';
        $numericSearch = preg_replace('/[^0-9]/', '', $search);
        if ($numericSearch !== '' && $numericSearch !== '0') {
            // Sargable numeric range for sequence_id/killmail_id instead of CAST(...AS CHAR) LIKE
            $lowerBound = (int) $numericSearch;
            $upperBound = (int) ($numericSearch . str_repeat('9', max(0, 15 - strlen($numericSearch))));
            $searchConditions[] = '(
                e.sequence_id BETWEEN ? AND ?
                OR e.killmail_id BETWEEN ? AND ?
                OR COALESCE(emc_va.entity_name, \'\') LIKE ?
                OR COALESCE(emc_vc.entity_name, \'\') LIKE ?
                OR COALESCE(ship.type_name, \'\') LIKE ?
                OR COALESCE(system_ref.system_name, \'\') LIKE ?
                OR COALESCE(region_ref.region_name, \'\') LIKE ?
            )';
            array_push($searchParams, $lowerBound, $upperBound, $lowerBound, $upperBound, $needle, $needle, $needle, $needle, $needle);
        } else {
            $searchConditions[] = '(
                COALESCE(emc_va.entity_name, \'\') LIKE ?
                OR COALESCE(emc_vc.entity_name, \'\') LIKE ?
                OR COALESCE(ship.type_name, \'\') LIKE ?
                OR COALESCE(system_ref.system_name, \'\') LIKE ?
                OR COALESCE(region_ref.region_name, \'\') LIKE ?
            )';
            array_push($searchParams, $needle, $needle, $needle, $needle, $needle);
        }
    }

    // --- COUNT query ---
    // Common path (no search): count straight off killmail_events. With
    // idx_killmail_mailtype_effective_seq this is a pure range scan with no
    // joins, no temp tables and no filesort. This is the change that fixes
    // the killmail-intelligence page timing out during backload.
    //
    // When $skipCount is true we return $totalItems=null and the caller fills
    // it in later via an async AJAX fetch. The keyset path always sets
    // $skipCount=true because "page N of M" is meaningless with cursor paging.
    $totalItems = null;
    $totalPages = null;
    if (!$skipCount && !$useKeyset) {
        if ($searchConditions === []) {
            $countWhereSql = ' WHERE ' . implode(' AND ', $baseConditions);
            $countRow = db_select_one(
                'SELECT COUNT(*) AS total_items FROM killmail_events e' . $countWhereSql,
                $baseParams
            );
        } else {
            $countConditions = array_merge($baseConditions, $searchConditions);
            $countWhereSql = ' WHERE ' . implode(' AND ', $countConditions);
            $countRow = db_select_one(
                'SELECT COUNT(*) AS total_items FROM killmail_events e' . $displayJoinsSql . $countWhereSql,
                array_merge($baseParams, $searchParams)
            );
        }
        $totalItems = (int) ($countRow['total_items'] ?? 0);
        $totalPages = max(1, (int) ceil($totalItems / $pageSize));
        $page = min($page, $totalPages);
        $offset = ($page - 1) * $pageSize;
    }

    // --- Row fetch ---
    // Needs the display joins (the SELECT references their columns) but the
    // expensive tracked-match derived table is replaced with per-row EXISTS
    // subqueries. These only run for the ~25 rows actually returned, so the
    // extra per-row cost is negligible. matches_attacker_* are always 0 in
    // this view: the old UNION-derived tracked table declared them but never
    // populated them (it only covered the victim side).
    $rowConditions = array_merge($baseConditions, $searchConditions);
    $rowParams = array_merge($baseParams, $searchParams);

    // Keyset predicate on (effective_killmail_at, sequence_id). "Next" walks
    // older rows; "prev" walks newer ones (then we reverse the result set in
    // PHP so output stays newest-first). Both branches use the existing
    // idx_killmail_mailtype_effective_seq index for a pure range scan.
    $orderSql = 'ORDER BY e.effective_killmail_at DESC, e.sequence_id DESC, e.killmail_id DESC';
    if ($useKeyset && $hasCursor) {
        if ($direction === 'prev') {
            $rowConditions[] = '(e.effective_killmail_at > ? OR (e.effective_killmail_at = ? AND e.sequence_id > ?))';
            $orderSql = 'ORDER BY e.effective_killmail_at ASC, e.sequence_id ASC, e.killmail_id ASC';
        } else {
            $rowConditions[] = '(e.effective_killmail_at < ? OR (e.effective_killmail_at = ? AND e.sequence_id < ?))';
        }
        $rowParams[] = $cursorAt;
        $rowParams[] = $cursorAt;
        $rowParams[] = $cursorSeq;
    }
    $rowWhereSql = ' WHERE ' . implode(' AND ', $rowConditions);
    // Fetch one extra row to detect has_next (or has_prev when going backward).
    $fetchLimit = $useKeyset ? $pageSize + 1 : $pageSize;
    $limitSql = $useKeyset
        ? "LIMIT {$fetchLimit}"
        : "LIMIT {$pageSize} OFFSET {$offset}";

    $rows = db_select(
        "SELECT
            e.sequence_id,
            e.killmail_id,
            e.killmail_hash,
            CONCAT(CAST(e.killmail_id AS CHAR), ':', e.killmail_hash) AS esi_killmail_key,
            e.killmail_time,
            e.uploaded_at,
            e.created_at,
            e.effective_killmail_at,
            e.victim_character_id,
            e.victim_corporation_id,
            e.victim_alliance_id,
            e.victim_ship_type_id,
            e.mail_type,
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
            COALESCE(emc_vc.entity_name, CONCAT('Corporation #', e.victim_corporation_id)) AS victim_corporation_label,
            COALESCE(emc_va.entity_name, CONCAT('Alliance #', e.victim_alliance_id)) AS victim_alliance_label,
            COALESCE(NULLIF(ship.type_name, ''), CONCAT('Type #', e.victim_ship_type_id)) AS ship_type_name,
            COALESCE(NULLIF(system_ref.system_name, ''), CONCAT('System #', e.solar_system_id)) AS system_name,
            COALESCE(NULLIF(region_ref.region_name, ''), CONCAT('Region #', e.region_id)) AS region_name,
            CASE WHEN EXISTS (
                SELECT 1 FROM corp_contacts cc
                 WHERE cc.contact_type = 'alliance' AND cc.standing > 0
                   AND cc.contact_id = e.victim_alliance_id
            ) THEN 1 ELSE 0 END AS matches_victim_alliance,
            CASE WHEN EXISTS (
                SELECT 1 FROM corp_contacts cc
                 WHERE cc.contact_type = 'corporation' AND cc.standing > 0
                   AND cc.contact_id = e.victim_corporation_id
            ) THEN 1 ELSE 0 END AS matches_victim_corporation,
            0 AS matches_attacker_alliance,
            0 AS matches_attacker_corporation,
            CASE WHEN EXISTS (
                SELECT 1 FROM corp_contacts cc
                 WHERE cc.contact_type = 'alliance' AND cc.standing > 0
                   AND cc.contact_id = e.victim_alliance_id
            ) OR EXISTS (
                SELECT 1 FROM corp_contacts cc
                 WHERE cc.contact_type = 'corporation' AND cc.standing > 0
                   AND cc.contact_id = e.victim_corporation_id
            ) THEN 1 ELSE 0 END AS matched_tracked
         FROM killmail_events e
         {$displayJoinsSql}
         {$rowWhereSql}
         {$orderSql}
         {$limitSql}",
        $rowParams
    );

    // Keyset post-processing: drop the "+1" probe row, detect has_more, flip
    // the "prev" direction back to newest-first, and compute the cursors the
    // UI needs for the next round of navigation.
    //
    // Semantics:
    //   hasMore=true means "extra row was returned, more data exists in the
    //   direction we were walking". For direction='next' that means more
    //   OLDER rows; for direction='prev' that means more NEWER rows.
    //   hasCursor=false means we're at the head (newest rows) — so Previous
    //   is hidden and Next is shown whenever there are more older rows.
    $hasMore = false;
    $nextCursor = null;
    $prevCursor = null;
    $showNext = false;
    $showPrev = false;
    if ($useKeyset) {
        if (count($rows) > $pageSize) {
            $hasMore = true;
            $rows = array_slice($rows, 0, $pageSize);
        }
        if ($direction === 'prev') {
            $rows = array_reverse($rows);
        }
        if ($rows !== []) {
            $firstRow = $rows[0];
            $lastRow = $rows[count($rows) - 1];
            $prevCursor = db_killmail_overview_build_cursor($firstRow);
            $nextCursor = db_killmail_overview_build_cursor($lastRow);
        }
        if (!$hasCursor) {
            // Head view: no Previous, Next only if more older rows exist.
            $showPrev = false;
            $showNext = $hasMore;
        } elseif ($direction === 'next') {
            // Walking older: Previous always (we came from newer). Next only
            // if more older rows remain.
            $showPrev = true;
            $showNext = $hasMore;
        } else {
            // Walking newer: Next always (we came from older). Previous only
            // if more newer rows remain.
            $showPrev = $hasMore;
            $showNext = true;
        }
    }

    $result = [
        'rows' => $rows,
        'page' => $page,
        'page_size' => $pageSize,
        'total_items' => $totalItems,
        'total_pages' => $totalPages,
        'use_keyset' => $useKeyset,
        'direction' => $direction,
        'next_cursor' => $showNext ? $nextCursor : null,
        'prev_cursor' => $showPrev ? $prevCursor : null,
    ];
    if ($totalItems !== null) {
        $result['showing_from'] = $totalItems > 0 ? $offset + 1 : 0;
        $result['showing_to'] = min($offset + $pageSize, $totalItems);
    } else {
        $result['showing_from'] = $rows === [] ? 0 : 1;
        $result['showing_to'] = count($rows);
    }

    return $result;
}

/**
 * Parse the opaque cursor used by the killmail overview keyset pagination.
 *
 * Format: "{Y-m-d H:i:s}|{sequence_id}" URL-safe-base64 encoded. Returning
 * [null, 0] means "no valid cursor" and the caller falls back to page 1.
 */
function db_killmail_overview_parse_cursor(string $cursor): array
{
    $cursor = trim($cursor);
    if ($cursor === '') {
        return [null, 0];
    }
    // Opaque URL-safe base64 so stray characters don't slip into the SQL
    // parameters. The structure inside is still a plain "at|seq" pair.
    $decoded = base64_decode(strtr($cursor, '-_', '+/'), true);
    if ($decoded === false) {
        return [null, 0];
    }
    $parts = explode('|', $decoded, 2);
    if (count($parts) !== 2) {
        return [null, 0];
    }
    $at = trim($parts[0]);
    $seq = (int) $parts[1];
    if ($at === '' || $seq <= 0) {
        return [null, 0];
    }
    // Reject obviously malformed datetimes so we don't pass garbage to MySQL.
    $ts = strtotime($at . ' UTC');
    if ($ts === false) {
        return [null, 0];
    }
    return [$at, $seq];
}

/**
 * Build the opaque cursor string for a killmail row. Pairs with
 * db_killmail_overview_parse_cursor().
 */
function db_killmail_overview_build_cursor(array $row): ?string
{
    $at = isset($row['effective_killmail_at']) ? trim((string) $row['effective_killmail_at']) : '';
    // effective_killmail_at is a generated column; fall back to killmail_time
    // then created_at for rows where the cursor column was not selected.
    if ($at === '') {
        $at = isset($row['killmail_time']) ? trim((string) $row['killmail_time']) : '';
    }
    if ($at === '') {
        $at = isset($row['created_at']) ? trim((string) $row['created_at']) : '';
    }
    $seq = isset($row['sequence_id']) ? (int) $row['sequence_id'] : 0;
    if ($at === '' || $seq <= 0) {
        return null;
    }
    return rtrim(strtr(base64_encode($at . '|' . $seq), '+/', '-_'), '=');
}

/**
 * Count-only variant used by the async count endpoint. Shares filter logic
 * with db_killmail_overview_page() but skips the row SELECT entirely.
 */
function db_killmail_overview_total_count(array $filters = []): int
{
    db_killmail_overview_schema_ensure();

    $allianceId = max(0, (int) ($filters['alliance_id'] ?? 0));
    $corporationId = max(0, (int) ($filters['corporation_id'] ?? 0));
    $trackedOnly = (bool) ($filters['tracked_only'] ?? false);
    $mailType = in_array((string) ($filters['mail_type'] ?? 'loss'), ['kill', 'loss', ''], true) ? (string) ($filters['mail_type'] ?? 'loss') : 'loss';
    $startDate = trim((string) ($filters['start_date'] ?? '2024-01-01 00:00:00'));

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
    if ($mailType !== '') {
        $conditions[] = 'e.mail_type = ?';
        $params[] = $mailType;
    }
    $conditions[] = 'e.effective_killmail_at >= ?';
    $params[] = $startDate;
    if ($trackedOnly) {
        $conditions[] = '(
            e.victim_alliance_id IN (
                SELECT contact_id FROM corp_contacts
                WHERE contact_type = \'alliance\' AND standing > 0
            )
            OR e.victim_corporation_id IN (
                SELECT contact_id FROM corp_contacts
                WHERE contact_type = \'corporation\' AND standing > 0
            )
        )';
    }

    $row = db_select_one(
        'SELECT COUNT(*) AS total_items FROM killmail_events e WHERE ' . implode(' AND ', $conditions),
        $params
    );
    return (int) ($row['total_items'] ?? 0);
}

function db_killmail_overview_duplicate_identity_check(array $filters = []): array
{
    $filters['page'] = 1;
    $filters['page_size'] = max(1, min(1000, (int) ($filters['page_size'] ?? 250)));
    $listing = db_killmail_overview_page($filters);
    $rows = array_values(array_filter((array) ($listing['rows'] ?? []), static fn (mixed $row): bool => is_array($row)));
    $counts = [];

    foreach ($rows as $row) {
        $key = trim((string) ($row['esi_killmail_key'] ?? ''));
        if ($key === '') {
            $key = (string) ((int) ($row['killmail_id'] ?? 0)) . ':' . (string) ($row['killmail_hash'] ?? '');
        }
        if ($key === ':' || $key === '') {
            continue;
        }
        $counts[$key] = ($counts[$key] ?? 0) + 1;
    }

    $duplicates = [];
    foreach ($counts as $key => $count) {
        if ($count > 1) {
            $duplicates[] = [
                'esi_killmail_key' => $key,
                'row_count' => $count,
            ];
        }
    }

    return $duplicates;
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
    $startedAt = microtime(true);
    item_scope_db_ensure_schema();

    $ids = array_values(array_unique(array_filter(array_map(static fn (mixed $id): int => (int) $id, $typeIds), static fn (int $id): bool => $id > 0)));
    if ($ids === []) {
        db_performance_track_call('db_ref_item_scope_metadata_by_ids', (microtime(true) - $startedAt) * 1000.0, ['ids_count' => 0]);
        return [];
    }
    $patternIds = $ids;
    sort($patternIds);

    $placeholders = implode(',', array_fill(0, count($ids), '?'));

    $rows = db_select(
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

    db_performance_track_call('db_ref_item_scope_metadata_by_ids', (microtime(true) - $startedAt) * 1000.0, ['ids_count' => count($ids), 'set' => md5(implode(',', $patternIds))]);

    return $rows;
}

function db_ref_item_scope_catalog(): array
{
    $startedAt = microtime(true);
    item_scope_db_ensure_schema();

    $catalog = [
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

    db_performance_track_call('db_ref_item_scope_catalog', (microtime(true) - $startedAt) * 1000.0);

    return $catalog;
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
    if (!db_runtime_schema_checks_enabled()) {
        return true;
    }

    static $cache = [];
    if (isset($cache[$tableName])) {
        return $cache[$tableName];
    }

    $row = db_select_one(
        'SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? LIMIT 1',
        [$tableName]
    );

    $exists = $row !== null;
    $cache[$tableName] = $exists;

    return $exists;
}

function db_column_exists(string $tableName, string $columnName): bool
{
    if (!db_runtime_schema_checks_enabled()) {
        return true;
    }

    static $cache = [];
    $key = $tableName . '.' . $columnName;
    if (isset($cache[$key])) {
        return $cache[$key];
    }

    $row = db_select_one(
        'SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1',
        [$tableName, $columnName]
    );

    $exists = $row !== null;
    $cache[$key] = $exists;

    return $exists;
}

function db_index_exists(string $tableName, string $indexName): bool
{
    if (!db_runtime_schema_checks_enabled()) {
        return true;
    }

    static $cache = [];
    $key = $tableName . '.' . $indexName;
    if (isset($cache[$key])) {
        return $cache[$key];
    }

    $row = db_select_one(
        'SELECT 1 FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1',
        [$tableName, $indexName]
    );

    $exists = $row !== null;
    $cache[$key] = $exists;

    return $exists;
}

function db_foreign_key_delete_rule(string $constraintName): ?string
{
    if (!db_runtime_schema_checks_enabled()) {
        return 'SET NULL';
    }

    $row = db_select_one(
        'SELECT DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = DATABASE() AND CONSTRAINT_NAME = ? LIMIT 1',
        [$constraintName]
    );

    return $row !== null ? strtoupper((string) ($row['DELETE_RULE'] ?? '')) : null;
}

function item_scope_db_ensure_schema(): void
{

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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

function db_item_priority_latest_snapshots(array $typeIds): array
{


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

function db_item_priority_snapshot_bulk_insert(array $rows, ?int $chunkSize = null): int
{


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

    if (!db_runtime_schema_checks_enabled()) {
        return;
    }
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

function db_worker_job_claim_next(string $workerId, array $queues = [], array $workloadClasses = [], ?array $executionModes = null, ?int $leaseSeconds = null): ?array
{
    db_worker_jobs_ensure_schema();
    db_worker_job_release_expired_claims();

    $safeWorkerId = mb_substr(trim($workerId), 0, 190);
    if ($safeWorkerId === '') {
        return null;
    }

    $leaseTtl = max(30, min(3600, (int) ($leaseSeconds ?? 300)));
    $filters = db_worker_job_filter_fragments($queues, $workloadClasses, $executionModes);
    $conditions = ["status IN ('queued', 'retry')", 'available_at <= UTC_TIMESTAMP()'];
    $params = [];
    foreach ($filters['conditions'] as $condition) {
        $conditions[] = $condition;
    }
    array_push($params, ...$filters['params']);

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

/**
 * @return array{conditions: array<int, string>, params: array<int, string>, queues: array<int, string>, workload_classes: array<int, string>, execution_modes: array<int, string>}
 */
function db_worker_job_filter_fragments(array $queues = [], array $workloadClasses = [], ?array $executionModes = null): array
{
    $conditions = [];
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

    $modeValues = array_values(array_filter(array_map(
        static function ($value): string {
            $mode = strtolower(trim((string) $value));
            return $mode === 'php' ? 'php' : ($mode === 'python' ? 'python' : '');
        },
        $executionModes ?? []
    ), static fn (string $value): bool => $value !== ''));
    if ($modeValues !== []) {
        $conditions[] = 'execution_mode IN (' . implode(',', array_fill(0, count($modeValues), '?')) . ')';
        array_push($params, ...$modeValues);
    }

    return [
        'conditions' => $conditions,
        'params' => $params,
        'queues' => $queueValues,
        'workload_classes' => $classValues,
        'execution_modes' => $modeValues,
    ];
}

/**
 * @return array<string, mixed>
 */
function db_worker_job_claim_diagnostics(array $queues = [], array $workloadClasses = [], ?array $executionModes = null): array
{
    db_worker_jobs_ensure_schema();

    $filters = db_worker_job_filter_fragments($queues, $workloadClasses, $executionModes);
    $whereFiltered = $filters['conditions'] === [] ? '' : (' AND ' . implode(' AND ', $filters['conditions']));
    $filteredParams = [];
    if ($whereFiltered !== '') {
        // The filtered predicate is used in four SELECT expressions below, so
        // repeat the bound values in the same order for each expression.
        for ($i = 0; $i < 4; $i++) {
            array_push($filteredParams, ...$filters['params']);
        }
    }

    $counts = db_select_one(
        "SELECT
            SUM(CASE WHEN status IN ('queued', 'retry') AND available_at <= UTC_TIMESTAMP() THEN 1 ELSE 0 END) AS ready_all,
            SUM(CASE WHEN status IN ('queued', 'retry') AND available_at <= UTC_TIMESTAMP() {$whereFiltered} THEN 1 ELSE 0 END) AS ready_filtered,
            SUM(CASE WHEN status IN ('queued', 'retry') AND available_at > UTC_TIMESTAMP() {$whereFiltered} THEN 1 ELSE 0 END) AS delayed_filtered,
            SUM(CASE WHEN status = 'running' {$whereFiltered} THEN 1 ELSE 0 END) AS running_filtered,
            MIN(CASE WHEN status IN ('queued', 'retry') {$whereFiltered} THEN available_at ELSE NULL END) AS next_available_filtered
         FROM worker_jobs",
        $filteredParams
    );

    $readyAll = max(0, (int) (($counts['ready_all'] ?? 0)));
    $readyFiltered = max(0, (int) (($counts['ready_filtered'] ?? 0)));
    $delayedFiltered = max(0, (int) (($counts['delayed_filtered'] ?? 0)));
    $runningFiltered = max(0, (int) (($counts['running_filtered'] ?? 0)));
    $nextAvailableFiltered = trim((string) ($counts['next_available_filtered'] ?? ''));

    $reason = 'no_matching_jobs';
    if ($readyFiltered > 0) {
        $reason = 'claim_race_lost';
    } elseif ($delayedFiltered > 0) {
        $reason = 'matching_jobs_delayed';
    } elseif ($runningFiltered > 0) {
        $reason = 'matching_jobs_running';
    } elseif ($readyAll > 0) {
        $reason = 'jobs_ready_but_filtered_out';
    }

    return [
        'reason' => $reason,
        'ready_jobs_all' => $readyAll,
        'ready_jobs_filtered' => $readyFiltered,
        'delayed_jobs_filtered' => $delayedFiltered,
        'running_jobs_filtered' => $runningFiltered,
        'next_available_at_filtered' => $nextAvailableFiltered !== '' ? $nextAvailableFiltered : null,
        'filters' => [
            'queues' => $filters['queues'],
            'workload_classes' => $filters['workload_classes'],
            'execution_modes' => $filters['execution_modes'],
        ],
    ];
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

function db_signals_recent(int $limit = 200): array
{
    $safeLimit = max(1, min(1000, $limit));

    return db_select(
        'SELECT id, signal_key, signal_type, severity, type_id, doctrine_fit_id, signal_title, signal_text, signal_payload_json, computed_at
         FROM signals
         ORDER BY computed_at DESC, id DESC
         LIMIT ' . $safeLimit
    );
}

function db_theater_side_composition(string $theaterId): array
{
    return db_select(
        'SELECT * FROM theater_side_composition WHERE theater_id = ? ORDER BY side ASC',
        [$theaterId]
    );
}

function db_pilot_search(string $query, int $limit = 30): array
{
    $q = trim($query);
    if ($q === '' || mb_strlen($q) < 2) {
        return [];
    }
    $safeLimit = max(1, min(100, $limit));

    // Step 1: find candidate characters via the indexed PK prefix on
    // entity_metadata_cache. The LIKE predicate is a leading-wildcard scan,
    // but the result set is bounded by $safeLimit so downstream enrichment
    // stays cheap.
    //
    // The previous implementation joined battle_participants AND
    // theater_participants in a single GROUP BY query. Because neither join
    // is bounded per character (a pilot can have hundreds of battle rows
    // and dozens of theater rows), the cartesian explosion forced MariaDB
    // to materialize millions of rows before COUNT(DISTINCT) could dedupe
    // them, and pilot-lookup.php would time out.
    $candidates = db_select(
        'SELECT entity_id AS character_id, entity_name AS character_name
         FROM entity_metadata_cache
         WHERE entity_type = "character"
           AND entity_name LIKE ?
         ORDER BY entity_name ASC
         LIMIT ' . $safeLimit,
        ['%' . $q . '%']
    );
    if ($candidates === []) {
        return [];
    }

    $charIds = array_map(static fn (array $r): int => (int) $r['character_id'], $candidates);
    $placeholders = implode(',', array_fill(0, count($charIds), '?'));

    // Step 2: battle_count per character.
    // Uses idx_battle_participants_character (character_id, battle_id) and
    // only touches rows for the bounded candidate set.
    $battleCountRows = db_select(
        'SELECT character_id, COUNT(DISTINCT battle_id) AS battle_count
         FROM battle_participants
         WHERE character_id IN (' . $placeholders . ')
         GROUP BY character_id',
        $charIds
    );
    $battleCounts = [];
    foreach ($battleCountRows as $r) {
        $battleCounts[(int) $r['character_id']] = (int) ($r['battle_count'] ?? 0);
    }

    // Step 3: most-recent alliance/corp per character (groupwise max on
    // computed_at). Bounded to the candidate set via character_id IN (...).
    $recentRows = db_select(
        'SELECT bp.character_id, bp.alliance_id, bp.corporation_id
         FROM battle_participants bp
         INNER JOIN (
             SELECT character_id, MAX(computed_at) AS max_computed
             FROM battle_participants
             WHERE character_id IN (' . $placeholders . ')
             GROUP BY character_id
         ) latest
           ON latest.character_id = bp.character_id
          AND latest.max_computed = bp.computed_at
         WHERE bp.character_id IN (' . $placeholders . ')',
        array_merge($charIds, $charIds)
    );
    $recentByChar = [];
    foreach ($recentRows as $r) {
        $cid = (int) $r['character_id'];
        if (!isset($recentByChar[$cid])) {
            $recentByChar[$cid] = [
                'alliance_id'    => $r['alliance_id']    !== null ? (int) $r['alliance_id']    : null,
                'corporation_id' => $r['corporation_id'] !== null ? (int) $r['corporation_id'] : null,
            ];
        }
    }

    // Step 4: role_proxy per character from theater_participants.
    // Relies on idx_theater_participants_character from
    // database/migrations/20260413_theater_participant_character_index.sql.
    $roleRows = db_select(
        'SELECT character_id, role_proxy, entry_time
         FROM theater_participants
         WHERE character_id IN (' . $placeholders . ')
           AND role_proxy IS NOT NULL
         ORDER BY entry_time DESC',
        $charIds
    );
    $roleByChar = [];
    foreach ($roleRows as $r) {
        $cid = (int) $r['character_id'];
        // First row (latest entry_time) wins.
        if (!isset($roleByChar[$cid])) {
            $roleByChar[$cid] = (string) $r['role_proxy'];
        }
    }

    // Step 5: resolve alliance/corporation names in one batched lookup.
    $allianceIds = [];
    $corporationIds = [];
    foreach ($recentByChar as $rc) {
        if ($rc['alliance_id']    !== null && $rc['alliance_id']    > 0) $allianceIds[]    = $rc['alliance_id'];
        if ($rc['corporation_id'] !== null && $rc['corporation_id'] > 0) $corporationIds[] = $rc['corporation_id'];
    }
    $allianceNames    = [];
    $corporationNames = [];
    if ($allianceIds !== []) {
        foreach (db_entity_metadata_cache_get_many('alliance', $allianceIds) as $row) {
            $allianceNames[(int) $row['entity_id']] = (string) ($row['entity_name'] ?? '');
        }
    }
    if ($corporationIds !== []) {
        foreach (db_entity_metadata_cache_get_many('corporation', $corporationIds) as $row) {
            $corporationNames[(int) $row['entity_id']] = (string) ($row['entity_name'] ?? '');
        }
    }

    // Step 6: assemble and sort (battle_count DESC, then name ASC — matches
    // the ordering of the original single-query implementation).
    $results = [];
    foreach ($candidates as $c) {
        $cid = (int) $c['character_id'];
        $recent = $recentByChar[$cid] ?? ['alliance_id' => null, 'corporation_id' => null];
        $allianceId    = $recent['alliance_id'];
        $corporationId = $recent['corporation_id'];
        $results[] = [
            'character_id'     => $cid,
            'character_name'   => (string) ($c['character_name'] ?? ''),
            'alliance_id'      => $allianceId,
            'corporation_id'   => $corporationId,
            'alliance_name'    => $allianceId    !== null ? ($allianceNames[$allianceId]       ?? null) : null,
            'corporation_name' => $corporationId !== null ? ($corporationNames[$corporationId] ?? null) : null,
            'battle_count'     => $battleCounts[$cid] ?? 0,
            'fleet_function'   => $roleByChar[$cid] ?? null,
        ];
    }

    usort($results, static function (array $a, array $b): int {
        $cmp = ($b['battle_count'] ?? 0) <=> ($a['battle_count'] ?? 0);
        if ($cmp !== 0) {
            return $cmp;
        }
        return strcmp((string) ($a['character_name'] ?? ''), (string) ($b['character_name'] ?? ''));
    });

    return $results;
}

function db_pilot_profile(int $characterId): ?array
{
    if ($characterId <= 0) {
        return null;
    }

    // Core identity
    $character = db_select_one(
        'SELECT emc.entity_id AS character_id, emc.entity_name AS character_name
         FROM entity_metadata_cache emc
         WHERE emc.entity_type = "character" AND emc.entity_id = ?',
        [$characterId]
    );
    if ($character === null) {
        return null;
    }

    // Aggregate stats from theater_participants (most complete source)
    $theaterStats = db_select_one(
        'SELECT
            COUNT(DISTINCT tp.theater_id) AS theater_count,
            SUM(tp.kills) AS total_kills,
            SUM(tp.deaths) AS total_deaths,
            SUM(tp.damage_done) AS total_damage_done,
            SUM(tp.damage_taken) AS total_damage_taken,
            SUM(tp.battles_present) AS total_battles
         FROM theater_participants tp
         WHERE tp.character_id = ?',
        [$characterId]
    );

    // Most recent theater participation for alliance/corp
    $recentParticipation = db_select_one(
        'SELECT tp.alliance_id, tp.corporation_id, tp.role_proxy,
                COALESCE(emc_a.entity_name, CONCAT("Alliance #", tp.alliance_id)) AS alliance_name,
                COALESCE(emc_c.entity_name, CONCAT("Corp #", tp.corporation_id)) AS corporation_name
         FROM theater_participants tp
         LEFT JOIN entity_metadata_cache emc_a ON emc_a.entity_type = "alliance" AND emc_a.entity_id = tp.alliance_id
         LEFT JOIN entity_metadata_cache emc_c ON emc_c.entity_type = "corporation" AND emc_c.entity_id = tp.corporation_id
         WHERE tp.character_id = ?
         ORDER BY tp.entry_time DESC
         LIMIT 1',
        [$characterId]
    );

    // Fallback: resolve alliance/corp from battle_participants if no theater data
    if ($recentParticipation === null) {
        $recentParticipation = db_select_one(
            'SELECT bp.alliance_id, bp.corporation_id, NULL AS role_proxy,
                    COALESCE(emc_a.entity_name, CONCAT("Alliance #", bp.alliance_id)) AS alliance_name,
                    COALESCE(emc_c.entity_name, CONCAT("Corp #", bp.corporation_id)) AS corporation_name
             FROM battle_participants bp
             LEFT JOIN entity_metadata_cache emc_a ON emc_a.entity_type = "alliance" AND emc_a.entity_id = bp.alliance_id
             LEFT JOIN entity_metadata_cache emc_c ON emc_c.entity_type = "corporation" AND emc_c.entity_id = bp.corporation_id
             WHERE bp.character_id = ?
             ORDER BY bp.computed_at DESC
             LIMIT 1',
            [$characterId]
        );
    }

    // Final fallback: resolve from killmail_attackers
    if ($recentParticipation === null) {
        $recentParticipation = db_select_one(
            'SELECT ka.alliance_id, ka.corporation_id, NULL AS role_proxy,
                    COALESCE(emc_a.entity_name, "") AS alliance_name,
                    COALESCE(emc_c.entity_name, "") AS corporation_name
             FROM killmail_attackers ka
             LEFT JOIN entity_metadata_cache emc_a ON emc_a.entity_type = "alliance" AND emc_a.entity_id = ka.alliance_id
             LEFT JOIN entity_metadata_cache emc_c ON emc_c.entity_type = "corporation" AND emc_c.entity_id = ka.corporation_id
             WHERE ka.character_id = ?
             ORDER BY ka.id DESC
             LIMIT 1',
            [$characterId]
        );
    }

    // Ship types flown (from theater_participants ship_type_ids JSON)
    $shipRows = db_select(
        'SELECT tp.ship_type_ids, tp.role_proxy, tp.theater_id
         FROM theater_participants tp
         WHERE tp.character_id = ? AND tp.ship_type_ids IS NOT NULL',
        [$characterId]
    );
    $shipCounts = [];
    foreach ($shipRows as $sr) {
        $ids = json_decode((string) ($sr['ship_type_ids'] ?? '[]'), true);
        if (!is_array($ids)) continue;
        foreach ($ids as $typeId) {
            $typeId = (int) $typeId;
            if ($typeId > 0) {
                $shipCounts[$typeId] = ($shipCounts[$typeId] ?? 0) + 1;
            }
        }
    }
    arsort($shipCounts);
    $topShipIds = array_slice(array_keys($shipCounts), 0, 10);
    $shipNames = [];
    if ($topShipIds !== []) {
        $ph = implode(',', array_fill(0, count($topShipIds), '?'));
        $shipNameRows = db_select(
            "SELECT type_id, type_name, group_id FROM ref_item_types WHERE type_id IN ({$ph})",
            $topShipIds
        );
        foreach ($shipNameRows as $sn) {
            $shipNames[(int) $sn['type_id']] = $sn;
        }
    }
    $shipHistory = [];
    foreach (array_slice($shipCounts, 0, 10, true) as $typeId => $count) {
        $info = $shipNames[$typeId] ?? [];
        $shipHistory[] = [
            'type_id' => $typeId,
            'type_name' => (string) ($info['type_name'] ?? "Ship #{$typeId}"),
            'group_id' => (int) ($info['group_id'] ?? 0),
            'times_flown' => $count,
        ];
    }

    // Suspicion signals (from intelligence pipeline)
    $suspicion = db_select_one(
        'SELECT css.*, COALESCE(cao.historical_overlap_score, 0) AS historical_overlap_score,
                COALESCE(cao.combined_risk_score, 0) AS combined_risk_score,
                COALESCE(cao.correlated_flag, 0) AS correlated_flag
         FROM character_suspicion_signals css
         LEFT JOIN character_alliance_overlap cao ON cao.character_id = css.character_id
         WHERE css.character_id = ?',
        [$characterId]
    );

    // Theater history with performance
    $theaterHistory = db_select(
        'SELECT tp.theater_id, tp.kills, tp.deaths, tp.damage_done, tp.damage_taken,
                tp.role_proxy, tp.side, tp.battles_present, tp.suspicion_score,
                t.start_time,
                COALESCE(rs.system_name, CONCAT("System #", t.primary_system_id)) AS primary_system_name,
                COALESCE(rr.region_name, CONCAT("Region #", t.region_id)) AS region_name,
                COALESCE(emc_a.entity_name, CONCAT("Alliance #", tp.alliance_id)) AS alliance_name
         FROM theater_participants tp
         INNER JOIN theaters t ON t.theater_id = tp.theater_id
         LEFT JOIN ref_systems rs ON rs.system_id = t.primary_system_id
         LEFT JOIN ref_regions rr ON rr.region_id = t.region_id
         LEFT JOIN entity_metadata_cache emc_a ON emc_a.entity_type = "alliance" AND emc_a.entity_id = tp.alliance_id
         WHERE tp.character_id = ?
         ORDER BY t.start_time DESC
         LIMIT 20',
        [$characterId]
    );

    // Killmail-based stats from behavioral scoring pipeline (covers all kills, not just theater)
    $behavioralStats = db_select_one(
        'SELECT total_kill_count, solo_kill_count, gang_kill_count, large_battle_count,
                behavioral_risk_score, percentile_rank, confidence_tier,
                fleet_absence_ratio, companion_consistency_score,
                cross_side_small_rate, asymmetry_preference,
                geographic_concentration_score, temporal_regularity_score,
                post_engagement_continuation_rate, kill_concentration_score
         FROM character_behavioral_scores
         WHERE character_id = ?',
        [$characterId]
    );

    // Killmail-sourced death count and damage from temporal metrics (most complete non-theater source)
    $killmailLifetime = db_select_one(
        'SELECT SUM(kills_total) AS km_kills, SUM(losses_total) AS km_losses,
                SUM(damage_total) AS km_damage, SUM(battles_present) AS km_battles
         FROM character_temporal_metrics
         WHERE character_id = ?',
        [$characterId]
    );

    // Associates — characters who appear in the same theaters most often
    $associates = db_select(
        'SELECT tp2.character_id AS assoc_character_id,
                COALESCE(emc.entity_name, CONCAT("Character #", tp2.character_id)) AS assoc_name,
                COALESCE(emc_a.entity_name, "") AS assoc_alliance,
                tp2.alliance_id AS assoc_alliance_id,
                COUNT(DISTINCT tp2.theater_id) AS shared_theaters,
                tp2.role_proxy AS assoc_role
         FROM theater_participants tp1
         INNER JOIN theater_participants tp2
             ON tp2.theater_id = tp1.theater_id
             AND tp2.side = tp1.side
             AND tp2.character_id != tp1.character_id
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = tp2.character_id
         LEFT JOIN entity_metadata_cache emc_a ON emc_a.entity_type = "alliance" AND emc_a.entity_id = tp2.alliance_id
         WHERE tp1.character_id = ?
         GROUP BY tp2.character_id
         ORDER BY shared_theaters DESC
         LIMIT 30',
        [$characterId]
    );

    return [
        'character' => $character,
        'alliance_name' => (string) ($recentParticipation['alliance_name'] ?? ''),
        'corporation_name' => (string) ($recentParticipation['corporation_name'] ?? ''),
        'alliance_id' => (int) ($recentParticipation['alliance_id'] ?? 0),
        'corporation_id' => (int) ($recentParticipation['corporation_id'] ?? 0),
        'fleet_function' => (string) ($recentParticipation['role_proxy'] ?? 'mainline_dps'),
        'stats' => $theaterStats,
        'behavioral_stats' => $behavioralStats,
        'killmail_lifetime' => $killmailLifetime,
        'ships' => $shipHistory,
        'suspicion' => $suspicion,
        'theater_history' => $theaterHistory,
        'associates' => $associates,
    ];
}

function db_battle_intelligence_top_characters(int $limit = 50): array
{
    $safeLimit = max(1, min(200, $limit));
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    if ($trackedAllianceIds === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($trackedAllianceIds), '?'));

    // Blended leaderboard: combines Lane 1 (counterintel) and Lane 2 (behavioral).
    // Characters with only behavioral scores are included — they appear with null
    // Lane 1 fields, ranked by their behavioral score.
    // Blending weights mirror db_character_blended_intelligence():
    //   large_battles >= 10 → 65/35, 5-9 → 50/50, 1-4 → 35/65, 0 → 0/100.
    return db_select(
        'SELECT
            COALESCE(ccs.character_id, cbs.character_id) AS character_id,
            ccs.review_priority_score,
            ccs.percentile_rank,
            ccs.confidence_score,
            ccs.evidence_count,
            COALESCE(ccs.computed_at, cbs.computed_at) AS computed_at,
            ccf.anomalous_battle_presence_count, ccf.control_battle_presence_count,
            ccf.anomalous_battle_denominator, ccf.control_battle_denominator,
            ccf.anomalous_presence_rate, ccf.control_presence_rate,
            ccf.enemy_same_hull_survival_lift, ccf.enemy_sustain_lift,
            ccf.co_presence_anomalous_density, ccf.graph_bridge_score,
            ccf.corp_hop_frequency_180d, ccf.short_tenure_ratio_180d, ccf.repeatability_score,
            cbs.behavioral_risk_score,
            cbs.confidence_tier AS behavioral_confidence_tier,
            cbs.solo_kill_count, cbs.gang_kill_count, cbs.large_battle_count,
            COALESCE(emc.entity_name, CONCAT("Character #", COALESCE(ccs.character_id, cbs.character_id))) AS character_name,
            CASE
                WHEN ccs.character_id IS NOT NULL AND cbs.character_id IS NOT NULL THEN
                    CASE
                        WHEN COALESCE(cbs.large_battle_count, 0) >= 10 THEN 0.65 * ccs.review_priority_score + 0.35 * cbs.behavioral_risk_score
                        WHEN COALESCE(cbs.large_battle_count, 0) >= 5  THEN 0.50 * ccs.review_priority_score + 0.50 * cbs.behavioral_risk_score
                        WHEN COALESCE(cbs.large_battle_count, 0) >= 1  THEN 0.35 * ccs.review_priority_score + 0.65 * cbs.behavioral_risk_score
                        ELSE cbs.behavioral_risk_score
                    END
                WHEN ccs.character_id IS NOT NULL THEN ccs.review_priority_score
                ELSE cbs.behavioral_risk_score
            END AS blended_score
         FROM character_counterintel_scores ccs
         LEFT JOIN character_counterintel_features ccf ON ccf.character_id = ccs.character_id
         LEFT JOIN character_behavioral_scores cbs ON cbs.character_id = ccs.character_id
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = ccs.character_id
         WHERE ccs.character_id IN (
             SELECT bp.character_id FROM battle_participants bp
             WHERE bp.alliance_id IN (' . $placeholders . ')
         )
         UNION
         SELECT
            cbs.character_id,
            NULL AS review_priority_score, NULL AS percentile_rank,
            NULL AS confidence_score, NULL AS evidence_count,
            cbs.computed_at,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            cbs.behavioral_risk_score,
            cbs.confidence_tier AS behavioral_confidence_tier,
            cbs.solo_kill_count, cbs.gang_kill_count, cbs.large_battle_count,
            COALESCE(emc.entity_name, CONCAT("Character #", cbs.character_id)) AS character_name,
            cbs.behavioral_risk_score AS blended_score
         FROM character_behavioral_scores cbs
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = cbs.character_id
         LEFT JOIN character_counterintel_scores ccs_exclude ON ccs_exclude.character_id = cbs.character_id
         WHERE ccs_exclude.character_id IS NULL
           AND cbs.character_id IN (
               SELECT ka.character_id FROM killmail_attackers ka
               INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
               WHERE ke.victim_alliance_id IN (' . $placeholders . ')
                  OR ka.alliance_id IN (' . $placeholders . ')
           )
         ORDER BY blended_score DESC, review_priority_score DESC
         LIMIT ' . $safeLimit,
        array_merge($trackedAllianceIds, $trackedAllianceIds, $trackedAllianceIds)
    );
}

function db_battle_intelligence_top_battles(int $limit = 50): array
{
    $safeLimit = max(1, min(200, $limit));
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    if ($trackedAllianceIds === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($trackedAllianceIds), '?'));

    return db_select(
        'SELECT eos.battle_id, eos.side_key, eos.anomaly_class, eos.overperformance_score, eos.sustain_lift_score,
                eos.hull_survival_lift_score, eos.control_delta_score, eos.evidence_json, eos.computed_at,
                br.system_id, br.started_at, br.ended_at, br.participant_count,
                COALESCE(rs.system_name, CONCAT("System #", br.system_id)) AS system_name,
                COALESCE(emc_side.entity_name, eos.side_key) AS side_name
         FROM battle_enemy_overperformance_scores eos
         INNER JOIN battle_rollups br ON br.battle_id = eos.battle_id
         LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
         LEFT JOIN entity_metadata_cache emc_side
             ON emc_side.entity_type = CASE WHEN eos.side_key LIKE "a:%" THEN "alliance" WHEN eos.side_key LIKE "c:%" THEN "corporation" ELSE "" END
             AND emc_side.entity_id = CAST(SUBSTRING(eos.side_key, 3) AS UNSIGNED)
         WHERE eos.battle_id IN (
             SELECT DISTINCT bp.battle_id FROM battle_participants bp
             WHERE bp.alliance_id IN (' . $placeholders . ')
         )
         ORDER BY eos.overperformance_score DESC, br.participant_count DESC
         LIMIT ' . $safeLimit,
        $trackedAllianceIds
    );
}

function db_battle_intelligence_character(int $characterId): ?array
{
    if ($characterId <= 0) {
        return null;
    }

    // Primary: counterintel scores (full-featured pipeline)
    $row = db_select_one(
        'SELECT ccs.character_id, ccs.review_priority_score, ccs.percentile_rank, ccs.confidence_score, ccs.evidence_count, ccs.computed_at,
                ccf.anomalous_battle_presence_count, ccf.control_battle_presence_count, ccf.anomalous_battle_denominator, ccf.control_battle_denominator, ccf.anomalous_presence_rate, ccf.control_presence_rate,
                ccf.enemy_same_hull_survival_lift, ccf.enemy_sustain_lift, ccf.co_presence_anomalous_density, ccf.graph_bridge_score,
                ccf.corp_hop_frequency_180d, ccf.short_tenure_ratio_180d, ccf.repeatability_score,
                coh.source AS org_history_source, coh.current_corporation_id, coh.current_alliance_id, coh.corp_hops_180d, coh.short_tenure_hops_180d,
                coh.hostile_adjacent_hops_180d, coh.history_json AS org_history_json, coh.fetched_at AS org_history_fetched_at,
                COALESCE(emc.entity_name, CONCAT("Character #", ccs.character_id)) AS character_name,
                "counterintel" AS data_source
         FROM character_counterintel_scores ccs
         LEFT JOIN character_counterintel_features ccf ON ccf.character_id = ccs.character_id
         LEFT JOIN character_org_history_cache coh ON coh.character_id = ccs.character_id
            AND coh.fetched_at = (
                SELECT MAX(coh2.fetched_at)
                FROM character_org_history_cache coh2
                WHERE coh2.character_id = ccs.character_id
            )
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = ccs.character_id
         WHERE ccs.character_id = ?
         LIMIT 1',
        [$characterId]
    );

    if ($row) {
        return $row;
    }

    // Fallback: suspicion scores (v2 batch pipeline — fewer features but wider coverage)
    $row = db_select_one(
        'SELECT css.character_id, css.suspicion_score AS review_priority_score, css.percentile_rank,
                0.0 AS confidence_score, css.support_evidence_count AS evidence_count, css.computed_at,
                0 AS anomalous_battle_presence_count, 0 AS control_battle_presence_count,
                0 AS anomalous_battle_denominator, 0 AS control_battle_denominator,
                0.0 AS anomalous_presence_rate, 0.0 AS control_presence_rate,
                0.0 AS enemy_same_hull_survival_lift,
                css.high_sustain_frequency AS enemy_sustain_lift,
                0.0 AS co_presence_anomalous_density, 0.0 AS graph_bridge_score,
                0.0 AS corp_hop_frequency_180d, 0.0 AS short_tenure_ratio_180d, 0.0 AS repeatability_score,
                coh.source AS org_history_source, coh.current_corporation_id, coh.current_alliance_id, coh.corp_hops_180d, coh.short_tenure_hops_180d,
                coh.hostile_adjacent_hops_180d, coh.history_json AS org_history_json, coh.fetched_at AS org_history_fetched_at,
                COALESCE(emc.entity_name, CONCAT("Character #", css.character_id)) AS character_name,
                "suspicion_v2" AS data_source
         FROM character_suspicion_scores css
         LEFT JOIN character_org_history_cache coh ON coh.character_id = css.character_id
            AND coh.fetched_at = (
                SELECT MAX(coh2.fetched_at)
                FROM character_org_history_cache coh2
                WHERE coh2.character_id = css.character_id
            )
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = css.character_id
         WHERE css.character_id = ?
         LIMIT 1',
        [$characterId]
    );

    if ($row) {
        return $row;
    }

    // Minimal fallback: battle intelligence only (character exists in battles but below scoring threshold)
    $row = db_select_one(
        'SELECT cbi.character_id, 0.0 AS review_priority_score, 0.0 AS percentile_rank,
                0.0 AS confidence_score, 0 AS evidence_count, cbi.computed_at,
                0 AS anomalous_battle_presence_count, 0 AS control_battle_presence_count,
                0 AS anomalous_battle_denominator, 0 AS control_battle_denominator,
                0.0 AS anomalous_presence_rate, 0.0 AS control_presence_rate,
                0.0 AS enemy_same_hull_survival_lift, 0.0 AS enemy_sustain_lift,
                0.0 AS co_presence_anomalous_density, 0.0 AS graph_bridge_score,
                0.0 AS corp_hop_frequency_180d, 0.0 AS short_tenure_ratio_180d, 0.0 AS repeatability_score,
                NULL AS org_history_source, NULL AS current_corporation_id, NULL AS current_alliance_id,
                NULL AS corp_hops_180d, NULL AS short_tenure_hops_180d, NULL AS hostile_adjacent_hops_180d,
                NULL AS org_history_json, NULL AS org_history_fetched_at,
                COALESCE(emc.entity_name, CONCAT("Character #", cbi.character_id)) AS character_name,
                "below_threshold" AS data_source,
                cbi.eligible_battle_count, cbi.total_battle_count
         FROM character_battle_intelligence cbi
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = cbi.character_id
         WHERE cbi.character_id = ?
         LIMIT 1',
        [$characterId]
    );

    return $row ?: null;
}

function db_battle_intelligence_character_battles(int $characterId, int $limit = 20): array
{
    if ($characterId <= 0) {
        return [];
    }

    $safeLimit = max(1, min(100, $limit));

    return db_select(
        'SELECT baf.battle_id, baf.side_key, baf.centrality_score, baf.visibility_score,
                baf.participated_in_high_sustain, baf.participated_in_low_sustain,
                eos.overperformance_score, eos.sustain_lift_score, eos.hull_survival_lift_score, eos.control_delta_score, eos.anomaly_class,
                br.system_id, br.started_at, br.ended_at, br.participant_count,
                COALESCE(rs.system_name, CONCAT("System #", br.system_id)) AS system_name,
                COALESCE(emc_side.entity_name, baf.side_key) AS side_name
         FROM battle_actor_features baf
         INNER JOIN battle_rollups br ON br.battle_id = baf.battle_id
         LEFT JOIN battle_enemy_overperformance_scores eos ON eos.battle_id = baf.battle_id AND eos.side_key = baf.side_key
         LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
         LEFT JOIN entity_metadata_cache emc_side
             ON emc_side.entity_type = CASE WHEN baf.side_key LIKE "a:%" THEN "alliance" WHEN baf.side_key LIKE "c:%" THEN "corporation" ELSE "" END
             AND emc_side.entity_id = CAST(SUBSTRING(baf.side_key, 3) AS UNSIGNED)
         WHERE baf.character_id = ?
         ORDER BY ABS(COALESCE(eos.overperformance_score, 0)) DESC, br.started_at DESC
         LIMIT ' . $safeLimit,
        [$characterId]
    );
}

function db_battle_intelligence_battle(string $battleId): ?array
{
    $safeBattleId = trim($battleId);
    if ($safeBattleId === '') {
        return null;
    }

    $row = db_select_one(
        'SELECT br.*, COALESCE(rs.system_name, CONCAT("System #", br.system_id)) AS system_name
         FROM battle_rollups br
         LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
         WHERE br.battle_id = ?
         LIMIT 1',
        [$safeBattleId]
    );

    return $row ?: null;
}

function db_battle_intelligence_battle_sides(string $battleId): array
{
    $safeBattleId = trim($battleId);
    if ($safeBattleId === '') {
        return [];
    }

    return db_select(
        'SELECT eos.battle_id, eos.side_key, eos.overperformance_score, eos.sustain_lift_score, eos.hull_survival_lift_score, eos.control_delta_score,
                eos.anomaly_class, eos.evidence_json, eos.computed_at,
                bsm.participant_count, bsm.kill_rate_per_minute, bsm.median_sustain_factor, bsm.z_efficiency_score, bsm.efficiency_score,
                COALESCE(emc_side.entity_name, eos.side_key) AS side_name
         FROM battle_enemy_overperformance_scores eos
         LEFT JOIN battle_side_metrics bsm ON bsm.battle_id = eos.battle_id AND bsm.side_key = eos.side_key
         LEFT JOIN entity_metadata_cache emc_side
             ON emc_side.entity_type = CASE WHEN eos.side_key LIKE "a:%" THEN "alliance" WHEN eos.side_key LIKE "c:%" THEN "corporation" ELSE "" END
             AND emc_side.entity_id = CAST(SUBSTRING(eos.side_key, 3) AS UNSIGNED)
         WHERE eos.battle_id = ?
         ORDER BY eos.overperformance_score DESC',
        [$safeBattleId]
    );
}

function db_battle_intelligence_character_evidence(int $characterId, int $limit = 40): array
{
    if ($characterId <= 0) {
        return [];
    }

    $safeLimit = max(1, min(200, $limit));

    return db_select(
        'SELECT character_id, evidence_key, window_label, evidence_value,
                expected_value, deviation_value, z_score, mad_score,
                cohort_percentile, confidence_flag,
                evidence_text, evidence_payload_json, computed_at
         FROM character_counterintel_evidence
         WHERE character_id = ?
         ORDER BY COALESCE(ABS(deviation_value), COALESCE(ABS(evidence_value), 0)) DESC, evidence_key ASC, window_label ASC
         LIMIT ' . $safeLimit,
        [$characterId]
    );
}

/**
 * Produce a blended two-lane intelligence summary for a character.
 *
 * Lane 1  — battle intelligence  (counterintel / suspicion scores)
 * Lane 2  — behavioral scoring   (all-engagement behavioral pipeline)
 *
 * Blending rules
 *   large_battles >= 10  → Lane 1 weight 0.65 / Lane 2 weight 0.35
 *   large_battles  5–9   → Lane 1 weight 0.50 / Lane 2 weight 0.50
 *   large_battles  1–4   → Lane 1 weight 0.35 / Lane 2 weight 0.65
 *   large_battles  0     → Lane 2 only (behavioral only)
 *   no behavioral data   → Lane 1 only
 *
 * Returns null only if there is no data at all in either lane.
 */
function db_character_blended_intelligence(int $characterId): ?array
{
    if ($characterId <= 0) {
        return null;
    }

    // Fetch both lanes.
    $lane1 = db_battle_intelligence_character($characterId);
    $behavioral = db_character_behavioral_score($characterId);

    if ($lane1 === null && $behavioral === null) {
        return null;
    }

    $l1Score     = (float) ($lane1['review_priority_score'] ?? 0.0);
    $l1Pct       = (float) ($lane1['percentile_rank'] ?? 0.0);
    $l1Confidence = (float) ($lane1['confidence_score'] ?? 0.0);
    $l1Source    = (string) ($lane1['data_source'] ?? 'none');

    $l2Score     = (float) ($behavioral['behavioral_risk_score'] ?? 0.0);
    $l2Pct       = (float) ($behavioral['percentile_rank'] ?? 0.0);
    $l2Tier      = (string) ($behavioral['confidence_tier'] ?? 'low');
    $largeBattles = (int) ($behavioral['large_battle_count'] ?? 0);
    $soloKills   = (int) ($behavioral['solo_kill_count'] ?? 0);
    $gangKills   = (int) ($behavioral['gang_kill_count'] ?? 0);

    // Determine coverage source and blend weights.
    $hasLane1 = $lane1 !== null;
    $hasLane2 = $behavioral !== null;

    if ($hasLane1 && $hasLane2) {
        if ($largeBattles >= 10) {
            $w1 = 0.65; $w2 = 0.35;
            $evidenceSource = 'mixed_battle_dominant';
        } elseif ($largeBattles >= 5) {
            $w1 = 0.50; $w2 = 0.50;
            $evidenceSource = 'mixed_balanced';
        } elseif ($largeBattles >= 1) {
            $w1 = 0.35; $w2 = 0.65;
            $evidenceSource = 'mixed_behavioral_dominant';
        } else {
            $w1 = 0.0; $w2 = 1.0;
            $evidenceSource = 'behavioral_only';
        }
    } elseif ($hasLane1) {
        $w1 = 1.0; $w2 = 0.0;
        $evidenceSource = 'battle_only';
    } else {
        $w1 = 0.0; $w2 = 1.0;
        $evidenceSource = 'behavioral_only';
    }

    $blendedScore = round($w1 * $l1Score + $w2 * $l2Score, 6);
    $blendedPct   = round($w1 * $l1Pct   + $w2 * $l2Pct,   6);

    // Overall confidence: blend confidence scores, capped by data quality.
    $l2ConfidenceNumeric = match ($l2Tier) { 'high' => 0.9, 'medium' => 0.6, 'low' => 0.3, default => 0.1 };
    $blendedConfidence = $hasLane1 && $hasLane2
        ? round(($w1 * $l1Confidence + $w2 * $l2ConfidenceNumeric), 4)
        : ($hasLane1 ? $l1Confidence : $l2ConfidenceNumeric);

    return [
        'character_id'       => $characterId,
        'character_name'     => $lane1['character_name'] ?? $behavioral['character_name'] ?? null,
        'blended_score'      => $blendedScore,
        'blended_percentile' => $blendedPct,
        'blended_confidence' => $blendedConfidence,
        'evidence_source'    => $evidenceSource,
        'lane1_score'        => $hasLane1 ? $l1Score : null,
        'lane1_percentile'   => $hasLane1 ? $l1Pct   : null,
        'lane1_source'       => $hasLane1 ? $l1Source : null,
        'lane1_weight'       => $w1,
        'lane2_score'        => $hasLane2 ? $l2Score : null,
        'lane2_percentile'   => $hasLane2 ? $l2Pct   : null,
        'lane2_confidence'   => $hasLane2 ? $l2Tier  : null,
        'lane2_weight'       => $w2,
        'solo_kill_count'    => $soloKills,
        'gang_kill_count'    => $gangKills,
        'large_battle_count' => $largeBattles,
        'lane1_raw'          => $lane1,
        'lane2_raw'          => $behavioral,
    ];
}

function db_character_behavioral_score(int $characterId): ?array
{
    if ($characterId <= 0) {
        return null;
    }

    $row = db_select_one(
        'SELECT cbs.*, COALESCE(emc.entity_name, CONCAT("Character #", cbs.character_id)) AS character_name
         FROM character_behavioral_scores cbs
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = cbs.character_id
         WHERE cbs.character_id = ?
         LIMIT 1',
        [$characterId]
    );

    return $row ?: null;
}

function db_character_behavioral_signals(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }

    return db_select(
        'SELECT signal_key, window_label, signal_value, baseline_value, deviation, confidence_flag, signal_text, signal_payload_json, computed_at
         FROM character_behavioral_signals
         WHERE character_id = ?
         ORDER BY signal_value DESC, signal_key ASC',
        [$characterId]
    );
}

function db_character_small_engagement_copresence(int $characterId, int $limit = 15): array
{
    if ($characterId <= 0) {
        return [];
    }

    $safeLimit = max(1, min(50, $limit));

    return db_select(
        'SELECT sec.character_id_a, sec.character_id_b, sec.window_label, sec.co_kill_count,
                sec.unique_victim_count, sec.unique_system_count, sec.edge_weight, sec.last_event_at,
                COALESCE(emc.entity_name, CONCAT("Character #", CASE WHEN sec.character_id_a = ? THEN sec.character_id_b ELSE sec.character_id_a END)) AS companion_name
         FROM small_engagement_copresence sec
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character"
            AND emc.entity_id = CASE WHEN sec.character_id_a = ? THEN sec.character_id_b ELSE sec.character_id_a END
         WHERE (sec.character_id_a = ? OR sec.character_id_b = ?)
         ORDER BY sec.edge_weight DESC
         LIMIT ' . $safeLimit,
        [$characterId, $characterId, $characterId, $characterId]
    );
}

function db_battle_intelligence_battle_hull_anomalies(string $battleId, int $limit = 60): array
{
    $safeBattleId = trim($battleId);
    if ($safeBattleId === '') {
        return [];
    }

    $safeLimit = max(1, min(200, $limit));

    return db_select(
        'SELECT hsam.battle_id, hsam.side_key, hsam.victim_ship_type_id, hsam.hull_survival_seconds, hsam.baseline_survival_seconds, hsam.survival_lift, hsam.sample_count, hsam.computed_at,
                COALESCE(emc_side.entity_name, hsam.side_key) AS side_name,
                COALESCE(rit.type_name, CONCAT("Type #", hsam.victim_ship_type_id)) AS ship_name
         FROM hull_survival_anomaly_metrics hsam
         LEFT JOIN entity_metadata_cache emc_side
             ON emc_side.entity_type = CASE WHEN hsam.side_key LIKE "a:%" THEN "alliance" WHEN hsam.side_key LIKE "c:%" THEN "corporation" ELSE "" END
             AND emc_side.entity_id = CAST(SUBSTRING(hsam.side_key, 3) AS UNSIGNED)
         LEFT JOIN ref_item_types rit ON rit.type_id = hsam.victim_ship_type_id
         WHERE hsam.battle_id = ?
         ORDER BY ABS(hsam.survival_lift) DESC, hsam.sample_count DESC
         LIMIT ' . $safeLimit,
        [$safeBattleId]
    );
}

function db_battle_intelligence_battle_notable_actors(string $battleId, int $limit = 30): array
{
    $safeBattleId = trim($battleId);
    if ($safeBattleId === '') {
        return [];
    }

    $safeLimit = max(1, min(200, $limit));

    return db_select(
        'SELECT baf.character_id, baf.side_key, baf.centrality_score, baf.visibility_score, baf.participation_count,
                baf.participated_in_high_sustain, baf.participated_in_low_sustain,
                COALESCE(emc.entity_name, CONCAT("Character #", baf.character_id)) AS character_name,
                COALESCE(emc_side.entity_name, baf.side_key) AS side_name
         FROM battle_actor_features baf
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = baf.character_id
         LEFT JOIN entity_metadata_cache emc_side
             ON emc_side.entity_type = CASE WHEN baf.side_key LIKE "a:%" THEN "alliance" WHEN baf.side_key LIKE "c:%" THEN "corporation" ELSE "" END
             AND emc_side.entity_id = CAST(SUBSTRING(baf.side_key, 3) AS UNSIGNED)
         WHERE baf.battle_id = ?
         ORDER BY baf.visibility_score DESC, baf.centrality_score DESC
         LIMIT ' . $safeLimit,
        [$safeBattleId]
    );
}

// ---------------------------------------------------------------------------
// Enhanced Intelligence Platform (KGv2) — data quality, temporal, typed
// interactions, community detection, motifs, evidence paths, analyst feedback,
// query presets
// ---------------------------------------------------------------------------

function db_graph_data_quality_latest(): ?array
{
    return db_select_one(
        'SELECT run_id, stage, characters_total, characters_with_battles,
                orphan_characters, duplicate_relationships, missing_alliance_ids,
                stale_data_count, identity_mismatches, quality_score, gate_passed,
                gate_details_json, computed_at
         FROM graph_data_quality_metrics
         ORDER BY computed_at DESC
         LIMIT 1'
    );
}

function db_character_temporal_metrics(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }
    return db_select(
        'SELECT window_label, battles_present, kills_total, losses_total,
                damage_total, suspicion_score, co_presence_density,
                engagement_rate_avg, computed_at
         FROM character_temporal_metrics
         WHERE character_id = ?
         ORDER BY FIELD(window_label, "7d", "30d", "90d")',
        [$characterId]
    );
}

function db_character_copresence_signals(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }
    return db_select(
        'SELECT window_label, pair_frequency_delta, out_of_cluster_ratio,
                out_of_cluster_ratio_delta, expected_cluster_decay,
                total_edge_weight, unique_associates, recurring_pair_count,
                cohort_percentile, computed_at
         FROM character_copresence_signals
         WHERE character_id = ?
         ORDER BY FIELD(window_label, "7d", "30d", "90d")',
        [$characterId]
    );
}

function db_character_copresence_top_edges(int $characterId, string $windowLabel = '30d', int $limit = 15): array
{
    if ($characterId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(50, $limit));
    return db_select(
        'SELECT cce.character_id_a, cce.character_id_b, cce.event_type,
                cce.edge_weight, cce.event_count, cce.last_event_at, cce.system_id,
                CASE WHEN cce.character_id_a = ? THEN cce.character_id_b ELSE cce.character_id_a END AS other_character_id,
                COALESCE(emc.entity_name, CONCAT("Character #", CASE WHEN cce.character_id_a = ? THEN cce.character_id_b ELSE cce.character_id_a END)) AS other_character_name,
                (SELECT tp.alliance_id FROM theater_participants tp
                 INNER JOIN theaters t ON t.theater_id = tp.theater_id
                 WHERE tp.character_id = CASE WHEN cce.character_id_a = ? THEN cce.character_id_b ELSE cce.character_id_a END
                 ORDER BY t.start_time DESC LIMIT 1) AS other_alliance_id
         FROM character_copresence_edges cce
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = "character"
             AND emc.entity_id = CASE WHEN cce.character_id_a = ? THEN cce.character_id_b ELSE cce.character_id_a END
         WHERE (cce.character_id_a = ? OR cce.character_id_b = ?)
           AND cce.window_label = ?
           AND cce.event_count >= 3
         ORDER BY cce.edge_weight DESC
         LIMIT ' . $safeLimit,
        [$characterId, $characterId, $characterId, $characterId, $characterId, $characterId, $windowLabel]
    );
}

function db_character_typed_interactions(int $characterId, int $limit = 50): array
{
    if ($characterId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(200, $limit));
    return db_select(
        'SELECT cti.character_a_id, cti.character_b_id, cti.interaction_type,
                cti.interaction_count, cti.last_interaction_at,
                CASE WHEN cti.character_a_id = ? THEN cti.character_b_id ELSE cti.character_a_id END AS other_character_id,
                COALESCE(emc.entity_name, CONCAT("Character #", CASE WHEN cti.character_a_id = ? THEN cti.character_b_id ELSE cti.character_a_id END)) AS other_character_name
         FROM character_typed_interactions cti
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = "character"
             AND emc.entity_id = CASE WHEN cti.character_a_id = ? THEN cti.character_b_id ELSE cti.character_a_id END
         WHERE cti.character_a_id = ? OR cti.character_b_id = ?
         ORDER BY cti.interaction_count DESC
         LIMIT ' . $safeLimit,
        [$characterId, $characterId, $characterId, $characterId, $characterId]
    );
}

function db_graph_community_assignments(int $characterId): ?array
{
    if ($characterId <= 0) {
        return null;
    }
    return db_select_one(
        'SELECT community_id, community_size, membership_score, is_bridge,
                betweenness_centrality, pagerank_score, degree_centrality, computed_at
         FROM graph_community_assignments
         WHERE character_id = ?',
        [$characterId]
    );
}

function db_graph_community_top_members(int $communityId, int $limit = 30): array
{
    $safeLimit = max(1, min(200, $limit));
    return db_select(
        'SELECT gca.character_id, gca.pagerank_score, gca.betweenness_centrality,
                gca.degree_centrality, gca.is_bridge,
                COALESCE(emc.entity_name, CONCAT("Character #", gca.character_id)) AS character_name
         FROM graph_community_assignments gca
         LEFT JOIN entity_metadata_cache emc ON emc.entity_type = "character" AND emc.entity_id = gca.character_id
         WHERE gca.community_id = ?
         ORDER BY gca.pagerank_score DESC
         LIMIT ' . $safeLimit,
        [$communityId]
    );
}

function db_graph_motif_detections_recent(int $limit = 50): array
{
    $safeLimit = max(1, min(200, $limit));
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    if ($trackedAllianceIds === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($trackedAllianceIds), '?'));

    // Filter motifs where at least one member belongs to a tracked alliance
    return db_select(
        'SELECT gmd.motif_type, gmd.member_ids_json, gmd.battle_ids_json, gmd.occurrence_count,
                gmd.suspicion_relevance, gmd.first_seen_at, gmd.last_seen_at, gmd.computed_at
         FROM graph_motif_detections gmd
         WHERE gmd.suspicion_relevance > 0.1
           AND EXISTS (
               SELECT 1 FROM battle_participants bp
               WHERE bp.alliance_id IN (' . $placeholders . ')
                 AND JSON_CONTAINS(gmd.member_ids_json, CAST(bp.character_id AS CHAR))
           )
         ORDER BY gmd.suspicion_relevance DESC, gmd.occurrence_count DESC
         LIMIT ' . $safeLimit,
        $trackedAllianceIds
    );
}

function db_character_evidence_paths(int $characterId, int $limit = 10): array
{
    if ($characterId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(50, $limit));
    return db_select(
        'SELECT path_rank, path_description, path_nodes_json, path_edges_json,
                path_score, computed_at
         FROM character_evidence_paths
         WHERE character_id = ?
         ORDER BY path_rank ASC
         LIMIT ' . $safeLimit,
        [$characterId]
    );
}

// ---------------------------------------------------------------------------
// Movement footprint queries
// ---------------------------------------------------------------------------

function db_character_movement_footprints(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }
    return db_select(
        'SELECT window_label, unique_systems_count, unique_regions_count,
                unique_constellations_count, battles_in_window,
                top_systems_json, top_regions_json,
                system_entropy, system_hhi, region_entropy, region_hhi,
                dominant_system_id, dominant_system_ratio,
                dominant_region_id, dominant_region_ratio,
                js_divergence_systems, cosine_distance_systems,
                js_divergence_regions, cosine_distance_regions,
                hostile_system_overlap_count, hostile_system_overlap_ratio,
                hostile_region_overlap_count, hostile_region_overlap_ratio,
                footprint_expansion_score, footprint_contraction_score,
                new_area_entry_score, hostile_overlap_change_score,
                cohort_z_footprint_size, cohort_z_entropy, cohort_z_hostile_overlap,
                cohort_percentile_footprint,
                computed_at, prev_computed_at
         FROM character_movement_footprints
         WHERE character_id = ?
         ORDER BY FIELD(window_label, "7d", "30d", "90d", "lifetime")',
        [$characterId]
    );
}

function db_character_system_distribution(int $characterId, string $windowLabel = '30d', int $limit = 20): array
{
    if ($characterId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(100, $limit));
    return db_select(
        'SELECT csd.system_id, csd.region_id, csd.battle_count, csd.ratio,
                COALESCE(rs.system_name, CONCAT("System #", csd.system_id)) AS system_name,
                COALESCE(rr.region_name, CONCAT("Region #", csd.region_id)) AS region_name,
                rs.security
         FROM character_system_distribution csd
         LEFT JOIN ref_systems rs ON rs.system_id = csd.system_id
         LEFT JOIN ref_regions rr ON rr.region_id = csd.region_id
         WHERE csd.character_id = ?
           AND csd.window_label = ?
         ORDER BY csd.battle_count DESC
         LIMIT ' . $safeLimit,
        [$characterId, $windowLabel]
    );
}

/** Query Neo4j for geographic movement patterns — which regions this character has traversed through corp members. */
function db_neo4j_character_movement_graph(int $characterId): ?array
{
    if ($characterId <= 0 || !(bool) config('neo4j.enabled', false)) {
        return null;
    }

    // Regions where this character has been active via battle participation
    $regionRows = neo4j_query(
        'MATCH (c:Character {character_id: $charId})-[:PARTICIPATED_IN]->(b:Battle)
         MATCH (b)-[:IN_SYSTEM]->(sys)
         RETURN
           sys.region_id AS region_id,
           count(DISTINCT b) AS battle_count,
           collect(DISTINCT sys.system_id)[..5] AS sample_systems
         ORDER BY battle_count DESC
         LIMIT 10',
        ['charId' => $characterId]
    );

    // Characters who share the same operational footprint (co-located in same systems)
    $coLocatedRows = neo4j_query(
        'MATCH (c:Character {character_id: $charId})-[:PARTICIPATED_IN]->(b:Battle)
         MATCH (other:Character)-[:PARTICIPATED_IN]->(b)
         WHERE other.character_id <> $charId
         WITH other, count(DISTINCT b) AS shared_battles
         WHERE shared_battles >= 3
         RETURN
           other.character_id AS character_id,
           shared_battles
         ORDER BY shared_battles DESC
         LIMIT 15',
        ['charId' => $characterId]
    );

    return [
        'regions' => $regionRows ?: [],
        'co_located_characters' => $coLocatedRows ?: [],
    ];
}

function db_analyst_feedback_for_character(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }
    return db_select(
        'SELECT id, label, confidence, analyst_notes, context_json, created_at
         FROM analyst_feedback
         WHERE character_id = ?
         ORDER BY created_at DESC
         LIMIT 20',
        [$characterId]
    );
}

function db_analyst_feedback_save(int $characterId, string $label, float $confidence, ?string $notes, ?string $contextJson): bool
{
    if ($characterId <= 0) {
        return false;
    }
    $allowed = ['true_positive', 'false_positive', 'needs_review', 'confirmed_clean'];
    if (!in_array($label, $allowed, true)) {
        return false;
    }
    $confidence = max(0.0, min(1.0, $confidence));
    db()->prepare(
        'INSERT INTO analyst_feedback (character_id, label, confidence, analyst_notes, context_json)
         VALUES (?, ?, ?, ?, ?)'
    )->execute([$characterId, $label, $confidence, $notes, $contextJson]);
    return true;
}

function db_character_feature_histograms(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }
    return db_select(
        'SELECT window_label, hour_histogram, weekday_histogram, computed_at
         FROM character_feature_histograms
         WHERE character_id = ?
         ORDER BY FIELD(window_label, "7d", "30d", "90d", "lifetime")',
        [$characterId]
    );
}

function db_character_temporal_behavior_signals(int $characterId): array
{
    if ($characterId <= 0) {
        return [];
    }
    return db_select(
        'SELECT evidence_key, window_label, evidence_value, expected_value,
                deviation_value, z_score, mad_score, cohort_percentile,
                confidence_flag, evidence_text, evidence_payload_json, computed_at
         FROM character_counterintel_evidence
         WHERE character_id = ?
           AND evidence_key IN ("active_hour_shift", "weekday_profile_shift", "cadence_burstiness", "reactivation_after_dormancy")
         ORDER BY FIELD(evidence_key, "active_hour_shift", "weekday_profile_shift", "cadence_burstiness", "reactivation_after_dormancy")',
        [$characterId]
    );
}

function db_character_pipeline_enqueue(array $characterIds, string $reason = 'new_data', float $priority = 0.0): int
{
    $characterIds = array_values(array_unique(array_filter(array_map('intval', $characterIds), static fn(int $id): bool => $id > 0)));
    if ($characterIds === []) {
        return 0;
    }
    $affected = 0;
    foreach (array_chunk($characterIds, 500) as $chunk) {
        $placeholders = implode(',', array_fill(0, count($chunk), '(?, ?, ?, "pending", UTC_TIMESTAMP())'));
        $params = [];
        foreach ($chunk as $cid) {
            $params[] = $cid;
            $params[] = $reason;
            $params[] = round($priority, 4);
        }
        $affected += (int) db_execute(
            'INSERT INTO character_processing_queue (character_id, reason, priority, status, created_at)
             VALUES ' . $placeholders . '
             ON DUPLICATE KEY UPDATE
                 status = IF(status IN ("done","failed"), "pending", status),
                 priority = GREATEST(priority, VALUES(priority)),
                 reason = VALUES(reason),
                 attempts = IF(status IN ("done","failed"), 0, attempts),
                 last_error = IF(status IN ("done","failed"), NULL, last_error),
                 updated_at = CURRENT_TIMESTAMP',
            $params
        );
    }
    return $affected;
}

function db_character_pipeline_status(int $characterId): ?array
{
    if ($characterId <= 0) {
        return null;
    }
    $row = db_select_one(
        'SELECT character_id, last_source_event_at, histogram_at, temporal_at,
                counterintel_at, org_history_at, last_fully_processed_at,
                histogram_error, counterintel_error
         FROM character_pipeline_status
         WHERE character_id = ?',
        [$characterId]
    );
    if ($row === null) {
        return null;
    }
    // Derive per-stage freshness: stale if source is newer than stage output
    $sourceAt = $row['last_source_event_at'];
    $row['histogram_fresh'] = $row['histogram_at'] !== null && ($sourceAt === null || $row['histogram_at'] >= $sourceAt);
    $row['counterintel_fresh'] = $row['counterintel_at'] !== null && ($sourceAt === null || $row['counterintel_at'] >= $sourceAt);
    $row['temporal_fresh'] = $row['temporal_at'] !== null && ($sourceAt === null || $row['temporal_at'] >= $sourceAt);
    $row['fully_fresh'] = $row['histogram_fresh'] && $row['counterintel_fresh'] && $row['temporal_fresh'];
    return $row;
}

function db_character_pipeline_health(): array
{
    $queue = db_select_one(
        'SELECT
            SUM(status = "pending") AS pending_count,
            SUM(status = "processing") AS processing_count,
            SUM(status = "failed") AS failed_count,
            SUM(status = "done") AS done_count,
            MIN(CASE WHEN status = "pending" THEN created_at END) AS oldest_pending_at,
            TIMESTAMPDIFF(SECOND, MIN(CASE WHEN status = "pending" THEN created_at END), UTC_TIMESTAMP()) AS oldest_pending_age_seconds,
            SUM(status = "processing" AND locked_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 180 SECOND)) AS stale_locked_count
         FROM character_processing_queue'
    );
    $stages = db_select_one(
        'SELECT
            SUM(last_source_event_at > histogram_at OR histogram_at IS NULL) AS histogram_stale_count,
            SUM(histogram_at > counterintel_at OR counterintel_at IS NULL) AS counterintel_stale_from_histogram,
            SUM(last_source_event_at > counterintel_at OR counterintel_at IS NULL) AS counterintel_stale_from_source,
            SUM(histogram_error IS NOT NULL) AS histogram_error_count,
            SUM(counterintel_error IS NOT NULL) AS counterintel_error_count,
            COUNT(*) AS total_tracked
         FROM character_pipeline_status
         WHERE last_source_event_at IS NOT NULL'
    );
    $recentErrors = db_select(
        'SELECT character_id, last_error, attempts, updated_at
         FROM character_processing_queue
         WHERE status = "failed" AND last_error IS NOT NULL
         ORDER BY updated_at DESC
         LIMIT 10'
    );
    return [
        'queue' => $queue ?: [],
        'stages' => $stages ?: [],
        'recent_errors' => $recentErrors,
    ];
}

function db_analyst_recalibration_log(int $limit = 20): array
{
    $safeLimit = max(1, min(100, $limit));
    return db_select(
        'SELECT run_id, total_labels, true_positives, false_positives,
                precision_score, recall_estimate, weight_adjustments,
                threshold_changes, computed_at
         FROM analyst_recalibration_log
         ORDER BY computed_at DESC
         LIMIT ' . $safeLimit
    );
}

function db_graph_query_presets_active(): array
{
    return db_select(
        'SELECT id, preset_key, label, description, category, query_type,
                query_template, parameters_json, display_columns, sort_order
         FROM graph_query_presets
         WHERE is_active = 1
         ORDER BY sort_order ASC, label ASC'
    );
}

function db_graph_query_preset_execute(string $presetKey, array $params = []): array
{
    $presetKey = trim($presetKey);
    if ($presetKey === '') {
        return [];
    }
    $preset = db_select_one(
        'SELECT query_type, query_template, parameters_json
         FROM graph_query_presets
         WHERE preset_key = ? AND is_active = 1',
        [$presetKey]
    );
    if (!$preset || ($preset['query_type'] ?? '') !== 'mariadb') {
        return [];
    }
    $template = $preset['query_template'] ?? '';
    if ($template === '') {
        return [];
    }
    $defaultParams = json_decode($preset['parameters_json'] ?? '{}', true) ?: [];
    $merged = array_merge($defaultParams, $params);
    $limit = max(1, min(500, (int)($merged['limit'] ?? 50)));

    // For character-based presets, inject a tracked-alliance filter so only
    // characters belonging to our tracked alliances appear in results.
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    $queryParams = [$limit];

    if ($trackedAllianceIds !== [] && stripos($template, 'character_id') !== false && $presetKey !== 'recurring_motifs') {
        $placeholders = implode(',', array_fill(0, count($trackedAllianceIds), '?'));
        $trackedCte = 'SELECT _inner.* FROM (' . $template . ') _inner '
            . 'WHERE _inner.character_id IN (SELECT bp.character_id FROM battle_participants bp WHERE bp.alliance_id IN (' . $placeholders . '))';
        $template = $trackedCte;
        $queryParams = array_merge([$limit], $trackedAllianceIds);
    }

    return db_select($template, $queryParams);
}

function db_graph_community_overview(int $limit = 30): array
{
    $safeLimit = max(1, min(100, $limit));
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    if ($trackedAllianceIds === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($trackedAllianceIds), '?'));

    return db_select(
        'SELECT gca.community_id, gca.community_size,
                COUNT(*) AS member_count,
                SUM(gca.is_bridge) AS bridge_count,
                AVG(gca.pagerank_score) AS avg_pagerank,
                MAX(gca.pagerank_score) AS max_pagerank,
                AVG(gca.betweenness_centrality) AS avg_betweenness,
                (SELECT COALESCE(emc.entity_name, CONCAT(\'Character #\', top_m.character_id))
                 FROM graph_community_assignments top_m
                 LEFT JOIN entity_metadata_cache emc
                     ON emc.entity_type = \'character\' AND emc.entity_id = top_m.character_id
                 WHERE top_m.community_id = gca.community_id
                 ORDER BY top_m.pagerank_score DESC
                 LIMIT 1) AS top_member_name
         FROM graph_community_assignments gca
         WHERE gca.community_id IN (
             SELECT gca2.community_id FROM graph_community_assignments gca2
             INNER JOIN battle_participants bp ON bp.character_id = gca2.character_id
                 AND bp.alliance_id IN (' . $placeholders . ')
             GROUP BY gca2.community_id
         )
         GROUP BY gca.community_id, gca.community_size
         HAVING member_count >= 3
         ORDER BY member_count DESC
         LIMIT ' . $safeLimit,
        $trackedAllianceIds
    );
}

function db_counterintel_scores_for_characters(array $characterIds): array
{
    $ids = array_map('intval', array_filter($characterIds, static fn($id): bool => ((int) $id) > 0));
    if ($ids === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($ids), '?'));

    $rows = db_select(
        'SELECT ccs.character_id, ccs.review_priority_score, ccs.percentile_rank, ccs.confidence_score
         FROM character_counterintel_scores ccs
         WHERE ccs.character_id IN (' . $placeholders . ')',
        $ids
    );

    $indexed = [];
    foreach ($rows as $r) {
        $indexed[(int) $r['character_id']] = $r;
    }
    return $indexed;
}

// ---------------------------------------------------------------------------
// Theater intelligence
// ---------------------------------------------------------------------------

function db_theaters_list(int $limit = 50, int $offset = 0, ?string $regionFilter = null, ?float $minAnomaly = null): array
{
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));

    if ($trackedAllianceIds === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($trackedAllianceIds), '?'));

    $sql = 'SELECT t.theater_id, t.label, t.primary_system_id, t.region_id,
                   t.start_time, t.end_time, t.duration_seconds,
                   t.battle_count, t.system_count, t.total_kills, t.total_isk,
                   t.participant_count, t.anomaly_score, t.computed_at, t.locked_at,
                   t.ai_headline, t.ai_verdict, t.ai_summary_model, t.ai_summary_at,
                   t.created_at, t.updated_at,
                   rs.system_name AS primary_system_name, rr.region_name
            FROM theaters t
            LEFT JOIN theater_alliance_summary tas
                ON tas.theater_id = t.theater_id
                AND tas.alliance_id IN (' . $placeholders . ')
                AND tas.participant_count >= 2
            LEFT JOIN ref_systems rs ON rs.system_id = t.primary_system_id
            LEFT JOIN ref_regions rr ON rr.region_id = t.region_id
            WHERE (tas.theater_id IS NOT NULL OR (t.clustering_method = \'manual\' AND t.dismissed_at IS NULL))';
    $params = $trackedAllianceIds;
    $conditions = [];
    if ($regionFilter !== null) {
        $conditions[] = 't.region_id = ?';
        $params[] = (int)$regionFilter;
    }
    if ($minAnomaly !== null) {
        $conditions[] = 't.anomaly_score >= ?';
        $params[] = $minAnomaly;
    }
    if ($conditions !== []) {
        $sql .= ' AND ' . implode(' AND ', $conditions);
    }
    $sql .= ' GROUP BY t.theater_id ORDER BY t.start_time DESC LIMIT ' . max(1, min(200, (int)$limit)) . ' OFFSET ' . max(0, (int)$offset);
    return db_select($sql, $params);
}

function db_theater_detail(string $theaterId): ?array
{
    return db_select_one(
        'SELECT t.*, rs.system_name AS primary_system_name, rr.region_name
         FROM theaters t
         LEFT JOIN ref_systems rs ON rs.system_id = t.primary_system_id
         LEFT JOIN ref_regions rr ON rr.region_id = t.region_id
         WHERE t.theater_id = ?',
        [$theaterId]
    );
}

function db_theaters_locked(): array
{
    return db_select(
        'SELECT t.theater_id, t.locked_at, t.ai_headline, t.ai_verdict, t.ai_summary_model, t.ai_summary_at,
                t.start_time, t.end_time, t.total_kills, t.participant_count,
                rs.system_name AS primary_system_name, rr.region_name
         FROM theaters t
         LEFT JOIN ref_systems rs ON rs.system_id = t.primary_system_id
         LEFT JOIN ref_regions rr ON rr.region_id = t.region_id
         WHERE t.locked_at IS NOT NULL
         ORDER BY t.locked_at DESC'
    );
}

function db_theater_battles(string $theaterId): array
{
    return db_select(
        'SELECT tb.*, br.system_id, br.started_at, br.ended_at, br.participant_count,
                br.battle_size_class, rs.system_name,
                COALESCE(kc.kill_count, 0) AS kill_count
         FROM theater_battles tb
         INNER JOIN battle_rollups br ON br.battle_id = tb.battle_id
         LEFT JOIN ref_systems rs ON rs.system_id = br.system_id
         LEFT JOIN (
             SELECT ke.battle_id, COUNT(DISTINCT ke.killmail_id) AS kill_count
             FROM killmail_events ke
             WHERE ke.battle_id IN (SELECT tb2.battle_id FROM theater_battles tb2 WHERE tb2.theater_id = ?)
             GROUP BY ke.battle_id
         ) kc ON kc.battle_id = tb.battle_id
         WHERE tb.theater_id = ?
         ORDER BY br.started_at ASC',
        [$theaterId, $theaterId]
    );
}

function db_theater_systems(string $theaterId): array
{
    return db_select(
        'SELECT * FROM theater_systems WHERE theater_id = ? ORDER BY participant_count DESC',
        [$theaterId]
    );
}

function db_theater_timeline(string $theaterId): array
{
    return db_select(
        'SELECT * FROM theater_timeline WHERE theater_id = ? ORDER BY bucket_time ASC',
        [$theaterId]
    );
}

function db_theater_alliance_summary(string $theaterId): array
{
    return db_select(
        'SELECT tas.*,
                CASE
                    WHEN tas.alliance_id > 0
                        THEN COALESCE(emc_a.entity_name, tas.alliance_name, CONCAT("Alliance #", tas.alliance_id))
                    WHEN tas.corporation_id > 0
                        THEN COALESCE(emc_c.entity_name, CONCAT("Corporation #", tas.corporation_id))
                    ELSE COALESCE(tas.alliance_name, "Unknown")
                END AS alliance_name
         FROM theater_alliance_summary tas
         LEFT JOIN entity_metadata_cache emc_a
              ON emc_a.entity_type = "alliance" AND emc_a.entity_id = tas.alliance_id AND tas.alliance_id > 0
         LEFT JOIN entity_metadata_cache emc_c
              ON emc_c.entity_type = "corporation" AND emc_c.entity_id = tas.corporation_id AND tas.corporation_id > 0
         WHERE tas.theater_id = ?
         ORDER BY tas.total_isk_killed DESC',
        [$theaterId]
    );
}

function db_theater_participants(string $theaterId, ?string $sideFilter = null, bool $suspiciousOnly = false, int $limit = 200): array
{
    $sql = 'SELECT tp.*,
                   COALESCE(emc.entity_name, tp.character_name, CONCAT("Character #", tp.character_id)) AS character_name,
                   COALESCE(emc_a.entity_name, CONCAT("Alliance #", tp.alliance_id)) AS alliance_name,
                   COALESCE(emc_c.entity_name, CONCAT("Corp #", tp.corporation_id)) AS corporation_name
            FROM theater_participants tp
            LEFT JOIN entity_metadata_cache emc
                 ON emc.entity_type = "character" AND emc.entity_id = tp.character_id
            LEFT JOIN entity_metadata_cache emc_a
                 ON emc_a.entity_type = "alliance" AND emc_a.entity_id = tp.alliance_id
            LEFT JOIN entity_metadata_cache emc_c
                 ON emc_c.entity_type = "corporation" AND emc_c.entity_id = tp.corporation_id
            WHERE tp.theater_id = ?';
    $params = [$theaterId];
    if ($sideFilter !== null) {
        $sql .= ' AND tp.side = ?';
        $params[] = $sideFilter;
    }
    if ($suspiciousOnly) {
        $sql .= ' AND tp.is_suspicious = 1';
    }
    $sql .= ' ORDER BY tp.kills DESC, tp.damage_done DESC LIMIT ' . max(1, min(1000, (int)$limit));
    return db_select($sql, $params);
}

function db_theater_structure_kills(string $theaterId): array
{
    return db_select(
        'SELECT tsk.*,
                COALESCE(emc_a.entity_name, CONCAT("Alliance #", tsk.victim_alliance_id)) AS alliance_name,
                COALESCE(emc_c.entity_name, CONCAT("Corp #", tsk.victim_corporation_id)) AS corporation_name,
                ke.sequence_id AS sequence_id
         FROM theater_structure_kills tsk
         LEFT JOIN entity_metadata_cache emc_a
              ON emc_a.entity_type = "alliance" AND emc_a.entity_id = tsk.victim_alliance_id
         LEFT JOIN entity_metadata_cache emc_c
              ON emc_c.entity_type = "corporation" AND emc_c.entity_id = tsk.victim_corporation_id
         LEFT JOIN killmail_events ke
              ON ke.killmail_id = tsk.killmail_id
         WHERE tsk.theater_id = ?
         ORDER BY tsk.isk_lost DESC',
        [$theaterId]
    );
}

/**
 * Look up all victim killmails for the given battle IDs.
 * Returns rows with sequence_id, killmail_id, victim_character_id, victim_ship_type_id
 * so the theater participants view can link lost ships to killmail details.
 */
function db_theater_victim_killmails_by_battles(array $battleIds): array
{
    if ($battleIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($battleIds), '?'));

    return db_select(
        "SELECT ke.sequence_id, ke.killmail_id, ke.victim_character_id, ke.victim_ship_type_id
         FROM killmail_events ke
         WHERE ke.battle_id IN ({$placeholders})
           AND ke.victim_character_id IS NOT NULL
           AND ke.victim_character_id > 0
         ORDER BY ke.sequence_id ASC",
        array_values($battleIds)
    );
}

function db_theater_suspicion_summary(string $theaterId): ?array
{
    return db_select_one(
        'SELECT * FROM theater_suspicion_summary WHERE theater_id = ?',
        [$theaterId]
    );
}

function db_theater_graph_summary(string $theaterId): ?array
{
    return db_select_one(
        'SELECT * FROM theater_graph_summary WHERE theater_id = ?',
        [$theaterId]
    );
}

function db_theater_graph_participants(string $theaterId, int $limit = 100): array
{
    return db_select(
        'SELECT tgp.*, tp.side,
                COALESCE(emc.entity_name, tp.character_name, CONCAT("Character #", tgp.character_id)) AS character_name
         FROM theater_graph_participants tgp
         LEFT JOIN theater_participants tp ON tp.theater_id = tgp.theater_id AND tp.character_id = tgp.character_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = "character" AND emc.entity_id = tgp.character_id
         WHERE tgp.theater_id = ?
         ORDER BY tgp.bridge_score DESC
         LIMIT ' . max(1, min(500, (int)$limit)),
        [$theaterId]
    );
}

function db_battle_turning_points(string $battleId): array
{
    return db_select(
        'SELECT * FROM battle_turning_points WHERE battle_id = ? ORDER BY turning_point_at ASC',
        [$battleId]
    );
}

function db_theater_turning_points(string $theaterId): array
{
    return db_select(
        'SELECT btp.*
         FROM battle_turning_points btp
         INNER JOIN theater_battles tb ON tb.battle_id = btp.battle_id
         WHERE tb.theater_id = ?
         ORDER BY btp.turning_point_at ASC',
        [$theaterId]
    );
}

function db_theater_side_labels(array $theaterIds): array
{
    if ($theaterIds === []) {
        return [];
    }
    $placeholders = implode(',', array_fill(0, count($theaterIds), '?'));
    $rows = db_select(
        "SELECT tas.theater_id, tas.alliance_id, tas.corporation_id, tas.side, tas.participant_count,
                CASE
                    WHEN tas.alliance_id > 0
                        THEN COALESCE(emc_a.entity_name, tas.alliance_name, CONCAT('Alliance #', tas.alliance_id))
                    WHEN tas.corporation_id > 0
                        THEN COALESCE(emc_c.entity_name, CONCAT('Corporation #', tas.corporation_id))
                    ELSE COALESCE(tas.alliance_name, 'Unknown')
                END AS alliance_name
         FROM theater_alliance_summary tas
         LEFT JOIN entity_metadata_cache emc_a
              ON emc_a.entity_type = 'alliance' AND emc_a.entity_id = tas.alliance_id AND tas.alliance_id > 0
         LEFT JOIN entity_metadata_cache emc_c
              ON emc_c.entity_type = 'corporation' AND emc_c.entity_id = tas.corporation_id AND tas.corporation_id > 0
         WHERE tas.theater_id IN ({$placeholders})
         ORDER BY tas.participant_count DESC",
        $theaterIds
    );

    // Load friendly/hostile alliance IDs from ESI contacts (+ manual additions)
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    $opponentAllianceIds = array_map('intval', array_column(db_killmail_opponent_alliances_active(), 'alliance_id'));

    $trackedSet = array_flip($trackedAllianceIds);
    $opponentSet = array_flip($opponentAllianceIds);

    // Group alliances per theater into friendly / hostile buckets based on
    // runtime classification — not the raw side column which may use legacy
    // side_a/side_b values or have gaps.
    $grouped = [];
    foreach ($rows as $r) {
        $tid = (string) $r['theater_id'];
        $aid = (int) $r['alliance_id'];
        $pilots = (int) $r['participant_count'];
        $name = (string) $r['alliance_name'];

        if ($aid > 0 && isset($trackedSet[$aid])) {
            $bucket = 'friendly';
        } elseif ($aid > 0 && isset($opponentSet[$aid])) {
            $bucket = 'hostile';
        } else {
            // Not-friendly defaults to hostile in a combat context
            $bucket = 'hostile';
        }

        if (!isset($grouped[$tid][$bucket])) {
            $grouped[$tid][$bucket] = [
                'top_name' => $name,
                'top_alliance_id' => $aid,
                'top_pilots' => $pilots,
                'count' => 0,
                'total_pilots' => 0,
            ];
        }
        $grouped[$tid][$bucket]['count']++;
        $grouped[$tid][$bucket]['total_pilots'] += $pilots;
        // Keep top alliance by pilot count
        if ($pilots > $grouped[$tid][$bucket]['top_pilots']) {
            $grouped[$tid][$bucket]['top_name'] = $name;
            $grouped[$tid][$bucket]['top_alliance_id'] = $aid;
            $grouped[$tid][$bucket]['top_pilots'] = $pilots;
        }
    }

    return $grouped;
}

function db_theater_fleet_composition(string $theaterId): array
{
    // Use flying_ship_type_id from theater_participants (the non-pod combat ship each pilot flew)
    // rather than battle_participants.ship_type_id which may be recorded as the capsule when a
    // pilot loses their ship and is subsequently podded.
    // Includes side from Python theater analysis so PHP can split into friendly/enemy compositions.
    return db_select(
        "SELECT tp.flying_ship_type_id AS ship_type_id,
                COALESCE(rit.type_name, CONCAT('Type #', tp.flying_ship_type_id)) AS ship_name,
                COALESCE(rig.group_name, '') AS ship_group,
                tp.side,
                COALESCE(tp.alliance_id, 0) AS alliance_id,
                COALESCE(tp.corporation_id, 0) AS corporation_id,
                COUNT(*) AS pilot_count
         FROM theater_participants tp
         LEFT JOIN ref_item_types rit ON rit.type_id = tp.flying_ship_type_id
         LEFT JOIN ref_item_groups rig ON rig.group_id = rit.group_id
         WHERE tp.theater_id = ?
           AND tp.flying_ship_type_id IS NOT NULL
           AND tp.flying_ship_type_id > 0
         GROUP BY tp.flying_ship_type_id, rit.type_name, rig.group_name,
                  tp.side, COALESCE(tp.alliance_id, 0), COALESCE(tp.corporation_id, 0)
         ORDER BY pilot_count DESC",
        [$theaterId]
    );
}

/**
 * Sum attacker damage_done per (alliance_id, corporation_id) across every
 * killmail in a theater.
 *
 * Aggregates the raw killmail_attackers rows so the caller can classify each
 * group to a side using the same closure as other per-side rollups. Rows with
 * no alliance and no corporation (NPCs, structures) fall into the (0,0) group
 * and should be reported as "unattributed" rather than silently dropped.
 *
 * @return list<array{alliance_id: int, corporation_id: int, total_damage: float}>
 */
function db_theater_damage_by_attacker_group(string $theaterId): array
{
    return db_select(
        "SELECT
            COALESCE(ka.alliance_id, 0) AS alliance_id,
            COALESCE(ka.corporation_id, 0) AS corporation_id,
            COALESCE(SUM(ka.damage_done), 0) AS total_damage
         FROM killmail_attackers ka
         INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
         INNER JOIN theater_battles tb ON tb.battle_id = ke.battle_id
         WHERE tb.theater_id = ?
         GROUP BY COALESCE(ka.alliance_id, 0), COALESCE(ka.corporation_id, 0)",
        [$theaterId]
    );
}

/**
 * Count final blows per attacker alliance/corporation for a theater.
 *
 * Returns raw per-group counts so the caller can classify sides consistently
 * using the same closure as kills/losses (avoids mismatch with Python's
 * graph-inferred side stored in theater_alliance_summary).
 *
 * @return list<array{alliance_id: int, corporation_id: int, final_blows: int, isk_killed: float}>
 */
function db_theater_final_blows_by_attacker_group(string $theaterId): array
{
    return db_select(
        "SELECT
            COALESCE(ka.alliance_id, 0) AS alliance_id,
            COALESCE(ka.corporation_id, 0) AS corporation_id,
            COUNT(*) AS final_blows,
            COALESCE(SUM(ke.zkb_total_value), 0) AS isk_killed
         FROM killmail_attackers ka
         INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
         INNER JOIN theater_battles tb ON tb.battle_id = ke.battle_id
         WHERE tb.theater_id = ?
           AND ka.final_blow = 1
           AND (ka.character_id IS NULL OR ka.character_id != ke.victim_character_id)
         GROUP BY COALESCE(ka.alliance_id, 0), COALESCE(ka.corporation_id, 0)",
        [$theaterId]
    );
}

/**
 * Count losses per victim alliance directly from killmail_events.
 *
 * Unlike the alliance_summary (which misses victims without an alliance),
 * this query counts ALL killmails in the theater grouped by victim_alliance_id
 * so the caller can classify each group with its own closure.
 *
 * @return list<array{victim_alliance_id: int, losses: int, isk_lost: float}>
 */
function db_theater_losses_by_victim_alliance(string $theaterId): array
{
    return db_select(
        "SELECT
            COALESCE(ke.victim_alliance_id, 0) AS victim_alliance_id,
            COALESCE(ke.victim_corporation_id, 0) AS victim_corporation_id,
            COUNT(DISTINCT ke.killmail_id) AS losses,
            COALESCE(SUM(ke.zkb_total_value), 0) AS isk_lost
         FROM killmail_events ke
         INNER JOIN theater_battles tb ON tb.battle_id = ke.battle_id
         WHERE tb.theater_id = ?
         GROUP BY ke.victim_alliance_id, ke.victim_corporation_id",
        [$theaterId]
    );
}

function db_theater_notable_kills(string $theaterId, int $limit = 10): array
{
    return db_select(
        'SELECT ke.killmail_id,
                ke.victim_character_id,
                ke.victim_alliance_id,
                ke.victim_ship_type_id,
                COALESCE(ke.zkb_total_value, 0) AS isk_value,
                ke.effective_killmail_at AS kill_time,
                COALESCE(emc_v.entity_name, CONCAT("Character #", ke.victim_character_id)) AS victim_name,
                COALESCE(rit.type_name, CONCAT("Type #", ke.victim_ship_type_id)) AS ship_name,
                COALESCE(rig.group_name, "") AS ship_group,
                COALESCE(emc_a.entity_name, CONCAT("Alliance #", ke.victim_alliance_id)) AS victim_alliance_name,
                COALESCE(tas.side, "") AS victim_side
         FROM killmail_events ke
         INNER JOIN theater_battles tb ON tb.battle_id = ke.battle_id
         LEFT JOIN entity_metadata_cache emc_v
              ON emc_v.entity_type = "character" AND emc_v.entity_id = ke.victim_character_id
         LEFT JOIN entity_metadata_cache emc_a
              ON emc_a.entity_type = "alliance" AND emc_a.entity_id = ke.victim_alliance_id
         LEFT JOIN ref_item_types rit ON rit.type_id = ke.victim_ship_type_id
         LEFT JOIN ref_item_groups rig ON rig.group_id = rit.group_id
         LEFT JOIN theater_alliance_summary tas
              ON tas.theater_id = tb.theater_id
              AND tas.alliance_id = COALESCE(ke.victim_alliance_id, 0)
              AND tas.corporation_id = CASE WHEN COALESCE(ke.victim_alliance_id, 0) > 0 THEN 0 ELSE COALESCE(ke.victim_corporation_id, 0) END
         WHERE tb.theater_id = ?
           AND ke.zkb_total_value IS NOT NULL
         ORDER BY ke.zkb_total_value DESC
         LIMIT ' . max(1, min(25, $limit)),
        [$theaterId]
    );
}

function db_theater_top_performers(string $theaterId, int $limit = 10): array
{
    return db_select(
        'SELECT tp.character_id,
                COALESCE(emc.entity_name, tp.character_name, CONCAT("Character #", tp.character_id)) AS character_name,
                tp.side,
                tp.kills,
                tp.deaths,
                tp.damage_done,
                tp.role_proxy,
                tp.battles_present,
                COALESCE(emc_a.entity_name, CONCAT("Alliance #", tp.alliance_id)) AS alliance_name,
                COALESCE(rit.type_name, "") AS ship_name,
                COALESCE(rig.group_name, "") AS ship_group
         FROM theater_participants tp
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = "character" AND emc.entity_id = tp.character_id
         LEFT JOIN entity_metadata_cache emc_a
              ON emc_a.entity_type = "alliance" AND emc_a.entity_id = tp.alliance_id
         LEFT JOIN ref_item_types rit ON rit.type_id = tp.flying_ship_type_id
         LEFT JOIN ref_item_groups rig ON rig.group_id = rit.group_id
         WHERE tp.theater_id = ?
         ORDER BY tp.damage_done DESC
         LIMIT ' . max(1, min(50, $limit)),
        [$theaterId]
    );
}

// ---------------------------------------------------------------------------
// Database migrations
// ---------------------------------------------------------------------------

function db_ensure_schema_migrations_table(): void
{
    db()->exec(
        'CREATE TABLE IF NOT EXISTS schema_migrations (
            id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            filename VARCHAR(255) NOT NULL,
            file_hash CHAR(64) NOT NULL,
            applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            duration_ms INT UNSIGNED NOT NULL DEFAULT 0,
            status ENUM("applied","failed") NOT NULL DEFAULT "applied",
            error_message TEXT DEFAULT NULL,
            UNIQUE KEY uq_schema_migrations_filename (filename)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );
}

function db_schema_migration_applied(string $filename): ?array
{
    return db_select_one(
        'SELECT id, filename, file_hash, applied_at, status FROM schema_migrations WHERE filename = ?',
        [$filename]
    );
}

function db_schema_migration_record(string $filename, string $fileHash, int $durationMs, string $status, ?string $errorMessage = null): void
{
    db_execute(
        'INSERT INTO schema_migrations (filename, file_hash, applied_at, duration_ms, status, error_message)
         VALUES (?, ?, UTC_TIMESTAMP(), ?, ?, ?)
         ON DUPLICATE KEY UPDATE
            file_hash = VALUES(file_hash),
            applied_at = VALUES(applied_at),
            duration_ms = VALUES(duration_ms),
            status = VALUES(status),
            error_message = VALUES(error_message)',
        [$filename, $fileHash, $durationMs, $status, $errorMessage]
    );
}

function db_schema_migrations_all(): array
{
    return db_select('SELECT filename, file_hash, applied_at, status, error_message FROM schema_migrations ORDER BY filename ASC');
}

// ── Item Criticality Index ─────────────────────────────────────────────────

function db_item_criticality_index(int $typeId): ?array
{
    $rows = db_select(
        'SELECT ici.*, COALESCE(rit.type_name, CONCAT("Type #", ici.type_id)) AS type_name
         FROM item_criticality_index ici
         LEFT JOIN ref_item_types rit ON rit.type_id = ici.type_id
         WHERE ici.type_id = ?
         LIMIT 1',
        [$typeId]
    );
    return $rows[0] ?? null;
}

function db_item_criticality_top(int $limit = 50, string $sort = 'priority_index'): array
{
    $allowed = ['priority_index', 'criticality_score', 'trend_score', 'market_stress_score', 'spof_impact_score', 'dependency_score'];
    $orderCol = in_array($sort, $allowed, true) ? $sort : 'priority_index';
    return db_select(
        "SELECT ici.*, COALESCE(rit.type_name, CONCAT('Type #', ici.type_id)) AS type_name
         FROM item_criticality_index ici
         LEFT JOIN ref_item_types rit ON rit.type_id = ici.type_id
         ORDER BY {$orderCol} DESC
         LIMIT ?",
        [max(1, min(200, $limit))]
    );
}

function db_item_graph_intelligence_by_type_ids(array $typeIds): array
{
    $typeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $id): bool => $id > 0)));
    if ($typeIds === []) {
        return [];
    }
    $placeholders = implode(', ', array_fill(0, count($typeIds), '?'));

    // Prefer item_criticality_index (richer), fall back to item_dependency_score.
    $rows = db_select(
        "SELECT
            ids.type_id,
            COALESCE(ici.dependency_score, ids.dependency_score, 0.0) AS dependency_score,
            COALESCE(ici.doctrine_count, ids.doctrine_count, 0) AS graph_doctrine_count,
            COALESCE(ici.fit_count, ids.fit_count, 0) AS graph_fit_count,
            COALESCE(ici.criticality_score, 0.0) AS criticality_score,
            COALESCE(ici.priority_index, 0.0) AS priority_index,
            COALESCE(ici.spof_flag, 0) AS spof_flag,
            COALESCE(ici.trend_score, 0.0) AS trend_score,
            COALESCE(ici.substitute_count, 0) AS substitute_count,
            ici.consumption_30d,
            ici.avg_daily_consumption,
            ici.stock_days_remaining
         FROM item_dependency_score ids
         LEFT JOIN item_criticality_index ici ON ici.type_id = ids.type_id
         WHERE ids.type_id IN ({$placeholders})
         UNION
         SELECT
            ici2.type_id,
            COALESCE(ici2.dependency_score, 0.0) AS dependency_score,
            COALESCE(ici2.doctrine_count, 0) AS graph_doctrine_count,
            COALESCE(ici2.fit_count, 0) AS graph_fit_count,
            COALESCE(ici2.criticality_score, 0.0) AS criticality_score,
            COALESCE(ici2.priority_index, 0.0) AS priority_index,
            COALESCE(ici2.spof_flag, 0) AS spof_flag,
            COALESCE(ici2.trend_score, 0.0) AS trend_score,
            COALESCE(ici2.substitute_count, 0) AS substitute_count,
            ici2.consumption_30d,
            ici2.avg_daily_consumption,
            ici2.stock_days_remaining
         FROM item_criticality_index ici2
         WHERE ici2.type_id IN ({$placeholders})
           AND ici2.type_id NOT IN (SELECT type_id FROM item_dependency_score WHERE type_id IN ({$placeholders}))",
        array_merge($typeIds, $typeIds, $typeIds)
    );
    $indexed = [];
    foreach ($rows as $row) {
        $indexed[(int) $row['type_id']] = $row;
    }
    return $indexed;
}

function db_item_spof_items(int $limit = 30): array
{
    return db_select(
        'SELECT ici.*, COALESCE(rit.type_name, CONCAT("Type #", ici.type_id)) AS type_name
         FROM item_criticality_index ici
         LEFT JOIN ref_item_types rit ON rit.type_id = ici.type_id
         WHERE ici.spof_flag = 1
         ORDER BY ici.spof_impact_score DESC, ici.criticality_score DESC
         LIMIT ?',
        [max(1, min(100, $limit))]
    );
}

// ---------------------------------------------------------------------------
// Alliance Dossiers
// ---------------------------------------------------------------------------

function db_alliance_dossiers_list(int $limit = 100, int $offset = 0, ?string $search = null): array
{
    $where = '';
    $params = [];
    if ($search !== null && trim($search) !== '') {
        $where = 'WHERE ad.alliance_name LIKE ?';
        $params[] = '%' . trim($search) . '%';
    }
    $params[] = max(1, min(200, $limit));
    $params[] = max(0, $offset);
    return db_select(
        "SELECT ad.*
         FROM alliance_dossiers ad
         {$where}
         ORDER BY ad.recent_killmails DESC, ad.total_killmails DESC
         LIMIT ? OFFSET ?",
        $params
    );
}

function db_alliance_dossier(int $allianceId): ?array
{
    $rows = db_select(
        'SELECT ad.*,
                emc.entity_name AS ref_alliance_name,
                rr.region_name AS primary_region_name,
                rs.system_name AS primary_system_name
         FROM alliance_dossiers ad
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = \'alliance\' AND emc.entity_id = ad.alliance_id
         LEFT JOIN ref_regions rr ON rr.region_id = ad.primary_region_id
         LEFT JOIN ref_systems rs ON rs.system_id = ad.primary_system_id
         WHERE ad.alliance_id = ?
         LIMIT 1',
        [$allianceId]
    );
    $row = $rows[0] ?? null;
    if ($row === null) {
        return null;
    }
    // Decode JSON columns
    foreach (['top_co_present_json', 'top_enemies_json', 'top_regions_json', 'top_systems_json', 'top_ship_classes_json', 'top_ship_types_json', 'behavior_summary_json', 'trend_summary_json'] as $col) {
        $raw = $row[$col] ?? null;
        $key = str_replace('_json', '', $col);
        $row[$key] = (is_string($raw) && trim($raw) !== '') ? (json_decode($raw, true) ?? []) : [];
    }
    return $row;
}

function db_alliance_dossiers_count(?string $search = null): int
{
    $where = '';
    $params = [];
    if ($search !== null && trim($search) !== '') {
        $where = 'WHERE alliance_name LIKE ?';
        $params[] = '%' . trim($search) . '%';
    }
    $rows = db_select("SELECT COUNT(*) AS cnt FROM alliance_dossiers {$where}", $params);
    return (int) ($rows[0]['cnt'] ?? 0);
}

// ---------------------------------------------------------------------------
// System Threat Scores (Theater Map)
// ---------------------------------------------------------------------------

function db_system_threat_scores(string $threatLevel = 'all', int $limit = 500): array
{
    $where = '';
    $params = [];
    if ($threatLevel !== 'all') {
        $where = 'WHERE sts.threat_level = ?';
        $params[] = $threatLevel;
    }
    $params[] = max(1, min(2000, $limit));
    return db_select(
        "SELECT sts.*, rs.system_name, rs.x, rs.y, rs.z, rs.security,
                rc.constellation_name, rr.region_name, rr.region_id
         FROM system_threat_scores sts
         LEFT JOIN ref_systems rs ON rs.system_id = sts.system_id
         LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         {$where}
         ORDER BY sts.hotspot_score DESC
         LIMIT ?",
        $params
    );
}

function db_system_threat_score(int $systemId): ?array
{
    $rows = db_select(
        'SELECT sts.*, rs.system_name, rs.x, rs.y, rs.z, rs.security,
                rc.constellation_name, rr.region_name
         FROM system_threat_scores sts
         LEFT JOIN ref_systems rs ON rs.system_id = sts.system_id
         LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         WHERE sts.system_id = ?
         LIMIT 1',
        [$systemId]
    );
    return $rows[0] ?? null;
}

function db_theater_map_systems(int $regionId = 0): array
{
    $where = 'WHERE sts.hotspot_score > 0';
    $params = [];
    if ($regionId > 0) {
        $where .= ' AND rs.region_id = ?';
        $params[] = $regionId;
    }
    $params[] = 2000;
    return db_select(
        "SELECT sts.system_id, sts.battle_count, sts.recent_battle_count,
                sts.total_kills, sts.total_isk_destroyed, sts.threat_level,
                sts.hotspot_score, sts.dominant_hostile_name, sts.last_battle_at,
                rs.system_name, rs.x, rs.y, rs.z, rs.security,
                rr.region_id, rr.region_name
         FROM system_threat_scores sts
         INNER JOIN ref_systems rs ON rs.system_id = sts.system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         {$where}
         ORDER BY sts.hotspot_score DESC
         LIMIT ?",
        $params
    );
}

function db_theater_map_regions(): array
{
    return db_select(
        'SELECT rr.region_id, rr.region_name, COUNT(sts.system_id) AS threat_systems,
                SUM(sts.battle_count) AS total_battles, MAX(sts.hotspot_score) AS max_hotspot
         FROM system_threat_scores sts
         INNER JOIN ref_systems rs ON rs.system_id = sts.system_id
         INNER JOIN ref_regions rr ON rr.region_id = rs.region_id
         WHERE sts.hotspot_score > 0
         GROUP BY rr.region_id, rr.region_name
         ORDER BY total_battles DESC',
        []
    );
}

// ---------------------------------------------------------------------------
// Manual Theater Creation
// ---------------------------------------------------------------------------

/**
 * Search ref_systems by name (exact or LIKE).
 */
function db_ref_system_search(string $query, int $limit = 20): array
{
    return db_select(
        'SELECT rs.system_id, rs.system_name, rs.constellation_id, rs.region_id,
                rr.region_name, rc.constellation_name
         FROM ref_systems rs
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
         WHERE rs.system_name LIKE ?
         ORDER BY
            CASE WHEN rs.system_name = ? THEN 0 ELSE 1 END,
            rs.system_name ASC
         LIMIT ?',
        ['%' . $query . '%', $query, $limit]
    );
}

/**
 * Find battles in a system (or all systems in the same constellation) within a time range.
 */
function db_battles_in_range(int $systemId, string $startTime, string $endTime, bool $includeConstellation = false): array
{
    if ($includeConstellation) {
        return db_select(
            'SELECT br.battle_id, br.system_id, br.started_at, br.ended_at,
                    br.participant_count, br.battle_size_class,
                    rs.system_name, rs.region_id
             FROM battle_rollups br
             INNER JOIN ref_systems rs ON rs.system_id = br.system_id
             WHERE rs.constellation_id = (SELECT constellation_id FROM ref_systems WHERE system_id = ? LIMIT 1)
               AND br.started_at <= ?
               AND br.ended_at   >= ?
             ORDER BY br.started_at ASC',
            [$systemId, $endTime, $startTime]
        );
    }

    return db_select(
        'SELECT br.battle_id, br.system_id, br.started_at, br.ended_at,
                br.participant_count, br.battle_size_class,
                rs.system_name, rs.region_id
         FROM battle_rollups br
         INNER JOIN ref_systems rs ON rs.system_id = br.system_id
         WHERE br.system_id = ?
           AND br.started_at <= ?
           AND br.ended_at   >= ?
         ORDER BY br.started_at ASC',
        [$systemId, $endTime, $startTime]
    );
}

/**
 * Create a manual theater from a set of battles.
 * Returns the theater_id on success, null on failure.
 */
function db_create_manual_theater(array $battles, ?string $label = null): ?string
{
    if ($battles === []) {
        return null;
    }

    // Deterministic theater_id from sorted battle IDs (mirrors Python _compute_theater_id)
    $battleIds = array_map(fn(array $b): string => (string) $b['battle_id'], $battles);
    sort($battleIds);
    $theaterId = hash('sha256', implode('|', $battleIds));

    // Check if this exact theater already exists
    $existing = db_select_one('SELECT theater_id, total_kills FROM theaters WHERE theater_id = ?', [$theaterId]);
    if ($existing !== null) {
        // If it exists but has no analysis data, repopulate summary tables
        if ((int) ($existing['total_kills'] ?? 0) === 0) {
            db_theater_finalize_manual($theaterId, $battleIds);
        }
        return $theaterId;
    }

    // Compute aggregates
    $startTimes = array_map(fn(array $b): string => (string) $b['started_at'], $battles);
    $endTimes = array_map(fn(array $b): string => (string) $b['ended_at'], $battles);
    sort($startTimes);
    rsort($endTimes);
    $startTime = $startTimes[0];
    $endTime = $endTimes[0];
    $durationSeconds = max(1, (int) (strtotime($endTime) - strtotime($startTime)));

    // Systems
    $systems = [];
    foreach ($battles as $b) {
        $sid = (int) $b['system_id'];
        if (!isset($systems[$sid])) {
            $systems[$sid] = [
                'system_id' => $sid,
                'system_name' => (string) ($b['system_name'] ?? ''),
                'kill_count' => 0,
                'participant_count' => 0,
            ];
        }
        $systems[$sid]['kill_count'] += (int) ($b['participant_count'] ?? 0);
        $systems[$sid]['participant_count'] += (int) ($b['participant_count'] ?? 0);
    }

    // Primary system = most participants
    $primarySystem = null;
    $primaryRegionId = null;
    foreach ($systems as $s) {
        if ($primarySystem === null || $s['participant_count'] > $primarySystem['participant_count']) {
            $primarySystem = $s;
        }
    }
    if ($primarySystem !== null) {
        // Get region from the first battle matching primary system
        foreach ($battles as $b) {
            if ((int) $b['system_id'] === $primarySystem['system_id'] && !empty($b['region_id'])) {
                $primaryRegionId = (int) $b['region_id'];
                break;
            }
        }
    }

    // Count unique participants across all battles
    $participantRows = db_select(
        'SELECT COUNT(DISTINCT character_id) AS cnt
         FROM battle_participants
         WHERE battle_id IN (' . implode(',', array_fill(0, count($battleIds), '?')) . ')',
        $battleIds
    );
    $participantCount = (int) ($participantRows[0]['cnt'] ?? 0);

    $now = date('Y-m-d H:i:s');

    // Insert theater
    db_execute(
        'INSERT INTO theaters (
            theater_id, label, primary_system_id, region_id,
            start_time, end_time, duration_seconds,
            battle_count, system_count, total_kills, total_isk,
            participant_count, anomaly_score,
            clustering_method, computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, 0, ?, ?)',
        [
            $theaterId,
            $label,
            $primarySystem['system_id'] ?? null,
            $primaryRegionId,
            $startTime,
            $endTime,
            $durationSeconds,
            count($battles),
            count($systems),
            $participantCount,
            'manual',
            $now,
        ]
    );

    // Insert theater_battles
    foreach ($battles as $b) {
        db_execute(
            'INSERT IGNORE INTO theater_battles (theater_id, battle_id, system_id, weight, phase)
             VALUES (?, ?, ?, 1.0000, NULL)',
            [$theaterId, (string) $b['battle_id'], (int) $b['system_id']]
        );
    }

    // Insert theater_systems
    foreach ($systems as $s) {
        db_execute(
            'INSERT IGNORE INTO theater_systems (theater_id, system_id, system_name, kill_count, isk_destroyed, participant_count, weight, phase)
             VALUES (?, ?, ?, ?, 0, ?, 1.0000, NULL)',
            [$theaterId, $s['system_id'], $s['system_name'], $s['kill_count'], $s['participant_count']]
        );
    }

    db_theater_finalize_manual($theaterId, $battleIds);

    return $theaterId;
}

/**
 * Deterministic finalize: truncate derived tables and rebuild from the
 * authoritative constituent battle set in theater_battles.
 *
 * Source of truth: theater_battles -> killmail_events/killmail_attackers.
 * All higher-level tables are derived outputs that get wiped and rebuilt.
 *
 * Called on:
 * - Manual theater creation
 * - Lock & Generate AI Report (before snapshot)
 * - Re-creation of existing theater with stale data
 */
function db_theater_finalize_manual(string $theaterId, ?array $battleIds = null): void
{
    // Resolve battle IDs from theater_battles if not provided
    if ($battleIds === null) {
        $rows = db_select(
            'SELECT battle_id FROM theater_battles WHERE theater_id = ?',
            [$theaterId]
        );
        $battleIds = array_map(fn(array $r): string => (string) $r['battle_id'], $rows);
    }

    if ($battleIds === []) {
        return;
    }
    $placeholders = implode(',', array_fill(0, count($battleIds), '?'));

    // ── Truncate all derived tables for this theater ────────────────────
    db_execute('DELETE FROM theater_alliance_summary WHERE theater_id = ?', [$theaterId]);
    db_execute('DELETE FROM theater_participants WHERE theater_id = ?', [$theaterId]);
    db_execute('DELETE FROM theater_timeline WHERE theater_id = ?', [$theaterId]);
    db_execute('DELETE FROM theater_side_composition WHERE theater_id = ?', [$theaterId]);
    db_execute('DELETE FROM theater_suspicion_summary WHERE theater_id = ?', [$theaterId]);
    db_execute('DELETE FROM theater_graph_summary WHERE theater_id = ?', [$theaterId]);
    db_execute('DELETE FROM theater_graph_participants WHERE theater_id = ?', [$theaterId]);
    db_execute('DELETE FROM theater_structure_kills WHERE theater_id = ?', [$theaterId]);
    // theater_battles + theater_systems stay — they define the battle set
    // and are updated below with actual kill/ISK counts.

    // Clear stale snapshot and AI cache so they don't survive the rebuild
    db_execute(
        'UPDATE theaters SET snapshot_data = NULL, ai_summary = NULL, ai_headline = NULL,
                ai_verdict = NULL, ai_summary_model = NULL, ai_summary_at = NULL
         WHERE theater_id = ?',
        [$theaterId]
    );

    // ── Rebuild from killmail source of truth ───────────────────────────

    // 1. Compute total_kills and total_isk from killmail_events
    $killStats = db_select_one(
        "SELECT COUNT(DISTINCT ke.killmail_id) AS total_kills,
                COALESCE(SUM(ke.zkb_total_value), 0) AS total_isk
         FROM killmail_events ke
         WHERE ke.battle_id IN ({$placeholders})",
        $battleIds
    );
    $totalKills = (int) ($killStats['total_kills'] ?? 0);
    $totalIsk = (float) ($killStats['total_isk'] ?? 0);

    // Recount unique participants from battle_participants (authoritative)
    $participantStats = db_select_one(
        "SELECT COUNT(DISTINCT character_id) AS cnt
         FROM battle_participants
         WHERE battle_id IN ({$placeholders})",
        $battleIds
    );
    $participantCount = (int) ($participantStats['cnt'] ?? 0);

    db_execute(
        'UPDATE theaters SET total_kills = ?, total_isk = ?, participant_count = ? WHERE theater_id = ?',
        [$totalKills, $totalIsk, $participantCount, $theaterId]
    );

    // Update theater_systems with actual kill counts and ISK
    $systemKills = db_select(
        "SELECT ke.solar_system_id AS system_id,
                COUNT(DISTINCT ke.killmail_id) AS kill_count,
                COALESCE(SUM(ke.zkb_total_value), 0) AS isk_destroyed
         FROM killmail_events ke
         WHERE ke.battle_id IN ({$placeholders})
           AND ke.solar_system_id IS NOT NULL
         GROUP BY ke.solar_system_id",
        $battleIds
    );
    foreach ($systemKills as $sk) {
        db_execute(
            'UPDATE theater_systems SET kill_count = ?, isk_destroyed = ?
             WHERE theater_id = ? AND system_id = ?',
            [(int) $sk['kill_count'], (float) $sk['isk_destroyed'], $theaterId, (int) $sk['system_id']]
        );
    }

    // 2. Populate theater_alliance_summary from killmail data
    // Victims: each unique killmail = 1 loss for the victim's alliance
    $victimSummary = db_select(
        "SELECT COALESCE(ke.victim_alliance_id, 0) AS alliance_id,
                COALESCE(ke.victim_corporation_id, 0) AS corporation_id,
                COUNT(DISTINCT ke.killmail_id) AS losses,
                COALESCE(SUM(ke.zkb_total_value), 0) AS isk_lost
         FROM killmail_events ke
         WHERE ke.battle_id IN ({$placeholders})
           AND (ke.victim_character_id IS NOT NULL AND ke.victim_character_id > 0)
         GROUP BY COALESCE(ke.victim_alliance_id, 0), COALESCE(ke.victim_corporation_id, 0)",
        $battleIds
    );

    // Attackers: final blow = 1 kill credit for the attacker's alliance
    $attackerSummary = db_select(
        "SELECT COALESCE(ka.alliance_id, 0) AS alliance_id,
                COALESCE(ka.corporation_id, 0) AS corporation_id,
                COUNT(DISTINCT ke.killmail_id) AS kills,
                COALESCE(SUM(ke.zkb_total_value), 0) AS isk_killed
         FROM killmail_events ke
         INNER JOIN killmail_attackers ka ON ka.sequence_id = ke.sequence_id AND ka.final_blow = 1
         WHERE ke.battle_id IN ({$placeholders})
         GROUP BY COALESCE(ka.alliance_id, 0), COALESCE(ka.corporation_id, 0)",
        $battleIds
    );

    // Participant counts per alliance from battle_participants
    $allianceParticipants = db_select(
        "SELECT COALESCE(bp.alliance_id, 0) AS alliance_id,
                COALESCE(bp.corporation_id, 0) AS corporation_id,
                COUNT(DISTINCT bp.character_id) AS participant_count
         FROM battle_participants bp
         WHERE bp.battle_id IN ({$placeholders})
         GROUP BY COALESCE(bp.alliance_id, 0), COALESCE(bp.corporation_id, 0)",
        $battleIds
    );

    // Merge into a single summary keyed by alliance_id:corporation_id
    $allianceMerged = [];
    $makeKey = static fn(int $aid, int $cid): string => $aid . ':' . $cid;
    $ensureEntry = static function (string $key, int $aid, int $cid) use (&$allianceMerged): void {
        if (!isset($allianceMerged[$key])) {
            $allianceMerged[$key] = [
                'alliance_id' => $aid, 'corporation_id' => $cid,
                'participant_count' => 0, 'kills' => 0, 'losses' => 0,
                'isk_killed' => 0.0, 'isk_lost' => 0.0,
            ];
        }
    };

    foreach ($allianceParticipants as $row) {
        $key = $makeKey((int) $row['alliance_id'], (int) $row['corporation_id']);
        $ensureEntry($key, (int) $row['alliance_id'], (int) $row['corporation_id']);
        $allianceMerged[$key]['participant_count'] = (int) $row['participant_count'];
    }
    foreach ($victimSummary as $row) {
        $key = $makeKey((int) $row['alliance_id'], (int) $row['corporation_id']);
        $ensureEntry($key, (int) $row['alliance_id'], (int) $row['corporation_id']);
        $allianceMerged[$key]['losses'] += (int) $row['losses'];
        $allianceMerged[$key]['isk_lost'] += (float) $row['isk_lost'];
    }
    foreach ($attackerSummary as $row) {
        $key = $makeKey((int) $row['alliance_id'], (int) $row['corporation_id']);
        $ensureEntry($key, (int) $row['alliance_id'], (int) $row['corporation_id']);
        $allianceMerged[$key]['kills'] += (int) $row['kills'];
        $allianceMerged[$key]['isk_killed'] += (float) $row['isk_killed'];
    }

    // Classify sides using tracked/opponent alliance config
    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    $opponentAllianceIds = array_map('intval', array_column(db_killmail_opponent_alliances_active(), 'alliance_id'));
    $trackedCorpIds = array_map('intval', array_column(db_killmail_tracked_corporations_active(), 'corporation_id'));
    $opponentCorpIds = array_map('intval', array_column(db_killmail_opponent_corporations_active(), 'corporation_id'));

    $classifySide = static function (int $aid, int $cid) use ($trackedAllianceIds, $opponentAllianceIds, $trackedCorpIds, $opponentCorpIds): string {
        if ($aid > 0 && in_array($aid, $trackedAllianceIds, true)) return 'friendly';
        if ($cid > 0 && in_array($cid, $trackedCorpIds, true)) return 'friendly';
        if ($aid > 0 && in_array($aid, $opponentAllianceIds, true)) return 'opponent';
        if ($cid > 0 && in_array($cid, $opponentCorpIds, true)) return 'opponent';
        return 'third_party';
    };

    foreach ($allianceMerged as $entry) {
        $aid = (int) $entry['alliance_id'];
        $cid = (int) $entry['corporation_id'];
        if ($aid === 0 && $cid === 0) continue;
        $side = $classifySide($aid, $cid);
        $totalIskEntry = $entry['isk_killed'] + $entry['isk_lost'];
        $efficiency = $totalIskEntry > 0 ? $entry['isk_killed'] / $totalIskEntry : 0.0;

        db_execute(
            'INSERT IGNORE INTO theater_alliance_summary
                (theater_id, alliance_id, corporation_id, alliance_name, side,
                 participant_count, total_kills, total_losses, total_damage,
                 total_isk_killed, total_isk_lost, efficiency)
             VALUES (?, ?, ?, NULL, ?, ?, ?, ?, 0, ?, ?, ?)',
            [
                $theaterId, $aid, $cid, $side,
                (int) $entry['participant_count'],
                (int) $entry['kills'],
                (int) $entry['losses'],
                $entry['isk_killed'],
                $entry['isk_lost'],
                round($efficiency, 4),
            ]
        );
    }

    // 3. Populate theater_participants from battle_participants + killmail ledger
    $charLedger = db_select(
        "SELECT bp.character_id,
                MAX(bp.alliance_id) AS alliance_id,
                MAX(bp.corporation_id) AS corporation_id,
                COUNT(DISTINCT bp.battle_id) AS battles_present,
                COALESCE(final_kills.kill_count, 0) AS final_kills,
                COALESCE(involvements.involved_count, 0) AS contributed_kills,
                COALESCE(final_kills.isk_killed, 0) AS isk_killed,
                COALESCE(losses.loss_count, 0) AS deaths,
                COALESCE(dmg.damage_done, 0) AS damage_done,
                COALESCE(losses.isk_lost, 0) AS isk_lost
         FROM battle_participants bp
         LEFT JOIN (
             SELECT ka.character_id,
                    COUNT(DISTINCT ke.killmail_id) AS kill_count,
                    COALESCE(SUM(ke.zkb_total_value), 0) AS isk_killed
             FROM killmail_attackers ka
             INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
             WHERE ke.battle_id IN ({$placeholders})
               AND ka.character_id IS NOT NULL AND ka.character_id > 0
               AND ka.final_blow = 1
             GROUP BY ka.character_id
         ) final_kills ON final_kills.character_id = bp.character_id
         LEFT JOIN (
             SELECT ka.character_id, COUNT(DISTINCT ke.killmail_id) AS involved_count
             FROM killmail_attackers ka
             INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
             WHERE ke.battle_id IN ({$placeholders})
               AND ka.character_id IS NOT NULL AND ka.character_id > 0
             GROUP BY ka.character_id
         ) involvements ON involvements.character_id = bp.character_id
         LEFT JOIN (
             SELECT ke.victim_character_id AS character_id,
                    COUNT(DISTINCT ke.killmail_id) AS loss_count,
                    COALESCE(SUM(ke.zkb_total_value), 0) AS isk_lost
             FROM killmail_events ke
             WHERE ke.battle_id IN ({$placeholders})
               AND ke.victim_character_id IS NOT NULL AND ke.victim_character_id > 0
             GROUP BY ke.victim_character_id
         ) losses ON losses.character_id = bp.character_id
         LEFT JOIN (
             SELECT ka.character_id, COALESCE(SUM(ka.damage_done), 0) AS damage_done
             FROM killmail_attackers ka
             INNER JOIN killmail_events ke ON ke.sequence_id = ka.sequence_id
             WHERE ke.battle_id IN ({$placeholders})
               AND ka.character_id IS NOT NULL AND ka.character_id > 0
             GROUP BY ka.character_id
         ) dmg ON dmg.character_id = bp.character_id
         WHERE bp.battle_id IN ({$placeholders})
         GROUP BY bp.character_id",
        array_merge($battleIds, $battleIds, $battleIds, $battleIds, $battleIds)
    );

    foreach ($charLedger as $ch) {
        $chAid = (int) ($ch['alliance_id'] ?? 0);
        $chCid = (int) ($ch['corporation_id'] ?? 0);
        $chSide = $classifySide($chAid, $chCid);
        db_execute(
            'INSERT IGNORE INTO theater_participants
                (theater_id, character_id, character_name, alliance_id, corporation_id, side,
                 kills, final_kills, contributed_kills, isk_killed,
                 deaths, damage_done, damage_taken, isk_lost, battles_present)
             VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)',
            [
                $theaterId,
                (int) $ch['character_id'],
                $chAid,
                $chCid,
                $chSide,
                (int) $ch['final_kills'],
                (int) $ch['final_kills'],
                (int) $ch['contributed_kills'],
                (float) $ch['isk_killed'],
                (int) $ch['deaths'],
                (float) $ch['damage_done'],
                (float) $ch['isk_lost'],
                (int) $ch['battles_present'],
            ]
        );
    }

    // 4. Populate theater_timeline (15-min buckets matching battle windows)
    $timelineBuckets = db_select(
        "SELECT DATE_FORMAT(ke.effective_killmail_at, '%Y-%m-%d %H:%i:00') AS bucket_time,
                COUNT(DISTINCT ke.killmail_id) AS kills,
                COALESCE(SUM(ke.zkb_total_value), 0) AS isk_destroyed
         FROM killmail_events ke
         WHERE ke.battle_id IN ({$placeholders})
         GROUP BY DATE_FORMAT(ke.effective_killmail_at, '%Y-%m-%d %H:%i:00')
         ORDER BY bucket_time ASC",
        $battleIds
    );
    foreach ($timelineBuckets as $tb) {
        db_execute(
            'INSERT IGNORE INTO theater_timeline
                (theater_id, bucket_time, bucket_seconds, kills, isk_destroyed,
                 side_a_kills, side_b_kills, side_a_isk, side_b_isk, momentum_score)
             VALUES (?, ?, 60, ?, ?, 0, 0, 0, 0, 0)',
            [$theaterId, $tb['bucket_time'], (int) $tb['kills'], (float) $tb['isk_destroyed']]
        );
    }
}

// ---------------------------------------------------------------------------
// Threat Corridors
// ---------------------------------------------------------------------------

function db_threat_corridors_list(int $limit = 50, int $offset = 0, int $regionId = 0): array
{
    $where = 'WHERE tc.is_active = 1';
    $params = [];
    if ($regionId > 0) {
        $where .= ' AND tc.region_id = ?';
        $params[] = $regionId;
    }
    $params[] = max(1, min(200, $limit));
    $params[] = max(0, $offset);
    $rows = db_select(
        "SELECT tc.*, rr.region_name
         FROM threat_corridors tc
         LEFT JOIN ref_regions rr ON rr.region_id = tc.region_id
         {$where}
         ORDER BY tc.corridor_score DESC
         LIMIT ? OFFSET ?",
        $params
    );
    foreach ($rows as &$row) {
        $row['system_ids'] = json_decode($row['system_ids_json'] ?? '[]', true) ?: [];
        $row['system_names'] = json_decode($row['system_names_json'] ?? '[]', true) ?: [];
        $row['hostile_alliance_ids'] = json_decode($row['hostile_alliance_ids_json'] ?? '[]', true) ?: [];
    }
    unset($row);
    return $rows;
}

function db_threat_corridor(int $corridorId): ?array
{
    $rows = db_select(
        'SELECT tc.*, rr.region_name
         FROM threat_corridors tc
         LEFT JOIN ref_regions rr ON rr.region_id = tc.region_id
         WHERE tc.corridor_id = ?
         LIMIT 1',
        [$corridorId]
    );
    $row = $rows[0] ?? null;
    if ($row === null) {
        return null;
    }
    $row['system_ids'] = json_decode($row['system_ids_json'] ?? '[]', true) ?: [];
    $row['system_names'] = json_decode($row['system_names_json'] ?? '[]', true) ?: [];
    $row['hostile_alliance_ids'] = json_decode($row['hostile_alliance_ids_json'] ?? '[]', true) ?: [];
    return $row;
}

function db_threat_corridor_systems(int $corridorId): array
{
    return db_select(
        'SELECT tcs.*, rs.system_name, rs.security,
                sts.threat_level, sts.hotspot_score, sts.dominant_hostile_name
         FROM threat_corridor_systems tcs
         LEFT JOIN ref_systems rs ON rs.system_id = tcs.system_id
         LEFT JOIN system_threat_scores sts ON sts.system_id = tcs.system_id
         WHERE tcs.corridor_id = ?
         ORDER BY tcs.position_in_corridor ASC',
        [$corridorId]
    );
}

function db_threat_corridors_count(int $regionId = 0): int
{
    $where = 'WHERE is_active = 1';
    $params = [];
    if ($regionId > 0) {
        $where .= ' AND region_id = ?';
        $params[] = $regionId;
    }
    $rows = db_select("SELECT COUNT(*) AS cnt FROM threat_corridors {$where}", $params);
    return (int) ($rows[0]['cnt'] ?? 0);
}

function db_threat_corridor_regions(): array
{
    return db_select(
        'SELECT rr.region_id, rr.region_name, COUNT(tc.corridor_id) AS corridor_count,
                MAX(tc.corridor_score) AS max_score
         FROM threat_corridors tc
         INNER JOIN ref_regions rr ON rr.region_id = tc.region_id
         WHERE tc.is_active = 1
         GROUP BY rr.region_id, rr.region_name
         ORDER BY corridor_count DESC',
        []
    );
}

function db_threat_corridor_graph_subgraph(array $corridorSystemIds, int $surroundingHops = 1): array
{
    $corridorSystemIds = array_values(array_unique(array_map('intval', $corridorSystemIds)));
    $corridorSystemIds = array_values(array_filter($corridorSystemIds, static fn (int $sid): bool => $sid > 0));
    if ($corridorSystemIds === []) {
        return ['nodes' => [], 'edges' => []];
    }

    $surroundingHops = max(0, min(3, $surroundingHops));
    $nodeIds = [];
    $edgePairs = [];

    $neoRows = neo4j_query(
        '
        UNWIND $corridor_ids AS corridor_id
        MATCH (c:System {system_id: corridor_id})
        MATCH (c)-[:CONNECTS_TO|JUMP_BRIDGE*0..' . $surroundingHops . ']-(n:System)
        WITH collect(DISTINCT n.system_id) AS node_ids
        UNWIND node_ids AS a_id
        MATCH (a:System {system_id: a_id})-[:CONNECTS_TO|JUMP_BRIDGE]-(b:System)
        WHERE b.system_id IN node_ids
        RETURN node_ids,
               collect(DISTINCT [a.system_id, b.system_id]) AS edge_pairs
        ',
        ['corridor_ids' => $corridorSystemIds]
    );

    if ($neoRows !== []) {
        $row = $neoRows[0];
        $nodeIds = array_values(array_unique(array_map('intval', (array) ($row['node_ids'] ?? []))));
        foreach ((array) ($row['edge_pairs'] ?? []) as $pair) {
            $a = (int) ($pair[0] ?? 0);
            $b = (int) ($pair[1] ?? 0);
            if ($a <= 0 || $b <= 0 || $a === $b) {
                continue;
            }
            $left = min($a, $b);
            $right = max($a, $b);
            $edgePairs[$left . ':' . $right] = [$left, $right];
        }
    }

    // Fall through to SQL if Neo4j returned only the corridor systems themselves
    // (hop-0 self-match) without actually expanding to surrounding neighbors.
    $corridorSet = array_fill_keys($corridorSystemIds, true);
    $neoExpanded = false;
    foreach ($nodeIds as $nid) {
        if (!isset($corridorSet[$nid])) {
            $neoExpanded = true;
            break;
        }
    }
    if ($nodeIds === [] || (!$neoExpanded && $surroundingHops > 0)) {
        $nodeIds = $corridorSystemIds;
        $edgePairs = [];
        $frontier = $corridorSystemIds;
        $seen = array_fill_keys($corridorSystemIds, true);
        for ($hop = 0; $hop < $surroundingHops; $hop++) {
            if ($frontier === []) {
                break;
            }
            $placeholders = implode(',', array_fill(0, count($frontier), '?'));
            $rows = db_select(
                "SELECT system_id, dest_system_id
                 FROM ref_stargates
                 WHERE system_id IN ({$placeholders})",
                $frontier
            );
            $nextFrontier = [];
            foreach ($rows as $r) {
                $a = (int) ($r['system_id'] ?? 0);
                $b = (int) ($r['dest_system_id'] ?? 0);
                if ($a <= 0 || $b <= 0 || $a === $b) {
                    continue;
                }
                if (!isset($seen[$b])) {
                    $seen[$b] = true;
                    $nextFrontier[] = $b;
                }
            }
            $frontier = array_values(array_unique($nextFrontier));
        }
        $nodeIds = array_map('intval', array_keys($seen));
        if ($nodeIds !== []) {
            $nodePlaceholders = implode(',', array_fill(0, count($nodeIds), '?'));
            $rows = db_select(
                "SELECT system_id, dest_system_id
                 FROM ref_stargates
                 WHERE system_id IN ({$nodePlaceholders})
                   AND dest_system_id IN ({$nodePlaceholders})",
                array_merge($nodeIds, $nodeIds)
            );
            foreach ($rows as $r) {
                $a = (int) ($r['system_id'] ?? 0);
                $b = (int) ($r['dest_system_id'] ?? 0);
                if ($a <= 0 || $b <= 0 || $a === $b) {
                    continue;
                }
                $left = min($a, $b);
                $right = max($a, $b);
                $edgePairs[$left . ':' . $right] = [$left, $right];
            }
        }
    }

    if ($nodeIds === []) {
        return ['nodes' => [], 'edges' => []];
    }

    $nodePlaceholders = implode(',', array_fill(0, count($nodeIds), '?'));
    $nodes = db_select(
        "SELECT rs.system_id, rs.system_name, rs.security, sts.threat_level
         FROM ref_systems rs
         LEFT JOIN system_threat_scores sts ON sts.system_id = rs.system_id
         WHERE rs.system_id IN ({$nodePlaceholders})",
        $nodeIds
    );
    $nodeMap = [];
    foreach ($nodes as $row) {
        $sid = (int) ($row['system_id'] ?? 0);
        if ($sid <= 0) {
            continue;
        }
        $nodeMap[$sid] = [
            'system_id' => $sid,
            'system_name' => (string) ($row['system_name'] ?? (string) $sid),
            'security' => (float) ($row['security'] ?? 0.0),
            'threat_level' => (string) ($row['threat_level'] ?? ''),
        ];
    }
    foreach ($nodeIds as $sid) {
        $sid = (int) $sid;
        if ($sid <= 0 || isset($nodeMap[$sid])) {
            continue;
        }
        $nodeMap[$sid] = [
            'system_id' => $sid,
            'system_name' => (string) $sid,
            'security' => 0.0,
            'threat_level' => '',
        ];
    }

    return [
        'nodes' => array_values($nodeMap),
        'edges' => array_values($edgePairs),
    ];
}

/**
 * Find the shortest route between two solar systems via stargates.
 *
 * Returns an ordered array of system IDs from $srcSystemId to $dstSystemId
 * (inclusive), or an empty array when no path exists.
 *
 * Tries Neo4j shortestPath first; falls back to BFS over ref_stargates.
 */
function db_shortest_route_between_systems(int $srcSystemId, int $dstSystemId): array
{
    if ($srcSystemId <= 0 || $dstSystemId <= 0 || $srcSystemId === $dstSystemId) {
        return [$srcSystemId];
    }

    // --- Neo4j attempt ---
    $neoRows = neo4j_query(
        '
        MATCH (src:System {system_id: $src_id}), (dst:System {system_id: $dst_id})
        MATCH p = shortestPath((src)-[:CONNECTS_TO*..50]-(dst))
        RETURN [n IN nodes(p) | n.system_id] AS route
        ',
        ['src_id' => $srcSystemId, 'dst_id' => $dstSystemId]
    );
    if ($neoRows !== []) {
        $route = array_map('intval', (array) ($neoRows[0]['route'] ?? []));
        if (count($route) >= 2) {
            return $route;
        }
    }

    // --- SQL BFS fallback ---
    $maxHops = 50;
    $prev = [$srcSystemId => -1];
    $frontier = [$srcSystemId];
    $found = false;
    for ($hop = 0; $hop < $maxHops && $frontier !== []; $hop++) {
        $placeholders = implode(',', array_fill(0, count($frontier), '?'));
        $rows = db_select(
            "SELECT system_id, dest_system_id FROM ref_stargates WHERE system_id IN ({$placeholders})",
            $frontier
        );
        $nextFrontier = [];
        foreach ($rows as $r) {
            $nb = (int) ($r['dest_system_id'] ?? 0);
            if ($nb <= 0 || isset($prev[$nb])) {
                continue;
            }
            $prev[$nb] = (int) ($r['system_id'] ?? 0);
            if ($nb === $dstSystemId) {
                $found = true;
                break 2;
            }
            $nextFrontier[] = $nb;
        }
        $frontier = array_values(array_unique($nextFrontier));
    }

    if (!$found) {
        return [];
    }

    $route = [];
    $cur = $dstSystemId;
    while ($cur !== -1) {
        $route[] = $cur;
        $cur = $prev[$cur] ?? -1;
    }
    return array_reverse($route);
}

// ---------------------------------------------------------------------------
// Neo4j Intelligence Graph — PHP query layer (HTTP transactional endpoint)
// ---------------------------------------------------------------------------

/**
 * Execute a Cypher query against Neo4j via HTTP API.
 * Returns array of result rows or empty array on failure / disabled.
 */
function neo4j_query(string $cypher, array $parameters = []): array
{
    if (!(bool) config('neo4j.enabled', false)) {
        return [];
    }

    $baseUrl = rtrim((string) config('neo4j.url', 'http://127.0.0.1:7474'), '/');
    $database = (string) config('neo4j.database', 'neo4j');
    $url = $baseUrl . '/db/' . $database . '/tx/commit';
    $timeout = max(3, (int) config('neo4j.timeout_seconds', 15));

    $payload = json_encode([
        'statements' => [[
            'statement' => $cypher,
            'parameters' => (object) $parameters,
            'resultDataContents' => ['row'],
        ]],
    ]);

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_POSTFIELDS => $payload,
        CURLOPT_TIMEOUT => $timeout,
        CURLOPT_CONNECTTIMEOUT => 3,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Accept: application/json',
        ],
    ]);

    $username = (string) config('neo4j.username', 'neo4j');
    $password = (string) config('neo4j.password', '');
    if ($username !== '') {
        curl_setopt($ch, CURLOPT_USERPWD, $username . ':' . $password);
    }

    $response = curl_exec($ch);
    $httpCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    if ($httpCode < 200 || $httpCode >= 300 || !is_string($response)) {
        return [];
    }

    $data = json_decode($response, true);
    if (!is_array($data) || !empty($data['errors'])) {
        return [];
    }

    $results = $data['results'][0] ?? null;
    if (!is_array($results)) {
        return [];
    }

    $columns = (array) ($results['columns'] ?? []);
    $rows = [];
    foreach ((array) ($results['data'] ?? []) as $datum) {
        $row = [];
        foreach ($columns as $i => $col) {
            $row[$col] = $datum['row'][$i] ?? null;
        }
        $rows[] = $row;
    }

    return $rows;
}

/**
 * Cross-side shared corp history for a theater/battle.
 * Returns hostile pilots who previously served in the same corp as a friendly pilot.
 */
function db_neo4j_cross_side_shared_history(string $battleId, int $limit = 200): array
{
    return neo4j_query(
        'MATCH (hostile:Character)-[:PARTICIPATED_IN {side: "hostile"}]->(b:Battle {battle_id: $battleId})
         MATCH (friendly:Character)-[:PARTICIPATED_IN {side: "friendly"}]->(b)
         MATCH (hostile)-[:MEMBER_OF]->(corp:Corporation)<-[:MEMBER_OF]-(friendly)
         WHERE NOT corp.is_npc
         RETURN
           hostile.character_id AS hostile_id,
           hostile.name AS hostile_pilot,
           friendly.character_id AS friendly_id,
           friendly.name AS friendly_pilot,
           corp.corporation_id AS shared_corp_id,
           corp.name AS shared_corp_name
         ORDER BY hostile.name
         LIMIT $limit',
        ['battleId' => $battleId, 'limit' => $limit]
    );
}

/**
 * Recent defectors: hostile pilots who left a friendly-aligned corp within N days.
 */
function db_neo4j_recent_defectors(string $battleId, int $withinDays = 90): array
{
    return neo4j_query(
        'MATCH (c:Character)-[:PARTICIPATED_IN {side: "hostile"}]->(b:Battle {battle_id: $battleId})
         MATCH (c)-[m:MEMBER_OF]->(corp:Corporation)
         WHERE m.to IS NOT NULL
           AND NOT corp.is_npc
           AND duration.inDays(m.to, datetime()).days < $withinDays
         MATCH (friendly:Character)-[:PARTICIPATED_IN {side: "friendly"}]->(b)
         MATCH (friendly)-[:MEMBER_OF]->(corp)
         RETURN DISTINCT
           c.character_id AS character_id,
           c.name AS pilot_name,
           corp.name AS corp_name,
           corp.corporation_id AS corp_id,
           toString(m.to) AS left_on,
           duration.inDays(m.to, datetime()).days AS days_ago
         ORDER BY days_ago ASC
         LIMIT 50',
        ['battleId' => $battleId, 'withinDays' => $withinDays]
    );
}

/**
 * Alliance infiltration risk score for a battle.
 */
function db_neo4j_alliance_infiltration_risk(string $battleId, int $friendlyAllianceId): array
{
    $rows = neo4j_query(
        'MATCH (hostile:Character)-[:PARTICIPATED_IN {side: "hostile"}]->(b:Battle {battle_id: $battleId})
         MATCH (friendly_corp:Corporation)<-[:PART_OF]-(:Alliance {alliance_id: $friendlyAllianceId})
         MATCH (hostile)-[m:MEMBER_OF]->(friendly_corp)
         WHERE NOT friendly_corp.is_npc
         RETURN
           count(DISTINCT hostile) AS pilots_with_friendly_history,
           count(DISTINCT CASE WHEN duration.inDays(m.to, datetime()).days < 90
             THEN hostile END) AS recent_defectors,
           count(DISTINCT CASE WHEN m.is_short_stay
             THEN hostile END) AS short_stay_visits',
        ['battleId' => $battleId, 'friendlyAllianceId' => $friendlyAllianceId]
    );
    return $rows[0] ?? ['pilots_with_friendly_history' => 0, 'recent_defectors' => 0, 'short_stay_visits' => 0];
}

/**
 * Cross-side corp overlap for a specific character in a battle context.
 */
function db_neo4j_character_cross_side_overlap(int $characterId, string $battleId): array
{
    return neo4j_query(
        'MATCH (c:Character {character_id: $charId})-[:PARTICIPATED_IN]->(b:Battle {battle_id: $battleId})
         MATCH (c)-[:CURRENT_CORP]->(corp:Corporation)<-[:CURRENT_CORP]-(other:Character)
         MATCH (other)-[p:PARTICIPATED_IN]->(b)
         WHERE NOT corp.is_npc AND p.side <> "friendly"
         RETURN
           corp.name AS corp_name,
           corp.corporation_id AS corp_id,
           count(DISTINCT other) AS enemy_count
         ORDER BY enemy_count DESC
         LIMIT 20',
        ['charId' => $characterId, 'battleId' => $battleId]
    );
}

/**
 * Character-level Neo4j intelligence summary: corp overlap count, defector status, hostile adjacency.
 */
function db_neo4j_character_intelligence(int $characterId): array
{
    // Cross-side overlap count (across all battles)
    $overlapRows = neo4j_query(
        'MATCH (c:Character {character_id: $charId})-[:CURRENT_CORP]->(corp:Corporation)<-[:CURRENT_CORP]-(other:Character)
         WHERE NOT corp.is_npc
         MATCH (other)-[:PARTICIPATED_IN {side: "hostile"}]->(:Battle)
         RETURN
           count(DISTINCT corp) AS shared_corps,
           count(DISTINCT other) AS enemy_overlap_count',
        ['charId' => $characterId]
    );

    // Recent defector status (left any corp that has friendly members in last 90d)
    $defectorRows = neo4j_query(
        'MATCH (c:Character {character_id: $charId})-[m:MEMBER_OF]->(corp:Corporation)
         WHERE m.to IS NOT NULL
           AND NOT corp.is_npc
           AND duration.inDays(m.to, datetime()).days < 90
         MATCH (friendly:Character)-[:PARTICIPATED_IN {side: "friendly"}]->(:Battle)
         MATCH (friendly)-[:MEMBER_OF]->(corp)
         RETURN
           corp.name AS corp_name,
           duration.inDays(m.to, datetime()).days AS days_ago
         ORDER BY days_ago ASC
         LIMIT 1',
        ['charId' => $characterId]
    );

    // Hostile corp adjacency
    $adjacencyRows = neo4j_query(
        'MATCH (c:Character {character_id: $charId})-[:CURRENT_CORP]->(corp:Corporation)<-[:CURRENT_CORP]-(other:Character)
         WHERE NOT corp.is_npc
         MATCH (other)-[:PARTICIPATED_IN {side: "hostile"}]->(:Battle)
         RETURN count(DISTINCT other) AS hostile_neighbors',
        ['charId' => $characterId]
    );

    $overlap = $overlapRows[0] ?? ['shared_corps' => 0, 'enemy_overlap_count' => 0];
    $defector = $defectorRows[0] ?? null;
    $adjacency = $adjacencyRows[0] ?? ['hostile_neighbors' => 0];

    return [
        'shared_corps' => (int) ($overlap['shared_corps'] ?? 0),
        'enemy_overlap_count' => (int) ($overlap['enemy_overlap_count'] ?? 0),
        'is_recent_defector' => $defector !== null,
        'defector_corp_name' => $defector['corp_name'] ?? null,
        'defector_days_ago' => $defector !== null ? (int) ($defector['days_ago'] ?? 0) : null,
        'hostile_neighbors' => (int) ($adjacency['hostile_neighbors'] ?? 0),
    ];
}

// ---------------------------------------------------------------------------
// Enrichment Queue — MySQL helpers (PHP side)
// ---------------------------------------------------------------------------

/**
 * Queue characters from a battle for EveWho enrichment.
 * Hostile characters get priority boost.
 */
function db_enrichment_queue_from_battle(array $participants): void
{
    if ($participants === []) {
        return;
    }

    $values = [];
    $params = [];
    foreach ($participants as $p) {
        $charId = (int) ($p['character_id'] ?? 0);
        if ($charId <= 0) {
            continue;
        }
        $score = (float) ($p['suspicion_score'] ?? 0);
        $side = (string) ($p['side'] ?? '');
        $priority = ($side === 'hostile' || $side === 'opponent') ? $score * 2 : $score;
        $values[] = '(?, "pending", ?, NOW())';
        $params[] = $charId;
        $params[] = round($priority, 4);
    }

    if ($values === []) {
        return;
    }

    db_execute(
        'INSERT INTO enrichment_queue (character_id, status, priority, queued_at)
         VALUES ' . implode(', ', $values) . '
         ON DUPLICATE KEY UPDATE
           status = IF(status = "done", status, VALUES(status)),
           priority = GREATEST(priority, VALUES(priority))',
        $params
    );
}

/**
 * Get enrichment progress for display.
 */
function db_enrichment_queue_progress(array $characterIds): array
{
    if ($characterIds === []) {
        return ['total' => 0, 'done' => 0, 'pending' => 0, 'processing' => 0, 'failed' => 0];
    }

    $placeholders = implode(',', array_fill(0, count($characterIds), '?'));
    $rows = db_select(
        "SELECT status, COUNT(*) AS cnt FROM enrichment_queue WHERE character_id IN ({$placeholders}) GROUP BY status",
        $characterIds
    );

    $result = ['total' => count($characterIds), 'done' => 0, 'pending' => 0, 'processing' => 0, 'failed' => 0];
    foreach ($rows as $row) {
        $result[(string) $row['status']] = (int) $row['cnt'];
    }
    return $result;
}

/**
 * Pipeline Observatory — aggregated counts and status for all pipeline stages.
 *
 * Returns a structured array with KPIs, per-stage metrics, and recent activity.
 * All queries are simple COUNTs or MAX() on existing tables — no new tables needed.
 */
function db_pipeline_observatory_data(): array
{
    // ── Stage 1: Collection counts ───────────────────────────────────────────
    $killmails       = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM killmail_events") ?? [])['cnt'] ?? 0;
    $attackerRecords = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM killmail_attackers") ?? [])['cnt'] ?? 0;
    $marketOrders    = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM market_orders_current") ?? [])['cnt'] ?? 0;

    // ── Stage 2: Entity resolution ───────────────────────────────────────────
    $entitiesResolved   = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM entity_metadata_cache") ?? [])['cnt'] ?? 0;
    $charactersResolved = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM entity_metadata_cache WHERE entity_type = 'character'") ?? [])['cnt'] ?? 0;
    $allianceHistories  = (int) (db_select_one("SELECT COUNT(DISTINCT character_id) AS cnt FROM character_alliance_history") ?? [])['cnt'] ?? 0;

    // Unique characters across killmails (attackers + victims)
    $uniqueCharacters = (int) (db_select_one(
        "SELECT COUNT(DISTINCT character_id) AS cnt FROM killmail_attackers WHERE character_id IS NOT NULL AND character_id > 0"
    ) ?? [])['cnt'] ?? 0;
    $entityCoverage = $uniqueCharacters > 0 ? min(100, round($charactersResolved / $uniqueCharacters * 100, 1)) : 0;

    // ── Stage 3: Graph enrichment ────────────────────────────────────────────
    $communities       = (int) (db_select_one("SELECT COUNT(DISTINCT community_id) AS cnt FROM graph_community_assignments") ?? [])['cnt'] ?? 0;
    $communityMembers  = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM graph_community_assignments") ?? [])['cnt'] ?? 0;
    $motifs            = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM graph_motif_detections") ?? [])['cnt'] ?? 0;
    $copresenceEdges   = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM character_copresence_edges") ?? [])['cnt'] ?? 0;
    $evidencePaths     = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM character_evidence_paths") ?? [])['cnt'] ?? 0;

    // ── Stage 4: Intelligence ────────────────────────────────────────────────
    $suspicionScoredV2  = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM character_suspicion_scores") ?? [])['cnt'] ?? 0;
    $suspicionScoredCI  = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM character_counterintel_scores") ?? [])['cnt'] ?? 0;
    $suspicionScored    = max($suspicionScoredV2, $suspicionScoredCI);
    $suspicionBelowThreshold = (int) (db_select_one(
        "SELECT COUNT(*) AS cnt FROM character_battle_intelligence WHERE eligible_battle_count < 5"
    ) ?? [])['cnt'] ?? 0;
    $behavioralScored = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM character_behavioral_scores") ?? [])['cnt'] ?? 0;
    $dossiers          = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM alliance_dossiers") ?? [])['cnt'] ?? 0;
    $threatCorridors   = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM threat_corridors") ?? [])['cnt'] ?? 0;

    // Unique alliances in battles for dossier coverage
    $alliancesInBattles = (int) (db_select_one(
        "SELECT COUNT(DISTINCT alliance_id) AS cnt FROM killmail_attackers WHERE alliance_id IS NOT NULL AND alliance_id > 0"
    ) ?? [])['cnt'] ?? 0;
    $dossierCoverage = $alliancesInBattles > 0 ? min(100, round($dossiers / $alliancesInBattles * 100, 1)) : 0;
    $suspicionCoverage = $uniqueCharacters > 0 ? min(100, round($suspicionScored / $uniqueCharacters * 100, 1)) : 0;

    // ── Stage 5: Analytics ───────────────────────────────────────────────────
    $snapshots = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM intelligence_snapshots") ?? [])['cnt'] ?? 0;

    // ── Sovereignty ──────────────────────────────────────────────────────────
    $sovSystems     = 0;
    $sovStructures  = 0;
    $sovCampaigns   = 0;
    $sovAlerts      = 0;
    try {
        $sovSystems    = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM sovereignty_map WHERE owner_entity_id IS NOT NULL") ?? [])['cnt'] ?? 0;
        $sovStructures = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM sovereignty_structures") ?? [])['cnt'] ?? 0;
        $sovCampaigns  = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM sovereignty_campaigns WHERE is_active = 1") ?? [])['cnt'] ?? 0;
        $sovAlerts     = (int) (db_select_one("SELECT COUNT(*) AS cnt FROM sovereignty_alerts WHERE status = 'active'") ?? [])['cnt'] ?? 0;
    } catch (Throwable) {
        // Tables may not exist yet if migration hasn't run.
    }

    // ── Job health summary from scheduler ────────────────────────────────────
    $jobStats = db_select(
        "SELECT latest_status, COUNT(*) AS cnt FROM scheduler_job_current_status GROUP BY latest_status"
    );
    $jobHealth = ['success' => 0, 'failed' => 0, 'running' => 0, 'skipped' => 0, 'unknown' => 0];
    foreach ($jobStats as $row) {
        $s = (string) $row['latest_status'];
        if (isset($jobHealth[$s])) {
            $jobHealth[$s] = (int) $row['cnt'];
        } else {
            $jobHealth['unknown'] += (int) $row['cnt'];
        }
    }
    $jobHealth['total'] = array_sum($jobHealth);

    // ── Recent job activity (last 50 completions) ────────────────────────────
    $recentRuns = db_select(
        "SELECT job_name, status, duration_ms, rows_processed, rows_written, started_at, finished_at
         FROM job_runs
         WHERE status IN ('success', 'failed', 'skipped')
         ORDER BY finished_at DESC
         LIMIT 50"
    );

    // ── Per-stage last-run timestamps ────────────────────────────────────────
    // New Python job?  Add it to the appropriate stage below.
    // Full wiring checklist: see python/orchestrator/jobs/__init__.py (11 points).
    $stageJobKeys = [
        'collection'  => ['market_hub_current_sync', 'esi_character_queue_sync', 'esi_affiliation_sync', 'esi_alliance_history_sync', 'character_killmail_sync', 'evewho_enrichment_sync', 'evewho_alliance_member_sync', 'sovereignty_campaigns_sync', 'sovereignty_structures_sync', 'sovereignty_map_sync'],
        'resolution'  => ['entity_metadata_resolve_sync', 'esi_alliance_history_sync'],
        'graph'       => ['compute_graph_sync', 'graph_community_detection_sync', 'graph_motif_detection_sync', 'graph_typed_interactions_sync', 'graph_temporal_metrics_sync', 'graph_evidence_paths_sync', 'compute_copresence_edges', 'compute_graph_sync_killmail_entities', 'compute_graph_sync_killmail_edges', 'neo4j_ml_exploration'],
        'intelligence' => ['compute_suspicion_scores_v2', 'compute_alliance_dossiers', 'compute_threat_corridors', 'compute_counterintel_pipeline', 'intelligence_pipeline', 'compute_battle_rollups', 'compute_behavioral_scoring', 'compute_sovereignty_alerts', 'compute_bloom_entry_points', 'compute_spy_feature_snapshots', 'build_spy_training_split', 'compute_identity_resolution', 'graph_spy_ring_projection', 'compute_spy_network_cases', 'compute_spy_risk_profiles', 'train_spy_shadow_model', 'score_spy_shadow_ml'],
        'analytics'   => ['dashboard_summary_sync', 'analytics_bucket_1h_sync', 'analytics_bucket_1d_sync', 'forecasting_ai_sync', 'discord_webhook_filter'],
        'maintenance' => ['log_to_issues', 'cache_expiry_cleanup_sync'],
    ];

    $allJobKeys = [];
    foreach ($stageJobKeys as $keys) {
        $allJobKeys = array_merge($allJobKeys, $keys);
    }
    $allJobKeys = array_values(array_unique($allJobKeys));

    $stageHealth = [];
    if ($allJobKeys !== []) {
        $placeholders = implode(',', array_fill(0, count($allJobKeys), '?'));
        $statusRows = db_select(
            "SELECT job_key, latest_status, last_success_at, last_failure_at
             FROM scheduler_job_current_status
             WHERE job_key IN ({$placeholders})",
            $allJobKeys
        );
        $statusByKey = [];
        foreach ($statusRows as $row) {
            $statusByKey[(string) $row['job_key']] = $row;
        }

        foreach ($stageJobKeys as $stage => $keys) {
            $succeeded = 0;
            $failed = 0;
            $lastSuccess = null;
            foreach ($keys as $k) {
                $info = $statusByKey[$k] ?? null;
                if ($info === null) continue;
                if ($info['latest_status'] === 'success') $succeeded++;
                if ($info['latest_status'] === 'failed') $failed++;
                if ($info['last_success_at'] !== null) {
                    if ($lastSuccess === null || $info['last_success_at'] > $lastSuccess) {
                        $lastSuccess = $info['last_success_at'];
                    }
                }
            }
            $total = count($keys);
            $stageHealth[$stage] = [
                'total_jobs'    => $total,
                'succeeded'     => $succeeded,
                'failed'        => $failed,
                'pct'           => $total > 0 ? round($succeeded / $total * 100) : 0,
                'last_success'  => $lastSuccess,
            ];
        }
    }

    return [
        'kpis' => [
            'killmails'         => $killmails,
            'attacker_records'  => $attackerRecords,
            'market_orders'     => $marketOrders,
            'unique_characters' => $uniqueCharacters,
            'entities_resolved' => $entitiesResolved,
            'characters_resolved' => $charactersResolved,
            'entity_coverage'   => $entityCoverage,
            'alliance_histories' => $allianceHistories,
            'communities'       => $communities,
            'community_members' => $communityMembers,
            'motifs'            => $motifs,
            'copresence_edges'  => $copresenceEdges,
            'evidence_paths'    => $evidencePaths,
            'suspicion_scored'  => $suspicionScored,
            'suspicion_coverage' => $suspicionCoverage,
            'suspicion_below_threshold' => $suspicionBelowThreshold,
            'behavioral_scored' => $behavioralScored,
            'dossiers'          => $dossiers,
            'dossier_coverage'  => $dossierCoverage,
            'alliances_in_battles' => $alliancesInBattles,
            'threat_corridors'  => $threatCorridors,
            'snapshots'         => $snapshots,
            'sov_systems'       => $sovSystems,
            'sov_structures'    => $sovStructures,
            'sov_campaigns'     => $sovCampaigns,
            'sov_alerts'        => $sovAlerts,
        ],
        'job_health'   => $jobHealth,
        'stage_health' => $stageHealth,
        'recent_runs'  => $recentRuns,
    ];
}

// ===========================================================================
// Neo4j offload: graph-native query alternatives
// ===========================================================================
// These functions try Neo4j first for queries that are inherently graph
// traversals, falling back to the existing MariaDB path when Neo4j is
// disabled or returns empty results.

/**
 * Community overview with Neo4j-first path.
 *
 * The MariaDB version does a heavy correlated subquery through
 * graph_community_assignments + battle_participants + entity_metadata_cache.
 * Neo4j can answer "communities with members who participated in battles for
 * these alliances" natively via label-filtered traversal.
 */
function db_graph_community_overview_neo4j(int $limit = 30): array
{
    if (!(bool) config('neo4j.enabled', false)) {
        return [];
    }

    $trackedAllianceIds = array_map('intval', array_column(db_killmail_tracked_alliances_active(), 'alliance_id'));
    if ($trackedAllianceIds === []) {
        return [];
    }

    $safeLimit = max(1, min(100, $limit));

    $rows = neo4j_query(
        'MATCH (c:Character)-[:PARTICIPATED_IN]->(b:Battle)
         WHERE c.alliance_id IN $allianceIds AND c.community_id IS NOT NULL
         WITH c.community_id AS community_id, collect(DISTINCT c.character_id) AS member_ids
         WHERE size(member_ids) >= 3
         UNWIND member_ids AS mid
         MATCH (m:Character {character_id: mid})
         WITH community_id,
              count(m) AS member_count,
              sum(CASE WHEN m.betweenness_approx > 0.1 THEN 1 ELSE 0 END) AS bridge_count,
              avg(COALESCE(m.pr, 0.0)) AS avg_pagerank,
              max(COALESCE(m.pr, 0.0)) AS max_pagerank,
              avg(COALESCE(m.betweenness_approx, 0.0)) AS avg_betweenness,
              head(collect(m.name)[..1]) AS top_member_name
         RETURN community_id, member_count, bridge_count, avg_pagerank,
                max_pagerank, avg_betweenness,
                COALESCE(top_member_name, "Unknown") AS top_member_name
         ORDER BY member_count DESC
         LIMIT $lim',
        ['allianceIds' => $trackedAllianceIds, 'lim' => $safeLimit]
    );

    if ($rows !== []) {
        return array_map(static function (array $row): array {
            return [
                'community_id'     => (string) ($row['community_id'] ?? ''),
                'community_size'   => (int) ($row['member_count'] ?? 0),
                'member_count'     => (int) ($row['member_count'] ?? 0),
                'bridge_count'     => (int) ($row['bridge_count'] ?? 0),
                'avg_pagerank'     => (float) ($row['avg_pagerank'] ?? 0.0),
                'max_pagerank'     => (float) ($row['max_pagerank'] ?? 0.0),
                'avg_betweenness'  => (float) ($row['avg_betweenness'] ?? 0.0),
                'top_member_name'  => (string) ($row['top_member_name'] ?? 'Unknown'),
            ];
        }, $rows);
    }

    return [];
}

/**
 * Constellation graph via Neo4j universe topology.
 *
 * The MariaDB version requires two queries (ref_systems + ref_stargates self-join).
 * Neo4j traverses CONNECTS_TO relationships natively with a single Cypher query.
 */
function db_constellation_graph_neo4j(array $constellationIds): array
{
    if (!(bool) config('neo4j.enabled', false)) {
        return ['nodes' => [], 'edges' => []];
    }

    $constellationIds = array_values(array_unique(array_filter(array_map('intval', $constellationIds), static fn(int $id): bool => $id > 0)));
    if ($constellationIds === []) {
        return ['nodes' => [], 'edges' => []];
    }

    $rows = neo4j_query(
        'MATCH (s:System)-[:IN_CONSTELLATION]->(c:Constellation)
         WHERE c.constellation_id IN $constellationIds
         WITH collect(s) AS systems, collect(s.system_id) AS sysIds
         UNWIND systems AS s
         OPTIONAL MATCH (s)-[:CONNECTS_TO]-(other:System)
         WHERE other.system_id IN sysIds
         RETURN s.system_id AS system_id, s.name AS system_name,
                s.security AS security, s.constellation_id AS constellation_id,
                collect(DISTINCT other.system_id) AS connected_to',
        ['constellationIds' => $constellationIds]
    );

    if ($rows === []) {
        return ['nodes' => [], 'edges' => []];
    }

    $nodes = [];
    $edgePairs = [];
    foreach ($rows as $row) {
        $sid = (int) ($row['system_id'] ?? 0);
        if ($sid <= 0) {
            continue;
        }
        $nodes[] = [
            'system_id'        => $sid,
            'system_name'      => (string) ($row['system_name'] ?? ''),
            'security'         => (float) ($row['security'] ?? 0.0),
            'constellation_id' => (int) ($row['constellation_id'] ?? 0),
        ];
        foreach ((array) ($row['connected_to'] ?? []) as $destId) {
            $destId = (int) $destId;
            if ($destId > 0 && $destId !== $sid) {
                $left = min($sid, $destId);
                $right = max($sid, $destId);
                $edgePairs[$left . ':' . $right] = [$left, $right];
            }
        }
    }

    return [
        'nodes' => $nodes,
        'edges' => array_values($edgePairs),
    ];
}

/**
 * Co-presence top edges via Neo4j.
 *
 * MariaDB must scan both character_id_a and character_id_b columns with OR
 * conditions. Neo4j traverses CO_OCCURS_WITH edges bidirectionally natively.
 */
function db_character_copresence_top_edges_neo4j(int $characterId, string $windowLabel = '30d', int $limit = 15): array
{
    if ($characterId <= 0 || !(bool) config('neo4j.enabled', false)) {
        return [];
    }

    $safeLimit = max(1, min(50, $limit));

    return neo4j_query(
        'MATCH (c:Character {character_id: $charId})-[r:CO_OCCURS_WITH]-(other:Character)
         WHERE r.recent_weight IS NOT NULL AND r.occurrence_count >= 3
         RETURN other.character_id AS other_character_id,
                COALESCE(other.name, "Character #" + toString(other.character_id)) AS other_character_name,
                r.occurrence_count AS event_count,
                CASE $window
                    WHEN "7d" THEN COALESCE(r.recent_weight, 0.0)
                    WHEN "90d" THEN COALESCE(r.all_time_weight, 0.0)
                    ELSE COALESCE(r.weight, 0.0)
                END AS edge_weight,
                r.last_seen AS last_event_at,
                other.alliance_id AS other_alliance_id
         ORDER BY edge_weight DESC
         LIMIT $lim',
        ['charId' => $characterId, 'window' => $windowLabel, 'lim' => $safeLimit]
    );
}

/**
 * Item dependency/criticality via Neo4j doctrine→fit→item graph.
 *
 * The MariaDB version uses a UNION + NOT IN subquery across two tables.
 * Neo4j can traverse Doctrine-[:USES]->Fit-[:CONTAINS]->Item natively
 * and compute dependency/criticality in a single traversal.
 */
function db_item_graph_intelligence_by_type_ids_neo4j(array $typeIds): array
{
    if (!(bool) config('neo4j.enabled', false)) {
        return [];
    }

    $typeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $id): bool => $id > 0)));
    if ($typeIds === []) {
        return [];
    }

    $rows = neo4j_query(
        'UNWIND $typeIds AS tid
         MATCH (i:Item {type_id: tid})
         OPTIONAL MATCH (f:Fit)-[:CONTAINS]->(i)
         OPTIONAL MATCH (d:Doctrine)-[:USES]->(f)
         OPTIONAL MATCH (f)-[cr:USES_CRITICAL_ITEM]->(i)
         WITH i.type_id AS type_id,
              count(DISTINCT d) AS doctrine_count,
              count(DISTINCT f) AS fit_count,
              COALESCE(max(cr.criticality_score), 0.0) AS criticality_score,
              count(DISTINCT f) * 1.0 / (CASE WHEN count(DISTINCT d) > 0 THEN count(DISTINCT d) ELSE 1 END) AS dependency_score
         RETURN type_id, dependency_score, doctrine_count AS graph_doctrine_count,
                fit_count AS graph_fit_count, criticality_score
         ORDER BY type_id',
        ['typeIds' => $typeIds]
    );

    if ($rows === []) {
        return [];
    }

    $indexed = [];
    foreach ($rows as $row) {
        $indexed[(int) $row['type_id']] = $row;
    }
    return $indexed;
}

/**
 * Alliance relationship graph via Neo4j.
 *
 * Returns ALLIED_WITH and HOSTILE_TO edges for a given set of alliance IDs.
 * This is a pure graph query that MariaDB can only serve from materialized
 * read-model tables, while Neo4j has the edges as first-class relationships.
 */
function db_alliance_relationship_graph_neo4j(array $allianceIds, int $limit = 50): array
{
    if (!(bool) config('neo4j.enabled', false)) {
        return ['allies' => [], 'hostiles' => []];
    }

    $allianceIds = array_values(array_unique(array_filter(array_map('intval', $allianceIds), static fn(int $id): bool => $id > 0)));
    if ($allianceIds === []) {
        return ['allies' => [], 'hostiles' => []];
    }

    $safeLimit = max(1, min(200, $limit));

    $allies = neo4j_query(
        'MATCH (a:Alliance)-[r:ALLIED_WITH]-(b:Alliance)
         WHERE a.alliance_id IN $ids
         RETURN a.alliance_id AS source_id, COALESCE(a.name, "") AS source_name,
                b.alliance_id AS target_id, COALESCE(b.name, "") AS target_name,
                r.weight_30d AS weight, r.shared_killmails AS shared_killmails,
                r.computed_at AS computed_at
         ORDER BY r.weight_30d DESC
         LIMIT $lim',
        ['ids' => $allianceIds, 'lim' => $safeLimit]
    );

    $hostiles = neo4j_query(
        'MATCH (a:Alliance)-[r:HOSTILE_TO]-(b:Alliance)
         WHERE a.alliance_id IN $ids
         RETURN a.alliance_id AS source_id, COALESCE(a.name, "") AS source_name,
                b.alliance_id AS target_id, COALESCE(b.name, "") AS target_name,
                r.weight_30d AS weight, r.engagements AS engagements,
                r.computed_at AS computed_at
         ORDER BY r.weight_30d DESC
         LIMIT $lim',
        ['ids' => $allianceIds, 'lim' => $safeLimit]
    );

    return [
        'allies'   => $allies ?: [],
        'hostiles' => $hostiles ?: [],
    ];
}

/**
 * Bloom Operational Intelligence — read-side projection for the dashboard.
 *
 * Reads pre-ranked rows from `bloom_entry_points_materialized`, which is
 * maintained by the `compute_bloom_entry_points` job. The job writes the
 * same top-N that the Neo4j tier labels (:HotBattle, :HighRiskPilot,
 * :StrategicSystem, :HotAlliance) represent, so the PHP dashboard can
 * render the four tiers with one cheap indexed SQL read per panel — no
 * Neo4j HTTP round-trip on the request path.
 *
 * The `detail_json` column is decoded into `detail` for the caller; keys
 * inside it are tier-specific (e.g. started_at/participant_count for
 * HotBattle, region_name for StrategicSystem).
 */
function db_bloom_entry_points_by_tier(string $tier, int $limit = 10): array
{
    $tier = trim($tier);
    if ($tier === '') {
        return [];
    }

    $safeLimit = max(1, min(50, $limit));

    $rows = db_select(
        'SELECT tier, rank_in_tier, entity_ref_type, entity_ref_id,
                entity_name, score, detail_json, refreshed_at
           FROM bloom_entry_points_materialized
          WHERE tier = :tier
          ORDER BY rank_in_tier ASC
          LIMIT ' . (int) $safeLimit,
        ['tier' => $tier]
    );

    foreach ($rows as &$row) {
        $detail = [];
        if (!empty($row['detail_json']) && is_string($row['detail_json'])) {
            $decoded = json_decode($row['detail_json'], true);
            if (is_array($decoded)) {
                $detail = $decoded;
            }
        }
        $row['detail'] = $detail;
        unset($row['detail_json']);
    }
    unset($row);

    // Enrich entity names from entity_metadata_cache for ref types that have
    // canonical names there (alliance, character, corporation, system, type,
    // region). The compute_bloom_entry_points job materializes names as read
    // from Neo4j, which can be empty or out of date — resulting in placeholder
    // labels like "Alliance 99003581" rendering on the dashboard. The cache is
    // the authoritative source for display names, so we prefer it whenever we
    // have a resolved row.
    $idsByType = [];
    foreach ($rows as $i => $row) {
        $refType = (string) ($row['entity_ref_type'] ?? '');
        $refId = (int) ($row['entity_ref_id'] ?? 0);
        if ($refId <= 0) {
            continue;
        }
        $cacheType = match ($refType) {
            'alliance_id' => 'alliance',
            'character_id' => 'character',
            'corporation_id' => 'corporation',
            'system_id' => 'system',
            'type_id' => 'type',
            'region_id' => 'region',
            default => null,
        };
        if ($cacheType === null) {
            continue;
        }
        $idsByType[$cacheType][$refId][] = $i;
    }

    foreach ($idsByType as $cacheType => $idIndex) {
        $ids = array_keys($idIndex);
        if ($ids === []) {
            continue;
        }
        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        try {
            $cached = db_select(
                'SELECT entity_id, entity_name
                   FROM entity_metadata_cache
                  WHERE entity_type = ?
                    AND entity_id IN (' . $placeholders . ')',
                array_merge([$cacheType], array_map('intval', $ids))
            );
        } catch (Throwable) {
            continue;
        }
        foreach ($cached as $cacheRow) {
            $name = trim((string) ($cacheRow['entity_name'] ?? ''));
            if ($name === '') {
                continue;
            }
            $cid = (int) ($cacheRow['entity_id'] ?? 0);
            if (!isset($idIndex[$cid])) {
                continue;
            }
            foreach ($idIndex[$cid] as $rowIdx) {
                $rows[$rowIdx]['entity_name'] = $name;
            }
        }
    }

    return $rows;
}

// ===========================================================================
// Wrappers: Neo4j-preferred with MariaDB fallback
// ===========================================================================

/**
 * Community overview: tries Neo4j first, falls back to MariaDB.
 */
function db_graph_community_overview_preferred(int $limit = 30): array
{
    $neo4jRows = db_graph_community_overview_neo4j($limit);
    if ($neo4jRows !== []) {
        return $neo4jRows;
    }
    return db_graph_community_overview($limit);
}

/**
 * Constellation graph: tries Neo4j first, falls back to MariaDB.
 */
function db_constellation_graph_preferred(array $constellationIds): array
{
    $neo4jResult = db_constellation_graph_neo4j($constellationIds);
    if ($neo4jResult['nodes'] !== []) {
        return $neo4jResult;
    }
    return db_constellation_graph($constellationIds);
}

/**
 * Co-presence top edges: tries Neo4j first, falls back to MariaDB.
 */
function db_character_copresence_top_edges_preferred(int $characterId, string $windowLabel = '30d', int $limit = 15): array
{
    $neo4jRows = db_character_copresence_top_edges_neo4j($characterId, $windowLabel, $limit);
    if ($neo4jRows !== []) {
        return $neo4jRows;
    }
    return db_character_copresence_top_edges($characterId, $windowLabel, $limit);
}

/**
 * Item graph intelligence: tries Neo4j first, falls back to MariaDB.
 */
function db_item_graph_intelligence_by_type_ids_preferred(array $typeIds): array
{
    $neo4jResult = db_item_graph_intelligence_by_type_ids_neo4j($typeIds);
    if ($neo4jResult !== []) {
        return $neo4jResult;
    }
    return db_item_graph_intelligence_by_type_ids($typeIds);
}

// ===========================================================================
// InfluxDB offload: time-series query alternatives
// ===========================================================================
// These functions extend the existing InfluxDB read-path to cover additional
// time-series queries that currently run entirely on MariaDB.

/**
 * Killmail hull loss window summaries via InfluxDB.
 *
 * Replaces the MariaDB GROUP BY DATE(effective_killmail_at), victim_ship_type_id
 * pattern with a native InfluxDB aggregateWindow query.
 */
function db_killmail_hull_loss_window_summaries_influx(array $hullTypeIds, int $hours = 24 * 7): array
{
    if (!db_influx_read_enabled()) {
        return [];
    }

    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $hullTypeIds), static fn (int $id): bool => $id > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $filterParts = [];
    foreach ($normalizedTypeIds as $tid) {
        $filterParts[] = 'r.hull_type_id == "' . $tid . '"';
    }
    $typeFilter = implode(' or ', $filterParts);
    $safeHours = max(24, min(24 * 30, $hours));

    $flux = 'from(bucket: "' . db_influx_bucket() . '")
  |> range(start: -' . $safeHours . 'h)
  |> filter(fn: (r) => r._measurement == "killmail_hull_loss" and r.window == "1d")
  |> filter(fn: (r) => ' . $typeFilter . ')
  |> filter(fn: (r) => r._field == "loss_count" or r._field == "victim_count" or r._field == "killmail_count")
  |> pivot(rowKey: ["_time", "hull_type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["hull_type_id"])
  |> sum(column: "loss_count")
  |> yield(name: "hull_loss_summary")';

    $rows = db_influx_query_rows($flux);
    if ($rows === []) {
        return [];
    }

    return array_map(static function (array $row): array {
        return [
            'hull_type_id'  => (int) ($row['hull_type_id'] ?? 0),
            'loss_count'    => (int) ($row['loss_count'] ?? 0),
            'victim_count'  => (int) ($row['victim_count'] ?? 0),
            'killmail_count' => (int) ($row['killmail_count'] ?? 0),
        ];
    }, $rows);
}

/**
 * Killmail item loss window summaries via InfluxDB.
 *
 * Replaces the MariaDB GROUP BY on killmail_item_loss_1d/1h with a native
 * InfluxDB query using aggregateWindow and time-range filtering.
 */
function db_killmail_item_loss_window_summaries_influx(array $typeIds, int $hours = 24 * 7): array
{
    if (!db_influx_read_enabled()) {
        return [];
    }

    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $id): bool => $id > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $filterParts = [];
    foreach ($normalizedTypeIds as $tid) {
        $filterParts[] = 'r.type_id == "' . $tid . '"';
    }
    $typeFilter = implode(' or ', $filterParts);
    $safeHours = max(24, min(24 * 30, $hours));

    $flux = 'from(bucket: "' . db_influx_bucket() . '")
  |> range(start: -' . $safeHours . 'h)
  |> filter(fn: (r) => r._measurement == "killmail_item_loss" and r.window == "1d")
  |> filter(fn: (r) => ' . $typeFilter . ')
  |> filter(fn: (r) => r._field == "loss_count" or r._field == "quantity_lost" or r._field == "victim_count" or r._field == "killmail_count")
  |> pivot(rowKey: ["_time", "type_id"], columnKey: ["_field"], valueColumn: "_value")
  |> group(columns: ["type_id"])
  |> sum(column: "quantity_lost")
  |> yield(name: "item_loss_summary")';

    $rows = db_influx_query_rows($flux);
    if ($rows === []) {
        return [];
    }

    return array_map(static function (array $row): array {
        return [
            'type_id'       => (int) ($row['type_id'] ?? 0),
            'loss_count'    => (int) ($row['loss_count'] ?? 0),
            'quantity_lost' => (int) ($row['quantity_lost'] ?? 0),
            'victim_count'  => (int) ($row['victim_count'] ?? 0),
            'killmail_count' => (int) ($row['killmail_count'] ?? 0),
        ];
    }, $rows);
}

/**
 * Market item stock window summaries with InfluxDB-first path.
 *
 * Extends db_market_item_stock_window_summaries() to try InfluxDB first when
 * read_mode is 'preferred' or 'primary', reducing MariaDB load for stock
 * trend pages.
 */
function db_market_item_stock_window_summaries_preferred(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    $readMode = db_influx_read_mode();

    if ($typeIds === [] && $readMode === 'primary') {
        $influxRows = db_market_item_stock_window_summaries_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

    if ($typeIds === [] && $readMode === 'preferred') {
        $influxRows = db_market_item_stock_window_summaries_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

    $rows = db_market_item_stock_window_summaries($sourceType, $sourceId, $startDate, $endDate, $typeIds);

    if ($rows === [] && $typeIds === [] && $readMode === 'fallback') {
        $influxRows = db_market_item_stock_window_summaries_influx($sourceType, $sourceId, $startDate, $endDate);
        if ($influxRows !== []) {
            return $influxRows;
        }
    }

    return $rows;
}

/**
 * Return the configured InfluxDB bucket name.
 */
function db_influx_bucket(): string
{
    return (string) config('influxdb.bucket', 'supplycore_rollups');
}

// ────────────────────────────────────────────────────────────────────────────
// Opposition Daily Intelligence
// ────────────────────────────────────────────────────────────────────────────

/**
 * Fetch all opposition daily snapshots for a given date.
 */
function db_opposition_daily_snapshots(string $date): array
{
    $pdo = db();
    $stmt = $pdo->prepare(
        "SELECT * FROM opposition_daily_snapshots WHERE snapshot_date = :d ORDER BY kills DESC, isk_destroyed DESC"
    );
    $stmt->execute(['d' => $date]);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    foreach ($rows as &$row) {
        foreach (['active_systems_json', 'active_regions_json', 'ship_classes_json', 'ship_types_json',
                  'allies_json', 'enemies_json', 'theaters_json', 'notable_kills_json',
                  'threat_corridors_json', 'trend_summary_json'] as $col) {
            if (isset($row[$col]) && is_string($row[$col])) {
                $row[$col] = json_decode($row[$col], true);
            }
        }
    }
    return $rows;
}

/**
 * Fetch opposition daily snapshots for a specific alliance over N days.
 */
function db_opposition_daily_snapshot_alliance(int $allianceId, string $endDate, int $days = 7): array
{
    $pdo = db();
    $stmt = $pdo->prepare(
        "SELECT * FROM opposition_daily_snapshots
         WHERE alliance_id = :aid AND snapshot_date BETWEEN DATE_SUB(:d, INTERVAL :days DAY) AND :d2
         ORDER BY snapshot_date DESC"
    );
    $stmt->execute(['aid' => $allianceId, 'd' => $endDate, 'days' => $days, 'd2' => $endDate]);
    $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
    foreach ($rows as &$row) {
        foreach (['active_systems_json', 'active_regions_json', 'ship_classes_json', 'ship_types_json',
                  'allies_json', 'enemies_json', 'theaters_json', 'notable_kills_json',
                  'threat_corridors_json', 'trend_summary_json'] as $col) {
            if (isset($row[$col]) && is_string($row[$col])) {
                $row[$col] = json_decode($row[$col], true);
            }
        }
    }
    return $rows;
}

/**
 * Fetch an opposition daily briefing.
 */
function db_opposition_daily_briefing(string $date, string $type = 'global', ?int $allianceId = null): ?array
{
    $pdo = db();
    if ($type === 'global') {
        $stmt = $pdo->prepare(
            "SELECT * FROM opposition_daily_briefings WHERE briefing_date = :d AND briefing_type = 'global' LIMIT 1"
        );
        $stmt->execute(['d' => $date]);
    } else {
        $stmt = $pdo->prepare(
            "SELECT * FROM opposition_daily_briefings WHERE briefing_date = :d AND briefing_type = 'alliance' AND alliance_id = :aid LIMIT 1"
        );
        $stmt->execute(['d' => $date, 'aid' => $allianceId]);
    }
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row ?: null;
}

/**
 * Fetch recent opposition daily briefings (global).
 */
function db_opposition_daily_briefings_recent(int $days = 14): array
{
    $pdo = db();
    // Return only the latest briefing per date (dedup multiple runs per day)
    $stmt = $pdo->prepare(
        "SELECT b.*
         FROM opposition_daily_briefings b
         INNER JOIN (
             SELECT briefing_date, MAX(id) AS max_id
             FROM opposition_daily_briefings
             WHERE briefing_type = 'global' AND briefing_date >= DATE_SUB(CURDATE(), INTERVAL :days1 DAY)
             GROUP BY briefing_date
         ) latest ON b.id = latest.max_id
         ORDER BY b.briefing_date DESC"
    );
    $stmt->execute(['days1' => $days]);
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

/**
 * Fetch per-alliance briefings for a specific date.
 */
function db_opposition_alliance_briefings(string $date): array
{
    $pdo = db();
    $stmt = $pdo->prepare(
        "SELECT * FROM opposition_daily_briefings
         WHERE briefing_date = :d AND briefing_type = 'alliance'
         ORDER BY FIELD(threat_assessment, 'critical', 'high', 'elevated', 'moderate', 'low'), alliance_name ASC"
    );
    $stmt->execute(['d' => $date]);
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

/**
 * Fetch per-alliance briefings history for a specific alliance.
 */
function db_opposition_alliance_briefings_history(int $allianceId, int $days = 7): array
{
    $pdo = db();
    $stmt = $pdo->prepare(
        "SELECT * FROM opposition_daily_briefings
         WHERE briefing_type = 'alliance' AND alliance_id = :aid
           AND briefing_date >= DATE_SUB(CURDATE(), INTERVAL :days DAY)
         ORDER BY briefing_date DESC"
    );
    $stmt->execute(['aid' => $allianceId, 'days' => $days]);
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

/**
 * Save an opposition daily briefing.
 */
function db_opposition_daily_briefing_save(array $briefing): void
{
    $pdo = db();
    $stmt = $pdo->prepare(
        "INSERT INTO opposition_daily_briefings (
            briefing_date, briefing_type, alliance_id, alliance_name,
            generation_status, model_name,
            headline, summary, key_developments, threat_assessment, action_items,
            source_payload_json, response_json, error_message, computed_at
        ) VALUES (
            :briefing_date, :briefing_type, :alliance_id, :alliance_name,
            :generation_status, :model_name,
            :headline, :summary, :key_developments, :threat_assessment, :action_items,
            :source_payload_json, :response_json, :error_message, :computed_at
        ) ON DUPLICATE KEY UPDATE
            generation_status = VALUES(generation_status), model_name = VALUES(model_name),
            headline = VALUES(headline), summary = VALUES(summary),
            key_developments = VALUES(key_developments), threat_assessment = VALUES(threat_assessment),
            action_items = VALUES(action_items),
            source_payload_json = VALUES(source_payload_json), response_json = VALUES(response_json),
            error_message = VALUES(error_message), computed_at = VALUES(computed_at)"
    );
    $stmt->execute([
        'briefing_date' => $briefing['briefing_date'],
        'briefing_type' => $briefing['briefing_type'],
        'alliance_id' => $briefing['alliance_id'] ?? null,
        'alliance_name' => $briefing['alliance_name'] ?? null,
        'generation_status' => $briefing['generation_status'] ?? 'ready',
        'model_name' => $briefing['model_name'] ?? null,
        'headline' => $briefing['headline'] ?? null,
        'summary' => $briefing['summary'] ?? null,
        'key_developments' => $briefing['key_developments'] ?? null,
        'threat_assessment' => $briefing['threat_assessment'] ?? null,
        'action_items' => $briefing['action_items'] ?? null,
        'source_payload_json' => $briefing['source_payload_json'] ?? null,
        'response_json' => $briefing['response_json'] ?? null,
        'error_message' => $briefing['error_message'] ?? null,
        'computed_at' => $briefing['computed_at'] ?? gmdate('Y-m-d H:i:s'),
    ]);
}

/**
 * Check if an alliance is tracked for opposition intel.
 */
function db_opposition_intel_is_tracked(int $allianceId): bool
{
    $pdo = db();
    $stmt = $pdo->prepare("SELECT 1 FROM opposition_intel_tracked WHERE alliance_id = :aid");
    $stmt->execute(['aid' => $allianceId]);
    return (bool) $stmt->fetchColumn();
}

/**
 * Toggle opposition intel tracking for an alliance.
 */
function db_opposition_intel_toggle_track(int $allianceId): bool
{
    $pdo = db();
    if (db_opposition_intel_is_tracked($allianceId)) {
        $stmt = $pdo->prepare("DELETE FROM opposition_intel_tracked WHERE alliance_id = :aid");
        $stmt->execute(['aid' => $allianceId]);
        return false;
    }
    $stmt = $pdo->prepare(
        "INSERT INTO opposition_intel_tracked (alliance_id, tracked_at) VALUES (:aid, UTC_TIMESTAMP())
         ON DUPLICATE KEY UPDATE tracked_at = UTC_TIMESTAMP()"
    );
    $stmt->execute(['aid' => $allianceId]);
    return true;
}

/**
 * Get all alliances tracked for opposition intel.
 */
function db_opposition_intel_tracked_alliances(): array
{
    $pdo = db();
    $stmt = $pdo->query(
        "SELECT oit.alliance_id, oit.tracked_at,
                COALESCE(emc.entity_name, CONCAT('Alliance #', oit.alliance_id)) AS alliance_name
         FROM opposition_intel_tracked oit
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = oit.alliance_id
         ORDER BY oit.tracked_at ASC"
    );
    return $stmt->fetchAll(PDO::FETCH_ASSOC);
}

/**
 * Get the latest snapshot date with data.
 */
function db_opposition_latest_snapshot_date(): ?string
{
    $pdo = db();
    $stmt = $pdo->query("SELECT MAX(snapshot_date) AS d FROM opposition_daily_snapshots");
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    return $row['d'] ?? null;
}

// ── Sovereignty Monitoring ──────────────────────────────────────────────────

function db_sovereignty_alerts_active(int $limit = 50): array
{
    return db_select(
        "SELECT sa.*,
                rs.system_name,
                rs.security AS system_security,
                rr.region_name,
                emc.entity_name AS alliance_name
         FROM sovereignty_alerts sa
         LEFT JOIN ref_systems rs ON rs.system_id = sa.solar_system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = sa.alliance_id
         WHERE sa.status = 'active'
         ORDER BY FIELD(sa.severity, 'critical', 'warning', 'info'),
                  sa.detected_at DESC
         LIMIT ?",
        [max(1, min(200, $limit))]
    );
}

function db_sovereignty_alerts_count(): int
{
    $rows = db_select(
        "SELECT COUNT(*) AS cnt FROM sovereignty_alerts
         WHERE status = 'active' AND severity IN ('critical', 'warning')"
    );
    return (int) ($rows[0]['cnt'] ?? 0);
}

function db_sovereignty_campaigns_active(?string $standingFilter = null): array
{
    $where = 'sc.is_active = 1';
    $params = [];

    if ($standingFilter === 'friendly') {
        $standings = db_corp_contacts_by_standing();
        $ids = $standings['friendly_alliance_ids'];
        if ($ids) {
            $ph = implode(',', array_fill(0, count($ids), '?'));
            $where .= " AND sc.defender_id IN ({$ph})";
            $params = array_merge($params, $ids);
        } else {
            $where .= ' AND 0';
        }
    }

    return db_select(
        "SELECT sc.*,
                rs.system_name,
                rs.security AS system_security,
                rr.region_name,
                rc.constellation_name,
                emc.entity_name AS defender_name,
                rit.type_name AS structure_type_name,
                rsr.structure_role,
                rsr.is_sov_structure,
                cc.standing AS defender_standing,
                CASE
                    WHEN sc.event_type LIKE '%defense%' THEN 'Sovereignty Defense'
                    WHEN rsr.structure_role IN ('sov_hub','legacy_ihub','legacy_tcu') THEN 'Sovereignty Capture'
                    WHEN sc.event_type LIKE '%freeport%' THEN 'Freeport'
                    WHEN sc.event_type LIKE '%station%' THEN 'Station Contest'
                    ELSE 'Unknown'
                END AS campaign_type_normalized
         FROM sovereignty_campaigns sc
         LEFT JOIN ref_systems rs ON rs.system_id = sc.solar_system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN ref_constellations rc ON rc.constellation_id = sc.constellation_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = sc.defender_id
         LEFT JOIN sovereignty_structures sst ON sst.structure_id = sc.structure_id
         LEFT JOIN ref_item_types rit ON rit.type_id = sst.structure_type_id
         LEFT JOIN ref_sov_structure_roles rsr ON rsr.structure_type_id = sst.structure_type_id
         LEFT JOIN corp_contacts cc
              ON cc.contact_type = 'alliance' AND cc.contact_id = sc.defender_id
         WHERE {$where}
         ORDER BY CASE WHEN cc.standing > 0 THEN 0 WHEN cc.standing < 0 THEN 2 ELSE 1 END,
                  sc.start_time ASC",
        $params
    );
}

function db_sovereignty_campaigns_history(int $limit = 50, int $offset = 0): array
{
    return db_select(
        "SELECT sch.*,
                rs.system_name,
                rs.security AS system_security,
                rr.region_name,
                emc.entity_name AS defender_name,
                cc.standing AS defender_standing,
                CASE
                    WHEN sch.event_type LIKE '%defense%' THEN 'Sovereignty Defense'
                    WHEN sch.event_type LIKE '%freeport%' THEN 'Freeport'
                    WHEN sch.event_type LIKE '%station%' THEN 'Station Contest'
                    ELSE 'Sovereignty Contest'
                END AS campaign_type_normalized
         FROM sovereignty_campaigns_history sch
         LEFT JOIN ref_systems rs ON rs.system_id = sch.solar_system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = sch.defender_id
         LEFT JOIN corp_contacts cc
              ON cc.contact_type = 'alliance' AND cc.contact_id = sch.defender_id
         ORDER BY sch.ended_at DESC
         LIMIT ? OFFSET ?",
        [max(1, min(200, $limit)), max(0, $offset)]
    );
}

function db_sovereignty_map_list(int $limit = 50, int $offset = 0, ?string $search = null, ?string $filter = null): array
{
    $where = ['1=1'];
    $params = [];

    if ($search !== null && trim($search) !== '') {
        $term = '%' . trim($search) . '%';
        $where[] = '(rs.system_name LIKE ? OR rr.region_name LIKE ? OR emc.entity_name LIKE ?)';
        $params[] = $term;
        $params[] = $term;
        $params[] = $term;
    }

    if ($filter === 'friendly') {
        $standings = db_corp_contacts_by_standing();
        $ids = $standings['friendly_alliance_ids'];
        if ($ids) {
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $where[] = "sm.alliance_id IN ({$placeholders})";
            $params = array_merge($params, $ids);
        } else {
            $where[] = '0';
        }
    } elseif ($filter === 'hostile') {
        $standings = db_corp_contacts_by_standing();
        $ids = $standings['hostile_alliance_ids'];
        if ($ids) {
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $where[] = "sm.alliance_id IN ({$placeholders})";
            $params = array_merge($params, $ids);
        } else {
            $where[] = '0';
        }
    } elseif ($filter === 'neutral') {
        $standings = db_corp_contacts_by_standing();
        $allKnown = array_merge($standings['friendly_alliance_ids'], $standings['hostile_alliance_ids']);
        if ($allKnown) {
            $placeholders = implode(',', array_fill(0, count($allKnown), '?'));
            $where[] = "(sm.alliance_id IS NULL OR sm.alliance_id NOT IN ({$placeholders}))";
            $params = array_merge($params, $allKnown);
        }
    }

    $whereClause = implode(' AND ', $where);
    $params[] = max(1, min(200, $limit));
    $params[] = max(0, $offset);

    return db_select(
        "SELECT sm.*,
                rs.system_name,
                rs.security AS system_security,
                rs.constellation_id,
                rr.region_name,
                rc.constellation_name,
                emc.entity_name AS owner_name,
                ss.structure_role,
                ss.vulnerability_occupancy_level AS adm,
                ss.vulnerable_start_time,
                ss.vulnerable_end_time,
                CASE
                    WHEN ss.vulnerability_occupancy_level IS NULL THEN NULL
                    WHEN ss.vulnerability_occupancy_level < 2.0 THEN 'critical'
                    WHEN ss.vulnerability_occupancy_level < 3.0 THEN 'weak'
                    WHEN ss.vulnerability_occupancy_level < 4.5 THEN 'stable'
                    ELSE 'strong'
                END AS adm_status,
                CASE
                    WHEN ss.vulnerable_start_time <= UTC_TIMESTAMP()
                         AND ss.vulnerable_end_time >= UTC_TIMESTAMP() THEN 1
                    ELSE 0
                END AS is_vulnerable_now,
                cc.standing AS owner_standing
         FROM sovereignty_map sm
         LEFT JOIN ref_systems rs ON rs.system_id = sm.system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = sm.owner_entity_id
         LEFT JOIN sovereignty_structures ss ON ss.solar_system_id = sm.system_id
              AND ss.structure_role IN ('sov_hub','legacy_ihub')
         LEFT JOIN corp_contacts cc
              ON cc.contact_type = 'alliance' AND cc.contact_id = sm.alliance_id
         WHERE {$whereClause}
           AND sm.owner_entity_id IS NOT NULL
         ORDER BY rr.region_name, rs.system_name
         LIMIT ? OFFSET ?",
        $params
    );
}

function db_sovereignty_map_count(?string $search = null, ?string $filter = null): int
{
    $where = ['sm.owner_entity_id IS NOT NULL'];
    $params = [];

    if ($search !== null && trim($search) !== '') {
        $term = '%' . trim($search) . '%';
        $where[] = '(rs.system_name LIKE ? OR rr.region_name LIKE ? OR emc.entity_name LIKE ?)';
        $params[] = $term;
        $params[] = $term;
        $params[] = $term;
    }

    if ($filter === 'friendly') {
        $standings = db_corp_contacts_by_standing();
        $ids = $standings['friendly_alliance_ids'];
        if ($ids) {
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $where[] = "sm.alliance_id IN ({$placeholders})";
            $params = array_merge($params, $ids);
        } else {
            $where[] = '0';
        }
    } elseif ($filter === 'hostile') {
        $standings = db_corp_contacts_by_standing();
        $ids = $standings['hostile_alliance_ids'];
        if ($ids) {
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $where[] = "sm.alliance_id IN ({$placeholders})";
            $params = array_merge($params, $ids);
        } else {
            $where[] = '0';
        }
    } elseif ($filter === 'neutral') {
        $standings = db_corp_contacts_by_standing();
        $allKnown = array_merge($standings['friendly_alliance_ids'], $standings['hostile_alliance_ids']);
        if ($allKnown) {
            $placeholders = implode(',', array_fill(0, count($allKnown), '?'));
            $where[] = "(sm.alliance_id IS NULL OR sm.alliance_id NOT IN ({$placeholders}))";
            $params = array_merge($params, $allKnown);
        }
    }

    $whereClause = implode(' AND ', $where);
    $rows = db_select(
        "SELECT COUNT(*) AS cnt
         FROM sovereignty_map sm
         LEFT JOIN ref_systems rs ON rs.system_id = sm.system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = sm.owner_entity_id
         WHERE {$whereClause}",
        $params
    );
    return (int) ($rows[0]['cnt'] ?? 0);
}

function db_sovereignty_structures_list(int $limit = 50, int $offset = 0, ?string $filter = null): array
{
    $where = ['1=1'];
    $params = [];

    if ($filter === 'friendly' || $filter === 'hostile') {
        $standings = db_corp_contacts_by_standing();
        $ids = $filter === 'friendly' ? $standings['friendly_alliance_ids'] : $standings['hostile_alliance_ids'];
        if ($ids) {
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $where[] = "ss.alliance_id IN ({$placeholders})";
            $params = array_merge($params, $ids);
        } else {
            $where[] = '0';
        }
    }

    $whereClause = implode(' AND ', $where);
    $params[] = max(1, min(200, $limit));
    $params[] = max(0, $offset);

    return db_select(
        "SELECT ss.*,
                rs.system_name,
                rs.security AS system_security,
                rr.region_name,
                emc.entity_name AS alliance_name,
                rit.type_name AS structure_type_name,
                cc.standing AS alliance_standing,
                CASE
                    WHEN ss.vulnerability_occupancy_level IS NULL THEN NULL
                    WHEN ss.vulnerability_occupancy_level < 2.0 THEN 'critical'
                    WHEN ss.vulnerability_occupancy_level < 3.0 THEN 'weak'
                    WHEN ss.vulnerability_occupancy_level < 4.5 THEN 'stable'
                    ELSE 'strong'
                END AS adm_status,
                CASE
                    WHEN ss.vulnerable_start_time <= UTC_TIMESTAMP()
                         AND ss.vulnerable_end_time >= UTC_TIMESTAMP() THEN 1
                    ELSE 0
                END AS is_vulnerable_now,
                TIMESTAMPDIFF(MINUTE, UTC_TIMESTAMP(), ss.vulnerable_start_time) AS vulnerable_in_minutes,
                TIMESTAMPDIFF(MINUTE, ss.vulnerable_start_time, ss.vulnerable_end_time) AS vulnerable_duration_minutes
         FROM sovereignty_structures ss
         LEFT JOIN ref_systems rs ON rs.system_id = ss.solar_system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = ss.alliance_id
         LEFT JOIN ref_item_types rit ON rit.type_id = ss.structure_type_id
         LEFT JOIN corp_contacts cc
              ON cc.contact_type = 'alliance' AND cc.contact_id = ss.alliance_id
         WHERE {$whereClause}
         ORDER BY rr.region_name, rs.system_name
         LIMIT ? OFFSET ?",
        $params
    );
}

function db_sovereignty_system_detail(int $systemId): array
{
    $result = ['owner' => null, 'structures' => [], 'campaigns' => [], 'map_history' => [], 'structure_history' => []];

    $ownerRows = db_select(
        "SELECT sm.*,
                rs.system_name, rs.security AS system_security,
                rs.constellation_id, rr.region_name, rc.constellation_name,
                emc.entity_name AS owner_name,
                cc.standing AS owner_standing
         FROM sovereignty_map sm
         LEFT JOIN ref_systems rs ON rs.system_id = sm.system_id
         LEFT JOIN ref_regions rr ON rr.region_id = rs.region_id
         LEFT JOIN ref_constellations rc ON rc.constellation_id = rs.constellation_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = sm.owner_entity_id
         LEFT JOIN corp_contacts cc
              ON cc.contact_type = 'alliance' AND cc.contact_id = sm.alliance_id
         WHERE sm.system_id = ?
         LIMIT 1",
        [$systemId]
    );
    $result['owner'] = $ownerRows[0] ?? null;

    $result['structures'] = db_select(
        "SELECT ss.*,
                emc.entity_name AS alliance_name,
                rit.type_name AS structure_type_name,
                CASE
                    WHEN ss.vulnerability_occupancy_level IS NULL THEN NULL
                    WHEN ss.vulnerability_occupancy_level < 2.0 THEN 'critical'
                    WHEN ss.vulnerability_occupancy_level < 3.0 THEN 'weak'
                    WHEN ss.vulnerability_occupancy_level < 4.5 THEN 'stable'
                    ELSE 'strong'
                END AS adm_status,
                CASE
                    WHEN ss.vulnerable_start_time <= UTC_TIMESTAMP()
                         AND ss.vulnerable_end_time >= UTC_TIMESTAMP() THEN 1
                    ELSE 0
                END AS is_vulnerable_now,
                TIMESTAMPDIFF(MINUTE, UTC_TIMESTAMP(), ss.vulnerable_start_time) AS vulnerable_in_minutes,
                TIMESTAMPDIFF(MINUTE, ss.vulnerable_start_time, ss.vulnerable_end_time) AS vulnerable_duration_minutes
         FROM sovereignty_structures ss
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = ss.alliance_id
         LEFT JOIN ref_item_types rit ON rit.type_id = ss.structure_type_id
         WHERE ss.solar_system_id = ?
         ORDER BY FIELD(ss.structure_role, 'sov_hub', 'legacy_ihub', 'legacy_tcu', 'other', 'unknown')",
        [$systemId]
    );

    $result['campaigns'] = db_select(
        "SELECT sc.*,
                emc.entity_name AS defender_name,
                cc.standing AS defender_standing
         FROM sovereignty_campaigns sc
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = sc.defender_id
         LEFT JOIN corp_contacts cc
              ON cc.contact_type = 'alliance' AND cc.contact_id = sc.defender_id
         WHERE sc.solar_system_id = ?
           AND sc.is_active = 1",
        [$systemId]
    );

    $result['map_history'] = db_select(
        "SELECT smh.*,
                emc_prev.entity_name AS previous_owner_name,
                emc_new.entity_name AS new_owner_name
         FROM sovereignty_map_history smh
         LEFT JOIN entity_metadata_cache emc_prev
              ON emc_prev.entity_type = 'alliance' AND emc_prev.entity_id = smh.previous_owner_entity_id
         LEFT JOIN entity_metadata_cache emc_new
              ON emc_new.entity_type = 'alliance' AND emc_new.entity_id = smh.new_owner_entity_id
         WHERE smh.system_id = ?
         ORDER BY smh.changed_at DESC
         LIMIT 100",
        [$systemId]
    );

    $result['structure_history'] = db_select(
        "SELECT ssh.*,
                rit.type_name AS structure_type_name,
                emc.entity_name AS alliance_name
         FROM sovereignty_structures_history ssh
         LEFT JOIN ref_item_types rit ON rit.type_id = ssh.structure_type_id
         LEFT JOIN entity_metadata_cache emc
              ON emc.entity_type = 'alliance' AND emc.entity_id = ssh.alliance_id
         WHERE ssh.solar_system_id = ?
         ORDER BY ssh.recorded_at DESC
         LIMIT 100",
        [$systemId]
    );

    return $result;
}

function db_sovereignty_stats_for_alliance(int $allianceId): array
{
    $rows = db_select(
        "SELECT
            (SELECT COUNT(*) FROM sovereignty_map WHERE alliance_id = ?) AS systems_held,
            (SELECT COUNT(*) FROM sovereignty_structures WHERE alliance_id = ?) AS structures_count,
            (SELECT AVG(vulnerability_occupancy_level) FROM sovereignty_structures
             WHERE alliance_id = ? AND vulnerability_occupancy_level IS NOT NULL) AS avg_adm,
            (SELECT COUNT(*) FROM sovereignty_campaigns
             WHERE defender_id = ? AND is_active = 1) AS active_campaigns,
            (SELECT COUNT(*) FROM sovereignty_map_history
             WHERE previous_alliance_id = ?
               AND changed_at > DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)) AS losses_30d,
            (SELECT COUNT(*) FROM sovereignty_map_history
             WHERE new_alliance_id = ?
               AND changed_at > DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)) AS gains_30d",
        [$allianceId, $allianceId, $allianceId, $allianceId, $allianceId, $allianceId]
    );
    $r = $rows[0] ?? [];
    return [
        'systems_held' => (int) ($r['systems_held'] ?? 0),
        'structures_count' => (int) ($r['structures_count'] ?? 0),
        'avg_adm' => $r['avg_adm'] !== null ? round((float) $r['avg_adm'], 1) : null,
        'active_campaigns' => (int) ($r['active_campaigns'] ?? 0),
        'losses_30d' => (int) ($r['losses_30d'] ?? 0),
        'gains_30d' => (int) ($r['gains_30d'] ?? 0),
    ];
}

function db_sovereignty_dashboard_metrics(): array
{
    $standings = db_corp_contacts_by_standing();
    $friendlyIds = $standings['friendly_alliance_ids'];
    $hostileIds = $standings['hostile_alliance_ids'];

    $metrics = [
        'friendly_systems_held' => 0,
        'hostile_systems_held' => 0,
        'friendly_under_contest' => 0,
        'avg_friendly_adm' => null,
        'friendly_vulnerable_now' => 0,
        'friendly_low_adm_count' => 0,
        'active_hostile_campaigns' => 0,
        'ownership_changes_7d' => 0,
        'ownership_changes_30d' => 0,
    ];

    if ($friendlyIds) {
        $ph = implode(',', array_fill(0, count($friendlyIds), '?'));
        $r = db_select("SELECT COUNT(*) AS cnt FROM sovereignty_map WHERE alliance_id IN ({$ph})", $friendlyIds);
        $metrics['friendly_systems_held'] = (int) ($r[0]['cnt'] ?? 0);

        $r = db_select(
            "SELECT AVG(vulnerability_occupancy_level) AS avg_adm FROM sovereignty_structures
             WHERE alliance_id IN ({$ph}) AND vulnerability_occupancy_level IS NOT NULL",
            $friendlyIds
        );
        $metrics['avg_friendly_adm'] = $r[0]['avg_adm'] !== null ? round((float) $r[0]['avg_adm'], 1) : null;

        $r = db_select(
            "SELECT COUNT(*) AS cnt FROM sovereignty_structures
             WHERE alliance_id IN ({$ph})
               AND vulnerable_start_time <= UTC_TIMESTAMP()
               AND vulnerable_end_time >= UTC_TIMESTAMP()",
            $friendlyIds
        );
        $metrics['friendly_vulnerable_now'] = (int) ($r[0]['cnt'] ?? 0);

        $r = db_select(
            "SELECT COUNT(*) AS cnt FROM sovereignty_structures
             WHERE alliance_id IN ({$ph})
               AND vulnerability_occupancy_level IS NOT NULL
               AND vulnerability_occupancy_level < 3.0",
            $friendlyIds
        );
        $metrics['friendly_low_adm_count'] = (int) ($r[0]['cnt'] ?? 0);

        $r = db_select(
            "SELECT COUNT(*) AS cnt FROM sovereignty_campaigns
             WHERE defender_id IN ({$ph}) AND is_active = 1",
            $friendlyIds
        );
        $metrics['friendly_under_contest'] = (int) ($r[0]['cnt'] ?? 0);
    }

    if ($hostileIds) {
        $ph = implode(',', array_fill(0, count($hostileIds), '?'));
        $r = db_select("SELECT COUNT(*) AS cnt FROM sovereignty_map WHERE alliance_id IN ({$ph})", $hostileIds);
        $metrics['hostile_systems_held'] = (int) ($r[0]['cnt'] ?? 0);

        $r = db_select(
            "SELECT COUNT(*) AS cnt FROM sovereignty_campaigns
             WHERE defender_id IN ({$ph}) AND is_active = 1",
            $hostileIds
        );
        $metrics['active_hostile_campaigns'] = (int) ($r[0]['cnt'] ?? 0);
    }

    $r = db_select("SELECT COUNT(*) AS cnt FROM sovereignty_map_history WHERE changed_at > DATE_SUB(UTC_TIMESTAMP(), INTERVAL 7 DAY)");
    $metrics['ownership_changes_7d'] = (int) ($r[0]['cnt'] ?? 0);

    $r = db_select("SELECT COUNT(*) AS cnt FROM sovereignty_map_history WHERE changed_at > DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 DAY)");
    $metrics['ownership_changes_30d'] = (int) ($r[0]['cnt'] ?? 0);

    return $metrics;
}

function db_sovereignty_alert_resolve(int $alertId): void
{
    $pdo = db();
    $stmt = $pdo->prepare("UPDATE sovereignty_alerts SET status = 'resolved', resolved_at = UTC_TIMESTAMP() WHERE id = ?");
    $stmt->execute([$alertId]);
}

// ---------------------------------------------------------------------------
//  Map Intelligence queries
// ---------------------------------------------------------------------------

/**
 * Fetch map intelligence scores for a set of systems.
 * Returns array keyed by system_id with chokepoint_score, connectivity_score, etc.
 */
function db_map_system_intelligence(array $systemIds): array
{
    $systemIds = array_values(array_filter(array_map('intval', $systemIds), static fn(int $id): bool => $id > 0));
    if ($systemIds === []) {
        return [];
    }
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $rows = db_select(
        "SELECT system_id, chokepoint_score, connectivity_score, is_bridge, community_id, label_priority
         FROM map_system_intelligence
         WHERE system_id IN ({$ph})",
        $systemIds
    );
    $result = [];
    foreach ($rows as $r) {
        $sid = (int) $r['system_id'];
        $result[$sid] = [
            'chokepoint_score'   => (float) ($r['chokepoint_score'] ?? 0),
            'connectivity_score' => (float) ($r['connectivity_score'] ?? 0),
            'is_bridge'          => (bool) ($r['is_bridge'] ?? false),
            'community_id'       => $r['community_id'] !== null ? (int) $r['community_id'] : null,
            'label_priority'     => (float) ($r['label_priority'] ?? 0),
        ];
    }
    return $result;
}

/**
 * Fetch map edge intelligence for edges between given systems.
 * Returns array keyed by "from:to" (normalized min:max) with risk scores.
 */
function db_map_edge_intelligence(array $systemIds): array
{
    $systemIds = array_values(array_filter(array_map('intval', $systemIds), static fn(int $id): bool => $id > 0));
    if ($systemIds === []) {
        return [];
    }
    $ph = implode(',', array_fill(0, count($systemIds), '?'));
    $rows = db_select(
        "SELECT from_system_id, to_system_id, corridor_count, corridor_score_sum,
                battle_count, is_bridge_edge, risk_score
         FROM map_edge_intelligence
         WHERE from_system_id IN ({$ph}) AND to_system_id IN ({$ph})",
        array_merge($systemIds, $systemIds)
    );
    $result = [];
    foreach ($rows as $r) {
        $key = min((int) $r['from_system_id'], (int) $r['to_system_id'])
             . ':' . max((int) $r['from_system_id'], (int) $r['to_system_id']);
        $result[$key] = [
            'corridor_count'     => (int) ($r['corridor_count'] ?? 0),
            'corridor_score_sum' => (float) ($r['corridor_score_sum'] ?? 0),
            'battle_count'       => (int) ($r['battle_count'] ?? 0),
            'is_bridge_edge'     => (bool) ($r['is_bridge_edge'] ?? false),
            'risk_score'         => (float) ($r['risk_score'] ?? 0),
        ];
    }
    return $result;
}

// ---------------------------------------------------------------------------
// Intelligence Events — Analyst Consumption Surface (Phase 3)
// ---------------------------------------------------------------------------

/**
 * Active event queue: paginated list of events for analyst triage.
 * Supports filtering by family, severity, state, and entity.
 */
function db_intelligence_events_queue(
    int $limit = 50,
    int $offset = 0,
    string $family = '',
    string $severity = '',
    string $state = 'active',
    string $entityType = '',
    int $entityId = 0,
    string $sortBy = 'impact_score',
    string $sortDir = 'DESC'
): array {
    $where = [];
    $params = [];

    if ($state !== '' && $state !== 'all') {
        $where[] = 'ie.state = ?';
        $params[] = $state;
    }
    if ($family !== '') {
        $where[] = 'ie.event_family = ?';
        $params[] = $family;
    }
    if ($severity !== '') {
        $where[] = 'ie.severity = ?';
        $params[] = $severity;
    }
    if ($entityType !== '') {
        $where[] = 'ie.entity_type = ?';
        $params[] = $entityType;
    }
    if ($entityId > 0) {
        $where[] = 'ie.entity_id = ?';
        $params[] = $entityId;
    }

    $whereClause = $where !== [] ? 'WHERE ' . implode(' AND ', $where) : '';

    // Whitelist sort columns
    $allowedSort = ['impact_score', 'severity', 'first_detected_at', 'last_updated_at', 'escalation_count'];
    if (!in_array($sortBy, $allowedSort, true)) {
        $sortBy = 'impact_score';
    }
    $sortDir = strtoupper($sortDir) === 'ASC' ? 'ASC' : 'DESC';

    $sql = "SELECT ie.*,
                   emc.entity_name AS entity_name,
                   cohc.current_corporation_id,
                   cohc.current_alliance_id,
                   emc_corp.entity_name AS corporation_name,
                   emc_ally.entity_name AS alliance_name
            FROM intelligence_events ie
            LEFT JOIN entity_metadata_cache emc
                ON emc.entity_type = ie.entity_type COLLATE utf8mb4_general_ci AND emc.entity_id = ie.entity_id
            LEFT JOIN character_org_history_cache cohc
                ON ie.entity_type = 'character' AND cohc.character_id = ie.entity_id AND cohc.source = 'evewho'
            LEFT JOIN entity_metadata_cache emc_corp
                ON emc_corp.entity_type = 'corporation' AND emc_corp.entity_id = cohc.current_corporation_id
            LEFT JOIN entity_metadata_cache emc_ally
                ON emc_ally.entity_type = 'alliance' AND emc_ally.entity_id = cohc.current_alliance_id
            {$whereClause}
            ORDER BY {$sortBy} {$sortDir}
            LIMIT ? OFFSET ?";
    $params[] = $limit;
    $params[] = $offset;

    return db_select($sql, $params);
}

/**
 * Count events matching the same filters (for pagination).
 */
function db_intelligence_events_count(
    string $family = '',
    string $severity = '',
    string $state = 'active',
    string $entityType = '',
    int $entityId = 0
): int {
    $where = [];
    $params = [];

    if ($state !== '' && $state !== 'all') {
        $where[] = 'state = ?';
        $params[] = $state;
    }
    if ($family !== '') {
        $where[] = 'event_family = ?';
        $params[] = $family;
    }
    if ($severity !== '') {
        $where[] = 'severity = ?';
        $params[] = $severity;
    }
    if ($entityType !== '') {
        $where[] = 'entity_type = ?';
        $params[] = $entityType;
    }
    if ($entityId > 0) {
        $where[] = 'entity_id = ?';
        $params[] = $entityId;
    }

    $whereClause = $where !== [] ? 'WHERE ' . implode(' AND ', $where) : '';

    $row = db_select_one("SELECT COUNT(*) AS cnt FROM intelligence_events {$whereClause}", $params);
    return (int) ($row['cnt'] ?? 0);
}

/**
 * Summary counts for the event queue dashboard header.
 */
function db_intelligence_events_summary(): array
{
    $rows = db_select(
        "SELECT state,
                severity,
                event_family,
                COUNT(*) AS cnt
         FROM intelligence_events
         GROUP BY state, severity, event_family"
    );

    $summary = [
        'total_active' => 0,
        'active_critical' => 0,
        'active_high' => 0,
        'active_medium' => 0,
        'active_low' => 0,
        'active_info' => 0,
        'active_threat' => 0,
        'active_quality' => 0,
        'acknowledged' => 0,
        'resolved_24h' => 0,
    ];

    foreach ($rows as $r) {
        $cnt = (int) $r['cnt'];
        if ($r['state'] === 'active') {
            $summary['total_active'] += $cnt;
            $key = 'active_' . $r['severity'];
            if (isset($summary[$key])) {
                $summary[$key] += $cnt;
            }
            if ($r['event_family'] === 'threat') {
                $summary['active_threat'] += $cnt;
            } elseif ($r['event_family'] === 'profile_quality') {
                $summary['active_quality'] += $cnt;
            }
        } elseif ($r['state'] === 'acknowledged') {
            $summary['acknowledged'] += $cnt;
        }
    }

    // Recently resolved (last 24h)
    $resolved = db_select_one(
        "SELECT COUNT(*) AS cnt FROM intelligence_events
         WHERE state = 'resolved' AND resolved_at >= DATE_SUB(NOW(), INTERVAL 24 HOUR)"
    );
    $summary['resolved_24h'] = (int) ($resolved['cnt'] ?? 0);

    return $summary;
}

/**
 * Single event detail with entity name resolution.
 */
function db_intelligence_event_detail(int $eventId): ?array
{
    if ($eventId <= 0) {
        return null;
    }
    return db_select_one(
        "SELECT ie.*,
                emc.entity_name AS entity_name,
                cohc.current_corporation_id,
                cohc.current_alliance_id,
                emc_corp.entity_name AS corporation_name,
                emc_ally.entity_name AS alliance_name
         FROM intelligence_events ie
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = ie.entity_type COLLATE utf8mb4_general_ci AND emc.entity_id = ie.entity_id
         LEFT JOIN character_org_history_cache cohc
             ON ie.entity_type = 'character' AND cohc.character_id = ie.entity_id AND cohc.source = 'evewho'
         LEFT JOIN entity_metadata_cache emc_corp
             ON emc_corp.entity_type = 'corporation' AND emc_corp.entity_id = cohc.current_corporation_id
         LEFT JOIN entity_metadata_cache emc_ally
             ON emc_ally.entity_type = 'alliance' AND emc_ally.entity_id = cohc.current_alliance_id
         WHERE ie.id = ?",
        [$eventId]
    );
}

/**
 * State transition history for an event (audit log).
 */
function db_intelligence_event_history(int $eventId): array
{
    if ($eventId <= 0) {
        return [];
    }
    return db_select(
        "SELECT * FROM intelligence_event_history
         WHERE event_id = ?
         ORDER BY created_at DESC",
        [$eventId]
    );
}

/**
 * Notes for an event.
 */
function db_intelligence_event_notes(int $eventId): array
{
    if ($eventId <= 0) {
        return [];
    }
    return db_select(
        "SELECT * FROM intelligence_event_notes
         WHERE event_id = ?
         ORDER BY created_at DESC",
        [$eventId]
    );
}

/**
 * Add a note to an event.
 */
function db_intelligence_event_note_add(int $eventId, string $analyst, string $note): bool
{
    if ($eventId <= 0 || $analyst === '' || $note === '') {
        return false;
    }
    db()->prepare(
        'INSERT INTO intelligence_event_notes (event_id, analyst, note)
         VALUES (?, ?, ?)'
    )->execute([$eventId, $analyst, $note]);
    return true;
}

/**
 * Acknowledge an event: set state to 'acknowledged', record who did it.
 * Returns true if the event was actually updated.
 */
function db_intelligence_event_acknowledge(int $eventId, string $analyst, ?string $reason = null): bool
{
    if ($eventId <= 0) {
        return false;
    }
    $event = db_select_one("SELECT id, state, severity FROM intelligence_events WHERE id = ?", [$eventId]);
    if ($event === null || $event['state'] !== 'active') {
        return false;
    }

    db()->prepare(
        "UPDATE intelligence_events
         SET state = 'acknowledged', acknowledged_by = ?, last_updated_at = NOW()
         WHERE id = ? AND state = 'active'"
    )->execute([$analyst, $eventId]);

    // Audit trail
    db()->prepare(
        "INSERT INTO intelligence_event_history
            (event_id, previous_state, new_state, previous_severity, new_severity, changed_by, reason)
         VALUES (?, 'active', 'acknowledged', ?, ?, ?, ?)"
    )->execute([$eventId, $event['severity'], $event['severity'], $analyst, $reason]);

    return true;
}

/**
 * Resolve an event manually (analyst-driven resolution).
 */
function db_intelligence_event_resolve(int $eventId, string $analyst, ?string $reason = null): bool
{
    if ($eventId <= 0) {
        return false;
    }
    $event = db_select_one("SELECT id, state, severity FROM intelligence_events WHERE id = ?", [$eventId]);
    if ($event === null || !in_array($event['state'], ['active', 'acknowledged'], true)) {
        return false;
    }

    db()->prepare(
        "UPDATE intelligence_events
         SET state = 'resolved', resolved_at = NOW(), last_updated_at = NOW()
         WHERE id = ?"
    )->execute([$eventId]);

    db()->prepare(
        "INSERT INTO intelligence_event_history
            (event_id, previous_state, new_state, previous_severity, new_severity, changed_by, reason)
         VALUES (?, ?, 'resolved', ?, ?, ?, ?)"
    )->execute([$eventId, $event['state'], $event['severity'], $event['severity'], $analyst, $reason]);

    return true;
}

/**
 * Suppress an event: hide from queue for a duration unless materially worsened.
 * Semantics: acknowledge + "do not re-surface until the suppress period expires
 * OR the condition materially worsens."
 *
 * @param int $hours Suppression duration in hours (default 72 = 3 days)
 */
function db_intelligence_event_suppress(int $eventId, string $analyst, int $hours = 72, ?string $reason = null): bool
{
    if ($eventId <= 0) {
        return false;
    }
    $event = db_select_one("SELECT id, state, severity FROM intelligence_events WHERE id = ?", [$eventId]);
    if ($event === null || !in_array($event['state'], ['active', 'acknowledged'], true)) {
        return false;
    }

    db()->prepare(
        "UPDATE intelligence_events
         SET state = 'suppressed',
             suppressed_until = DATE_ADD(NOW(), INTERVAL ? HOUR),
             suppressed_by = ?,
             last_updated_at = NOW()
         WHERE id = ?"
    )->execute([$hours, $analyst, $eventId]);

    db()->prepare(
        "INSERT INTO intelligence_event_history
            (event_id, previous_state, new_state, previous_severity, new_severity, changed_by, reason)
         VALUES (?, ?, 'suppressed', ?, ?, ?, ?)"
    )->execute([$eventId, $event['state'], $event['severity'], $event['severity'], $analyst,
                ($reason !== null ? $reason . ' ' : '') . "(suppressed for {$hours}h)"]);

    return true;
}

/**
 * Bulk acknowledge: acknowledge multiple events at once.
 * Returns count of events actually updated.
 */
function db_intelligence_events_bulk_acknowledge(array $eventIds, string $analyst, ?string $reason = null): int
{
    $eventIds = array_filter(array_map('intval', $eventIds), static fn(int $id): bool => $id > 0);
    if ($eventIds === [] || $analyst === '') {
        return 0;
    }

    $count = 0;
    foreach ($eventIds as $id) {
        if (db_intelligence_event_acknowledge($id, $analyst, $reason)) {
            $count++;
        }
    }
    return $count;
}

/**
 * Acknowledge ALL active events matching the given filters in a single bulk operation.
 * Much faster than iterating one-by-one for large event queues.
 */
function db_intelligence_events_bulk_acknowledge_all(string $family, string $severity, string $analyst, ?string $reason = null): int
{
    if ($analyst === '') {
        return 0;
    }

    $where = ['ie.state = ?'];
    $params = ['active'];

    if ($family !== '') {
        $where[] = 'ie.event_family = ?';
        $params[] = $family;
    }
    if ($severity !== '') {
        $where[] = 'ie.severity = ?';
        $params[] = $severity;
    }

    $whereClause = implode(' AND ', $where);

    return db_transaction(static function (PDO $pdo) use ($whereClause, $params, $analyst, $reason): int {
        // Insert audit trail rows before updating state
        $historyParams = array_merge([$analyst, $reason ?? 'Bulk acknowledge all'], $params);
        $pdo->prepare(
            "INSERT INTO intelligence_event_history
                (event_id, previous_state, new_state, previous_severity, new_severity, changed_by, reason)
             SELECT ie.id, 'active', 'acknowledged', ie.severity, ie.severity, ?, ?
             FROM intelligence_events ie
             WHERE {$whereClause}"
        )->execute($historyParams);

        // Bulk update all matching events
        $updateParams = array_merge([$analyst], $params);
        $stmt = $pdo->prepare(
            "UPDATE intelligence_events ie
             SET ie.state = 'acknowledged', ie.acknowledged_by = ?, ie.last_updated_at = NOW()
             WHERE {$whereClause}"
        );
        $stmt->execute($updateParams);

        return $stmt->rowCount();
    });
}

/**
 * "Why this fired" evidence: get CIP profile snapshot and active signals
 * for the entity at the time of event detection.
 */
function db_intelligence_event_evidence(int $eventId): array
{
    $event = db_intelligence_event_detail($eventId);
    if ($event === null || $event['entity_type'] !== 'character') {
        return ['event' => $event, 'profile' => null, 'signals' => [], 'history' => []];
    }

    $characterId = (int) $event['entity_id'];

    // Current profile
    $profile = db_select_one(
        "SELECT * FROM character_intelligence_profiles WHERE character_id = ?",
        [$characterId]
    );

    // Active signals (ordered by contribution weight)
    $signals = db_select(
        "SELECT cis.*, isd.display_name, isd.signal_domain, isd.weight_default
         FROM character_intelligence_signals cis
         JOIN intelligence_signal_definitions isd ON isd.signal_type = cis.signal_type
         WHERE cis.character_id = ?
         ORDER BY (cis.signal_value * cis.confidence * isd.weight_default) DESC
         LIMIT 20",
        [$characterId]
    );

    // Profile history (last 7 snapshots for trend)
    $history = db_select(
        "SELECT snapshot_date, risk_score, risk_rank, risk_percentile,
                confidence, freshness, signal_coverage
         FROM character_intelligence_profile_history
         WHERE character_id = ?
         ORDER BY snapshot_date DESC
         LIMIT 7",
        [$characterId]
    );

    // Active compound signals
    $compounds = db_select(
        "SELECT cics.*, icd.display_name, icd.description AS compound_description
         FROM character_intelligence_compound_signals cics
         LEFT JOIN intelligence_compound_definitions icd
             ON icd.compound_type = cics.compound_type
         WHERE cics.character_id = ?
         ORDER BY cics.score DESC",
        [$characterId]
    );

    return [
        'event' => $event,
        'profile' => $profile,
        'signals' => $signals,
        'compounds' => $compounds,
        'history' => $history,
    ];
}

/**
 * Related events: other events for the same entity.
 */
function db_intelligence_event_related(int $eventId, int $limit = 10): array
{
    $event = db_select_one("SELECT entity_type, entity_id FROM intelligence_events WHERE id = ?", [$eventId]);
    if ($event === null) {
        return [];
    }
    return db_select(
        "SELECT id, event_type, event_family, state, severity, impact_score,
                title, first_detected_at, last_updated_at
         FROM intelligence_events
         WHERE entity_type = ? AND entity_id = ? AND id != ?
         ORDER BY last_updated_at DESC
         LIMIT ?",
        [$event['entity_type'], $event['entity_id'], $eventId, $limit]
    );
}

/**
 * Latest digest for the dashboard.
 */
function db_intelligence_event_digest_latest(string $digestType = 'daily'): ?array
{
    return db_select_one(
        "SELECT * FROM intelligence_event_digests
         WHERE digest_type = ?
         ORDER BY period_end DESC
         LIMIT 1",
        [$digestType]
    );
}

// ---------------------------------------------------------------------------
// Compound Signal Analytics (Phase 4.5)
// ---------------------------------------------------------------------------

/**
 * Latest compound analytics snapshots (one per compound type).
 */
function db_compound_analytics_latest(): array
{
    return db_select(
        "SELECT cas.*
         FROM compound_analytics_snapshots cas
         INNER JOIN (
             SELECT compound_type, MAX(snapshot_date) AS max_date
             FROM compound_analytics_snapshots
             GROUP BY compound_type
         ) latest ON cas.compound_type = latest.compound_type
                  AND cas.snapshot_date = latest.max_date
         ORDER BY cas.active_count DESC"
    );
}

/**
 * Compound analytics history for a specific compound (last N days).
 */
function db_compound_analytics_history(string $compoundType, int $days = 30): array
{
    return db_select(
        "SELECT * FROM compound_analytics_snapshots
         WHERE compound_type = ?
           AND snapshot_date >= DATE_SUB(CURDATE(), INTERVAL ? DAY)
         ORDER BY snapshot_date DESC",
        [$compoundType, $days]
    );
}

/**
 * Record an analyst outcome for a character that has active compound signals.
 * This captures which compounds were present when the analyst made a judgment,
 * enabling per-compound precision tracking.
 */
function db_compound_analyst_outcome_record(
    int $characterId,
    string $outcome,
    string $analyst,
    ?int $eventId = null,
    ?string $notes = null
): int {
    if ($characterId <= 0 || $outcome === '') {
        return 0;
    }
    $allowed = ['true_positive', 'false_positive', 'inconclusive', 'confirmed_clean'];
    if (!in_array($outcome, $allowed, true)) {
        return 0;
    }

    // Get profile snapshot
    $profile = db_select_one(
        "SELECT risk_score, risk_rank FROM character_intelligence_profiles WHERE character_id = ?",
        [$characterId]
    );

    // Get all active compounds for this character
    $compounds = db_select(
        "SELECT cics.compound_type, cics.score, cics.confidence, icd.compound_type AS def_type
         FROM character_intelligence_compound_signals cics
         LEFT JOIN intelligence_compound_definitions icd ON icd.compound_type = cics.compound_type
         WHERE cics.character_id = ?",
        [$characterId]
    );

    $count = 0;
    foreach ($compounds as $comp) {
        // Look up family from definitions table or default
        $family = '';
        $familyRow = db_select_one(
            "SELECT compound_family FROM compound_analytics_snapshots WHERE compound_type = ? ORDER BY snapshot_date DESC LIMIT 1",
            [$comp['compound_type']]
        );
        if ($familyRow !== null) {
            $family = (string) $familyRow['compound_family'];
        }

        db()->prepare(
            "INSERT INTO compound_analyst_outcomes
                (character_id, compound_type, compound_family,
                 compound_score, compound_confidence,
                 outcome, event_id, risk_score_at_outcome, risk_rank_at_outcome,
                 analyst, notes)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )->execute([
            $characterId, $comp['compound_type'], $family,
            (float) ($comp['score'] ?? 0), (float) ($comp['confidence'] ?? 0),
            $outcome, $eventId,
            (float) ($profile['risk_score'] ?? 0),
            (int) ($profile['risk_rank'] ?? 0),
            $analyst, $notes,
        ]);
        $count++;
    }
    return $count;
}

/**
 * Latest calibration snapshot for priority band display.
 */
function db_intelligence_calibration_latest(): ?array
{
    return db_select_one(
        "SELECT * FROM intelligence_calibration_snapshots
         ORDER BY snapshot_date DESC
         LIMIT 1"
    );
}

/**
 * Assign a priority band based on calibrated thresholds.
 */
function intelligence_priority_band(float $riskScore, ?array $calibration): string
{
    if ($calibration === null) {
        return '';
    }
    $critical = (float) ($calibration['band_critical_floor'] ?? 0);
    $high = (float) ($calibration['band_high_floor'] ?? 0);
    $moderate = (float) ($calibration['band_moderate_floor'] ?? 0);
    $low = (float) ($calibration['band_low_floor'] ?? 0);

    if ($critical > 0 && $riskScore >= $critical) { return 'critical'; }
    if ($high > 0 && $riskScore >= $high) { return 'high'; }
    if ($moderate > 0 && $riskScore >= $moderate) { return 'moderate'; }
    if ($low > 0 && $riskScore >= $low) { return 'low'; }
    return 'noise';
}

/**
 * Per-compound outcome summary for the analytics dashboard.
 */
function db_compound_outcome_summary(): array
{
    return db_select(
        "SELECT compound_type, compound_family, outcome,
                COUNT(*) AS cnt,
                AVG(compound_score) AS avg_score
         FROM compound_analyst_outcomes
         GROUP BY compound_type, compound_family, outcome
         ORDER BY compound_type, outcome"
    );
}

// ---------------------------------------------------------------------------
// CIP Incident Log
// ---------------------------------------------------------------------------

/**
 * Recent CIP incidents (unresolved first, then recent).
 */
function db_cip_incidents_recent(int $limit = 20): array
{
    return db_select(
        "SELECT * FROM cip_incident_log
         ORDER BY resolved ASC, created_at DESC
         LIMIT ?",
        [$limit]
    );
}

/**
 * Mark an incident as resolved.
 */
function db_cip_incident_resolve(int $incidentId, string $analyst): bool
{
    if ($incidentId <= 0) {
        return false;
    }
    db()->prepare(
        "UPDATE cip_incident_log
         SET resolved = 1, resolved_by = ?, resolved_at = NOW()
         WHERE id = ? AND resolved = 0"
    )->execute([$analyst, $incidentId]);
    return true;
}

/**
 * Count of unresolved incidents.
 */
function db_cip_incidents_unresolved_count(): int
{
    $row = db_select_one("SELECT COUNT(*) AS cnt FROM cip_incident_log WHERE resolved = 0");
    return (int) ($row['cnt'] ?? 0);
}

// ---------------------------------------------------------------------------
// CIP Admin & Operational Health (Phase 5 hardening)
// ---------------------------------------------------------------------------

/**
 * Toggle a compound definition's enabled state.
 */
function db_compound_toggle_enabled(string $compoundType, bool $enabled): bool
{
    if ($compoundType === '') {
        return false;
    }
    db()->prepare(
        "UPDATE intelligence_compound_definitions
         SET enabled = ?
         WHERE compound_type = ?"
    )->execute([$enabled ? 1 : 0, $compoundType]);
    return true;
}

/**
 * Get all compound definitions with their enabled state.
 */
function db_compound_definitions_all(): array
{
    return db_select(
        "SELECT compound_type, display_name, enabled, version,
                base_weight, severity_default, score_mode
         FROM intelligence_compound_definitions
         ORDER BY compound_type"
    );
}

/**
 * Calibration history (last N snapshots).
 */
function db_calibration_history(int $limit = 14): array
{
    return db_select(
        "SELECT * FROM intelligence_calibration_snapshots
         ORDER BY snapshot_date DESC
         LIMIT ?",
        [$limit]
    );
}

/**
 * Override a specific calibration threshold.
 * Stores the override in app_settings so it persists across calibration runs.
 * The calibration job reads these overrides and respects them.
 *
 * Keys: cip_override_surge_delta, cip_override_rank_jump, cip_override_freshness_floor
 * Value "auto" or empty = use calibrated value.
 */
function db_calibration_override_set(string $key, string $value): bool
{
    $allowed = ['cip_override_surge_delta', 'cip_override_rank_jump', 'cip_override_freshness_floor', 'cip_calibration_frozen'];
    if (!in_array($key, $allowed, true)) {
        return false;
    }
    db()->prepare(
        "INSERT INTO app_settings (setting_key, setting_value)
         VALUES (?, ?)
         ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)"
    )->execute([$key, $value]);
    return true;
}

/**
 * Get all CIP override settings.
 */
function db_calibration_overrides_get(): array
{
    $keys = ['cip_override_surge_delta', 'cip_override_rank_jump', 'cip_override_freshness_floor', 'cip_calibration_frozen'];
    $placeholders = implode(',', array_fill(0, count($keys), '?'));
    $rows = db_select(
        "SELECT setting_key, setting_value FROM app_settings WHERE setting_key IN ({$placeholders})",
        $keys
    );
    $result = [];
    foreach ($rows as $r) {
        $result[$r['setting_key']] = $r['setting_value'];
    }
    return $result;
}

/**
 * Operational health metrics for the CIP system.
 */
function db_cip_operational_health(): array
{
    // Event volume by state
    $eventsByState = db_select(
        "SELECT state, COUNT(*) AS cnt FROM intelligence_events GROUP BY state"
    );

    // Event creation rate (last 7 days, per day)
    $dailyCreation = db_select(
        "SELECT DATE(first_detected_at) AS event_date, COUNT(*) AS cnt
         FROM intelligence_events
         WHERE first_detected_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
         GROUP BY DATE(first_detected_at)
         ORDER BY event_date DESC"
    );

    // Compound activation summary
    $compoundCounts = db_select(
        "SELECT compound_type, COUNT(*) AS active_count,
                AVG(score) AS avg_score, AVG(confidence) AS avg_conf
         FROM character_intelligence_compound_signals
         GROUP BY compound_type
         ORDER BY active_count DESC"
    );

    // Profile coverage
    $profileStats = db_select_one(
        "SELECT COUNT(*) AS total_profiles,
                AVG(risk_score) AS avg_risk,
                AVG(confidence) AS avg_confidence,
                AVG(freshness) AS avg_freshness,
                AVG(effective_coverage) AS avg_coverage,
                AVG(signal_count) AS avg_signals
         FROM character_intelligence_profiles
         WHERE signal_count > 0"
    );

    // Event engine last run
    $lastRun = db_select_one(
        "SELECT last_finished_at AS finished_at, latest_status, latest_event_type
         FROM scheduler_job_current_status
         WHERE job_key = 'cip_event_engine'"
    );

    // Suppression stats
    $suppressStats = db_select_one(
        "SELECT COUNT(*) AS total_suppressed,
                COUNT(CASE WHEN suppressed_until > NOW() THEN 1 END) AS still_suppressed,
                COUNT(CASE WHEN suppressed_until <= NOW() THEN 1 END) AS expired_suppressions
         FROM intelligence_events
         WHERE state = 'suppressed' OR suppressed_until IS NOT NULL"
    );

    return [
        'events_by_state' => $eventsByState,
        'daily_creation' => $dailyCreation,
        'compound_counts' => $compoundCounts,
        'profile_stats' => $profileStats,
        'last_run' => $lastRun,
        'suppress_stats' => $suppressStats,
    ];
}

// ---------------------------------------------------------------------------
// Spy detection — cases, risk profiles, identity links
// ---------------------------------------------------------------------------

function db_spy_network_cases_recent(int $limit = 50, ?string $severity = null, ?string $status = null): array
{
    $safeLimit = max(1, min(200, $limit));
    $conditions = [];
    $params = [];
    if ($severity !== null && in_array($severity, ['monitor', 'medium', 'high', 'critical'], true)) {
        $conditions[] = 'severity_tier = ?';
        $params[] = $severity;
    }
    if ($status !== null && in_array($status, ['open', 'reviewing', 'closed', 'reopened'], true)) {
        $conditions[] = 'status = ?';
        $params[] = $status;
    }
    $where = $conditions === [] ? '' : ('WHERE ' . implode(' AND ', $conditions));
    return db_select(
        'SELECT case_id, community_id, community_source, ring_score, confidence_score,
                severity_tier, member_count, suspicious_member_ratio, bridge_concentration,
                hostile_overlap_density, recurrence_stability, identity_density,
                status, first_detected_at, last_reinforced_at, computed_at
         FROM spy_network_cases ' . $where . '
         ORDER BY ring_score DESC, last_reinforced_at DESC
         LIMIT ' . $safeLimit,
        $params
    );
}

function db_spy_network_case_detail(int $caseId): ?array
{
    if ($caseId <= 0) {
        return null;
    }
    return db_select_one(
        'SELECT case_id, community_id, community_source, identity_cluster_id,
                ring_score, confidence_score, severity_tier, member_count,
                suspicious_member_ratio, bridge_concentration, recent_growth_score,
                hostile_overlap_density, recurrence_stability, identity_density,
                feature_breakdown_json, status, status_changed_at,
                first_detected_at, last_reinforced_at, model_version,
                computed_at, source_run_id
         FROM spy_network_cases
         WHERE case_id = ?',
        [$caseId]
    );
}

function db_spy_network_case_members(int $caseId, int $limit = 200): array
{
    if ($caseId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(500, $limit));
    return db_select(
        'SELECT sncm.character_id, sncm.member_contribution_score, sncm.role_label,
                sncm.evidence_json, sncm.computed_at,
                COALESCE(emc.entity_name, CONCAT("Character #", sncm.character_id)) AS character_name
         FROM spy_network_case_members sncm
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = "character" AND emc.entity_id = sncm.character_id
         WHERE sncm.case_id = ?
         ORDER BY sncm.member_contribution_score DESC
         LIMIT ' . $safeLimit,
        [$caseId]
    );
}

function db_spy_network_case_edges(int $caseId, int $limit = 200): array
{
    if ($caseId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(500, $limit));
    return db_select(
        'SELECT character_id_a, character_id_b, edge_type, edge_weight,
                component_weights_json, evidence_json, computed_at
         FROM spy_network_case_edges
         WHERE case_id = ?
         ORDER BY edge_weight DESC
         LIMIT ' . $safeLimit,
        [$caseId]
    );
}

function db_character_spy_risk_profile(int $characterId): ?array
{
    if ($characterId <= 0) {
        return null;
    }
    return db_select_one(
        'SELECT character_id, spy_risk_score, risk_percentile, confidence_score,
                confidence_tier, severity_tier,
                bridge_infiltration_score, pre_op_infiltration_score, hostile_overlap_score,
                temporal_coordination_score, identity_association_score, ring_context_score,
                behavioral_anomaly_score, org_movement_score, predicted_hostile_link_score,
                top_case_id, explanation_json, component_weights_json,
                model_version, computed_at, source_run_id
         FROM character_spy_risk_profiles
         WHERE character_id = ?',
        [$characterId]
    );
}

function db_character_spy_risk_evidence(int $characterId, int $limit = 50): array
{
    if ($characterId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(200, $limit));
    return db_select(
        'SELECT evidence_key, window_label, evidence_value, expected_value, deviation_value,
                z_score, cohort_percentile, confidence_flag, contribution_to_score,
                evidence_text, computed_at
         FROM character_spy_risk_evidence
         WHERE character_id = ?
         ORDER BY contribution_to_score DESC, evidence_key ASC
         LIMIT ' . $safeLimit,
        [$characterId]
    );
}

function db_character_spy_risk_top(int $limit = 50, ?string $severity = null): array
{
    $safeLimit = max(1, min(200, $limit));
    $conditions = [];
    $params = [];
    if ($severity !== null && in_array($severity, ['monitor', 'medium', 'high', 'critical'], true)) {
        $conditions[] = 'csrp.severity_tier = ?';
        $params[] = $severity;
    }
    $where = $conditions === [] ? '' : ('WHERE ' . implode(' AND ', $conditions));
    return db_select(
        'SELECT csrp.character_id, csrp.spy_risk_score, csrp.risk_percentile,
                csrp.confidence_score, csrp.confidence_tier, csrp.severity_tier,
                csrp.top_case_id, csrp.computed_at,
                COALESCE(emc.entity_name, CONCAT("Character #", csrp.character_id)) AS character_name
         FROM character_spy_risk_profiles csrp
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = "character" AND emc.entity_id = csrp.character_id
         ' . $where . '
         ORDER BY csrp.spy_risk_score DESC
         LIMIT ' . $safeLimit,
        $params
    );
}

function db_character_identity_links(int $characterId, int $limit = 25): array
{
    if ($characterId <= 0) {
        return [];
    }
    $safeLimit = max(1, min(200, $limit));
    return db_select(
        'SELECT cil.link_id, cil.character_id_a, cil.character_id_b, cil.link_score,
                cil.confidence_tier, cil.window_label,
                cil.org_history_score, cil.copresence_score, cil.temporal_score,
                cil.cross_side_score, cil.behavior_sim_score, cil.embedding_sim_score,
                cil.computed_at,
                CASE WHEN cil.character_id_a = ? THEN cil.character_id_b ELSE cil.character_id_a END AS other_character_id,
                COALESCE(emc.entity_name,
                    CONCAT("Character #", CASE WHEN cil.character_id_a = ? THEN cil.character_id_b ELSE cil.character_id_a END))
                    AS other_character_name
         FROM character_identity_links cil
         LEFT JOIN entity_metadata_cache emc
             ON emc.entity_type = "character"
             AND emc.entity_id = CASE WHEN cil.character_id_a = ? THEN cil.character_id_b ELSE cil.character_id_a END
         WHERE cil.character_id_a = ? OR cil.character_id_b = ?
         ORDER BY cil.link_score DESC
         LIMIT ' . $safeLimit,
        [$characterId, $characterId, $characterId, $characterId, $characterId]
    );
}

function db_spy_detection_summary(): array
{
    $caseCounts = db_select_one(
        "SELECT COUNT(*) AS total,
                SUM(CASE WHEN severity_tier = 'critical' THEN 1 ELSE 0 END) AS critical,
                SUM(CASE WHEN severity_tier = 'high' THEN 1 ELSE 0 END) AS high,
                SUM(CASE WHEN severity_tier = 'medium' THEN 1 ELSE 0 END) AS medium,
                SUM(CASE WHEN severity_tier = 'monitor' THEN 1 ELSE 0 END) AS monitor,
                SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_cases,
                SUM(CASE WHEN status = 'reviewing' THEN 1 ELSE 0 END) AS reviewing_cases,
                MAX(last_reinforced_at) AS last_reinforced_at
         FROM spy_network_cases"
    );

    $riskCounts = db_select_one(
        "SELECT COUNT(*) AS total_profiles,
                SUM(CASE WHEN severity_tier = 'critical' THEN 1 ELSE 0 END) AS critical_profiles,
                SUM(CASE WHEN severity_tier = 'high' THEN 1 ELSE 0 END) AS high_profiles,
                SUM(CASE WHEN confidence_tier = 'high' THEN 1 ELSE 0 END) AS high_confidence,
                MAX(computed_at) AS last_computed_at
         FROM character_spy_risk_profiles"
    );

    $identityCounts = db_select_one(
        "SELECT COUNT(*) AS total_links,
                SUM(CASE WHEN confidence_tier = 'high' THEN 1 ELSE 0 END) AS high_confidence,
                MAX(computed_at) AS last_computed_at
         FROM character_identity_links"
    );

    return [
        'cases' => $caseCounts ?: [],
        'risk_profiles' => $riskCounts ?: [],
        'identity_links' => $identityCounts ?: [],
    ];
}
