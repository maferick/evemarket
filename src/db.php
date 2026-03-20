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
            $pdo->commit();
        } else {
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

function db_market_orders_current_bulk_upsert(array $orders, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert(
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
}

function db_market_orders_history_bulk_insert(array $orders, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert(
        'market_orders_history',
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
        [],
        $chunkSize
    );
}


function db_market_orders_history_prune_before(string $cutoffObservedAt): int
{
    db_query_cache_clear();
    $stmt = db()->prepare('DELETE FROM market_orders_history WHERE observed_at < ?');
    $stmt->execute([$cutoffObservedAt]);

    return $stmt->rowCount();
}

function db_market_orders_current_latest_snapshot_rows(string $sourceType, int $sourceId): array
{
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);

    if ($safeSourceType === '' || $safeSourceId <= 0) {
        return [];
    }

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
           AND observed_at = (
                SELECT MAX(observed_at)
                FROM market_orders_current
                WHERE source_type = ?
                  AND source_id = ?
           )
         ORDER BY type_id ASC, is_buy_order ASC, price ASC, order_id ASC',
        [$safeSourceType, $safeSourceId, $safeSourceType, $safeSourceId]
    );
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

    return db_bulk_insert_or_upsert(
        'market_order_snapshots_summary',
        [
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
        ],
        $normalizedRows,
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

function db_market_orders_snapshot_metrics_window_raw(string $sourceType, int $sourceId, string $startObservedAt): array
{
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);
    $safeStartObservedAt = trim($startObservedAt);

    if ($safeSourceType === '' || $safeSourceId <= 0 || $safeStartObservedAt === '') {
        return [];
    }

    $rows = db_select(
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
            FROM market_orders_history moh
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
                FROM market_orders_history
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
    $safeSourceType = trim($sourceType);
    $safeSourceId = max(0, $sourceId);
    $safeStartObservedAt = trim($startObservedAt);

    if ($safeSourceType === '' || $safeSourceId <= 0 || $safeStartObservedAt === '') {
        return [];
    }

    $rows = db_select_cached(
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
         FROM market_order_snapshots_summary
         WHERE source_type = ?
           AND source_id = ?
           AND observed_at >= ?
         ORDER BY observed_at ASC, type_id ASC",
        [$safeSourceType, $safeSourceId, $safeStartObservedAt],
        60,
        'market.snapshot.metrics'
    );

    if ($rows === []) {
        return db_market_orders_snapshot_metrics_window_raw($safeSourceType, $safeSourceId, $safeStartObservedAt);
    }

    return array_map('db_market_order_snapshots_summary_normalize_row', $rows);
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

function db_market_orders_history_stock_health_series(
    string $sourceType,
    int $sourceId,
    string $startDate,
    string $endDate,
    array $typeIds = []
): array {
    $params = [$sourceType, $sourceId, $startDate, $endDate];
    $typeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
        $typeFilterSql = " AND moss.type_id IN ({$placeholders})";
        $rawTypeFilterSql = " AND moh.type_id IN ({$placeholders})";
        $params = array_merge($params, $normalizedTypeIds);
    }

    $rows = db_select_cached(
        "SELECT
            moss.observed_date,
            moss.type_id,
            rit.type_name,
            SUM(moss.total_sell_volume) AS sell_volume,
            SUM(moss.total_buy_volume) AS buy_volume,
            SUM(moss.sell_order_count) AS sell_order_count,
            SUM(moss.buy_order_count) AS buy_order_count,
            AVG(moss.best_sell_price) AS avg_sell_price,
            AVG(moss.best_buy_price) AS avg_buy_price,
            MAX(moss.observed_at) AS last_observed_at
         FROM market_order_snapshots_summary moss
         LEFT JOIN ref_item_types rit ON rit.type_id = moss.type_id
         WHERE moss.source_type = ?
           AND moss.source_id = ?
           AND moss.observed_date BETWEEN ? AND ?{$typeFilterSql}
         GROUP BY moss.observed_date, moss.type_id, rit.type_name
         ORDER BY moss.observed_date ASC, moss.type_id ASC",
        $params
        ,
        60,
        'market.snapshot.stock-health'
    );

    if ($rows !== []) {
        return $rows;
    }

    return db_select(
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
         FROM market_orders_history moh
         LEFT JOIN ref_item_types rit ON rit.type_id = moh.type_id
         WHERE moh.source_type = ?
           AND moh.source_id = ?
           AND moh.observed_date BETWEEN ? AND ?{$rawTypeFilterSql}
         GROUP BY moh.observed_date, moh.type_id, rit.type_name
         ORDER BY moh.observed_date ASC, moh.type_id ASC",
        array_merge([$sourceType, $sourceId, $startDate, $endDate], array_slice($params, 4))
    );
}

function db_market_orders_current_source_aggregates(string $sourceType, int $sourceId, array $typeIds = []): array
{
    $params = [$sourceType, $sourceId];
    $typeFilterSql = '';
    $rawTypeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $typePlaceholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
        $typeFilterSql = " AND moss.type_id IN ({$typePlaceholders})";
        $rawTypeFilterSql = " AND moc.type_id IN ({$typePlaceholders})";
        $params = array_merge($params, $normalizedTypeIds);
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
         FROM market_order_snapshots_summary moss
         LEFT JOIN ref_item_types rit ON rit.type_id = moss.type_id
         WHERE moss.source_type = ?
           AND moss.source_id = ?
           AND moss.observed_at = (
                SELECT MAX(summary_latest.observed_at)
                FROM market_order_snapshots_summary summary_latest
                WHERE summary_latest.source_type = ?
                  AND summary_latest.source_id = ?
           ){$typeFilterSql}
         ORDER BY moss.type_id ASC",
        [$sourceType, $sourceId, $sourceType, $sourceId, ...array_slice($params, 2)],
        60,
        'market.snapshot.current-aggregates'
    );

    if ($rows !== []) {
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
           AND moc.source_id = ?{$rawTypeFilterSql}
         GROUP BY moc.type_id, rit.type_name
         ORDER BY moc.type_id ASC",
        $params
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

function db_sync_schedule_fetch_due_jobs(int $limit = 20): array
{
    $safeLimit = max(1, min(200, $limit));

    return db_select(
        'SELECT id, job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until
         FROM sync_schedules
         WHERE enabled = 1
           AND next_run_at IS NOT NULL
           AND next_run_at <= UTC_TIMESTAMP()
           AND (locked_until IS NULL OR locked_until <= UTC_TIMESTAMP())
         ORDER BY next_run_at ASC, id ASC
         LIMIT ' . $safeLimit
    );
}

function db_sync_schedule_claim_job(int $scheduleId, int $lockTtlSeconds = 300): ?array
{
    $safeLockTtl = max(30, min(3600, $lockTtlSeconds));

    $stmt = db()->prepare(
        'UPDATE sync_schedules
         SET locked_until = DATE_ADD(UTC_TIMESTAMP(), INTERVAL ? SECOND),
             last_status = ?,
             last_error = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
           AND enabled = 1
           AND next_run_at IS NOT NULL
           AND next_run_at <= UTC_TIMESTAMP()
           AND (locked_until IS NULL OR locked_until <= UTC_TIMESTAMP())
         LIMIT 1'
    );

    $stmt->execute([$safeLockTtl, 'running', $scheduleId]);
    if ($stmt->rowCount() !== 1) {
        return null;
    }

    return db_select_one(
        'SELECT id, job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until
         FROM sync_schedules
         WHERE id = ?
         LIMIT 1',
        [$scheduleId]
    );
}

function db_sync_schedule_mark_success(int $scheduleId): bool
{
    return db_execute(
        'UPDATE sync_schedules
         SET last_run_at = UTC_TIMESTAMP(),
             last_status = ?,
             last_error = NULL,
             next_run_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL interval_seconds SECOND),
             locked_until = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        ['success', $scheduleId]
    );
}

function db_sync_schedule_mark_failure(int $scheduleId, string $errorMessage): bool
{
    return db_execute(
        'UPDATE sync_schedules
         SET last_run_at = UTC_TIMESTAMP(),
             last_status = ?,
             last_error = ?,
             next_run_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL interval_seconds SECOND),
             locked_until = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE id = ?
         LIMIT 1',
        ['failed', mb_substr($errorMessage, 0, 500), $scheduleId]
    );
}

function db_sync_schedule_fetch_by_job_keys(array $jobKeys): array
{
    $keys = array_values(array_filter(array_map(static fn (mixed $jobKey): string => trim((string) $jobKey), $jobKeys), static fn (string $jobKey): bool => $jobKey !== ''));
    if ($keys === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($keys), '?'));

    return db_select(
        "SELECT id, job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until
         FROM sync_schedules
         WHERE job_key IN ($placeholders)",
        $keys
    );
}

function db_sync_schedule_fetch_by_id(int $scheduleId): ?array
{
    if ($scheduleId <= 0) {
        return null;
    }

    $row = db_select_one(
        'SELECT id, job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until
         FROM sync_schedules
         WHERE id = ?
         LIMIT 1',
        [$scheduleId]
    );

    return $row !== [] ? $row : null;
}

function db_sync_schedule_running_job_keys(array $jobKeys): array
{
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
           AND last_status = 'running'
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

function db_sync_schedule_ensure_job(string $jobKey, int $enabled, int $intervalSeconds): bool
{
    $normalizedKey = trim($jobKey);
    if ($normalizedKey === '') {
        return false;
    }

    return db_execute(
        'INSERT INTO sync_schedules (job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until)
         VALUES (
            ?,
            ?,
            ?,
            CASE WHEN ? = 1 THEN UTC_TIMESTAMP() ELSE NULL END,
            NULL,
            NULL,
            NULL,
            NULL
         )
         ON DUPLICATE KEY UPDATE
            next_run_at = CASE
                WHEN enabled = 1 AND next_run_at IS NULL THEN UTC_TIMESTAMP()
                ELSE next_run_at
            END,
            updated_at = CURRENT_TIMESTAMP',
        [$normalizedKey, $enabled, $intervalSeconds, $enabled]
    );
}

function db_sync_schedule_upsert(string $jobKey, int $enabled, int $intervalSeconds): bool
{
    $normalizedKey = trim($jobKey);
    if ($normalizedKey === '') {
        return false;
    }

    return db_execute(
        'INSERT INTO sync_schedules (job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until)
         VALUES (
            ?,
            ?,
            ?,
            CASE WHEN ? = 1 THEN UTC_TIMESTAMP() ELSE NULL END,
            NULL,
            NULL,
            NULL,
            NULL
         )
         ON DUPLICATE KEY UPDATE
            enabled = VALUES(enabled),
            interval_seconds = VALUES(interval_seconds),
            next_run_at = CASE
                WHEN VALUES(enabled) = 1 THEN COALESCE(next_run_at, DATE_ADD(UTC_TIMESTAMP(), INTERVAL VALUES(interval_seconds) SECOND))
                ELSE NULL
            END,
            locked_until = CASE WHEN VALUES(enabled) = 1 THEN locked_until ELSE NULL END,
            updated_at = CURRENT_TIMESTAMP',
        [$normalizedKey, $enabled, $intervalSeconds, $enabled]
    );
}

function db_sync_schedule_force_due_all_enabled(): int
{
    $stmt = db()->prepare(
        'UPDATE sync_schedules
         SET next_run_at = UTC_TIMESTAMP(),
             locked_until = NULL,
             updated_at = CURRENT_TIMESTAMP
         WHERE enabled = 1'
    );

    $stmt->execute();

    return (int) $stmt->rowCount();
}

function db_sync_schedule_force_due_by_job_keys(array $jobKeys): int
{
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
                locked_until = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE enabled = 1
              AND job_key IN ($placeholders)";

    $stmt = db()->prepare($sql);
    $stmt->execute($keys);

    return (int) $stmt->rowCount();
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
    return db_execute(
        'INSERT INTO killmail_events (
            sequence_id,
            killmail_id,
            killmail_hash,
            uploaded_at,
            sequence_updated,
            killmail_time,
            solar_system_id,
            region_id,
            victim_character_id,
            victim_corporation_id,
            victim_alliance_id,
            victim_ship_type_id,
            zkb_json,
            raw_killmail_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            killmail_id = VALUES(killmail_id),
            killmail_hash = VALUES(killmail_hash),
            uploaded_at = VALUES(uploaded_at),
            sequence_updated = VALUES(sequence_updated),
            killmail_time = VALUES(killmail_time),
            solar_system_id = VALUES(solar_system_id),
            region_id = VALUES(region_id),
            victim_character_id = VALUES(victim_character_id),
            victim_corporation_id = VALUES(victim_corporation_id),
            victim_alliance_id = VALUES(victim_alliance_id),
            victim_ship_type_id = VALUES(victim_ship_type_id),
            zkb_json = VALUES(zkb_json),
            raw_killmail_json = VALUES(raw_killmail_json),
            updated_at = CURRENT_TIMESTAMP',
        [
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
            (string) ($event['zkb_json'] ?? '{}'),
            (string) ($event['raw_killmail_json'] ?? '{}'),
        ]
    );
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
            MAX(matched.matches_victim_corporation) AS matches_victim_corporation
        FROM (
            SELECT
                e.sequence_id,
                1 AS matches_victim_alliance,
                0 AS matches_victim_corporation
            FROM killmail_tracked_alliances ta
            INNER JOIN killmail_events e
                ON e.victim_alliance_id = ta.alliance_id{$effectiveFilterSql}{$sequenceFilterSql}
            WHERE ta.is_active = 1

            UNION ALL

            SELECT
                e.sequence_id,
                0 AS matches_victim_alliance,
                1 AS matches_victim_corporation
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

function db_killmail_detail(int $sequenceId): ?array
{
    if ($sequenceId <= 0) {
        return null;
    }

    $matchSql = db_killmail_tracked_match_sql('e');

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
            e.zkb_json,
            e.raw_killmail_json,
            e.created_at,
            e.updated_at,
            COALESCE(NULLIF(victim_tc.label, ''), '') AS victim_corporation_label,
            COALESCE(NULLIF(victim_ta.label, ''), '') AS victim_alliance_label,
            COALESCE(NULLIF(ship.type_name, ''), '') AS ship_type_name,
            COALESCE(NULLIF(system_ref.system_name, ''), '') AS system_name,
            COALESCE(NULLIF(region_ref.region_name, ''), '') AS region_name,
            CASE WHEN victim_ta.alliance_id IS NULL THEN 0 ELSE 1 END AS matches_victim_alliance,
            CASE WHEN victim_tc.corporation_id IS NULL THEN 0 ELSE 1 END AS matches_victim_corporation,
            CASE WHEN {$matchSql} THEN 1 ELSE 0 END AS matched_tracked
         FROM killmail_events e
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
            e.zkb_json,
            e.victim_corporation_id,
            e.victim_alliance_id,
            e.victim_ship_type_id,
            e.solar_system_id,
            e.region_id,
            COALESCE(NULLIF(victim_tc.label, ''), CONCAT('Corporation #', e.victim_corporation_id)) AS victim_corporation_label,
            COALESCE(NULLIF(victim_ta.label, ''), CONCAT('Alliance #', e.victim_alliance_id)) AS victim_alliance_label,
            COALESCE(NULLIF(ship.type_name, ''), CONCAT('Type #', e.victim_ship_type_id)) AS ship_type_name,
            COALESCE(NULLIF(system_ref.system_name, ''), CONCAT('System #', e.solar_system_id)) AS system_name,
            COALESCE(NULLIF(region_ref.region_name, ''), CONCAT('Region #', e.region_id)) AS region_name,
            COALESCE(tracked.matches_victim_alliance, 0) AS matches_victim_alliance,
            COALESCE(tracked.matches_victim_corporation, 0) AS matches_victim_corporation,
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
    $safeHours = max(1, min(24 * 30, $hours));
    $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
    if ($normalizedTypeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
    $trackedMatchesSql = db_killmail_tracked_matches_sql("(UTC_TIMESTAMP() - INTERVAL {$safeHours} HOUR)");
    $quantitySql = "GREATEST(
        COALESCE(i.quantity_destroyed, 0) + COALESCE(i.quantity_dropped, 0),
        CASE
            WHEN COALESCE(i.quantity_destroyed, 0) + COALESCE(i.quantity_dropped, 0) > 0 THEN 0
            WHEN i.item_role IN ('fitted', 'destroyed', 'dropped') THEN 1
            ELSE 0
        END
    )";

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
        'CREATE TABLE IF NOT EXISTS doctrine_fit_groups (
            doctrine_fit_id INT UNSIGNED NOT NULL,
            doctrine_group_id INT UNSIGNED NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (doctrine_fit_id, doctrine_group_id),
            KEY idx_doctrine_fit_groups_group (doctrine_group_id),
            CONSTRAINT fk_doctrine_fit_groups_fit FOREIGN KEY (doctrine_fit_id) REFERENCES doctrine_fits(id) ON DELETE CASCADE,
            CONSTRAINT fk_doctrine_fit_groups_group FOREIGN KEY (doctrine_group_id) REFERENCES doctrine_groups(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4'
    );

    db()->exec(
        'INSERT IGNORE INTO doctrine_fit_groups (doctrine_fit_id, doctrine_group_id)
         SELECT id, doctrine_group_id
         FROM doctrine_fits
         WHERE doctrine_group_id IS NOT NULL'
    );

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
                'SELECT doctrine_group_id FROM doctrine_fit_groups WHERE doctrine_fit_id = ? ORDER BY doctrine_group_id ASC LIMIT 1',
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

function db_doctrine_fit_membership_rows(int $fitId): array
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return [];
    }

    return db_select(
        'SELECT dg.id, dg.group_name
         FROM doctrine_fit_groups dfg
         INNER JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         WHERE dfg.doctrine_fit_id = ?
         ORDER BY dg.group_name ASC',
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
            GROUP_CONCAT(DISTINCT dfg.doctrine_group_id ORDER BY dfg.doctrine_group_id SEPARATOR ",") AS group_ids_csv,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR "||") AS group_names_csv
         FROM doctrine_fits df
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
         LEFT JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.source_type, df.source_format, df.source_reference, df.notes, df.import_body, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at
         ORDER BY df.updated_at DESC, df.fit_name ASC'
    );
}

function db_doctrine_fits_by_group(int $groupId): array
{
    doctrine_db_ensure_schema();

    if ($groupId <= 0) {
        return [];
    }

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
            COALESCE(SUM(dfi.quantity), 0) AS required_quantity,
            COUNT(dfi.id) AS fit_line_count
         FROM doctrine_fit_groups dfg
         INNER JOIN doctrine_fits df ON df.id = dfg.doctrine_fit_id
         LEFT JOIN doctrine_fit_items dfi ON dfi.doctrine_fit_id = df.id
         WHERE dfg.doctrine_group_id = ?
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.source_type, df.source_format, df.source_reference, df.notes, df.import_body, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at
         ORDER BY df.updated_at DESC, df.fit_name ASC',
        [$groupId]
    );
}

function db_doctrine_ungrouped_fits(): array
{
    doctrine_db_ensure_schema();

    return db_select(
        'SELECT df.*
         FROM doctrine_fits df
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
         WHERE dfg.doctrine_fit_id IS NULL
         ORDER BY df.updated_at DESC, df.fit_name ASC'
    );
}

function db_doctrine_fit_by_id(int $fitId): ?array
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return null;
    }

    return db_select_one(
        'SELECT
            df.*,
            pg.group_name AS primary_group_name,
            GROUP_CONCAT(DISTINCT dfg.doctrine_group_id ORDER BY dfg.doctrine_group_id SEPARATOR ",") AS group_ids_csv,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR "||") AS group_names_csv
         FROM doctrine_fits df
         LEFT JOIN doctrine_groups pg ON pg.id = df.doctrine_group_id
         LEFT JOIN doctrine_fit_groups dfg ON dfg.doctrine_fit_id = df.id
         LEFT JOIN doctrine_groups dg ON dg.id = dfg.doctrine_group_id
         WHERE df.id = ?
         GROUP BY df.id, df.doctrine_group_id, df.fit_name, df.ship_name, df.ship_type_id, df.source_type, df.source_format, df.source_reference, df.notes, df.import_body, df.raw_html, df.raw_buyall, df.raw_eft, df.metadata_json, df.parse_warnings_json, df.parse_status, df.review_status, df.conflict_state, df.fingerprint_hash, df.warning_count, df.item_count, df.unresolved_count, df.created_at, df.updated_at, pg.group_name
         LIMIT 1',
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

function db_doctrine_fit_replace_groups(int $fitId, array $groupIds): void
{
    $groupIds = array_values(array_unique(array_filter(array_map('intval', $groupIds), static fn (int $id): bool => $id > 0)));
    db_execute('DELETE FROM doctrine_fit_groups WHERE doctrine_fit_id = ?', [$fitId]);

    if ($groupIds !== []) {
        $rows = [];
        foreach ($groupIds as $groupId) {
            $rows[] = ['doctrine_fit_id' => $fitId, 'doctrine_group_id' => $groupId];
        }
        db_bulk_insert_or_upsert('doctrine_fit_groups', ['doctrine_fit_id', 'doctrine_group_id'], $rows);
    }

    db_execute(
        'UPDATE doctrine_fits SET doctrine_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
        [$groupIds[0] ?? null, $fitId]
    );
}

function db_doctrine_fit_create(array $fit, array $items, array $groupIds = []): int
{
    doctrine_db_ensure_schema();

    return db_transaction(function () use ($fit, $items, $groupIds): int {
        $groupIds = array_values(array_unique(array_filter(array_map('intval', $groupIds), static fn (int $id): bool => $id > 0)));
        db_execute(
            'INSERT INTO doctrine_fits (
                doctrine_group_id,
                fit_name,
                ship_name,
                ship_type_id,
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            [
                $groupIds[0] ?? (isset($fit['doctrine_group_id']) ? (int) $fit['doctrine_group_id'] : null),
                (string) ($fit['fit_name'] ?? ''),
                (string) ($fit['ship_name'] ?? ''),
                isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null,
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
        db_doctrine_fit_replace_groups($fitId, $groupIds);

        return $fitId;
    });
}

function db_doctrine_fit_update(int $fitId, array $fit, array $items, array $groupIds = []): bool
{
    doctrine_db_ensure_schema();

    if ($fitId <= 0) {
        return false;
    }

    return db_transaction(function () use ($fitId, $fit, $items, $groupIds): bool {
        $groupIds = array_values(array_unique(array_filter(array_map('intval', $groupIds), static fn (int $id): bool => $id > 0)));
        db_execute(
            'UPDATE doctrine_fits
             SET doctrine_group_id = ?, fit_name = ?, ship_name = ?, ship_type_id = ?, source_type = ?, source_format = ?, source_reference = ?, notes = ?, import_body = ?, raw_html = ?, raw_buyall = ?, raw_eft = ?, metadata_json = ?, parse_warnings_json = ?, parse_status = ?, review_status = ?, conflict_state = ?, fingerprint_hash = ?, warning_count = ?, item_count = ?, unresolved_count = ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ? LIMIT 1',
            [
                $groupIds[0] ?? null,
                (string) ($fit['fit_name'] ?? ''),
                (string) ($fit['ship_name'] ?? ''),
                isset($fit['ship_type_id']) ? (int) $fit['ship_type_id'] : null,
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
        db_doctrine_fit_replace_groups($fitId, $groupIds);

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
            COUNT(DISTINCT dfg.doctrine_group_id) AS group_count,
            COUNT(DISTINCT dfi.id) AS normalized_item_rows,
            GROUP_CONCAT(DISTINCT dg.group_name ORDER BY dg.group_name SEPARATOR "||") AS group_names_csv
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
            foreach ($groupIds as $groupId) {
                $rows[] = ['doctrine_fit_id' => $fitId, 'doctrine_group_id' => $groupId];
            }
        }

        db_bulk_insert_or_upsert('doctrine_fit_groups', ['doctrine_fit_id', 'doctrine_group_id'], $rows);

        $memberships = db_select(
            "SELECT doctrine_fit_id, MIN(doctrine_group_id) AS primary_group_id
             FROM doctrine_fit_groups
             WHERE doctrine_fit_id IN ({$fitPlaceholders})
             GROUP BY doctrine_fit_id",
            $fitIds
        );

        foreach ($memberships as $row) {
            db_execute(
                'UPDATE doctrine_fits SET doctrine_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
                [(int) ($row['primary_group_id'] ?? 0), (int) ($row['doctrine_fit_id'] ?? 0)]
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

        $memberships = db_select(
            "SELECT doctrine_fit_id, MIN(doctrine_group_id) AS primary_group_id
             FROM doctrine_fit_groups
             WHERE doctrine_fit_id IN ({$fitPlaceholders})
             GROUP BY doctrine_fit_id",
            $fitIds
        );
        $primaryByFit = [];
        foreach ($memberships as $row) {
            $primaryByFit[(int) ($row['doctrine_fit_id'] ?? 0)] = (int) ($row['primary_group_id'] ?? 0);
        }

        foreach ($fitIds as $fitId) {
            db_execute(
                'UPDATE doctrine_fits SET doctrine_group_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ? LIMIT 1',
                [($primaryByFit[$fitId] ?? 0) > 0 ? $primaryByFit[$fitId] : null, $fitId]
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
