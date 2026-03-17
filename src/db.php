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
    $stmt = db()->prepare($sql);

    return $stmt->execute($params);
}

function db_transaction(callable $callback): mixed
{
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
    $stmt = db()->prepare('DELETE FROM market_orders_history WHERE observed_at < ?');
    $stmt->execute([$cutoffObservedAt]);

    return $stmt->rowCount();
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

function db_market_orders_current_distinct_type_ids(string $sourceType, int $sourceId, int $limit = 500): array
{
    $safeLimit = max(1, min($limit, 5000));
    $rows = db_select(
        "SELECT DISTINCT type_id
         FROM market_orders_current
         WHERE source_type = ? AND source_id = ?
         ORDER BY type_id ASC
         LIMIT {$safeLimit}",
        [$sourceType, $sourceId]
    );

    return array_values(array_map(static fn (array $row): int => (int) ($row['type_id'] ?? 0), $rows));
}

function db_market_history_daily_distinct_type_ids(string $sourceType, int $sourceId, int $limit = 500): array
{
    $safeLimit = max(1, min($limit, 5000));
    $rows = db_select(
        "SELECT DISTINCT type_id
         FROM market_history_daily
         WHERE source_type = ? AND source_id = ?
         ORDER BY type_id ASC
         LIMIT {$safeLimit}",
        [$sourceType, $sourceId]
    );

    return array_values(array_map(static fn (array $row): int => (int) ($row['type_id'] ?? 0), $rows));
}

function db_market_history_daily_recent_window(string $sourceType, int $sourceId, int $days = 8, int $typeLimit = 120): array
{
    $safeDays = max(1, min($days, 60));
    $safeTypeLimit = max(1, min($typeLimit, 500));

    $typeRows = db_select(
        "SELECT type_id
         FROM market_history_daily
         WHERE source_type = ? AND source_id = ?
         GROUP BY type_id
         ORDER BY MAX(trade_date) DESC, SUM(volume) DESC
         LIMIT {$safeTypeLimit}",
        [$sourceType, $sourceId]
    );

    $typeIds = array_values(array_filter(array_map(static fn (array $row): int => (int) ($row['type_id'] ?? 0), $typeRows), static fn (int $typeId): bool => $typeId > 0));
    if ($typeIds === []) {
        return [];
    }

    $placeholders = implode(',', array_fill(0, count($typeIds), '?'));
    $params = array_merge([$sourceType, $sourceId], $typeIds);

    return db_select(
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
        $params
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

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $placeholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
        $typeFilterSql = " AND mhd.type_id IN ({$placeholders})";
        $params = array_merge($params, $normalizedTypeIds);
    }

    return db_select(
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

    return db_select(
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
        [$hubSourceId, $allianceStructureId, $startDate, $endDate, ...array_slice($params, 4)]
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
        $typeFilterSql = " AND moh.type_id IN ({$placeholders})";
        $params = array_merge($params, $normalizedTypeIds);
    }

    return db_select(
        "SELECT
            DATE(moh.observed_at) AS observed_date,
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
           AND DATE(moh.observed_at) BETWEEN ? AND ?{$typeFilterSql}
         GROUP BY DATE(moh.observed_at), moh.type_id, rit.type_name
         ORDER BY observed_date ASC, moh.type_id ASC",
        $params
    );
}

function db_market_orders_current_source_aggregates(string $sourceType, int $sourceId, array $typeIds = []): array
{
    $params = [$sourceType, $sourceId];
    $typeFilterSql = '';

    if ($typeIds !== []) {
        $normalizedTypeIds = array_values(array_unique(array_filter(array_map('intval', $typeIds), static fn (int $typeId): bool => $typeId > 0)));
        if ($normalizedTypeIds === []) {
            return [];
        }

        $typePlaceholders = implode(',', array_fill(0, count($normalizedTypeIds), '?'));
        $typeFilterSql = " AND moc.type_id IN ({$typePlaceholders})";
        $params = array_merge($params, $normalizedTypeIds);
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
           AND moc.source_id = ?{$typeFilterSql}
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

function db_sync_schedule_upsert(string $jobKey, int $enabled, int $intervalSeconds): bool
{
    $normalizedKey = trim($jobKey);
    if ($normalizedKey === '') {
        return false;
    }

    return db_execute(
        'INSERT INTO sync_schedules (job_key, enabled, interval_seconds, next_run_at, last_run_at, last_status, last_error, locked_until)
         VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL)
         ON DUPLICATE KEY UPDATE
            enabled = VALUES(enabled),
            interval_seconds = VALUES(interval_seconds),
            next_run_at = CASE
                WHEN VALUES(enabled) = 1 THEN COALESCE(next_run_at, DATE_ADD(UTC_TIMESTAMP(), INTERVAL VALUES(interval_seconds) SECOND))
                ELSE NULL
            END,
            locked_until = CASE WHEN VALUES(enabled) = 1 THEN locked_until ELSE NULL END,
            updated_at = CURRENT_TIMESTAMP',
        [$normalizedKey, $enabled, $intervalSeconds]
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
    db_transaction(static function (): void {
        db_execute('TRUNCATE TABLE ref_item_types');
        db_execute('TRUNCATE TABLE ref_market_groups');
        db_execute('TRUNCATE TABLE ref_npc_stations');
        db_execute('TRUNCATE TABLE ref_systems');
        db_execute('TRUNCATE TABLE ref_constellations');
        db_execute('TRUNCATE TABLE ref_regions');
    });
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

function db_ref_item_types_bulk_upsert(array $rows, ?int $chunkSize = null): int
{
    return db_bulk_insert_or_upsert('ref_item_types', ['type_id', 'group_id', 'market_group_id', 'type_name', 'description', 'published', 'volume'], $rows, ['group_id', 'market_group_id', 'type_name', 'description', 'published', 'volume'], $chunkSize);
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

function db_killmail_ingestion_status(): array
{
    $state = db_sync_state_get('killmail.r2z2.stream');
    $latest = db_select_one('SELECT MAX(sequence_id) AS max_sequence_id, MAX(uploaded_at) AS max_uploaded_at FROM killmail_events');

    return [
        'state' => $state,
        'max_sequence_id' => isset($latest['max_sequence_id']) ? (int) $latest['max_sequence_id'] : null,
        'max_uploaded_at' => $latest['max_uploaded_at'] ?? null,
    ];
}

function db_killmail_filtered_recent(int $limit = 50): array
{
    $limit = max(1, min(500, $limit));

    return db_select(
        "SELECT e.sequence_id, e.killmail_id, e.killmail_hash, e.uploaded_at, e.killmail_time,
                e.victim_alliance_id, e.victim_corporation_id, e.victim_ship_type_id, e.solar_system_id, e.region_id
         FROM killmail_events e
         WHERE (
            EXISTS (SELECT 1 FROM killmail_tracked_alliances ta WHERE ta.is_active = 1 AND ta.alliance_id = e.victim_alliance_id)
            OR EXISTS (SELECT 1 FROM killmail_tracked_corporations tc WHERE tc.is_active = 1 AND tc.corporation_id = e.victim_corporation_id)
            OR EXISTS (
                SELECT 1 FROM killmail_attackers a
                WHERE a.sequence_id = e.sequence_id
                  AND (
                      EXISTS (SELECT 1 FROM killmail_tracked_alliances ta2 WHERE ta2.is_active = 1 AND ta2.alliance_id = a.alliance_id)
                      OR EXISTS (SELECT 1 FROM killmail_tracked_corporations tc2 WHERE tc2.is_active = 1 AND tc2.corporation_id = a.corporation_id)
                  )
            )
         )
         ORDER BY e.sequence_id DESC
         LIMIT {$limit}"
    );
}
