<?php

declare(strict_types=1);

function supplycore_redis_setting(string $key, mixed $default = null): mixed
{
    if (function_exists('get_setting')) {
        return get_setting($key, $default);
    }

    return $default;
}

function supplycore_redis_config(): array
{
    $prefix = trim((string) supplycore_redis_setting('redis_prefix', (string) config('redis.prefix', 'supplycore')));

    return [
        'enabled' => ((string) supplycore_redis_setting('redis_cache_enabled', config('redis.enabled', false) ? '1' : '0')) === '1',
        'lock_enabled' => ((string) supplycore_redis_setting('redis_locking_enabled', config('redis.lock_enabled', true) ? '1' : '0')) === '1',
        'host' => trim((string) supplycore_redis_setting('redis_host', (string) config('redis.host', '127.0.0.1'))),
        'port' => max(1, min(65535, (int) supplycore_redis_setting('redis_port', (string) config('redis.port', 6379)))),
        'database' => max(0, min(15, (int) supplycore_redis_setting('redis_database', (string) config('redis.database', 0)))),
        'password' => (string) supplycore_redis_setting('redis_password', (string) config('redis.password', '')),
        'prefix' => $prefix !== '' ? $prefix : 'supplycore',
        'connect_timeout' => max(0.1, (float) config('redis.connect_timeout', 1.5)),
        'read_timeout' => max(0.1, (float) config('redis.read_timeout', 1.5)),
    ];
}

function supplycore_redis_enabled(): bool
{
    $config = supplycore_redis_config();

    return $config['enabled'] === true;
}

function supplycore_redis_locking_enabled(): bool
{
    $config = supplycore_redis_config();

    return $config['enabled'] === true && $config['lock_enabled'] === true;
}

function supplycore_redis_prefixed_key(string $key): string
{
    $config = supplycore_redis_config();

    return $config['prefix'] . ':' . ltrim($key, ':');
}

function supplycore_redis_max_payload_bytes(): int
{
    $defaultLimit = 64 * 1024 * 1024;
    $hardLimit = (512 * 1024 * 1024) - 1024;
    $configured = (int) config('redis.max_payload_bytes', $defaultLimit);

    return max(1024, min($hardLimit, $configured));
}

function supplycore_redis_value_fits_limits(string $value): bool
{
    return strlen($value) <= supplycore_redis_max_payload_bytes();
}

function supplycore_redis_disconnect(): void
{
    static $socket = null;
    static $signature = null;

    if (is_resource($socket)) {
        fclose($socket);
    }

    $socket = null;
    $signature = null;
}

function supplycore_redis_socket()
{
    static $socket = null;
    static $signature = null;

    $config = supplycore_redis_config();
    if ($config['enabled'] !== true) {
        return null;
    }

    $nextSignature = md5(json_encode([
        $config['host'],
        $config['port'],
        $config['database'],
        $config['password'],
        $config['connect_timeout'],
        $config['read_timeout'],
    ], JSON_THROW_ON_ERROR));

    if (is_resource($socket) && $signature === $nextSignature) {
        return $socket;
    }

    if (is_resource($socket)) {
        fclose($socket);
    }

    $socket = @stream_socket_client(
        sprintf('tcp://%s:%d', $config['host'], $config['port']),
        $errorCode,
        $errorMessage,
        $config['connect_timeout']
    );

    if (!is_resource($socket)) {
        $socket = null;
        $signature = null;

        return null;
    }

    stream_set_timeout($socket, (int) $config['read_timeout'], (int) (($config['read_timeout'] - floor($config['read_timeout'])) * 1_000_000));

    $signature = $nextSignature;

    try {
        if ($config['password'] !== '') {
            $authResult = supplycore_redis_command_raw($socket, ['AUTH', $config['password']]);
            if ($authResult !== 'OK') {
                throw new RuntimeException('Redis AUTH failed.');
            }
        }

        if ($config['database'] > 0) {
            $selectResult = supplycore_redis_command_raw($socket, ['SELECT', (string) $config['database']]);
            if ($selectResult !== 'OK') {
                throw new RuntimeException('Redis SELECT failed.');
            }
        }
    } catch (Throwable) {
        fclose($socket);
        $socket = null;
        $signature = null;

        return null;
    }

    return $socket;
}

function supplycore_redis_command(array $parts): mixed
{
    $socket = supplycore_redis_socket();
    if (!is_resource($socket)) {
        return null;
    }

    try {
        return supplycore_redis_command_raw($socket, $parts);
    } catch (Throwable) {
        supplycore_redis_disconnect();

        return null;
    }
}

function supplycore_redis_command_raw($socket, array $parts): mixed
{
    supplycore_redis_write_all($socket, '*' . count($parts) . "\r\n");

    foreach ($parts as $part) {
        $chunk = (string) $part;
        supplycore_redis_write_all($socket, '$' . strlen($chunk) . "\r\n");
        supplycore_redis_write_all($socket, $chunk);
        supplycore_redis_write_all($socket, "\r\n");
    }

    return supplycore_redis_read_response($socket);
}

function supplycore_redis_write_all($socket, string $payload): void
{
    $totalBytes = strlen($payload);
    $writtenBytes = 0;

    while ($writtenBytes < $totalBytes) {
        $chunk = substr($payload, $writtenBytes);
        $written = @fwrite($socket, $chunk);
        if ($written === false || $written <= 0) {
            throw new RuntimeException('Failed to write Redis command.');
        }

        $writtenBytes += $written;
    }
}

function supplycore_redis_read_response($socket): mixed
{
    $line = fgets($socket);
    if ($line === false) {
        throw new RuntimeException('Failed to read Redis response.');
    }

    $prefix = $line[0] ?? '';
    $payload = rtrim(substr($line, 1), "\r\n");

    return match ($prefix) {
        '+' => $payload,
        '-' => throw new RuntimeException($payload !== '' ? $payload : 'Redis command failed.'),
        ':' => (int) $payload,
        '$' => supplycore_redis_read_bulk_string($socket, (int) $payload),
        '*' => supplycore_redis_read_array($socket, (int) $payload),
        default => throw new RuntimeException('Unsupported Redis response type: ' . $prefix),
    };
}

function supplycore_redis_read_bulk_string($socket, int $length): ?string
{
    if ($length < 0) {
        return null;
    }

    $remaining = $length + 2;
    $data = '';

    while (strlen($data) < $remaining) {
        $chunk = fread($socket, $remaining - strlen($data));
        if ($chunk === false || $chunk === '') {
            throw new RuntimeException('Failed to read Redis bulk string.');
        }

        $data .= $chunk;
    }

    return substr($data, 0, $length);
}

function supplycore_redis_read_array($socket, int $length): ?array
{
    if ($length < 0) {
        return null;
    }

    $items = [];
    for ($index = 0; $index < $length; $index++) {
        $items[] = supplycore_redis_read_response($socket);
    }

    return $items;
}

function supplycore_redis_get(string $key): ?string
{
    $result = supplycore_redis_command(['GET', supplycore_redis_prefixed_key($key)]);

    return is_string($result) ? $result : null;
}

function supplycore_redis_set(string $key, string $value, int $ttlSeconds): bool
{
    if (!supplycore_redis_value_fits_limits($value)) {
        return false;
    }

    $safeTtl = max(1, $ttlSeconds);
    $result = supplycore_redis_command(['SET', supplycore_redis_prefixed_key($key), $value, 'EX', (string) $safeTtl]);

    return $result === 'OK';
}

function supplycore_redis_get_json(string $key): ?array
{
    $payload = supplycore_redis_get($key);
    if ($payload === null || $payload === '') {
        return null;
    }

    $decoded = json_decode($payload, true);

    return is_array($decoded) ? $decoded : null;
}

function supplycore_redis_set_json(string $key, array $value, int $ttlSeconds): bool
{
    $json = json_encode($value, JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE);
    if (!is_string($json) || $json === '') {
        return false;
    }

    return supplycore_redis_set($key, $json, $ttlSeconds);
}

function supplycore_redis_set_nx(string $key, string $value, int $ttlSeconds): bool
{
    if (!supplycore_redis_value_fits_limits($value)) {
        return false;
    }

    $safeTtl = max(1, $ttlSeconds);
    $result = supplycore_redis_command(['SET', supplycore_redis_prefixed_key($key), $value, 'EX', (string) $safeTtl, 'NX']);

    return $result === 'OK';
}

function supplycore_redis_del(array $keys): int
{
    $normalized = array_values(array_filter(array_map(
        static fn (mixed $key): string => trim((string) $key),
        $keys
    ), static fn (string $key): bool => $key !== ''));

    if ($normalized === []) {
        return 0;
    }

    $result = supplycore_redis_command(array_merge(
        ['DEL'],
        array_map('supplycore_redis_prefixed_key', $normalized)
    ));

    return is_int($result) ? $result : 0;
}

function supplycore_redis_incr(string $key): int
{
    $result = supplycore_redis_command(['INCR', supplycore_redis_prefixed_key($key)]);

    return is_int($result) ? $result : 0;
}

function supplycore_redis_mget(array $keys): array
{
    $normalized = array_values(array_filter(array_map(
        static fn (mixed $key): string => trim((string) $key),
        $keys
    ), static fn (string $key): bool => $key !== ''));

    if ($normalized === []) {
        return [];
    }

    $result = supplycore_redis_command(array_merge(
        ['MGET'],
        array_map('supplycore_redis_prefixed_key', $normalized)
    ));

    return is_array($result) ? $result : [];
}

function supplycore_redis_lock_acquire(string $lockName, int $ttlSeconds = 30): ?string
{
    if (!supplycore_redis_locking_enabled()) {
        return null;
    }

    $token = bin2hex(random_bytes(16));
    $acquired = supplycore_redis_set_nx('lock:' . $lockName, $token, max(1, $ttlSeconds));

    return $acquired ? $token : null;
}

function supplycore_redis_lock_release(string $lockName, string $token): bool
{
    if (!supplycore_redis_locking_enabled() || $token === '') {
        return false;
    }

    $script = 'if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end';
    $result = supplycore_redis_command([
        'EVAL',
        $script,
        '1',
        supplycore_redis_prefixed_key('lock:' . $lockName),
        $token,
    ]);

    return (int) $result === 1;
}
