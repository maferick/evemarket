<?php

declare(strict_types=1);

/**
 * Redis-backed session handler.
 *
 * PHP's default (files) session handler holds an exclusive flock() on the
 * session file for the entire request lifetime, which serialises concurrent
 * tabs sharing a session cookie. Storing sessions in Redis instead keeps
 * writes short (a single SET with EX) and removes the kernel-level file lock
 * that was blocking the second tab while the first was still rendering.
 *
 * The handler talks to Redis via the app's existing pure-PHP RESP client in
 * src/cache.php, so it does not require the phpredis extension. If Redis is
 * disabled or unreachable, registration falls back silently to the default
 * files handler so the app still works.
 */
final class SupplycoreRedisSessionHandler implements
    SessionHandlerInterface,
    SessionUpdateTimestampHandlerInterface
{
    private int $ttlSeconds;

    public function __construct()
    {
        $configured = (int) ini_get('session.gc_maxlifetime');
        $this->ttlSeconds = $configured > 0 ? $configured : 1440;
    }

    public function open(string $path, string $name): bool
    {
        return true;
    }

    public function close(): bool
    {
        return true;
    }

    #[\ReturnTypeWillChange]
    public function read(string $id): string
    {
        $value = supplycore_redis_get($this->key($id));

        return is_string($value) ? $value : '';
    }

    public function write(string $id, string $data): bool
    {
        return supplycore_redis_set($this->key($id), $data, $this->ttlSeconds);
    }

    public function destroy(string $id): bool
    {
        supplycore_redis_del([$this->key($id)]);

        return true;
    }

    #[\ReturnTypeWillChange]
    public function gc(int $maxLifetime): int
    {
        // Redis expires keys natively via EX — no sweep required.
        return 0;
    }

    public function validateId(string $id): bool
    {
        // Only accept IDs that already exist in Redis so PHP doesn't
        // recycle one it generated against a store it cannot reach.
        return supplycore_redis_get($this->key($id)) !== null;
    }

    public function updateTimestamp(string $id, string $data): bool
    {
        // Cheapest correct implementation: re-SET with the full TTL.
        return $this->write($id, $data);
    }

    private function key(string $id): string
    {
        return 'sess:' . $id;
    }
}

/**
 * Install the Redis session handler if Redis is enabled, the session storage
 * toggle is on, and the server is actually reachable. Must be called before
 * session_start().
 */
function supplycore_register_session_handler(): void
{
    if (!function_exists('supplycore_redis_enabled') || !supplycore_redis_enabled()) {
        return;
    }

    $sessionToggle = (string) supplycore_redis_setting(
        'redis_session_enabled',
        (bool) config('redis.session_enabled', true) ? '1' : '0'
    );
    if ($sessionToggle !== '1') {
        return;
    }

    // Probe the connection once up-front. If Redis is unreachable at the
    // start of a request, skip registration entirely — PHP will use the
    // default files handler and the request still succeeds.
    if (!is_resource(supplycore_redis_socket())) {
        return;
    }

    session_set_save_handler(new SupplycoreRedisSessionHandler(), true);
}
