<?php

declare(strict_types=1);

$config = [
    'app' => [
        'name' => 'SupplyCore',
        'env' => getenv('APP_ENV') ?: 'development',
        'base_url' => getenv('APP_BASE_URL') ?: 'http://localhost:8080',
        'timezone' => getenv('APP_TIMEZONE') ?: 'UTC',
    ],
    'db' => [
        'host' => getenv('DB_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('DB_PORT') ?: 3306),
        'database' => getenv('DB_DATABASE') ?: 'supplycore',
        'username' => getenv('DB_USERNAME') ?: 'root',
        'password' => getenv('DB_PASSWORD') ?: '',
        'charset' => 'utf8mb4',
        'socket' => trim((string) (getenv('DB_SOCKET') ?: '')),
    ],
    'redis' => [
        'enabled' => (getenv('REDIS_ENABLED') ?: '0') === '1',
        'host' => getenv('REDIS_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('REDIS_PORT') ?: 6379),
        'database' => (int) (getenv('REDIS_DATABASE') ?: 0),
        'password' => getenv('REDIS_PASSWORD') ?: '',
        'prefix' => getenv('REDIS_PREFIX') ?: 'supplycore',
        'connect_timeout' => (float) (getenv('REDIS_CONNECT_TIMEOUT') ?: 1.5),
        'read_timeout' => (float) (getenv('REDIS_READ_TIMEOUT') ?: 1.5),
        'lock_enabled' => (getenv('REDIS_LOCK_ENABLED') ?: '1') === '1',
    ],
    'scheduler' => [
        'default_timeout_seconds' => max(30, (int) (getenv('SCHEDULER_DEFAULT_TIMEOUT_SECONDS') ?: 300)),
        'supervisor_mode' => getenv('SCHEDULER_SUPERVISOR_MODE') ?: 'php',
        'systemd_service' => getenv('SCHEDULER_SYSTEMD_SERVICE') ?: 'supplycore-scheduler.service',
        'python_service_name' => getenv('SCHEDULER_PYTHON_SERVICE_NAME') ?: 'supplycore-orchestrator.service',
        'python_heavy_jobs_enabled' => (getenv('SCHEDULER_PYTHON_HEAVY_JOBS_ENABLED') ?: '1') !== '0',
        'python_php_fallback_enabled' => (getenv('SCHEDULER_PYTHON_PHP_FALLBACK_ENABLED') ?: '1') !== '0',
    ],
    'orchestrator' => [
        'heartbeat_file' => getenv('ORCHESTRATOR_HEARTBEAT_FILE') ?: 'storage/run/orchestrator-heartbeat.json',
        'lock_file' => getenv('ORCHESTRATOR_LOCK_FILE') ?: 'storage/run/orchestrator.lock',
        'state_dir' => getenv('ORCHESTRATOR_STATE_DIR') ?: 'storage/run',
        'health_check_interval_seconds' => max(5, (int) (getenv('ORCHESTRATOR_HEALTH_CHECK_INTERVAL_SECONDS') ?: 15)),
        'worker_grace_seconds' => max(15, (int) (getenv('ORCHESTRATOR_WORKER_GRACE_SECONDS') ?: 45)),
        'worker_start_backoff_seconds' => max(1, (int) (getenv('ORCHESTRATOR_WORKER_START_BACKOFF_SECONDS') ?: 5)),
        'max_consecutive_health_failures' => max(1, (int) (getenv('ORCHESTRATOR_MAX_CONSECUTIVE_HEALTH_FAILURES') ?: 3)),
    ],
    'workers' => [
        'queue_name' => getenv('SUPPLYCORE_WORKER_QUEUE') ?: 'default',
        'claim_ttl_seconds' => max(30, (int) (getenv('SUPPLYCORE_WORKER_CLAIM_TTL_SECONDS') ?: 300)),
        'idle_sleep_seconds' => max(1, (int) (getenv('SUPPLYCORE_WORKER_IDLE_SLEEP_SECONDS') ?: 10)),
        'compute_idle_sleep_seconds' => max(1, (int) (getenv('SUPPLYCORE_COMPUTE_IDLE_SLEEP_SECONDS') ?: 15)),
        'sync_idle_sleep_seconds' => max(1, (int) (getenv('SUPPLYCORE_SYNC_IDLE_SLEEP_SECONDS') ?: 8)),
        'memory_pause_threshold_bytes' => max(134217728, (int) (getenv('SUPPLYCORE_WORKER_MEMORY_PAUSE_THRESHOLD_BYTES') ?: 402653184)),
        'memory_abort_threshold_bytes' => max(268435456, (int) (getenv('SUPPLYCORE_WORKER_MEMORY_ABORT_THRESHOLD_BYTES') ?: 536870912)),
        'retry_backoff_seconds' => max(5, (int) (getenv('SUPPLYCORE_WORKER_RETRY_BACKOFF_SECONDS') ?: 30)),
        'zkill_log_file' => getenv('SUPPLYCORE_ZKILL_LOG_FILE') ?: 'storage/logs/zkill.log',
        'worker_log_file' => getenv('SUPPLYCORE_WORKER_LOG_FILE') ?: 'storage/logs/worker.log',
        'compute_log_file' => getenv('SUPPLYCORE_COMPUTE_LOG_FILE') ?: 'storage/logs/compute.log',
        'pool_state_file' => getenv('SUPPLYCORE_WORKER_POOL_STATE_FILE') ?: 'storage/run/worker-pool-heartbeat.json',
        'zkill_state_file' => getenv('SUPPLYCORE_ZKILL_STATE_FILE') ?: 'storage/run/zkill-heartbeat.json',
    ],
];

/**
 * @return array<string, mixed>|null
 */
function supplycore_load_local_config(string $localConfigPath): ?array
{
    if (!is_file($localConfigPath)) {
        return null;
    }

    try {
        $localConfig = (static function (string $path): mixed {
            return require $path;
        })($localConfigPath);
    } catch (ParseError $exception) {
        throw new RuntimeException(
            sprintf('Invalid PHP syntax in %s: %s', $localConfigPath, $exception->getMessage()),
            previous: $exception,
        );
    }

    if ($localConfig === null) {
        return null;
    }

    if (!is_array($localConfig)) {
        throw new RuntimeException(sprintf('Local config file at %s must return an array.', $localConfigPath));
    }

    return $localConfig;
}

$localConfigPath = __DIR__ . '/local.php';
$localConfig = supplycore_load_local_config($localConfigPath);
if (is_array($localConfig)) {
    $config = array_replace_recursive($config, $localConfig);
}

return $config;
