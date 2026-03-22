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
];

$localConfigPath = __DIR__ . '/local.php';
if (is_file($localConfigPath)) {
    $localConfig = require $localConfigPath;
    if (is_array($localConfig)) {
        $config = array_replace_recursive($config, $localConfig);
    }
}

return $config;
