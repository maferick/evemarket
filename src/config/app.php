<?php

declare(strict_types=1);

$config = [
    'app' => [
        'name' => 'SupplyCore',
        'env' => getenv('APP_ENV') ?: 'development',
        'base_url' => 'http://localhost:8080',
        'timezone' => 'UTC',
    ],
    'db' => [
        'host' => getenv('DB_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('DB_PORT') ?: 3306),
        'database' => getenv('DB_DATABASE') ?: 'supplycore',
        'username' => getenv('DB_USERNAME') ?: 'supplycore',
        'password' => getenv('DB_PASSWORD') ?: 'StrongPasswordHere',
        'charset' => 'utf8mb4',
        'socket' => trim((string) (getenv('DB_SOCKET') ?: '')),
    ],
    'redis' => [
        'enabled' => false,
        'host' => '127.0.0.1',
        'port' => 6379,
        'database' => 0,
        'password' => '',
        'prefix' => 'supplycore',
        'connect_timeout' => 1.5,
        'read_timeout' => 1.5,
        'lock_enabled' => true,
    ],
    'neo4j' => [
        'enabled' => false,
        'url' => 'http://127.0.0.1:7474',
        'username' => 'neo4j',
        'password' => '',
        'database' => 'neo4j',
        'timeout_seconds' => 15,
        'log_file' => 'storage/logs/graph-sync.log',
    ],
    'influxdb' => [
        'enabled' => false,
        'read_enabled' => false,
        'url' => 'http://127.0.0.1:8086',
        'org' => '',
        'bucket' => 'supplycore_rollups',
        'token' => '',
        'timeout_seconds' => 15,
        'export_batch_size' => 1000,
        'export_overlap_seconds' => 21600,
        'export_log_file' => 'storage/logs/influx-rollup-export.log',
    ],
    'battle_intelligence' => [
        'log_file' => 'storage/logs/battle-intelligence.log',
    ],
    'scheduler' => [
        'default_timeout_seconds' => 300,
        'supervisor_mode' => 'php',
        'systemd_service' => 'supplycore-scheduler.service',
        'python_service_name' => 'supplycore-orchestrator.service',
    ],
    'orchestrator' => [
        'heartbeat_file' => 'storage/run/orchestrator-heartbeat.json',
        'lock_file' => 'storage/run/orchestrator.lock',
        'state_dir' => 'storage/run',
        'health_check_interval_seconds' => 15,
        'worker_grace_seconds' => 45,
        'worker_start_backoff_seconds' => 5,
        'max_consecutive_health_failures' => 3,
    ],
    'rebuild' => [
        'status_file' => 'storage/run/rebuild-data-model-status.json',
        'lock_file' => 'storage/run/rebuild-data-model.lock',
        'progress_interval_seconds' => 2,
    ],
    'workers' => [
        'queue_name' => 'default',
        'claim_ttl_seconds' => 300,
        'idle_sleep_seconds' => 10,
        'compute_idle_sleep_seconds' => 15,
        'sync_idle_sleep_seconds' => 8,
        'memory_pause_threshold_bytes' => 402653184,
        'memory_abort_threshold_bytes' => 536870912,
        'retry_backoff_seconds' => 30,
        'zkill_stuck_cursor_threshold' => 3,
        'zkill_log_file' => 'storage/logs/zkill.log',
        'worker_log_file' => 'storage/logs/worker.log',
        'compute_log_file' => 'storage/logs/compute.log',
        'pool_state_file' => 'storage/run/worker-pool-heartbeat.json',
        'zkill_state_file' => 'storage/run/zkill-heartbeat.json',
    ],
];

$localConfigPath = __DIR__ . '/local.php';
if (is_file($localConfigPath)) {
    $localConfig = (static function (string $path): mixed {
        return require $path;
    })($localConfigPath);

    if (is_array($localConfig)) {
        $config = array_replace_recursive($config, $localConfig);
    }
}

return $config;
