<?php

declare(strict_types=1);

return [
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
    'ollama' => [
        'enabled' => (getenv('OLLAMA_ENABLED') ?: '0') === '1',
        'url' => rtrim(getenv('OLLAMA_URL') ?: 'http://localhost:11434/api', '/'),
        'model' => getenv('OLLAMA_MODEL') ?: 'qwen2.5:1.5b-instruct',
        'timeout' => max(1, (int) (getenv('OLLAMA_TIMEOUT') ?: 20)),
    ],
    'scheduler' => [
        'default_timeout_seconds' => max(30, (int) (getenv('SCHEDULER_DEFAULT_TIMEOUT_SECONDS') ?: 300)),
    ],
];
