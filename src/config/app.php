<?php

declare(strict_types=1);

return [
    'app' => [
        'name' => 'EveMarket',
        'env' => getenv('APP_ENV') ?: 'development',
        'base_url' => getenv('APP_BASE_URL') ?: 'http://localhost:8080',
        'timezone' => getenv('APP_TIMEZONE') ?: 'UTC',
    ],
    'db' => [
        'host' => getenv('DB_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('DB_PORT') ?: 3306),
        'database' => getenv('DB_DATABASE') ?: 'evemarket',
        'username' => getenv('DB_USERNAME') ?: 'root',
        'password' => getenv('DB_PASSWORD') ?: '',
        'charset' => 'utf8mb4',
    ],
];
