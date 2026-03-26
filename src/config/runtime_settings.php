<?php

declare(strict_types=1);

return [
    'db' => [
        'title' => 'Database (env/bootstrap)',
        'description' => 'Read-only infrastructure settings loaded from environment variables.',
        'fields' => [
            'db.host' => ['type' => 'string', 'default' => '127.0.0.1', 'editable' => false, 'sensitive' => false, 'env_only' => true, 'database_backed' => false],
            'db.port' => ['type' => 'int', 'default' => 3306, 'editable' => false, 'sensitive' => false, 'env_only' => true, 'database_backed' => false],
            'db.database' => ['type' => 'string', 'default' => 'supplycore', 'editable' => false, 'sensitive' => false, 'env_only' => true, 'database_backed' => false],
            'db.username' => ['type' => 'string', 'default' => 'supplycore', 'editable' => false, 'sensitive' => false, 'env_only' => true, 'database_backed' => false],
            'db.password' => ['type' => 'string', 'default' => '', 'editable' => false, 'sensitive' => true, 'env_only' => true, 'database_backed' => false],
            'db.socket' => ['type' => 'string', 'default' => '', 'editable' => false, 'sensitive' => false, 'env_only' => true, 'database_backed' => false],
        ],
    ],
    'app' => [
        'title' => 'App',
        'description' => 'Core runtime behavior.',
        'fields' => [
            'app.base_url' => ['type' => 'string', 'default' => 'http://localhost:8080', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'app.timezone' => ['type' => 'string', 'default' => 'UTC', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'redis' => [
        'title' => 'Redis',
        'description' => 'Cache and lock settings.',
        'fields' => [
            'redis.enabled' => ['type' => 'bool', 'default' => false, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'redis.host' => ['type' => 'string', 'default' => '127.0.0.1', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'redis.port' => ['type' => 'int', 'default' => 6379, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'redis.database' => ['type' => 'int', 'default' => 0, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'redis.password' => ['type' => 'string', 'default' => '', 'editable' => true, 'sensitive' => true, 'env_only' => false, 'database_backed' => true],
            'redis.prefix' => ['type' => 'string', 'default' => 'supplycore', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'neo4j' => [
        'title' => 'Neo4j',
        'description' => 'Graph intelligence runtime connectivity.',
        'fields' => [
            'neo4j.enabled' => ['type' => 'bool', 'default' => false, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'neo4j.url' => ['type' => 'string', 'default' => 'http://127.0.0.1:7474', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'neo4j.username' => ['type' => 'string', 'default' => 'neo4j', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'neo4j.password' => ['type' => 'string', 'default' => '', 'editable' => true, 'sensitive' => true, 'env_only' => false, 'database_backed' => true],
            'neo4j.database' => ['type' => 'string', 'default' => 'neo4j', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'neo4j.timeout_seconds' => ['type' => 'int', 'default' => 15, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'influxdb' => [
        'title' => 'InfluxDB',
        'description' => 'Rollup export and query integration settings.',
        'fields' => [
            'influxdb.enabled' => ['type' => 'bool', 'default' => false, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'influxdb.read_enabled' => ['type' => 'bool', 'default' => false, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'influxdb.url' => ['type' => 'string', 'default' => 'http://127.0.0.1:8086', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'influxdb.org' => ['type' => 'string', 'default' => '', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'influxdb.bucket' => ['type' => 'string', 'default' => 'supplycore_rollups', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'influxdb.token' => ['type' => 'string', 'default' => '', 'editable' => true, 'sensitive' => true, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'scheduler' => [
        'title' => 'Scheduler',
        'description' => 'Job runtime supervisor behavior.',
        'fields' => [
            'scheduler.default_timeout_seconds' => ['type' => 'int', 'default' => 300, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'scheduler.supervisor_mode' => ['type' => 'string', 'default' => 'php', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'workers' => [
        'title' => 'Workers',
        'description' => 'Queue worker pacing and memory limits.',
        'fields' => [
            'workers.idle_sleep_seconds' => ['type' => 'int', 'default' => 10, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'workers.compute_idle_sleep_seconds' => ['type' => 'int', 'default' => 15, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'workers.sync_idle_sleep_seconds' => ['type' => 'int', 'default' => 8, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'workers.retry_backoff_seconds' => ['type' => 'int', 'default' => 30, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'workers.memory_pause_threshold_bytes' => ['type' => 'int', 'default' => 402653184, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'workers.memory_abort_threshold_bytes' => ['type' => 'int', 'default' => 536870912, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'battle_intelligence' => [
        'title' => 'Battle intelligence',
        'description' => 'Battle intelligence module runtime settings.',
        'fields' => [
            'battle_intelligence.log_file' => ['type' => 'string', 'default' => 'storage/logs/battle-intelligence.log', 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'orchestrator' => [
        'title' => 'Orchestrator',
        'description' => 'Python orchestrator runtime files and controls.',
        'fields' => [
            'orchestrator.health_check_interval_seconds' => ['type' => 'int', 'default' => 15, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'orchestrator.worker_grace_seconds' => ['type' => 'int', 'default' => 45, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
            'orchestrator.worker_start_backoff_seconds' => ['type' => 'int', 'default' => 5, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
    'rebuild' => [
        'title' => 'Rebuild',
        'description' => 'Data-model rebuild runtime controls.',
        'fields' => [
            'rebuild.progress_interval_seconds' => ['type' => 'int', 'default' => 2, 'editable' => true, 'sensitive' => false, 'env_only' => false, 'database_backed' => true],
        ],
    ],
];
