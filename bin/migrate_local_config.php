<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

$path = $argv[1] ?? (__DIR__ . '/../src/config/local.php');
$result = migrate_local_config_to_app_settings($path);

if (($result['ok'] ?? false) !== true) {
    fwrite(STDERR, (string) ($result['message'] ?? 'Migration failed') . PHP_EOL);
    exit(1);
}

fwrite(STDOUT, sprintf("%s Imported=%d\n", (string) ($result['message'] ?? 'Migration complete'), (int) ($result['imported'] ?? 0)));
