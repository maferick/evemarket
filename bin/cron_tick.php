#!/usr/bin/env php
<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

fwrite(STDOUT, json_encode([
    'event' => 'cron_tick.disabled',
    'ts' => gmdate(DATE_ATOM),
    'status' => 'disabled',
    'message' => 'cron_tick.php no longer schedules work. Run the continuous Python worker pool and dedicated zKill worker services instead.',
], JSON_UNESCAPED_SLASHES) . PHP_EOL);

exit(0);
