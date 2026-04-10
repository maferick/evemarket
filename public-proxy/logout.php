<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

proxy_session_destroy();

header('Location: /');
exit;
