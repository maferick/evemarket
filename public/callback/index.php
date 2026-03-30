<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

if (($_GET['state'] ?? '') !== ($_SESSION['esi_oauth_state'] ?? null)) {
    flash('success', 'ESI callback rejected due to invalid state.');
    header('Location: /settings?section=automation-sync&subsection=esi-login');
    exit;
}

unset($_SESSION['esi_oauth_state']);

if (isset($_GET['error'])) {
    $error = (string) ($_GET['error'] ?? 'unknown_error');
    flash('success', 'ESI login failed: ' . $error);
    header('Location: /settings?section=automation-sync&subsection=esi-login');
    exit;
}

$code = trim((string) ($_GET['code'] ?? ''));
if ($code === '') {
    flash('success', 'ESI callback did not include an authorization code.');
    header('Location: /settings?section=automation-sync&subsection=esi-login');
    exit;
}

try {
    $payload = esi_exchange_oauth_code($code);
    esi_store_oauth_and_cache($payload['token'], $payload['verify']);
    flash('success', 'ESI login connected successfully for ' . ((string) ($payload['verify']['CharacterName'] ?? 'character')) . '.');
} catch (Throwable $exception) {
    flash('success', 'ESI login failed: ' . $exception->getMessage());
}

header('Location: /settings?section=automation-sync&subsection=esi-login');
exit;
