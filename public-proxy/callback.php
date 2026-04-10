<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';
require_once __DIR__ . '/lib/esi-sso.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

$errorPage = static function (string $message) use ($siteName): void {
    http_response_code(400);
    echo '<!doctype html><html><head><meta charset="UTF-8"><title>Login failed — ' . $siteName . '</title><link rel="stylesheet" href="assets/css/proxy.css"></head><body>';
    echo '<div class="proxy-shell"><main class="proxy-main"><section class="surface-primary" style="max-width: 640px; margin: 2rem auto;">';
    echo '<h1 class="text-2xl font-semibold text-slate-50 text-center">Login failed</h1>';
    echo '<p class="proxy-error mt-3 text-center">' . proxy_e($message) . '</p>';
    echo '<p class="text-sm text-center mt-4"><a class="text-accent" href="login.php">Try again</a></p>';
    echo '</section></main></div></body></html>';
    exit;
};

if (isset($_GET['error'])) {
    $errorPage('ESI returned an error: ' . (string) $_GET['error']);
}

$state = (string) ($_GET['state'] ?? '');
$code  = (string) ($_GET['code']  ?? '');

if ($state === '' || $code === '') {
    $errorPage('Missing state or code in callback.');
}

$stateRow = proxy_oauth_state_consume($state);
if ($stateRow === null) {
    $errorPage('Invalid or expired login state. Please start the login again.');
}

try {
    $payload = proxy_esi_exchange_code($code);
} catch (Throwable $e) {
    $errorPage($e->getMessage());
}

$verify = $payload['verify'];
$characterId   = (int)    ($verify['CharacterID']   ?? 0);
$characterName = (string) ($verify['CharacterName'] ?? '');

if ($characterId <= 0 || $characterName === '') {
    $errorPage('ESI verify response did not include a character ID.');
}

// Enrich with corp/alliance from public ESI
$public = proxy_esi_fetch_public_character($characterId);
$corporationId = (int) ($public['corporation_id'] ?? 0);
$allianceId    = (int) ($public['alliance_id']    ?? 0);

// Optional: fetch corp/alliance names via SupplyCore if we can — but keep
// this simple. The character page will pull names fresh.
$corporationName = '';
$allianceName    = '';
if ($corporationId > 0) {
    $ch = curl_init('https://esi.evetech.net/latest/corporations/' . $corporationId . '/?datasource=tranquility');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 5,
        CURLOPT_CONNECTTIMEOUT => 3,
        CURLOPT_HTTPHEADER     => ['Accept: application/json', 'User-Agent: SupplyCore-Proxy/1.0'],
    ]);
    $body = curl_exec($ch);
    curl_close($ch);
    $decoded = $body !== false ? json_decode((string) $body, true) : null;
    if (is_array($decoded)) {
        $corporationName = (string) ($decoded['name'] ?? '');
    }
}
if ($allianceId > 0) {
    $ch = curl_init('https://esi.evetech.net/latest/alliances/' . $allianceId . '/?datasource=tranquility');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_TIMEOUT        => 5,
        CURLOPT_CONNECTTIMEOUT => 3,
        CURLOPT_HTTPHEADER     => ['Accept: application/json', 'User-Agent: SupplyCore-Proxy/1.0'],
    ]);
    $body = curl_exec($ch);
    curl_close($ch);
    $decoded = $body !== false ? json_decode((string) $body, true) : null;
    if (is_array($decoded)) {
        $allianceName = (string) ($decoded['name'] ?? '');
    }
}

$profile = [
    'character_id'     => $characterId,
    'character_name'   => $characterName,
    'corporation_id'   => $corporationId,
    'corporation_name' => $corporationName,
    'alliance_id'      => $allianceId,
    'alliance_name'    => $allianceName,
];

try {
    proxy_session_create($profile);
} catch (Throwable $e) {
    $errorPage('Could not create session: ' . $e->getMessage());
}

$returnTo = (string) ($stateRow['return_to'] ?? '/');
if (!str_starts_with($returnTo, '/')) {
    $returnTo = '/';
}

header('Location: character.php?character_id=' . $characterId);
exit;
