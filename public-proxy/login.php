<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';
require_once __DIR__ . '/lib/esi-sso.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

$session = proxy_session_current();
if ($session !== null) {
    header('Location: character.php?character_id=' . (int) $session['character_id']);
    exit;
}

$error = null;

// If POST with consent → start the real SSO flow
if ($_SERVER['REQUEST_METHOD'] === 'POST' && ($_POST['consent'] ?? '') === 'yes') {
    try {
        $returnTo = (string) ($_POST['return_to'] ?? '/');
        if (!str_starts_with($returnTo, '/')) {
            $returnTo = '/';
        }
        $state = proxy_oauth_state_create($returnTo);
        header('Location: ' . proxy_esi_authorize_url($state));
        exit;
    } catch (Throwable $e) {
        $error = $e->getMessage();
    }
}

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Login — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">
        <section class="surface-primary" style="max-width: 640px; margin: 2rem auto;">
            <h1 class="text-2xl font-semibold text-slate-50 text-center">Log in with EVE Online</h1>
            <p class="text-sm text-muted text-center mt-2">
                Sign in with your EVE character to view your personal killboard, kill history, and statistics.
            </p>

            <?php if ($error !== null): ?>
                <p class="proxy-error mt-4"><?= proxy_e($error) ?></p>
            <?php endif; ?>

            <div class="surface-tertiary mt-4" style="line-height: 1.6;">
                <p class="text-sm font-semibold text-slate-200">Privacy notice — please read before logging in:</p>
                <ul class="text-xs text-slate-300 mt-2" style="padding-left: 1.25rem; list-style: disc;">
                    <li>We store your EVE <strong>character ID, character name, corporation, and alliance</strong> to display your personal killboard.</li>
                    <li>Your IP address is <strong>hashed (SHA-256 with a secret server-side pepper)</strong> and stored alongside your character. We do not store the raw address.</li>
                    <li>These hashes are used to detect when multiple characters share a connection, e.g. for alt-account identification. The raw IP is not recoverable from the hash.</li>
                    <li>We do not share your data with third parties.</li>
                    <li>You may request deletion of your profile and associated data at any time — contact the site operator.</li>
                    <li>We only request the <code>publicData</code> ESI scope. No wallet, assets, or mail access is requested.</li>
                </ul>
            </div>

            <p class="text-xs text-muted mt-3 text-center">
                By clicking "Log in with EVE Online" you acknowledge the notice above and consent
                to the processing described.
            </p>

            <form method="post" style="margin-top: 1rem; text-align: center;">
                <input type="hidden" name="consent" value="yes">
                <input type="hidden" name="return_to" value="<?= proxy_e((string) ($_GET['return_to'] ?? '/')) ?>">
                <button type="submit" class="btn-esi">
                    Log in with EVE Online
                </button>
            </form>

            <p class="text-xs text-muted mt-4 text-center">
                <a href="privacy.php" class="text-accent">Read the full privacy notice</a>
            </p>
        </section>
    </main>
</div>
</body>
</html>
