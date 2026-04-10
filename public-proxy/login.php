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
        <section class="surface-primary proxy-login-card">
            <p class="proxy-login-kicker">Pilot Access</p>
            <h1 class="text-2xl font-semibold text-slate-50 text-center mt-2">Connect with EVE Online</h1>
            <p class="text-sm text-muted text-center mt-2">
                Sign in securely to unlock your personalized battle intelligence profile.
            </p>

            <?php if ($error !== null): ?>
                <p class="proxy-error mt-4"><?= proxy_e($error) ?></p>
            <?php endif; ?>

            <form method="post" class="proxy-login-form">
                <input type="hidden" name="consent" value="yes">
                <input type="hidden" name="return_to" value="<?= proxy_e((string) ($_GET['return_to'] ?? '/')) ?>">
                <button type="submit" class="btn-esi">
                    <span class="icon" aria-hidden="true">🚀</span>
                    Log in with EVE Online
                </button>
            </form>
        </section>
    </main>
</div>
</body>
</html>
