<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));
$contact  = proxy_e((string) ($config['privacy_contact'] ?? ''));
?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Privacy — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">
        <section class="surface-primary" style="max-width: 760px; margin: 2rem auto;">
            <h1 class="text-2xl font-semibold text-slate-50">Privacy Notice</h1>

            <p class="text-sm text-muted mt-2">Last updated: <?= date('Y-m-d') ?></p>

            <h2 class="text-sm font-semibold text-slate-200 mt-4">What we collect</h2>
            <ul class="text-sm text-slate-300 mt-2" style="padding-left: 1.25rem; list-style: disc; line-height: 1.7;">
                <li><strong>Character information.</strong> When you log in with EVE Online SSO we receive your
                    character ID, character name, and (via the public ESI endpoint) your corporation and alliance.
                    We only request the <code>publicData</code> ESI scope.</li>
                <li><strong>Hashed network identifiers.</strong> Every time you load a page while logged in, your
                    IP address and User-Agent are passed through SHA-256 together with a secret server-side pepper.
                    Only the resulting hash is stored. The raw IP address is never written to disk.</li>
                <li><strong>Session cookie.</strong> A random opaque session ID is stored in a cookie named
                    <code>sc_session</code> to keep you logged in for up to 30 days.</li>
            </ul>

            <h2 class="text-sm font-semibold text-slate-200 mt-4">Why we process it</h2>
            <ul class="text-sm text-slate-300 mt-2" style="padding-left: 1.25rem; list-style: disc; line-height: 1.7;">
                <li>To show you your personal killboard and statistics.</li>
                <li>To group characters that appear from the same connection so intelligence analysts on the site
                    operator's team can identify likely alt-accounts.</li>
                <li>To protect the service from abuse.</li>
            </ul>

            <h2 class="text-sm font-semibold text-slate-200 mt-4">Legal basis</h2>
            <p class="text-sm text-slate-300 mt-2">
                Where GDPR, UK DPA 2018, CCPA, or similar data-protection laws apply, we rely on your
                <strong>explicit consent</strong> (given on the login screen) as the legal basis for this processing.
                You can withdraw that consent at any time by logging out and asking for deletion.
            </p>

            <h2 class="text-sm font-semibold text-slate-200 mt-4">Retention</h2>
            <p class="text-sm text-slate-300 mt-2">
                Session records are deleted 30 days after they expire. Character profile and IP-hash rows are
                retained until you request deletion or until they are 12 months inactive, whichever comes first.
            </p>

            <h2 class="text-sm font-semibold text-slate-200 mt-4">Your rights</h2>
            <p class="text-sm text-slate-300 mt-2">
                You can ask us to delete your character profile and all associated hash records. If you are in the
                EU/UK you also have the right to access, correct, and export your data, and to lodge a complaint
                with your local data-protection authority.
            </p>

            <h2 class="text-sm font-semibold text-slate-200 mt-4">Contact</h2>
            <p class="text-sm text-slate-300 mt-2">
                <?= $contact !== '' ? $contact : 'Contact the site operator in-game or on your corporation\'s Discord.' ?>
            </p>

            <h2 class="text-sm font-semibold text-slate-200 mt-4">CCP / EVE Online disclaimer</h2>
            <p class="text-xs text-muted mt-2">
                EVE Online and the EVE logo are the registered trademarks of CCP hf.
                This site is not affiliated with or endorsed by CCP.
            </p>
        </section>
    </main>
</div>
</body>
</html>
