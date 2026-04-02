<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <main class="proxy-main">
        <header class="proxy-header">
            <h1 class="proxy-title"><?= $siteName ?></h1>
            <p class="proxy-subtitle">Theater battle reports are accessed via direct link.</p>
        </header>
        <section class="proxy-card">
            <p class="proxy-muted">To view a battle report, use a URL like:</p>
            <code class="proxy-code">theater?theater_id=YOUR_THEATER_ID</code>
        </section>
    </main>
</div>
</body>
</html>
