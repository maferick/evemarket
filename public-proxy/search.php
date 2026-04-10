<?php

declare(strict_types=1);

require_once __DIR__ . '/lib/api-client.php';
require_once __DIR__ . '/lib/session.php';

$config = proxy_config();
$siteName = proxy_e((string) ($config['site_name'] ?? 'Battle Reports'));

$query = trim((string) ($_GET['q'] ?? ''));
$data = null;
if ($query !== '' && mb_strlen($query) >= 3) {
    $data = proxy_api_get('/api/public/search.php', ['q' => $query]);
}

$results = (array) ($data['results'] ?? []);
$hasError = $data !== null && isset($data['error']) && $data['error'] !== null;

?>
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Search — <?= $siteName ?></title>
    <link rel="stylesheet" href="assets/css/proxy.css">
</head>
<body>
<div class="proxy-shell">
    <?php include __DIR__ . '/partials/_nav.php'; ?>
    <main class="proxy-main">

        <section class="surface-primary">
            <h1 class="text-2xl font-semibold text-slate-50">
                Search<?= $query !== '' ? ': "' . proxy_e($query) . '"' : '' ?>
            </h1>

            <?php if ($query === ''): ?>
                <p class="text-sm text-muted mt-2">Use the search box above to find characters, corporations, alliances, or solar systems.</p>
            <?php elseif (mb_strlen($query) < 3): ?>
                <p class="text-sm text-muted mt-2">Please enter at least 3 characters.</p>
            <?php elseif ($hasError): ?>
                <p class="proxy-error mt-2"><?= proxy_e((string) $data['error']) ?></p>
            <?php else: ?>

                <?php
                $sections = [
                    'characters'   => 'Pilots',
                    'corporations' => 'Corporations',
                    'alliances'    => 'Alliances',
                    'systems'      => 'Systems',
                ];
                $hasAny = false;
                ?>

                <?php foreach ($sections as $key => $label): ?>
                    <?php $list = (array) ($results[$key] ?? []); ?>
                    <?php if ($list === []) continue; ?>
                    <?php $hasAny = true; ?>
                    <div class="mt-4">
                        <h2 class="text-sm font-semibold text-slate-200 mb-2"><?= $label ?> (<?= count($list) ?>)</h2>
                        <ul class="search-list">
                            <?php foreach ($list as $row): ?>
                                <li>
                                    <?php if ($key === 'characters'): ?>
                                        <a class="flex items-center gap-2" href="character.php?character_id=<?= (int) $row['id'] ?>">
                                            <img class="w-6 h-6 rounded-full" src="https://images.evetech.net/characters/<?= (int) $row['id'] ?>/portrait?size=32" alt="">
                                            <span class="text-sm text-slate-100"><?= proxy_e((string) $row['name']) ?></span>
                                        </a>
                                    <?php elseif ($key === 'corporations'): ?>
                                        <a class="flex items-center gap-2" href="https://zkillboard.com/corporation/<?= (int) $row['id'] ?>/" target="_blank" rel="noopener">
                                            <img class="w-6 h-6 rounded" src="https://images.evetech.net/corporations/<?= (int) $row['id'] ?>/logo?size=32" alt="">
                                            <span class="text-sm text-slate-100"><?= proxy_e((string) $row['name']) ?></span>
                                            <span class="text-[11px] text-muted">↗ zKill</span>
                                        </a>
                                    <?php elseif ($key === 'alliances'): ?>
                                        <a class="flex items-center gap-2" href="https://zkillboard.com/alliance/<?= (int) $row['id'] ?>/" target="_blank" rel="noopener">
                                            <img class="w-6 h-6 rounded" src="https://images.evetech.net/alliances/<?= (int) $row['id'] ?>/logo?size=32" alt="">
                                            <span class="text-sm text-slate-100"><?= proxy_e((string) $row['name']) ?></span>
                                            <span class="text-[11px] text-muted">↗ zKill</span>
                                        </a>
                                    <?php else: // systems ?>
                                        <div class="text-sm text-slate-100">
                                            <?= proxy_e((string) $row['name']) ?>
                                            <?php if (($row['region_name'] ?? '') !== ''): ?>
                                                <span class="text-[11px] text-muted">· <?= proxy_e((string) $row['region_name']) ?></span>
                                            <?php endif; ?>
                                        </div>
                                    <?php endif; ?>
                                </li>
                            <?php endforeach; ?>
                        </ul>
                    </div>
                <?php endforeach; ?>

                <?php if (!$hasAny): ?>
                    <p class="text-sm text-muted mt-3">No results found.</p>
                <?php endif; ?>
            <?php endif; ?>
        </section>
    </main>
</div>
</body>
</html>
