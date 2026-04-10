<?php

declare(strict_types=1);

/**
 * Shared top navigation for the public proxy pages.
 *
 * Assumes lib/api-client.php and lib/session.php have been required
 * by the calling page. Safe-renders the logged-in pilot if present.
 */

if (!function_exists('proxy_e')) {
    require_once __DIR__ . '/../lib/api-client.php';
}
if (!function_exists('proxy_session_current')) {
    require_once __DIR__ . '/../lib/session.php';
}

$__navSession = proxy_session_current();
$__navCurrent = basename((string) ($_SERVER['SCRIPT_NAME'] ?? ''), '.php');
$__navConfig  = proxy_config();
$__navSite    = proxy_e((string) ($__navConfig['site_name'] ?? 'Battle Reports'));

$__navLinks = [
    ['href' => 'index.php',       'key' => 'index',       'label' => 'Theaters'],
    ['href' => 'recent.php',      'key' => 'recent',      'label' => 'Recent Kills'],
    ['href' => 'leaderboard.php', 'key' => 'leaderboard', 'label' => 'Leaderboard'],
];
?>
<header class="proxy-nav">
    <div class="proxy-nav-inner">
        <a class="proxy-nav-brand" href="index.php">
            <span class="proxy-nav-brand-title"><?= $__navSite ?></span>
        </a>
        <nav class="proxy-nav-links">
            <?php foreach ($__navLinks as $__link): ?>
                <a class="proxy-nav-link <?= $__navCurrent === $__link['key'] ? 'is-active' : '' ?>"
                   href="<?= proxy_e((string) $__link['href']) ?>">
                    <?= proxy_e((string) $__link['label']) ?>
                </a>
            <?php endforeach; ?>
        </nav>
        <form class="proxy-nav-search" method="get" action="search.php">
            <input type="search" name="q" placeholder="Search pilots, corps, alliances, systems…"
                   value="<?= proxy_e((string) ($_GET['q'] ?? '')) ?>"
                   minlength="3" maxlength="100" autocomplete="off">
        </form>
        <div class="proxy-nav-user">
            <?php if ($__navSession !== null): ?>
                <a class="proxy-nav-link" href="character.php?character_id=<?= (int) $__navSession['character_id'] ?>">
                    <img src="https://images.evetech.net/characters/<?= (int) $__navSession['character_id'] ?>/portrait?size=32"
                         alt="" class="proxy-nav-avatar">
                    <span><?= proxy_e((string) $__navSession['character_name']) ?></span>
                </a>
                <a class="proxy-nav-link proxy-nav-logout" href="logout.php">Log out</a>
            <?php else: ?>
                <a class="proxy-nav-link proxy-nav-login" href="login.php">Log in with EVE</a>
            <?php endif; ?>
        </div>
    </div>
</header>
