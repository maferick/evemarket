<?php
/**
 * Auto-doctrines page.
 *
 * Replaces the hand-maintained /public/doctrine/ pages. Lists every
 * automatically-detected doctrine with loss stats, target fits, and
 * inline controls for hide/pin/runway overrides. No fit editing — the
 * canonical fit is owned by `compute_auto_doctrines.py`.
 */

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

// ─── Handle POST actions (hide/pin/runway) ─────────────────────────────────
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $action = (string) ($_POST['action'] ?? '');
    $doctrineId = (int) ($_POST['doctrine_id'] ?? 0);
    $token = (string) ($_POST['csrf_token'] ?? '');
    if (!validate_csrf($token)) {
        flash('doctrines_error', 'CSRF token mismatch — try again.');
    } elseif ($doctrineId <= 0) {
        flash('doctrines_error', 'Missing doctrine id.');
    } else {
        switch ($action) {
            case 'set_hidden':
                auto_doctrine_set_hidden($doctrineId, (bool) (int) ($_POST['value'] ?? 0));
                flash('doctrines_ok', 'Visibility updated.');
                break;
            case 'set_pinned':
                auto_doctrine_set_pinned($doctrineId, (bool) (int) ($_POST['value'] ?? 0));
                flash('doctrines_ok', 'Pin state updated.');
                break;
            case 'set_runway':
                $daysRaw = trim((string) ($_POST['runway_days'] ?? ''));
                $days = ($daysRaw === '') ? null : (int) $daysRaw;
                if ($days !== null && $days <= 0) {
                    flash('doctrines_error', 'Runway must be a positive integer (or blank to reset).');
                } else {
                    auto_doctrine_set_runway($doctrineId, $days);
                    flash('doctrines_ok', 'Runway updated.');
                }
                break;
            default:
                flash('doctrines_error', 'Unknown action.');
        }
    }
    header('Location: /doctrines/' . (isset($_POST['scroll_to']) ? '#' . urlencode((string) $_POST['scroll_to']) : ''));
    exit;
}

// ─── GET: render the list ──────────────────────────────────────────────────
$title = 'Doctrines';
$includeHidden = isset($_GET['include_hidden']) && $_GET['include_hidden'] === '1';
$doctrines = auto_doctrine_list(['include_hidden' => $includeHidden]);
$settings = auto_doctrine_settings();
$flashOk = flash('doctrines_ok');
$flashErr = flash('doctrines_error');

$activeCount = 0;
$pinnedCount = 0;
$hiddenCount = 0;
$totalLossWindow = 0;
foreach ($doctrines as $d) {
    if ($d['is_active']) { $activeCount++; }
    if ($d['is_pinned']) { $pinnedCount++; }
    if ($d['is_hidden']) { $hiddenCount++; }
    $totalLossWindow += (int) $d['loss_count_window'];
}

$pageHeaderBadge = 'Automatic';
$pageHeaderSummary = sprintf(
    'Doctrines are detected automatically from your killmail losses using %d-day windows and Jaccard ≥ %.2f similarity. Min losses to activate: %d.',
    (int) $settings['window_days'],
    (float) $settings['jaccard_threshold'],
    (int) $settings['min_losses_threshold']
);
$pageHeaderMeta = [
    ['label' => 'Active',       'value' => (string) $activeCount,       'caption' => 'Detected'],
    ['label' => 'Pinned',       'value' => (string) $pinnedCount,       'caption' => 'Always included'],
    ['label' => 'Hidden',       'value' => (string) $hiddenCount,       'caption' => 'Excluded from buy-all'],
    ['label' => 'Losses (win.)', 'value' => (string) $totalLossWindow,   'caption' => sprintf('Last %dd', (int) $settings['window_days'])],
];

include __DIR__ . '/../../src/views/partials/header.php';
?>
<!-- ui-section:doctrines-list:start -->
<section class="surface-primary" data-ui-section="doctrines-list">
    <div class="section-header flex items-center justify-between border-b border-white/8 pb-4">
        <div>
            <h2 class="text-lg font-semibold">Detected doctrines</h2>
            <p class="text-sm text-white/60">Pin critical fits so they always drive buy-all. Hide noisy ones. Adjust runway per doctrine.</p>
        </div>
        <div class="flex items-center gap-3">
            <a href="/doctrines/?include_hidden=<?= $includeHidden ? '0' : '1' ?>" class="text-sm text-cyan-300 hover:text-cyan-200">
                <?= $includeHidden ? 'Hide hidden' : 'Show hidden' ?>
            </a>
            <a href="/buy-all/" class="text-sm text-cyan-300 hover:text-cyan-200">Buy-all →</a>
        </div>
    </div>

    <?php if ($flashOk !== null): ?>
        <div class="mt-4 rounded border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-sm text-emerald-200"><?= htmlspecialchars($flashOk) ?></div>
    <?php endif; ?>
    <?php if ($flashErr !== null): ?>
        <div class="mt-4 rounded border border-rose-500/30 bg-rose-500/10 px-4 py-2 text-sm text-rose-200"><?= htmlspecialchars($flashErr) ?></div>
    <?php endif; ?>

    <?php if (empty($doctrines)): ?>
        <div class="mt-6 rounded border border-white/8 bg-white/3 px-4 py-6 text-center text-sm text-white/60">
            No doctrines detected yet. Once your alliance accumulates at least
            <?= (int) $settings['min_losses_threshold'] ?> similar losses per fit within the last
            <?= (int) $settings['window_days'] ?> days, they'll show up here automatically.
        </div>
    <?php else: ?>
        <div class="mt-4 overflow-x-auto">
            <table class="w-full text-sm">
                <thead class="text-white/60">
                    <tr class="border-b border-white/8">
                        <th class="px-3 py-2 text-left">Hull / Doctrine</th>
                        <th class="px-3 py-2 text-right">Losses (win.)</th>
                        <th class="px-3 py-2 text-right">Daily rate</th>
                        <th class="px-3 py-2 text-right">Target fits</th>
                        <th class="px-3 py-2 text-right">Runway</th>
                        <th class="px-3 py-2 text-right">Priority</th>
                        <th class="px-3 py-2 text-center">Pin</th>
                        <th class="px-3 py-2 text-center">Hide</th>
                    </tr>
                </thead>
                <tbody>
                <?php foreach ($doctrines as $d): ?>
                    <tr class="border-b border-white/5 <?= $d['is_hidden'] ? 'opacity-40' : '' ?>" id="doctrine-<?= (int) $d['id'] ?>">
                        <td class="px-3 py-2">
                            <div class="flex items-center gap-2">
                                <img src="https://images.evetech.net/types/<?= (int) $d['hull_type_id'] ?>/icon?size=32" width="32" height="32" alt="" loading="lazy" class="rounded">
                                <div>
                                    <div class="font-semibold text-white"><?= htmlspecialchars($d['canonical_name']) ?></div>
                                    <div class="text-xs text-white/50">
                                        <?= htmlspecialchars($d['hull_name']) ?>
                                        <?php if ($d['is_pinned']): ?><span class="ml-2 rounded bg-cyan-500/20 px-1.5 py-0.5 text-cyan-200">pinned</span><?php endif; ?>
                                        <?php if (!$d['is_active'] && !$d['is_pinned']): ?><span class="ml-2 rounded bg-white/10 px-1.5 py-0.5 text-white/50">inactive</span><?php endif; ?>
                                    </div>
                                </div>
                            </div>
                        </td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= (int) $d['loss_count_window'] ?></td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= number_format((float) $d['daily_loss_rate'], 2) ?></td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= (int) $d['target_fits'] ?></td>
                        <td class="px-3 py-2 text-right">
                            <form method="post" action="/doctrines/" class="inline-flex items-center gap-1">
                                <input type="hidden" name="action" value="set_runway">
                                <input type="hidden" name="doctrine_id" value="<?= (int) $d['id'] ?>">
                                <input type="hidden" name="csrf_token" value="<?= htmlspecialchars(csrf_token()) ?>">
                                <input type="hidden" name="scroll_to" value="doctrine-<?= (int) $d['id'] ?>">
                                <input type="number" name="runway_days" min="1" max="365"
                                       value="<?= $d['runway_days_override'] !== null ? (int) $d['runway_days_override'] : '' ?>"
                                       placeholder="<?= (int) $d['runway_days_effective'] ?>"
                                       class="w-16 rounded border border-white/10 bg-black/40 px-2 py-1 text-right tabular-nums text-white">
                                <button type="submit" class="rounded bg-white/10 px-2 py-1 text-xs text-white/80 hover:bg-white/20">Set</button>
                            </form>
                        </td>
                        <td class="px-3 py-2 text-right tabular-nums"><?= number_format((float) $d['priority_score'], 1) ?></td>
                        <td class="px-3 py-2 text-center">
                            <form method="post" action="/doctrines/" class="inline">
                                <input type="hidden" name="action" value="set_pinned">
                                <input type="hidden" name="doctrine_id" value="<?= (int) $d['id'] ?>">
                                <input type="hidden" name="value" value="<?= $d['is_pinned'] ? '0' : '1' ?>">
                                <input type="hidden" name="csrf_token" value="<?= htmlspecialchars(csrf_token()) ?>">
                                <input type="hidden" name="scroll_to" value="doctrine-<?= (int) $d['id'] ?>">
                                <button type="submit" class="text-lg" title="<?= $d['is_pinned'] ? 'Unpin' : 'Pin' ?>">
                                    <?= $d['is_pinned'] ? '📌' : '📍' ?>
                                </button>
                            </form>
                        </td>
                        <td class="px-3 py-2 text-center">
                            <form method="post" action="/doctrines/" class="inline">
                                <input type="hidden" name="action" value="set_hidden">
                                <input type="hidden" name="doctrine_id" value="<?= (int) $d['id'] ?>">
                                <input type="hidden" name="value" value="<?= $d['is_hidden'] ? '0' : '1' ?>">
                                <input type="hidden" name="csrf_token" value="<?= htmlspecialchars(csrf_token()) ?>">
                                <input type="hidden" name="scroll_to" value="doctrine-<?= (int) $d['id'] ?>">
                                <button type="submit" class="text-lg" title="<?= $d['is_hidden'] ? 'Unhide' : 'Hide' ?>">
                                    <?= $d['is_hidden'] ? '👁‍🗨' : '👁' ?>
                                </button>
                            </form>
                        </td>
                    </tr>
                    <?php
                    $detail = auto_doctrine_detail((int) $d['id']);
                    $modules = $detail['modules'] ?? [];
                    if (!empty($modules)):
                    ?>
                    <tr class="border-b border-white/5 bg-white/3">
                        <td colspan="8" class="px-3 py-2">
                            <details>
                                <summary class="cursor-pointer text-xs text-white/50 hover:text-white/80">
                                    Core fit (<?= count($modules) ?> modules)
                                </summary>
                                <ul class="mt-2 grid grid-cols-1 gap-1 text-xs text-white/70 md:grid-cols-2">
                                    <?php foreach ($modules as $m): ?>
                                        <li class="flex items-center justify-between gap-2">
                                            <span class="truncate">
                                                <?= (int) $m['quantity'] ?>× <?= htmlspecialchars((string) ($m['type_name'] ?? '')) ?>
                                            </span>
                                            <span class="text-white/40"><?= htmlspecialchars((string) $m['flag_category']) ?> · <?= number_format(((float) $m['observation_frequency']) * 100, 0) ?>%</span>
                                        </li>
                                    <?php endforeach; ?>
                                </ul>
                            </details>
                        </td>
                    </tr>
                    <?php endif; ?>
                <?php endforeach; ?>
                </tbody>
            </table>
        </div>
    <?php endif; ?>
</section>
<!-- ui-section:doctrines-list:end -->
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
