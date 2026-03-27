<?php

declare(strict_types=1);
require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Theater Intelligence';

// Filters
$regionFilter = isset($_GET['region_id']) ? (string) $_GET['region_id'] : null;
$minAnomaly = isset($_GET['min_anomaly']) ? (float) $_GET['min_anomaly'] : null;
$page = max(1, (int) ($_GET['page'] ?? 1));
$perPage = 50;
$offset = ($page - 1) * $perPage;

$theaters = db_theaters_list($perPage, $offset, $regionFilter, $minAnomaly);

include __DIR__ . '/../../src/views/partials/header.php';
?>

<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Theater Intelligence</h1>
            <p class="mt-2 text-sm text-muted">Multi-system battle theaters grouped by time proximity, system proximity, and participant overlap.</p>
        </div>
        <div class="flex gap-2">
            <a href="/battle-intelligence" class="btn-secondary">Battle Intelligence</a>
        </div>
    </div>
</section>

<section class="surface-primary mt-4">
    <form method="GET" class="flex gap-3 items-end flex-wrap">
        <div>
            <label class="text-xs text-muted block mb-1">Region ID</label>
            <input type="number" name="region_id" value="<?= htmlspecialchars((string) ($regionFilter ?? ''), ENT_QUOTES) ?>"
                   class="w-32 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100" placeholder="Any">
        </div>
        <div>
            <label class="text-xs text-muted block mb-1">Min Anomaly</label>
            <input type="number" name="min_anomaly" step="0.01" min="0" max="1"
                   value="<?= htmlspecialchars((string) ($minAnomaly ?? ''), ENT_QUOTES) ?>"
                   class="w-32 rounded bg-slate-800 border border-border px-2 py-1.5 text-sm text-slate-100" placeholder="0.00">
        </div>
        <button type="submit" class="btn-secondary h-fit">Filter</button>
        <?php if ($regionFilter !== null || $minAnomaly !== null): ?>
            <a href="/theater-intelligence" class="text-sm text-accent">Clear</a>
        <?php endif; ?>
    </form>
</section>

<section class="surface-primary mt-4">
    <div class="table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">Region</th>
                    <th class="px-3 py-2 text-left">Primary System</th>
                    <th class="px-3 py-2 text-right">Battles</th>
                    <th class="px-3 py-2 text-right">Systems</th>
                    <th class="px-3 py-2 text-right">Participants</th>
                    <th class="px-3 py-2 text-right">Kills</th>
                    <th class="px-3 py-2 text-right">ISK</th>
                    <th class="px-3 py-2 text-right">Duration</th>
                    <th class="px-3 py-2 text-right">Anomaly</th>
                    <th class="px-3 py-2 text-left">Start</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
                <?php if ($theaters === []): ?>
                    <tr><td colspan="11" class="px-3 py-6 text-sm text-muted">No theaters found. Run the theater clustering job to generate data.</td></tr>
                <?php else: ?>
                    <?php foreach ($theaters as $t): ?>
                        <?php
                            $durationMin = max(1, (int) ($t['duration_seconds'] ?? 0)) / 60;
                            $anomaly = (float) ($t['anomaly_score'] ?? 0);
                            $anomalyClass = $anomaly >= 0.6 ? 'text-red-400' : ($anomaly >= 0.3 ? 'text-yellow-400' : 'text-slate-300');
                            $battleCount = (int) ($t['battle_count'] ?? 0);
                            $sizeLabel = $battleCount > 5 ? 'bg-red-900/60 text-red-300' : ($battleCount > 2 ? 'bg-orange-900/60 text-orange-300' : 'bg-slate-700 text-slate-300');
                        ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($t['region_name'] ?? 'Unknown'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($t['primary_system_name'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right">
                                <span class="inline-block rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider <?= $sizeLabel ?>">
                                    <?= (int) ($t['battle_count'] ?? 0) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-right"><?= (int) ($t['system_count'] ?? 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($t['participant_count'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($t['total_kills'] ?? 0)) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($t['total_isk'] ?? 0), 0) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format($durationMin, 0) ?>m</td>
                            <td class="px-3 py-2 text-right <?= $anomalyClass ?>"><?= number_format($anomaly, 3) ?></td>
                            <td class="px-3 py-2 text-slate-300 text-xs"><?= htmlspecialchars((string) ($t['start_time'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a class="text-accent text-sm" href="/theater-intelligence/view.php?theater_id=<?= urlencode((string) ($t['theater_id'] ?? '')) ?>">View</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>

    <?php if (count($theaters) >= $perPage): ?>
        <div class="mt-3 flex gap-2 text-sm">
            <?php if ($page > 1): ?>
                <a href="?page=<?= $page - 1 ?><?= $regionFilter !== null ? '&region_id=' . urlencode($regionFilter) : '' ?><?= $minAnomaly !== null ? '&min_anomaly=' . $minAnomaly : '' ?>" class="text-accent">Previous</a>
            <?php endif; ?>
            <a href="?page=<?= $page + 1 ?><?= $regionFilter !== null ? '&region_id=' . urlencode($regionFilter) : '' ?><?= $minAnomaly !== null ? '&min_anomaly=' . $minAnomaly : '' ?>" class="text-accent">Next</a>
        </div>
    <?php endif; ?>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
