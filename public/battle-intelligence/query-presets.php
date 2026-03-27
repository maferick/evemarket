<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Intelligence Query Presets';
$presets = db_graph_query_presets_active();

$selectedPreset = trim((string) ($_GET['preset'] ?? ''));
$results = [];
if ($selectedPreset !== '') {
    $results = db_graph_query_preset_execute($selectedPreset);
}

include __DIR__ . '/../../src/views/partials/header.php';
?>
<section class="surface-primary">
    <div class="flex items-center justify-between gap-4">
        <div>
            <p class="text-xs uppercase tracking-[0.16em] text-muted">Battle intelligence</p>
            <h1 class="mt-1 text-2xl font-semibold text-slate-50">Query presets</h1>
            <p class="mt-2 text-sm text-muted">Pre-built intelligence queries for rapid analyst drilldowns.</p>
        </div>
        <a href="/battle-intelligence" class="btn-secondary">Back to leaderboard</a>
    </div>

    <div class="mt-5 grid gap-3 md:grid-cols-2 lg:grid-cols-3">
        <?php foreach ($presets as $preset): ?>
            <?php $key = (string) ($preset['preset_key'] ?? ''); ?>
            <a href="?preset=<?= urlencode($key) ?>" class="surface-tertiary block hover:ring-1 hover:ring-accent/40 transition <?= $selectedPreset === $key ? 'ring-1 ring-accent' : '' ?>">
                <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($preset['label'] ?? $key), ENT_QUOTES) ?></p>
                <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($preset['description'] ?? ''), ENT_QUOTES) ?></p>
                <span class="mt-2 inline-block rounded-full bg-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-300"><?= htmlspecialchars((string) ($preset['category'] ?? 'general'), ENT_QUOTES) ?></span>
            </a>
        <?php endforeach; ?>
        <?php if ($presets === []): ?>
            <p class="text-sm text-muted col-span-full">No active presets configured. Run the migration to seed defaults.</p>
        <?php endif; ?>
    </div>

    <?php if ($selectedPreset !== ''): ?>
        <?php
            $activePreset = null;
            foreach ($presets as $p) {
                if (($p['preset_key'] ?? '') === $selectedPreset) {
                    $activePreset = $p;
                    break;
                }
            }
        ?>
        <h2 class="mt-6 text-lg font-semibold text-slate-100"><?= htmlspecialchars((string) ($activePreset['label'] ?? $selectedPreset), ENT_QUOTES) ?> — Results</h2>
        <?php if ($results === []): ?>
            <p class="mt-3 text-sm text-muted">No results returned. The underlying data may not be populated yet.</p>
        <?php else: ?>
            <?php
                $displayCols = [];
                if (!empty($activePreset['display_columns'])) {
                    $decoded = json_decode((string) $activePreset['display_columns'], true);
                    if (is_array($decoded)) {
                        $displayCols = $decoded;
                    }
                }
                if ($displayCols === [] && isset($results[0])) {
                    $displayCols = array_keys($results[0]);
                }
            ?>
            <div class="mt-3 table-shell">
                <table class="table-ui">
                    <thead>
                    <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                        <?php foreach ($displayCols as $col): ?>
                            <th class="px-3 py-2 text-left"><?= htmlspecialchars(str_replace('_', ' ', (string) $col), ENT_QUOTES) ?></th>
                        <?php endforeach; ?>
                    </tr>
                    </thead>
                    <tbody>
                    <?php foreach ($results as $row): ?>
                        <tr class="border-b border-border/50">
                            <?php foreach ($displayCols as $col): ?>
                                <?php $val = $row[$col] ?? ''; ?>
                                <td class="px-3 py-2 text-sm">
                                    <?php if ($col === 'character_name' && isset($row['character_id'])): ?>
                                        <a class="text-accent" href="/battle-intelligence/character.php?character_id=<?= urlencode((string) ((int) $row['character_id'])) ?>"><?= htmlspecialchars((string) $val, ENT_QUOTES) ?></a>
                                    <?php elseif (is_numeric($val)): ?>
                                        <?= htmlspecialchars(number_format((float) $val, 4), ENT_QUOTES) ?>
                                    <?php elseif (is_string($val) && str_starts_with($val, '[')): ?>
                                        <pre class="text-[11px] text-slate-300 max-w-xs overflow-auto"><?= htmlspecialchars($val, ENT_QUOTES) ?></pre>
                                    <?php else: ?>
                                        <?= htmlspecialchars((string) $val, ENT_QUOTES) ?>
                                    <?php endif; ?>
                                </td>
                            <?php endforeach; ?>
                        </tr>
                    <?php endforeach; ?>
                    </tbody>
                </table>
            </div>
        <?php endif; ?>
    <?php endif; ?>
</section>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
