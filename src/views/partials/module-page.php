<?php

$summary = is_array($summary ?? null) ? $summary : [];
$tableColumns = is_array($tableColumns ?? null) ? $tableColumns : [];
$tableRows = is_array($tableRows ?? null) ? $tableRows : [];
$emptyMessage = is_string($emptyMessage ?? null) ? $emptyMessage : 'No records available yet.';
?>
<?php if ($summary !== []): ?>
    <section class="grid gap-4 md:grid-cols-3">
        <?php foreach ($summary as $card): ?>
            <article class="rounded-xl border border-border bg-card p-5 shadow-lg shadow-black/20">
                <p class="text-xs uppercase tracking-[0.2em] text-muted"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-2xl font-semibold"><?= htmlspecialchars((string) ($card['value'] ?? ''), ENT_QUOTES) ?></p>
                <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
            </article>
        <?php endforeach; ?>
    </section>
<?php endif; ?>

<section class="mt-6 rounded-xl border border-border bg-card p-6">
    <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-border text-sm">
            <thead>
            <tr class="text-left text-xs uppercase tracking-[0.15em] text-muted">
                <?php foreach ($tableColumns as $column): ?>
                    <th class="px-3 py-2 font-medium"><?= htmlspecialchars((string) $column, ENT_QUOTES) ?></th>
                <?php endforeach; ?>
            </tr>
            </thead>
            <tbody class="divide-y divide-border/70">
            <?php if ($tableRows === []): ?>
                <tr>
                    <td class="px-3 py-6 text-muted" colspan="<?= max(1, count($tableColumns)) ?>"><?= htmlspecialchars($emptyMessage, ENT_QUOTES) ?></td>
                </tr>
            <?php else: ?>
                <?php foreach ($tableRows as $row): ?>
                    <tr class="text-slate-200">
                        <?php foreach (array_keys($tableColumns) as $key): ?>
                            <td class="px-3 py-3"><?= htmlspecialchars((string) ($row[$key] ?? ''), ENT_QUOTES) ?></td>
                        <?php endforeach; ?>
                    </tr>
                <?php endforeach; ?>
            <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>
