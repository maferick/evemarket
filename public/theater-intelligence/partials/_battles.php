<section class="surface-primary mt-4">
    <h2 class="text-lg font-semibold text-slate-50">Constituent Battles</h2>
    <div class="mt-3 table-shell">
        <table class="table-ui">
            <thead>
                <tr class="border-b border-border/70 text-xs uppercase tracking-[0.15em] text-muted">
                    <th class="px-3 py-2 text-left">System</th>
                    <th class="px-3 py-2 text-right">Participants</th>
                    <th class="px-3 py-2 text-left">Size</th>
                    <th class="px-3 py-2 text-left">Start</th>
                    <th class="px-3 py-2 text-left">End</th>
                    <th class="px-3 py-2 text-right">Weight</th>
                    <th class="px-3 py-2 text-right"></th>
                </tr>
            </thead>
            <tbody>
                <?php if ($battles === []): ?>
                    <tr><td colspan="7" class="px-3 py-6 text-sm text-muted">No battles linked.</td></tr>
                <?php else: ?>
                    <?php foreach ($battles as $b): ?>
                        <tr class="border-b border-border/50">
                            <td class="px-3 py-2 text-slate-100"><?= htmlspecialchars((string) ($b['system_name'] ?? '-'), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((int) ($b['participant_count'] ?? 0)) ?></td>
                            <td class="px-3 py-2">
                                <span class="inline-block rounded-full bg-slate-700 px-2 py-0.5 text-[10px] uppercase tracking-wider text-slate-300">
                                    <?= htmlspecialchars((string) ($b['battle_size_class'] ?? ''), ENT_QUOTES) ?>
                                </span>
                            </td>
                            <td class="px-3 py-2 text-slate-300 text-xs"><?= htmlspecialchars((string) ($b['started_at'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-slate-300 text-xs"><?= htmlspecialchars((string) ($b['ended_at'] ?? ''), ENT_QUOTES) ?></td>
                            <td class="px-3 py-2 text-right"><?= number_format((float) ($b['weight'] ?? 0), 2) ?></td>
                            <td class="px-3 py-2 text-right">
                                <a class="text-accent text-sm" href="/battle-intelligence/battle.php?battle_id=<?= urlencode((string) ($b['battle_id'] ?? '')) ?>">Detail</a>
                            </td>
                        </tr>
                    <?php endforeach; ?>
                <?php endif; ?>
            </tbody>
        </table>
    </div>
</section>
