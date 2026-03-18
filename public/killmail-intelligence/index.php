<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Killmail Loss Overview';
$data = killmail_overview_data();
$summary = $data['summary'] ?? [];
$status = $data['status'] ?? [];
$rows = $data['rows'] ?? [];
$filters = $data['filters'] ?? [];
$pagination = $data['pagination'] ?? [];
$error = $data['error'] ?? null;
$emptyMessage = (string) ($data['empty_message'] ?? 'No killmails available yet.');

$queryParams = $_GET;
$buildPageUrl = static function (int $targetPage) use ($queryParams): string {
    $params = $queryParams;
    $params['page'] = max(1, $targetPage);

    return current_path() . '?' . http_build_query($params);
};

include __DIR__ . '/../../src/views/partials/header.php';
?>
<?php if (is_string($error) && trim($error) !== ''): ?>
    <section class="mb-6 rounded-xl border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
        Killmail overview could not load fully. <?= htmlspecialchars($error, ENT_QUOTES) ?>
    </section>
<?php endif; ?>

<section class="mb-6 flex flex-wrap items-center justify-between gap-4 surface-secondary">
    <div>
        <p class="text-xs uppercase tracking-[0.2em] text-muted">Operational visibility</p>
        <h2 class="mt-1 text-lg font-medium text-slate-50">Tracked victim losses, stored locally</h2>
        <p class="mt-2 max-w-3xl text-sm text-muted">Use this workspace to validate that killmail ingestion is capturing the victim side correctly, inspect stored loss items, and prepare for future loss-vs-market analysis without centering raw database IDs.</p>
    </div>
    <div class="flex flex-wrap gap-2">
        <span class="rounded-full border px-3 py-1 text-xs uppercase tracking-[0.15em] <?= ($status['ingestion_enabled'] ?? false) ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200' : 'border-amber-500/40 bg-amber-500/10 text-amber-100' ?>">
            <?= ($status['ingestion_enabled'] ?? false) ? 'Ingestion enabled' : 'Ingestion disabled' ?>
        </span>
        <span class="rounded-full border border-border bg-black/20 px-3 py-1 text-xs uppercase tracking-[0.15em] text-slate-200">
            Last sync: <?= htmlspecialchars((string) ($status['last_sync_outcome'] ?? 'Unknown'), ENT_QUOTES) ?>
        </span>
    </div>
</section>

<section class="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
    <?php foreach ($summary as $card): ?>
        <article class="surface-secondary">
            <p class="text-xs uppercase tracking-[0.2em] text-muted"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
            <p class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($card['value'] ?? '—'), ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
        </article>
    <?php endforeach; ?>
</section>

<section class="mt-6 grid gap-4 xl:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)]">
    <article class="surface-secondary">
        <div class="flex items-center justify-between gap-3">
            <h2 class="text-base font-medium text-slate-50">Pipeline status</h2>
            <span class="text-xs uppercase tracking-[0.15em] text-muted"><?= htmlspecialchars((string) ($status['sync_status'] ?? 'idle'), ENT_QUOTES) ?></span>
        </div>
        <div class="mt-4 grid gap-3 md:grid-cols-2">
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Cursor position</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($status['current_cursor'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-muted">Current saved stream cursor / last processed sequence if available.</p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Latest run outcome</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($status['last_sync_outcome'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-muted">Source rows <?= number_format((int) ($status['last_run_source_rows'] ?? 0)) ?> · inserted <?= number_format((int) ($status['last_run_written_rows'] ?? 0)) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Last successful sync</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($status['last_sync_relative'] ?? 'Never'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-muted"><?= htmlspecialchars((string) ($status['last_success_at'] ?? '—'), ENT_QUOTES) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Latest stored killmail</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($status['last_ingested_at'] ?? '—'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-muted">Latest uploaded_at <?= htmlspecialchars((string) ($status['last_uploaded_at'] ?? '—'), ENT_QUOTES) ?></p>
            </div>
        </div>
        <?php if ((string) ($status['last_error'] ?? '') !== ''): ?>
            <div class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                Last sync error: <?= htmlspecialchars((string) $status['last_error'], ENT_QUOTES) ?>
            </div>
        <?php endif; ?>
    </article>

    <article class="surface-secondary">
        <div class="flex items-center justify-between gap-3">
            <h2 class="text-base font-medium text-slate-50">Victim tracking context</h2>
            <span class="text-xs text-muted">Loss validation foundation</span>
        </div>
        <div class="mt-4 space-y-3">
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Tracked alliances</p>
                <p class="mt-2 text-xl font-semibold text-slate-50"><?= number_format((int) ($status['tracked_alliance_count'] ?? 0)) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Tracked corporations</p>
                <p class="mt-2 text-xl font-semibold text-slate-50"><?= number_format((int) ($status['tracked_corporation_count'] ?? 0)) ?></p>
            </div>
            <div class="surface-tertiary text-sm text-slate-400">
                Tracked matching is now victim-only. The overview highlights losses suffered by the tracked alliances and corporations, while attacker data is reserved for detail inspection.
            </div>
        </div>
    </article>
</section>

<section class="mt-6 surface-secondary shadow-lg shadow-black/20">
    <form method="get" action="<?= htmlspecialchars(current_path(), ENT_QUOTES) ?>" class="surface-tertiary">
        <div class="grid gap-4 xl:grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)_minmax(0,1fr)_auto_auto] xl:items-end">
            <label class="block text-sm text-muted">
                <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Search stored losses</span>
                <input type="search" name="q" value="<?= htmlspecialchars((string) ($filters['search'] ?? ''), ENT_QUOTES) ?>" placeholder="Search sequence, killmail ID, ship, system, region, or tracked victim labels..." class="w-full field-input">
            </label>
            <label class="block text-sm text-muted">
                <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Victim alliance</span>
                <select name="alliance_id" class="w-full field-select">
                    <?php foreach ((array) ($filters['alliance_options'] ?? []) as $optionValue => $optionLabel): ?>
                        <option value="<?= htmlspecialchars((string) $optionValue, ENT_QUOTES) ?>" <?= (string) $optionValue === (string) ($filters['alliance_id'] ?? 0) ? 'selected' : '' ?>><?= htmlspecialchars((string) $optionLabel, ENT_QUOTES) ?></option>
                    <?php endforeach; ?>
                </select>
            </label>
            <label class="block text-sm text-muted">
                <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Victim corporation</span>
                <select name="corporation_id" class="w-full field-select">
                    <?php foreach ((array) ($filters['corporation_options'] ?? []) as $optionValue => $optionLabel): ?>
                        <option value="<?= htmlspecialchars((string) $optionValue, ENT_QUOTES) ?>" <?= (string) $optionValue === (string) ($filters['corporation_id'] ?? 0) ? 'selected' : '' ?>><?= htmlspecialchars((string) $optionLabel, ENT_QUOTES) ?></option>
                    <?php endforeach; ?>
                </select>
            </label>
            <label class="block text-sm text-muted">
                <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Page size</span>
                <select name="page_size" class="w-full field-select">
                    <?php foreach ((array) ($filters['page_size_options'] ?? [25, 50, 100]) as $option): ?>
                        <?php $sizeValue = max(1, (int) $option); ?>
                        <option value="<?= $sizeValue ?>" <?= $sizeValue === (int) ($pagination['page_size'] ?? 25) ? 'selected' : '' ?>><?= $sizeValue ?></option>
                    <?php endforeach; ?>
                </select>
            </label>
            <label class="flex items-center gap-3 surface-tertiary text-sm text-slate-200">
                <input type="hidden" name="tracked_only" value="0">
                <input type="checkbox" name="tracked_only" value="1" <?= ($filters['tracked_only'] ?? false) ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                <span>Tracked victim losses only</span>
            </label>
        </div>
        <div class="mt-4 flex flex-wrap items-center justify-between gap-3">
            <div class="text-sm text-muted">
                Showing <?= (int) ($pagination['showing_from'] ?? 0) ?>-<?= (int) ($pagination['showing_to'] ?? 0) ?> of <?= number_format((int) ($pagination['total_items'] ?? 0)) ?> stored losses
            </div>
            <div class="flex flex-wrap gap-2">
                <button type="submit" class="btn-primary">Apply filters</button>
                <a href="<?= htmlspecialchars(current_path(), ENT_QUOTES) ?>" class="btn-secondary">Reset</a>
            </div>
        </div>
        <input type="hidden" name="page" value="1">
    </form>

    <div class="mt-5 table-shell">
        <table class="table-ui">
            <thead>
            <tr class="border-b border-border/80 bg-white/[0.03] text-left text-xs uppercase tracking-[0.15em] text-muted">
                <th class="px-3 py-2 font-medium">Loss</th>
                <th class="px-3 py-2 font-medium">Victim</th>
                <th class="px-3 py-2 font-medium">Ship lost</th>
                <th class="px-3 py-2 font-medium">Location</th>
                <th class="px-3 py-2 font-medium">Tracked victim match</th>
                <th class="px-3 py-2 font-medium">Stored locally</th>
                <th class="px-3 py-2 font-medium">Inspect</th>
            </tr>
            </thead>
            <tbody>
            <?php if ($rows === []): ?>
                <tr>
                    <td colspan="7" class="px-4 py-8">
                        <div class="surface-tertiary">
                            <p class="text-base font-medium text-slate-50">No killmails to show yet.</p>
                            <p class="mt-2 text-sm text-muted"><?= htmlspecialchars($emptyMessage, ENT_QUOTES) ?></p>
                        </div>
                    </td>
                </tr>
            <?php else: ?>
                <?php foreach ($rows as $index => $row): ?>
                    <tr class="border-b border-border/60 text-slate-200 transition hover:bg-accent/10 <?= $index % 2 === 1 ? 'bg-white/[0.01]' : '' ?>">
                        <td class="px-3 py-3 align-top">
                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['killmail_time_display'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted">Sequence #<?= htmlspecialchars(number_format((int) ($row['sequence_id'] ?? 0)), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted">Killmail <?= htmlspecialchars((string) ($row['killmail_id'] ?? '—'), ENT_QUOTES) ?></p>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['victim_corporation'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['victim_corporation_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-2 text-xs text-slate-300"><?= htmlspecialchars((string) ($row['victim_alliance'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['victim_alliance_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['ship_type'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['ship_type_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['system'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['system_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-2 text-xs text-slate-300"><?= htmlspecialchars((string) ($row['region'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['region_id_display'] ?? '—'), ENT_QUOTES) ?></p>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= ($row['matched_tracked'] ?? false) ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-200' : 'border-slate-500/40 bg-slate-500/10 text-slate-300' ?>">
                                <?= ($row['matched_tracked'] ?? false) ? 'Tracked victim' : 'Untracked victim' ?>
                            </span>
                            <p class="mt-2 max-w-xs text-xs text-muted"><?= htmlspecialchars((string) ($row['match_context'] ?? ''), ENT_QUOTES) ?></p>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['created_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted">Uploaded <?= htmlspecialchars((string) ($row['uploaded_at_display'] ?? '—'), ENT_QUOTES) ?></p>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <a href="<?= htmlspecialchars((string) ($row['inspect_url'] ?? '#'), ENT_QUOTES) ?>" class="inline-flex items-center btn-primary px-3 py-2">Inspect loss</a>
                        </td>
                    </tr>
                <?php endforeach; ?>
            <?php endif; ?>
            </tbody>
        </table>
    </div>

    <div class="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-muted">
        <p>Page <?= max(1, (int) ($pagination['page'] ?? 1)) ?> / <?= max(1, (int) ($pagination['total_pages'] ?? 1)) ?></p>
        <div class="flex items-center gap-2">
            <a href="<?= htmlspecialchars($buildPageUrl(((int) ($pagination['page'] ?? 1)) - 1), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= ((int) ($pagination['page'] ?? 1)) <= 1 ? 'pointer-events-none opacity-40' : '' ?>">Previous</a>
            <a href="<?= htmlspecialchars($buildPageUrl(((int) ($pagination['page'] ?? 1)) + 1), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= ((int) ($pagination['page'] ?? 1)) >= ((int) ($pagination['total_pages'] ?? 1)) ? 'pointer-events-none opacity-40' : '' ?>">Next</a>
        </div>
    </div>
</section>

<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
