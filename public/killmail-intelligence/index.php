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
try {
    $runtimeCard = supplycore_dataset_runtime_status([
        'key' => 'killmail.r2z2.stream',
        'label' => 'Killmail stream',
        'source' => 'killmail',
        'fresh_seconds' => 15 * 60,
        'delayed_seconds' => 45 * 60,
    ]);
} catch (Throwable) {
    $runtimeCard = [
        'key' => 'killmail.r2z2.stream',
        'label' => 'Killmail stream',
        'freshness_state' => 'stale',
        'freshness_label' => 'Stale',
        'freshness_tone' => supplycore_operational_status_view_model('stale')['tone'],
        'last_success_at_raw' => null,
        'last_success_at' => 'Unavailable',
        'last_success_relative' => 'Never',
        'show_latest_failure' => false,
        'latest_failure_message' => null,
        'tracked_alliance_count' => 0,
        'tracked_corporation_count' => 0,
        'running_now' => false,
    ];
}
$lastSyncAt = trim((string) ($runtimeCard['last_success_at_raw'] ?? ($status['last_success_at_raw'] ?? '')));
$freshnessReferenceAt = trim((string) ($runtimeCard['freshness_reference_at_raw'] ?? $lastSyncAt));
$lastSyncTimestamp = $freshnessReferenceAt !== '' ? (strtotime($freshnessReferenceAt) ?: null) : null;
$lastSyncAgeSeconds = $lastSyncTimestamp !== null ? max(0, time() - $lastSyncTimestamp) : null;
$pageFreshness = supplycore_page_freshness_view_model([
    'computed_at' => $freshnessReferenceAt !== '' ? $freshnessReferenceAt : null,
    'freshness_state' => (string) ($runtimeCard['freshness_state'] ?? ($lastSyncAgeSeconds === null ? 'stale' : ($lastSyncAgeSeconds <= 15 * 60 ? 'fresh' : 'stale'))),
    'freshness_label' => (string) ($runtimeCard['freshness_label'] ?? ($lastSyncAt === '' ? 'Awaiting sync' : 'Killmail sync')),
]);
$liveRefreshConfig = supplycore_live_refresh_page_config('killmail_intelligence');
$health = is_array($status['health'] ?? null) ? $status['health'] : [];
$workerNoWriteReason = trim((string) ($health['worker_no_write_reason'] ?? ''));

$queryParams = $_GET;
$buildPageUrl = static function (int $targetPage) use ($queryParams): string {
    $params = $queryParams;
    $params['page'] = max(1, $targetPage);

    return current_path() . '?' . http_build_query($params);
};

include __DIR__ . '/../../src/views/partials/header.php';
if (function_exists('ob_flush')) { @ob_flush(); }
@flush();
?>
<?php if (is_string($error) && trim($error) !== ''): ?>
    <section class="mb-6 rounded-xl border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
        Killmail overview could not load fully. <?= htmlspecialchars($error, ENT_QUOTES) ?>
    </section>
<?php endif; ?>

<!-- ui-section:killmail-overview-summary:start -->
<section class="grid gap-4 md:grid-cols-2 xl:grid-cols-5" data-ui-section="killmail-overview-summary">
    <?php foreach ($summary as $card): ?>
        <article class="surface-secondary">
            <p class="text-xs uppercase tracking-[0.2em] text-muted"><?= htmlspecialchars((string) ($card['label'] ?? ''), ENT_QUOTES) ?></p>
            <p class="mt-2 text-2xl font-semibold text-slate-50"><?= htmlspecialchars((string) ($card['value'] ?? '—'), ENT_QUOTES) ?></p>
            <p class="mt-2 text-sm text-muted"><?= htmlspecialchars((string) ($card['context'] ?? ''), ENT_QUOTES) ?></p>
        </article>
    <?php endforeach; ?>
</section>
<!-- ui-section:killmail-overview-summary:end -->

<?php if (!empty($runtimeCard['show_latest_failure'])): ?>
    <section class="mt-6 rounded-2xl border px-4 py-4 text-sm <?= htmlspecialchars((string) ($health['tone'] ?? 'border-amber-500/40 bg-amber-500/10 text-amber-100'), ENT_QUOTES) ?>">
        <div class="flex flex-wrap items-start justify-between gap-3">
            <div>
                <p class="font-medium text-slate-50">Latest killmail refresh failed</p>
                <p class="mt-1 text-sm opacity-90"><?= htmlspecialchars((string) ($runtimeCard['latest_failure_message'] ?? 'Review the latest worker pass and freshness.'), ENT_QUOTES) ?></p>
            </div>
            <div class="text-xs uppercase tracking-[0.14em] opacity-80">
                Last success <?= htmlspecialchars((string) ($runtimeCard['last_success_at'] ?? 'Unavailable'), ENT_QUOTES) ?>
            </div>
        </div>
    </section>
<?php endif; ?>

<!-- ui-section:killmail-overview-status:start -->
<section class="mt-6 grid gap-4" data-ui-section="killmail-overview-status">
    <article class="surface-secondary">
        <div class="flex flex-wrap items-start justify-between gap-3">
            <div>
                <h2 class="text-base font-medium text-slate-50">Current data freshness</h2>
                <p class="mt-1 text-sm text-muted">Keep the default view focused on the dataset, the last successful refresh, and the latest failure only when the latest run failed.</p>
            </div>
            <div class="flex flex-wrap gap-2">
                <span class="rounded-full border px-3 py-1 text-xs uppercase tracking-[0.15em] <?= htmlspecialchars((string) ($runtimeCard['freshness_tone'] ?? 'border-border bg-black/20 text-slate-200'), ENT_QUOTES) ?>">
                    <?= htmlspecialchars((string) ($runtimeCard['freshness_label'] ?? 'Status unavailable'), ENT_QUOTES) ?>
                </span>
                <span class="rounded-full border border-border bg-black/20 px-3 py-1 text-xs uppercase tracking-[0.15em] text-slate-200">
                    Live updates <?= htmlspecialchars((string) ($liveRefreshConfig === null ? 'Off' : (($pageFreshness['state'] ?? 'stale') === 'stale' ? 'Degraded' : 'On')), ENT_QUOTES) ?>
                </span>
            </div>
        </div>
        <div class="mt-4 grid gap-3 md:grid-cols-4">
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Dataset</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($runtimeCard['label'] ?? 'Killmail stream'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-muted"><?= htmlspecialchars((string) ($runtimeCard['key'] ?? 'killmail.r2z2.stream'), ENT_QUOTES) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Last successful run</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($runtimeCard['last_success_relative'] ?? 'Never'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-muted"><?= htmlspecialchars((string) ($runtimeCard['last_success_at'] ?? '—'), ENT_QUOTES) ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Freshness</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= htmlspecialchars((string) ($runtimeCard['freshness_label'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                <p class="mt-1 text-sm text-muted"><?= !empty($runtimeCard['running_now']) ? 'A worker heartbeat is active right now.' : 'Based on the latest successful ingestion timestamp.' ?></p>
            </div>
            <div class="surface-tertiary">
                <p class="text-xs uppercase tracking-[0.15em] text-muted">Tracked scope</p>
                <p class="mt-2 text-lg font-semibold text-slate-50"><?= number_format((int) ($runtimeCard['tracked_alliance_count'] ?? 0)) ?>A · <?= number_format((int) ($runtimeCard['tracked_corporation_count'] ?? 0)) ?>C</p>
                <p class="mt-1 text-sm text-muted">Losses only surface when tracked victim entities match.</p>
            </div>
        </div>
        <?php if (!empty($runtimeCard['show_latest_failure'])): ?>
            <div class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                <p class="font-medium text-slate-50">Latest failure</p>
                <p class="mt-1"><?= htmlspecialchars((string) ($runtimeCard['latest_failure_message'] ?? 'Review the latest worker pass and freshness.'), ENT_QUOTES) ?></p>
            </div>
        <?php endif; ?>
        <details class="mt-4 rounded-2xl border border-white/8 bg-black/20 p-3">
            <summary class="cursor-pointer list-none text-sm font-medium text-slate-100">Diagnostics</summary>
            <div class="mt-3 grid gap-3 sm:grid-cols-2 text-sm text-slate-300">
                <p><span class="text-slate-500">Worker heartbeat</span><br><span class="mt-1 inline-block text-slate-100"><?= htmlspecialchars((string) ($status['worker_seen_relative'] ?? 'No heartbeat'), ENT_QUOTES) ?></span><br><span class="text-xs text-muted"><?= htmlspecialchars((string) ($status['worker_seen_at'] ?? 'No heartbeat recorded'), ENT_QUOTES) ?></span></p>
                <p><span class="text-slate-500">Latest run outcome</span><br><span class="mt-1 inline-block text-slate-100"><?= htmlspecialchars((string) ($status['last_sync_outcome'] ?? 'Unknown'), ENT_QUOTES) ?></span><br><span class="text-xs text-muted"><?= htmlspecialchars((string) ($status['last_run_finished_at'] ?? 'Unavailable'), ENT_QUOTES) ?></span></p>
                <p><span class="text-slate-500">Worker rows</span><br><span class="mt-1 inline-block text-slate-100">seen <?= number_format((int) ($status['worker_rows_seen'] ?? 0)) ?> · matched <?= number_format((int) ($status['worker_rows_matched'] ?? 0)) ?> · skipped existing <?= number_format((int) ($status['worker_rows_skipped_existing'] ?? 0)) ?> · filtered <?= number_format((int) ($status['worker_rows_filtered_out'] ?? 0)) ?> · attempted <?= number_format((int) ($status['worker_rows_write_attempted'] ?? 0)) ?> · failed <?= number_format((int) ($status['worker_rows_failed'] ?? 0)) ?> · written <?= number_format((int) ($status['worker_rows_written'] ?? 0)) ?></span></p>
                <p><span class="text-slate-500">Cursor movement</span><br><span class="mt-1 inline-block text-slate-100"><?= htmlspecialchars((string) (($status['worker_cursor_before'] ?? '') !== '' ? $status['worker_cursor_before'] : '—'), ENT_QUOTES) ?> → <?= htmlspecialchars((string) (($status['worker_cursor_after'] ?? '') !== '' ? $status['worker_cursor_after'] : ($status['worker_cursor'] ?? '—')), ENT_QUOTES) ?></span><br><span class="text-xs text-muted">Sequences <?= htmlspecialchars((string) (($status['worker_first_sequence_seen'] ?? '') !== '' ? $status['worker_first_sequence_seen'] : '—'), ENT_QUOTES) ?> to <?= htmlspecialchars((string) (($status['worker_last_sequence_seen'] ?? '') !== '' ? $status['worker_last_sequence_seen'] : '—'), ENT_QUOTES) ?></span></p>
                <p><span class="text-slate-500">No-progress guard</span><br><span class="mt-1 inline-block text-slate-100"><?= !empty($status['worker_stuck_detected']) ? 'Stuck condition detected' : 'No stuck condition' ?></span><br><span class="text-xs text-muted">Repeated same cursor <?= number_format((int) ($status['worker_same_cursor_no_progress_count'] ?? 0)) ?> times (threshold <?= number_format((int) ($status['worker_stuck_threshold'] ?? 0)) ?>).</span></p>
                <p><span class="text-slate-500">Operator paths</span><br><span class="mt-1 inline-block text-slate-100 break-all"><?= htmlspecialchars((string) ($status['worker_log_file'] ?? 'Unavailable'), ENT_QUOTES) ?></span><br><span class="text-xs text-muted break-all"><?= htmlspecialchars((string) ($status['worker_state_file'] ?? 'Unavailable'), ENT_QUOTES) ?></span></p>
                <?php if ((string) ($status['worker_outcome_reason'] ?? '') !== ''): ?>
                    <p class="sm:col-span-2"><span class="text-slate-500">Latest worker reason</span><br><span class="mt-1 inline-block text-slate-100"><?= htmlspecialchars((string) ($status['worker_outcome_reason'] ?? ''), ENT_QUOTES) ?></span></p>
                <?php endif; ?>
                <?php if ((string) ($status['worker_no_write_reason'] ?? '') !== ''): ?>
                    <p class="sm:col-span-2"><span class="text-slate-500">Zero-write reason</span><br><span class="mt-1 inline-block text-slate-100"><?= htmlspecialchars((string) ($status['worker_no_write_reason'] ?? ''), ENT_QUOTES) ?></span></p>
                <?php endif; ?>
            </div>
        </details>
    </article>
</section>
<!-- ui-section:killmail-overview-status:end -->

<!-- ui-section:killmail-overview-table:start -->
<section class="mt-6 surface-secondary shadow-lg shadow-black/20" data-ui-section="killmail-overview-table">
    <form method="get" action="<?= htmlspecialchars(current_path(), ENT_QUOTES) ?>" class="surface-tertiary">
        <div class="grid gap-4 xl:grid-cols-[minmax(0,1.5fr)_minmax(0,1fr)_minmax(0,1fr)_auto_auto] xl:items-end">
            <label class="block text-sm text-muted">
                <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Search losses</span>
                <input type="search" name="q" value="<?= htmlspecialchars((string) ($filters['search'] ?? ''), ENT_QUOTES) ?>" placeholder="Search ship, system, region, corporation, alliance, sequence, or killmail..." class="w-full field-input">
            </label>
            <?php
                $selectedAllianceId = (int) ($filters['alliance_id'] ?? 0);
                $selectedAllianceLabel = '';
                if ($selectedAllianceId > 0) {
                    $selectedAllianceLabel = (string) (($filters['alliance_options'] ?? [])[(string) $selectedAllianceId] ?? 'Alliance #' . $selectedAllianceId);
                }
                $selectedCorpId = (int) ($filters['corporation_id'] ?? 0);
                $selectedCorpLabel = '';
                if ($selectedCorpId > 0) {
                    $selectedCorpLabel = (string) (($filters['corporation_options'] ?? [])[(string) $selectedCorpId] ?? 'Corporation #' . $selectedCorpId);
                }
            ?>
            <div class="block text-sm text-muted">
                <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Victim alliance</span>
                <div class="relative" data-autocomplete data-autocomplete-url="/api/search-alliances.php">
                    <input type="hidden" name="alliance_id" value="<?= $selectedAllianceId ?>" data-autocomplete-value>
                    <input type="search" class="w-full field-input" placeholder="Search alliances..." value="<?= htmlspecialchars($selectedAllianceLabel, ENT_QUOTES) ?>" data-autocomplete-input autocomplete="off">
                    <ul class="absolute left-0 right-0 top-full z-30 mt-1 hidden max-h-48 overflow-y-auto rounded-xl border border-white/10 bg-[#0d1422] shadow-lg" data-autocomplete-list></ul>
                </div>
            </div>
            <div class="block text-sm text-muted">
                <span class="mb-1 block text-xs uppercase tracking-[0.18em]">Victim corporation</span>
                <div class="relative" data-autocomplete data-autocomplete-url="/api/search-corporations.php">
                    <input type="hidden" name="corporation_id" value="<?= $selectedCorpId ?>" data-autocomplete-value>
                    <input type="search" class="w-full field-input" placeholder="Search corporations..." value="<?= htmlspecialchars($selectedCorpLabel, ENT_QUOTES) ?>" data-autocomplete-input autocomplete="off">
                    <ul class="absolute left-0 right-0 top-full z-30 mt-1 hidden max-h-48 overflow-y-auto rounded-xl border border-white/10 bg-[#0d1422] shadow-lg" data-autocomplete-list></ul>
                </div>
            </div>
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
                <span>Tracked entity killmails only</span>
            </label>
        </div>
        <div class="mt-4 flex flex-wrap items-center justify-between gap-3">
            <div class="text-sm text-muted">
                Showing <?= (int) ($pagination['showing_from'] ?? 0) ?>-<?= (int) ($pagination['showing_to'] ?? 0) ?> of <?= number_format((int) ($pagination['total_items'] ?? 0)) ?> recorded losses
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
                <th class="px-3 py-2 font-medium">Victim</th>
                <th class="px-3 py-2 font-medium">Final blow</th>
                <th class="px-3 py-2 font-medium">Ship & location</th>
                <th class="px-3 py-2 font-medium">Signals</th>
                <th class="px-3 py-2 font-medium">Value & time</th>
                <th class="px-3 py-2 font-medium">Inspect</th>
            </tr>
            </thead>
            <tbody>
            <?php if ($rows === []): ?>
                <tr>
                    <td colspan="6" class="px-4 py-8">
                        <div class="surface-tertiary">
                            <p class="text-base font-medium text-slate-50">No killmails to show yet.</p>
                            <p class="mt-2 text-sm text-muted"><?= htmlspecialchars($emptyMessage, ENT_QUOTES) ?></p>
                        </div>
                    </td>
                </tr>
            <?php else: ?>
                <?php foreach ($rows as $index => $row): ?>
                    <?php $hasPod = isset($row['pod_kill']); $pod = $hasPod ? $row['pod_kill'] : null; ?>
                    <tr class="border-b border-border/60 text-slate-200 transition hover:bg-accent/10 <?= $index % 2 === 1 ? 'bg-white/[0.01]' : '' ?>">
                        <td class="px-3 py-3 align-top">
                            <div class="flex items-start gap-3">
                                <?php if ((string) ($row['victim_portrait_url'] ?? '') !== ''): ?>
                                    <img src="<?= htmlspecialchars((string) $row['victim_portrait_url'], ENT_QUOTES) ?>" alt="" width="40" height="40" loading="lazy" class="h-10 w-10 rounded-xl object-cover">
                                <?php endif; ?>
                                <div>
                                    <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['victim_character'] ?? '—'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-300"><?= htmlspecialchars((string) ($row['victim_corporation'] ?? '—'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['victim_alliance'] ?? '—'), ENT_QUOTES) ?></p>
                                    <p class="mt-2 text-xs text-muted"><?= htmlspecialchars((string) ($row['killmail_time_display'] ?? '—'), ENT_QUOTES) ?></p>
                                </div>
                            </div>
                            <?php if ($hasPod): ?>
                                <div class="mt-2 border-t border-dashed border-border/30 pt-1.5 text-xs text-slate-400">
                                    + Pod kill (<?= htmlspecialchars((string) ($pod['estimated_value_display'] ?? '—'), ENT_QUOTES) ?>)
                                </div>
                            <?php endif; ?>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <div class="flex items-start gap-3">
                                <?php if ((string) ($row['final_blow_portrait_url'] ?? '') !== ''): ?>
                                    <img src="<?= htmlspecialchars((string) $row['final_blow_portrait_url'], ENT_QUOTES) ?>" alt="" width="40" height="40" loading="lazy" class="h-10 w-10 rounded-xl object-cover">
                                <?php endif; ?>
                                <div>
                                    <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['final_blow_character'] ?? 'Unknown character'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-300"><?= htmlspecialchars((string) ($row['final_blow_corporation'] ?? '—'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['final_blow_alliance'] ?? '—'), ENT_QUOTES) ?></p>
                                </div>
                            </div>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <div class="flex items-start gap-3">
                                <?php if ((string) ($row['ship_icon_url'] ?? '') !== ''): ?>
                                    <img src="<?= htmlspecialchars((string) $row['ship_icon_url'], ENT_QUOTES) ?>" alt="" width="40" height="40" loading="lazy" class="h-10 w-10 rounded-lg bg-black/30 object-contain p-1">
                                <?php endif; ?>
                                <div>
                                    <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['ship_type'] ?? '—'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-slate-300"><?= htmlspecialchars((string) ($row['system'] ?? '—'), ENT_QUOTES) ?></p>
                                    <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($row['region'] ?? '—'), ENT_QUOTES) ?></p>
                                    <?php if ((string) ($row['final_blow_ship'] ?? '') !== '' && (string) ($row['final_blow_ship'] ?? '') !== 'Ship unavailable'): ?>
                                        <p class="mt-2 text-xs text-muted">Final blow ship: <?= htmlspecialchars((string) ($row['final_blow_ship'] ?? '—'), ENT_QUOTES) ?></p>
                                    <?php endif; ?>
                                </div>
                            </div>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <div class="flex max-w-[10rem] flex-wrap gap-1.5">
                                <?php
                                    $flagsDisplay = trim((string) ($row['killmail_flags_display'] ?? ''));
                                    $isDefaultFlags = $flagsDisplay === '' || $flagsDisplay === 'No special flags';
                                    $signalLabel = trim((string) ($row['signal_strength']['label'] ?? 'Light signal'));
                                    $impactLabel = trim((string) ($row['supply_impact']['label'] ?? 'Low supply impact'));
                                    $isDefaultSignal = in_array($signalLabel, ['Light signal', 'Signal'], true);
                                    $isDefaultImpact = in_array($impactLabel, ['Low supply impact', 'Impact'], true);
                                ?>
                                <?php if ($row['matched_tracked'] ?? false): ?>
                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] border-emerald-500/40 bg-emerald-500/10 text-emerald-200" title="Tracked entity">Tracked</span>
                                <?php endif; ?>
                                <?php if (!$isDefaultSignal): ?>
                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= htmlspecialchars((string) ($row['signal_strength']['tone'] ?? 'border-slate-500/40 bg-slate-500/10 text-slate-300'), ENT_QUOTES) ?>">
                                        <?= htmlspecialchars($signalLabel, ENT_QUOTES) ?>
                                    </span>
                                <?php endif; ?>
                                <?php if (!$isDefaultImpact): ?>
                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] <?= htmlspecialchars((string) ($row['supply_impact']['tone'] ?? 'border-slate-500/40 bg-slate-500/10 text-slate-300'), ENT_QUOTES) ?>">
                                        <?= htmlspecialchars($impactLabel, ENT_QUOTES) ?>
                                    </span>
                                <?php endif; ?>
                                <?php if (!$isDefaultFlags): ?>
                                    <span class="rounded-full border px-2 py-0.5 text-[11px] uppercase tracking-[0.08em] border-amber-500/40 bg-amber-500/10 text-amber-200"><?= htmlspecialchars($flagsDisplay, ENT_QUOTES) ?></span>
                                <?php endif; ?>
                                <?php if ($isDefaultFlags && $isDefaultSignal && $isDefaultImpact && !($row['matched_tracked'] ?? false)): ?>
                                    <span class="text-xs text-muted">Routine loss</span>
                                <?php endif; ?>
                            </div>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <p class="font-medium text-slate-50"><?= htmlspecialchars((string) ($row['estimated_value_display'] ?? 'Value unavailable'), ENT_QUOTES) ?></p>
                            <?php if ((string) ($row['final_blow_weapon'] ?? '') !== '' && (string) ($row['final_blow_weapon'] ?? '') !== 'Weapon unavailable'): ?>
                                <p class="mt-1 text-xs text-slate-300"><?= htmlspecialchars((string) ($row['final_blow_weapon'] ?? '—'), ENT_QUOTES) ?></p>
                            <?php endif; ?>
                            <p class="mt-2 text-xs text-muted"><?= supplycore_datetime_html($row['killmail_time'] ?? null) ?></p>
                        </td>
                        <td class="px-3 py-3 align-top">
                            <a href="<?= htmlspecialchars((string) ($row['inspect_url'] ?? '#'), ENT_QUOTES) ?>" class="inline-flex items-center btn-primary px-3 py-2">Inspect loss</a>
                            <?php if ($hasPod): ?>
                                <a href="<?= htmlspecialchars((string) ($pod['inspect_url'] ?? '#'), ENT_QUOTES) ?>" class="mt-2 inline-flex items-center btn-primary px-3 py-2">Inspect pod</a>
                            <?php endif; ?>
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
<!-- ui-section:killmail-overview-table:end -->

<script>
(() => {
    document.querySelectorAll('[data-autocomplete]').forEach(container => {
        const url = container.getAttribute('data-autocomplete-url');
        const input = container.querySelector('[data-autocomplete-input]');
        const hidden = container.querySelector('[data-autocomplete-value]');
        const list = container.querySelector('[data-autocomplete-list]');
        if (!url || !input || !hidden || !list) return;

        let debounce = null;
        let abortCtrl = null;

        function show(results) {
            list.innerHTML = '';
            if (results.length === 0) {
                list.classList.add('hidden');
                return;
            }
            results.forEach(r => {
                const li = document.createElement('li');
                li.textContent = r.label;
                li.className = 'cursor-pointer px-3 py-2 text-sm text-slate-200 hover:bg-sky-500/16';
                li.addEventListener('mousedown', e => {
                    e.preventDefault();
                    hidden.value = r.id;
                    input.value = r.label;
                    list.classList.add('hidden');
                });
                list.appendChild(li);
            });
            list.classList.remove('hidden');
        }

        input.addEventListener('input', () => {
            clearTimeout(debounce);
            const q = input.value.trim();
            if (q === '') {
                hidden.value = '0';
                list.classList.add('hidden');
                return;
            }
            debounce = setTimeout(() => {
                if (abortCtrl) abortCtrl.abort();
                abortCtrl = new AbortController();
                fetch(url + '?q=' + encodeURIComponent(q), { signal: abortCtrl.signal })
                    .then(r => r.json())
                    .then(data => show(data.results || []))
                    .catch(() => {});
            }, 200);
        });

        input.addEventListener('blur', () => {
            setTimeout(() => list.classList.add('hidden'), 150);
        });

        input.addEventListener('focus', () => {
            if (input.value.trim() !== '' && list.children.length > 0) {
                list.classList.remove('hidden');
            }
        });

        // Allow clearing
        input.addEventListener('search', () => {
            if (input.value === '') {
                hidden.value = '0';
                list.classList.add('hidden');
            }
        });
    });
})();
</script>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
