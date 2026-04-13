<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$title = 'Killmail Loss Overview';
$data = killmail_overview_data();
$summary = $data['summary'] ?? [];
$mailTypeBreakdown = $data['mail_type_breakdown'] ?? [];
$status = $data['status'] ?? [];
$rows = $data['rows'] ?? [];
$filters = $data['filters'] ?? [];
$pagination = $data['pagination'] ?? [];
$histogram = $data['histogram'] ?? [];
$coverage = $data['coverage'] ?? [];
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
    // Page-number navigation is only used on the search path; keyset cursors
    // are orthogonal so make sure they don't leak into a page-number URL.
    unset($params['cursor'], $params['dir']);

    return current_path() . '?' . http_build_query($params);
};
$buildCursorUrl = static function (?string $cursor, string $dir) use ($queryParams): string {
    $params = $queryParams;
    if ($cursor === null || $cursor === '') {
        // "First page" shortcut — drop cursor and dir entirely.
        unset($params['cursor'], $params['dir'], $params['page']);
    } else {
        $params['cursor'] = $cursor;
        $params['dir'] = $dir === 'prev' ? 'prev' : 'next';
        unset($params['page']);
    }

    $query = http_build_query($params);
    return current_path() . ($query !== '' ? '?' . $query : '');
};
$overviewHasSearch = trim((string) ($_GET['q'] ?? '')) !== '';

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

<?php if ($mailTypeBreakdown !== []): ?>
    <!-- ui-section:killmail-overview-mail-types:start -->
    <section class="mt-6 surface-secondary" data-ui-section="killmail-overview-mail-types">
        <div class="flex flex-wrap items-start justify-between gap-3">
            <div>
                <h2 class="text-base font-medium text-slate-50">Storage breakdown by mail_type</h2>
                <p class="mt-1 text-sm text-muted">
                    Every R2Z2 stream killmail and every per-character backfill killmail is stored, classified into one of these buckets.
                    <span class="text-slate-100">Tracked and opponent rows are retained indefinitely; <code>untracked</code> rows are pruned after 90 days</span>
                    by the <code>killmail_untracked_retention</code> job, so their count reflects the rolling window not the all-time total.
                </p>
            </div>
        </div>
        <div class="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            <?php foreach ($mailTypeBreakdown as $bucket): ?>
                <article class="rounded-lg border px-3 py-3 <?= htmlspecialchars((string) ($bucket['tone'] ?? 'border-border bg-black/20 text-slate-200'), ENT_QUOTES) ?>">
                    <div class="flex items-baseline justify-between gap-3">
                        <p class="text-xs uppercase tracking-[0.2em]"><?= htmlspecialchars((string) ($bucket['label'] ?? ''), ENT_QUOTES) ?></p>
                        <p class="text-xl font-semibold"><?= number_format((int) ($bucket['count'] ?? 0)) ?></p>
                    </div>
                    <p class="mt-1 text-xs opacity-80"><?= htmlspecialchars((string) ($bucket['description'] ?? ''), ENT_QUOTES) ?></p>
                    <p class="mt-1 font-mono text-[10px] uppercase tracking-[0.2em] opacity-60">mail_type = <?= htmlspecialchars((string) ($bucket['key'] ?? ''), ENT_QUOTES) ?></p>
                </article>
            <?php endforeach; ?>
        </div>
    </section>
    <!-- ui-section:killmail-overview-mail-types:end -->
<?php endif; ?>

<section class="mt-6 surface-secondary">
    <div class="flex flex-wrap items-start justify-between gap-3">
        <div>
            <h2 class="text-base font-medium text-slate-50">Loss histogram</h2>
            <p class="mt-1 text-sm text-muted">Monthly loss counts from <?= htmlspecialchars((string) ($coverage['start_date'] ?? '2024-01-01'), ENT_QUOTES) ?> onward.</p>
        </div>
    </div>
    <?php if ($histogram === []): ?>
        <p class="mt-4 text-sm text-muted">No loss histogram buckets available yet.</p>
    <?php else: ?>
        <div class="mt-4 flex items-stretch gap-2 overflow-x-auto pb-1">
            <?php foreach ($histogram as $bucket):
                $pct = max(0, min(100, (int) ($bucket['percent'] ?? 0)));
                $count = (int) ($bucket['count'] ?? 0);
                $label = (string) ($bucket['label'] ?? '');
            ?>
                <div class="flex min-w-[44px] flex-1 flex-col items-center gap-1 text-xs text-slate-300">
                    <div
                        class="flex w-full flex-col justify-end rounded-t bg-white/5"
                        style="height: 160px;"
                        title="<?= htmlspecialchars($label . ': ' . number_format($count), ENT_QUOTES) ?>"
                    >
                        <div
                            class="w-full rounded-t bg-indigo-400/80"
                            style="height: <?= $pct ?>%;"
                        ></div>
                    </div>
                    <span class="text-[10px] text-slate-100"><?= number_format($count) ?></span>
                    <span class="whitespace-nowrap text-[10px] text-muted"><?= htmlspecialchars($label, ENT_QUOTES) ?></span>
                </div>
            <?php endforeach; ?>
        </div>
    <?php endif; ?>
</section>

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
                <!--
                    ESI coverage panel — lazy-loaded via
                    /api/killmail-intelligence/esi-coverage.php to keep the main
                    page render off the ~200s snapshot query. The initial paint
                    comes from whatever is in page_cache (warm = real numbers,
                    empty = zeros) and the JS fetch in the inline <script> at
                    the bottom of this file overwrites with fresh values. Each
                    number carries a data-esi-coverage-field attribute so the
                    JS can target it without rebuilding the markup.
                -->
                <p class="sm:col-span-2" data-esi-coverage-header>
                    <span class="flex flex-wrap items-center justify-between gap-2">
                        <span class="text-slate-500">ESI coverage</span>
                        <span class="flex items-center gap-2">
                            <span data-esi-coverage-status class="text-xs text-muted">Loading…</span>
                            <button type="button" data-esi-coverage-refresh class="rounded border border-white/10 px-2 py-0.5 text-xs text-slate-200 hover:bg-white/5 disabled:opacity-40" aria-label="Refresh ESI coverage">↻ Refresh</button>
                        </span>
                    </span>
                </p>
                <p><span class="text-slate-500">Participant characters</span><br><span class="mt-1 inline-block text-slate-100" data-esi-coverage-field="participant_characters"><?= number_format((int) ($coverage['participant_characters'] ?? 0)) ?></span><br><span class="text-xs text-muted">Distinct victim + attacker IDs on losses since <?= htmlspecialchars((string) ($coverage['start_date'] ?? '2024-01-01'), ENT_QUOTES) ?>.</span></p>
                <p><span class="text-slate-500">ESI queue coverage</span><br><span class="mt-1 inline-block text-slate-100"><span data-esi-coverage-field="queued_in_esi_character_queue"><?= number_format((int) ($coverage['queued_in_esi_character_queue'] ?? 0)) ?></span> / <span data-esi-coverage-field="participant_characters"><?= number_format((int) ($coverage['participant_characters'] ?? 0)) ?></span></span><br><span class="text-xs text-muted">pending <span data-esi-coverage-field="esi_queue_pending"><?= number_format((int) ($coverage['esi_queue_pending'] ?? 0)) ?></span> · done <span data-esi-coverage-field="esi_queue_done"><?= number_format((int) ($coverage['esi_queue_done'] ?? 0)) ?></span> · error <span data-esi-coverage-field="esi_queue_error"><?= number_format((int) ($coverage['esi_queue_error'] ?? 0)) ?></span></span></p>
                <p><span class="text-slate-500">Current affiliation coverage</span><br><span class="mt-1 inline-block text-slate-100"><span data-esi-coverage-field="with_current_affiliation"><?= number_format((int) ($coverage['with_current_affiliation'] ?? 0)) ?></span> / <span data-esi-coverage-field="participant_characters"><?= number_format((int) ($coverage['participant_characters'] ?? 0)) ?></span></span><br><span class="text-xs text-muted">Participants with ESI current corp/alliance rows.</span></p>
                <p><span class="text-slate-500">History sync processed</span><br><span class="mt-1 inline-block text-slate-100"><span data-esi-coverage-field="history_refresh_completed"><?= number_format((int) ($coverage['history_refresh_completed'] ?? 0)) ?></span> / <span data-esi-coverage-field="participant_characters"><?= number_format((int) ($coverage['participant_characters'] ?? 0)) ?></span></span><br><span class="text-xs text-muted">Participants whose alliance-history sync has run at least once (<code>last_history_refresh_at</code> set).</span></p>
                <p><span class="text-slate-500">Alliance history rows</span><br><span class="mt-1 inline-block text-slate-100"><span data-esi-coverage-field="with_alliance_history_row"><?= number_format((int) ($coverage['with_alliance_history_row'] ?? $coverage['with_alliance_history'] ?? 0)) ?></span> / <span data-esi-coverage-field="participant_characters"><?= number_format((int) ($coverage['participant_characters'] ?? 0)) ?></span></span><br><span class="text-xs text-muted">Participants with at least one historical alliance period. Lower than "history sync processed" is expected: characters who were never in an alliance produce zero rows.</span></p>
                <p class="sm:col-span-2"><span class="text-slate-500">Per-character killmail backfill</span><br><span class="mt-1 inline-block text-slate-100"><span data-esi-coverage-field="enrolled_for_killmail_backfill"><?= number_format((int) ($coverage['enrolled_for_killmail_backfill'] ?? 0)) ?></span> / <span data-esi-coverage-field="participant_characters"><?= number_format((int) ($coverage['participant_characters'] ?? 0)) ?></span> enrolled</span><br><span class="text-xs text-muted">queue: pending <span data-esi-coverage-field="killmail_backfill_pending"><?= number_format((int) ($coverage['killmail_backfill_pending'] ?? 0)) ?></span> · processing <span data-esi-coverage-field="killmail_backfill_processing"><?= number_format((int) ($coverage['killmail_backfill_processing'] ?? 0)) ?></span> · done <span data-esi-coverage-field="killmail_backfill_done"><?= number_format((int) ($coverage['killmail_backfill_done'] ?? 0)) ?></span> · error <span data-esi-coverage-field="killmail_backfill_error"><?= number_format((int) ($coverage['killmail_backfill_error'] ?? 0)) ?></span> · backfill_complete <span data-esi-coverage-field="killmail_backfill_complete_flag"><?= number_format((int) ($coverage['killmail_backfill_complete_flag'] ?? 0)) ?></span>. Passive R2Z2 ingest alone does not guarantee historical coverage — participants must be enrolled in <code>character_killmail_queue</code> for per-character zKB backfill.</span></p>
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
            <?php
                $totalItemsValue = $pagination['total_items'] ?? null;
                $showingFrom = (int) ($pagination['showing_from'] ?? 0);
                $showingTo = (int) ($pagination['showing_to'] ?? 0);
            ?>
            <div class="text-sm text-muted" data-loss-count-label>
                <?php if ($totalItemsValue !== null): ?>
                    Showing <?= $showingFrom ?>-<?= $showingTo ?> of <?= number_format((int) $totalItemsValue) ?> recorded losses
                <?php else: ?>
                    Showing <?= $showingFrom ?>-<?= $showingTo ?> recorded losses <span class="ml-1 text-xs text-slate-500" data-loss-count-suffix>· total counting…</span>
                <?php endif; ?>
            </div>
            <div class="flex flex-wrap items-center gap-2">
                <span data-search-submit-spinner class="hidden items-center gap-2 text-xs text-muted" role="status" aria-live="polite">
                    <svg class="h-4 w-4 animate-spin text-slate-300" viewBox="0 0 24 24" fill="none" aria-hidden="true">
                        <circle cx="12" cy="12" r="10" stroke="currentColor" stroke-width="3" class="opacity-25"></circle>
                        <path d="M4 12a8 8 0 018-8" stroke="currentColor" stroke-width="3" stroke-linecap="round" class="opacity-75"></path>
                    </svg>
                    <span>Searching…</span>
                </span>
                <button type="submit" class="btn-primary" data-search-submit-btn>Apply filters</button>
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

    <?php
        $useKeyset = (bool) ($pagination['use_keyset'] ?? false);
        $paginationNoSearch = !$overviewHasSearch;
        $nextCursor = $pagination['next_cursor'] ?? null;
        $prevCursor = $pagination['prev_cursor'] ?? null;
        $pageNum = max(1, (int) ($pagination['page'] ?? 1));
        $totalPagesValue = $pagination['total_pages'] ?? null;
        $currentCursor = trim((string) ($_GET['cursor'] ?? ''));
    ?>
    <div class="mt-4 flex flex-wrap items-center justify-between gap-3 text-sm text-muted">
        <?php if ($paginationNoSearch): ?>
            <p data-loss-pagination-label>
                <?php if ($useKeyset || $currentCursor !== ''): ?>
                    Cursor view — newest first
                <?php else: ?>
                    Latest losses
                <?php endif; ?>
                <?php if ($totalPagesValue === null): ?>
                    <span class="ml-2 text-xs text-slate-500" data-loss-pagination-total>· total pages: <span data-loss-pagination-total-value>counting…</span></span>
                <?php else: ?>
                    <span class="ml-2 text-xs text-slate-500">· <?= number_format((int) $totalPagesValue) ?> pages total</span>
                <?php endif; ?>
            </p>
        <?php else: ?>
            <p>Page <?= $pageNum ?> / <?= $totalPagesValue !== null ? max(1, (int) $totalPagesValue) : 1 ?></p>
        <?php endif; ?>
        <div class="flex items-center gap-2">
            <?php if ($paginationNoSearch): ?>
                <?php
                    // Keyset navigation. "First" resets to the head view;
                    // Previous/Next use the cursors returned by the DB layer.
                    $isHead = $currentCursor === '';
                    $prevDisabled = $prevCursor === null || $prevCursor === '';
                    $nextDisabled = $nextCursor === null || $nextCursor === '';
                ?>
                <a href="<?= htmlspecialchars($buildCursorUrl(null, 'next'), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= $isHead ? 'pointer-events-none opacity-40' : '' ?>">First</a>
                <a href="<?= htmlspecialchars($buildCursorUrl($prevCursor, 'prev'), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= $prevDisabled ? 'pointer-events-none opacity-40' : '' ?>">Previous</a>
                <a href="<?= htmlspecialchars($buildCursorUrl($nextCursor, 'next'), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= $nextDisabled ? 'pointer-events-none opacity-40' : '' ?>">Next</a>
            <?php else: ?>
                <a href="<?= htmlspecialchars($buildPageUrl($pageNum - 1), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= $pageNum <= 1 ? 'pointer-events-none opacity-40' : '' ?>">Previous</a>
                <a href="<?= htmlspecialchars($buildPageUrl($pageNum + 1), ENT_QUOTES) ?>" class="btn-secondary px-3 py-1.5 <?= ($totalPagesValue !== null && $pageNum >= (int) $totalPagesValue) ? 'pointer-events-none opacity-40' : '' ?>">Next</a>
            <?php endif; ?>
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

    // -----------------------------------------------------------------------
    // ESI coverage panel — lazy loader
    //
    // Keeps the main page off the ~200s ESI coverage snapshot query.
    // Fetches /api/killmail-intelligence/esi-coverage.php on load, populates
    // every [data-esi-coverage-field] span, and surfaces cache age + a
    // refresh button. Handles HTTP 202 (single-flight in progress) by
    // polling until the refresh completes.
    // -----------------------------------------------------------------------
    const coverageEndpoint = '/api/killmail-intelligence/esi-coverage.php';
    const coverageStatusEl = document.querySelector('[data-esi-coverage-status]');
    const coverageRefreshBtn = document.querySelector('[data-esi-coverage-refresh]');
    const coverageFieldEls = document.querySelectorAll('[data-esi-coverage-field]');
    let coveragePollTimer = null;
    let coverageInFlight = false;

    function formatCoverageNumber(value) {
        const n = typeof value === 'number' ? value : parseInt(value, 10);
        return Number.isFinite(n) ? n.toLocaleString() : '—';
    }

    function formatCoverageAge(seconds) {
        if (seconds == null) return 'never';
        if (seconds < 60) return seconds + 's ago';
        if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
        if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
        return Math.floor(seconds / 86400) + 'd ago';
    }

    function setCoverageStatus(text, tone) {
        if (!coverageStatusEl) return;
        coverageStatusEl.textContent = text;
        coverageStatusEl.className = 'text-xs ' + (tone === 'error' ? 'text-amber-300' : (tone === 'fresh' ? 'text-emerald-300' : 'text-muted'));
    }

    function applyCoverageData(coverage) {
        if (!coverage || typeof coverage !== 'object') return;
        coverageFieldEls.forEach(el => {
            const key = el.getAttribute('data-esi-coverage-field');
            if (key && Object.prototype.hasOwnProperty.call(coverage, key)) {
                el.textContent = formatCoverageNumber(coverage[key]);
            }
        });
    }

    function applyCoverageStatusFromResponse(coverage, refreshing) {
        const status = coverage && coverage.cache_status;
        const age = coverage && coverage.cache_age_seconds;
        if (refreshing) {
            setCoverageStatus('Refreshing… (last updated ' + formatCoverageAge(age) + ')', 'muted');
            return;
        }
        if (status === 'empty') {
            setCoverageStatus('No data yet — click ↻ Refresh', 'muted');
            return;
        }
        if (status === 'warm') {
            setCoverageStatus('Updated ' + formatCoverageAge(age), 'fresh');
            return;
        }
        if (status === 'stale') {
            setCoverageStatus('Updated ' + formatCoverageAge(age) + ' (stale)', 'muted');
            return;
        }
        setCoverageStatus('Updated ' + formatCoverageAge(age), 'muted');
    }

    function scheduleCoveragePoll(seconds) {
        if (coveragePollTimer) clearTimeout(coveragePollTimer);
        coveragePollTimer = setTimeout(() => loadCoverage(false), Math.max(1, seconds) * 1000);
    }

    async function loadCoverage(force) {
        if (coverageInFlight) return;
        if (!coverageFieldEls.length) return;
        coverageInFlight = true;
        if (coverageRefreshBtn) coverageRefreshBtn.disabled = true;
        setCoverageStatus(force ? 'Forcing refresh…' : 'Loading…', 'muted');

        try {
            const url = coverageEndpoint + (force ? '?force=1' : '');
            const response = await fetch(url, { headers: { 'Accept': 'application/json' } });
            const data = await response.json().catch(() => null);

            if (response.status === 202 && data && data.ok) {
                applyCoverageData(data.coverage);
                applyCoverageStatusFromResponse(data.coverage, true);
                scheduleCoveragePoll(data.retry_after_seconds || 15);
                return;
            }
            if (!response.ok || !data || !data.ok) {
                setCoverageStatus('Load failed — retry with ↻', 'error');
                return;
            }

            applyCoverageData(data.coverage);
            applyCoverageStatusFromResponse(data.coverage, false);
        } catch (err) {
            setCoverageStatus('Load failed — retry with ↻', 'error');
        } finally {
            coverageInFlight = false;
            if (coverageRefreshBtn) coverageRefreshBtn.disabled = false;
        }
    }

    if (coverageRefreshBtn) {
        coverageRefreshBtn.addEventListener('click', () => {
            if (coveragePollTimer) {
                clearTimeout(coveragePollTimer);
                coveragePollTimer = null;
            }
            loadCoverage(true);
        });
    }

    if (coverageFieldEls.length) {
        // Kick off the initial fetch after the main page has rendered.
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => loadCoverage(false));
        } else {
            loadCoverage(false);
        }
    }

    // -----------------------------------------------------------------------
    // Deferred loss-count loader
    //
    // The overview page now skips the 1.5M-row COUNT(*) on the initial render
    // and fetches it here instead. The label starts as "Showing N-M recorded
    // losses · total counting…" and is rewritten once the fetch returns.
    // -----------------------------------------------------------------------
    const lossCountLabel = document.querySelector('[data-loss-count-label]');
    const paginationTotalValueEl = document.querySelector('[data-loss-pagination-total-value]');

    function formatCountNumber(n) {
        const v = typeof n === 'number' ? n : parseInt(n, 10);
        return Number.isFinite(v) ? v.toLocaleString() : '—';
    }

    function applyTotalToLabel(total, pageSize) {
        if (!lossCountLabel) return;
        // Re-derive the "Showing N-M" from the rendered row count so we don't
        // rely on any intermediate text parsing.
        const tableRows = document.querySelectorAll('[data-ui-section="killmail-overview-table"] tbody tr');
        const rowCount = tableRows.length === 1 && tableRows[0].querySelector('[colspan]') ? 0 : tableRows.length;
        // Only rewrite the "counting…" path — if the label already carries a
        // concrete total (search path) leave it alone.
        const suffix = lossCountLabel.querySelector('[data-loss-count-suffix]');
        if (!suffix) return;
        const totalFormatted = formatCountNumber(total);
        lossCountLabel.textContent = 'Showing ' + (rowCount > 0 ? '1-' + rowCount : '0') + ' of ' + totalFormatted + ' recorded losses';
    }

    function applyTotalToPagination(total, pageSize) {
        if (!paginationTotalValueEl) return;
        const size = Math.max(1, parseInt(pageSize, 10) || 25);
        const pages = Math.max(1, Math.ceil(total / size));
        paginationTotalValueEl.textContent = pages.toLocaleString() + ' pages (' + formatCountNumber(total) + ' losses)';
    }

    async function loadLossCount() {
        if (!lossCountLabel && !paginationTotalValueEl) return;
        // Only the default list defers the count; search preserves the
        // server-rendered total.
        const urlParams = new URLSearchParams(window.location.search);
        if ((urlParams.get('q') || '').trim() !== '') return;

        const countParams = new URLSearchParams();
        ['alliance_id', 'corporation_id', 'mail_type', 'tracked_only'].forEach(name => {
            const v = urlParams.get(name);
            if (v !== null && v !== '') countParams.set(name, v);
        });
        const pageSize = urlParams.get('page_size') || '25';

        try {
            const response = await fetch('/api/killmail-intelligence/count.php?' + countParams.toString(), {
                headers: { 'Accept': 'application/json' },
            });
            const data = await response.json().catch(() => null);
            if (!response.ok || !data || !data.ok) {
                const suffix = lossCountLabel && lossCountLabel.querySelector('[data-loss-count-suffix]');
                if (suffix) suffix.textContent = '· total unavailable';
                if (paginationTotalValueEl) paginationTotalValueEl.textContent = 'unavailable';
                return;
            }
            applyTotalToLabel(data.total, pageSize);
            applyTotalToPagination(data.total, pageSize);
        } catch (err) {
            const suffix = lossCountLabel && lossCountLabel.querySelector('[data-loss-count-suffix]');
            if (suffix) suffix.textContent = '· total unavailable';
            if (paginationTotalValueEl) paginationTotalValueEl.textContent = 'unavailable';
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', loadLossCount);
    } else {
        loadLossCount();
    }

    // -----------------------------------------------------------------------
    // Search-submit spinner
    //
    // Surfacing a visible "Searching…" status lets the user see that the
    // request is in flight on slow search queries (multi-join + LIKE path).
    // -----------------------------------------------------------------------
    const overviewForm = document.querySelector('[data-ui-section="killmail-overview-table"] form');
    const searchSubmitSpinner = document.querySelector('[data-search-submit-spinner]');
    const searchSubmitBtn = document.querySelector('[data-search-submit-btn]');
    if (overviewForm && searchSubmitSpinner && searchSubmitBtn) {
        overviewForm.addEventListener('submit', () => {
            searchSubmitSpinner.classList.remove('hidden');
            searchSubmitSpinner.classList.add('inline-flex');
            searchSubmitBtn.setAttribute('disabled', 'disabled');
            searchSubmitBtn.classList.add('opacity-60', 'pointer-events-none');
        });
    }
})();
</script>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
