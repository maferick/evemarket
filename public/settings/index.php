<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$sections = setting_sections();
$section = active_section();
$title = 'Settings';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    if (!validate_csrf($_POST['_token'] ?? null)) {
        http_response_code(419);
        exit('Invalid CSRF token');
    }

    $submittedSection = $_POST['section'] ?? 'general';

    $saved = false;
    $saveMessage = null;

    switch ($submittedSection) {
        case 'general':
            $saved = save_settings([
                'app_name' => sanitize_app_name((string) ($_POST['app_name'] ?? app_name())),
                'brand_family_name' => sanitize_app_name((string) ($_POST['brand_family_name'] ?? brand_family_name())),
                'brand_console_label' => sanitize_brand_label((string) ($_POST['brand_console_label'] ?? brand_console_label()), brand_family_name() . ' Console'),
                'brand_tagline' => sanitize_brand_label((string) ($_POST['brand_tagline'] ?? brand_tagline()), 'Alliance logistics intelligence platform'),
                'brand_logo_path' => sanitize_brand_asset_path((string) ($_POST['brand_logo_path'] ?? brand_logo_path()), '/assets/branding/supplycore-logo.svg'),
                'brand_favicon_path' => sanitize_brand_asset_path((string) ($_POST['brand_favicon_path'] ?? brand_favicon_path()), '/assets/branding/supplycore-favicon.svg'),
                'app_timezone' => sanitize_timezone((string) ($_POST['app_timezone'] ?? 'UTC')),
                'default_currency' => sanitize_currency((string) ($_POST['default_currency'] ?? 'ISK')),
            ]);
            break;

        case 'trading-stations':
            $saved = save_settings([
                'market_station_id' => sanitize_station_selection($_POST['market_station_id'] ?? null, 'market'),
                'alliance_station_id' => sanitize_station_selection($_POST['alliance_station_id'] ?? null, 'alliance'),
            ]);
            if ($saved) {
                supplycore_cache_bust(['market_compare', 'dashboard', 'doctrine', 'metadata_structures']);
            }
            break;

        case 'item-scope':
            $payload = item_scope_settings_payload_from_request($_POST);
            $saved = save_settings($payload['settings']);
            if ($saved) {
                supplycore_cache_bust(['market_compare', 'dashboard', 'doctrine', 'killmail_detail', 'killmail_overview']);
            }
            if (($payload['messages'] ?? []) !== []) {
                $saveMessage = implode(' ', (array) $payload['messages']);
            }
            break;

        case 'ai-briefings':
            $saved = save_settings(ai_briefing_settings_from_request($_POST));
            break;

        case 'esi-login':
            $saved = save_settings([
                'esi_client_id' => trim($_POST['esi_client_id'] ?? ''),
                'esi_client_secret' => trim($_POST['esi_client_secret'] ?? ''),
                'esi_callback_url' => trim($_POST['esi_callback_url'] ?? ''),
                'esi_scopes' => trim($_POST['esi_scopes'] ?? implode(' ', esi_default_scopes())),
                'esi_enabled' => isset($_POST['esi_enabled']) ? '1' : '0',
            ]);
            break;


        case 'killmail-intelligence':
            $killmailSave = save_killmail_intelligence_settings($_POST);
            $saved = (bool) ($killmailSave['ok'] ?? false);
            $unresolved = (array) ($killmailSave['unresolved'] ?? []);
            if ($unresolved !== []) {
                $saveMessage = 'Some names were not resolved: ' . implode('; ', $unresolved);
            }
            break;

        case 'data-sync':
            $dataSyncAction = trim((string) ($_POST['data_sync_action'] ?? 'save'));

            if ($dataSyncAction === 'run-now') {
                $requestedJob = trim((string) ($_POST['run_now_job_key'] ?? ''));
                $runNow = run_data_sync_now($requestedJob === '' ? null : $requestedJob);
                $saved = (bool) ($runNow['ok'] ?? false);
                flash('success', (string) ($runNow['message'] ?? 'Run now completed.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'retry-job') {
                $requestedJob = trim((string) (($_POST['job_action_job_key'] ?? $_GET['job_action_job_key'] ?? '')));
                $retry = retry_data_sync_job_now($requestedJob);
                $saved = (bool) ($retry['ok'] ?? false);
                flash('success', (string) ($retry['message'] ?? 'Start now submitted.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'stop-investigate-job') {
                $requestedJob = trim((string) (($_POST['job_action_job_key'] ?? $_GET['job_action_job_key'] ?? '')));
                $stop = stop_data_sync_job_for_investigation($requestedJob);
                $saved = (bool) ($stop['ok'] ?? false);
                flash('success', (string) ($stop['message'] ?? 'Job stopped for investigation.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'start-profiling-run') {
                $profiling = scheduler_profiling_start($_POST);
                flash('success', (string) ($profiling['message'] ?? 'Performance Monitoring Run request submitted.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'cancel-profiling-run') {
                $profiling = scheduler_profiling_cancel_active();
                flash('success', (string) ($profiling['message'] ?? 'Performance Monitoring Run cancelled.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'apply-profiling-run') {
                $profilingRunId = max(0, (int) ($_POST['profiling_run_id'] ?? 0));
                $profiling = scheduler_profiling_apply_recommendations($profilingRunId, false);
                flash('success', (string) ($profiling['message'] ?? 'Profiling recommendations applied.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'apply-profiling-run-preserve-manual') {
                $profilingRunId = max(0, (int) ($_POST['profiling_run_id'] ?? 0));
                $profiling = scheduler_profiling_apply_recommendations($profilingRunId, true);
                flash('success', (string) ($profiling['message'] ?? 'Profiling recommendations applied while preserving manual overrides.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'dismiss-profiling-run') {
                $profilingRunId = max(0, (int) ($_POST['profiling_run_id'] ?? 0));
                $profiling = scheduler_profiling_dismiss_recommendations($profilingRunId);
                flash('success', (string) ($profiling['message'] ?? 'Profiling recommendations dismissed.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'rollback-profiling-run') {
                $profiling = scheduler_profiling_rollback_last_apply();
                flash('success', (string) ($profiling['message'] ?? 'Rolled back the last synthesized schedule snapshot.'));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'reset-scheduler') {
                $resetResult = scheduler_reset_runtime_state();
                $saved = (bool) ($resetResult['ok'] ?? false);
                flash('success', scheduler_reset_runtime_state_message($resetResult));
                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            if ($dataSyncAction === 'static-data-import') {
                try {
                    $result = static_data_import_reference_data('auto', false);
                    $saved = (bool) ($result['ok'] ?? false);
                    $message = $saved
                        ? ('Static data import completed in ' . strtoupper((string) ($result['mode'] ?? 'auto')) . ' mode. Build ' . (string) ($result['build_id'] ?? '-') . '; changed=' . ((bool) ($result['changed'] ?? false) ? 'yes' : 'no') . '; rows=' . (int) ($result['rows_written'] ?? 0) . '.')
                        : 'Static data import did not complete.';
                    flash('success', $message);
                } catch (Throwable $exception) {
                    $saved = false;
    $saveMessage = null;
                    flash('success', 'Static data import failed: ' . $exception->getMessage());
                }

                header('Location: /settings?section=' . urlencode($submittedSection));
                exit;
            }

            $dataSyncSave = save_data_sync_settings($_POST);
            $saved = (bool) ($dataSyncSave['ok'] ?? false);
            $saveMessage = (string) ($dataSyncSave['message'] ?? 'Settings saved successfully.');
            break;

        case 'deal-alerts':
            $saved = save_settings(deal_alert_settings_from_request($_POST));
            break;
    }

    flash('success', $saveMessage ?? ($saved ? 'Settings saved successfully.' : 'Database unavailable. Settings were not persisted.'));
    header('Location: /settings?section=' . urlencode($submittedSection));
    exit;
}

$settingValues = get_settings([
    'app_name',
    'brand_family_name',
    'brand_console_label',
    'brand_tagline',
    'brand_logo_path',
    'brand_favicon_path',
    'app_timezone',
    'default_currency',
    'market_station_id',
    'alliance_station_id',
    ...item_scope_setting_keys(),
    ...ai_briefing_setting_keys(),
    'esi_client_id',
    'esi_client_secret',
    'esi_callback_url',
    'esi_scopes',
    'esi_enabled',
    'killmail_ingestion_enabled',
    'killmail_ingestion_poll_sleep_seconds',
    'killmail_ingestion_max_sequences_per_run',
    'killmail_demand_prediction_mode',
    'scheduler_operational_profile',
    'incremental_updates_enabled',
    'incremental_strategy',
    'incremental_delete_policy',
    'incremental_chunk_size',
    'alliance_current_pipeline_enabled',
    'alliance_history_pipeline_enabled',
    'hub_history_pipeline_enabled',
    'market_hub_local_history_pipeline_enabled',
    'alliance_current_backfill_start_date',
    'alliance_history_backfill_start_date',
    'hub_history_backfill_start_date',
    'market_history_retention_raw_days',
    'market_history_retention_hourly_days',
    'market_history_retention_daily_days',
    'sync_automation_enabled_since',
    'static_data_source_url',
    'redis_cache_enabled',
    'redis_locking_enabled',
    'redis_host',
    'redis_port',
    'redis_database',
    'redis_password',
    'redis_prefix',
    ...deal_alerts_setting_keys(),
]);

$dataSyncSettingValues = data_sync_pipeline_settings_view($settingValues);
$dealAlertSettingValues = deal_alert_settings_view($settingValues);

$dbStatus = db_connection_status();
$latestEsiToken = null;
$requiredStructureScopes = esi_required_market_structure_scopes();
$missingStructureScopes = [];
$syncStatusCards = [];
$syncDashboard = sync_schedule_settings_view_model();
$configuredSyncJobs = array_values((array) ($syncDashboard['configured_jobs'] ?? []));
$discoveredSyncJobs = array_values((array) ($syncDashboard['discovered_jobs'] ?? []));
$internalSyncJobs = array_values((array) ($syncDashboard['internal_jobs'] ?? []));
$profilingActiveRun = is_array($syncDashboard['profiling_active_run'] ?? null) ? $syncDashboard['profiling_active_run'] : null;
$profilingRuns = array_values((array) ($syncDashboard['profiling_runs'] ?? []));
$profilingPreviewRun = is_array($syncDashboard['profiling_preview_run'] ?? null) ? $syncDashboard['profiling_preview_run'] : $profilingActiveRun;
$profilingSamples = array_values((array) ($syncDashboard['profiling_samples'] ?? []));
$profilingPairings = array_values((array) ($syncDashboard['profiling_pairings'] ?? []));
$scheduleSnapshots = array_values((array) ($syncDashboard['schedule_snapshots'] ?? []));
$runNowJobOptions = [];
$staticDataState = null;
$settingsPipelineHealth = array_values((array) ($syncDashboard['pipeline_health'] ?? []));
$settingsSystemStatus = (array) ($syncDashboard['system_status'] ?? []);
$rebuildStatus = supplycore_rebuild_status_read();
$runtimeDatasetCards = array_values((array) ($syncDashboard['runtime_dataset_cards'] ?? []));
if ($dbStatus['ok']) {
    $latestEsiToken = db_latest_esi_oauth_token();
    if ($latestEsiToken !== null) {
        $missingStructureScopes = esi_missing_scopes($latestEsiToken, $requiredStructureScopes);
    }

    $staticDataState = db_static_data_import_state_get(static_data_source_key());

    $syncStatusCards = [
        [
            'label' => 'Alliance Orders',
            'status' => sync_status_from_prefix('alliance.structure.', 6),
        ],
        [
            'label' => 'Hub History',
            'status' => sync_status_from_prefix('market.hub.', 4),
        ],
        [
            'label' => 'Maintenance',
            'status' => sync_status_from_prefix('maintenance.', 3),
        ],
    ];

}


$trackedAlliances = [];
$trackedCorporations = [];
$killmailStatus = null;
$killmailWorkerStatus = [];
$killmailStatusSummary = [];
$itemScope = item_scope_view_model();
$ollamaConfig = supplycore_ai_ollama_config();
$ollamaStatus = supplycore_ai_status_summary();
if ($dbStatus['ok']) {
    try {
        $trackedAlliances = db_killmail_tracked_alliances_active();
        $trackedCorporations = db_killmail_tracked_corporations_active();
        $killmailStatus = db_killmail_ingestion_status();
        $killmailWorkerStatus = zkill_worker_runtime_status();
    } catch (Throwable) {
        $trackedAlliances = [];
        $trackedCorporations = [];
        $killmailStatus = null;
        $killmailWorkerStatus = [];
    }
}

if (is_array($killmailStatus)) {
    $killmailState = is_array($killmailStatus['state'] ?? null) ? $killmailStatus['state'] : [];
    $killmailLatestRun = is_array($killmailStatus['latest_run'] ?? null) ? $killmailStatus['latest_run'] : [];
    $killmailStatusSummary = [
        'ingestion_enabled' => ($settingValues['killmail_ingestion_enabled'] ?? '0') === '1',
        'last_success_at_raw' => isset($killmailState['last_success_at']) ? (string) $killmailState['last_success_at'] : null,
        'last_success_at' => killmail_format_datetime(isset($killmailState['last_success_at']) ? (string) $killmailState['last_success_at'] : null),
        'last_sync_relative' => killmail_relative_datetime(isset($killmailState['last_success_at']) ? (string) $killmailState['last_success_at'] : null),
        'current_cursor' => (string) ($killmailState['last_cursor'] ?? 'Unavailable'),
        'last_run_source_rows' => (int) ($killmailLatestRun['source_rows'] ?? 0),
        'last_run_written_rows' => (int) ($killmailLatestRun['written_rows'] ?? 0),
        'last_error' => trim((string) ($killmailState['last_error_message'] ?? '')),
        'tracked_alliance_count' => count($trackedAlliances),
        'tracked_corporation_count' => count($trackedCorporations),
    ];
    $killmailStatusSummary['health'] = killmail_ingestion_health_summary($killmailStatusSummary, $killmailWorkerStatus);
}
$killmailRuntimeCard = null;
foreach ($runtimeDatasetCards as $runtimeDatasetCard) {
    if ((string) ($runtimeDatasetCard['key'] ?? '') === 'killmail_stream') {
        $killmailRuntimeCard = $runtimeDatasetCard;
        break;
    }
}

foreach ($configuredSyncJobs as $schedule) {
    $runNowJobOptions[] = [
        'job_key' => (string) ($schedule['job_key'] ?? ''),
        'label' => (string) ($schedule['label'] ?? ''),
    ];
}

$settingsLiveRefreshSummary = supplycore_live_refresh_summary(null);
$pageHeaderBadge = 'Business settings';
$pageHeaderSummary = 'Keep settings focused on business choices, data freshness, and only show deep runtime detail when you need it.';
$pageHeaderMeta = [
    [
        'label' => 'Settings focus',
        'value' => (string) ($sections[$section]['title'] ?? 'Settings'),
        'caption' => (string) ($sections[$section]['description'] ?? ''),
    ],
    [
        'label' => 'Live updates',
        'value' => $settingsLiveRefreshSummary['mode_label'],
        'caption' => $settingsLiveRefreshSummary['health_message'],
    ],
];

include __DIR__ . '/../../src/views/partials/header.php';
?>
<div class="grid gap-6 xl:grid-cols-[260px_1fr]">
    <aside class="surface-secondary">
        <h2 class="px-3 text-sm font-medium">Configuration Areas</h2>
        <div class="mt-3 space-y-1">
            <?php foreach ($sections as $key => $meta): ?>
                <a href="/settings?section=<?= urlencode($key) ?>"
                   class="block rounded-lg px-3 py-2 text-sm <?= $section === $key ? 'bg-accent/20 text-white' : 'text-muted hover:bg-white/5 hover:text-slate-100' ?>">
                    <?= htmlspecialchars($meta['title'], ENT_QUOTES) ?>
                </a>
            <?php endforeach; ?>
        </div>
    </aside>

    <section class="surface-primary">
        <h2 class="text-xl font-semibold"><?= htmlspecialchars($sections[$section]['title'], ENT_QUOTES) ?></h2>
        <p class="mt-1 text-sm text-muted"><?= htmlspecialchars($sections[$section]['description'], ENT_QUOTES) ?></p>

        <?php if (!$dbStatus['ok']): ?>
            <div class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-200">
                Database is currently unreachable; showing fallback values.
            </div>
        <?php endif; ?>

        <?php if ($section === 'general'): ?>
            <?php
            $businessConfigCards = [
                ['href' => '/settings?section=trading-stations', 'title' => 'Trading stations', 'copy' => 'Reference hub and alliance destination used by market, doctrine, and buy-all workflows.'],
                ['href' => '/settings?section=item-scope', 'title' => 'Item scope', 'copy' => 'Control which items matter for trading, doctrine readiness, and restock decisions.'],
                ['href' => '/settings?section=deal-alerts', 'title' => 'Deal alerts', 'copy' => 'Tune how aggressively SupplyCore flags profitable market anomalies.'],
                ['href' => '/settings?section=killmail-intelligence', 'title' => 'Doctrine + killmail inputs', 'copy' => 'Tracked alliances, corporations, and demand signals that feed readiness and replenishment views.'],
                ['href' => '/settings?section=ai-briefings', 'title' => 'AI briefings', 'copy' => 'Choose whether background AI summaries run and which provider they use.'],
                ['href' => '/settings?section=data-sync', 'title' => 'Sync behavior', 'copy' => 'Control update cadence, freshness expectations, and manual run controls.'],
            ];
            ?>
            <div class="mt-6 space-y-6">
                <section class="space-y-4">
                    <div>
                        <p class="text-sm font-semibold text-slate-100">Business configuration</p>
                        <p class="mt-1 text-sm text-muted">Open the settings area that matches the business decision you need to make.</p>
                    </div>
                    <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                        <?php foreach ($businessConfigCards as $card): ?>
                            <a href="<?= htmlspecialchars($card['href'], ENT_QUOTES) ?>" class="rounded-2xl border border-border bg-black/20 p-4 transition hover:bg-black/30">
                                <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars($card['title'], ENT_QUOTES) ?></p>
                                <p class="mt-2 text-sm text-muted"><?= htmlspecialchars($card['copy'], ENT_QUOTES) ?></p>
                            </a>
                        <?php endforeach; ?>
                    </div>
                </section>

                <section class="space-y-4">
                    <div>
                        <p class="text-sm font-semibold text-slate-100">Data freshness summary</p>
                        <p class="mt-1 text-sm text-muted">Check the user-facing datasets here before drilling into runtime details.</p>
                    </div>
                    <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                        <?php foreach (array_slice($runtimeDatasetCards, 0, 4) as $datasetCard): ?>
                            <article class="rounded-2xl border border-border bg-black/20 p-4">
                                <div class="flex items-start justify-between gap-3">
                                    <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($datasetCard['label'] ?? 'Dataset'), ENT_QUOTES) ?></p>
                                    <span class="inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.14em] <?= htmlspecialchars((string) ($datasetCard['freshness_tone'] ?? supplycore_operational_status_view_model('stale')['tone']), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($datasetCard['freshness_label'] ?? 'Stale'), ENT_QUOTES) ?></span>
                                </div>
                                <p class="mt-3 text-sm text-slate-200"><?= htmlspecialchars((string) ($datasetCard['key'] ?? 'dataset'), ENT_QUOTES) ?></p>
                                <p class="mt-2 text-xs text-muted">Last successful update <?= htmlspecialchars((string) ($datasetCard['last_success_relative'] ?? $datasetCard['last_success_at'] ?? 'Unknown'), ENT_QUOTES) ?></p>
                                <?php if (!empty($datasetCard['show_latest_failure'])): ?>
                                    <p class="mt-2 text-xs text-rose-200">Latest failure: <?= htmlspecialchars((string) ($datasetCard['latest_failure_message'] ?? ''), ENT_QUOTES) ?></p>
                                <?php endif; ?>
                            </article>
                        <?php endforeach; ?>
                        <?php if ($runtimeDatasetCards === []): ?>
                            <div class="rounded-2xl border border-dashed border-border bg-black/20 p-4 text-sm text-muted md:col-span-2 xl:col-span-4">No pipeline freshness summary is available yet.</div>
                        <?php endif; ?>
                    </div>
                </section>

                <details class="rounded-2xl border border-border bg-black/20 p-4" <?= in_array((string) ($settingsSystemStatus['status'] ?? ''), ['critical', 'degraded'], true) ? 'open' : '' ?>>
                    <summary class="cursor-pointer list-none">
                        <div class="flex items-start justify-between gap-3">
                            <div>
                                <p class="text-sm font-semibold text-slate-100">Advanced diagnostics</p>
                                <p class="mt-1 text-sm text-muted">Scheduler internals, transport details, and developer-facing signals stay here by default.</p>
                            </div>
                            <?php $systemTone = supplycore_operational_status_view_model((string) ($settingsSystemStatus['status'] ?? 'degraded'), (string) ($settingsSystemStatus['label'] ?? 'Delayed')); ?>
                            <span class="inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] uppercase tracking-[0.14em] <?= htmlspecialchars($systemTone['tone'], ENT_QUOTES) ?>"><?= htmlspecialchars($systemTone['label'], ENT_QUOTES) ?></span>
                        </div>
                    </summary>
                    <div class="mt-4 grid gap-4 md:grid-cols-2">
                        <div class="rounded-xl border border-border bg-black/30 p-4">
                            <p class="text-sm font-semibold text-slate-100">Live updates</p>
                            <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars($settingsLiveRefreshSummary['mode_label'], ENT_QUOTES) ?> · <?= htmlspecialchars($settingsLiveRefreshSummary['last_refresh_relative'], ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars($settingsLiveRefreshSummary['health_message'], ENT_QUOTES) ?></p>
                        </div>
                        <div class="rounded-xl border border-border bg-black/30 p-4">
                            <p class="text-sm font-semibold text-slate-100">System status</p>
                            <p class="mt-2 text-sm text-slate-300"><?= htmlspecialchars((string) ($settingsSystemStatus['reason'] ?? 'Status unavailable.'), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted">Open <a href="/settings?section=data-sync" class="text-slate-100 underline decoration-dotted underline-offset-4">Data Sync</a> for scheduler state, runtime activity, and recovery controls.</p>
                        </div>
                    </div>
                </details>

                <details class="rounded-2xl border border-border bg-black/20 p-4">
                    <summary class="cursor-pointer list-none text-sm font-semibold text-slate-100">Workspace labels and branding</summary>
                    <form class="mt-4 space-y-4" method="post">
                        <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                        <input type="hidden" name="section" value="general">
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Application name</span>
                            <input name="app_name" value="<?= htmlspecialchars($settingValues['app_name'] ?? app_name(), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Brand family</span>
                            <input name="brand_family_name" value="<?= htmlspecialchars($settingValues['brand_family_name'] ?? brand_family_name(), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Console label</span>
                            <input name="brand_console_label" value="<?= htmlspecialchars($settingValues['brand_console_label'] ?? brand_console_label(), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Brand tagline</span>
                            <input name="brand_tagline" value="<?= htmlspecialchars($settingValues['brand_tagline'] ?? brand_tagline(), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Logo path</span>
                            <input name="brand_logo_path" value="<?= htmlspecialchars($settingValues['brand_logo_path'] ?? brand_logo_path(), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Favicon path</span>
                            <input name="brand_favicon_path" value="<?= htmlspecialchars($settingValues['brand_favicon_path'] ?? brand_favicon_path(), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <div class="grid gap-4 md:grid-cols-2">
                            <label class="block space-y-2">
                                <span class="text-sm text-muted">Timezone</span>
                                <input name="app_timezone" value="<?= htmlspecialchars($settingValues['app_timezone'] ?? app_timezone(), ENT_QUOTES) ?>" class="w-full field-input" />
                            </label>
                            <label class="block space-y-2">
                                <span class="text-sm text-muted">Default currency</span>
                                <input name="default_currency" value="<?= htmlspecialchars($settingValues['default_currency'] ?? default_currency(), ENT_QUOTES) ?>" class="w-full field-input" />
                            </label>
                        </div>
                        <button class="btn-primary">Save workspace settings</button>
                    </form>
                </details>
            </div>
        <?php elseif ($section === 'trading-stations'): ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="trading-stations">
                <label class="block space-y-2" id="market-station-search-field">
                    <span class="text-sm text-muted">Reference Market Hub</span>
                    <?php
                        $marketStationId = trim((string) ($settingValues['market_station_id'] ?? ''));
                        $marketStationName = selected_station_name('market_station_id');
                    ?>
                    <input type="hidden" name="market_station_id" id="market_station_id" value="<?= htmlspecialchars($marketStationId, ENT_QUOTES) ?>">
                    <input
                        type="text"
                        id="market_station_search"
                        autocomplete="off"
                        value="<?= htmlspecialchars($marketStationName ?? '', ENT_QUOTES) ?>"
                        placeholder="Search reference market hubs by station name"
                        class="w-full field-input"
                    />
                    <p id="market_station_status" class="text-xs text-muted">
                        <?= htmlspecialchars($marketStationId === ''
                            ? 'Type at least 2 characters to search reference market hubs.'
                            : ('Selected market hub: ' . ($marketStationName ?? ('Station #' . $marketStationId)) . ' (#' . $marketStationId . ').'), ENT_QUOTES) ?>
                    </p>
                    <ul id="market_station_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                </label>
                <label class="block space-y-2" id="alliance-structure-search-field">
                    <span class="text-sm text-muted">Operational Trading Destination</span>
                    <?php
                        $allianceStationId = trim((string) ($settingValues['alliance_station_id'] ?? ''));
                        $allianceStationName = selected_station_name('alliance_station_id');
                    ?>
                    <input type="hidden" name="alliance_station_id" id="alliance_station_id" value="<?= htmlspecialchars($allianceStationId, ENT_QUOTES) ?>">
                    <input
                        type="text"
                        id="alliance_structure_search"
                        autocomplete="off"
                        value="<?= htmlspecialchars($allianceStationName ?? '', ENT_QUOTES) ?>"
                        placeholder="Search operational destinations (NPC stations + structures)"
                        class="w-full field-input"
                    />
                    <p id="alliance_structure_status" class="text-xs text-muted">
                        <?= htmlspecialchars($allianceStationId === ''
                            ? 'Search ESI destinations (NPC stations + alliance structures).'
                            : ('Selected destination: ' . ($allianceStationName ?? ('Destination #' . $allianceStationId)) . ' (#' . $allianceStationId . ').'), ENT_QUOTES) ?>
                    </p>
                    <ul id="alliance_structure_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                    <p class="text-xs text-muted">Used as your operational destination for alliance-vs-hub comparisons. If you pick an NPC station, structure-only sync jobs stay disabled automatically.</p>
                </label>
                <?php if ($latestEsiToken !== null && $missingStructureScopes !== []): ?>
                    <div class="rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-100">
                        Missing required scopes for structure-market sync: <span class="font-medium"><?= htmlspecialchars(implode(', ', $missingStructureScopes), ENT_QUOTES) ?></span>.
                        Update scopes to include <span class="font-medium">esi-universe.read_structures.v1</span> and <span class="font-medium">esi-markets.structure_markets.v1</span>, then reconnect your ESI character.
                    </div>
                <?php endif; ?>
                <button class="btn-primary">Save Trading Stations</button>
            </form>

            <script>
                (() => {
                    const initializeSearchField = ({
                        inputId,
                        hiddenId,
                        resultsId,
                        statusId,
                        minimumQueryLength,
                        searchingLabel,
                        selectionStatusPrefix,
                        emptyQueryMessage,
                        noResultsMessage,
                        fetchResults,
                    }) => {
                        const input = document.getElementById(inputId);
                        const hidden = document.getElementById(hiddenId);
                        const results = document.getElementById(resultsId);
                        const status = document.getElementById(statusId);

                        if (!input || !hidden || !results || !status) {
                            return;
                        }

                        let debounceTimer = null;

                        const clearResults = () => {
                            results.innerHTML = '';
                            results.classList.add('hidden');
                        };

                        const renderResults = (items) => {
                            clearResults();

                            if (!Array.isArray(items) || items.length === 0) {
                                status.textContent = noResultsMessage;
                                return;
                            }

                            const fragment = document.createDocumentFragment();

                            items.forEach((item) => {
                                const row = document.createElement('li');
                                const button = document.createElement('button');
                                const details = [];

                                if (item.system) {
                                    details.push('System ' + item.system);
                                }

                                if (item.type) {
                                    details.push('Type ' + item.type);
                                }

                                button.type = 'button';
                                button.className = 'flex w-full flex-col items-start gap-1 px-3 py-2 text-left text-sm hover:bg-white/5';
                                button.innerHTML = '<span class="text-slate-100"></span><span class="text-xs text-muted"></span>';
                                button.querySelector('span').textContent = item.name;
                                button.querySelectorAll('span')[1].textContent = '#' + item.id + (details.length ? ' · ' + details.join(' · ') : '');
                                button.addEventListener('click', () => {
                                    hidden.value = String(item.id);
                                    input.value = item.name;
                                    status.textContent = selectionStatusPrefix + item.name + ' (#' + item.id + ').';
                                    clearResults();
                                });

                                row.appendChild(button);
                                fragment.appendChild(row);
                            });

                            results.appendChild(fragment);
                            results.classList.remove('hidden');
                        };

                        input.addEventListener('input', () => {
                            const query = input.value.trim();

                            hidden.value = '';

                            if (debounceTimer !== null) {
                                clearTimeout(debounceTimer);
                            }

                            if (query.length < minimumQueryLength) {
                                status.textContent = emptyQueryMessage;
                                clearResults();
                                return;
                            }

                            debounceTimer = window.setTimeout(async () => {
                                status.textContent = searchingLabel;

                                try {
                                    const items = await fetchResults(query);
                                    status.textContent = 'Select an option from the list.';
                                    renderResults(items);
                                } catch (error) {
                                    status.textContent = error instanceof Error ? error.message : 'Lookup failed.';
                                    clearResults();
                                }
                            }, 250);
                        });
                    };

                    initializeSearchField({
                        inputId: 'market_station_search',
                        hiddenId: 'market_station_id',
                        resultsId: 'market_station_results',
                        statusId: 'market_station_status',
                        minimumQueryLength: 2,
                        searchingLabel: 'Searching…',
                        selectionStatusPrefix: 'Selected market hub: ',
                        emptyQueryMessage: 'Type at least 2 characters to search reference market hubs.',
                        noResultsMessage: 'No matching reference market hubs found.',
                        fetchResults: async (query) => {
                            const response = await fetch('/settings/market-stations.php?q=' + encodeURIComponent(query), {
                                headers: { 'Accept': 'application/json' },
                            });

                            const payload = await response.json();
                            if (!response.ok) {
                                throw new Error(payload.error || 'Lookup failed.');
                            }

                            return payload.results || [];
                        },
                    });

                    initializeSearchField({
                        inputId: 'alliance_structure_search',
                        hiddenId: 'alliance_station_id',
                        resultsId: 'alliance_structure_results',
                        statusId: 'alliance_structure_status',
                        minimumQueryLength: 2,
                        searchingLabel: 'Searching…',
                        selectionStatusPrefix: 'Selected destination: ',
                        emptyQueryMessage: 'Type at least 2 characters to search operational destinations.',
                        noResultsMessage: 'No matching operational destinations found.',
                        fetchResults: async (query) => {
                            const response = await fetch('/settings/esi-structures.php?q=' + encodeURIComponent(query), {
                                headers: { 'Accept': 'application/json' },
                            });

                            const payload = await response.json();
                            if (!response.ok) {
                                throw new Error(payload.error || 'Lookup failed.');
                            }

                            return payload.results || [];
                        },
                    });
                })();
            </script>
        <?php elseif ($section === 'ai-briefings'): ?>
            <form class="mt-6 space-y-6" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="ai-briefings">

                <div class="grid gap-4 md:grid-cols-3">
                    <div class="rounded-2xl border border-white/8 bg-white/[0.03] p-4">
                        <p class="text-xs uppercase tracking-[0.16em] text-muted">Configured Mode</p>
                        <p class="mt-2 text-2xl font-semibold text-slate-50"><?= ($ollamaConfig['enabled'] ?? false) ? 'Enabled' : 'Fallback only' ?></p>
                        <p class="mt-1 text-sm text-muted">Provider: <?= htmlspecialchars(ollama_provider_options()[(string) ($ollamaConfig['provider'] ?? 'local')] ?? 'Local Ollama', ENT_QUOTES) ?>.</p>
                    </div>
                    <div class="rounded-2xl border <?= ($ollamaStatus['ok'] ?? false) ? 'border-emerald-500/20 bg-emerald-500/10' : 'border-amber-500/20 bg-amber-500/10' ?> p-4">
                        <p class="text-xs uppercase tracking-[0.16em] <?= ($ollamaStatus['ok'] ?? false) ? 'text-emerald-200/80' : 'text-amber-200/80' ?>">Connection Status</p>
                        <p class="mt-2 text-2xl font-semibold <?= ($ollamaStatus['ok'] ?? false) ? 'text-emerald-100' : 'text-amber-100' ?>"><?= htmlspecialchars((string) ($ollamaStatus['label'] ?? 'Not configured'), ENT_QUOTES) ?></p>
                        <p class="mt-1 text-sm <?= ($ollamaStatus['ok'] ?? false) ? 'text-emerald-100/70' : 'text-amber-100/70' ?>"><?= htmlspecialchars((string) ($ollamaStatus['description'] ?? ''), ENT_QUOTES) ?></p>
                    </div>
                    <div class="rounded-2xl border border-sky-500/20 bg-sky-500/10 p-4">
                        <p class="text-xs uppercase tracking-[0.16em] text-sky-200/80">Scheduler Behavior</p>
                        <p class="mt-2 text-2xl font-semibold text-sky-100">Non-blocking</p>
                        <p class="mt-1 text-sm text-sky-100/70">Disabled or unreachable AI falls back to deterministic summaries instead of blocking the job.</p>
                    </div>
                    <div class="rounded-2xl border border-violet-500/20 bg-violet-500/10 p-4 md:col-span-3">
                        <p class="text-xs uppercase tracking-[0.16em] text-violet-200/80">Capability Tier</p>
                        <div class="mt-2 flex flex-wrap items-center gap-3">
                            <p class="text-2xl font-semibold text-violet-50"><?= htmlspecialchars(strtoupper((string) ($ollamaConfig['capability_tier'] ?? 'small')), ENT_QUOTES) ?></p>
                            <span class="badge border-violet-300/20 bg-violet-400/10 text-violet-100">
                                <?= (($ollamaConfig['capability_override'] ?? 'auto') === 'auto')
                                    ? ('auto from model: ' . htmlspecialchars((string) ($ollamaConfig['inferred_tier'] ?? 'small'), ENT_QUOTES))
                                    : ('manual override: ' . htmlspecialchars((string) ($ollamaConfig['capability_override'] ?? 'small'), ENT_QUOTES)) ?>
                            </span>
                        </div>
                        <p class="mt-2 text-sm text-violet-100/75">Candidate batching, prompt depth, enabled tasks, and dashboard richness all scale from this centralized AI strategy.</p>
                    </div>
                </div>

                <label class="flex items-start gap-3 rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3">
                    <input type="hidden" name="ollama_enabled" value="0">
                    <input type="checkbox" name="ollama_enabled" value="1" <?= ($settingValues['ollama_enabled'] ?? '0') === '1' ? 'checked' : '' ?> class="mt-1 size-4 rounded border-border bg-black">
                    <span>
                        <span class="block text-sm font-medium text-slate-100">Enable AI doctrine briefings</span>
                        <span class="mt-1 block text-xs text-muted">Turn this off to force deterministic fallback summaries while still keeping briefing records populated.</span>
                    </span>
                </label>

                <div class="grid gap-4 md:grid-cols-2">
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">AI Provider</span>
                        <select name="ollama_provider" class="w-full field-input">
                            <?php $selectedProvider = (string) ($settingValues['ollama_provider'] ?? ($ollamaConfig['provider'] ?? 'local')); ?>
                            <?php foreach (ollama_provider_options() as $value => $label): ?>
                                <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= $selectedProvider === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                            <?php endforeach; ?>
                        </select>
                        <p class="text-xs text-muted">Use <span class="font-medium text-slate-100">Local Ollama</span> for your existing self-hosted API, or switch to <span class="font-medium text-slate-100">Runpod Serverless</span> to submit bearer-authenticated async jobs and poll their status in the background.</p>
                    </label>
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Local Ollama API URL</span>
                        <input name="ollama_url" value="<?= htmlspecialchars($settingValues['ollama_url'] ?? ($ollamaConfig['url'] ?? 'http://localhost:11434/api'), ENT_QUOTES) ?>" class="w-full field-input" />
                        <p class="text-xs text-muted">Use the API base URL, for example <span class="font-medium text-slate-100">http://localhost:11434/api</span>.</p>
                    </label>
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Runpod Serverless Endpoint</span>
                        <input name="ollama_runpod_url" value="<?= htmlspecialchars($settingValues['ollama_runpod_url'] ?? ($ollamaConfig['runpod_url'] ?? ''), ENT_QUOTES) ?>" class="w-full field-input" placeholder="https://api.runpod.ai/v2/.../run" />
                        <p class="text-xs text-muted">Paste the full Runpod async request URL, for example <span class="font-medium text-slate-100">https://api.runpod.ai/v2/58qz2qbho8h3f1/run</span>. Existing <span class="font-medium text-slate-100">/runsync</span> URLs are converted automatically.</p>
                    </label>
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Runpod API Key</span>
                        <input name="ollama_runpod_api_key" type="password" value="<?= htmlspecialchars($settingValues['ollama_runpod_api_key'] ?? ($ollamaConfig['runpod_api_key'] ?? ''), ENT_QUOTES) ?>" class="w-full field-input" placeholder="Bearer token for the Runpod endpoint" />
                        <?php if (($ollamaConfig['runpod_api_key_masked'] ?? '') !== ''): ?>
                            <p class="text-xs text-muted">Saved key preview: <span class="font-medium text-slate-100"><?= htmlspecialchars((string) $ollamaConfig['runpod_api_key_masked'], ENT_QUOTES) ?></span>.</p>
                        <?php else: ?>
                            <p class="text-xs text-muted">Stored only when you save settings. Leave blank if you are staying on the local provider.</p>
                        <?php endif; ?>
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Model Name</span>
                        <input name="ollama_model" value="<?= htmlspecialchars($settingValues['ollama_model'] ?? ($ollamaConfig['model'] ?? 'qwen2.5:1.5b-instruct'), ENT_QUOTES) ?>" class="w-full field-input" />
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Request Timeout (seconds)</span>
                        <input type="number" min="1" max="300" step="1" name="ollama_timeout" value="<?= htmlspecialchars($settingValues['ollama_timeout'] ?? (string) ($ollamaConfig['timeout'] ?? 20), ENT_QUOTES) ?>" class="w-full field-input" />
                    </label>
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Capability Tier</span>
                        <select name="ollama_capability_tier" class="w-full field-input">
                            <?php $selectedTier = (string) ($settingValues['ollama_capability_tier'] ?? ($ollamaConfig['capability_override'] ?? 'auto')); ?>
                            <?php foreach (['auto' => 'Auto-detect from model', 'small' => 'Small', 'medium' => 'Medium', 'large' => 'Large'] as $value => $label): ?>
                                <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= $selectedTier === $value ? 'selected' : '' ?>><?= htmlspecialchars($label, ENT_QUOTES) ?></option>
                            <?php endforeach; ?>
                        </select>
                        <p class="text-xs text-muted">Leave this on auto to infer capability from the configured model name, or pin a tier if the model naming does not include parameter size.</p>
                    </label>
                </div>

                <div class="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 text-sm text-muted">
                    Configure either the local Ollama API or the Runpod serverless endpoint here, then manage cadence under <a href="/settings?section=data-sync" class="font-medium text-slate-100 hover:text-white">Settings → Data Sync</a> for the <span class="font-medium text-slate-100">rebuild_ai_briefings</span> scheduler job. Runpod requests now submit asynchronously and poll for completion within the configured timeout window. Small tiers stay compact, medium tiers add explanation and deltas, and large tiers unlock richer operator briefings while still keeping deterministic calculations authoritative.
                </div>

                <button class="btn-primary">Save AI Briefing Settings</button>
            </form>
        <?php elseif ($section === 'item-scope'): ?>
            <?php
                $itemScopeConfig = $itemScope['config'] ?? item_scope_default_config();
                $itemScopeCatalog = $itemScope['catalog'] ?? ['categories' => [], 'groups' => [], 'market_groups' => [], 'meta_groups' => []];
                $itemScopeStats = $itemScope['stats'] ?? ['published_count' => 0, 'in_scope_count' => 0, 'excluded_count' => 0];
                $operationalRows = $itemScope['operational_rows'] ?? [];
                $tierRows = $itemScope['tier_rows'] ?? [];
                $noiseRows = $itemScope['noise_rows'] ?? [];
                $includeOverridesText = implode("\n", array_map(
                    static fn (array $row): string => (string) ((int) ($row['type_id'] ?? 0)) . ' | ' . (string) ($row['type_name'] ?? ('Type #' . (int) ($row['type_id'] ?? 0))),
                    (array) (($itemScope['override_rows']['include'] ?? []))
                ));
                $excludeOverridesText = implode("\n", array_map(
                    static fn (array $row): string => (string) ((int) ($row['type_id'] ?? 0)) . ' | ' . (string) ($row['type_name'] ?? ('Type #' . (int) ($row['type_id'] ?? 0))),
                    (array) (($itemScope['override_rows']['exclude'] ?? []))
                ));
            ?>
            <div class="mt-6 grid gap-4 md:grid-cols-3">
                <div class="rounded-2xl border border-white/8 bg-white/[0.03] p-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-muted">Published Types</p>
                    <p class="mt-2 text-2xl font-semibold text-slate-50"><?= number_format((int) ($itemScopeStats['published_count'] ?? 0)) ?></p>
                    <p class="mt-1 text-sm text-muted">Reference inventory available to the shared item-scope service.</p>
                </div>
                <div class="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-emerald-200/80">Currently In Scope</p>
                    <p class="mt-2 text-2xl font-semibold text-emerald-100"><?= number_format((int) ($itemScopeStats['in_scope_count'] ?? 0)) ?></p>
                    <p class="mt-1 text-sm text-emerald-100/70">Shared across doctrine readiness, market gaps, dashboard summaries, and killmail demand.</p>
                </div>
                <div class="rounded-2xl border border-amber-500/20 bg-amber-500/10 p-4">
                    <p class="text-xs uppercase tracking-[0.16em] text-amber-200/80">Filtered Out</p>
                    <p class="mt-2 text-2xl font-semibold text-amber-100"><?= number_format((int) ($itemScopeStats['excluded_count'] ?? 0)) ?></p>
                    <p class="mt-1 text-sm text-amber-100/70">Removed by the operational allow-list, tier controls, noise filters, or explicit overrides.</p>
                </div>
            </div>

            <div class="mt-6 rounded-2xl border border-white/8 bg-white/[0.03] p-4">
                <h3 class="text-sm font-semibold text-slate-100">Operational Summary</h3>
                <ul class="mt-3 space-y-2 text-sm text-muted">
                    <?php foreach ((array) ($itemScope['summary_lines'] ?? []) as $line): ?>
                        <li>• <?= htmlspecialchars((string) $line, ENT_QUOTES) ?></li>
                    <?php endforeach; ?>
                </ul>
            </div>

            <form class="mt-6 space-y-6" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="item-scope">

                <div class="grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
                    <label class="rounded-2xl border border-white/8 bg-white/[0.03] p-4">
                        <span class="text-sm font-medium text-slate-100">Scope Mode</span>
                        <select name="item_scope_mode" class="mt-3 w-full field-input">
                            <option value="allow_list" <?= ($itemScopeConfig['mode'] ?? 'allow_list') === 'allow_list' ? 'selected' : '' ?>>Alliance logistics allow-list</option>
                            <option value="allow_all" <?= ($itemScopeConfig['mode'] ?? 'allow_list') === 'allow_all' ? 'selected' : '' ?>>Allow all published items, then apply shared exclusions</option>
                        </select>
                        <p class="mt-2 text-xs text-muted">Allow-list mode is the recommended default: it keeps the scope centered on operational categories instead of the full SDE universe.</p>
                    </label>

                    <div class="rounded-2xl border border-white/8 bg-white/[0.03] p-4 text-sm text-muted">
                        <p class="font-medium text-slate-100">Rule precedence</p>
                        <ol class="mt-3 list-decimal space-y-2 pl-5">
                            <li>Explicit item overrides always win.</li>
                            <li>Noise filters and advanced excludes remove unwanted inventory before it reaches downstream analytics.</li>
                            <li>Operational categories and advanced includes define the baseline logistics universe in allow-list mode.</li>
                            <li>Tier toggles use metaGroupID so doctrine-safe tiers can be curated without using raw meta levels.</li>
                        </ol>
                    </div>
                </div>

                <div class="rounded-2xl border border-cyan-500/20 bg-cyan-500/10 p-5">
                    <div class="flex flex-col gap-2 md:flex-row md:items-end md:justify-between">
                        <div>
                            <h3 class="text-sm font-semibold text-cyan-100">Operational categories</h3>
                            <p class="mt-1 text-xs text-cyan-100/70">High-level alliance logistics buckets mapped from categoryID first, then refined with group and market taxonomy only where needed.</p>
                        </div>
                        <p class="text-xs text-cyan-100/60">Default profile surfaces doctrine-ready ships, modules, rigs, charges, drones, structure fuel, and boosters.</p>
                    </div>
                    <div class="mt-4 grid gap-3 xl:grid-cols-2">
                        <?php foreach ((array) $operationalRows as $row): ?>
                            <?php $rowKey = (string) ($row['key'] ?? ''); ?>
                            <?php if ($rowKey === '') { continue; } ?>
                            <label class="flex items-start gap-3 rounded-2xl border border-cyan-400/15 bg-black/20 p-4 text-sm text-cyan-50">
                                <input type="checkbox" name="item_scope_operational_category_keys[]" value="<?= htmlspecialchars($rowKey, ENT_QUOTES) ?>" class="mt-1" <?= !empty($row['selected']) ? 'checked' : '' ?>>
                                <span class="block min-w-0 flex-1">
                                    <span class="flex items-center gap-2">
                                        <span class="font-medium"><?= htmlspecialchars((string) ($row['label'] ?? $rowKey), ENT_QUOTES) ?></span>
                                        <?php if (!empty($row['default'])): ?>
                                            <span class="rounded-full border border-cyan-300/30 bg-cyan-400/10 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-cyan-100/70">Default</span>
                                        <?php endif; ?>
                                    </span>
                                    <span class="mt-1 block text-xs text-cyan-100/70"><?= htmlspecialchars((string) ($row['description'] ?? ''), ENT_QUOTES) ?></span>
                                    <span class="mt-2 block text-xs text-cyan-100/60"><?= number_format((int) ($row['type_count'] ?? 0)) ?> published types currently map here</span>
                                </span>
                            </label>
                        <?php endforeach; ?>
                    </div>
                </div>

                <div class="grid gap-4 xl:grid-cols-2">
                    <div class="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-4">
                        <h3 class="text-sm font-semibold text-emerald-100">Tier filtering</h3>
                        <p class="mt-1 text-xs text-emerald-100/70">Simple metaGroupID toggles for alliance-ready tiers. Tech I and Tech II are enabled by default; Deadspace and Officer stay off unless explicitly enabled.</p>
                        <div class="mt-4 grid gap-3">
                            <?php foreach ((array) $tierRows as $row): ?>
                                <?php $tierId = (int) ($row['meta_group_id'] ?? 0); ?>
                                <?php if ($tierId <= 0) { continue; } ?>
                                <label class="flex items-start gap-3 rounded-xl border border-emerald-400/15 bg-black/20 p-3 text-sm text-emerald-50">
                                    <input type="checkbox" name="item_scope_tier_meta_group_ids[]" value="<?= $tierId ?>" class="mt-1" <?= !empty($row['selected']) ? 'checked' : '' ?>>
                                    <span>
                                        <span class="block font-medium"><?= htmlspecialchars((string) ($row['meta_group_name'] ?? ('Meta Group #' . $tierId)), ENT_QUOTES) ?></span>
                                        <span class="text-xs text-emerald-100/60"><?= number_format((int) ($row['type_count'] ?? 0)) ?> published types with this meta group</span>
                                    </span>
                                </label>
                            <?php endforeach; ?>
                        </div>
                    </div>

                    <div class="rounded-2xl border border-rose-500/20 bg-rose-500/10 p-4">
                        <h3 class="text-sm font-semibold text-rose-100">Noise filters</h3>
                        <p class="mt-1 text-xs text-rose-100/70">Shared exclusions applied before market, doctrine, and loss-demand analytics are evaluated.</p>
                        <div class="mt-4 grid gap-3">
                            <?php foreach ((array) $noiseRows as $row): ?>
                                <?php $rowKey = (string) ($row['key'] ?? ''); ?>
                                <?php if ($rowKey === '') { continue; } ?>
                                <label class="flex items-start gap-3 rounded-xl border border-rose-400/15 bg-black/20 p-3 text-sm text-rose-50">
                                    <input type="checkbox" name="item_scope_noise_filter_keys[]" value="<?= htmlspecialchars($rowKey, ENT_QUOTES) ?>" class="mt-1" <?= !empty($row['selected']) ? 'checked' : '' ?>>
                                    <span>
                                        <span class="flex items-center gap-2">
                                            <span class="font-medium"><?= htmlspecialchars((string) ($row['label'] ?? $rowKey), ENT_QUOTES) ?></span>
                                            <?php if (!empty($row['default'])): ?>
                                                <span class="rounded-full border border-rose-300/30 bg-rose-400/10 px-2 py-0.5 text-[10px] uppercase tracking-[0.2em] text-rose-100/70">Default</span>
                                            <?php endif; ?>
                                        </span>
                                        <span class="mt-1 block text-xs text-rose-100/70"><?= htmlspecialchars((string) ($row['description'] ?? ''), ENT_QUOTES) ?></span>
                                        <span class="mt-2 block text-xs text-rose-100/60"><?= number_format((int) ($row['type_count'] ?? 0)) ?> published types currently match this filter</span>
                                    </span>
                                </label>
                            <?php endforeach; ?>
                        </div>
                    </div>
                </div>

                <details class="rounded-2xl border border-white/8 bg-white/[0.03] p-4">
                    <summary class="cursor-pointer list-none text-sm font-semibold text-slate-100">Advanced mode</summary>
                    <p class="mt-2 text-xs text-muted">Advanced controls stay hidden by default. Use them only when you need group-level or market-group-level exceptions beyond the curated operational model.</p>

                    <div class="mt-4 grid gap-4 xl:grid-cols-2">
                        <div class="rounded-2xl border border-emerald-500/20 bg-emerald-500/10 p-4">
                            <h4 class="text-sm font-semibold text-emerald-100">Advanced include rules</h4>
                            <p class="mt-1 text-xs text-emerald-100/70">Use these to extend the operational universe with specific groups or market branches.</p>
                            <div class="mt-4 grid gap-4">
                                <?php
                                    $advancedIncludeSections = [
                                        'item_scope_include_group_ids' => ['title' => 'Groups', 'rows' => $itemScopeCatalog['groups'] ?? [], 'id' => 'group_id', 'label' => 'group_name', 'count' => 'type_count'],
                                        'item_scope_include_market_group_ids' => ['title' => 'Market groups', 'rows' => $itemScopeCatalog['market_groups'] ?? [], 'id' => 'market_group_id', 'label' => 'market_group_name', 'count' => 'type_count'],
                                    ];
                                ?>
                                <?php foreach ($advancedIncludeSections as $fieldName => $meta): ?>
                                    <div>
                                        <p class="text-xs uppercase tracking-[0.16em] text-emerald-100/70"><?= htmlspecialchars((string) $meta['title'], ENT_QUOTES) ?></p>
                                        <div class="mt-2 max-h-56 space-y-2 overflow-y-auto rounded-xl border border-emerald-400/15 bg-black/20 p-3">
                                            <?php foreach ((array) $meta['rows'] as $row): ?>
                                                <?php $rowId = (int) ($row[$meta['id']] ?? 0); ?>
                                                <?php if ($rowId <= 0) { continue; } ?>
                                                <label class="flex items-start gap-3 text-sm text-emerald-50">
                                                    <input type="checkbox" name="<?= htmlspecialchars($fieldName, ENT_QUOTES) ?>[]" value="<?= $rowId ?>" class="mt-1" <?= in_array($rowId, (array) ($itemScopeConfig[str_replace('item_scope_', '', $fieldName)] ?? []), true) ? 'checked' : '' ?>>
                                                    <span>
                                                        <span class="block"><?= htmlspecialchars((string) ($row[$meta['label']] ?? ('#' . $rowId)), ENT_QUOTES) ?></span>
                                                        <span class="text-xs text-emerald-100/60"><?= number_format((int) ($row[$meta['count']] ?? 0)) ?> published types</span>
                                                    </span>
                                                </label>
                                            <?php endforeach; ?>
                                        </div>
                                    </div>
                                <?php endforeach; ?>
                            </div>
                        </div>

                        <div class="rounded-2xl border border-rose-500/20 bg-rose-500/10 p-4">
                            <h4 class="text-sm font-semibold text-rose-100">Advanced exclude rules</h4>
                            <p class="mt-1 text-xs text-rose-100/70">Use these to carve out specific problem groups or market branches after the shared defaults have done most of the work.</p>
                            <div class="mt-4 grid gap-4">
                                <?php
                                    $advancedExcludeSections = [
                                        'item_scope_exclude_group_ids' => ['title' => 'Groups', 'rows' => $itemScopeCatalog['groups'] ?? [], 'id' => 'group_id', 'label' => 'group_name', 'count' => 'type_count'],
                                        'item_scope_exclude_market_group_ids' => ['title' => 'Market groups', 'rows' => $itemScopeCatalog['market_groups'] ?? [], 'id' => 'market_group_id', 'label' => 'market_group_name', 'count' => 'type_count'],
                                    ];
                                ?>
                                <?php foreach ($advancedExcludeSections as $fieldName => $meta): ?>
                                    <div>
                                        <p class="text-xs uppercase tracking-[0.16em] text-rose-100/70"><?= htmlspecialchars((string) $meta['title'], ENT_QUOTES) ?></p>
                                        <div class="mt-2 max-h-56 space-y-2 overflow-y-auto rounded-xl border border-rose-400/15 bg-black/20 p-3">
                                            <?php foreach ((array) $meta['rows'] as $row): ?>
                                                <?php $rowId = (int) ($row[$meta['id']] ?? 0); ?>
                                                <?php if ($rowId <= 0) { continue; } ?>
                                                <label class="flex items-start gap-3 text-sm text-rose-50">
                                                    <input type="checkbox" name="<?= htmlspecialchars($fieldName, ENT_QUOTES) ?>[]" value="<?= $rowId ?>" class="mt-1" <?= in_array($rowId, (array) ($itemScopeConfig[str_replace('item_scope_', '', $fieldName)] ?? []), true) ? 'checked' : '' ?>>
                                                    <span>
                                                        <span class="block"><?= htmlspecialchars((string) ($row[$meta['label']] ?? ('#' . $rowId)), ENT_QUOTES) ?></span>
                                                        <span class="text-xs text-rose-100/60"><?= number_format((int) ($row[$meta['count']] ?? 0)) ?> published types</span>
                                                    </span>
                                                </label>
                                            <?php endforeach; ?>
                                        </div>
                                    </div>
                                <?php endforeach; ?>
                            </div>
                        </div>
                    </div>

                    <div class="mt-4 grid gap-4 xl:grid-cols-2">
                        <label class="rounded-2xl border border-white/8 bg-black/20 p-4">
                            <span class="text-sm font-semibold text-slate-100">Explicit include overrides</span>
                            <textarea name="item_scope_include_overrides" rows="8" class="mt-3 w-full field-input font-mono" placeholder="Exact item name or numeric type ID per line"><?= htmlspecialchars($includeOverridesText, ENT_QUOTES) ?></textarea>
                            <p class="mt-2 text-xs text-muted">Use this when one item must remain visible even if broader rules would remove it.</p>
                        </label>
                        <label class="rounded-2xl border border-white/8 bg-black/20 p-4">
                            <span class="text-sm font-semibold text-slate-100">Explicit exclude overrides</span>
                            <textarea name="item_scope_exclude_overrides" rows="8" class="mt-3 w-full field-input font-mono" placeholder="Exact item name or numeric type ID per line"><?= htmlspecialchars($excludeOverridesText, ENT_QUOTES) ?></textarea>
                            <p class="mt-2 text-xs text-muted">Use this when one item should stay suppressed even though its category or group remains enabled.</p>
                        </label>
                    </div>
                </details>

                <button class="btn-primary">Save Item Scope</button>
            </form>
        <?php elseif ($section === 'killmail-intelligence'): ?>
            <?php
                $trackedAllianceSelections = array_values(array_map(static fn (array $row): array => [
                    'id' => (int) ($row['alliance_id'] ?? 0),
                    'name' => (string) ($row['label'] ?? ('Alliance #' . (int) ($row['alliance_id'] ?? 0))),
                    'type' => 'Alliance',
                ], array_filter($trackedAlliances, static fn (array $row): bool => (int) ($row['alliance_id'] ?? 0) > 0)));
                $trackedCorporationSelections = array_values(array_map(static fn (array $row): array => [
                    'id' => (int) ($row['corporation_id'] ?? 0),
                    'name' => (string) ($row['label'] ?? ('Corporation #' . (int) ($row['corporation_id'] ?? 0))),
                    'type' => 'Corporation',
                ], array_filter($trackedCorporations, static fn (array $row): bool => (int) ($row['corporation_id'] ?? 0) > 0)));
                $alliancesText = implode("
", array_map(static fn (array $row): string => (string) $row['id'] . ' | ' . (string) $row['name'], $trackedAllianceSelections));
                $corporationsText = implode("
", array_map(static fn (array $row): string => (string) $row['id'] . ' | ' . (string) $row['name'], $trackedCorporationSelections));
                $statusState = is_array($killmailStatus['state'] ?? null) ? $killmailStatus['state'] : [];
                $killmailHealth = is_array($killmailStatusSummary['health'] ?? null) ? $killmailStatusSummary['health'] : [];
            ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="killmail-intelligence">

                <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                    <article class="rounded-xl border p-4 <?= htmlspecialchars((string) (($killmailRuntimeCard['freshness_tone'] ?? $killmailHealth['tone']) ?? 'border-border bg-black/20 text-slate-200'), ENT_QUOTES) ?>">
                        <p class="text-xs uppercase tracking-[0.16em] opacity-70">Dataset</p>
                        <p class="mt-2 text-sm font-semibold text-slate-50"><?= htmlspecialchars((string) (($killmailRuntimeCard['label'] ?? null) ?: 'Killmail stream'), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs opacity-90"><?= htmlspecialchars((string) (($killmailRuntimeCard['key'] ?? null) ?: 'killmail.r2z2.stream'), ENT_QUOTES) ?></p>
                    </article>
                    <article class="rounded-xl border border-border bg-black/20 p-4">
                        <p class="text-xs uppercase tracking-[0.16em] text-muted">Last success</p>
                        <p class="mt-2 text-sm font-semibold text-slate-50"><?= htmlspecialchars((string) (($killmailRuntimeCard['last_success_relative'] ?? null) ?: ($killmailStatusSummary['last_sync_relative'] ?? 'Never')), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs text-muted"><?= htmlspecialchars((string) (($killmailRuntimeCard['last_success_at'] ?? null) ?: ($killmailStatusSummary['last_success_at'] ?? 'Unavailable')), ENT_QUOTES) ?></p>
                    </article>
                    <article class="rounded-xl border border-border bg-black/20 p-4">
                        <p class="text-xs uppercase tracking-[0.16em] text-muted">Freshness</p>
                        <p class="mt-2 text-sm font-semibold text-slate-50"><?= htmlspecialchars((string) (($killmailRuntimeCard['freshness_label'] ?? null) ?: ($killmailHealth['label'] ?? 'Unknown')), ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs text-muted"><?= !empty($killmailRuntimeCard['running_now']) ? 'Worker heartbeat is active now.' : 'Based on the latest successful ingestion timestamp.' ?></p>
                    </article>
                    <article class="rounded-xl border border-border bg-black/20 p-4">
                        <p class="text-xs uppercase tracking-[0.16em] text-muted">Latest failure</p>
                        <p class="mt-2 text-sm font-semibold text-slate-50"><?= htmlspecialchars(!empty($killmailRuntimeCard['show_latest_failure']) ? 'Failed' : 'None', ENT_QUOTES) ?></p>
                        <p class="mt-2 text-xs text-muted"><?= htmlspecialchars(!empty($killmailRuntimeCard['show_latest_failure']) ? (string) ($killmailRuntimeCard['latest_failure_message'] ?? '') : 'Clears automatically after the next successful run.', ENT_QUOTES) ?></p>
                    </article>
                    <article class="rounded-xl border border-border bg-black/20 p-4">
                        <p class="text-xs uppercase tracking-[0.16em] text-muted">Tracked entities</p>
                        <p class="mt-2 text-sm font-semibold text-slate-50"><?= number_format((int) (($killmailRuntimeCard['tracked_alliance_count'] ?? null) ?: ($killmailStatusSummary['tracked_alliance_count'] ?? 0))) ?> alliances · <?= number_format((int) (($killmailRuntimeCard['tracked_corporation_count'] ?? null) ?: ($killmailStatusSummary['tracked_corporation_count'] ?? 0))) ?> corporations</p>
                        <p class="mt-2 text-xs text-muted">These determine which victim-side losses are retained for the tracked loss board.</p>
                    </article>
                </div>

                <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <input type="hidden" name="killmail_ingestion_enabled" value="0">
                    <input type="checkbox" name="killmail_ingestion_enabled" value="1" <?= ($settingValues['killmail_ingestion_enabled'] ?? '0') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                    <span class="text-sm">Enable zKillboard R2Z2 ingestion</span>
                </label>

                <div class="grid gap-4 md:grid-cols-2">
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Poll Sleep Seconds (min 6)</span>
                        <input type="number" min="6" max="300" step="1" name="killmail_ingestion_poll_sleep_seconds" value="<?= htmlspecialchars($settingValues['killmail_ingestion_poll_sleep_seconds'] ?? '10', ENT_QUOTES) ?>" class="w-full field-input" />
                        <span class="text-xs text-muted">Continuous Python polling now defaults to 10 seconds between live-stream retry attempts after the worker reaches the R2Z2 tip or gets rate limited.</span>
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Max Sequences Per Run</span>
                        <input type="number" min="1" max="5000" step="1" name="killmail_ingestion_max_sequences_per_run" value="<?= htmlspecialchars($settingValues['killmail_ingestion_max_sequences_per_run'] ?? '120', ENT_QUOTES) ?>" class="w-full field-input" />
                    </label>
                </div>

                <div class="grid gap-4 lg:grid-cols-2">
                    <label class="block space-y-2" id="killmail-alliance-search-field">
                        <span class="text-sm text-muted">Tracked Alliances</span>
                        <textarea name="tracked_alliance_names" id="tracked_alliance_names" rows="4" class="hidden"><?= htmlspecialchars($alliancesText, ENT_QUOTES) ?></textarea>
                        <div class="flex gap-2">
                            <input
                                type="text"
                                id="tracked_alliance_search"
                                autocomplete="off"
                                placeholder="Search alliances by name or add an exact alliance ID"
                                class="w-full field-input"
                            />
                            <button type="button" id="tracked_alliance_add" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm text-slate-100 transition hover:bg-white/5">Add</button>
                        </div>
                        <p id="tracked_alliance_status" class="text-xs text-muted">
                            <?= htmlspecialchars($trackedAllianceSelections === []
                                ? 'Search by name, or add an exact numeric alliance ID.'
                                : ('Tracking ' . count($trackedAllianceSelections) . ' alliance' . (count($trackedAllianceSelections) === 1 ? '' : 's') . '.'), ENT_QUOTES) ?>
                        </p>
                        <ul id="tracked_alliance_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                        <div id="tracked_alliance_selected" class="space-y-2 rounded-lg border border-border bg-black/10 p-3"></div>
                    </label>

                    <label class="block space-y-2" id="killmail-corporation-search-field">
                        <span class="text-sm text-muted">Tracked Corporations</span>
                        <textarea name="tracked_corporation_names" id="tracked_corporation_names" rows="4" class="hidden"><?= htmlspecialchars($corporationsText, ENT_QUOTES) ?></textarea>
                        <div class="flex gap-2">
                            <input
                                type="text"
                                id="tracked_corporation_search"
                                autocomplete="off"
                                placeholder="Search corporations by name or add an exact corporation ID"
                                class="w-full field-input"
                            />
                            <button type="button" id="tracked_corporation_add" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm text-slate-100 transition hover:bg-white/5">Add</button>
                        </div>
                        <p id="tracked_corporation_status" class="text-xs text-muted">
                            <?= htmlspecialchars($trackedCorporationSelections === []
                                ? 'Search by name, or add an exact numeric corporation ID.'
                                : ('Tracking ' . count($trackedCorporationSelections) . ' corporation' . (count($trackedCorporationSelections) === 1 ? '' : 's') . '.'), ENT_QUOTES) ?>
                        </p>
                        <ul id="tracked_corporation_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                        <div id="tracked_corporation_selected" class="space-y-2 rounded-lg border border-border bg-black/10 p-3"></div>
                    </label>
                </div>

                <div class="rounded-lg border border-border bg-black/20 p-3 text-sm text-muted space-y-2">
                    <p>Add alliances and corporations from the lookup, or enter exact numeric IDs when you already know them.</p>
                    <p>The continuous zKill worker keeps a killmail only when the victim belongs to one of these tracked entities. Attacker-only matches are ignored for the tracked loss board.</p>
                    <p>Each saved entry is stored locally as <span class="text-slate-100">ID + name</span>, and you can remove anything here before saving if an alliance or corporation leaves the group you care about.</p>
                </div>

                <script>
                    (() => {
                        const createTrackedEntityPicker = ({
                            inputId,
                            addButtonId,
                            hiddenId,
                            resultsId,
                            statusId,
                            selectedId,
                            allowedType,
                            initialItems,
                        }) => {
                            const input = document.getElementById(inputId);
                            const addButton = document.getElementById(addButtonId);
                            const hidden = document.getElementById(hiddenId);
                            const results = document.getElementById(resultsId);
                            const status = document.getElementById(statusId);
                            const selected = document.getElementById(selectedId);

                            if (!input || !addButton || !hidden || !results || !status || !selected) {
                                return;
                            }

                            const items = new Map();
                            let debounceTimer = null;

                            const syncHiddenField = () => {
                                hidden.value = Array.from(items.values())
                                    .map((item) => String(item.id) + ' | ' + item.name)
                                    .join('\n');
                            };

                            const defaultStatus = () => items.size === 0
                                ? 'Search by name, or add an exact numeric ' + allowedType.toLowerCase() + ' ID.'
                                : 'Tracking ' + items.size + ' ' + allowedType.toLowerCase() + (items.size === 1 ? '' : 's') + '. Remove any that are no longer relevant before saving.';

                            const updateStatus = (message = null) => {
                                status.textContent = message ?? defaultStatus();
                            };

                            const clearResults = () => {
                                results.innerHTML = '';
                                results.classList.add('hidden');
                            };

                            const parseDirectEntry = (value) => {
                                const match = value.trim().match(/^([1-9][0-9]{0,19})(?:\s*[|,:-]\s*(.+))?$/);
                                if (!match) {
                                    return null;
                                }

                                const id = Number(match[1]);
                                if (!Number.isFinite(id) || id <= 0) {
                                    return null;
                                }

                                const label = String(match[2] || '').trim();

                                return {
                                    id,
                                    name: label,
                                    type: allowedType,
                                    labelProvided: label !== '',
                                };
                            };

                            const renderSelected = () => {
                                selected.innerHTML = '';

                                if (items.size === 0) {
                                    const empty = document.createElement('p');
                                    empty.className = 'text-xs text-muted';
                                    empty.textContent = 'No ' + allowedType.toLowerCase() + 's selected yet.';
                                    selected.appendChild(empty);
                                    syncHiddenField();
                                    updateStatus();
                                    return;
                                }

                                Array.from(items.values())
                                    .sort((a, b) => a.name.localeCompare(b.name))
                                    .forEach((item) => {
                                        const row = document.createElement('div');
                                        row.className = 'flex items-center justify-between gap-3 rounded-lg border border-border bg-black/20 px-3 py-2';

                                        const meta = document.createElement('div');
                                        meta.className = 'min-w-0';

                                        const name = document.createElement('p');
                                        name.className = 'truncate text-sm text-slate-100';
                                        name.textContent = item.name;

                                        const details = document.createElement('p');
                                        details.className = 'text-xs text-muted';
                                        details.textContent = allowedType + ' · #' + item.id;

                                        meta.appendChild(name);
                                        meta.appendChild(details);

                                        const remove = document.createElement('button');
                                        remove.type = 'button';
                                        remove.className = 'rounded-lg border border-border px-3 py-1 text-xs text-muted transition hover:bg-white/5 hover:text-slate-100';
                                        remove.setAttribute('aria-label', 'Remove ' + item.name);
                                        remove.textContent = 'Remove';
                                        remove.addEventListener('click', () => {
                                            items.delete(String(item.id));
                                            renderSelected();
                                        });

                                        row.appendChild(meta);
                                        row.appendChild(remove);
                                        selected.appendChild(row);
                                    });

                                syncHiddenField();
                                updateStatus();
                            };

                            const addItem = (item) => {
                                if (!item || String(item.type || '') !== allowedType) {
                                    return false;
                                }

                                const id = Number(item.id || 0);
                                const name = String(item.name || '').trim() || (allowedType + ' #' + id);
                                if (!Number.isFinite(id) || id <= 0) {
                                    return false;
                                }

                                items.set(String(id), { id, name, type: allowedType });
                                input.value = '';
                                clearResults();
                                renderSelected();
                                updateStatus('Added ' + name + ' (#' + id + ').');
                                return true;
                            };

                            const renderResults = (rows, message = null) => {
                                clearResults();

                                const options = Array.isArray(rows)
                                    ? rows.filter((row) => String(row.type || '') === allowedType && !items.has(String(row.id || '')))
                                    : [];

                                if (options.length === 0) {
                                    updateStatus(message || ('No matching ' + allowedType.toLowerCase() + 's found.'));
                                    return;
                                }

                                const fragment = document.createDocumentFragment();
                                options.forEach((item) => {
                                    const row = document.createElement('li');
                                    const button = document.createElement('button');
                                    button.type = 'button';
                                    button.className = 'flex w-full flex-col items-start gap-1 px-3 py-2 text-left text-sm hover:bg-white/5';

                                    const title = document.createElement('span');
                                    title.className = 'text-slate-100';
                                    title.textContent = item.name;

                                    const details = document.createElement('span');
                                    details.className = 'text-xs text-muted';
                                    details.textContent = item.type + ' · #' + item.id;

                                    button.appendChild(title);
                                    button.appendChild(details);
                                    button.addEventListener('click', () => addItem(item));
                                    row.appendChild(button);
                                    fragment.appendChild(row);
                                });

                                results.appendChild(fragment);
                                results.classList.remove('hidden');
                                updateStatus('Select a ' + allowedType.toLowerCase() + ' from the list, or press Add to use the top result.');
                            };

                            const fetchResults = async (query, { autoAddFirst = false, fallbackItem = null } = {}) => {
                                const response = await fetch('/settings/killmail-entities.php?q=' + encodeURIComponent(query) + '&type=' + encodeURIComponent(allowedType.toLowerCase()), {
                                    headers: { 'Accept': 'application/json' },
                                });
                                const payload = await response.json();
                                if (!response.ok) {
                                    throw new Error(payload.error || 'Lookup failed.');
                                }

                                const rows = Array.isArray(payload.results) ? payload.results : [];
                                const options = rows.filter((row) => String(row.type || '') === allowedType && !items.has(String(row.id || '')));

                                if (autoAddFirst) {
                                    if (options[0]) {
                                        addItem(options[0]);
                                    } else if (fallbackItem !== null) {
                                        addItem(fallbackItem);
                                        updateStatus('Added ' + allowedType.toLowerCase() + ' #' + fallbackItem.id + ' without a resolved name.');
                                    } else {
                                        clearResults();
                                        updateStatus(payload.message || ('No matching ' + allowedType.toLowerCase() + 's found.'));
                                    }
                                    return;
                                }

                                renderResults(rows, payload.message || null);
                            };

                            const runLookup = async ({ autoAddFirst = false } = {}) => {
                                const query = input.value.trim();
                                if (query === '') {
                                    updateStatus('Enter an ' + allowedType.toLowerCase() + ' name or an exact numeric ID.');
                                    return;
                                }

                                const direct = parseDirectEntry(query);
                                if (direct !== null) {
                                    if (direct.labelProvided) {
                                        addItem(direct);
                                        return;
                                    }

                                    try {
                                        await fetchResults(query, {
                                            autoAddFirst: true,
                                            fallbackItem: {
                                                id: direct.id,
                                                name: '',
                                                type: allowedType,
                                            },
                                        });
                                    } catch (error) {
                                        clearResults();
                                        updateStatus(error instanceof Error ? error.message : 'Lookup failed.');
                                    }
                                    return;
                                }

                                updateStatus('Searching ' + allowedType.toLowerCase() + 's…');

                                try {
                                    await fetchResults(query, { autoAddFirst });
                                } catch (error) {
                                    clearResults();
                                    updateStatus(error instanceof Error ? error.message : 'Lookup failed.');
                                }
                            };

                            input.addEventListener('input', () => {
                                const query = input.value.trim();

                                if (debounceTimer !== null) {
                                    clearTimeout(debounceTimer);
                                }

                                if (query === '') {
                                    clearResults();
                                    updateStatus();
                                    return;
                                }

                                const direct = parseDirectEntry(query);
                                if (direct !== null) {
                                    clearResults();
                                    updateStatus(direct.labelProvided
                                        ? ('Press Add to include ' + allowedType.toLowerCase() + ' #' + direct.id + '.')
                                        : ('Press Add to resolve and include ' + allowedType.toLowerCase() + ' #' + direct.id + '.'));
                                    return;
                                }

                                if (query.length < 2) {
                                    clearResults();
                                    updateStatus('Type at least 2 characters to search ' + allowedType.toLowerCase() + 's by name, or enter an exact numeric ID.');
                                    return;
                                }

                                debounceTimer = window.setTimeout(() => {
                                    void runLookup();
                                }, 250);
                            });

                            input.addEventListener('keydown', (event) => {
                                if (event.key !== 'Enter') {
                                    return;
                                }

                                event.preventDefault();
                                void runLookup({ autoAddFirst: true });
                            });

                            addButton.addEventListener('click', () => {
                                void runLookup({ autoAddFirst: true });
                            });

                            document.addEventListener('click', (event) => {
                                if (!results.contains(event.target) && event.target !== input) {
                                    clearResults();
                                }
                            });

                            initialItems.forEach((item) => {
                                const id = Number(item.id || 0);
                                const name = String(item.name || '').trim();
                                if (!Number.isFinite(id) || id <= 0 || name === '') {
                                    return;
                                }

                                items.set(String(id), { id, name, type: allowedType });
                            });

                            renderSelected();
                        };

                        createTrackedEntityPicker({
                            inputId: 'tracked_alliance_search',
                            addButtonId: 'tracked_alliance_add',
                            hiddenId: 'tracked_alliance_names',
                            resultsId: 'tracked_alliance_results',
                            statusId: 'tracked_alliance_status',
                            selectedId: 'tracked_alliance_selected',
                            allowedType: 'Alliance',
                            initialItems: <?= json_encode($trackedAllianceSelections, JSON_THROW_ON_ERROR) ?>,
                        });

                        createTrackedEntityPicker({
                            inputId: 'tracked_corporation_search',
                            addButtonId: 'tracked_corporation_add',
                            hiddenId: 'tracked_corporation_names',
                            resultsId: 'tracked_corporation_results',
                            statusId: 'tracked_corporation_status',
                            selectedId: 'tracked_corporation_selected',
                            allowedType: 'Corporation',
                            initialItems: <?= json_encode($trackedCorporationSelections, JSON_THROW_ON_ERROR) ?>,
                        });
                    })();
                </script>

                <details class="rounded-xl border border-border bg-black/20 p-4">
                    <summary class="cursor-pointer list-none text-sm font-medium text-slate-100">Advanced diagnostics</summary>
                    <div class="mt-4 grid gap-4 lg:grid-cols-2 text-sm text-muted">
                        <div class="space-y-1 rounded-lg border border-border bg-black/20 p-3">
                            <p><span class="text-slate-100">Last cursor:</span> <?= htmlspecialchars((string) ($statusState['last_cursor'] ?? '-'), ENT_QUOTES) ?></p>
                            <p><span class="text-slate-100">Last success:</span> <?= htmlspecialchars((string) ($statusState['last_success_at'] ?? '-'), ENT_QUOTES) ?></p>
                            <p><span class="text-slate-100">Last status:</span> <?= htmlspecialchars((string) ($statusState['status'] ?? 'idle'), ENT_QUOTES) ?></p>
                            <p><span class="text-slate-100">Latest ingested sequence:</span> <?= htmlspecialchars((string) ($killmailStatus['max_sequence_id'] ?? '-'), ENT_QUOTES) ?></p>
                            <p><span class="text-slate-100">Latest uploaded_at:</span> <?= htmlspecialchars((string) ($killmailStatus['max_uploaded_at'] ?? '-'), ENT_QUOTES) ?></p>
                        </div>
                        <div class="space-y-1 rounded-lg border border-border bg-black/20 p-3">
                            <p><span class="text-slate-100">Worker heartbeat:</span> <?= htmlspecialchars((string) ($killmailWorkerStatus['seen_at_relative'] ?? 'No heartbeat'), ENT_QUOTES) ?></p>
                            <p><span class="text-slate-100">Worker rows:</span> seen <?= number_format((int) ($killmailWorkerStatus['rows_seen'] ?? 0)) ?> · matched <?= number_format((int) ($killmailWorkerStatus['rows_matched'] ?? 0)) ?> · skipped existing <?= number_format((int) ($killmailWorkerStatus['rows_skipped_existing'] ?? 0)) ?> · filtered <?= number_format((int) ($killmailWorkerStatus['rows_filtered_out'] ?? 0)) ?> · written <?= number_format((int) ($killmailWorkerStatus['rows_written'] ?? 0)) ?></p>
                            <p><span class="text-slate-100">Cursor movement:</span> <?= htmlspecialchars((string) (($killmailWorkerStatus['cursor_before'] ?? '') !== '' ? $killmailWorkerStatus['cursor_before'] : '—'), ENT_QUOTES) ?> → <?= htmlspecialchars((string) (($killmailWorkerStatus['cursor_after'] ?? '') !== '' ? $killmailWorkerStatus['cursor_after'] : ($killmailWorkerStatus['cursor'] ?? '—')), ENT_QUOTES) ?></p>
                            <?php if (trim((string) ($killmailWorkerStatus['outcome_reason'] ?? '')) !== ''): ?>
                                <p><span class="text-slate-100">Latest worker reason:</span> <?= htmlspecialchars((string) ($killmailWorkerStatus['outcome_reason'] ?? ''), ENT_QUOTES) ?></p>
                            <?php endif; ?>
                        </div>
                        <label class="block space-y-2 lg:col-span-2">
                            <span class="text-sm text-muted">Demand Prediction Mode</span>
                            <input type="text" name="killmail_demand_prediction_mode" value="<?= htmlspecialchars($settingValues['killmail_demand_prediction_mode'] ?? 'baseline', ENT_QUOTES) ?>" class="w-full field-input" />
                            <span class="text-xs text-muted">Keep future-facing tuning here so the default settings view stays focused on freshness, tracked entities, and whether ingestion is working.</span>
                        </label>
                    </div>
                </details>

                <p class="text-sm text-muted">Ingestion consumes R2Z2 as an ordered stream and keeps killmails only when a tracked alliance or corporation appears on the victim side. That keeps the board focused on tracked losses while leaving attacker details available only inside each stored loss.</p>
                <button class="btn-primary">Save Killmail Intelligence Settings</button>
            </form>
        <?php elseif ($section === 'esi-login'): ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="esi-login">
                <label class="block space-y-2">
                    <span class="text-sm text-muted">ESI Client ID</span>
                    <input name="esi_client_id" value="<?= htmlspecialchars($settingValues['esi_client_id'] ?? '', ENT_QUOTES) ?>" class="w-full field-input" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">ESI Client Secret</span>
                    <input name="esi_client_secret" type="password" value="<?= htmlspecialchars($settingValues['esi_client_secret'] ?? '', ENT_QUOTES) ?>" class="w-full field-input" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Callback URL</span>
                    <input name="esi_callback_url" value="<?= htmlspecialchars($settingValues['esi_callback_url'] ?? base_url('/callback'), ENT_QUOTES) ?>" class="w-full field-input" />
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Enabled Scopes (space separated)</span>
                    <textarea name="esi_scopes" rows="4" class="w-full field-input"><?= htmlspecialchars($settingValues['esi_scopes'] ?? implode(' ', esi_default_scopes()), ENT_QUOTES) ?></textarea>
                </label>
                <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <input type="checkbox" name="esi_enabled" value="1" <?= ($settingValues['esi_enabled'] ?? '0') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                    <span class="text-sm">Enable ESI OAuth login</span>
                </label>
                <div class="flex flex-wrap items-center gap-3">
                    <button class="btn-primary">Save ESI Login Settings</button>
                    <?php if (($settingValues['esi_enabled'] ?? '0') === '1' && ($settingValues['esi_client_id'] ?? '') !== ''): ?>
                        <a class="rounded-lg border border-border px-4 py-2 text-sm hover:bg-white/5" href="<?= htmlspecialchars(esi_sso_authorize_url(), ENT_QUOTES) ?>">Connect ESI Character</a>
                    <?php endif; ?>
                </div>
            </form>

            <div class="mt-6 surface-tertiary text-sm text-muted">
                <p class="font-medium text-slate-200">ESI OAuth Status</p>
                <?php if ($latestEsiToken === null): ?>
                    <p class="mt-2">No ESI token is stored yet.</p>
                <?php else: ?>
                    <p class="mt-2">Connected character: <span class="text-slate-100"><?= htmlspecialchars($latestEsiToken['character_name'], ENT_QUOTES) ?></span> (<?= (int) $latestEsiToken['character_id'] ?>)</p>
                    <p class="mt-1">Token expires at (UTC): <?= htmlspecialchars($latestEsiToken['expires_at'], ENT_QUOTES) ?></p>
                    <p class="mt-1">Scopes: <?= htmlspecialchars($latestEsiToken['scopes'], ENT_QUOTES) ?></p>
                    <?php if ($missingStructureScopes !== []): ?>
                        <div class="mt-3 rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-amber-100">
                            Required scopes are missing: <span class="font-medium"><?= htmlspecialchars(implode(', ', $missingStructureScopes), ENT_QUOTES) ?></span>.
                            Add <span class="font-medium">esi-universe.read_structures.v1</span> and <span class="font-medium">esi-markets.structure_markets.v1</span> in the scopes field, save, then reconnect your character.
                        </div>
                    <?php endif; ?>
                <?php endif; ?>
            </div>
        <?php elseif ($section === 'deal-alerts'): ?>
            <form class="mt-6 space-y-5" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="deal-alerts">

                <div class="grid gap-4 xl:grid-cols-[minmax(0,1.2fr)_minmax(300px,0.8fr)]">
                    <div class="space-y-4">
                        <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                            <input type="hidden" name="deal_alerts_enabled" value="0">
                            <input type="checkbox" name="deal_alerts_enabled" value="1" <?= ($dealAlertSettingValues['deal_alerts_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                            <span class="text-sm">Enable dedicated deal-alert anomaly scanning</span>
                        </label>

                        <div class="grid gap-4 md:grid-cols-2">
                            <label class="block space-y-2">
                                <span class="text-sm text-muted">Historical baseline window (days)</span>
                                <input type="number" min="3" max="60" step="1" name="deal_alert_baseline_days" value="<?= htmlspecialchars($dealAlertSettingValues['deal_alert_baseline_days'] ?? '14', ENT_QUOTES) ?>" class="w-full field-input" />
                                <p class="text-xs text-muted">Uses local SupplyCore history for median and weighted-average baselines.</p>
                            </label>
                            <label class="block space-y-2">
                                <span class="text-sm text-muted">Minimum history points</span>
                                <input type="number" min="3" max="30" step="1" name="deal_alert_min_history_points" value="<?= htmlspecialchars($dealAlertSettingValues['deal_alert_min_history_points'] ?? '5', ENT_QUOTES) ?>" class="w-full field-input" />
                                <p class="text-xs text-muted">Skip alerting when local history is still too thin to form a reliable normal price.</p>
                            </label>
                        </div>

                        <div class="rounded-2xl border border-border bg-black/20 p-4">
                            <div>
                                <p class="text-sm font-semibold text-slate-100">Severity thresholds (% of normal price)</p>
                                <p class="mt-1 text-xs text-muted">Listings at or below these thresholds are promoted into escalating urgency tiers.</p>
                            </div>
                            <div class="mt-4 grid gap-4 md:grid-cols-2">
                                <label class="block space-y-2">
                                    <span class="text-sm text-muted">Critical misprice</span>
                                    <input type="number" min="0.10" max="100" step="0.10" name="deal_alert_critical_threshold_percent" value="<?= htmlspecialchars($dealAlertSettingValues['deal_alert_critical_threshold_percent'] ?? '1.00', ENT_QUOTES) ?>" class="w-full field-input" />
                                </label>
                                <label class="block space-y-2">
                                    <span class="text-sm text-muted">Very strong deal</span>
                                    <input type="number" min="0.10" max="100" step="0.10" name="deal_alert_very_strong_threshold_percent" value="<?= htmlspecialchars($dealAlertSettingValues['deal_alert_very_strong_threshold_percent'] ?? '5.00', ENT_QUOTES) ?>" class="w-full field-input" />
                                </label>
                                <label class="block space-y-2">
                                    <span class="text-sm text-muted">Strong deal</span>
                                    <input type="number" min="0.10" max="100" step="0.10" name="deal_alert_strong_threshold_percent" value="<?= htmlspecialchars($dealAlertSettingValues['deal_alert_strong_threshold_percent'] ?? '10.00', ENT_QUOTES) ?>" class="w-full field-input" />
                                </label>
                                <label class="block space-y-2">
                                    <span class="text-sm text-muted">Watch threshold</span>
                                    <input type="number" min="0.10" max="100" step="0.10" name="deal_alert_watch_threshold_percent" value="<?= htmlspecialchars($dealAlertSettingValues['deal_alert_watch_threshold_percent'] ?? '15.00', ENT_QUOTES) ?>" class="w-full field-input" />
                                </label>
                            </div>
                        </div>
                    </div>

                    <div class="space-y-4 rounded-2xl border border-border bg-black/20 p-4">
                        <div>
                            <p class="text-sm font-semibold text-slate-100">Popup behavior</p>
                            <p class="mt-1 text-xs text-muted">Keep the alert obvious, but only when urgency is high enough to act immediately.</p>
                        </div>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Show popup for</span>
                            <select name="deal_alert_popup_min_severity" class="w-full field-input">
                                <?php foreach (deal_alert_popup_severity_options() as $value => $label): ?>
                                    <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= ($dealAlertSettingValues['deal_alert_popup_min_severity'] ?? 'very_strong') === $value ? 'selected' : '' ?>>
                                        <?= htmlspecialchars($label, ENT_QUOTES) ?>
                                    </option>
                                <?php endforeach; ?>
                            </select>
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Dismiss popup for (minutes)</span>
                            <input type="number" min="5" max="1440" step="5" name="deal_alert_popup_dismiss_minutes" value="<?= htmlspecialchars($dealAlertSettingValues['deal_alert_popup_dismiss_minutes'] ?? '120', ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <div class="rounded-xl border border-rose-400/20 bg-rose-500/10 p-3 text-sm text-rose-100">
                            Critical alerts compare the current cheapest sell listing against SupplyCore&apos;s own local history. The resulting popup includes item, current price, expected price, severity, market, and freshness so operators can react without opening the full market pages first.
                        </div>
                        <div class="rounded-xl border border-border bg-black/30 p-3 text-xs text-muted space-y-1">
                            <p>Reference Hub coverage uses the first-party snapshot-history table when it exists, then falls back to stored hub daily history.</p>
                            <p>Alliance Market coverage uses the alliance market daily history already collected by SupplyCore.</p>
                            <p>Deduplication is keyed by item + market + suspicious price band so identical refreshes do not spam the UI.</p>
                        </div>
                    </div>
                </div>

                <button class="btn-primary">Save Deal Alert Settings</button>
            </form>
        <?php else: ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="data-sync">
                <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <input type="checkbox" name="incremental_updates_enabled" value="1" <?= ($dataSyncSettingValues['incremental_updates_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                    <span class="text-sm">Enable incremental database updates</span>
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Incremental Strategy</span>
                    <select name="incremental_strategy" class="w-full field-input">
                        <?php foreach (incremental_strategy_options() as $value => $label): ?>
                            <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= ($dataSyncSettingValues['incremental_strategy'] ?? 'watermark_upsert') === $value ? 'selected' : '' ?>>
                                <?= htmlspecialchars($label, ENT_QUOTES) ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Delete Handling Policy</span>
                    <select name="incremental_delete_policy" class="w-full field-input">
                        <?php foreach (incremental_delete_policy_options() as $value => $label): ?>
                            <option value="<?= htmlspecialchars($value, ENT_QUOTES) ?>" <?= ($dataSyncSettingValues['incremental_delete_policy'] ?? 'reconcile') === $value ? 'selected' : '' ?>>
                                <?= htmlspecialchars($label, ENT_QUOTES) ?>
                            </option>
                        <?php endforeach; ?>
                    </select>
                </label>
                <label class="block space-y-2">
                    <span class="text-sm text-muted">Chunk Size</span>
                    <input type="number" min="100" max="10000" step="100" name="incremental_chunk_size" value="<?= htmlspecialchars($dataSyncSettingValues['incremental_chunk_size'] ?? '1000', ENT_QUOTES) ?>" class="w-full field-input" />
                </label>

                <?php $schedulerHealth = (array) ($syncDashboard['health_summary'] ?? []); ?>
                <?php $schedulerDaemon = (array) ($syncDashboard['daemon_state'] ?? ($schedulerHealth['daemon'] ?? [])); ?>
                <?php $systemStatus = (array) ($syncDashboard['system_status'] ?? []); ?>
                <?php $resourceWarnings = array_values((array) ($syncDashboard['resource_warnings'] ?? [])); ?>
                <?php $activeIssues = array_values((array) ($syncDashboard['active_issues'] ?? [])); ?>
                <?php $pipelineHealth = array_values((array) ($syncDashboard['pipeline_health'] ?? [])); ?>
                <?php $selectedProfile = (string) ($syncDashboard['selected_profile'] ?? ($dataSyncSettingValues['scheduler_operational_profile'] ?? 'medium')); ?>
                <?php $profileOptions = (array) ($syncDashboard['profile_options'] ?? scheduler_operational_profile_options()); ?>
                <?php $profileRuntime = (array) ($syncDashboard['profile_runtime'] ?? []); ?>
                <div class="space-y-4">
                    <div>
                        <p class="text-sm text-slate-100">Data freshness summary</p>
                        <p class="mt-1 text-xs text-muted">Keep the visible view centered on datasets, last successful refreshes, freshness, and only the latest failures.</p>
                    </div>
                    <div class="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                        <?php foreach ($runtimeDatasetCards as $datasetCard): ?>
                            <article class="rounded-xl border border-border bg-black/20 p-4">
                                <div class="flex items-start justify-between gap-3">
                                    <div>
                                        <p class="text-sm text-slate-100"><?= htmlspecialchars((string) ($datasetCard['label'] ?? 'Dataset'), ENT_QUOTES) ?></p>
                                        <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($datasetCard['key'] ?? 'dataset'), ENT_QUOTES) ?></p>
                                    </div>
                                    <span class="inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium uppercase tracking-[0.16em] <?= htmlspecialchars((string) ($datasetCard['freshness_tone'] ?? supplycore_operational_status_view_model('stale')['tone']), ENT_QUOTES) ?>"><?= htmlspecialchars((string) ($datasetCard['freshness_label'] ?? 'Stale'), ENT_QUOTES) ?></span>
                                </div>
                                <p class="mt-4 text-xs uppercase tracking-[0.16em] text-muted">Last successful run</p>
                                <p class="mt-2 text-sm font-semibold text-slate-50"><?= htmlspecialchars((string) ($datasetCard['last_success_relative'] ?? 'Never'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($datasetCard['last_success_at'] ?? 'Unavailable'), ENT_QUOTES) ?></p>
                                <?php if (!empty($datasetCard['show_latest_failure'])): ?>
                                    <div class="mt-4 rounded-lg border border-rose-400/40 bg-rose-500/10 p-3 text-xs text-rose-100">
                                        <p class="font-medium uppercase tracking-[0.14em] text-rose-200">Latest failure</p>
                                        <p class="mt-2"><?= htmlspecialchars((string) ($datasetCard['latest_failure_message'] ?? ''), ENT_QUOTES) ?></p>
                                    </div>
                                <?php endif; ?>
                            </article>
                        <?php endforeach; ?>
                        <?php if ($runtimeDatasetCards === []): ?>
                            <div class="rounded-xl border border-dashed border-border bg-black/20 p-4 text-sm text-muted md:col-span-2 xl:col-span-3">No dataset freshness cards are available yet.</div>
                        <?php endif; ?>
                    </div>

                    <details class="rounded-xl border border-border bg-black/20 p-4">
                        <summary class="cursor-pointer list-none">
                            <div class="flex items-start justify-between gap-3">
                                <div>
                                    <p class="text-sm text-slate-100">Advanced diagnostics</p>
                                    <p class="mt-1 text-xs text-muted">Worker runtime, scheduler internals, contention, and planner details stay behind diagnostics.</p>
                                </div>
                                <span class="inline-flex items-center rounded-full border border-border px-3 py-1 text-xs uppercase tracking-[0.16em] text-muted">expand</span>
                            </div>
                        </summary>
                        <div class="mt-4 space-y-4">
                            <div>
                                <p class="text-sm text-slate-100">Continuous worker runtime</p>
                                <p class="mt-1 text-xs text-muted">The Python worker pool owns recurring cadence, retries, and schedule rows now. The zKill stream is tracked separately as a dedicated continuous worker, while the cards below show the currently active scheduler runtime profile for normal jobs.</p>
                            </div>

                            <div class="rounded-xl border border-border bg-black/20 p-4 space-y-4">
                        <div class="flex flex-wrap items-start justify-between gap-3">
                            <div>
                                <p class="text-sm text-slate-100">Active runtime profile</p>
                                <p class="mt-1 text-xs text-muted">Profile registration happens automatically from code/bootstrap, so there is nothing to save manually here anymore.</p>
                            </div>
                            <span class="inline-flex items-center rounded-full border border-cyan-400/30 bg-cyan-500/10 px-3 py-1 text-xs font-medium uppercase tracking-[0.16em] text-cyan-100"><?= htmlspecialchars($selectedProfile, ENT_QUOTES) ?></span>
                        </div>
                        <div class="grid gap-3 lg:grid-cols-3">
                            <?php foreach ($profileOptions as $profileValue => $profileMeta): ?>
                                <?php $isActiveProfile = $selectedProfile === $profileValue; ?>
                                <div class="rounded-xl border p-4 text-sm <?= $isActiveProfile ? 'border-cyan-400/40 bg-cyan-500/10' : 'border-border bg-black/30' ?>">
                                    <div>
                                        <div class="flex items-center justify-between gap-3">
                                            <p class="font-medium text-slate-100"><?= htmlspecialchars((string) ($profileMeta['label'] ?? ucfirst($profileValue)), ENT_QUOTES) ?></p>
                                            <?php if ($isActiveProfile): ?>
                                                <span class="inline-flex items-center rounded-full border border-cyan-400/30 bg-cyan-500/10 px-2 py-0.5 text-[11px] font-medium uppercase tracking-[0.14em] text-cyan-100">active</span>
                                            <?php endif; ?>
                                        </div>
                                        <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($profileMeta['description'] ?? ''), ENT_QUOTES) ?></p>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>
                        <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-4 text-sm">
                            <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Auto concurrency</p><p class="mt-2 font-semibold text-white"><?= (int) ($profileRuntime['max_concurrent_jobs'] ?? 0) ?> workers</p></div>
                            <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">CPU budget</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars(number_format((float) ($profileRuntime['cpu_budget_percent'] ?? 0), 0), ENT_QUOTES) ?>%</p></div>
                            <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Daemon poll</p><p class="mt-2 font-semibold text-white"><?= (int) ($profileRuntime['daemon_poll_interval_seconds'] ?? 0) ?>s idle · <?= (int) ($profileRuntime['daemon_running_poll_interval_seconds'] ?? 0) ?>s active</p></div>
                            <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Self-healing</p><p class="mt-2 font-semibold text-white">Auto respawn on recycle</p></div>
                        </div>
                            </div>

                    <?php if ($syncStatusCards !== []): ?>
                        <details class="rounded-xl border border-border bg-black/20 p-4">
                            <summary class="cursor-pointer list-none">
                                <div class="flex items-start justify-between gap-3">
                                    <div>
                                        <p class="text-sm text-slate-100">Advanced diagnostics</p>
                                        <p class="mt-1 text-xs text-muted">Detailed pipeline counters and last error messages stay here unless freshness is degraded.</p>
                                    </div>
                                    <span class="inline-flex items-center rounded-full border border-border px-3 py-1 text-xs uppercase tracking-[0.16em] text-muted">expand</span>
                                </div>
                            </summary>
                            <div class="mt-4 grid gap-3 md:grid-cols-3">
                                <?php foreach ($syncStatusCards as $syncCard): ?>
                                    <?php $status = $syncCard['status']; ?>
                                    <article class="surface-tertiary text-sm">
                                        <p class="text-xs uppercase tracking-[0.16em] text-muted"><?= htmlspecialchars($syncCard['label'], ENT_QUOTES) ?></p>
                                        <p class="mt-2 text-sm text-slate-100">Last success: <?= htmlspecialchars($status['last_success_at'] ?? 'Never', ENT_QUOTES) ?></p>
                                        <p class="mt-1 text-sm text-muted">Rows written (recent runs): <?= (int) ($status['recent_rows_written'] ?? 0) ?></p>
                                        <p class="mt-1 text-xs text-rose-200">Last error: <?= htmlspecialchars($status['last_error_message'] ?? 'None', ENT_QUOTES) ?></p>
                                    </article>
                                <?php endforeach; ?>
                            </div>
                        </details>
                    <?php endif; ?>

                    <div>
                        <p class="text-sm text-slate-100">Operational jobs</p>
                        <p class="mt-1 text-xs text-muted">Each card keeps only the job name, status, last run duration, and operating state visible by default. Metrics and versions move into the expandable details.</p>
                    </div>
                    <div class="space-y-3">
                        <?php foreach ($configuredSyncJobs as $schedule): ?>
                            <?php
                                $jobState = (string) ($schedule['job_state'] ?? 'waiting');
                                $jobStateClass = $jobState === 'running'
                                    ? 'border-sky-400/40 bg-sky-500/10 text-sky-100'
                                    : ($jobState === 'blocked'
                                        ? 'border-rose-400/40 bg-rose-500/10 text-rose-100'
                                        : ($jobState === 'skipped'
                                            ? 'border-slate-400/40 bg-slate-500/10 text-slate-100'
                                            : 'border-amber-400/40 bg-amber-500/10 text-amber-100'));
                                $statusLabel = (string) ($schedule['status_label'] ?? 'No runs yet');
                                $statusClass = $statusLabel === 'Failed' || $statusLabel === 'Timed out'
                                    ? 'border-rose-400/40 bg-rose-500/10 text-rose-100'
                                    : ($statusLabel === 'Blocked'
                                        ? 'border-amber-400/40 bg-amber-500/10 text-amber-100'
                                        : 'border-border bg-black/30 text-slate-100');
                                $jobKey = (string) ($schedule['job_key'] ?? '');
                            ?>
                            <div class="rounded-xl border border-border bg-black/20 p-4">
                                <div class="flex flex-wrap items-start justify-between gap-3">
                                    <div class="space-y-2">
                                        <div class="flex flex-wrap items-center gap-2">
                                            <p class="text-sm font-medium text-slate-100"><?= htmlspecialchars((string) ($schedule['label'] ?? $schedule['job_key']), ENT_QUOTES) ?></p>
                                            <span class="inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.14em] <?= $statusClass ?>"><?= htmlspecialchars($statusLabel, ENT_QUOTES) ?></span>
                                            <span class="inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.14em] <?= $jobStateClass ?>"><?= htmlspecialchars($jobState, ENT_QUOTES) ?></span>
                                        </div>
                                        <p class="text-xs text-muted font-mono"><?= htmlspecialchars($jobKey, ENT_QUOTES) ?></p>
                                        <p class="text-xs text-muted">Runs every <?= (int) ($schedule['interval_minutes'] ?? 0) ?> min · timeout <?= (int) ($schedule['resolved_timeout_seconds'] ?? 0) ?> sec</p>
                                        <p class="text-xs text-muted">Next due: <?= htmlspecialchars((string) ($schedule['next_due_at'] ?? 'Not scheduled'), ENT_QUOTES) ?></p>
                                    </div>
                                    <div class="rounded-lg border border-border bg-black/30 p-3 text-sm">
                                        <p class="text-xs uppercase tracking-[0.16em] text-muted">Last run duration</p>
                                        <p class="mt-2 font-semibold text-white"><?= htmlspecialchars(($schedule['last_duration_seconds'] ?? null) !== null ? number_format((float) $schedule['last_duration_seconds'], 1) . 's' : '—', ENT_QUOTES) ?></p>
                                    </div>
                                </div>
                                <?php if ((array) ($schedule['issues'] ?? []) !== []): ?>
                                    <div class="mt-3 flex flex-wrap gap-2 text-xs">
                                        <?php foreach ((array) ($schedule['issues'] ?? []) as $issue): ?>
                                            <span class="inline-flex items-center rounded-full border <?= (string) ($issue['severity'] ?? 'high') === 'critical' ? 'border-rose-400/40 bg-rose-500/10 text-rose-100' : 'border-amber-400/40 bg-amber-500/10 text-amber-100' ?> px-2.5 py-1"><?= htmlspecialchars((string) ($issue['title'] ?? ''), ENT_QUOTES) ?></span>
                                        <?php endforeach; ?>
                                    </div>
                                <?php endif; ?>
                                <div class="mt-3 flex flex-wrap items-center gap-2">
                                    <?php if (!empty($schedule['needs_attention_action'])): ?>
                                        <button type="submit" name="data_sync_action" value="stop-investigate-job" class="inline-flex items-center rounded-full border border-amber-400/40 bg-amber-500/10 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.14em] text-amber-100 hover:bg-amber-500/20" formaction="/settings?section=data-sync&amp;job_action_job_key=<?= urlencode($jobKey) ?>" formnovalidate>
                                            <span>Stop &amp; investigate</span>
                                        </button>
                                    <?php endif; ?>
                                    <?php if ((string) ($schedule['last_result'] ?? '') === 'failed'): ?>
                                        <button type="submit" name="data_sync_action" value="retry-job" class="inline-flex items-center rounded-full border border-emerald-400/40 bg-emerald-500/10 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.14em] text-emerald-100 hover:bg-emerald-500/20" formaction="/settings?section=data-sync&amp;job_action_job_key=<?= urlencode($jobKey) ?>" formnovalidate>
                                            <span>Retry now</span>
                                        </button>
                                    <?php endif; ?>
                                </div>
                                <details class="mt-3 rounded-lg border border-border bg-black/30 p-3 text-xs text-muted">
                                    <summary class="cursor-pointer list-none text-slate-100">Advanced metrics</summary>
                                    <div class="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">CPU</span><span class="mt-1 block text-slate-100">last <?= htmlspecialchars(number_format((float) ($schedule['last_cpu_percent'] ?? 0), 1), ENT_QUOTES) ?>% · p95 <?= htmlspecialchars(number_format((float) ($schedule['p95_cpu_percent'] ?? 0), 1), ENT_QUOTES) ?>%</span></div>
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">Memory</span><span class="mt-1 block text-slate-100">last <?= htmlspecialchars(scheduler_format_bytes(isset($schedule['last_memory_peak_bytes']) ? (int) $schedule['last_memory_peak_bytes'] : 0), ENT_QUOTES) ?> · p95 <?= htmlspecialchars(scheduler_format_bytes(isset($schedule['p95_memory_peak_bytes']) ? (int) $schedule['p95_memory_peak_bytes'] : 0), ENT_QUOTES) ?></span></div>
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">Duration trend</span><span class="mt-1 block text-slate-100">avg <?= htmlspecialchars(($schedule['average_duration_seconds'] ?? null) !== null ? number_format((float) $schedule['average_duration_seconds'], 1) . 's' : '—', ENT_QUOTES) ?> · p95 <?= htmlspecialchars(($schedule['p95_duration_seconds'] ?? null) !== null ? number_format((float) $schedule['p95_duration_seconds'], 1) . 's' : '—', ENT_QUOTES) ?></span></div>
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">Wait pressure</span><span class="mt-1 block text-slate-100">locks <?= (int) ($schedule['lock_conflicts_recent'] ?? 0) ?> · queue <?= htmlspecialchars(number_format((float) ($schedule['last_queue_wait_seconds'] ?? 0), 1), ENT_QUOTES) ?>s</span></div>
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">Projected resources</span><span class="mt-1 block text-slate-100">CPU <?= htmlspecialchars(number_format((float) ($schedule['projected_cpu_percent'] ?? 0), 1), ENT_QUOTES) ?>% · memory <?= htmlspecialchars(scheduler_format_bytes(isset($schedule['projected_memory_bytes']) ? (int) $schedule['projected_memory_bytes'] : 0), ENT_QUOTES) ?></span></div>
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">Change-aware</span><span class="mt-1 block text-slate-100"><?= !empty($schedule['change_aware']) ? 'Yes' : 'No' ?></span><span class="mt-1 block"><?= htmlspecialchars((string) ($schedule['last_no_change_reason'] ?? ''), ENT_QUOTES) ?></span></div>
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">Versions</span><span class="mt-1 block text-slate-100">input <?= htmlspecialchars(substr((string) ($schedule['last_materialization_input_version'] ?? 'n/a'), 0, 12), ENT_QUOTES) ?></span><span class="mt-1 block">output <?= htmlspecialchars(substr((string) ($schedule['last_materialization_output_version'] ?? 'n/a'), 0, 12), ENT_QUOTES) ?></span></div>
                                        <div><span class="block text-[11px] uppercase tracking-[0.14em]">Planner context</span><span class="mt-1 block text-slate-100"><?= htmlspecialchars((string) ($schedule['capacity_reason'] ?? 'No recent planner note.'), ENT_QUOTES) ?></span></div>
                                    </div>
                                </details>
                            </div>
                        <?php endforeach; ?>
                    </div>
                        </div>
                    </details>

                    <details class="rounded-xl border border-border bg-black/20 p-4">
                        <summary class="cursor-pointer list-none">
                            <div class="flex flex-wrap items-center justify-between gap-3">
                                <div>
                                    <p class="text-sm text-slate-100">Advanced Diagnostics</p>
                                    <p class="mt-1 text-xs text-muted">Scheduler internals, daemon lease details, watchdog state, profiling context, offsets, and raw telemetry stay available here without crowding the main operational view.</p>
                                </div>
                                <span class="inline-flex items-center rounded-full border border-border px-2.5 py-1 text-[11px] uppercase tracking-[0.14em] text-muted">expand</span>
                            </div>
                        </summary>
                        <div class="mt-4 space-y-4">
                            <div class="grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
                                <article class="rounded-xl border border-border bg-black/30 p-4">
                                    <div class="flex items-start justify-between gap-3">
                                        <div>
                                            <p class="text-sm text-slate-100">Scheduler daemon state</p>
                                            <p class="mt-1 text-xs text-muted">Lease, loop counts, watchdog state, and recycle controls.</p>
                                        </div>
                                        <?php $daemonStatus = (string) ($schedulerDaemon['derived_status'] ?? 'stopped'); ?>
                                        <?php $daemonStatusClass = $daemonStatus === 'running' ? 'border-emerald-400/30 bg-emerald-500/10 text-emerald-100' : ($daemonStatus === 'degraded' ? 'border-amber-400/40 bg-amber-500/10 text-amber-100' : 'border-rose-400/40 bg-rose-500/10 text-rose-100'); ?>
                                        <span class="inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium uppercase tracking-[0.16em] <?= $daemonStatusClass ?>"><?= htmlspecialchars($daemonStatus, ENT_QUOTES) ?></span>
                                    </div>
                                    <div class="mt-4 grid gap-3 sm:grid-cols-2 text-sm">
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Owner</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($schedulerDaemon['owner_label'] ?? 'unclaimed'), ENT_QUOTES) ?></p><p class="mt-1 text-xs text-muted">PID <?= (int) ($schedulerDaemon['owner_pid'] ?? 0) ?> · <?= htmlspecialchars((string) ($schedulerDaemon['owner_hostname'] ?? 'unknown'), ENT_QUOTES) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Loop / watchdog</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($schedulerDaemon['loop_state'] ?? 'idle'), ENT_QUOTES) ?></p><p class="mt-1 text-xs text-muted">loops <?= (int) ($schedulerDaemon['current_loop_count'] ?? 0) ?> · watchdog <?= htmlspecialchars((string) ($schedulerDaemon['watchdog_status'] ?? 'unknown'), ENT_QUOTES) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Heartbeat</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($schedulerDaemon['heartbeat_at'] ?? 'never'), ENT_QUOTES) ?></p><p class="mt-1 text-xs text-muted">Age <?= isset($schedulerDaemon['heartbeat_age_seconds']) ? htmlspecialchars(human_duration_ago((int) $schedulerDaemon['heartbeat_age_seconds']), ENT_QUOTES) : '—' ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Lease</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($schedulerDaemon['lease_expires_at'] ?? 'n/a'), ENT_QUOTES) ?></p><p class="mt-1 text-xs text-muted">memory <?= htmlspecialchars(scheduler_format_bytes((int) ($schedulerDaemon['current_memory_bytes'] ?? 0)), ENT_QUOTES) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Recovery</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($schedulerDaemon['last_recovery_event'] ?? 'No recovery event recorded.'), ENT_QUOTES) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Control flags</p><p class="mt-2 font-semibold text-white">Stop requested: <?= !empty($schedulerDaemon['stop_requested']) ? 'Yes' : 'No' ?> · Restart requested: <?= !empty($schedulerDaemon['restart_requested']) ? 'Yes' : 'No' ?></p></div>
                                    </div>
                                </article>
                                <article class="rounded-xl border border-border bg-black/30 p-4">
                                    <?php $rebuildStatusValue = (string) ($rebuildStatus['status'] ?? 'idle'); ?>
                                    <?php $rebuildStatusClass = in_array($rebuildStatusValue, ['running', 'starting'], true)
                                        ? 'border-emerald-400/30 bg-emerald-500/10 text-emerald-100'
                                        : ($rebuildStatusValue === 'failed'
                                            ? 'border-rose-400/40 bg-rose-500/10 text-rose-100'
                                            : 'border-border bg-black/30 text-slate-100'); ?>
                                    <div class="flex items-start justify-between gap-3">
                                        <div>
                                            <p class="text-sm text-slate-100">Derived rebuild status</p>
                                            <p class="mt-1 text-xs text-muted">Latest Python-orchestrated rebuild heartbeat from the live status file.</p>
                                        </div>
                                        <span class="inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium uppercase tracking-[0.16em] <?= $rebuildStatusClass ?>"><?= htmlspecialchars($rebuildStatusValue, ENT_QUOTES) ?></span>
                                    </div>
                                    <div class="mt-4 grid gap-3 sm:grid-cols-2 text-sm">
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Run</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($rebuildStatus['run_id'] ?? 'No rebuild recorded'), ENT_QUOTES) ?></p><p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($rebuildStatus['mode'] ?? 'mode unknown'), ENT_QUOTES) ?> · window <?= number_format((int) ($rebuildStatus['window_days'] ?? 0)) ?>d</p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Phase</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($rebuildStatus['current_phase'] ?? 'idle'), ENT_QUOTES) ?></p><p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($rebuildStatus['dataset'] ?? 'No dataset active'), ENT_QUOTES) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Progress</p><p class="mt-2 font-semibold text-white">scanned <?= number_format((int) ($rebuildStatus['rows_scanned'] ?? 0)) ?> · written <?= number_format((int) ($rebuildStatus['rows_written'] ?? 0)) ?></p><p class="mt-1 text-xs text-muted">elapsed <?= htmlspecialchars((string) ($rebuildStatus['elapsed_seconds_display'] ?? '0s'), ENT_QUOTES) ?> · update <?= htmlspecialchars((string) ($rebuildStatus['last_progress_update_relative'] ?? 'never'), ENT_QUOTES) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Lifecycle</p><p class="mt-2 font-semibold text-white"><?= htmlspecialchars((string) ($rebuildStatus['started_at'] ?? 'not started'), ENT_QUOTES) ?></p><p class="mt-1 text-xs text-muted">updated <?= htmlspecialchars((string) ($rebuildStatus['updated_at_relative'] ?? 'never'), ENT_QUOTES) ?><?= trim((string) ($rebuildStatus['completed_at'] ?? '')) !== '' ? ' · completed ' . htmlspecialchars((string) ($rebuildStatus['completed_at'] ?? ''), ENT_QUOTES) : '' ?></p></div>
                                    </div>
                                    <?php if (trim((string) ($rebuildStatus['error_message'] ?? '')) !== ''): ?>
                                        <div class="mt-3 rounded-lg border border-rose-400/30 bg-rose-500/10 p-3 text-sm text-rose-100">
                                            <?= htmlspecialchars((string) ($rebuildStatus['error_message'] ?? ''), ENT_QUOTES) ?>
                                        </div>
                                    <?php endif; ?>
                                </article>
                            </div>

                            <div class="grid gap-4 xl:grid-cols-[1fr_1fr]">
                                <article class="rounded-xl border border-border bg-black/30 p-4">
                                    <p class="text-sm text-slate-100">Running jobs</p>
                                    <p class="mt-1 text-xs text-muted">Live projected workload if the scheduler is actively dispatching work.</p>
                                    <div class="mt-4 space-y-2 text-sm">
                                        <?php if (((array) ($syncDashboard['running_jobs'] ?? [])) === []): ?>
                                            <div class="rounded-lg border border-dashed border-border bg-black/30 p-3 text-muted">No scheduler workloads are currently running.</div>
                                        <?php endif; ?>
                                        <?php foreach ((array) ($syncDashboard['running_jobs'] ?? []) as $runningJob): ?>
                                            <div class="rounded-lg border border-border bg-black/30 p-3">
                                                <div class="flex items-center justify-between gap-3"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($runningJob['job_key'] ?? ''), ENT_QUOTES) ?></span><span class="text-xs uppercase tracking-[0.14em] text-muted"><?= htmlspecialchars((string) ($runningJob['resource_class'] ?? 'medium'), ENT_QUOTES) ?></span></div>
                                                <p class="mt-1 text-xs text-muted">CPU <?= htmlspecialchars(number_format((float) ($runningJob['projected_cpu_percent'] ?? 0), 1), ENT_QUOTES) ?>% · memory <?= htmlspecialchars(scheduler_format_bytes(isset($runningJob['projected_memory_bytes']) ? (int) ($runningJob['projected_memory_bytes']) : 0), ENT_QUOTES) ?> · started <?= htmlspecialchars((string) ($runningJob['started_at'] ?? 'unknown'), ENT_QUOTES) ?></p>
                                            </div>
                                        <?php endforeach; ?>
                                    </div>
                                </article>
                                <article class="rounded-xl border border-border bg-black/30 p-4">
                                    <p class="text-sm text-slate-100">Change-aware decisions</p>
                                    <div class="mt-4 grid gap-3 sm:grid-cols-2 xl:grid-cols-4 text-sm">
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Change-aware jobs</p><p class="mt-2 text-2xl font-semibold text-white"><?= (int) (($syncDashboard['change_aware_summary']['change_aware_jobs'] ?? 0)) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Skipped (no change)</p><p class="mt-2 text-2xl font-semibold text-white"><?= (int) (($syncDashboard['change_aware_summary']['skipped_no_change'] ?? 0)) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Skipped (within freshness window)</p><p class="mt-2 text-2xl font-semibold text-white"><?= (int) (($syncDashboard['change_aware_summary']['skipped_within_freshness_window'] ?? 0)) ?></p></div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3"><p class="text-xs uppercase tracking-[0.16em] text-muted">Forced refreshes</p><p class="mt-2 text-2xl font-semibold text-white"><?= (int) (($syncDashboard['change_aware_summary']['forced_refresh_due_to_staleness'] ?? 0)) ?></p></div>
                                    </div>
                                </article>
                                <article class="rounded-xl border border-border bg-black/30 p-4">
                                    <p class="text-sm text-slate-100">Recent scheduler actions</p>
                                    <div class="mt-4 space-y-2 text-xs text-muted">
                                        <?php foreach (array_slice((array) ($syncDashboard['recent_actions'] ?? []), 0, 8) as $action): ?>
                                            <div class="rounded-lg border border-border bg-black/30 p-3">
                                                <div class="flex flex-wrap items-center justify-between gap-2"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($action['job_key'] ?? ''), ENT_QUOTES) ?></span><span><?= htmlspecialchars((string) ($action['created_at'] ?? ''), ENT_QUOTES) ?></span></div>
                                                <p class="mt-1 text-slate-100"><?= htmlspecialchars((string) ($action['reason_text'] ?? ''), ENT_QUOTES) ?></p>
                                                <p class="mt-1">Actor <?= htmlspecialchars((string) ($action['actor'] ?? 'system'), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($action['action_type'] ?? ''), ENT_QUOTES) ?></p>
                                            </div>
                                        <?php endforeach; ?>
                                    </div>
                                </article>
                            </div>

                            <div class="grid gap-4 xl:grid-cols-2">
                                <div>
                                    <p class="text-sm text-slate-100">Discovered jobs</p>
                                    <div class="mt-2 space-y-2 text-xs text-muted">
                                        <?php if ($discoveredSyncJobs === []): ?>
                                            <div class="rounded-lg border border-dashed border-border bg-black/30 p-3">No extra discovered jobs are waiting for review.</div>
                                        <?php endif; ?>
                                        <?php foreach ($discoveredSyncJobs as $schedule): ?>
                                            <div class="rounded-lg border border-border bg-black/30 p-3">
                                                <div class="flex items-center justify-between gap-2"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($schedule['label'] ?? $schedule['job_key']), ENT_QUOTES) ?></span><span>discovered</span></div>
                                                <p class="mt-1"><?= htmlspecialchars((string) ($schedule['job_key'] ?? ''), ENT_QUOTES) ?></p>
                                            </div>
                                        <?php endforeach; ?>
                                    </div>
                                </div>
                                <div>
                                    <p class="text-sm text-slate-100">Internal jobs</p>
                                    <div class="mt-2 space-y-2 text-xs text-muted">
                                        <?php if ($internalSyncJobs === []): ?>
                                            <div class="rounded-lg border border-dashed border-border bg-black/30 p-3">No internal scheduler mechanics were surfaced by discovery.</div>
                                        <?php endif; ?>
                                        <?php foreach ($internalSyncJobs as $schedule): ?>
                                            <div class="rounded-lg border border-border bg-black/30 p-3">
                                                <div class="flex items-center justify-between gap-2"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($schedule['label'] ?? $schedule['job_key']), ENT_QUOTES) ?></span><span>internal</span></div>
                                                <p class="mt-1"><?= htmlspecialchars((string) ($schedule['job_key'] ?? ''), ENT_QUOTES) ?></p>
                                            </div>
                                        <?php endforeach; ?>
                                    </div>
                                </div>
                            </div>

                            <div class="grid gap-4 xl:grid-cols-2">
                                <article class="rounded-xl border border-border bg-black/30 p-4">
                                    <p class="text-sm text-slate-100">Profiling & schedule snapshots</p>
                                    <div class="mt-4 space-y-3 text-xs text-muted">
                                        <div class="rounded-lg border border-border bg-black/30 p-3">
                                            <p class="font-medium text-slate-100">Active profiling run</p>
                                            <p class="mt-1"><?= $profilingActiveRun !== null ? htmlspecialchars((string) ($profilingActiveRun['current_phase'] ?? $profilingActiveRun['run_status'] ?? 'active'), ENT_QUOTES) : 'No active profiling run.' ?></p>
                                        </div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3">
                                            <p class="font-medium text-slate-100">Latest profiling runs</p>
                                            <div class="mt-2 space-y-2">
                                                <?php foreach (array_slice($profilingRuns, 0, 3) as $profilingRun): ?>
                                                    <div>
                                                        <p class="text-slate-100"><?= htmlspecialchars((string) ($profilingRun['run_status'] ?? 'unknown'), ENT_QUOTES) ?></p>
                                                        <p><?= htmlspecialchars((string) ($profilingRun['created_at'] ?? ''), ENT_QUOTES) ?></p>
                                                    </div>
                                                <?php endforeach; ?>
                                                <?php if ($profilingRuns === []): ?>
                                                    <p>No profiling history available.</p>
                                                <?php endif; ?>
                                            </div>
                                        </div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3">
                                            <p class="font-medium text-slate-100">Recent schedule snapshots</p>
                                            <div class="mt-2 space-y-2">
                                                <?php foreach (array_slice($scheduleSnapshots, 0, 3) as $snapshot): ?>
                                                    <div>
                                                        <p class="text-slate-100"><?= htmlspecialchars((string) ($snapshot['snapshot_label'] ?? $snapshot['created_at'] ?? 'snapshot'), ENT_QUOTES) ?></p>
                                                        <p><?= htmlspecialchars((string) ($snapshot['created_at'] ?? ''), ENT_QUOTES) ?></p>
                                                    </div>
                                                <?php endforeach; ?>
                                                <?php if ($scheduleSnapshots === []): ?>
                                                    <p>No schedule snapshots recorded.</p>
                                                <?php endif; ?>
                                            </div>
                                        </div>
                                    </div>
                                </article>
                                <article class="rounded-xl border border-border bg-black/30 p-4">
                                    <p class="text-sm text-slate-100">Offsets & detailed logs</p>
                                    <div class="mt-4 space-y-3 text-xs text-muted">
                                        <div class="rounded-lg border border-border bg-black/30 p-3">
                                            <p class="font-medium text-slate-100">Protected offsets</p>
                                            <div class="mt-2 space-y-2">
                                                <?php foreach (array_slice(array_values(array_filter($configuredSyncJobs, static fn (array $job): bool => !empty($job['protected_offset']))), 0, 5) as $offsetJob): ?>
                                                    <div class="flex items-center justify-between gap-2">
                                                        <span class="text-slate-100"><?= htmlspecialchars((string) ($offsetJob['label'] ?? $offsetJob['job_key']), ENT_QUOTES) ?></span>
                                                        <span>offset <?= (int) ($offsetJob['offset_minutes'] ?? 0) ?> min</span>
                                                    </div>
                                                <?php endforeach; ?>
                                                <?php if (array_values(array_filter($configuredSyncJobs, static fn (array $job): bool => !empty($job['protected_offset']))) === []): ?>
                                                    <p>No protected offsets highlighted.</p>
                                                <?php endif; ?>
                                            </div>
                                        </div>
                                        <div class="rounded-lg border border-border bg-black/30 p-3">
                                            <p class="font-medium text-slate-100">Daemon log</p>
                                            <p class="mt-1 font-mono text-slate-100"><?= htmlspecialchars(scheduler_daemon_log_label(), ENT_QUOTES) ?></p>
                                            <p class="mt-1">Last exit: <?= htmlspecialchars((string) ($schedulerDaemon['last_exit_reason'] ?? 'n/a'), ENT_QUOTES) ?></p>
                                        </div>
                                    </div>
                                </article>
                            </div>

                            <div class="grid gap-4 xl:grid-cols-2">
                                <div>
                                    <p class="text-sm text-slate-100">Recent planner decisions</p>
                                    <div class="mt-2 space-y-2 text-xs text-muted">
                                        <?php foreach (array_slice((array) ($syncDashboard['recent_planner_decisions'] ?? []), 0, 10) as $decision): ?>
                                            <?php $decisionJson = json_decode((string) ($decision['decision_json'] ?? 'null'), true); ?>
                                            <div class="rounded-lg border border-border bg-black/30 p-3">
                                                <div class="flex items-center justify-between gap-2"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($decision['job_key'] ?? ''), ENT_QUOTES) ?></span><span><?= htmlspecialchars((string) ($decision['decision_type'] ?? ''), ENT_QUOTES) ?></span></div>
                                                <p class="mt-1"><?= htmlspecialchars((string) ($decision['reason_text'] ?? ''), ENT_QUOTES) ?></p>
                                                <p class="mt-1">CPU <?= htmlspecialchars(number_format((float) ($decisionJson['projected_cpu_percent'] ?? 0), 1), ENT_QUOTES) ?>% · mem <?= htmlspecialchars(scheduler_format_bytes(isset($decisionJson['projected_memory_bytes']) ? (int) $decisionJson['projected_memory_bytes'] : 0), ENT_QUOTES) ?></p>
                                            </div>
                                        <?php endforeach; ?>
                                    </div>
                                </div>
                                <div>
                                    <p class="text-sm text-slate-100">Recent resource telemetry</p>
                                    <div class="mt-2 space-y-2 text-xs text-muted">
                                        <?php foreach (array_slice((array) ($syncDashboard['recent_resource_metrics'] ?? []), 0, 10) as $metric): ?>
                                            <div class="rounded-lg border border-border bg-black/30 p-3">
                                                <div class="flex items-center justify-between gap-2"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($metric['job_key'] ?? ''), ENT_QUOTES) ?></span><span><?= htmlspecialchars((string) ($metric['created_at'] ?? ''), ENT_QUOTES) ?></span></div>
                                                <p class="mt-1">CPU <?= htmlspecialchars(number_format((float) ($metric['cpu_percent'] ?? 0), 1), ENT_QUOTES) ?>% · memory <?= htmlspecialchars(scheduler_format_bytes(isset($metric['memory_peak_bytes']) ? (int) $metric['memory_peak_bytes'] : 0), ENT_QUOTES) ?> · overlap <?= (int) ($metric['overlap_count'] ?? 0) ?></p>
                                            </div>
                                        <?php endforeach; ?>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </details>
                </div>

                <div class="rounded-lg border border-border bg-black/20 p-3 text-sm text-muted">
                    <?php $syncEnabledSince = sanitize_backfill_start_date($settingValues['sync_automation_enabled_since'] ?? '') ?: gmdate('Y-m-d'); ?>
                    Backfill start is automatic. Pipelines begin from the date sync automation was enabled: <span class="font-medium text-slate-100"><?= htmlspecialchars($syncEnabledSince, ENT_QUOTES) ?></span>.
                </div>

                <div class="space-y-3 rounded-lg border border-border bg-black/20 p-4">
                    <div>
                        <p class="text-sm text-slate-100">Market history retention tiers</p>
                        <p class="mt-1 text-xs text-muted">The tiered model keeps raw capture tables short-lived, hourly rollups for the medium troubleshooting window, and daily history projections for the long-lived UI/reporting window.</p>
                    </div>
                    <div class="grid gap-3 xl:grid-cols-3">
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Raw snapshots (days)</span>
                            <input type="number" min="1" max="3650" step="1" name="market_history_retention_raw_days" value="<?= htmlspecialchars($dataSyncSettingValues['market_history_retention_raw_days'] ?? '30', ENT_QUOTES) ?>" class="w-full field-input" />
                            <p class="text-xs text-muted">Applies to <span class="font-mono">market_orders_history</span> and <span class="font-mono">market_order_snapshots_summary</span>.</p>
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Hourly rollups (days)</span>
                            <input type="number" min="1" max="3650" step="1" name="market_history_retention_hourly_days" value="<?= htmlspecialchars($dataSyncSettingValues['market_history_retention_hourly_days'] ?? '90', ENT_QUOTES) ?>" class="w-full field-input" />
                            <p class="text-xs text-muted">Applies to <span class="font-mono">market_item_price_1h</span> and <span class="font-mono">market_item_stock_1h</span>.</p>
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Daily history (days)</span>
                            <input type="number" min="30" max="3650" step="1" name="market_history_retention_daily_days" value="<?= htmlspecialchars($dataSyncSettingValues['market_history_retention_daily_days'] ?? '365', ENT_QUOTES) ?>" class="w-full field-input" />
                            <p class="text-xs text-muted">Applies to <span class="font-mono">market_item_price_1d</span>, <span class="font-mono">market_item_stock_1d</span>, <span class="font-mono">market_history_daily</span>, and <span class="font-mono">market_hub_local_history_daily</span>.</p>
                        </label>
                    </div>
                </div>

                <?php $partitionDiagnostics = (array) ($syncDashboard['partition_diagnostics'] ?? []); ?>
                <?php $partitionedTables = array_values((array) ($partitionDiagnostics['partitioned_tables'] ?? [])); ?>
                <?php $evaluatedPartitionTables = array_values((array) ($partitionDiagnostics['evaluation_tables'] ?? [])); ?>
                <div class="space-y-3 rounded-lg border border-border bg-black/20 p-4">
                    <div>
                        <p class="text-sm text-slate-100">Raw partition health</p>
                        <p class="mt-1 text-xs text-muted">Tracks the partitioned append-heavy raw history tables, their oldest/newest monthly ranges, retention horizon, and whether future partitions already exist.</p>
                    </div>
                    <div class="space-y-3">
                        <?php foreach ($partitionedTables as $partitionTable): ?>
                            <?php
                                $partitions = array_values((array) ($partitionTable['partitions'] ?? []));
                                $oldestPartition = is_array($partitionTable['oldest_partition'] ?? null) ? $partitionTable['oldest_partition'] : null;
                                $newestPartition = is_array($partitionTable['newest_partition'] ?? null) ? $partitionTable['newest_partition'] : null;
                                $missingFuturePartitions = array_values((array) ($partitionTable['missing_future_partitions'] ?? []));
                            ?>
                            <div class="rounded-lg border border-border bg-black/30 p-3 text-xs text-muted space-y-3">
                                <div class="flex flex-wrap items-center justify-between gap-2">
                                    <div>
                                        <p class="text-sm font-medium text-slate-100"><?= htmlspecialchars((string) ($partitionTable['logical_table'] ?? ''), ENT_QUOTES) ?></p>
                                        <p class="mt-1">Physical table <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($partitionTable['partitioned_table'] ?? ''), ENT_QUOTES) ?></span> · read mode <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($partitionTable['read_mode'] ?? 'legacy'), ENT_QUOTES) ?></span> · write mode <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($partitionTable['write_mode'] ?? 'legacy'), ENT_QUOTES) ?></span></p>
                                    </div>
                                    <span class="inline-flex items-center rounded-full border <?= !empty($partitionTable['future_partitions_exist']) ? 'border-emerald-400/40 bg-emerald-500/10 text-emerald-100' : 'border-amber-400/40 bg-amber-500/10 text-amber-100' ?> px-3 py-1 uppercase tracking-[0.16em]">
                                        <?= !empty($partitionTable['future_partitions_exist']) ? 'future ready' : 'future missing' ?>
                                    </span>
                                </div>
                                <div class="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                                    <div class="rounded-lg border border-border bg-black/20 p-3">
                                        <p class="uppercase tracking-[0.16em]">Partitions</p>
                                        <p class="mt-2 text-lg font-semibold text-white"><?= (int) ($partitionTable['partition_count'] ?? count($partitions)) ?></p>
                                    </div>
                                    <div class="rounded-lg border border-border bg-black/20 p-3">
                                        <p class="uppercase tracking-[0.16em]">Retention horizon</p>
                                        <p class="mt-2 text-sm font-medium text-slate-100"><?= htmlspecialchars((string) ($partitionTable['retention_cutoff'] ?? '-'), ENT_QUOTES) ?></p>
                                        <p class="mt-1"><?= (int) ($partitionTable['retention_days'] ?? 0) ?> days</p>
                                    </div>
                                    <div class="rounded-lg border border-border bg-black/20 p-3">
                                        <p class="uppercase tracking-[0.16em]">Oldest partition</p>
                                        <p class="mt-2 text-sm font-medium text-slate-100"><?= htmlspecialchars((string) (($oldestPartition['partition_name'] ?? '-') ?: '-'), ENT_QUOTES) ?></p>
                                        <p class="mt-1"><?= htmlspecialchars((string) (($oldestPartition['boundary_exclusive'] ?? 'MAXVALUE') ?: 'MAXVALUE'), ENT_QUOTES) ?></p>
                                    </div>
                                    <div class="rounded-lg border border-border bg-black/20 p-3">
                                        <p class="uppercase tracking-[0.16em]">Newest finite partition</p>
                                        <p class="mt-2 text-sm font-medium text-slate-100"><?= htmlspecialchars((string) (($newestPartition['partition_name'] ?? '-') ?: '-'), ENT_QUOTES) ?></p>
                                        <p class="mt-1"><?= htmlspecialchars((string) (($newestPartition['boundary_exclusive'] ?? 'MAXVALUE') ?: 'MAXVALUE'), ENT_QUOTES) ?></p>
                                    </div>
                                </div>
                                <div class="rounded-lg border border-border bg-black/20 p-3">
                                    <p><span class="text-slate-100">Approx rows:</span> <?= htmlspecialchars(number_format((int) ($partitionTable['estimated_rows'] ?? 0)), ENT_QUOTES) ?></p>
                                    <p class="mt-1"><span class="text-slate-100">Current partitions:</span> <?= htmlspecialchars($partitions === [] ? 'None' : implode(', ', array_map(static fn (array $partition): string => (string) ($partition['partition_name'] ?? ''), $partitions)), ENT_QUOTES) ?></p>
                                    <p class="mt-1"><span class="text-slate-100">Future partitions:</span> <?= htmlspecialchars($missingFuturePartitions === [] ? 'Current month + lookahead window are present.' : 'Missing ' . implode(', ', $missingFuturePartitions), ENT_QUOTES) ?></p>
                                </div>
                            </div>
                        <?php endforeach; ?>
                        <?php if ($partitionedTables === []): ?>
                            <p class="text-xs text-muted">Partition diagnostics are unavailable until the database connection is healthy.</p>
                        <?php endif; ?>
                    </div>
                    <?php if ($evaluatedPartitionTables !== []): ?>
                        <div class="rounded-lg border border-border bg-black/20 p-3 text-xs text-muted space-y-2">
                            <p class="text-sm text-slate-100">Other table evaluations</p>
                            <?php foreach ($evaluatedPartitionTables as $candidate): ?>
                                <p><span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($candidate['table'] ?? ''), ENT_QUOTES) ?></span> · approx rows <?= htmlspecialchars(number_format((int) ($candidate['estimated_rows'] ?? 0)), ENT_QUOTES) ?> · <?= htmlspecialchars((string) ($candidate['reason'] ?? ''), ENT_QUOTES) ?></p>
                            <?php endforeach; ?>
                        </div>
                    <?php endif; ?>
                </div>

                <label class="block space-y-2">
                    <span class="text-sm text-muted">Static Data JSONL ZIP Source URL</span>
                    <input type="url" name="static_data_source_url" value="<?= htmlspecialchars($dataSyncSettingValues['static_data_source_url'] ?? 'https://developers.eveonline.com/static-data/eve-online-static-data-latest-jsonl.zip', ENT_QUOTES) ?>" class="w-full field-input" />
                    <p class="text-xs text-muted">Importer expects the official CCP JSONL ZIP payload (<span class="font-mono">.zip</span>) from developers.eveonline.com.</p>
                </label>

                <div class="space-y-3 rounded-lg border border-border bg-black/20 p-4">
                    <div>
                        <p class="text-sm text-slate-100">Redis performance layer</p>
                        <p class="mt-1 text-xs text-muted">Redis stays optional and non-authoritative. MySQL remains the source of truth while Redis accelerates cached summaries, comparison defaults, metadata lookups, and lightweight distributed locks.</p>
                    </div>
                    <label class="flex items-center gap-3">
                        <input type="hidden" name="redis_cache_enabled" value="0">
                        <input type="checkbox" name="redis_cache_enabled" value="1" <?= ($dataSyncSettingValues['redis_cache_enabled'] ?? (config('redis.enabled', false) ? '1' : '0')) === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable Redis cache-aside reads</span>
                    </label>
                    <label class="flex items-center gap-3">
                        <input type="hidden" name="redis_locking_enabled" value="0">
                        <input type="checkbox" name="redis_locking_enabled" value="1" <?= ($dataSyncSettingValues['redis_locking_enabled'] ?? (config('redis.lock_enabled', true) ? '1' : '0')) === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Prefer Redis distributed locks for schedulers and expensive recomputes</span>
                    </label>
                    <div class="grid gap-3 md:grid-cols-2">
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Redis Host</span>
                            <input type="text" name="redis_host" value="<?= htmlspecialchars($dataSyncSettingValues['redis_host'] ?? (string) config('redis.host', '127.0.0.1'), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Redis Port</span>
                            <input type="number" min="1" max="65535" step="1" name="redis_port" value="<?= htmlspecialchars($dataSyncSettingValues['redis_port'] ?? (string) config('redis.port', 6379), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Redis Database</span>
                            <input type="number" min="0" max="15" step="1" name="redis_database" value="<?= htmlspecialchars($dataSyncSettingValues['redis_database'] ?? (string) config('redis.database', 0), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Redis Key Prefix</span>
                            <input type="text" name="redis_prefix" value="<?= htmlspecialchars($dataSyncSettingValues['redis_prefix'] ?? (string) config('redis.prefix', 'supplycore'), ENT_QUOTES) ?>" class="w-full field-input" />
                        </label>
                    </div>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Redis Password</span>
                        <input type="password" name="redis_password" value="<?= htmlspecialchars($dataSyncSettingValues['redis_password'] ?? '', ENT_QUOTES) ?>" class="w-full field-input" autocomplete="new-password" />
                        <p class="text-xs text-muted">Leave blank for unauthenticated local Redis deployments.</p>
                    </label>
                </div>

                <div class="space-y-3">
                    <div>
                        <p class="text-sm text-muted">Pipeline toggles</p>
                        <p class="mt-1 text-xs text-muted">Use these defaults to keep related sync pipelines enabled alongside the continuous worker automation shown above.</p>
                    </div>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="alliance_current_pipeline_enabled" value="0">
                        <input type="checkbox" name="alliance_current_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['alliance_current_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable alliance current pipeline</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="alliance_history_pipeline_enabled" value="0">
                        <input type="checkbox" name="alliance_history_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['alliance_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable alliance history/backfill pipeline</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="hub_history_pipeline_enabled" value="0">
                        <input type="checkbox" name="hub_history_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['hub_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable market hub history pipeline</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="market_hub_local_history_pipeline_enabled" value="0">
                        <input type="checkbox" name="market_hub_local_history_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['market_hub_local_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable hub snapshot-history refresh pipeline</span>
                    </label>
                </div>

                <?php if ($staticDataState !== null): ?>
                    <div class="rounded-lg border border-border bg-black/20 p-3 text-sm text-muted space-y-1">
                        <p><span class="text-slate-100">Static Data Source:</span> <?= htmlspecialchars((string) ($staticDataState['source_url'] ?? ''), ENT_QUOTES) ?></p>
                        <p><span class="text-slate-100">Remote Build:</span> <?= htmlspecialchars((string) ($staticDataState['remote_build_id'] ?? '-'), ENT_QUOTES) ?></p>
                        <p><span class="text-slate-100">Imported Build:</span> <?= htmlspecialchars((string) ($staticDataState['imported_build_id'] ?? '-'), ENT_QUOTES) ?></p>
                        <p><span class="text-slate-100">Last Status:</span> <?= htmlspecialchars((string) ($staticDataState['status'] ?? 'idle'), ENT_QUOTES) ?> (<?= htmlspecialchars((string) ($staticDataState['imported_mode'] ?? '-'), ENT_QUOTES) ?>)</p>
                    </div>
                <?php endif; ?>

                <p class="text-sm text-muted">When enabled, future import/sync jobs will only process changed rows for better scalability.</p>
                <div class="flex flex-wrap items-center gap-2">
                    <button name="data_sync_action" value="save" class="btn-primary">Save Data Sync Settings</button>
                    <select name="run_now_job_key" class="field-input" aria-label="Run a data sync job now">
                        <?php foreach ($runNowJobOptions as $jobOption): ?>
                            <option value="<?= htmlspecialchars($jobOption['job_key'], ENT_QUOTES) ?>"><?= htmlspecialchars($jobOption['label'], ENT_QUOTES) ?></option>
                        <?php endforeach; ?>
                    </select>
                    <button name="data_sync_action" value="run-now" class="rounded-lg border border-border px-4 py-2 text-sm font-medium text-slate-100 hover:bg-white/5">Run selected now</button>
                    <p class="text-xs text-muted">Local History is available in this selector and is required to populate Trend Snippets.</p>
                    <button name="data_sync_action" value="reset-scheduler" class="rounded-lg border border-amber-400/40 bg-amber-500/10 px-4 py-2 text-sm font-medium text-amber-100 hover:bg-amber-500/20">Reset scheduler locks</button>
                    <p class="text-xs text-muted">Clears stuck schedule locks and attempts to terminate related background PHP workers before the next run.</p>
                    <button name="data_sync_action" value="static-data-import" class="rounded-lg border border-border px-4 py-2 text-sm font-medium text-slate-100 hover:bg-white/5">Import EVE Static Data</button>
                </div>
            </form>
        <?php endif; ?>
    </section>
</div>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
