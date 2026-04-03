<?php

declare(strict_types=1);

require_once __DIR__ . '/../../src/bootstrap.php';

$sections = setting_sections();
$section = active_section();
$sectionAliases = setting_section_aliases();
$sectionChildren = [
    'workspace' => [
        'general' => 'Workspace Settings',
    ],
    'market-scope' => [
        'trading-stations' => 'Trading Destinations',
        'item-scope' => 'Item Scope',
    ],
    'ai-alerts' => [
        'ai-briefings' => 'AI Briefings',
        'ai-report-management' => 'Report Management',
        'deal-alerts' => 'Deal Alerts',
        'killmail-intelligence' => 'Killmail Intelligence',
    ],
    'automation-sync' => [
        'automation-control' => 'Automation Control',
        'esi-login' => 'ESI Authentication',
        'data-sync' => 'Sync Operations',
    ],
    'backup-restore' => [
        'backup-restore' => 'Backup & Restore',
    ],
    'integrations' => [
        'public-api' => 'Public API',
    ],
    'runtime-diagnostics' => [
        'runtime-config' => 'Runtime Config',
        'influxdb-test' => 'InfluxDB Test',
    ],
];
$requestedSection = (string) ($_GET['section'] ?? 'workspace');
$requestedSubsection = trim((string) ($_GET['subsection'] ?? ''));
$knownChildren = $sectionChildren[$section] ?? [];
$activeSubsection = array_key_first($knownChildren) ?? 'general';
if (array_key_exists($requestedSubsection, $knownChildren)) {
    $activeSubsection = $requestedSubsection;
} elseif (array_key_exists($requestedSection, $sectionAliases)) {
    $legacySubsection = $requestedSection;
    if (array_key_exists($legacySubsection, $knownChildren)) {
        $activeSubsection = $legacySubsection;
    }
}

$sectionUrl = static function (string $parentKey, ?string $childKey = null): string {
    $query = '/settings?section=' . urlencode($parentKey);
    if ($childKey !== null) {
        $query .= '&subsection=' . urlencode($childKey);
    }

    return $query;
};
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
        case 'runtime-config':
            $saved = save_settings(runtime_config_settings_from_request($_POST));
            if ($saved) {
                supplycore_runtime_config_refresh();
            }
            break;

        case 'influxdb-test':
            $influxTestUrl = rtrim((string) config('influxdb.url', 'http://127.0.0.1:8086'), '/');
            $influxTestToken = trim((string) config('influxdb.token', ''));
            $influxTestOrg = trim((string) config('influxdb.org', ''));
            $influxTestBucket = trim((string) config('influxdb.bucket', 'supplycore_rollups'));
            $influxTestTimeout = max(3, (int) config('influxdb.timeout_seconds', 15));
            $testResults = [];

            // 1. Health check
            $ch = curl_init($influxTestUrl . '/health');
            curl_setopt_array($ch, [
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => min($influxTestTimeout, 5),
                CURLOPT_CONNECTTIMEOUT => 3,
                CURLOPT_HTTPHEADER => array_filter([
                    'Accept: application/json',
                    $influxTestToken !== '' ? ('Authorization: Token ' . $influxTestToken) : null,
                ]),
            ]);
            $healthBody = curl_exec($ch);
            $healthCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
            $healthError = curl_error($ch);
            $healthLatency = round((float) curl_getinfo($ch, CURLINFO_TOTAL_TIME) * 1000);
            curl_close($ch);

            $healthData = is_string($healthBody) ? @json_decode($healthBody, true) : null;
            $testResults['health'] = [
                'label' => 'Health endpoint',
                'ok' => $healthCode >= 200 && $healthCode < 400,
                'detail' => $healthError !== '' ? $healthError : ('HTTP ' . $healthCode . ' · ' . ($healthData['status'] ?? 'unknown') . ' · ' . $healthLatency . 'ms'),
                'version' => $healthData['version'] ?? null,
            ];

            // 2. Auth / bucket check – list buckets filtered by name
            if ($influxTestToken !== '' && $influxTestOrg !== '') {
                $bucketsEndpoint = $influxTestUrl . '/api/v2/buckets?org=' . rawurlencode($influxTestOrg) . '&name=' . rawurlencode($influxTestBucket);
                $ch = curl_init($bucketsEndpoint);
                curl_setopt_array($ch, [
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_TIMEOUT => min($influxTestTimeout, 5),
                    CURLOPT_CONNECTTIMEOUT => 3,
                    CURLOPT_HTTPHEADER => [
                        'Authorization: Token ' . $influxTestToken,
                        'Accept: application/json',
                    ],
                ]);
                $bucketsBody = curl_exec($ch);
                $bucketsCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
                $bucketsError = curl_error($ch);
                curl_close($ch);

                $bucketsData = is_string($bucketsBody) ? @json_decode($bucketsBody, true) : null;
                $bucketFound = false;
                if (is_array($bucketsData) && is_array($bucketsData['buckets'] ?? null)) {
                    foreach ($bucketsData['buckets'] as $b) {
                        if (($b['name'] ?? '') === $influxTestBucket) {
                            $bucketFound = true;
                            break;
                        }
                    }
                }

                $testResults['auth'] = [
                    'label' => 'Authentication',
                    'ok' => $bucketsCode >= 200 && $bucketsCode < 300,
                    'detail' => $bucketsError !== '' ? $bucketsError : ('HTTP ' . $bucketsCode . ($bucketsCode === 401 ? ' · invalid token' : ($bucketsCode === 403 ? ' · forbidden' : ''))),
                ];

                $testResults['bucket'] = [
                    'label' => 'Bucket "' . $influxTestBucket . '"',
                    'ok' => $bucketFound,
                    'detail' => $bucketFound ? 'Found' : ($bucketsCode >= 200 && $bucketsCode < 300 ? 'Bucket not found in organization' : 'Could not verify (auth failed)'),
                ];
            } else {
                $testResults['auth'] = [
                    'label' => 'Authentication',
                    'ok' => false,
                    'detail' => 'Token or org not configured',
                ];
            }

            // 3. Write test – write a single test point and delete it
            if (($testResults['auth']['ok'] ?? false) && ($testResults['bucket']['ok'] ?? false)) {
                $testTimestamp = time();
                $lineProtocol = 'supplycore_connection_test,source=settings test_value=1i ' . $testTimestamp;
                $writeEndpoint = $influxTestUrl . '/api/v2/write?org=' . rawurlencode($influxTestOrg)
                    . '&bucket=' . rawurlencode($influxTestBucket)
                    . '&precision=s';
                $ch = curl_init($writeEndpoint);
                curl_setopt_array($ch, [
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_POST => true,
                    CURLOPT_POSTFIELDS => $lineProtocol,
                    CURLOPT_TIMEOUT => min($influxTestTimeout, 5),
                    CURLOPT_CONNECTTIMEOUT => 3,
                    CURLOPT_HTTPHEADER => [
                        'Authorization: Token ' . $influxTestToken,
                        'Content-Type: text/plain',
                    ],
                ]);
                $writeBody = curl_exec($ch);
                $writeCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
                $writeError = curl_error($ch);
                curl_close($ch);

                $testResults['write'] = [
                    'label' => 'Write test',
                    'ok' => $writeCode >= 200 && $writeCode < 300,
                    'detail' => $writeError !== '' ? $writeError : ('HTTP ' . $writeCode . ($writeCode === 204 ? ' · OK' : (' · ' . trim((string) $writeBody)))),
                ];

                // 4. Query test – read back the test point
                $flux = 'from(bucket: "' . $influxTestBucket . '") |> range(start: -1m) |> filter(fn: (r) => r._measurement == "supplycore_connection_test") |> limit(n: 1)';
                $queryEndpoint = $influxTestUrl . '/api/v2/query?org=' . rawurlencode($influxTestOrg);
                $ch = curl_init($queryEndpoint);
                curl_setopt_array($ch, [
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_POST => true,
                    CURLOPT_POSTFIELDS => $flux,
                    CURLOPT_TIMEOUT => min($influxTestTimeout, 5),
                    CURLOPT_CONNECTTIMEOUT => 3,
                    CURLOPT_HTTPHEADER => [
                        'Authorization: Token ' . $influxTestToken,
                        'Content-Type: application/vnd.flux',
                        'Accept: application/csv',
                    ],
                ]);
                $queryBody = curl_exec($ch);
                $queryCode = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
                $queryError = curl_error($ch);
                curl_close($ch);

                $queryHasRows = is_string($queryBody) && preg_match('/supplycore_connection_test/', $queryBody);
                $testResults['query'] = [
                    'label' => 'Query test',
                    'ok' => $queryCode >= 200 && $queryCode < 300,
                    'detail' => $queryError !== '' ? $queryError : ('HTTP ' . $queryCode . ($queryHasRows ? ' · test row returned' : ' · no rows (write may be buffered)')),
                ];

                // 5. Clean up – delete the test measurement
                $deleteEndpoint = $influxTestUrl . '/api/v2/delete?org=' . rawurlencode($influxTestOrg)
                    . '&bucket=' . rawurlencode($influxTestBucket);
                $deletePayload = json_encode([
                    'start' => gmdate('Y-m-d\TH:i:s\Z', $testTimestamp - 1),
                    'stop' => gmdate('Y-m-d\TH:i:s\Z', $testTimestamp + 1),
                    'predicate' => '_measurement="supplycore_connection_test"',
                ]);
                $ch = curl_init($deleteEndpoint);
                curl_setopt_array($ch, [
                    CURLOPT_RETURNTRANSFER => true,
                    CURLOPT_POST => true,
                    CURLOPT_POSTFIELDS => $deletePayload,
                    CURLOPT_TIMEOUT => min($influxTestTimeout, 5),
                    CURLOPT_CONNECTTIMEOUT => 3,
                    CURLOPT_HTTPHEADER => [
                        'Authorization: Token ' . $influxTestToken,
                        'Content-Type: application/json',
                    ],
                ]);
                curl_exec($ch);
                curl_close($ch);
            }

            $allPassed = array_reduce($testResults, fn(bool $carry, array $r) => $carry && $r['ok'], true);
            $_SESSION['influxdb_test_results'] = $testResults;
            flash('success', $allPassed ? 'All InfluxDB tests passed.' : 'Some InfluxDB tests failed – see results below.');
            header('Location: /settings?section=runtime-diagnostics&subsection=influxdb-test');
            exit;

        case 'general':
            $saved = save_settings([
                'app_name' => sanitize_app_name((string) ($_POST['app_name'] ?? app_name())),
                'brand_family_name' => sanitize_app_name((string) ($_POST['brand_family_name'] ?? brand_family_name())),
                'brand_console_label' => sanitize_brand_label((string) ($_POST['brand_console_label'] ?? brand_console_label()), brand_family_name() . ' Console'),
                'brand_tagline' => sanitize_brand_label((string) ($_POST['brand_tagline'] ?? brand_tagline()), 'Alliance logistics intelligence platform'),
                'brand_logo_path' => sanitize_brand_asset_path((string) ($_POST['brand_logo_path'] ?? brand_logo_path()), '/assets/branding/supplycore-logo.svg'),
                'brand_favicon_path' => sanitize_brand_asset_path((string) ($_POST['brand_favicon_path'] ?? brand_favicon_path()), '/assets/branding/supplycore-favicon.svg'),
                'app_base_url' => sanitize_app_base_url((string) ($_POST['app_base_url'] ?? '')),
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

        case 'ai-report-management':
            $unlockTheaterId = trim((string) ($_POST['unlock_theater_id'] ?? ''));
            if ($unlockTheaterId !== '') {
                $unlockResult = theater_unlock_report($unlockTheaterId);
                $saved = (bool) ($unlockResult['ok'] ?? false);
                $saveMessage = $saved
                    ? 'Theater report unlocked and AI briefing cleared.'
                    : ('Unlock failed: ' . (string) ($unlockResult['error'] ?? 'Unknown error'));
            }
            break;

        case 'automation-control':
            $automationAction = trim((string) ($_POST['automation_action'] ?? 'save-flags'));
            if ($automationAction === 'enable-all-jobs') {
                $result = automation_runtime_set_all_jobs_enabled(true);
                $saved = (bool) ($result['ok'] ?? false);
                $saveMessage = (string) ($result['message'] ?? 'Requested all jobs to be enabled.');
                break;
            }
            if ($automationAction === 'disable-all-jobs') {
                $result = automation_runtime_set_all_jobs_enabled(false);
                $saved = (bool) ($result['ok'] ?? false);
                $saveMessage = (string) ($result['message'] ?? 'Requested all jobs to be disabled.');
                break;
            }
            if ($automationAction === 'enable-selected-jobs') {
                $result = automation_runtime_set_jobs_enabled((array) ($_POST['managed_job_keys'] ?? []), true);
                $saved = (bool) ($result['ok'] ?? false);
                $saveMessage = (string) ($result['message'] ?? 'Requested selected jobs to be enabled.');
                break;
            }
            if ($automationAction === 'disable-selected-jobs') {
                $result = automation_runtime_set_jobs_enabled((array) ($_POST['managed_job_keys'] ?? []), false);
                $saved = (bool) ($result['ok'] ?? false);
                $saveMessage = (string) ($result['message'] ?? 'Requested selected jobs to be disabled.');
                break;
            }
            $saved = save_settings(automation_runtime_settings_from_request($_POST));
            $saveMessage = $saved ? 'Automation control settings saved.' : null;
            break;

        case 'esi-login':
            // Note: esi_enabled is NOT saved here — it is controlled exclusively
            // from Automation Control to avoid accidentally disabling ESI login.
            $saved = save_settings([
                'esi_client_id' => trim($_POST['esi_client_id'] ?? ''),
                'esi_client_secret' => trim($_POST['esi_client_secret'] ?? ''),
                'esi_callback_url' => trim($_POST['esi_callback_url'] ?? ''),
                'esi_scopes' => trim($_POST['esi_scopes'] ?? implode(' ', esi_default_scopes())),
            ]);
            break;


        case 'killmail-intelligence':
            if (isset($_POST['killmail_backfill_start'])) {
                // Trigger history backfill as a background process
                $defaultBackfillStart = date('Y') . '-01-01';
                $requestedBackfillStart = sanitize_backfill_start_date($_POST['killmail_backfill_start_date'] ?? '');
                $backfillStart = $requestedBackfillStart !== '' ? $requestedBackfillStart : $defaultBackfillStart;
                $backfillEnd = date('Y-m-d');
                save_settings([
                    'killmail_backfill_start_date' => $backfillStart,
                    'killmail_backfill_end_date' => $backfillEnd,
                    'killmail_backfill_progress' => json_encode([
                        'phase' => 'collecting',
                        'entity' => 'starting...',
                        'entities_done' => 0,
                        'entities_total' => 0,
                        'killmails_found' => 0,
                        'updated_at' => gmdate('Y-m-d\TH:i:s\Z'),
                    ]),
                ]);
                $pythonBin = scheduler_python_binary();
                $appRoot = dirname(__DIR__, 2);
                $cmd = escapeshellarg($pythonBin) . ' ' . escapeshellarg($appRoot . '/bin/python_orchestrator.py')
                     . ' killmail-backfill --app-root ' . escapeshellarg($appRoot)
                     . ' >> ' . escapeshellarg($appRoot . '/storage/logs/killmail-backfill.log') . ' 2>&1 &';
                exec($cmd);
                $saved = true;
                $saveMessage = 'Killmail history backfill started for ' . $backfillStart . ' through ' . $backfillEnd . '. Progress will update on this page.';
                break;
            }
            $killmailSave = save_killmail_intelligence_settings($_POST);
            $saved = (bool) ($killmailSave['ok'] ?? false);
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

            if ($dataSyncAction === 'run-migrations') {
                $migrationResult = supplycore_run_migrations(false, false);
                $ran = (int) ($migrationResult['migrations_run'] ?? 0);
                $migrationErrors = (array) ($migrationResult['errors'] ?? []);
                if (count($migrationErrors) > 0) {
                    flash('error', sprintf('Migrations completed with %d error(s). Check the migration status panel for details.', count($migrationErrors)));
                } else {
                    flash('success', sprintf('Database migrations complete: %d applied.', $ran));
                }
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

        case 'backup-restore':
            $backupAction = trim((string) ($_POST['backup_action'] ?? 'export'));
            if ($backupAction === 'export') {
                try {
                    $scope = supplycore_backup_parse_scope((string) ($_POST['backup_scope'] ?? 'none'));
                    $payload = supplycore_backup_build_payload($scope);
                    supplycore_backup_send_download($payload);
                } catch (Throwable $exception) {
                    $saved = false;
                    $saveMessage = 'Backup export failed: ' . $exception->getMessage();
                }
                break;
            }

            if ($backupAction === 'restore') {
                $restoreSettings = sanitize_enabled_flag($_POST['restore_settings'] ?? '1') === '1';
                $restoreData = sanitize_enabled_flag($_POST['restore_data'] ?? '0') === '1';
                $dryRun = sanitize_enabled_flag($_POST['restore_dry_run'] ?? '1') === '1';
                $decoded = supplycore_backup_decode_uploaded_file($_FILES['backup_file'] ?? []);
                if (($decoded['ok'] ?? false) !== true) {
                    $saved = false;
                    $saveMessage = (string) ($decoded['message'] ?? 'Backup upload failed.');
                    break;
                }

                $restore = supplycore_backup_restore_payload(
                    (array) ($decoded['payload'] ?? []),
                    $restoreSettings,
                    $restoreData,
                    $dryRun
                );
                $saved = (bool) ($restore['ok'] ?? false);
                $saveMessage = (string) ($restore['message'] ?? 'Restore completed.');
                break;
            }

            $saved = false;
            $saveMessage = 'Unknown backup action requested.';
            break;

        case 'public-api':
            require_once __DIR__ . '/../../src/public_api.php';
            $apiAction = trim((string) ($_POST['api_action'] ?? ''));

            if ($apiAction === 'generate-key') {
                $label = trim((string) ($_POST['api_key_label'] ?? ''));
                $allowedIpsRaw = trim((string) ($_POST['api_key_allowed_ips'] ?? ''));
                if ($label === '') {
                    $saved = false;
                    $saveMessage = 'API key label is required.';
                    break;
                }
                $allowedIps = $allowedIpsRaw !== ''
                    ? array_values(array_filter(array_map('trim', explode(',', $allowedIpsRaw))))
                    : [];
                $newKey = public_api_key_generate($label, $allowedIps);
                $saved = true;
                $saveMessage = 'API key created: ' . $newKey['key_id'] . '. Secret (shown once): ' . $newKey['secret'];
                break;
            }

            if ($apiAction === 'revoke-key') {
                $revokeKeyId = trim((string) ($_POST['revoke_key_id'] ?? ''));
                if ($revokeKeyId !== '' && public_api_key_revoke($revokeKeyId)) {
                    $saved = true;
                    $saveMessage = 'API key ' . $revokeKeyId . ' has been revoked.';
                } else {
                    $saved = false;
                    $saveMessage = 'Could not revoke the specified API key.';
                }
                break;
            }

            if ($apiAction === 'generate-token') {
                $tokenKeyId = trim((string) ($_POST['token_key_id'] ?? ''));
                $ttl = max(1, min(60, (int) ($_POST['token_ttl'] ?? 10)));
                try {
                    $provisionToken = public_api_provision_token_create($tokenKeyId, $ttl);
                    $baseUrl = rtrim((string) get_setting('app_base_url', ''), '/');
                    $provisionUrl = $baseUrl . '/api/public/provision.php?token=' . urlencode($provisionToken);
                    $saved = true;
                    $saveMessage = 'Provisioning URL (expires in ' . $ttl . ' min): ' . $provisionUrl;
                } catch (Throwable $e) {
                    $saved = false;
                    $saveMessage = 'Token generation failed: ' . $e->getMessage();
                }
                break;
            }

            $saved = false;
            $saveMessage = 'Unknown API action.';
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
    'friendly_coalition_name',
    'opponent_coalition_name',
    'killmail_backfill_start_date',
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
$runtimeConfigSections = runtime_config_sections_for_ui();
$bootstrapDbConfig = (array) (supplycore_base_config()['db'] ?? []);

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
$automationJobs = automation_runtime_jobs_overview();
$automationEnabledCount = count(array_filter($automationJobs, static fn (array $job): bool => !empty($job['enabled'])));
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
$showSyncDiagnostics = sanitize_enabled_flag($_GET['show_sync_diagnostics'] ?? '0') === '1';
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
        <h2 class="px-3 text-sm font-medium">Settings</h2>
        <div class="mt-3 space-y-1">
            <?php foreach ($sections as $key => $meta): ?>
                <a href="<?= htmlspecialchars($sectionUrl($key), ENT_QUOTES) ?>"
                   class="block rounded-lg px-3 py-2 text-sm <?= $section === $key ? 'bg-accent/20 text-white' : 'text-muted hover:bg-white/5 hover:text-slate-100' ?>">
                    <?= htmlspecialchars($meta['title'], ENT_QUOTES) ?>
                </a>
            <?php endforeach; ?>
        </div>
    </aside>

    <section class="surface-primary">
        <h2 class="text-xl font-semibold"><?= htmlspecialchars($sections[$section]['title'], ENT_QUOTES) ?></h2>
        <p class="mt-1 text-sm text-muted"><?= htmlspecialchars($sections[$section]['description'], ENT_QUOTES) ?></p>
        <?php if (($sectionChildren[$section] ?? []) !== []): ?>
            <div class="mt-4 flex flex-wrap gap-2">
                <?php foreach ($sectionChildren[$section] as $subsectionKey => $subsectionLabel): ?>
                    <a href="<?= htmlspecialchars($sectionUrl($section, $subsectionKey), ENT_QUOTES) ?>"
                       class="inline-flex items-center rounded-full border px-3 py-1.5 text-xs uppercase tracking-[0.14em] <?= $activeSubsection === $subsectionKey ? 'border-accent/60 bg-accent/20 text-slate-100' : 'border-border text-muted hover:bg-white/5 hover:text-slate-100' ?>">
                        <?= htmlspecialchars($subsectionLabel, ENT_QUOTES) ?>
                    </a>
                <?php endforeach; ?>
            </div>
        <?php endif; ?>

        <?php if (!$dbStatus['ok']): ?>
            <div class="mt-4 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-200">
                Database is currently unreachable; showing fallback values.
            </div>
        <?php endif; ?>

        <?php if ($activeSubsection === 'runtime-config'): ?>
            <div class="mt-6 space-y-6">
                <section class="rounded-2xl border border-border bg-black/20 p-4">
                    <p class="text-sm font-semibold text-slate-100">Database connection (env-only)</p>
                    <p class="mt-1 text-sm text-muted">These values are loaded from <code>.env</code>/<code>getenv()</code> and are never saved from this UI.</p>
                    <div class="mt-4 grid gap-3 md:grid-cols-2">
                        <p class="text-sm text-slate-200">Host: <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($bootstrapDbConfig['host'] ?? ''), ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Port: <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($bootstrapDbConfig['port'] ?? ''), ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Database: <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($bootstrapDbConfig['database'] ?? ''), ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Username: <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($bootstrapDbConfig['username'] ?? ''), ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Socket: <span class="font-mono text-slate-100"><?= htmlspecialchars((string) ($bootstrapDbConfig['socket'] ?? ''), ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Password configured: <span class="font-mono text-slate-100"><?= ((string) ($bootstrapDbConfig['password'] ?? '')) !== '' ? 'yes' : 'no' ?></span></p>
                    </div>
                </section>

                <form method="post" class="space-y-6">
                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                    <input type="hidden" name="section" value="runtime-config">
                    <?php foreach ($runtimeConfigSections as $registrySectionKey => $registrySection): ?>
                        <?php if ($registrySectionKey === 'db') {
                            continue;
                        } ?>
                        <section class="rounded-2xl border border-border bg-black/20 p-4">
                            <p class="text-sm font-semibold text-slate-100"><?= htmlspecialchars((string) ($registrySection['title'] ?? $registrySectionKey), ENT_QUOTES) ?></p>
                            <p class="mt-1 text-xs text-muted"><?= htmlspecialchars((string) ($registrySection['description'] ?? ''), ENT_QUOTES) ?></p>
                            <div class="mt-4 grid gap-4 md:grid-cols-2">
                                <?php foreach ((array) ($registrySection['fields'] ?? []) as $path => $field): ?>
                                    <?php if (($field['editable'] ?? false) !== true) {
                                        continue;
                                    } ?>
                                    <label class="block space-y-2">
                                        <span class="text-sm text-muted"><?= htmlspecialchars($path, ENT_QUOTES) ?> (<?= htmlspecialchars((string) ($field['type'] ?? 'string'), ENT_QUOTES) ?>)</span>
                                        <?php if (($field['type'] ?? 'string') === 'bool'): ?>
                                            <input type="checkbox" name="<?= htmlspecialchars($path, ENT_QUOTES) ?>" value="1" <?= !empty($field['value']) ? 'checked' : '' ?>>
                                        <?php else: ?>
                                            <input name="<?= htmlspecialchars($path, ENT_QUOTES) ?>" value="<?= htmlspecialchars((string) ($field['value'] ?? ''), ENT_QUOTES) ?>" class="w-full field-input" />
                                        <?php endif; ?>
                                    </label>
                                <?php endforeach; ?>
                            </div>
                        </section>
                    <?php endforeach; ?>
                    <button class="btn-primary">Save runtime settings</button>
                </form>
            </div>
        <?php elseif ($activeSubsection === 'influxdb-test'): ?>
            <?php
            $influxTestEnabled = (bool) config('influxdb.enabled', false);
            $influxTestCurrentUrl = rtrim((string) config('influxdb.url', 'http://127.0.0.1:8086'), '/');
            $influxTestCurrentOrg = trim((string) config('influxdb.org', ''));
            $influxTestCurrentBucket = trim((string) config('influxdb.bucket', 'supplycore_rollups'));
            $influxTestCurrentToken = trim((string) config('influxdb.token', ''));
            $influxTestResults = $_SESSION['influxdb_test_results'] ?? null;
            unset($_SESSION['influxdb_test_results']);
            ?>
            <div class="mt-6 space-y-6">
                <section class="rounded-2xl border border-border bg-black/20 p-4">
                    <p class="text-sm font-semibold text-slate-100">InfluxDB Connection Test</p>
                    <p class="mt-1 text-xs text-muted">Run a full connectivity test against your configured InfluxDB instance. This will check health, authentication, bucket access, write, and query capabilities.</p>

                    <div class="mt-4 grid gap-3 md:grid-cols-2">
                        <p class="text-sm text-slate-200">URL: <span class="font-mono text-slate-100"><?= htmlspecialchars($influxTestCurrentUrl, ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Org: <span class="font-mono text-slate-100"><?= htmlspecialchars($influxTestCurrentOrg !== '' ? $influxTestCurrentOrg : '(not set)', ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Bucket: <span class="font-mono text-slate-100"><?= htmlspecialchars($influxTestCurrentBucket, ENT_QUOTES) ?></span></p>
                        <p class="text-sm text-slate-200">Token: <span class="font-mono text-slate-100"><?= $influxTestCurrentToken !== '' ? '••••' . htmlspecialchars(substr($influxTestCurrentToken, -4), ENT_QUOTES) : '(not set)' ?></span></p>
                        <p class="text-sm text-slate-200">Enabled: <span class="font-mono text-slate-100"><?= $influxTestEnabled ? 'yes' : 'no' ?></span></p>
                    </div>

                    <form method="post" class="mt-4">
                        <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                        <input type="hidden" name="section" value="influxdb-test">
                        <button class="btn-primary">Test Connection</button>
                    </form>
                </section>

                <?php if (is_array($influxTestResults) && $influxTestResults !== []): ?>
                    <section class="rounded-2xl border border-border bg-black/20 p-4">
                        <p class="text-sm font-semibold text-slate-100">Test Results</p>
                        <div class="mt-4 space-y-3">
                            <?php foreach ($influxTestResults as $testKey => $testResult): ?>
                                <?php
                                $isOk = (bool) ($testResult['ok'] ?? false);
                                $tone = $isOk
                                    ? 'border-emerald-400/20 bg-emerald-500/10 text-emerald-100'
                                    : 'border-rose-400/20 bg-rose-500/10 text-rose-100';
                                ?>
                                <div class="flex items-start gap-3 rounded-lg border p-3 <?= $tone ?>">
                                    <span class="mt-0.5 text-lg"><?= $isOk ? '&#10003;' : '&#10007;' ?></span>
                                    <div>
                                        <p class="text-sm font-medium"><?= htmlspecialchars((string) ($testResult['label'] ?? $testKey), ENT_QUOTES) ?></p>
                                        <p class="text-xs opacity-80"><?= htmlspecialchars((string) ($testResult['detail'] ?? ''), ENT_QUOTES) ?></p>
                                        <?php if (!empty($testResult['version'])): ?>
                                            <p class="text-xs opacity-60">Version: <?= htmlspecialchars((string) $testResult['version'], ENT_QUOTES) ?></p>
                                        <?php endif; ?>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    </section>
                <?php endif; ?>
            </div>
        <?php elseif ($activeSubsection === 'general'): ?>
            <?php
            $businessConfigCards = [
                ['href' => $sectionUrl('market-scope', 'trading-stations'), 'title' => 'Trading stations', 'copy' => 'Reference hub and alliance destination used by market, doctrine, and buy-all workflows.'],
                ['href' => $sectionUrl('market-scope', 'item-scope'), 'title' => 'Item scope', 'copy' => 'Control which items matter for trading, doctrine readiness, and restock decisions.'],
                ['href' => $sectionUrl('ai-alerts', 'deal-alerts'), 'title' => 'Deal alerts', 'copy' => 'Tune how aggressively SupplyCore flags profitable market anomalies.'],
                ['href' => $sectionUrl('ai-alerts', 'killmail-intelligence'), 'title' => 'Doctrine + killmail inputs', 'copy' => 'Tracked alliances, corporations, and demand signals that feed readiness and replenishment views.'],
                ['href' => $sectionUrl('ai-alerts', 'ai-briefings'), 'title' => 'AI briefings', 'copy' => 'Choose whether background AI summaries run and which provider they use.'],
                ['href' => $sectionUrl('ai-alerts', 'ai-report-management'), 'title' => 'AI report management', 'copy' => 'Unlock locked theater reports and clear AI briefings so they can be regenerated.'],
                ['href' => $sectionUrl('automation-sync', 'data-sync'), 'title' => 'Sync behavior', 'copy' => 'Control update cadence, freshness expectations, and manual run controls.'],
                ['href' => $sectionUrl('backup-restore', 'backup-restore'), 'title' => 'Backup & restore', 'copy' => 'Export settings snapshots and perform safe dry-run restores before applying changes.'],
                ['href' => $sectionUrl('automation-sync', 'automation-control'), 'title' => 'Automation control', 'copy' => 'Centralized toggles for ESI, zKill ingestion, pipelines, and recurring job enablement.'],
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
                            <p class="mt-1 text-xs text-muted">Open <a href="<?= htmlspecialchars($sectionUrl('automation-sync', 'data-sync'), ENT_QUOTES) ?>" class="text-slate-100 underline decoration-dotted underline-offset-4">Sync Operations</a> for scheduler state, runtime activity, and recovery controls.</p>
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
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Base URL</span>
                            <input name="app_base_url" value="<?= htmlspecialchars(get_setting('app_base_url', ''), ENT_QUOTES) ?>" placeholder="e.g. https://supplycore.example.com" class="w-full field-input" />
                            <span class="text-xs text-muted">Public URL used in provisioning tokens and external integrations. Include the scheme (https://).</span>
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
        <?php elseif ($activeSubsection === 'trading-stations'): ?>
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
                            : ('Selected market hub (NPC station): ' . ($marketStationName ?? ('Station #' . $marketStationId)) . ' (#' . $marketStationId . ').'), ENT_QUOTES) ?>
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
                                    const selectedType = item.type ? ' [' + item.type + ']' : '';
                                    status.textContent = selectionStatusPrefix + item.name + selectedType + ' (#' + item.id + ').';
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
                        selectionStatusPrefix: 'Selected market hub (NPC station): ',
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
        <?php elseif ($activeSubsection === 'ai-briefings'): ?>
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
                        <p class="text-xs text-muted">Use <span class="font-medium text-slate-100">Local Ollama</span> for self-hosted CPU/GPU inference, <span class="font-medium text-slate-100">Runpod Serverless</span> for async GPU jobs, or <span class="font-medium text-slate-100">Claude API</span> for fast hosted inference (requires separate API billing at <span class="font-medium text-slate-100">console.anthropic.com</span>).</p>
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
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Claude API Key</span>
                        <input name="claude_api_key" type="password" value="<?= htmlspecialchars($settingValues['claude_api_key'] ?? ($ollamaConfig['claude_api_key'] ?? ''), ENT_QUOTES) ?>" class="w-full field-input" placeholder="sk-ant-api03-..." />
                        <?php if (($ollamaConfig['claude_api_key_masked'] ?? '') !== ''): ?>
                            <p class="text-xs text-muted">Saved key preview: <span class="font-medium text-slate-100"><?= htmlspecialchars((string) $ollamaConfig['claude_api_key_masked'], ENT_QUOTES) ?></span>.</p>
                        <?php else: ?>
                            <p class="text-xs text-muted">Get your API key from <span class="font-medium text-slate-100">console.anthropic.com</span>. API usage is billed separately from Claude Pro subscriptions.</p>
                        <?php endif; ?>
                    </label>
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Claude Model</span>
                        <input name="claude_model" value="<?= htmlspecialchars($settingValues['claude_model'] ?? ($ollamaConfig['claude_model'] ?? 'claude-sonnet-4-20250514'), ENT_QUOTES) ?>" class="w-full field-input" />
                        <p class="text-xs text-muted">Recommended: <span class="font-medium text-slate-100">claude-sonnet-4-20250514</span> (fast, cheap) or <span class="font-medium text-slate-100">claude-haiku-4-5-20251001</span> (fastest, cheapest).</p>
                    </label>
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Groq API Key</span>
                        <input name="groq_api_key" type="password" value="<?= htmlspecialchars($settingValues['groq_api_key'] ?? ($ollamaConfig['groq_api_key'] ?? ''), ENT_QUOTES) ?>" class="w-full field-input" placeholder="gsk_..." />
                        <?php if (($ollamaConfig['groq_api_key_masked'] ?? '') !== ''): ?>
                            <p class="text-xs text-muted">Saved key preview: <span class="font-medium text-slate-100"><?= htmlspecialchars((string) $ollamaConfig['groq_api_key_masked'], ENT_QUOTES) ?></span>.</p>
                        <?php else: ?>
                            <p class="text-xs text-muted">Free tier at <span class="font-medium text-slate-100">console.groq.com</span>. Fast inference — great for CPU-only machines.</p>
                        <?php endif; ?>
                    </label>
                    <label class="block space-y-2 md:col-span-2">
                        <span class="text-sm text-muted">Groq Model</span>
                        <input name="groq_model" value="<?= htmlspecialchars($settingValues['groq_model'] ?? ($ollamaConfig['groq_model'] ?? 'meta-llama/llama-4-scout-17b-16e-instruct'), ENT_QUOTES) ?>" class="w-full field-input" />
                        <p class="text-xs text-muted">Recommended: <span class="font-medium text-slate-100">meta-llama/llama-4-scout-17b-16e-instruct</span> (500K TPD) or <span class="font-medium text-slate-100">llama-3.3-70b-versatile</span> (best quality, 100K TPD).</p>
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Model Name (Ollama/Runpod)</span>
                        <input name="ollama_model" value="<?= htmlspecialchars($settingValues['ollama_model'] ?? ($ollamaConfig['model'] ?? 'qwen2.5:1.5b-instruct'), ENT_QUOTES) ?>" class="w-full field-input" />
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Request Timeout (seconds)</span>
                        <input type="number" min="1" max="600" step="1" name="ollama_timeout" value="<?= htmlspecialchars($settingValues['ollama_timeout'] ?? (string) ($ollamaConfig['timeout'] ?? 20), ENT_QUOTES) ?>" class="w-full field-input" />
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

                <div class="mt-2">
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Theater AAR Prompt</span>
                        <textarea name="theater_aar_prompt" rows="12" class="w-full field-input font-mono text-xs leading-relaxed" placeholder="Leave blank to use the default 8-section AAR prompt. Custom prompts receive the battle data JSON appended automatically."><?= htmlspecialchars($settingValues['theater_aar_prompt'] ?? '', ENT_QUOTES) ?></textarea>
                        <p class="text-xs text-muted">Custom prompt for generating Theater After Action Reports. The battle data JSON (alliances, fleet composition, notable kills, top performers, turning points, ISK stats) is automatically appended. Leave empty to use the built-in structured AAR template with 8 sections (Executive Summary, Battle Overview, Fleet Composition, Key Turning Points, Tactical Assessment, Performance Insights, Risk Signals, Recommendations).</p>
                    </label>
                </div>

                <div class="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 text-sm text-muted">
                    Configure either the local Ollama API or the Runpod serverless endpoint here, then manage cadence under <a href="<?= htmlspecialchars($sectionUrl('automation-sync', 'data-sync'), ENT_QUOTES) ?>" class="font-medium text-slate-100 hover:text-white">Settings → Automation &amp; Sync</a> for the <span class="font-medium text-slate-100">rebuild_ai_briefings</span> scheduler job. Runpod requests now submit asynchronously and poll for completion within the configured timeout window. Small tiers stay compact, medium tiers add explanation and deltas, and large tiers unlock richer operator briefings while still keeping deterministic calculations authoritative.
                </div>

                <div class="flex items-center gap-4">
                    <button class="btn-primary">Save AI Briefing Settings</button>
                    <button type="button" id="test-ai-btn" class="btn-secondary" onclick="testAiConnection()">Test AI Connection</button>
                    <span id="test-ai-result" class="text-sm"></span>
                </div>
                <script>
                function testAiConnection() {
                    const btn = document.getElementById('test-ai-btn');
                    const result = document.getElementById('test-ai-result');
                    btn.disabled = true;
                    btn.textContent = 'Testing…';
                    result.textContent = '';
                    result.className = 'text-sm';
                    fetch('/settings/test-ai.php', {method: 'POST', headers: {'X-Requested-With': 'XMLHttpRequest'}})
                        .then(r => r.json())
                        .then(data => {
                            if (data.success) {
                                result.textContent = 'OK — ' + data.provider + ' / ' + data.model + ' (' + data.elapsed_ms + 'ms)';
                                result.className = 'text-sm text-green-400';
                            } else {
                                result.textContent = 'Failed — ' + data.provider + ': ' + (data.error || 'unknown error');
                                result.className = 'text-sm text-red-400';
                            }
                        })
                        .catch(err => {
                            result.textContent = 'Request failed: ' + err.message;
                            result.className = 'text-sm text-red-400';
                        })
                        .finally(() => {
                            btn.disabled = false;
                            btn.textContent = 'Test AI Connection';
                        });
                }
                </script>
            </form>
        <?php elseif ($activeSubsection === 'ai-report-management'): ?>
            <?php $lockedTheaters = db_theaters_locked(); ?>
            <div class="mt-6 space-y-6">
                <div class="rounded-2xl border border-white/8 bg-white/[0.03] px-4 py-3 text-sm text-muted">
                    Locked theater reports have their AI briefing frozen. Unlocking a report clears the AI-generated briefing and allows you to regenerate it with the <span class="font-medium text-slate-100">Lock &amp; Generate AI Report</span> button on the theater view page.
                </div>

                <?php if ($lockedTheaters === []): ?>
                    <div class="rounded-2xl border border-white/8 bg-white/[0.03] p-6 text-center text-sm text-muted">
                        No locked theater reports found.
                    </div>
                <?php else: ?>
                    <div class="overflow-x-auto rounded-2xl border border-white/8">
                        <table class="w-full text-sm">
                            <thead>
                                <tr class="border-b border-white/8 bg-white/[0.03] text-left text-xs uppercase tracking-[0.16em] text-muted">
                                    <th class="px-4 py-3">Theater</th>
                                    <th class="px-4 py-3">Date</th>
                                    <th class="px-4 py-3">Kills</th>
                                    <th class="px-4 py-3">Pilots</th>
                                    <th class="px-4 py-3">AI Verdict</th>
                                    <th class="px-4 py-3">AI Model</th>
                                    <th class="px-4 py-3">Locked At</th>
                                    <th class="px-4 py-3"></th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-white/5">
                                <?php foreach ($lockedTheaters as $lt): ?>
                                    <?php
                                        $ltSystem = (string) ($lt['primary_system_name'] ?? 'Unknown');
                                        $ltRegion = (string) ($lt['region_name'] ?? '');
                                        $ltVerdict = (string) ($lt['ai_verdict'] ?? '');
                                        $ltHeadline = (string) ($lt['ai_headline'] ?? '');
                                        $ltModel = (string) ($lt['ai_summary_model'] ?? '');
                                        $ltLockedAt = (string) ($lt['locked_at'] ?? '');
                                        $ltStartTime = (string) ($lt['start_time'] ?? '');
                                        $ltTheaterId = (string) ($lt['theater_id'] ?? '');
                                        $ltHasAi = $ltHeadline !== '' || $ltVerdict !== '';
                                    ?>
                                    <tr class="hover:bg-white/[0.02]">
                                        <td class="px-4 py-3">
                                            <a href="/theater-intelligence/view.php?theater_id=<?= htmlspecialchars(urlencode($ltTheaterId), ENT_QUOTES) ?>" class="font-medium text-slate-100 hover:text-white">
                                                <?= htmlspecialchars($ltSystem, ENT_QUOTES) ?>
                                            </a>
                                            <?php if ($ltRegion !== ''): ?>
                                                <span class="text-muted">(<?= htmlspecialchars($ltRegion, ENT_QUOTES) ?>)</span>
                                            <?php endif; ?>
                                            <?php if ($ltHeadline !== ''): ?>
                                                <p class="mt-0.5 text-xs text-muted truncate max-w-xs" title="<?= htmlspecialchars($ltHeadline, ENT_QUOTES) ?>"><?= htmlspecialchars($ltHeadline, ENT_QUOTES) ?></p>
                                            <?php endif; ?>
                                        </td>
                                        <td class="px-4 py-3 text-muted whitespace-nowrap"><?= htmlspecialchars(substr($ltStartTime, 0, 10), ENT_QUOTES) ?></td>
                                        <td class="px-4 py-3 text-muted"><?= (int) ($lt['total_kills'] ?? 0) ?></td>
                                        <td class="px-4 py-3 text-muted"><?= (int) ($lt['participant_count'] ?? 0) ?></td>
                                        <td class="px-4 py-3">
                                            <?php if ($ltVerdict !== ''): ?>
                                                <span class="inline-flex rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider
                                                    <?= match($ltVerdict) {
                                                        'decisive_victory' => 'bg-emerald-500/20 text-emerald-300 border border-emerald-500/30',
                                                        'victory' => 'bg-green-500/20 text-green-300 border border-green-500/30',
                                                        'close_fight' => 'bg-amber-500/20 text-amber-300 border border-amber-500/30',
                                                        'defeat' => 'bg-red-500/20 text-red-300 border border-red-500/30',
                                                        'decisive_defeat' => 'bg-red-600/20 text-red-200 border border-red-600/30',
                                                        'stalemate' => 'bg-slate-500/20 text-slate-300 border border-slate-500/30',
                                                        default => 'bg-white/10 text-muted border border-white/10',
                                                    } ?>"><?= htmlspecialchars(theater_ai_verdict_label($ltVerdict), ENT_QUOTES) ?></span>
                                            <?php else: ?>
                                                <span class="text-xs text-muted">No AI</span>
                                            <?php endif; ?>
                                        </td>
                                        <td class="px-4 py-3 text-xs text-muted"><?= $ltModel !== '' ? htmlspecialchars($ltModel, ENT_QUOTES) : '—' ?></td>
                                        <td class="px-4 py-3 text-xs text-muted whitespace-nowrap"><?= htmlspecialchars($ltLockedAt, ENT_QUOTES) ?></td>
                                        <td class="px-4 py-3 text-right">
                                            <form method="POST" class="inline" onsubmit="return confirm('Unlock this theater report and clear its AI briefing? You can regenerate it from the theater view page.');">
                                                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                                                <input type="hidden" name="section" value="ai-report-management">
                                                <input type="hidden" name="unlock_theater_id" value="<?= htmlspecialchars($ltTheaterId, ENT_QUOTES) ?>">
                                                <button type="submit" class="inline-flex items-center gap-1.5 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-1.5 text-xs font-medium text-amber-200 transition hover:bg-amber-500/20">
                                                    <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M8 11V7a4 4 0 118 0m-4 8v2m-6 4h12a2 2 0 002-2v-6a2 2 0 00-2-2H6a2 2 0 00-2 2v6a2 2 0 002 2z" /></svg>
                                                    Unlock &amp; Clear AI
                                                </button>
                                            </form>
                                        </td>
                                    </tr>
                                <?php endforeach; ?>
                            </tbody>
                        </table>
                    </div>
                <?php endif; ?>
            </div>
        <?php elseif ($activeSubsection === 'item-scope'): ?>
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
        <?php elseif ($activeSubsection === 'killmail-intelligence'): ?>
            <?php
                // Build corp contacts display data with resolved names
                $corpContactsList = db_corp_contacts_all();
                $esiContacts = array_filter($corpContactsList, static fn (array $c): bool =>
                    ((string) ($c['source'] ?? 'esi')) === 'esi'
                    && in_array((string) ($c['contact_type'] ?? ''), ['alliance', 'corporation', 'faction'], true)
                );
                $manualContacts = array_filter($corpContactsList, static fn (array $c): bool => ((string) ($c['source'] ?? 'esi')) === 'manual');
                $contactIdsByType = ['alliance' => [], 'corporation' => [], 'character' => []];
                foreach ($corpContactsList as $c) {
                    $cType = (string) $c['contact_type'];
                    $cId = (int) $c['contact_id'];
                    if ($cId > 0 && isset($contactIdsByType[$cType])) {
                        $contactIdsByType[$cType][$cId] = true;
                    }
                }
                $contactAllianceIds = array_keys($contactIdsByType['alliance']);
                $contactCorpIds = array_keys($contactIdsByType['corporation']);
                $contactCharacterIds = array_keys($contactIdsByType['character']);
                $contactNameMap = [];
                foreach (['alliance' => $contactAllianceIds, 'corporation' => $contactCorpIds, 'character' => $contactCharacterIds] as $cacheType => $cacheIds) {
                    foreach (db_entity_metadata_cache_get_many($cacheType, $cacheIds) as $e) {
                        $name = trim((string) ($e['entity_name'] ?? ''));
                        if ($name !== '') {
                            $contactNameMap[$cacheType . ':' . (int) $e['entity_id']] = $name;
                        }
                    }
                }

                // Queue any unresolved contact IDs for background name resolution
                $unresolvedByType = [];
                foreach ($contactIdsByType as $cType => $idMap) {
                    foreach (array_keys($idMap) as $cId) {
                        if (!isset($contactNameMap[$cType . ':' . $cId])) {
                            $unresolvedByType[$cType][] = $cId;
                        }
                    }
                }
                foreach ($unresolvedByType as $cType => $ids) {
                    if ($ids !== []) {
                        db_entity_metadata_cache_mark_pending($cType, $ids);
                    }
                }

                // Prepare manual contact picker selections
                $manualFriendlyAllianceSelections = [];
                $manualFriendlyCorporationSelections = [];
                $manualHostileAllianceSelections = [];
                $manualHostileCorporationSelections = [];
                foreach ($manualContacts as $mc) {
                    $mcId = (int) $mc['contact_id'];
                    $mcType = (string) $mc['contact_type'];
                    $mcStanding = (float) $mc['standing'];
                    $mcName = $contactNameMap[$mcType . ':' . $mcId] ?? ucfirst($mcType) . ' #' . $mcId;
                    $entry = ['id' => $mcId, 'name' => $mcName, 'type' => ucfirst($mcType)];
                    if ($mcStanding > 0 && $mcType === 'alliance') { $manualFriendlyAllianceSelections[] = $entry; }
                    elseif ($mcStanding > 0 && $mcType === 'corporation') { $manualFriendlyCorporationSelections[] = $entry; }
                    elseif ($mcStanding < 0 && $mcType === 'alliance') { $manualHostileAllianceSelections[] = $entry; }
                    elseif ($mcStanding < 0 && $mcType === 'corporation') { $manualHostileCorporationSelections[] = $entry; }
                }
                $manualFriendlyAlliancesText = implode("\n", array_map(static fn (array $r): string => $r['id'] . ' | ' . $r['name'], $manualFriendlyAllianceSelections));
                $manualFriendlyCorporationsText = implode("\n", array_map(static fn (array $r): string => $r['id'] . ' | ' . $r['name'], $manualFriendlyCorporationSelections));
                $manualHostileAlliancesText = implode("\n", array_map(static fn (array $r): string => $r['id'] . ' | ' . $r['name'], $manualHostileAllianceSelections));
                $manualHostileCorporationsText = implode("\n", array_map(static fn (array $r): string => $r['id'] . ' | ' . $r['name'], $manualHostileCorporationSelections));

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
                        <p class="text-xs uppercase tracking-[0.16em] text-muted">Tracked friendlies</p>
                        <p class="mt-2 text-sm font-semibold text-slate-50"><?= number_format((int) (($killmailRuntimeCard['tracked_alliance_count'] ?? null) ?: ($killmailStatusSummary['tracked_alliance_count'] ?? 0))) ?> alliances · <?= number_format((int) (($killmailRuntimeCard['tracked_corporation_count'] ?? null) ?: ($killmailStatusSummary['tracked_corporation_count'] ?? 0))) ?> corporations</p>
                        <p class="mt-2 text-xs text-muted">These friendly entities determine which victim-side losses are retained for the tracked loss board.</p>
                    </article>
                </div>

                <div class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <span class="inline-flex size-4 items-center justify-center rounded border <?= ($settingValues['killmail_ingestion_enabled'] ?? '0') === '1' ? 'border-emerald-500/60 bg-emerald-500/20 text-emerald-300' : 'border-border bg-black/40 text-slate-500' ?> text-xs"><?= ($settingValues['killmail_ingestion_enabled'] ?? '0') === '1' ? '✓' : '' ?></span>
                    <span class="text-sm">zKillboard R2Z2 ingestion <?= ($settingValues['killmail_ingestion_enabled'] ?? '0') === '1' ? 'enabled' : 'disabled' ?></span>
                    <a href="<?= htmlspecialchars($sectionUrl('automation-sync', 'automation-control'), ENT_QUOTES) ?>" class="ml-auto text-xs text-slate-400 underline decoration-dotted underline-offset-4">Change in Automation Control</a>
                </div>

                <div class="grid gap-4 md:grid-cols-2">
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Poll Sleep Seconds (min 6)</span>
                        <input type="number" min="6" max="300" step="1" name="killmail_ingestion_poll_sleep_seconds" value="<?= htmlspecialchars($settingValues['killmail_ingestion_poll_sleep_seconds'] ?? '10', ENT_QUOTES) ?>" class="w-full field-input" />
                        <span class="text-xs text-muted">Continuous Python polling now defaults to 10 seconds between live-stream retry attempts after the worker reaches the R2Z2 tip or gets rate limited.</span>
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Max Sequences Per Run</span>
                        <input type="number" min="1" max="5000" step="1" name="killmail_ingestion_max_sequences_per_run" value="<?= htmlspecialchars($settingValues['killmail_ingestion_max_sequences_per_run'] ?? '5000', ENT_QUOTES) ?>" class="w-full field-input" />
                    </label>
                </div>

                <h3 class="text-base font-semibold text-slate-100 mt-6">Coalition Names</h3>
                <p class="mt-1 text-xs text-muted">Optional display names for each side in battle reports. When set, this name replaces the largest alliance name in the side header (e.g. "WinterCo" instead of "Fraternity.").</p>
                <div class="grid gap-4 lg:grid-cols-2 mt-2">
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Friendly Coalition Name</span>
                        <input type="text" name="friendly_coalition_name" maxlength="100" placeholder="e.g. WinterCo" value="<?= htmlspecialchars((string) ($settingValues['friendly_coalition_name'] ?? ''), ENT_QUOTES) ?>" class="w-full field-input" />
                    </label>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Opponent Coalition Name</span>
                        <input type="text" name="opponent_coalition_name" maxlength="100" placeholder="e.g. Imperium" value="<?= htmlspecialchars((string) ($settingValues['opponent_coalition_name'] ?? ''), ENT_QUOTES) ?>" class="w-full field-input" />
                    </label>
                </div>

                <h3 class="text-base font-semibold text-slate-100 mt-6">Corp Contacts (ESI + Manual Standings)</h3>
                <p class="mt-1 text-xs text-muted">ESI contacts are synced from your corporation's in-game standings. You can also add manual contacts below — these are preserved across ESI syncs and used for theater side classification.</p>

                <?php if ($esiContacts !== []): ?>
                <?php
                    $esiFriendly10 = array_filter($esiContacts, static fn (array $c): bool => (float) $c['standing'] >= 10.0);
                    $esiFriendly5  = array_filter($esiContacts, static fn (array $c): bool => (float) $c['standing'] > 0 && (float) $c['standing'] < 10.0);
                    $esiHostile5   = array_filter($esiContacts, static fn (array $c): bool => (float) $c['standing'] < 0 && (float) $c['standing'] > -10.0);
                    $esiHostile10  = array_filter($esiContacts, static fn (array $c): bool => (float) $c['standing'] <= -10.0);
                    $esiFriendly = array_merge($esiFriendly10, $esiFriendly5);
                    $esiHostile = array_merge($esiHostile10, $esiHostile5);
                ?>
                <p class="text-xs uppercase tracking-[0.14em] text-muted mt-4 mb-2">ESI Synced (read-only)</p>
                <div class="grid gap-4 lg:grid-cols-2">
                    <div class="space-y-2">
                        <span class="text-sm text-blue-300">Friendly (positive standing)</span>
                        <?php if ($esiFriendly === []): ?>
                            <p class="text-xs text-muted">No friendly contacts from ESI.</p>
                        <?php else: ?>
                            <div class="max-h-72 overflow-y-auto rounded-lg border border-border bg-black/10 divide-y divide-border/50">
                                <?php
                                    $prevStandingGroup = null;
                                    foreach ($esiFriendly as $c):
                                    $cId = (int) $c['contact_id'];
                                    $cType = (string) $c['contact_type'];
                                    $cStanding = (float) $c['standing'];
                                    $cName = $contactNameMap[$cType . ':' . $cId] ?? ucfirst($cType) . ' #' . $cId;
                                    $standingGroup = $cStanding >= 10.0 ? '+10' : '+5';
                                    if ($standingGroup !== $prevStandingGroup):
                                        $prevStandingGroup = $standingGroup;
                                ?>
                                <div class="px-3 py-1.5 bg-blue-500/5">
                                    <span class="text-xs font-semibold uppercase tracking-wider <?= $cStanding >= 10.0 ? 'text-blue-300' : 'text-sky-400' ?>">Standing <?= $standingGroup ?></span>
                                </div>
                                <?php endif; ?>
                                <div class="flex items-center justify-between px-3 py-2">
                                    <div>
                                        <span class="text-sm text-slate-100"><?= htmlspecialchars($cName, ENT_QUOTES) ?></span>
                                        <span class="ml-2 text-xs text-muted"><?= htmlspecialchars(ucfirst($cType), ENT_QUOTES) ?></span>
                                    </div>
                                    <span class="text-xs font-mono <?= $cStanding >= 10.0 ? 'text-blue-400' : 'text-sky-400' ?>">+<?= number_format($cStanding, 1) ?></span>
                                </div>
                                <?php endforeach; ?>
                            </div>
                        <?php endif; ?>
                    </div>
                    <div class="space-y-2">
                        <span class="text-sm text-red-300">Hostile (negative standing)</span>
                        <?php if ($esiHostile === []): ?>
                            <p class="text-xs text-muted">No hostile contacts from ESI.</p>
                        <?php else: ?>
                            <div class="max-h-72 overflow-y-auto rounded-lg border border-border bg-black/10 divide-y divide-border/50">
                                <?php
                                    $prevStandingGroup = null;
                                    foreach ($esiHostile as $c):
                                    $cId = (int) $c['contact_id'];
                                    $cType = (string) $c['contact_type'];
                                    $cStanding = (float) $c['standing'];
                                    $cName = $contactNameMap[$cType . ':' . $cId] ?? ucfirst($cType) . ' #' . $cId;
                                    $standingGroup = $cStanding <= -10.0 ? '-10' : '-5';
                                    if ($standingGroup !== $prevStandingGroup):
                                        $prevStandingGroup = $standingGroup;
                                ?>
                                <div class="px-3 py-1.5 bg-red-500/5">
                                    <span class="text-xs font-semibold uppercase tracking-wider <?= $cStanding <= -10.0 ? 'text-red-300' : 'text-orange-400' ?>">Standing <?= $standingGroup ?></span>
                                </div>
                                <?php endif; ?>
                                <div class="flex items-center justify-between px-3 py-2">
                                    <div>
                                        <span class="text-sm text-slate-100"><?= htmlspecialchars($cName, ENT_QUOTES) ?></span>
                                        <span class="ml-2 text-xs text-muted"><?= htmlspecialchars(ucfirst($cType), ENT_QUOTES) ?></span>
                                    </div>
                                    <span class="text-xs font-mono <?= $cStanding <= -10.0 ? 'text-red-400' : 'text-orange-400' ?>"><?= number_format($cStanding, 1) ?></span>
                                </div>
                                <?php endforeach; ?>
                            </div>
                        <?php endif; ?>
                    </div>
                </div>
                <?php endif; ?>

                <p class="text-xs uppercase tracking-[0.14em] text-muted mt-4 mb-2">Manual Contacts (editable)</p>
                <p class="text-xs text-muted mb-2">Add alliances or corporations here to supplement ESI standings. Friendly contacts get +10.0 standing, hostile contacts get -10.0.</p>

                <div class="grid gap-4 lg:grid-cols-2">
                    <div class="space-y-3">
                        <span class="text-sm text-blue-300">Friendly Alliances</span>
                        <textarea name="manual_friendly_alliance_contacts" id="manual_friendly_alliance_contacts" class="hidden"><?= htmlspecialchars($manualFriendlyAlliancesText, ENT_QUOTES) ?></textarea>
                        <div class="flex gap-2">
                            <input type="text" id="manual_friendly_alliance_search" autocomplete="off" placeholder="Search alliances or enter ID" class="w-full field-input" />
                            <button type="button" id="manual_friendly_alliance_add" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm text-slate-100 transition hover:bg-white/5">Add</button>
                        </div>
                        <p id="manual_friendly_alliance_status" class="text-xs text-muted"><?= htmlspecialchars($manualFriendlyAllianceSelections === [] ? 'No manual friendly alliances.' : count($manualFriendlyAllianceSelections) . ' manual friendly alliance' . (count($manualFriendlyAllianceSelections) === 1 ? '' : 's') . '.', ENT_QUOTES) ?></p>
                        <ul id="manual_friendly_alliance_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                        <div id="manual_friendly_alliance_selected" class="space-y-2 rounded-lg border border-border bg-black/10 p-3"></div>
                    </div>
                    <div class="space-y-3">
                        <span class="text-sm text-blue-300">Friendly Corporations</span>
                        <textarea name="manual_friendly_corporation_contacts" id="manual_friendly_corporation_contacts" class="hidden"><?= htmlspecialchars($manualFriendlyCorporationsText, ENT_QUOTES) ?></textarea>
                        <div class="flex gap-2">
                            <input type="text" id="manual_friendly_corporation_search" autocomplete="off" placeholder="Search corporations or enter ID" class="w-full field-input" />
                            <button type="button" id="manual_friendly_corporation_add" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm text-slate-100 transition hover:bg-white/5">Add</button>
                        </div>
                        <p id="manual_friendly_corporation_status" class="text-xs text-muted"><?= htmlspecialchars($manualFriendlyCorporationSelections === [] ? 'No manual friendly corporations.' : count($manualFriendlyCorporationSelections) . ' manual friendly corporation' . (count($manualFriendlyCorporationSelections) === 1 ? '' : 's') . '.', ENT_QUOTES) ?></p>
                        <ul id="manual_friendly_corporation_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                        <div id="manual_friendly_corporation_selected" class="space-y-2 rounded-lg border border-border bg-black/10 p-3"></div>
                    </div>
                </div>

                <div class="grid gap-4 lg:grid-cols-2 mt-3">
                    <div class="space-y-3">
                        <span class="text-sm text-red-300">Hostile Alliances</span>
                        <textarea name="manual_hostile_alliance_contacts" id="manual_hostile_alliance_contacts" class="hidden"><?= htmlspecialchars($manualHostileAlliancesText, ENT_QUOTES) ?></textarea>
                        <div class="flex gap-2">
                            <input type="text" id="manual_hostile_alliance_search" autocomplete="off" placeholder="Search alliances or enter ID" class="w-full field-input" />
                            <button type="button" id="manual_hostile_alliance_add" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm text-slate-100 transition hover:bg-white/5">Add</button>
                        </div>
                        <p id="manual_hostile_alliance_status" class="text-xs text-muted"><?= htmlspecialchars($manualHostileAllianceSelections === [] ? 'No manual hostile alliances.' : count($manualHostileAllianceSelections) . ' manual hostile alliance' . (count($manualHostileAllianceSelections) === 1 ? '' : 's') . '.', ENT_QUOTES) ?></p>
                        <ul id="manual_hostile_alliance_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                        <div id="manual_hostile_alliance_selected" class="space-y-2 rounded-lg border border-border bg-black/10 p-3"></div>
                    </div>
                    <div class="space-y-3">
                        <span class="text-sm text-red-300">Hostile Corporations</span>
                        <textarea name="manual_hostile_corporation_contacts" id="manual_hostile_corporation_contacts" class="hidden"><?= htmlspecialchars($manualHostileCorporationsText, ENT_QUOTES) ?></textarea>
                        <div class="flex gap-2">
                            <input type="text" id="manual_hostile_corporation_search" autocomplete="off" placeholder="Search corporations or enter ID" class="w-full field-input" />
                            <button type="button" id="manual_hostile_corporation_add" class="rounded-lg border border-border bg-black/30 px-3 py-2 text-sm text-slate-100 transition hover:bg-white/5">Add</button>
                        </div>
                        <p id="manual_hostile_corporation_status" class="text-xs text-muted"><?= htmlspecialchars($manualHostileCorporationSelections === [] ? 'No manual hostile corporations.' : count($manualHostileCorporationSelections) . ' manual hostile corporation' . (count($manualHostileCorporationSelections) === 1 ? '' : 's') . '.', ENT_QUOTES) ?></p>
                        <ul id="manual_hostile_corporation_results" class="hidden max-h-60 overflow-y-auto surface-tertiary"></ul>
                        <div id="manual_hostile_corporation_selected" class="space-y-2 rounded-lg border border-border bg-black/10 p-3"></div>
                    </div>
                </div>

                <div class="rounded-lg border border-cyan-500/20 bg-cyan-500/5 p-3 text-sm text-muted space-y-2 mt-3">
                    <p>ESI contacts are read-only — update them in-game and wait for the next sync. Manual contacts are saved with this form and persist across syncs.</p>
                    <p>Theater Intelligence uses these contacts to classify sides: <span class="text-blue-300">positive standing = friendly</span>, <span class="text-red-300">negative standing = opponent</span>.</p>
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

                        // Manual corp contact pickers
                        createTrackedEntityPicker({
                            inputId: 'manual_friendly_alliance_search',
                            addButtonId: 'manual_friendly_alliance_add',
                            hiddenId: 'manual_friendly_alliance_contacts',
                            resultsId: 'manual_friendly_alliance_results',
                            statusId: 'manual_friendly_alliance_status',
                            selectedId: 'manual_friendly_alliance_selected',
                            allowedType: 'Alliance',
                            initialItems: <?= json_encode($manualFriendlyAllianceSelections, JSON_THROW_ON_ERROR) ?>,
                        });

                        createTrackedEntityPicker({
                            inputId: 'manual_friendly_corporation_search',
                            addButtonId: 'manual_friendly_corporation_add',
                            hiddenId: 'manual_friendly_corporation_contacts',
                            resultsId: 'manual_friendly_corporation_results',
                            statusId: 'manual_friendly_corporation_status',
                            selectedId: 'manual_friendly_corporation_selected',
                            allowedType: 'Corporation',
                            initialItems: <?= json_encode($manualFriendlyCorporationSelections, JSON_THROW_ON_ERROR) ?>,
                        });

                        createTrackedEntityPicker({
                            inputId: 'manual_hostile_alliance_search',
                            addButtonId: 'manual_hostile_alliance_add',
                            hiddenId: 'manual_hostile_alliance_contacts',
                            resultsId: 'manual_hostile_alliance_results',
                            statusId: 'manual_hostile_alliance_status',
                            selectedId: 'manual_hostile_alliance_selected',
                            allowedType: 'Alliance',
                            initialItems: <?= json_encode($manualHostileAllianceSelections, JSON_THROW_ON_ERROR) ?>,
                        });

                        createTrackedEntityPicker({
                            inputId: 'manual_hostile_corporation_search',
                            addButtonId: 'manual_hostile_corporation_add',
                            hiddenId: 'manual_hostile_corporation_contacts',
                            resultsId: 'manual_hostile_corporation_results',
                            statusId: 'manual_hostile_corporation_status',
                            selectedId: 'manual_hostile_corporation_selected',
                            allowedType: 'Corporation',
                            initialItems: <?= json_encode($manualHostileCorporationSelections, JSON_THROW_ON_ERROR) ?>,
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

                <?php
                    $backfillNeeded = killmail_backfill_needed();
                    $backfillProgress = killmail_backfill_progress();
                    $backfillRunning = $backfillProgress !== null;
                    $defaultKillmailBackfillStart = date('Y') . '-01-01';
                    $configuredKillmailBackfillStart = sanitize_backfill_start_date($settingValues['killmail_backfill_start_date'] ?? '');
                    $killmailBackfillStartInput = $configuredKillmailBackfillStart !== '' ? $configuredKillmailBackfillStart : $defaultKillmailBackfillStart;
                ?>
                <?php if ($backfillRunning): ?>
                    <div class="rounded-2xl border border-amber-500/30 bg-amber-500/5 px-4 py-3">
                        <div class="flex items-center gap-3">
                            <div class="size-2 animate-pulse rounded-full bg-amber-400"></div>
                            <p class="text-sm font-semibold text-amber-200">History Backfill In Progress</p>
                        </div>
                        <?php $backfillPhase = (string) ($backfillProgress['phase'] ?? 'collecting'); ?>
                        <?php if ($backfillPhase === 'collecting'): ?>
                            <div class="mt-2 grid gap-2 text-sm text-slate-300 md:grid-cols-3">
                                <p>Querying: <span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($backfillProgress['entity'] ?? '—'), ENT_QUOTES) ?></span></p>
                                <p>Entities: <span class="font-medium text-slate-100"><?= (int) ($backfillProgress['entities_done'] ?? 0) ?>/<?= (int) ($backfillProgress['entities_total'] ?? 0) ?></span></p>
                                <p>Killmails found: <span class="font-medium text-slate-100"><?= number_format((int) ($backfillProgress['killmails_found'] ?? 0)) ?></span></p>
                            </div>
                        <?php else: ?>
                            <div class="mt-2 grid gap-2 text-sm text-slate-300 md:grid-cols-5">
                                <p>ESI progress: <span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($backfillProgress['esi_progress'] ?? '—'), ENT_QUOTES) ?></span></p>
                                <p>Killmails seen: <span class="font-medium text-slate-100"><?= number_format((int) ($backfillProgress['killmails_seen'] ?? 0)) ?></span></p>
                                <p>Skipped (cached): <span class="font-medium text-slate-100"><?= number_format((int) ($backfillProgress['skipped_existing'] ?? 0)) ?></span></p>
                                <p>Written: <span class="font-medium text-slate-100"><?= number_format((int) ($backfillProgress['written'] ?? 0)) ?></span></p>
                                <p>Filtered: <span class="font-medium text-slate-100"><?= number_format((int) ($backfillProgress['filtered'] ?? 0)) ?></span></p>
                            </div>
                        <?php endif; ?>
                        <p class="mt-1 text-xs text-muted">Refresh this page to see updated progress. Backfill runs in the background.</p>
                    </div>
                <?php elseif ($backfillNeeded): ?>
                    <div class="rounded-2xl border border-blue-500/30 bg-blue-500/5 px-4 py-3">
                        <div class="flex flex-col gap-4">
                            <div>
                                <p class="text-sm font-semibold text-blue-200">History Backfill Available</p>
                                <p class="mt-1 text-xs text-muted">
                                    Fetch historical killmails from the zKillboard API for <?= date('Y') ?>,
                                    filtered by your tracked alliances and corporations. Full killmail details
                                    are then loaded from ESI. Already-stored killmails are skipped automatically.
                                </p>
                            </div>
                            <div class="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
                                <label class="block max-w-xs space-y-1">
                                    <span class="text-xs font-medium uppercase tracking-[0.14em] text-blue-100/80">Backfill start date</span>
                                    <input
                                        type="date"
                                        name="killmail_backfill_start_date"
                                        value="<?= htmlspecialchars($killmailBackfillStartInput, ENT_QUOTES) ?>"
                                        max="<?= htmlspecialchars(date('Y-m-d'), ENT_QUOTES) ?>"
                                        class="w-full field-input"
                                    />
                                </label>
                                <button type="submit" name="killmail_backfill_start" value="1" class="shrink-0 rounded-lg border border-blue-400/40 bg-blue-500/10 px-4 py-2 text-sm font-medium text-blue-100 hover:bg-blue-500/20" onclick="this.textContent='Starting…';var h=document.createElement('input');h.type='hidden';h.name='killmail_backfill_start';h.value='1';this.form.appendChild(h);this.disabled=true;">
                                    Start backfill
                                </button>
                            </div>
                        </div>
                    </div>
                <?php endif; ?>

                <p class="text-sm text-muted">Ingestion consumes R2Z2 as an ordered stream and keeps killmails only when a tracked alliance or corporation appears on the victim side. That keeps the board focused on tracked losses while leaving attacker details available only inside each stored loss.</p>
                <button class="btn-primary">Save Killmail Intelligence Settings</button>
            </form>
        <?php elseif ($activeSubsection === 'automation-control'): ?>
            <form class="mt-6 space-y-5" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="automation-control">

                <section class="space-y-3 rounded-2xl border border-border bg-black/20 p-4">
                    <div>
                        <p class="text-sm font-semibold text-slate-100">Integration runtime toggles</p>
                        <p class="mt-1 text-xs text-muted">Centralized runtime switches that were previously spread across multiple settings pages.</p>
                    </div>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="esi_enabled" value="0">
                        <input type="checkbox" name="esi_enabled" value="1" <?= ($settingValues['esi_enabled'] ?? '0') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable ESI OAuth import runtime</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="killmail_ingestion_enabled" value="0">
                        <input type="checkbox" name="killmail_ingestion_enabled" value="1" <?= ($settingValues['killmail_ingestion_enabled'] ?? '0') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable zKill stream ingestion runtime</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <input type="hidden" name="incremental_updates_enabled" value="0">
                        <input type="checkbox" name="incremental_updates_enabled" value="1" <?= ($dataSyncSettingValues['incremental_updates_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                        <span class="text-sm">Enable incremental update execution</span>
                    </label>
                    <div class="grid gap-3 md:grid-cols-2">
                        <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                            <input type="hidden" name="alliance_current_pipeline_enabled" value="0">
                            <input type="checkbox" name="alliance_current_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['alliance_current_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                            <span class="text-sm">Alliance current pipeline</span>
                        </label>
                        <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                            <input type="hidden" name="alliance_history_pipeline_enabled" value="0">
                            <input type="checkbox" name="alliance_history_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['alliance_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                            <span class="text-sm">Alliance history pipeline</span>
                        </label>
                        <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                            <input type="hidden" name="hub_history_pipeline_enabled" value="0">
                            <input type="checkbox" name="hub_history_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['hub_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                            <span class="text-sm">Hub history pipeline</span>
                        </label>
                        <label class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                            <input type="hidden" name="market_hub_local_history_pipeline_enabled" value="0">
                            <input type="checkbox" name="market_hub_local_history_pipeline_enabled" value="1" <?= ($dataSyncSettingValues['market_hub_local_history_pipeline_enabled'] ?? '1') === '1' ? 'checked' : '' ?> class="size-4 rounded border-border bg-black">
                            <span class="text-sm">Hub local history pipeline</span>
                        </label>
                    </div>
                    <button class="btn-primary" name="automation_action" value="save-flags">Save Runtime Toggles</button>
                </section>

                <section class="space-y-3 rounded-2xl border border-border bg-black/20 p-4">
                    <div class="flex flex-wrap items-center justify-between gap-3">
                        <div>
                            <p class="text-sm font-semibold text-slate-100">Recurring job run controls</p>
                            <p class="mt-1 text-xs text-muted">Enable or disable all recurring schedule rows from one place.</p>
                        </div>
                        <span class="inline-flex items-center rounded-full border border-border px-3 py-1 text-xs uppercase tracking-[0.14em] text-slate-100"><?= $automationEnabledCount ?> / <?= count($automationJobs) ?> enabled</span>
                    </div>
                    <div class="flex flex-wrap gap-2">
                        <button type="submit" name="automation_action" value="enable-all-jobs" class="rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 py-2 text-sm font-medium text-emerald-100 hover:bg-emerald-500/20">Enable all recurring jobs</button>
                        <button type="submit" name="automation_action" value="disable-all-jobs" class="rounded-lg border border-rose-400/40 bg-rose-500/10 px-4 py-2 text-sm font-medium text-rose-100 hover:bg-rose-500/20">Disable all recurring jobs</button>
                    </div>
                    <div class="grid gap-3 md:grid-cols-2">
                        <?php foreach ($automationJobs as $job): ?>
                            <label class="rounded-lg border border-border bg-black/30 p-3">
                                <span class="flex items-start justify-between gap-2">
                                    <span>
                                        <span class="text-sm text-slate-100"><?= htmlspecialchars((string) ($job['label'] ?? $job['job_key']), ENT_QUOTES) ?></span>
                                        <span class="mt-1 block text-xs text-muted font-mono"><?= htmlspecialchars((string) ($job['job_key'] ?? ''), ENT_QUOTES) ?></span>
                                    </span>
                                    <span class="text-xs <?= !empty($job['enabled']) ? 'text-emerald-200' : 'text-amber-200' ?>"><?= !empty($job['enabled']) ? 'enabled' : 'disabled' ?></span>
                                </span>
                                <span class="mt-2 block text-xs text-muted">Every <?= (int) ($job['interval_minutes'] ?? 0) ?> min · next <?= htmlspecialchars((string) (($job['next_due_at'] ?? '') !== '' ? $job['next_due_at'] : 'not scheduled'), ENT_QUOTES) ?></span>
                                <?php if (trim((string) ($job['review_reason'] ?? '')) !== ''): ?>
                                    <span class="mt-1 block text-xs text-amber-200"><?= htmlspecialchars((string) ($job['review_reason'] ?? ''), ENT_QUOTES) ?></span>
                                <?php endif; ?>
                                <span class="mt-2 flex items-center gap-2 text-xs text-muted">
                                    <input type="checkbox" name="managed_job_keys[]" value="<?= htmlspecialchars((string) ($job['job_key'] ?? ''), ENT_QUOTES) ?>" class="size-4 rounded border-border bg-black">
                                    Select
                                </span>
                            </label>
                        <?php endforeach; ?>
                        <?php if ($automationJobs === []): ?>
                            <div class="rounded-lg border border-dashed border-border bg-black/20 p-3 text-xs text-muted md:col-span-2">No manageable recurring jobs were discovered.</div>
                        <?php endif; ?>
                    </div>
                    <div class="flex flex-wrap gap-2">
                        <button type="submit" name="automation_action" value="enable-selected-jobs" class="rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-4 py-2 text-sm font-medium text-emerald-100 hover:bg-emerald-500/20">Enable selected jobs</button>
                        <button type="submit" name="automation_action" value="disable-selected-jobs" class="rounded-lg border border-rose-400/40 bg-rose-500/10 px-4 py-2 text-sm font-medium text-rose-100 hover:bg-rose-500/20">Disable selected jobs</button>
                    </div>
                </section>
            </form>
        <?php elseif ($activeSubsection === 'esi-login'): ?>
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
                <div class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <span class="inline-flex size-4 items-center justify-center rounded border <?= ($settingValues['esi_enabled'] ?? '0') === '1' ? 'border-emerald-500/60 bg-emerald-500/20 text-emerald-300' : 'border-border bg-black/40 text-slate-500' ?> text-xs"><?= ($settingValues['esi_enabled'] ?? '0') === '1' ? '✓' : '' ?></span>
                    <span class="text-sm">ESI OAuth login <?= ($settingValues['esi_enabled'] ?? '0') === '1' ? 'enabled' : 'disabled' ?></span>
                    <a href="<?= htmlspecialchars($sectionUrl('automation-sync', 'automation-control'), ENT_QUOTES) ?>" class="ml-auto text-xs text-slate-400 underline decoration-dotted underline-offset-4">Change in Automation Control</a>
                </div>
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
        <?php elseif ($activeSubsection === 'deal-alerts'): ?>
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
        <?php elseif ($activeSubsection === 'backup-restore'): ?>
            <?php $backupScopes = supplycore_backup_data_scope_options(); ?>
            <div class="mt-6 grid gap-6 xl:grid-cols-2">
                <form class="space-y-4 rounded-2xl border border-border bg-black/20 p-4" method="post">
                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                    <input type="hidden" name="section" value="backup-restore">
                    <input type="hidden" name="backup_action" value="export">
                    <div>
                        <p class="text-sm font-semibold text-slate-100">Create backup</p>
                        <p class="mt-1 text-xs text-muted">Generate a JSON backup that always includes all app settings and can optionally include all database tables.</p>
                    </div>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Backup scope</span>
                        <select name="backup_scope" class="w-full field-input">
                            <?php foreach ($backupScopes as $scopeKey => $scopeLabel): ?>
                                <option value="<?= htmlspecialchars($scopeKey, ENT_QUOTES) ?>"><?= htmlspecialchars($scopeLabel, ENT_QUOTES) ?></option>
                            <?php endforeach; ?>
                        </select>
                    </label>
                    <div class="rounded-xl border border-border bg-black/30 p-3 text-xs text-muted space-y-1">
                        <p>Smart defaults included: format versioning, UTC timestamp, and fingerprint metadata.</p>
                        <p>For large datasets, prefer settings-only backups for faster export/import cycles.</p>
                    </div>
                    <button class="btn-primary">Download backup JSON</button>
                </form>

                <form class="space-y-4 rounded-2xl border border-border bg-black/20 p-4" method="post" enctype="multipart/form-data">
                    <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                    <input type="hidden" name="section" value="backup-restore">
                    <input type="hidden" name="backup_action" value="restore">
                    <div>
                        <p class="text-sm font-semibold text-slate-100">Restore backup</p>
                        <p class="mt-1 text-xs text-muted">Upload a prior backup JSON, validate with dry-run, then apply settings and optional table data restore.</p>
                    </div>
                    <label class="block space-y-2">
                        <span class="text-sm text-muted">Backup file</span>
                        <input type="file" name="backup_file" accept="application/json,.json" class="w-full field-input" required>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/30 p-3">
                        <input type="hidden" name="restore_settings" value="0">
                        <input type="checkbox" name="restore_settings" value="1" checked class="size-4 rounded border-border bg-black">
                        <span class="text-sm text-slate-200">Restore settings (`app_settings`)</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/30 p-3">
                        <input type="hidden" name="restore_data" value="0">
                        <input type="checkbox" name="restore_data" value="1" class="size-4 rounded border-border bg-black">
                        <span class="text-sm text-slate-200">Restore table data included in backup</span>
                    </label>
                    <label class="flex items-center gap-3 rounded-lg border border-border bg-black/30 p-3">
                        <input type="hidden" name="restore_dry_run" value="0">
                        <input type="checkbox" name="restore_dry_run" value="1" checked class="size-4 rounded border-border bg-black">
                        <span class="text-sm text-slate-200">Dry-run first (validate only, no writes)</span>
                    </label>
                    <div class="rounded-xl border border-amber-400/30 bg-amber-500/10 p-3 text-xs text-amber-100">
                        Data restore truncates selected tables before inserting rows from backup. Keep dry-run enabled until you are ready to apply.
                    </div>
                    <button class="btn-primary">Validate / restore backup</button>
                </form>
            </div>
        <?php elseif ($activeSubsection === 'public-api'): ?>
            <?php
            require_once __DIR__ . '/../../src/public_api.php';
            $apiKeys = public_api_keys_load();
            $baseUrl = rtrim((string) get_setting('app_base_url', ''), '/');
            $flashMessage = flash('success');
            ?>
            <div class="mt-6 space-y-6">
                <?php if ($flashMessage !== null): ?>
                    <div class="rounded-lg border border-emerald-500/40 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200 break-all">
                        <?= htmlspecialchars($flashMessage, ENT_QUOTES) ?>
                    </div>
                <?php endif; ?>

                <!-- Existing API Keys -->
                <section class="rounded-2xl border border-border bg-black/20 p-4 space-y-4">
                    <div>
                        <p class="text-sm font-semibold text-slate-100">API Keys</p>
                        <p class="mt-1 text-xs text-muted">Registered HMAC key pairs for external integrations. Secrets are not shown after creation.</p>
                    </div>

                    <?php if ($apiKeys === []): ?>
                        <p class="text-sm text-muted">No API keys configured yet.</p>
                    <?php else: ?>
                        <div class="space-y-3">
                            <?php foreach ($apiKeys as $keyId => $keyEntry): ?>
                                <div class="flex flex-wrap items-center gap-3 rounded-xl border border-border bg-black/30 p-3">
                                    <div class="min-w-0 flex-1">
                                        <p class="text-sm font-medium text-slate-100"><?= htmlspecialchars((string) ($keyEntry['label'] ?? ''), ENT_QUOTES) ?></p>
                                        <p class="mt-0.5 font-mono text-xs text-muted"><?= htmlspecialchars($keyId, ENT_QUOTES) ?></p>
                                        <?php if (($keyEntry['allowed_ips'] ?? []) !== []): ?>
                                            <p class="mt-0.5 text-xs text-muted">IP allowlist: <?= htmlspecialchars(implode(', ', (array) $keyEntry['allowed_ips']), ENT_QUOTES) ?></p>
                                        <?php endif; ?>
                                        <p class="mt-0.5 text-xs text-muted">Created: <?= htmlspecialchars((string) ($keyEntry['created_at'] ?? '-'), ENT_QUOTES) ?></p>
                                    </div>
                                    <div class="flex items-center gap-2">
                                        <!-- Generate provisioning token -->
                                        <form method="post" class="inline">
                                            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                                            <input type="hidden" name="section" value="public-api">
                                            <input type="hidden" name="api_action" value="generate-token">
                                            <input type="hidden" name="token_key_id" value="<?= htmlspecialchars($keyId, ENT_QUOTES) ?>">
                                            <label class="sr-only" for="ttl_<?= htmlspecialchars($keyId, ENT_QUOTES) ?>">TTL (minutes)</label>
                                            <select name="token_ttl" id="ttl_<?= htmlspecialchars($keyId, ENT_QUOTES) ?>" class="field-input py-1 text-xs">
                                                <option value="5">5 min</option>
                                                <option value="10" selected>10 min</option>
                                                <option value="30">30 min</option>
                                                <option value="60">60 min</option>
                                            </select>
                                            <button type="submit" class="rounded-lg border border-accent/40 bg-accent/10 px-3 py-1.5 text-xs font-medium text-slate-100 hover:bg-accent/20">Export Token</button>
                                        </form>
                                        <!-- Revoke key -->
                                        <form method="post" class="inline" onsubmit="return confirm('Revoke this API key? Any integrations using it will stop working.')">
                                            <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                                            <input type="hidden" name="section" value="public-api">
                                            <input type="hidden" name="api_action" value="revoke-key">
                                            <input type="hidden" name="revoke_key_id" value="<?= htmlspecialchars($keyId, ENT_QUOTES) ?>">
                                            <button type="submit" class="rounded-lg border border-rose-400/40 bg-rose-500/10 px-3 py-1.5 text-xs font-medium text-rose-100 hover:bg-rose-500/20">Revoke</button>
                                        </form>
                                    </div>
                                </div>
                            <?php endforeach; ?>
                        </div>
                    <?php endif; ?>
                </section>

                <!-- Generate New Key -->
                <section class="rounded-2xl border border-border bg-black/20 p-4">
                    <form method="post" class="space-y-4">
                        <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                        <input type="hidden" name="section" value="public-api">
                        <input type="hidden" name="api_action" value="generate-key">
                        <div>
                            <p class="text-sm font-semibold text-slate-100">Create new API key</p>
                            <p class="mt-1 text-xs text-muted">Generate an HMAC key pair for a new integration. The secret is shown once after creation — copy it immediately.</p>
                        </div>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Label</span>
                            <input type="text" name="api_key_label" placeholder="e.g. Alliance proxy, Restock bot" class="w-full field-input" required>
                        </label>
                        <label class="block space-y-2">
                            <span class="text-sm text-muted">Allowed IPs (optional, comma-separated)</span>
                            <input type="text" name="api_key_allowed_ips" placeholder="e.g. 203.0.113.10, 198.51.100.5" class="w-full field-input">
                        </label>
                        <button class="btn-primary">Generate API key</button>
                    </form>
                </section>

                <!-- How provisioning works -->
                <section class="rounded-2xl border border-border bg-black/20 p-4 space-y-3">
                    <p class="text-sm font-semibold text-slate-100">How provisioning tokens work</p>
                    <div class="space-y-2 text-xs text-muted">
                        <p>Click <strong class="text-slate-200">Export Token</strong> next to an API key to generate a single-use provisioning URL.</p>
                        <p>Send the URL to whoever is setting up the remote proxy. When they open it, the proxy receives the full connection config (endpoint, key ID, secret) and the token is consumed — it cannot be reused.</p>
                        <p>Tokens expire after the selected TTL. If the token expires unused, generate a new one.</p>
                    </div>
                    <?php if ($baseUrl === ''): ?>
                        <div class="rounded-xl border border-amber-400/30 bg-amber-500/10 p-3 text-xs text-amber-100">
                            <strong>Warning:</strong> <code>app_base_url</code> is not set. Provisioning URLs will be incomplete. Set it in Workspace Settings or your <code>.env</code> file.
                        </div>
                    <?php endif; ?>
                </section>
            </div>
        <?php else: ?>
            <form class="mt-6 space-y-4" method="post">
                <input type="hidden" name="_token" value="<?= htmlspecialchars(csrf_token(), ENT_QUOTES) ?>">
                <input type="hidden" name="section" value="data-sync">
                <div class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                    <span class="inline-flex size-4 items-center justify-center rounded border <?= ($dataSyncSettingValues['incremental_updates_enabled'] ?? '1') === '1' ? 'border-emerald-500/60 bg-emerald-500/20 text-emerald-300' : 'border-border bg-black/40 text-slate-500' ?> text-xs"><?= ($dataSyncSettingValues['incremental_updates_enabled'] ?? '1') === '1' ? '✓' : '' ?></span>
                    <span class="text-sm">Incremental database updates <?= ($dataSyncSettingValues['incremental_updates_enabled'] ?? '1') === '1' ? 'enabled' : 'disabled' ?></span>
                    <a href="<?= htmlspecialchars($sectionUrl('automation-sync', 'automation-control'), ENT_QUOTES) ?>" class="ml-auto text-xs text-slate-400 underline decoration-dotted underline-offset-4">Change in Automation Control</a>
                </div>
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
                        <p class="mt-2 text-xs text-muted">
                            <a class="underline decoration-dotted underline-offset-4 hover:text-slate-100" href="<?= htmlspecialchars($sectionUrl('automation-sync', 'data-sync'), ENT_QUOTES) ?>&amp;show_sync_diagnostics=<?= $showSyncDiagnostics ? '0' : '1' ?>">
                                <?= $showSyncDiagnostics ? 'Hide advanced diagnostics' : 'Show advanced diagnostics' ?>
                            </a>
                        </p>
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

                    <?php
                        $migrationCheck = supplycore_check_pending_migrations();
                        $migrationPendingCount = (int) ($migrationCheck['pending'] ?? 0);
                        $migrationPendingFiles = (array) ($migrationCheck['files'] ?? []);
                        $allMigrations = [];
                        try {
                            $migPdo = supplycore_migration_pdo();
                            $migStmt = $migPdo->query('SELECT filename, file_hash, applied_at, status, error_message FROM schema_migrations ORDER BY filename ASC');
                            $allMigrations = $migStmt->fetchAll();
                        } catch (Throwable) {}
                        $failedMigrations = array_filter($allMigrations, static fn (array $m): bool => (string) ($m['status'] ?? '') === 'failed');
                    ?>
                    <div class="rounded-xl border <?= $migrationPendingCount > 0 ? 'border-amber-400/30 bg-amber-500/5' : (count($failedMigrations) > 0 ? 'border-rose-400/30 bg-rose-500/5' : 'border-border bg-black/20') ?> p-4">
                        <div class="flex items-start justify-between gap-3">
                            <div>
                                <p class="text-sm font-semibold text-slate-100">Database migrations</p>
                                <p class="mt-1 text-xs text-muted">SQL schema changes are auto-applied on page load. New or updated files in <code class="text-slate-300">database/migrations/</code> are detected and run automatically.</p>
                            </div>
                            <?php if ($migrationPendingCount > 0): ?>
                                <span class="inline-flex items-center rounded-full border border-amber-400/40 bg-amber-500/10 px-3 py-1 text-xs font-medium uppercase tracking-[0.16em] text-amber-100"><?= $migrationPendingCount ?> pending</span>
                            <?php elseif (count($failedMigrations) > 0): ?>
                                <span class="inline-flex items-center rounded-full border border-rose-400/40 bg-rose-500/10 px-3 py-1 text-xs font-medium uppercase tracking-[0.16em] text-rose-100"><?= count($failedMigrations) ?> failed</span>
                            <?php else: ?>
                                <span class="inline-flex items-center rounded-full border border-emerald-400/20 bg-emerald-500/10 px-3 py-1 text-xs font-medium uppercase tracking-[0.16em] text-emerald-100">Up to date</span>
                            <?php endif; ?>
                        </div>

                        <?php if ($migrationPendingCount > 0): ?>
                            <div class="mt-3 space-y-1">
                                <?php foreach ($migrationPendingFiles as $pendingFile): ?>
                                    <p class="text-xs text-amber-200"><span class="mr-2 font-medium uppercase tracking-[0.14em]">Pending</span> <?= htmlspecialchars($pendingFile, ENT_QUOTES) ?></p>
                                <?php endforeach; ?>
                            </div>
                            <div class="mt-3">
                                <button type="submit" name="data_sync_action" value="run-migrations" class="inline-flex items-center rounded-full border border-cyan-400/40 bg-cyan-500/10 px-3 py-1.5 text-xs font-medium uppercase tracking-[0.14em] text-cyan-100 hover:bg-cyan-500/20">Run migrations now</button>
                            </div>
                        <?php endif; ?>

                        <?php foreach ($failedMigrations as $failed): ?>
                            <div class="mt-3 rounded-lg border border-rose-400/40 bg-rose-500/10 p-3 text-xs text-rose-100">
                                <p class="font-medium uppercase tracking-[0.14em] text-rose-200">Failed: <?= htmlspecialchars((string) ($failed['filename'] ?? ''), ENT_QUOTES) ?></p>
                                <p class="mt-1"><?= htmlspecialchars((string) ($failed['error_message'] ?? 'Unknown error'), ENT_QUOTES) ?></p>
                                <p class="mt-1 text-rose-300/70"><?= htmlspecialchars((string) ($failed['applied_at'] ?? ''), ENT_QUOTES) ?></p>
                            </div>
                        <?php endforeach; ?>

                        <?php if (count($allMigrations) > 0): ?>
                            <details class="mt-3">
                                <summary class="cursor-pointer text-xs text-muted hover:text-slate-100">Show all <?= count($allMigrations) ?> migration(s)</summary>
                                <div class="mt-2 space-y-1">
                                    <?php foreach ($allMigrations as $migration): ?>
                                        <p class="text-xs <?= (string) ($migration['status'] ?? '') === 'failed' ? 'text-rose-200' : 'text-slate-400' ?>">
                                            <span class="mr-1 inline-block w-14 font-medium uppercase tracking-[0.14em] <?= (string) ($migration['status'] ?? '') === 'failed' ? 'text-rose-300' : 'text-emerald-300' ?>"><?= htmlspecialchars((string) ($migration['status'] ?? 'unknown'), ENT_QUOTES) ?></span>
                                            <?= htmlspecialchars((string) ($migration['filename'] ?? ''), ENT_QUOTES) ?>
                                            <span class="text-slate-500 ml-2"><?= htmlspecialchars((string) ($migration['applied_at'] ?? ''), ENT_QUOTES) ?></span>
                                        </p>
                                    <?php endforeach; ?>
                                </div>
                            </details>
                        <?php endif; ?>
                    </div>

                    <?php if ($showSyncDiagnostics): ?>
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
                                <?php
                                    $syncWarningIssues = array_filter((array) ($schedule['issues'] ?? []), static fn (array $issue): bool => in_array((string) ($issue['type'] ?? ''), ['missing_token', 'sync_warning'], true));
                                ?>
                                <?php if ($syncWarningIssues !== []): ?>
                                    <?php foreach ($syncWarningIssues as $syncWarning): ?>
                                        <div class="mt-3 rounded-lg border <?= (string) ($syncWarning['severity'] ?? 'high') === 'critical' ? 'border-rose-400/40 bg-rose-500/10' : 'border-amber-400/40 bg-amber-500/10' ?> p-3">
                                            <div class="flex items-start gap-2">
                                                <span class="mt-0.5 text-base leading-none"><?= (string) ($syncWarning['severity'] ?? 'high') === 'critical' ? '&#9888;' : '&#9888;' ?></span>
                                                <div>
                                                    <p class="text-sm font-medium <?= (string) ($syncWarning['severity'] ?? 'high') === 'critical' ? 'text-rose-100' : 'text-amber-100' ?>"><?= htmlspecialchars((string) ($syncWarning['title'] ?? ''), ENT_QUOTES) ?></p>
                                                    <p class="mt-1 text-xs <?= (string) ($syncWarning['severity'] ?? 'high') === 'critical' ? 'text-rose-200/70' : 'text-amber-200/70' ?>"><?= htmlspecialchars((string) ($syncWarning['description'] ?? ''), ENT_QUOTES) ?></p>
                                                </div>
                                            </div>
                                        </div>
                                    <?php endforeach; ?>
                                <?php endif; ?>
                                <div class="mt-3 flex flex-wrap items-center gap-2">
                                    <?php if (!empty($schedule['needs_attention_action'])): ?>
                                        <button type="submit" name="data_sync_action" value="stop-investigate-job" class="inline-flex items-center rounded-full border border-amber-400/40 bg-amber-500/10 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.14em] text-amber-100 hover:bg-amber-500/20" formaction="<?= htmlspecialchars($sectionUrl('automation-sync', 'data-sync'), ENT_QUOTES) ?>&amp;job_action_job_key=<?= urlencode($jobKey) ?>" formnovalidate>
                                            <span>Stop &amp; investigate</span>
                                        </button>
                                    <?php endif; ?>
                                    <?php if ((string) ($schedule['last_result'] ?? '') === 'failed'): ?>
                                        <button type="submit" name="data_sync_action" value="retry-job" class="inline-flex items-center rounded-full border border-emerald-400/40 bg-emerald-500/10 px-2.5 py-1 text-[11px] font-medium uppercase tracking-[0.14em] text-emerald-100 hover:bg-emerald-500/20" formaction="<?= htmlspecialchars($sectionUrl('automation-sync', 'data-sync'), ENT_QUOTES) ?>&amp;job_action_job_key=<?= urlencode($jobKey) ?>" formnovalidate>
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
                                                <div class="flex items-center justify-between gap-2"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($schedule['label'] ?? $schedule['job_key']), ENT_QUOTES) ?></span><span><?= htmlspecialchars((string) (($schedule['registry_category'] ?? '') === 'external_integrated' ? 'external' : 'review-needed'), ENT_QUOTES) ?></span></div>
                                                <p class="mt-1"><?= htmlspecialchars((string) ($schedule['job_key'] ?? ''), ENT_QUOTES) ?></p>
                                                <?php if (trim((string) ($schedule['review_reason'] ?? '')) !== ''): ?>
                                                    <p class="mt-1 text-amber-200"><?= htmlspecialchars((string) ($schedule['review_reason'] ?? ''), ENT_QUOTES) ?></p>
                                                <?php endif; ?>
                                            </div>
                                        <?php endforeach; ?>
                                    </div>
                                </div>
                                <div>
                                    <p class="text-sm text-slate-100">Triggered child jobs</p>
                                    <div class="mt-2 space-y-2 text-xs text-muted">
                                        <?php if ($internalSyncJobs === []): ?>
                                            <div class="rounded-lg border border-dashed border-border bg-black/30 p-3">No child jobs are currently modeled under parent pipelines.</div>
                                        <?php endif; ?>
                                        <?php foreach ($internalSyncJobs as $schedule): ?>
                                            <div class="rounded-lg border border-border bg-black/30 p-3">
                                                <div class="flex items-center justify-between gap-2"><span class="font-medium text-slate-100"><?= htmlspecialchars((string) ($schedule['label'] ?? $schedule['job_key']), ENT_QUOTES) ?></span><span>triggered</span></div>
                                                <p class="mt-1"><?= htmlspecialchars((string) ($schedule['job_key'] ?? ''), ENT_QUOTES) ?></p>
                                                <p class="mt-1">Triggered by <?= htmlspecialchars((string) ($schedule['parent_job_key'] ?? 'parent job'), ENT_QUOTES) ?></p>
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
                    <?php endif; ?>
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
                <?php if ($showSyncDiagnostics): ?>
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
                <?php endif; ?>

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
                    <div class="flex items-start justify-between gap-3">
                        <div>
                            <p class="text-sm text-muted">Pipeline toggles</p>
                            <p class="mt-1 text-xs text-muted">Pipeline runtime flags are managed centrally from Automation Control.</p>
                        </div>
                        <a href="<?= htmlspecialchars($sectionUrl('automation-sync', 'automation-control'), ENT_QUOTES) ?>" class="shrink-0 text-xs text-slate-400 underline decoration-dotted underline-offset-4">Automation Control →</a>
                    </div>
                    <?php
                        $pipelineFlags = [
                            ['key' => 'alliance_current_pipeline_enabled', 'label' => 'Alliance current pipeline'],
                            ['key' => 'alliance_history_pipeline_enabled', 'label' => 'Alliance history/backfill pipeline'],
                            ['key' => 'hub_history_pipeline_enabled', 'label' => 'Market hub history pipeline'],
                            ['key' => 'market_hub_local_history_pipeline_enabled', 'label' => 'Hub snapshot-history refresh pipeline'],
                        ];
                        foreach ($pipelineFlags as $flag):
                            $flagEnabled = ($dataSyncSettingValues[$flag['key']] ?? '1') === '1';
                    ?>
                    <div class="flex items-center gap-3 rounded-lg border border-border bg-black/20 p-3">
                        <span class="inline-flex size-4 items-center justify-center rounded border <?= $flagEnabled ? 'border-emerald-500/60 bg-emerald-500/20 text-emerald-300' : 'border-border bg-black/40 text-slate-500' ?> text-xs"><?= $flagEnabled ? '✓' : '' ?></span>
                        <span class="text-sm"><?= htmlspecialchars($flag['label'], ENT_QUOTES) ?></span>
                        <span class="ml-auto text-xs <?= $flagEnabled ? 'text-emerald-400' : 'text-slate-500' ?>"><?= $flagEnabled ? 'On' : 'Off' ?></span>
                    </div>
                    <?php endforeach; ?>
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
                    <p class="text-xs text-muted">Clears stuck schedule locks and resets the scheduler daemon state. Use when jobs appear stuck or locks are not releasing.</p>
                    <button name="data_sync_action" value="static-data-import" class="rounded-lg border border-border px-4 py-2 text-sm font-medium text-slate-100 hover:bg-white/5">Import EVE Static Data</button>
                </div>
            </form>
        <?php endif; ?>
    </section>
</div>
<?php include __DIR__ . '/../../src/views/partials/footer.php'; ?>
