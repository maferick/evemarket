<?php

declare(strict_types=1);

require_once __DIR__ . '/../../../src/bootstrap.php';
require_once __DIR__ . '/../../../src/public_api.php';

// ── Authenticate ──
$apiKey = public_api_authenticate();

// ── Optional filters ──
$page = max(1, (int) ($_GET['page'] ?? 1));
$perPage = min(50, max(1, (int) ($_GET['per_page'] ?? 50)));
$offset = ($page - 1) * $perPage;

// ── Load theaters (only those with tracked-alliance participation) ──
$theaters = db_theaters_list($perPage, $offset);

$theaterIds = array_column($theaters, 'theater_id');
$sideLabelsMap = $theaterIds !== [] ? db_theater_side_labels($theaterIds) : [];

// ── Build response rows ──
$rows = [];
foreach ($theaters as $t) {
    $tid = (string) ($t['theater_id'] ?? '');
    $sides = $sideLabelsMap[$tid] ?? [];
    $friendlyBucket = $sides['friendly'] ?? null;
    $hostileBucket = $sides['hostile'] ?? null;

    $friendlyLabel = 'Friendlies';
    if ($friendlyBucket !== null) {
        $friendlyLabel = $friendlyBucket['top_name'] . ($friendlyBucket['count'] > 1 ? ' +' . ($friendlyBucket['count'] - 1) : '');
    }

    $hostileLabel = 'Unclassified Hostiles';
    if ($hostileBucket !== null) {
        $otherCount = $hostileBucket['count'] - 1;
        $hostileLabel = $hostileBucket['top_name'] . ($otherCount > 0 ? ' +' . $otherCount : '');
    }

    $durationSec = max(1, (int) ($t['duration_seconds'] ?? 0));
    $durationLabel = $durationSec >= 3600
        ? number_format($durationSec / 3600, 1) . 'h'
        : ($durationSec >= 120 ? number_format($durationSec / 60, 0) . 'm' : $durationSec . 's');

    $rows[] = [
        'theater_id'          => $tid,
        'primary_system_name' => (string) ($t['primary_system_name'] ?? ''),
        'region_name'         => (string) ($t['region_name'] ?? ''),
        'battle_count'        => (int) ($t['battle_count'] ?? 0),
        'system_count'        => (int) ($t['system_count'] ?? 0),
        'participant_count'   => (int) ($t['participant_count'] ?? 0),
        'total_kills'         => (int) ($t['total_kills'] ?? 0),
        'total_isk'           => (float) ($t['total_isk'] ?? 0),
        'duration_seconds'    => $durationSec,
        'duration_label'      => $durationLabel,
        'start_time'          => (string) ($t['start_time'] ?? ''),
        'end_time'            => (string) ($t['end_time'] ?? ''),
        'locked_at'           => (string) ($t['locked_at'] ?? ''),
        'ai_headline'         => (string) ($t['ai_headline'] ?? ''),
        'ai_verdict'          => (string) ($t['ai_verdict'] ?? ''),
        'friendly_label'      => $friendlyLabel,
        'hostile_label'       => $hostileLabel,
    ];
}

public_api_respond([
    'theaters' => $rows,
    'page'     => $page,
    'per_page' => $perPage,
], 60);
