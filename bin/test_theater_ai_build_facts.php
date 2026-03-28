<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

function theater_guardrail_assert(bool $condition, string $message): void
{
    if (!$condition) {
        fwrite(STDERR, "FAIL: {$message}\n");
        exit(1);
    }
}

function theater_fixture_base(): array
{
    return [
        'theater_id' => 'fixture-theater',
        'primary_system_name' => 'MJ-5F9',
        'region_name' => 'Delve',
        'start_time' => '2026-03-27 00:00:00',
        'end_time' => '2026-03-27 00:15:00',
        'duration_seconds' => 900,
        'battle_count' => 1,
        'total_kills' => 10,
        'participant_count' => 10,
        'anomaly_score' => 0.321,
    ];
}

function build_snapshot(array $allianceSummary, array $participants, array $trackedAlliances, array $trackedCorporations, array $opponentAlliances, array $opponentCorporations): array
{
    return [
        'alliance_summary' => $allianceSummary,
        'participants' => $participants,
        'turning_points' => [],
        'systems' => [['system_name' => 'MJ-5F9']],
        'suspicion' => null,
        'fleet_comp' => [],
        'notable_kills' => [],
        'top_performers' => [],
        'tracked_alliances' => $trackedAlliances,
        'tracked_corporations' => $trackedCorporations,
        'opponent_alliances' => $opponentAlliances,
        'opponent_corporations' => $opponentCorporations,
    ];
}

// Case 1: Enemy has more pilots, but configured friendly side must still be left/friendly.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-1',
    theater_fixture_base(),
    build_snapshot(
        [
            ['side' => 'side_a', 'alliance_id' => 1001, 'alliance_name' => 'Friendly Alliance', 'participant_count' => 3, 'total_kills' => 5, 'total_losses' => 2, 'total_isk_killed' => 500, 'total_isk_lost' => 250, 'efficiency' => 0.66],
            ['side' => 'side_b', 'alliance_id' => 2002, 'alliance_name' => 'Enemy Blob', 'participant_count' => 9, 'total_kills' => 2, 'total_losses' => 5, 'total_isk_killed' => 250, 'total_isk_lost' => 500, 'efficiency' => 0.33],
        ],
        [
            ['side' => 'side_a', 'alliance_id' => 1001, 'corporation_id' => 0],
            ['side' => 'side_b', 'alliance_id' => 2002, 'corporation_id' => 0],
            ['side' => 'side_b', 'alliance_id' => 2002, 'corporation_id' => 0],
            ['side' => 'side_b', 'alliance_id' => 2002, 'corporation_id' => 0],
        ],
        [['alliance_id' => 1001, 'label' => 'Friendly Alliance']],
        [],
        [],
        []
    )
);
theater_guardrail_assert(($facts['friendly_coalition'][0]['alliance'] ?? '') === 'Friendly Alliance', 'Friendly side must stay side_a/left even when side_b has more pilots.');

// Case 2 (regression): settings-driven tracked entities configured via corporations only.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-2',
    theater_fixture_base(),
    build_snapshot(
        [
            ['side' => 'side_a', 'alliance_id' => 0, 'alliance_name' => 'Unknown Alliance', 'participant_count' => 2, 'total_kills' => 4, 'total_losses' => 1, 'total_isk_killed' => 400, 'total_isk_lost' => 100, 'efficiency' => 0.8],
            ['side' => 'side_b', 'alliance_id' => 3003, 'alliance_name' => 'Large Hostiles', 'participant_count' => 8, 'total_kills' => 1, 'total_losses' => 4, 'total_isk_killed' => 100, 'total_isk_lost' => 400, 'efficiency' => 0.2],
        ],
        [
            ['side' => 'side_a', 'alliance_id' => 0, 'corporation_id' => 4242],
            ['side' => 'side_a', 'alliance_id' => 0, 'corporation_id' => 4242],
            ['side' => 'side_b', 'alliance_id' => 3003, 'corporation_id' => 0],
            ['side' => 'side_b', 'alliance_id' => 3003, 'corporation_id' => 0],
            ['side' => 'side_b', 'alliance_id' => 3003, 'corporation_id' => 0],
        ],
        [],
        [['corporation_id' => 4242, 'label' => 'Tracked Corp']],
        [],
        []
    )
);
theater_guardrail_assert(($facts['friendly_coalition'][0]['alliance'] ?? '') === 'Unknown Alliance', 'Corporation-only tracked entities from settings must still anchor the friendly side.');

// Case 3: Both configured friendlies and opponents are present and must map left/right.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-3',
    theater_fixture_base(),
    build_snapshot(
        [
            ['side' => 'side_a', 'alliance_id' => 1111, 'alliance_name' => 'Blue Team', 'participant_count' => 4, 'total_kills' => 6, 'total_losses' => 2, 'total_isk_killed' => 600, 'total_isk_lost' => 200, 'efficiency' => 0.75],
            ['side' => 'side_b', 'alliance_id' => 2222, 'alliance_name' => 'Red Team', 'participant_count' => 4, 'total_kills' => 2, 'total_losses' => 6, 'total_isk_killed' => 200, 'total_isk_lost' => 600, 'efficiency' => 0.25],
        ],
        [
            ['side' => 'side_a', 'alliance_id' => 1111, 'corporation_id' => 7771],
            ['side' => 'side_b', 'alliance_id' => 2222, 'corporation_id' => 8882],
        ],
        [['alliance_id' => 1111, 'label' => 'Blue Team']],
        [],
        [['alliance_id' => 2222, 'label' => 'Red Team']],
        []
    )
);
theater_guardrail_assert(($facts['friendly_coalition'][0]['alliance'] ?? '') === 'Blue Team', 'Configured friendly alliance must always resolve to friendly/left.');
theater_guardrail_assert(($facts['enemy_coalition'][0]['alliance'] ?? '') === 'Red Team', 'Configured opponent alliance must always resolve to enemy/right.');

// Case 4: Fallback occurs only when no configured entities match either side.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-4',
    theater_fixture_base(),
    build_snapshot(
        [
            ['side' => 'side_a', 'alliance_id' => 7001, 'alliance_name' => 'Small Group', 'participant_count' => 2, 'total_kills' => 1, 'total_losses' => 3, 'total_isk_killed' => 100, 'total_isk_lost' => 300, 'efficiency' => 0.25],
            ['side' => 'side_b', 'alliance_id' => 7002, 'alliance_name' => 'Large Group', 'participant_count' => 7, 'total_kills' => 3, 'total_losses' => 1, 'total_isk_killed' => 300, 'total_isk_lost' => 100, 'efficiency' => 0.75],
        ],
        [
            ['side' => 'side_a', 'alliance_id' => 7001, 'corporation_id' => 70011],
            ['side' => 'side_b', 'alliance_id' => 7002, 'corporation_id' => 70021],
            ['side' => 'side_b', 'alliance_id' => 7002, 'corporation_id' => 70022],
        ],
        [['alliance_id' => 9991, 'label' => 'Not Present Friendly']],
        [['corporation_id' => 9992, 'label' => 'Not Present Friendly Corp']],
        [['alliance_id' => 9993, 'label' => 'Not Present Opponent']],
        [['corporation_id' => 9994, 'label' => 'Not Present Opponent Corp']]
    )
);
theater_guardrail_assert(($facts['friendly_coalition'][0]['alliance'] ?? '') === 'Small Group', 'Fallback should only occur when no configured entities match; side_a remains friendly by default.');

fwrite(STDOUT, "Theater AI side resolution guardrails passed.\n");
