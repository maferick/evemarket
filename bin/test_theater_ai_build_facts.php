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

function build_snapshot(array $allianceSummary, array $extra = []): array
{
    return array_merge([
        'alliance_summary' => $allianceSummary,
        'turning_points' => [],
        'systems' => [['system_name' => 'MJ-5F9']],
        'suspicion' => null,
        'fleet_comp' => [],
        'notable_kills' => [],
        'top_performers' => [],
    ], $extra);
}

// Case 1: Friendly alliance on "friendly" side, enemy on "opponent" side.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-1',
    theater_fixture_base(),
    build_snapshot([
        ['side' => 'friendly', 'alliance_id' => 1001, 'alliance_name' => 'Fraternity.', 'participant_count' => 3, 'total_kills' => 5, 'total_losses' => 2, 'total_isk_killed' => 500, 'total_isk_lost' => 250, 'efficiency' => 0.66],
        ['side' => 'opponent', 'alliance_id' => 2002, 'alliance_name' => 'Goonswarm Federation', 'participant_count' => 9, 'total_kills' => 2, 'total_losses' => 5, 'total_isk_killed' => 250, 'total_isk_lost' => 500, 'efficiency' => 0.33],
    ])
);
theater_guardrail_assert(($facts['friendly_coalition'][0]['alliance'] ?? '') === 'Fraternity.', 'Friendly alliance must be in friendly_coalition.');
theater_guardrail_assert(($facts['enemy_coalition'][0]['alliance'] ?? '') === 'Goonswarm Federation', 'Opponent alliance must be in enemy_coalition.');
theater_guardrail_assert($facts['friendly_pilots'] === 3, 'Friendly pilots must be 3.');
theater_guardrail_assert($facts['enemy_pilots'] === 9, 'Enemy pilots must be 9.');
theater_guardrail_assert($facts['friendly_isk_killed'] === '500 ISK', 'Friendly ISK killed must be 500.');
theater_guardrail_assert($facts['friendly_isk_lost'] === '250 ISK', 'Friendly ISK lost must be 250.');
theater_guardrail_assert($facts['efficiency'] === '66.7%', 'Efficiency must be 66.7%.');

// Case 2: Third-party alliances are excluded from both sides.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-2',
    theater_fixture_base(),
    build_snapshot([
        ['side' => 'friendly', 'alliance_id' => 1001, 'alliance_name' => 'Our Alliance', 'participant_count' => 5, 'total_kills' => 8, 'total_losses' => 1, 'total_isk_killed' => 800, 'total_isk_lost' => 100, 'efficiency' => 0.89],
        ['side' => 'opponent', 'alliance_id' => 2002, 'alliance_name' => 'Enemy Alliance', 'participant_count' => 4, 'total_kills' => 1, 'total_losses' => 8, 'total_isk_killed' => 100, 'total_isk_lost' => 800, 'efficiency' => 0.11],
        ['side' => 'third_party', 'alliance_id' => 3003, 'alliance_name' => 'Neutral Corp', 'participant_count' => 2, 'total_kills' => 1, 'total_losses' => 0, 'total_isk_killed' => 50, 'total_isk_lost' => 0, 'efficiency' => 1.0],
    ])
);
theater_guardrail_assert(count($facts['friendly_coalition']) === 1, 'Only friendly alliance should be in friendly_coalition.');
theater_guardrail_assert(count($facts['enemy_coalition']) === 1, 'Only opponent alliance should be in enemy_coalition.');
theater_guardrail_assert($facts['friendly_pilots'] === 5, 'Third-party pilots must not be counted.');

// Case 3: Fleet composition sides match correctly.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-3',
    theater_fixture_base(),
    build_snapshot(
        [
            ['side' => 'friendly', 'alliance_id' => 1001, 'alliance_name' => 'Blue Team', 'participant_count' => 4, 'total_kills' => 6, 'total_losses' => 2, 'total_isk_killed' => 600, 'total_isk_lost' => 200, 'efficiency' => 0.75],
            ['side' => 'opponent', 'alliance_id' => 2222, 'alliance_name' => 'Red Team', 'participant_count' => 4, 'total_kills' => 2, 'total_losses' => 6, 'total_isk_killed' => 200, 'total_isk_lost' => 600, 'efficiency' => 0.25],
        ],
        [
            'fleet_comp' => [
                ['ship_name' => 'Maelstrom', 'ship_group' => 'Battleship', 'pilot_count' => 20, 'side' => 'friendly'],
                ['ship_name' => 'Raven', 'ship_group' => 'Battleship', 'pilot_count' => 15, 'side' => 'opponent'],
            ],
        ]
    )
);
theater_guardrail_assert(count($facts['friendly_fleet_composition']) === 1, 'Friendly fleet must have 1 entry.');
theater_guardrail_assert(($facts['friendly_fleet_composition'][0]['ship'] ?? '') === 'Maelstrom', 'Friendly fleet must contain Maelstrom.');
theater_guardrail_assert(count($facts['enemy_fleet_composition']) === 1, 'Enemy fleet must have 1 entry.');
theater_guardrail_assert(($facts['enemy_fleet_composition'][0]['ship'] ?? '') === 'Raven', 'Enemy fleet must contain Raven.');

// Case 4: Notable kills lost_by correctly assigned.
$facts = theater_ai_build_facts_from_snapshot(
    'fixture-4',
    theater_fixture_base(),
    build_snapshot(
        [
            ['side' => 'friendly', 'alliance_id' => 1001, 'alliance_name' => 'Us', 'participant_count' => 5, 'total_kills' => 3, 'total_losses' => 1, 'total_isk_killed' => 300, 'total_isk_lost' => 100, 'efficiency' => 0.75],
        ],
        [
            'notable_kills' => [
                ['victim_name' => 'EnemyPilot', 'ship_name' => 'Titan', 'ship_group' => 'Titan', 'victim_alliance_name' => 'Them', 'isk_value' => 100000000000, 'kill_time' => '2026-03-27 00:10:00', 'victim_side' => 'opponent'],
                ['victim_name' => 'FriendlyPilot', 'ship_name' => 'Dread', 'ship_group' => 'Dreadnought', 'victim_alliance_name' => 'Us', 'isk_value' => 5000000000, 'kill_time' => '2026-03-27 00:12:00', 'victim_side' => 'friendly'],
            ],
        ]
    )
);
theater_guardrail_assert(($facts['notable_kills'][0]['lost_by'] ?? '') === 'enemy', 'Opponent victim must be lost_by=enemy.');
theater_guardrail_assert(($facts['notable_kills'][1]['lost_by'] ?? '') === 'friendly', 'Friendly victim must be lost_by=friendly.');

fwrite(STDOUT, "Theater AI side resolution guardrails passed.\n");
