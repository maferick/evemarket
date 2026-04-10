<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/functions.php';

function deal_alert_baseline_assert(bool $condition, string $message): void
{
    if (!$condition) {
        fwrite(STDERR, "FAIL: {$message}\n");
        exit(1);
    }
}

function deal_alert_baseline_assert_near(float $expected, float $actual, float $tolerance, string $message): void
{
    deal_alert_baseline_assert(
        abs($expected - $actual) <= $tolerance,
        sprintf('%s (expected ~%.4f, got %.4f)', $message, $expected, $actual)
    );
}

// Regression: Plagioclase III-Grade trades at ~38 ISK but one history row
// carries a trap / manipulation price of ~800k ISK. The legacy blend pushed
// normal_price to ~800_000 ISK and fired a CRITICAL MISPRICE alert for every
// legitimate listing. With outlier rejection the baseline must stay anchored
// to the typical price.
$rows = [];
for ($i = 0; $i < 13; $i++) {
    $rows[] = [
        'price' => 38.0 + ($i * 0.1),
        'weight' => 1_000_000,
        'origin' => 'market_hub_local_history_daily',
    ];
}
$rows[] = [
    'price' => 799_984.25,
    'weight' => 1001,
    'origin' => 'market_hub_local_history_daily',
];

$baseline = deal_alert_baseline_from_rows($rows);
deal_alert_baseline_assert($baseline !== null, 'Plagioclase: baseline must be computed');
deal_alert_baseline_assert_near(38.6, (float) $baseline['normal_price'], 1.0, 'Plagioclase: normal_price must track the cluster, not the outlier');
deal_alert_baseline_assert((int) $baseline['points'] === 13, 'Plagioclase: outlier row must be rejected');

// Two-sided outliers: both a 0.01 ISK misprint and an 800M ISK trap order in
// history should be ignored.
$rows = [];
for ($i = 0; $i < 12; $i++) {
    $rows[] = [
        'price' => 40.0 + ($i * 0.5),
        'weight' => 1000,
        'origin' => 'market_hub_local_history_daily',
    ];
}
$rows[] = ['price' => 0.01, 'weight' => 1, 'origin' => 'market_hub_local_history_daily'];
$rows[] = ['price' => 800_000_000.0, 'weight' => 1, 'origin' => 'market_hub_local_history_daily'];
$baseline = deal_alert_baseline_from_rows($rows);
deal_alert_baseline_assert($baseline !== null, 'Two-sided: baseline must be computed');
deal_alert_baseline_assert_near(42.75, (float) $baseline['normal_price'], 1.0, 'Two-sided: both outliers must be rejected');
deal_alert_baseline_assert((int) $baseline['points'] === 12, 'Two-sided: both outliers must drop from points');

// Legitimate 2x spike within the outlier band should still influence the blend.
$rows = [];
for ($i = 0; $i < 10; $i++) {
    $rows[] = ['price' => 100.0, 'weight' => 1000, 'origin' => 'market_history_daily'];
}
$rows[] = ['price' => 200.0, 'weight' => 3000, 'origin' => 'market_history_daily'];
$baseline = deal_alert_baseline_from_rows($rows);
deal_alert_baseline_assert($baseline !== null, 'Legit spike: baseline must be computed');
deal_alert_baseline_assert((int) $baseline['points'] === 11, 'Legit spike: no rows rejected');
deal_alert_baseline_assert((float) $baseline['normal_price'] > 100.0, 'Legit spike: blend must react to the upward shift');
deal_alert_baseline_assert((float) $baseline['normal_price'] < 150.0, 'Legit spike: blend must stay median-anchored');

// Stable market: baseline unchanged vs. the pre-fix behaviour.
$rows = [];
for ($i = 0; $i < 10; $i++) {
    $rows[] = ['price' => 1000.0 + ($i * 5), 'weight' => 2000, 'origin' => 'market_history_daily'];
}
$baseline = deal_alert_baseline_from_rows($rows);
deal_alert_baseline_assert($baseline !== null, 'Stable: baseline must be computed');
deal_alert_baseline_assert_near(1022.5, (float) $baseline['normal_price'], 0.5, 'Stable: blend unchanged on clean inputs');
deal_alert_baseline_assert((int) $baseline['points'] === 10, 'Stable: all rows retained');

// Degenerate input still returns null.
deal_alert_baseline_assert(deal_alert_baseline_from_rows([]) === null, 'Empty: returns null');
deal_alert_baseline_assert(
    deal_alert_baseline_from_rows([
        ['price' => 0.0, 'weight' => 1, 'origin' => 'market_history_daily'],
    ]) === null,
    'Zero-price: returns null'
);

// Small sample (3 points) from an alliance structure with close prices.
$rows = [
    ['price' => 1500.0, 'weight' => 100, 'origin' => 'market_history_daily'],
    ['price' => 1505.0, 'weight' => 100, 'origin' => 'market_history_daily'],
    ['price' => 1495.0, 'weight' => 100, 'origin' => 'market_history_daily'],
];
$baseline = deal_alert_baseline_from_rows($rows);
deal_alert_baseline_assert($baseline !== null, 'Small sample: baseline must be computed');
deal_alert_baseline_assert((int) $baseline['points'] === 3, 'Small sample: all points retained');
deal_alert_baseline_assert_near(1500.0, (float) $baseline['normal_price'], 5.0, 'Small sample: baseline stays close to median');

fwrite(STDOUT, "deal_alert_baseline_from_rows: all assertions passed\n");
