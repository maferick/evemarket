<?php

declare(strict_types=1);

require_once __DIR__ . '/../src/bootstrap.php';

function ai_briefing_debug_usage(): string
{
    return 'usage: php bin/ai_briefing_debug.php [--entity-type=fit|group --entity-id=<id>] [--limit=<n>] [--store]';
}

function ai_briefing_debug_main(array $argv): int
{
    $options = getopt('', ['entity-type::', 'entity-id::', 'limit::', 'store']);
    if ($options === false) {
        fwrite(STDERR, ai_briefing_debug_usage() . PHP_EOL);

        return 1;
    }

    $entityType = isset($options['entity-type']) ? trim((string) $options['entity-type']) : null;
    $entityId = isset($options['entity-id']) ? max(0, (int) $options['entity-id']) : null;
    $limit = isset($options['limit']) ? max(1, min(20, (int) $options['limit'])) : 5;
    $shouldStore = array_key_exists('store', $options);

    if (($entityType === null) !== ($entityId === null)) {
        fwrite(STDERR, '--entity-type and --entity-id must be supplied together.' . PHP_EOL);
        fwrite(STDERR, ai_briefing_debug_usage() . PHP_EOL);

        return 1;
    }

    if ($entityType !== null && !in_array($entityType, ['fit', 'group'], true)) {
        fwrite(STDERR, '--entity-type must be fit or group.' . PHP_EOL);

        return 1;
    }

    try {
        $preview = doctrine_ai_debug_preview($entityType, $entityId, $limit);
        $output = [
            'requested_entity' => $entityType !== null && $entityId !== null
                ? ['entity_type' => $entityType, 'entity_id' => $entityId]
                : null,
            'selected_candidate' => $preview['selected_candidate'] ?? null,
            'config' => $preview['config'] ?? [],
            'candidate_list' => $preview['candidate_list'] ?? [],
            'source_payload' => $preview['source_payload'] ?? [],
            'generation' => $preview['generation'] ?? [],
        ];

        if ($shouldStore) {
            $sourcePayload = is_array($preview['source_payload'] ?? null) ? $preview['source_payload'] : [];
            $generation = is_array($preview['generation'] ?? null) ? $preview['generation'] : [];
            $briefing = is_array($generation['briefing'] ?? null) ? $generation['briefing'] : [];
            $status = (string) ($generation['status'] ?? 'fallback');
            $errorMessage = ($generation['error_message'] ?? null) !== null ? (string) $generation['error_message'] : null;
            $stored = doctrine_ai_store_briefing($sourcePayload, $briefing, $status, $errorMessage);
            $output['stored'] = $stored;
        }

        fwrite(STDOUT, json_encode($output, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) . PHP_EOL);

        return 0;
    } catch (Throwable $exception) {
        fwrite(STDERR, json_encode([
            'error' => $exception->getMessage(),
        ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_INVALID_UTF8_SUBSTITUTE) . PHP_EOL);

        return 1;
    }
}

exit(ai_briefing_debug_main($argv));
