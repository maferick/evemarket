from __future__ import annotations

import json
import time

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
    started_at = time.time()
    rows_processed = db.fetch_scalar("SELECT COUNT(*) FROM market_order_current_projection")
    rows_written = db.execute(
        """INSERT INTO market_deal_alerts_current (
                alert_key, item_type_id, source_type, source_id, source_name, percent_band,
                current_price, normal_price, percent_of_normal, anomaly_score, severity, severity_rank,
                quantity_available, listing_count, best_order_id, observed_at, detected_at, last_seen_at,
                freshness_seconds, status, metadata_json
            )
            SELECT
                CONCAT(p.source_type, ':', p.source_id, ':', p.type_id) AS alert_key,
                p.type_id,
                p.source_type,
                p.source_id,
                CAST(p.source_id AS CHAR(190)) AS source_name,
                ROUND((p.best_sell_price / NULLIF(d.weighted_price, 0)) * 100, 2) AS percent_band,
                p.best_sell_price AS current_price,
                d.weighted_price AS normal_price,
                p.best_sell_price / NULLIF(d.weighted_price, 0) AS percent_of_normal,
                ABS(100 - ((p.best_sell_price / NULLIF(d.weighted_price, 0)) * 100)) AS anomaly_score,
                CASE
                    WHEN p.best_sell_price / NULLIF(d.weighted_price, 0) <= 0.50 THEN 'critical'
                    WHEN p.best_sell_price / NULLIF(d.weighted_price, 0) <= 0.70 THEN 'very_strong'
                    WHEN p.best_sell_price / NULLIF(d.weighted_price, 0) <= 0.85 THEN 'strong'
                    ELSE 'watch'
                END AS severity,
                CASE
                    WHEN p.best_sell_price / NULLIF(d.weighted_price, 0) <= 0.50 THEN 4
                    WHEN p.best_sell_price / NULLIF(d.weighted_price, 0) <= 0.70 THEN 3
                    WHEN p.best_sell_price / NULLIF(d.weighted_price, 0) <= 0.85 THEN 2
                    ELSE 1
                END AS severity_rank,
                p.total_sell_volume,
                p.sell_order_count,
                NULL,
                p.observed_at,
                UTC_TIMESTAMP(),
                UTC_TIMESTAMP(),
                TIMESTAMPDIFF(SECOND, p.observed_at, UTC_TIMESTAMP()),
                'active',
                JSON_OBJECT('baseline_model', 'weighted_price_1d', 'bucket_start', d.bucket_start)
            FROM market_order_current_projection p
            INNER JOIN market_item_price_1d d
                ON d.source_type = p.source_type
                AND d.source_id = p.source_id
                AND d.type_id = p.type_id
                AND d.bucket_start = (SELECT MAX(bucket_start) FROM market_item_price_1d)
            WHERE p.best_sell_price IS NOT NULL
                AND d.weighted_price IS NOT NULL
                AND d.weighted_price > 0
                AND p.best_sell_price / d.weighted_price <= 0.95
            ON DUPLICATE KEY UPDATE
                current_price = VALUES(current_price), normal_price = VALUES(normal_price), percent_of_normal = VALUES(percent_of_normal),
                anomaly_score = VALUES(anomaly_score), severity = VALUES(severity), severity_rank = VALUES(severity_rank),
                quantity_available = VALUES(quantity_available), listing_count = VALUES(listing_count), observed_at = VALUES(observed_at),
                last_seen_at = VALUES(last_seen_at), freshness_seconds = VALUES(freshness_seconds), status='active', metadata_json = VALUES(metadata_json)"""
    )

    duration_ms = int((time.time() - started_at) * 1000)
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    attempt_status = "success_alerts" if rows_written > 0 else "success_empty"

    # Update the materialization status so the deal alerts page shows correct state
    db.execute(
        """INSERT INTO market_deal_alert_materialization_status (
                snapshot_key, last_job_key, last_run_started_at, last_run_finished_at,
                last_success_at, last_materialized_at, first_materialized_at,
                last_attempt_status, last_success_status,
                last_reason_zero_output, last_failure_reason,
                last_deferred_at, last_deferred_reason,
                input_row_count, history_row_count, candidate_row_count,
                output_row_count, persisted_row_count, inactive_row_count,
                sources_scanned, last_duration_ms, metadata_json
            ) VALUES (
                'current', 'deal_alerts_sync', %s, %s,
                %s, %s, %s,
                %s, %s,
                NULL, NULL,
                NULL, NULL,
                %s, 0, %s,
                %s, %s, 0,
                0, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                last_job_key = VALUES(last_job_key),
                last_run_started_at = VALUES(last_run_started_at),
                last_run_finished_at = VALUES(last_run_finished_at),
                last_success_at = COALESCE(VALUES(last_success_at), last_success_at),
                last_materialized_at = COALESCE(VALUES(last_materialized_at), last_materialized_at),
                first_materialized_at = COALESCE(first_materialized_at, VALUES(first_materialized_at)),
                last_attempt_status = VALUES(last_attempt_status),
                last_success_status = COALESCE(VALUES(last_success_status), last_success_status),
                last_reason_zero_output = VALUES(last_reason_zero_output),
                last_failure_reason = VALUES(last_failure_reason),
                input_row_count = VALUES(input_row_count),
                history_row_count = VALUES(history_row_count),
                candidate_row_count = VALUES(candidate_row_count),
                output_row_count = VALUES(output_row_count),
                persisted_row_count = VALUES(persisted_row_count),
                inactive_row_count = VALUES(inactive_row_count),
                sources_scanned = VALUES(sources_scanned),
                last_duration_ms = VALUES(last_duration_ms),
                metadata_json = VALUES(metadata_json)""",
        (
            now, now,  # started, finished
            now if rows_written > 0 else None,  # success_at
            now if rows_written > 0 else None,  # materialized_at
            now if rows_written > 0 else None,  # first_materialized_at
            attempt_status, attempt_status if rows_written > 0 else None,
            rows_processed,  # input_row_count
            rows_written,    # candidate_row_count
            rows_written,    # output_row_count
            rows_written,    # persisted_row_count
            duration_ms,
            json.dumps({"baseline_model": "weighted_price_1d", "execution_mode": "python"}),
        ),
    )

    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [] if rows_written > 0 else ["No alert candidates met the configured anomaly threshold (<=95% of baseline)."],
        "summary": f"Materialized {rows_written} active deal alerts.",
        "meta": {"threshold_percent_of_normal_max": 0.95, "duration_ms": duration_ms},
    }


def run_deal_alerts_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="deal_alerts_sync", phase="C", objective="deal alerts", processor=_processor)
