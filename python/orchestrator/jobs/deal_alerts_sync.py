from __future__ import annotations

from ..db import SupplyCoreDb
from .sync_runtime import run_sync_phase_job


def _processor(db: SupplyCoreDb) -> dict[str, object]:
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
    return {
        "rows_processed": rows_processed,
        "rows_written": rows_written,
        "warnings": [] if rows_written > 0 else ["No alert candidates met the configured anomaly threshold (<=95% of baseline)."],
        "summary": f"Materialized {rows_written} active deal alerts.",
        "meta": {"threshold_percent_of_normal_max": 0.95},
    }


def run_deal_alerts_sync(db: SupplyCoreDb) -> dict[str, object]:
    return run_sync_phase_job(db, job_key="deal_alerts_sync", phase="C", objective="deal alerts", processor=_processor)
