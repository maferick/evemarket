from __future__ import annotations

import time
import uuid
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe

# Default intelligence pipeline weights (from intelligence_pipeline.py score assembly)
DEFAULT_WEIGHTS = {
    "selective_non_engagement": 0.35,
    "peer_norm_kills": -0.20,
    "peer_norm_damage": -0.20,
    "high_presence_low_output": 0.15,
    "token_participation": 0.10,
    "loss_without_attack": 0.10,
}

# Thresholds for triggering recalibration
MIN_LABELS_FOR_RECALIBRATION = 10
PRECISION_ALERT_THRESHOLD = 0.70
WEIGHT_ADJUSTMENT_STEP = 0.05
MAX_WEIGHT_MAGNITUDE = 0.50


def run_graph_analyst_recalibration(db: SupplyCoreDb) -> dict[str, Any]:
    started = time.perf_counter()
    job_name = "graph_analyst_recalibration"
    run_id = uuid.uuid4().hex[:16]

    # Read all analyst feedback
    feedback_rows = db.fetch_all(
        """SELECT af.character_id, af.label, af.confidence,
                  COALESCE(css.suspicion_score, 0) AS current_suspicion_score
           FROM analyst_feedback af
           LEFT JOIN character_suspicion_signals css ON css.character_id = af.character_id
           ORDER BY af.created_at DESC"""
    )

    total_labels = len(feedback_rows or [])

    if total_labels < MIN_LABELS_FOR_RECALIBRATION:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return JobResult.success(
            job_key=job_name,
            summary=f"Insufficient labels for recalibration ({total_labels}/{MIN_LABELS_FOR_RECALIBRATION}).",
            rows_processed=total_labels,
            rows_written=0,
            rows_seen=total_labels,
            duration_ms=duration_ms,
            meta={"total_labels": total_labels, "min_required": MIN_LABELS_FOR_RECALIBRATION},
        ).to_dict()

    # Classify feedback against current scores
    true_positives = 0
    false_positives = 0
    true_negatives = 0
    false_negatives = 0

    for row in feedback_rows:
        label = str(row.get("label") or "")
        score = float(row.get("current_suspicion_score") or 0)
        confidence = float(row.get("confidence") or 0.5)

        system_flagged = score > 0.5

        if label == "true_positive":
            if system_flagged:
                true_positives += 1
            else:
                false_negatives += 1
        elif label == "false_positive":
            if system_flagged:
                false_positives += 1
            else:
                true_negatives += 1
        elif label == "confirmed_clean":
            if system_flagged:
                false_positives += 1
            else:
                true_negatives += 1
        # 'needs_review' labels don't count toward precision/recall

    # Compute precision and recall
    precision = true_positives / max(1, true_positives + false_positives)
    recall = true_positives / max(1, true_positives + false_negatives)

    # Read current weights (from latest recalibration or defaults)
    latest_log = db.fetch_one(
        "SELECT weight_adjustments FROM analyst_recalibration_log ORDER BY computed_at DESC LIMIT 1"
    )
    current_weights = dict(DEFAULT_WEIGHTS)
    if latest_log and latest_log.get("weight_adjustments"):
        import json
        try:
            saved = json.loads(str(latest_log["weight_adjustments"]))
            after = saved.get("after") or {}
            if after:
                current_weights.update(after)
        except (json.JSONDecodeError, TypeError):
            pass

    # Determine if recalibration is needed
    new_weights = dict(current_weights)
    adjustments_made = False

    if precision < PRECISION_ALERT_THRESHOLD and (true_positives + false_positives) >= 5:
        # Too many false positives: reduce weights that amplify suspicion
        for key in ["selective_non_engagement", "high_presence_low_output", "token_participation"]:
            old_val = new_weights.get(key, 0)
            if old_val > 0:
                new_val = max(0.05, old_val - WEIGHT_ADJUSTMENT_STEP)
                new_weights[key] = round(new_val, 4)
                adjustments_made = True

        # Increase negative weights (which reduce suspicion)
        for key in ["peer_norm_kills", "peer_norm_damage"]:
            old_val = new_weights.get(key, 0)
            if old_val < 0:
                new_val = max(-MAX_WEIGHT_MAGNITUDE, old_val - WEIGHT_ADJUSTMENT_STEP)
                new_weights[key] = round(new_val, 4)
                adjustments_made = True

    if recall < PRECISION_ALERT_THRESHOLD and (true_positives + false_negatives) >= 5:
        # Too many false negatives: increase sensitivity
        for key in ["selective_non_engagement", "high_presence_low_output", "loss_without_attack"]:
            old_val = new_weights.get(key, 0)
            if old_val > 0 and old_val < MAX_WEIGHT_MAGNITUDE:
                new_val = min(MAX_WEIGHT_MAGNITUDE, old_val + WEIGHT_ADJUSTMENT_STEP)
                new_weights[key] = round(new_val, 4)
                adjustments_made = True

    # Log the recalibration
    weight_adjustments = {
        "before": current_weights,
        "after": new_weights,
        "adjustments_made": adjustments_made,
    }
    threshold_changes = {
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "precision_threshold": PRECISION_ALERT_THRESHOLD,
    }

    db.execute(
        """INSERT INTO analyst_recalibration_log
           (run_id, total_labels, true_positives, false_positives,
            precision_score, recall_estimate, weight_adjustments,
            threshold_changes, computed_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP())""",
        (
            run_id,
            total_labels,
            true_positives,
            false_positives,
            round(precision, 4),
            round(recall, 4),
            json_dumps_safe(weight_adjustments),
            json_dumps_safe(threshold_changes),
        ),
    )

    db.upsert_intelligence_snapshot(
        snapshot_key="analyst_recalibration_state",
        payload_json=json_dumps_safe({
            "run_id": run_id,
            "total_labels": total_labels,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "adjustments_made": adjustments_made,
            "current_weights": new_weights,
            "confusion_matrix": {
                "true_positives": true_positives,
                "false_positives": false_positives,
                "true_negatives": true_negatives,
                "false_negatives": false_negatives,
            },
        }),
        metadata_json=json_dumps_safe({"source": "analyst_feedback", "reason": "scheduler:python", "run_id": run_id}),
        expires_seconds=86400,
    )

    duration_ms = int((time.perf_counter() - started) * 1000)
    action = "adjusted weights" if adjustments_made else "no adjustment needed"
    return JobResult.success(
        job_key=job_name,
        summary=f"Recalibration: {total_labels} labels, precision={precision:.2f}, recall={recall:.2f}, {action}.",
        rows_processed=total_labels,
        rows_written=1,
        rows_seen=total_labels,
        duration_ms=duration_ms,
        meta={
            "run_id": run_id,
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "adjustments_made": adjustments_made,
        },
    ).to_dict()
