"""
Spy detection Phase 6 — Shadow ML trainer.

Trains a logistic regression baseline on versioned feature snapshots joined to
analyst-labeled training splits.  Persists the model artifact via the
``ArtifactStore`` abstraction and writes evaluation metrics.

v1: logistic regression with L2 regularisation.
v2 follow-up: XGBoost with SHAP attribution.
GNN: deferred to v2 (requires torch-geometric, serving infra).

Manual trigger only (``enabled_by_default=false``).
"""
from __future__ import annotations

import hashlib
import json
import logging
import pickle
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..model_store import get_artifact_store

logger = logging.getLogger(__name__)

MODEL_FAMILY = "logreg"
FEATURE_SET = "spy_v1"
DEFAULT_TOP_K = 100


def run_train_spy_shadow_model(
    db: SupplyCoreDb,
    neo4j_raw: dict | None = None,
    runtime: dict | None = None,
    *,
    payload: dict | None = None,
) -> dict[str, Any]:
    run_id = f"shadow_train_{uuid.uuid4().hex[:12]}"
    started = datetime.now(timezone.utc)

    # ── Find the latest training split ────────────────────────────────────
    split = db.fetch_one(
        """SELECT split_id, feature_set, feature_version, positive_count, negative_count
           FROM model_training_splits
           WHERE feature_set = %s
           ORDER BY created_at DESC LIMIT 1""",
        (FEATURE_SET,),
    )
    if not split:
        return JobResult.skipped(
            job_key="train_spy_shadow_model",
            reason="No training split found for feature_set=spy_v1",
        ).to_dict()

    split_id = split["split_id"]
    feature_version = split["feature_version"]

    # ── Load training data ────────────────────────────────────────────────
    members = db.fetch_all(
        """SELECT m.character_id, m.split_role, m.label, m.label_confidence,
                  s.feature_vector_json, s.feature_vector_hash
           FROM model_training_split_members m
           JOIN character_feature_snapshots s
             ON s.character_id = m.character_id
            AND s.feature_set = %s
            AND s.feature_version = %s
           WHERE m.split_id = %s
             AND m.label IN ('positive','negative')
           ORDER BY s.computed_at DESC""",
        (FEATURE_SET, feature_version, split_id),
    )

    if not members:
        return JobResult.skipped(
            job_key="train_spy_shadow_model",
            reason=f"No labeled members with features for split {split_id}",
        ).to_dict()

    # Deduplicate: keep latest snapshot per character
    seen: dict[int, dict] = {}
    for m in members:
        cid = m["character_id"]
        if cid not in seen:
            seen[cid] = m
    members = list(seen.values())

    # Parse feature vectors
    feature_names: list[str] | None = None
    X_rows: list[list[float]] = []
    y_labels: list[int] = []
    hashes: list[str] = []

    for m in members:
        fv = json.loads(m["feature_vector_json"]) if isinstance(m["feature_vector_json"], str) else m["feature_vector_json"]
        if feature_names is None:
            feature_names = sorted(fv.keys())
        row = [float(fv.get(k, 0.0)) for k in feature_names]
        X_rows.append(row)
        y_labels.append(1 if m["label"] == "positive" else 0)
        hashes.append(m["feature_vector_hash"])

    if not feature_names or len(X_rows) < 10:
        return JobResult.skipped(
            job_key="train_spy_shadow_model",
            reason=f"Too few training samples ({len(X_rows)}), need >= 10",
        ).to_dict()

    # ── Train logistic regression ─────────────────────────────────────────
    try:
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
        from sklearn.metrics import (
            roc_auc_score,
            average_precision_score,
            brier_score_loss,
            precision_score,
            recall_score,
        )
    except ImportError:
        return JobResult.failed(
            job_key="train_spy_shadow_model",
            error_text="scikit-learn not installed — cannot train shadow model",
        ).to_dict()

    import numpy as np

    X = np.array(X_rows, dtype=np.float64)
    y = np.array(y_labels, dtype=np.int32)

    # Split into train/validation by split_role
    train_mask = np.array([m["split_role"] == "train" for m in members])
    val_mask = np.array([m["split_role"] in ("validation", "test") for m in members])

    if train_mask.sum() < 5:
        # Fallback: use all data for training if split is too small
        train_mask = np.ones(len(members), dtype=bool)
        val_mask = train_mask.copy()

    scaler = StandardScaler()
    X_train = scaler.fit_transform(X[train_mask])
    y_train = y[train_mask]

    model = LogisticRegression(
        C=1.0,
        penalty="l2",
        solver="lbfgs",
        max_iter=1000,
        class_weight="balanced",
        random_state=42,
    )
    model.fit(X_train, y_train)

    # ── Evaluate ──────────────────────────────────────────────────────────
    X_val = scaler.transform(X[val_mask])
    y_val = y[val_mask]
    y_proba = model.predict_proba(X_val)[:, 1] if model.classes_[1] == 1 else model.predict_proba(X_val)[:, 0]

    metrics: dict[str, Any] = {}
    try:
        metrics["auc_roc"] = float(roc_auc_score(y_val, y_proba))
    except ValueError:
        metrics["auc_roc"] = None
    try:
        metrics["pr_auc"] = float(average_precision_score(y_val, y_proba))
    except ValueError:
        metrics["pr_auc"] = None
    metrics["brier_score"] = float(brier_score_loss(y_val, y_proba))

    # precision/recall at top-K
    top_k = min(DEFAULT_TOP_K, len(y_val))
    if top_k > 0:
        top_idx = np.argsort(y_proba)[::-1][:top_k]
        y_pred_topk = np.zeros_like(y_val)
        y_pred_topk[top_idx] = 1
        metrics["precision_at_k"] = float(precision_score(y_val, y_pred_topk, zero_division=0))
        metrics["recall_at_k"] = float(recall_score(y_val, y_pred_topk, zero_division=0))
    else:
        metrics["precision_at_k"] = None
        metrics["recall_at_k"] = None

    # Calibration (bin-based ECE)
    n_bins = 10
    bin_boundaries = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    cal_bins = []
    for i in range(n_bins):
        in_bin = (y_proba >= bin_boundaries[i]) & (y_proba < bin_boundaries[i + 1])
        count = int(in_bin.sum())
        if count > 0:
            avg_conf = float(y_proba[in_bin].mean())
            avg_acc = float(y_val[in_bin].mean())
            ece += abs(avg_conf - avg_acc) * count / len(y_val)
            cal_bins.append({"bin": i, "count": count, "avg_confidence": round(avg_conf, 4), "avg_accuracy": round(avg_acc, 4)})
    metrics["expected_calibration_error"] = round(ece, 6)

    # ── Persist model artifact ────────────────────────────────────────────
    model_version = f"1.0.{int(time.time())}"
    artifact_data = pickle.dumps({"scaler": scaler, "model": model, "feature_names": feature_names})

    store = get_artifact_store(runtime)
    artifact_key = f"{MODEL_FAMILY}/{model_version}.pkl"
    handle = store.put(artifact_key, artifact_data)

    # ── Write registry ────────────────────────────────────────────────────
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    db.execute(
        """INSERT INTO spy_model_registry
           (model_family, model_version, status, artifact_uri, artifact_sha256,
            artifact_size_bytes, feature_set, feature_version, created_at, notes)
           VALUES (%s, %s, 'experimental', %s, %s, %s, %s, %s, %s, %s)
           ON DUPLICATE KEY UPDATE
             artifact_uri = VALUES(artifact_uri),
             artifact_sha256 = VALUES(artifact_sha256),
             artifact_size_bytes = VALUES(artifact_size_bytes),
             notes = VALUES(notes)""",
        (MODEL_FAMILY, model_version, handle.uri, handle.sha256,
         handle.size_bytes, FEATURE_SET, feature_version, now_str,
         f"Auto-trained on split {split_id}"),
    )

    # ── Write metrics ─────────────────────────────────────────────────────
    db.execute(
        """INSERT INTO spy_model_run_metrics
           (run_id, model_family, model_version, split_id,
            auc_roc, pr_auc, precision_at_k, recall_at_k,
            brier_score, expected_calibration_error, top_k,
            calibration_json, drift_json,
            training_sample_count, shadow_sample_count,
            started_at, completed_at, status, notes)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'success', %s)""",
        (run_id, MODEL_FAMILY, model_version, split_id,
         metrics.get("auc_roc"), metrics.get("pr_auc"),
         metrics.get("precision_at_k"), metrics.get("recall_at_k"),
         metrics.get("brier_score"), metrics.get("expected_calibration_error"),
         top_k,
         json_dumps_safe({"bins": cal_bins}),
         json_dumps_safe({}),  # drift_json — no prior to compare on first run
         int(train_mask.sum()), 0,
         started.strftime("%Y-%m-%d %H:%M:%S"), now_str,
         f"Trained {MODEL_FAMILY} v{model_version} on {int(train_mask.sum())} samples"),
    )

    summary = (
        f"Trained {MODEL_FAMILY} v{model_version}: "
        f"AUC={metrics.get('auc_roc', 'N/A')}, "
        f"Brier={metrics.get('brier_score', 'N/A')}, "
        f"train_n={int(train_mask.sum())}, val_n={int(val_mask.sum())}"
    )
    logger.info(summary)

    return JobResult.success(
        job_key="train_spy_shadow_model",
        summary=summary,
        rows_processed=len(members),
        rows_written=1,
        meta={
            "run_id": run_id,
            "model_family": MODEL_FAMILY,
            "model_version": model_version,
            "split_id": split_id,
            "metrics": metrics,
            "artifact_uri": handle.uri,
        },
    ).to_dict()
