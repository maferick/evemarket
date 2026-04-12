"""
Spy detection Phase 6 — Shadow ML scorer.

For each model in ``spy_model_registry`` with status in
(experimental, candidate, promoted), loads the artifact and scores current
feature snapshots.  Hash-based skip optimisation avoids rescoring unchanged
characters.

Never writes to Neo4j, no Bloom tier, no primary-score side effects.
"""
from __future__ import annotations

import json
import logging
import pickle
import uuid
from datetime import datetime, timezone
from typing import Any

from ..db import SupplyCoreDb
from ..job_result import JobResult
from ..json_utils import json_dumps_safe
from ..model_store import ArtifactIntegrityError, ArtifactNotFound, get_artifact_store

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
RECENCY_HOURS = 24


def run_score_spy_shadow_ml(
    db: SupplyCoreDb,
    neo4j_raw: dict | None = None,
    runtime: dict | None = None,
    *,
    payload: dict | None = None,
) -> dict[str, Any]:
    run_id = f"shadow_score_{uuid.uuid4().hex[:12]}"
    started = datetime.now(timezone.utc)

    # ── Load active models from registry ──────────────────────────────────
    models = db.fetch_all(
        """SELECT model_family, model_version, artifact_uri, artifact_sha256,
                  feature_set, feature_version
           FROM spy_model_registry
           WHERE status IN ('experimental','candidate','promoted')
           ORDER BY created_at DESC"""
    )
    if not models:
        return JobResult.skipped(
            job_key="score_spy_shadow_ml",
            reason="No active models in spy_model_registry",
        ).to_dict()

    store = get_artifact_store(runtime)
    total_scored = 0
    total_skipped = 0
    model_summaries: list[dict] = []

    for model_row in models:
        mf = model_row["model_family"]
        mv = model_row["model_version"]
        uri = model_row["artifact_uri"]
        expected_sha = model_row["artifact_sha256"]
        feature_set = model_row["feature_set"]
        feature_version = model_row["feature_version"]

        # ── Load artifact ─────────────────────────────────────────────
        try:
            data = store.get(uri)
        except ArtifactNotFound:
            logger.error("Artifact not found for %s/%s: %s", mf, mv, uri)
            model_summaries.append({"model": f"{mf}/{mv}", "status": "artifact_not_found"})
            continue

        # Verify integrity
        import hashlib
        actual_sha = hashlib.sha256(data).hexdigest()
        if expected_sha and actual_sha != expected_sha:
            logger.error(
                "Artifact integrity mismatch for %s/%s: expected=%s actual=%s",
                mf, mv, expected_sha, actual_sha,
            )
            model_summaries.append({"model": f"{mf}/{mv}", "status": "integrity_error"})
            continue

        try:
            bundle = pickle.loads(data)
            model_obj = bundle["model"]
            scaler = bundle["scaler"]
            feature_names = bundle["feature_names"]
        except Exception as e:
            logger.error("Failed to deserialise artifact for %s/%s: %s", mf, mv, e)
            model_summaries.append({"model": f"{mf}/{mv}", "status": "deserialise_error"})
            continue

        # ── Load existing shadow hashes for skip optimisation ─────────
        existing_hashes: dict[int, str] = {}
        rows = db.fetch_all(
            """SELECT character_id, feature_vector_hash
               FROM character_spy_shadow_scores
               WHERE model_family = %s AND model_version = %s
                 AND split_label = 'live_shadow'""",
            (mf, mv),
        )
        for r in rows:
            existing_hashes[r["character_id"]] = r["feature_vector_hash"]

        # ── Load recent feature snapshots ─────────────────────────────
        snapshots = db.fetch_all(
            """SELECT character_id, feature_vector_json, feature_vector_hash
               FROM character_feature_snapshots
               WHERE feature_set = %s AND feature_version = %s
                 AND computed_at > DATE_SUB(NOW(), INTERVAL %s HOUR)
               ORDER BY character_id""",
            (feature_set, feature_version, RECENCY_HOURS),
        )

        # Deduplicate — latest per character
        snap_map: dict[int, dict] = {}
        for s in snapshots:
            snap_map[s["character_id"]] = s
        snapshots = list(snap_map.values())

        # Filter out unchanged
        to_score = []
        skipped = 0
        for s in snapshots:
            if existing_hashes.get(s["character_id"]) == s["feature_vector_hash"]:
                skipped += 1
            else:
                to_score.append(s)

        total_skipped += skipped

        if not to_score:
            model_summaries.append({
                "model": f"{mf}/{mv}",
                "status": "all_skipped",
                "skipped": skipped,
            })
            continue

        # ── Score in batches ──────────────────────────────────────────
        import numpy as np

        all_scores: list[float] = []
        all_chars: list[int] = []
        all_hashes: list[str] = []
        all_attributions: list[dict] = []

        for i in range(0, len(to_score), BATCH_SIZE):
            batch = to_score[i : i + BATCH_SIZE]
            X_rows = []
            for s in batch:
                fv = json.loads(s["feature_vector_json"]) if isinstance(s["feature_vector_json"], str) else s["feature_vector_json"]
                row = [float(fv.get(k, 0.0)) for k in feature_names]
                X_rows.append(row)

            X = np.array(X_rows, dtype=np.float64)
            X_scaled = scaler.transform(X)

            # Predict probabilities
            proba = model_obj.predict_proba(X_scaled)
            # Get positive class index
            pos_idx = list(model_obj.classes_).index(1) if 1 in model_obj.classes_ else 0
            scores = proba[:, pos_idx]

            # Attribution: coefficient * feature value (logistic regression)
            coefs = model_obj.coef_[0] if hasattr(model_obj, "coef_") else None
            for j, s in enumerate(batch):
                all_chars.append(s["character_id"])
                all_scores.append(float(scores[j]))
                all_hashes.append(s["feature_vector_hash"])

                if coefs is not None:
                    contribs = [(feature_names[k], round(float(coefs[k] * X_scaled[j, k]), 6)) for k in range(len(feature_names))]
                    contribs.sort(key=lambda c: abs(c[1]), reverse=True)
                    all_attributions.append({"top": [{"f": c[0], "v": c[1]} for c in contribs[:10]]})
                else:
                    all_attributions.append({})

        # Compute percentiles
        score_arr = np.array(all_scores)
        if len(score_arr) > 0:
            ranks = np.argsort(np.argsort(score_arr)).astype(float)
            percentiles = ranks / max(len(score_arr) - 1, 1)
        else:
            percentiles = np.array([])

        # ── Bulk upsert ───────────────────────────────────────────────
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for idx in range(len(all_chars)):
            db.execute(
                """INSERT INTO character_spy_shadow_scores
                   (character_id, model_family, model_version, prediction_score,
                    prediction_percentile, split_label, attribution_json,
                    feature_vector_hash, computed_at)
                   VALUES (%s, %s, %s, %s, %s, 'live_shadow', %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     prediction_score = VALUES(prediction_score),
                     prediction_percentile = VALUES(prediction_percentile),
                     attribution_json = VALUES(attribution_json),
                     feature_vector_hash = VALUES(feature_vector_hash),
                     computed_at = VALUES(computed_at)""",
                (all_chars[idx], mf, mv,
                 round(all_scores[idx], 6), round(float(percentiles[idx]), 6),
                 json_dumps_safe(all_attributions[idx]),
                 all_hashes[idx], now_str),
            )

        scored_count = len(all_chars)
        total_scored += scored_count
        model_summaries.append({
            "model": f"{mf}/{mv}",
            "status": "scored",
            "scored": scored_count,
            "skipped": skipped,
        })
        logger.info("Scored %d characters for %s/%s (skipped %d)", scored_count, mf, mv, skipped)

    # ── Write run metrics ─────────────────────────────────────────────────
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    for ms in model_summaries:
        if ms.get("status") == "scored":
            parts = ms["model"].split("/", 1)
            db.execute(
                """INSERT INTO spy_model_run_metrics
                   (run_id, model_family, model_version, split_id,
                    training_sample_count, shadow_sample_count,
                    started_at, completed_at, status, notes)
                   VALUES (%s, %s, %s, NULL, 0, %s, %s, %s, 'success', %s)
                   ON DUPLICATE KEY UPDATE
                     shadow_sample_count = VALUES(shadow_sample_count),
                     completed_at = VALUES(completed_at),
                     status = 'success'""",
                (f"{run_id}_{parts[1]}", parts[0], parts[1],
                 ms["scored"],
                 started.strftime("%Y-%m-%d %H:%M:%S"), now_str,
                 f"Shadow scored {ms['scored']} chars, skipped {ms['skipped']}"),
            )

    summary = f"Shadow scored {total_scored} characters across {len(models)} models (skipped {total_skipped} unchanged)"
    return JobResult.success(
        job_key="score_spy_shadow_ml",
        summary=summary,
        rows_processed=total_scored + total_skipped,
        rows_written=total_scored,
        meta={
            "run_id": run_id,
            "models": model_summaries,
            "total_scored": total_scored,
            "total_skipped": total_skipped,
        },
    ).to_dict()
