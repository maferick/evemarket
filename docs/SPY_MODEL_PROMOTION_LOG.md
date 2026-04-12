# Spy Model Promotion Log

Governance record for shadow ML model promotions. A shadow model may be
promoted to `status='promoted'` (and eventually contribute a weighted component
to `character_spy_risk_profiles`) only when **all** of the following hold for
≥ 3 consecutive weekly runs:

1. **Calibration**: Brier score ≤ 0.15 against held-out analyst-labeled data.
2. **Top-K precision**: precision@100 ≥ 0.60 vs `analyst_feedback` true_positive labels.
3. **Feature stability**: no feature drift > 2σ over 30 days (`drift_json` from `spy_model_run_metrics`).
4. **Manual analyst review**: 10 random positives + 10 random negatives from shadow's top decile, hand-scored.
5. **Sign-off**: two-person review — at least one `intel_lead` + one operator with `spy_model_registry` access.

## Promotion format

Each promotion is recorded as a section below with:

- **Date**
- **Model family / version**
- **Metric snapshots** (Brier, precision@100, AUC-ROC, PR-AUC, ECE)
- **Reviewer names** (in-game character names or handles)
- **Decision** (promote / reject / defer)
- **Notes** (rationale, conditions, caveats)

## Rollout mechanism

Promotion does NOT silently swap the primary score. A promoted model is added
to `character_spy_risk_profiles.component_weights_json` with a small initial
weight (e.g. 0.05). The weight is increased over subsequent quarters as
confidence builds. The primary score formula always remains transparent and
analyst-overridable.

---

## Promotions

_(No promotions recorded yet.)_
