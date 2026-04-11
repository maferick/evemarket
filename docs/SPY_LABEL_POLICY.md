# Spy Detection — Training Label Policy

This document is the **authoritative prose definition** of how rows in the
`analyst_feedback` table map to training labels for the spy detection
platform. The machine-readable version is
`python/orchestrator/jobs/spy_label_policy.py` — the two must stay in
sync. If they disagree, the Python module wins at runtime and the doc is
a bug.

Every job that reads `analyst_feedback` for ML purposes (training split
builder, Phase 6 trainer, any future labeler) **MUST** import
`spy_label_policy.apply()` rather than reimplementing the mapping. Drift
in label selection is a class of bug we specifically design out by
centralizing this policy.

## Policy version

| Field | Value |
|------|------|
| `POLICY_VERSION` | `spy_label_policy_v1` |

`POLICY_VERSION` is stamped into `model_training_splits.notes` for
provenance. Bump it whenever the mapping or thresholds change so old
splits remain identifiable after the code changes.

## Input — `analyst_feedback` rows

The `analyst_feedback` table (defined in
`database/migrations/20260327_enhanced_intelligence_platform.sql`) is
populated by the analyst feedback form in
`public/battle-intelligence/character.php` and consumed by
`python/orchestrator/jobs/graph_analyst_recalibration.py`. Relevant
columns:

| Column | Type | Notes |
|------|------|------|
| `character_id` | `BIGINT UNSIGNED` | Subject of the feedback. |
| `label` | `ENUM('true_positive','false_positive','needs_review','confirmed_clean')` | Analyst verdict. |
| `confidence` | `DECIMAL(5,3)` | Analyst's own confidence, 0..1. |
| `created_at` | `DATETIME` | When the verdict was recorded. |

## Mapping table

| `analyst_feedback.label` | Confidence floor | Training label | Rationale |
|------|------|------|------|
| `true_positive` | ≥ 0.60 | `positive` | Analyst-confirmed spy. Sub-threshold confidence excluded to avoid low-quality positives. |
| `false_positive` | ≥ 0.60 | `negative` | Analyst-confirmed clean after review. Treated as a hard negative. |
| `confirmed_clean` | ≥ 0.75 | `negative` | Stronger clean signal — higher confidence bar because the default population is also "not flagged". |
| `needs_review` | — | *excluded* | Pending adjudication; including biases either direction. |
| *any, below floor* | < floor | *excluded* | Below-threshold confidence is noise. |
| *unknown label string* | — | *excluded* | Defensive — future enum additions should not silently land as positives. |

## Confidence floors

Defined as constants in `spy_label_policy.py`:

| Constant | Value |
|------|------|
| `POSITIVE_CONFIDENCE_FLOOR` | `0.60` |
| `FALSE_POSITIVE_CONFIDENCE_FLOOR` | `0.60` |
| `CONFIRMED_CLEAN_CONFIDENCE_FLOOR` | `0.75` |

The `confirmed_clean` floor is strictly higher than the
`false_positive` floor because `false_positive` is an active rejection
of a prior flag (analyst has looked at the evidence), whereas
`confirmed_clean` can be recorded against anyone, including characters
who were never flagged in the first place. Requiring a higher
confidence for `confirmed_clean` prevents the training set from being
flooded with noisy easy negatives.

## Weak labels

Rows with `model_training_split_members.label_source != 'analyst_feedback'`
are considered **weak labels** (e.g. rule-based heuristics, synthetic
positives from known confirmed spies' alts). Weak labels are allowed
**only** in `split_role='train'` — never in `validation`, `test`, or
`holdout`.

* **Enforced in code**: `is_valid_split_role_for_weak_label()` in
  `spy_label_policy.py` returns `True` only for `train`. The split
  builder checks this before emitting a member row.
* **Rationale**: evaluation metrics must not be contaminated by the
  same heuristics that produced the weak labels; otherwise the model
  scores itself against its own rules.
* **Critical constraint (Phase 6)**: weak labels **must not** be sourced
  from the Phase 6 shadow model's own predictions — that creates a
  self-referential feedback loop where the model trains on its own
  output and collapses. This is documented in the Phase 6 risks
  section of the plan and enforced by code review on any new weak
  label source.

## Latest-row-wins

A character may have multiple `analyst_feedback` rows over time (e.g.
`needs_review` → `true_positive` after investigation). The split
builder takes the most recent row by `created_at` per character within
each window, then runs it through `apply()`. This keeps training sets
in sync with the analyst's current belief, not their historical
uncertainty.

## Usage

```python
from python.orchestrator.jobs import spy_label_policy

for row in feedback_rows:
    decision = spy_label_policy.apply(row)
    if decision.label is None:
        continue  # excluded — log `decision.reason` for audit
    write_member(
        split_id=split_id,
        character_id=row["character_id"],
        split_role="train",  # or "test", "validation", etc.
        label=decision.label,           # 'positive' or 'negative'
        label_source=decision.label_source,  # 'analyst_feedback'
        label_confidence=decision.effective_confidence,
    )
```

## Change process

Changes to this policy — the mapping, the confidence floors, or the
weak-label rules — **require**:

1. A matching edit to `python/orchestrator/jobs/spy_label_policy.py`.
2. A bump of `POLICY_VERSION` (e.g. `spy_label_policy_v1` → `v2`).
3. An entry in this doc's change log below.
4. A rebuild of any active `model_training_splits` rows — old splits
   keep their old `POLICY_VERSION` stamp for provenance and should be
   treated as immutable historical records.

## Change log

| Version | Date | Change |
|------|------|------|
| `spy_label_policy_v1` | 2026-04-11 | Initial policy. Introduced with spy detection Phase 2. |
