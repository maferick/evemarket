"""Authoritative label policy for the spy detection training pipeline.

This module is the single source of truth for mapping ``analyst_feedback``
rows to training labels. The prose version lives in
``docs/SPY_LABEL_POLICY.md`` — the two must stay in sync.

Every job that reads ``analyst_feedback`` for ML purposes (the training split
builder, the Phase 6 trainer, any future labeler) MUST import
:func:`apply` from this module instead of reimplementing the mapping.
Drift in label selection is a class of bug we specifically design out
by centralizing here.

Policy version
--------------

``POLICY_VERSION`` is stamped into ``model_training_splits.notes`` for
provenance. Bump it whenever the mapping or thresholds change so old
splits remain identifiable after the code changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

POLICY_VERSION = "spy_label_policy_v1"

TrainingLabel = Literal["positive", "negative"]

# Thresholds (confidence floors). The "confirmed_clean" threshold is stricter
# than "false_positive" because the default population is also "not flagged"
# — we need a stronger signal to call someone a hard negative.
POSITIVE_CONFIDENCE_FLOOR = 0.60
FALSE_POSITIVE_CONFIDENCE_FLOOR = 0.60
CONFIRMED_CLEAN_CONFIDENCE_FLOOR = 0.75


@dataclass(slots=True)
class LabelDecision:
    """Outcome of applying the policy to one analyst_feedback row.

    ``label`` is None when the row is excluded (e.g. needs_review, or a
    below-threshold confidence). ``reason`` is a stable string suitable for
    logging / aggregated policy audits.
    """

    label: TrainingLabel | None
    reason: str
    label_source: str = "analyst_feedback"
    effective_confidence: float = 0.0


def apply(feedback_row: dict[str, Any]) -> LabelDecision:
    """Map one ``analyst_feedback`` row to a training label (or exclusion).

    Parameters
    ----------
    feedback_row:
        A dict with at least ``label`` (str) and ``confidence`` (float-like).
        Extra fields are ignored — this keeps the helper decoupled from the
        exact SELECT shape.

    Returns
    -------
    LabelDecision
        ``label`` is None when the row is excluded. Callers should skip
        excluded rows rather than synthesizing an ``unknown`` training
        label — ``unknown`` is reserved for genuinely unlabeled members
        (e.g. a holdout split of the unlabeled population).
    """
    raw_label = str(feedback_row.get("label") or "").strip().lower()
    try:
        confidence = float(feedback_row.get("confidence") or 0.0)
    except (TypeError, ValueError):
        confidence = 0.0

    if raw_label == "true_positive":
        if confidence >= POSITIVE_CONFIDENCE_FLOOR:
            return LabelDecision(
                label="positive",
                reason="true_positive_accepted",
                effective_confidence=confidence,
            )
        return LabelDecision(
            label=None,
            reason="true_positive_below_confidence_floor",
            effective_confidence=confidence,
        )

    if raw_label == "false_positive":
        if confidence >= FALSE_POSITIVE_CONFIDENCE_FLOOR:
            return LabelDecision(
                label="negative",
                reason="false_positive_accepted",
                effective_confidence=confidence,
            )
        return LabelDecision(
            label=None,
            reason="false_positive_below_confidence_floor",
            effective_confidence=confidence,
        )

    if raw_label == "confirmed_clean":
        if confidence >= CONFIRMED_CLEAN_CONFIDENCE_FLOOR:
            return LabelDecision(
                label="negative",
                reason="confirmed_clean_accepted",
                effective_confidence=confidence,
            )
        return LabelDecision(
            label=None,
            reason="confirmed_clean_below_confidence_floor",
            effective_confidence=confidence,
        )

    if raw_label == "needs_review":
        return LabelDecision(
            label=None,
            reason="needs_review_excluded",
            effective_confidence=confidence,
        )

    return LabelDecision(
        label=None,
        reason=f"unknown_label_excluded:{raw_label}",
        effective_confidence=confidence,
    )


def is_valid_split_role_for_weak_label(split_role: str) -> bool:
    """Weak labels are allowed only in the ``train`` split role.

    Validation/test/holdout splits must use analyst_feedback-sourced labels
    so evaluation metrics are not contaminated by the same heuristics that
    produced the weak labels. This helper exists so the split builder can
    reject invalid combinations at insert time with a clear error.
    """
    return split_role == "train"
