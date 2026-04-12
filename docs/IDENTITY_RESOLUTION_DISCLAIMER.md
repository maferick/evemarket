# Identity Resolution — Analyst Disclaimer

This document holds the **authoritative operator-facing disclaimer** for any
feature that surfaces "likely shared operator" or "probable alt" inferences
in SupplyCore. Every UI surface and API endpoint that exposes
`character_identity_links`, `character_identity_clusters`, or derived fields
must render this disclaimer verbatim or reference the PHP string constant
`IDENTITY_RESOLUTION_DISCLAIMER` (defined in
`src/views/partials/character-intel-helpers.php`).

If the disclaimer here and the PHP constant disagree, **the PHP constant is
what ships to users** — update it in the same PR that changes this doc.

## Authoritative copy

> **Identity inference is probabilistic, not verified.** Links and clusters
> on this page are statistical inferences from org history, co-presence,
> temporal, and behavioral signals. They do **not** constitute a confirmed
> identification of a real-world operator and must not be cited as proof.
> Component breakdowns are shown so analysts can judge each signal on its
> own merits. Acting on this information (accusations, bans, bounties,
> public naming) without corroborating evidence is expressly not supported
> by the platform.

This copy is reproduced in API responses under `meta.disclaimer` so API
consumers cannot strip it inadvertently. It is also rendered at the top of
the identity cluster card in the Phase 5 `character.php` spy-risk panel.

## UI rules (enforced in code review)

1. **The composite `link_score` is never displayed without the component
   breakdown also visible.** Analysts must see why a link was scored the
   way it was.
2. **The label "same operator" is never used alone.** Copy is always
   "likely shared operator (inference)" or "probable alt (inference)".
3. **Clusters above the max-size cap are hidden from the analyst UI
   entirely**, not shown with a warning — they are high false-positive
   risk and should not be surfaced. The current cap is 12 members; any
   cluster above this is flagged in `identity_resolution_runs` but not
   rendered.
4. **Component scores are never aggregated in a chart that hides them
   behind a single bar.** Keep per-component visibility.
5. **High-confidence-only in shareable surfaces.** Any surface that can be
   copied to Discord, a report, or a case file restricts to
   `confidence_tier='high'` links. Low/medium tiers are analyst-internal.

## Data retention

| Table | Retention | Notes |
|------|------|------|
| `character_identity_links` | 180 days after last `computed_at` refresh | Stale links pruned by a follow-up retention job. |
| `character_identity_clusters` + `character_identity_cluster_members` | 180 days after last `computed_at` refresh | Cascade from cluster deletion. |
| `identity_resolution_runs` | 90 days | Run metadata only — no identity inferences stored here. |

Analysts can request deletion of specific links via a feedback mechanism
(follow-up feature). In v1 the fallback is a manual `DELETE FROM
character_identity_links WHERE link_id = ?` by an operator with a note in
the deletion log below.

## Access logging

Reads from `/api/spy-risk/*` endpoints that return identity cluster data
are logged to the existing audit log mechanism with `audit_scope =
'identity_resolution'`. This provides traceability if inferences are later
contested.

## Deletion log

Manual deletions of identity links or clusters must be appended here with
the operator, date, character_ids involved, and reason.

| Date | Operator | Scope | Reason |
|------|------|------|------|

## Change log

| Date | Change |
|------|------|
| 2026-04-11 | Initial disclaimer authored with Phase 3 of the spy detection platform. |
