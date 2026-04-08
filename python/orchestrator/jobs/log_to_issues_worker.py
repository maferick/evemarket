from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from ..db import SupplyCoreDb

logger = logging.getLogger(__name__)

# ── Error normalization ──────────────────────────────────────────────
# Strip volatile fragments so the same root-cause maps to one fingerprint.

_NORMALIZERS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}\S*"), "<TS>"),
    (re.compile(r"\b0x[0-9a-fA-F]+\b"), "<ADDR>"),
    (re.compile(r"\b\d{5,}\b"), "<ID>"),
    (re.compile(r"\s+"), " "),
]


def _normalize_error(text: str) -> str:
    out = text.strip()[:500]
    for pattern, repl in _NORMALIZERS:
        out = pattern.sub(repl, out)
    return out.strip()


def _fingerprint(job_name: str, error_normalized: str) -> str:
    return hashlib.sha256(f"{job_name}::{error_normalized}".encode()).hexdigest()


# ── GitHub API helpers ───────────────────────────────────────────────

def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "SupplyCore-LogToIssues/1.0",
    }


def _create_github_issue(
    token: str,
    repo: str,
    title: str,
    body: str,
    labels: list[str],
) -> dict[str, Any]:
    url = f"https://api.github.com/repos/{repo}/issues"
    payload = json.dumps({"title": title, "body": body, "labels": labels}).encode()
    req = urllib.request.Request(url, data=payload, headers=_github_headers(token), method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def _close_github_issue(token: str, repo: str, issue_number: int) -> None:
    url = f"https://api.github.com/repos/{repo}/issues/{issue_number}"
    payload = json.dumps({"state": "closed"}).encode()
    req = urllib.request.Request(url, data=payload, headers=_github_headers(token), method="PATCH")
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


def _comment_github_issue(token: str, repo: str, issue_number: int, body: str) -> None:
    url = f"https://api.github.com/repos/{repo}/issues/{issue_number}/comments"
    payload = json.dumps({"body": body}).encode()
    req = urllib.request.Request(url, data=payload, headers=_github_headers(token), method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        resp.read()


# ── Issue body formatting ────────────────────────────────────────────

def _format_issue_body(
    job_name: str,
    error_text: str,
    occurrences: list[dict[str, Any]],
) -> str:
    recent = occurrences[:5]
    occurrence_lines = "\n".join(
        f"| {r['id']} | {r['started_at']} | {r['duration_ms']}ms | `{(r.get('error_text') or '')[:80]}` |"
        for r in recent
    )
    return f"""## Auto-detected job failure: `{job_name}`

**Error pattern:**
```
{error_text[:500]}
```

**Occurrences:** {len(occurrences)} failure(s) in the scan window

### Recent failures

| Run ID | Started At | Duration | Error (truncated) |
|--------|-----------|----------|-------------------|
{occurrence_lines}

---

*This issue was automatically created by the SupplyCore log-to-issues worker.*
*Label: `auto-log` — Claude Code can pick this up for automated triage.*
"""


# ── Ensure the table exists ─────────────────────────────────────────

_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS log_issue_tracker (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fingerprint     VARCHAR(64)    NOT NULL,
    job_name        VARCHAR(120)   NOT NULL,
    error_pattern   VARCHAR(500)   NOT NULL,
    github_issue_number INT UNSIGNED DEFAULT NULL,
    github_issue_url    VARCHAR(500)  DEFAULT NULL,
    occurrence_count    INT UNSIGNED NOT NULL DEFAULT 1,
    first_seen_at   DATETIME       NOT NULL,
    last_seen_at    DATETIME       NOT NULL,
    resolved_at     DATETIME       DEFAULT NULL,
    created_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_fingerprint (fingerprint),
    KEY idx_log_issue_job (job_name),
    KEY idx_log_issue_resolved (resolved_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


# ── Core worker logic ────────────────────────────────────────────────

def run_log_to_issues(
    db: SupplyCoreDb,
    *,
    lookback_hours: int = 24,
    dry_run: bool = False,
    auto_close: bool = True,
) -> dict[str, object]:
    """Scan job_runs for failures and create/update GitHub issues.

    Returns a standard job result dict.
    """
    github_token = os.getenv("GITHUB_TOKEN", "").strip()
    github_repo = os.getenv("GITHUB_REPO", "maferick/SupplyCore").strip()

    if not github_token and not dry_run:
        return {
            "status": "skipped",
            "rows_processed": 0,
            "rows_written": 0,
            "summary": "GITHUB_TOKEN not set — skipping issue creation. Set GITHUB_TOKEN in .env to enable.",
            "warnings": ["GITHUB_TOKEN not configured"],
        }

    # Ensure tracker table exists (idempotent).
    db.execute(_ENSURE_TABLE_SQL)

    # ── 1. Fetch recent failures ─────────────────────────────────────
    failures = db.fetch_all(
        "SELECT id, job_name, error_text, started_at, finished_at, duration_ms, meta_json "
        "FROM job_runs "
        "WHERE status = 'failed' "
        "  AND job_name != 'log_to_issues' "
        "  AND started_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s HOUR) "
        "ORDER BY started_at DESC",
        (lookback_hours,),
    )

    if not failures:
        logger.info("No failed job_runs in the last %d hours.", lookback_hours)
        return {
            "status": "success",
            "rows_processed": 0,
            "rows_written": 0,
            "summary": f"No failures in the last {lookback_hours}h.",
            "warnings": [],
        }

    # ── 2. Group by fingerprint ──────────────────────────────────────
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in failures:
        error_raw = str(row.get("error_text") or "unknown error")
        norm = _normalize_error(error_raw)
        fp = _fingerprint(str(row["job_name"]), norm)
        grouped.setdefault(fp, []).append({**row, "_normalized": norm})

    issues_created = 0
    issues_updated = 0
    issues_skipped = 0
    warnings: list[str] = []

    # ── 3. Process each group ────────────────────────────────────────
    for fp, occurrences in grouped.items():
        job_name = str(occurrences[0]["job_name"])
        norm_error = str(occurrences[0]["_normalized"])
        first_ts = min(r["started_at"] for r in occurrences)
        last_ts = max(r["started_at"] for r in occurrences)

        existing = db.fetch_one(
            "SELECT id, github_issue_number, occurrence_count, resolved_at "
            "FROM log_issue_tracker WHERE fingerprint = %s",
            (fp,),
        )

        if existing:
            # Always bump count and update last_seen.
            db.execute(
                "UPDATE log_issue_tracker "
                "SET occurrence_count = occurrence_count + %s, "
                "    last_seen_at = %s "
                "WHERE id = %s",
                (len(occurrences), last_ts, existing["id"]),
            )

            if existing.get("github_issue_number"):
                # Issue already filed — just update count.
                # If it was previously resolved but now reoccurring, reopen.
                if existing.get("resolved_at"):
                    db.execute(
                        "UPDATE log_issue_tracker SET resolved_at = NULL WHERE id = %s",
                        (existing["id"],),
                    )
                    if not dry_run and github_token:
                        try:
                            _comment_github_issue(
                                github_token, github_repo,
                                int(existing["github_issue_number"]),
                                f"This failure has reoccurred ({len(occurrences)} new occurrence(s) since last resolution). Reopening for triage.",
                            )
                        except Exception as exc:
                            warnings.append(f"Failed to comment on #{existing['github_issue_number']}: {exc}")
                issues_updated += 1
                continue

            # Tracked but never filed (previous run had no token) — fall
            # through to issue-creation below so the GitHub issue gets made.

        # ── New failure pattern — create issue ───────────────────────
        error_short = norm_error[:80]
        title = f"[Auto] {job_name}: {error_short}"
        if len(title) > 120:
            title = title[:117] + "..."

        body = _format_issue_body(
            job_name,
            str(occurrences[0].get("error_text") or "unknown"),
            occurrences,
        )

        issue_number = None
        issue_url = None

        if not dry_run and github_token:
            try:
                resp = _create_github_issue(
                    github_token, github_repo, title, body,
                    labels=["auto-log", "bug"],
                )
                issue_number = resp.get("number")
                issue_url = resp.get("html_url")
                logger.info(
                    "Created GitHub issue #%s for %s",
                    issue_number, job_name,
                    extra={"payload": {"issue_number": issue_number, "job_name": job_name}},
                )
            except urllib.error.HTTPError as exc:
                err_body = exc.read().decode("utf-8", errors="replace")[:300]
                warnings.append(f"GitHub API error creating issue for {job_name}: {exc.code} {err_body}")
                logger.warning("GitHub API error: %s %s", exc.code, err_body)
                issues_skipped += 1
                continue
            except Exception as exc:
                warnings.append(f"Failed to create issue for {job_name}: {exc}")
                logger.warning("Issue creation failed: %s", exc)
                issues_skipped += 1
                continue
        elif dry_run:
            logger.info("[DRY RUN] Would create issue: %s", title)

        db.execute(
            "INSERT INTO log_issue_tracker "
            "(fingerprint, job_name, error_pattern, github_issue_number, "
            " github_issue_url, occurrence_count, first_seen_at, last_seen_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "  github_issue_number = COALESCE(VALUES(github_issue_number), github_issue_number), "
            "  github_issue_url = COALESCE(VALUES(github_issue_url), github_issue_url), "
            "  last_seen_at = VALUES(last_seen_at)",
            (fp, job_name, norm_error[:500], issue_number, issue_url,
             len(occurrences), first_ts, last_ts),
        )
        issues_created += 1

    # ── 4. Auto-close resolved issues ────────────────────────────────
    auto_closed = 0
    if auto_close and not dry_run and github_token:
        stale = db.fetch_all(
            "SELECT id, github_issue_number, job_name "
            "FROM log_issue_tracker "
            "WHERE resolved_at IS NULL "
            "  AND github_issue_number IS NOT NULL "
            "  AND last_seen_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 72 HOUR)"
        )
        for row in stale:
            try:
                _comment_github_issue(
                    github_token, github_repo,
                    int(row["github_issue_number"]),
                    "No recurrence in 72 hours — auto-closing. Will reopen if the failure returns.",
                )
                _close_github_issue(github_token, github_repo, int(row["github_issue_number"]))
                db.execute(
                    "UPDATE log_issue_tracker SET resolved_at = UTC_TIMESTAMP() WHERE id = %s",
                    (row["id"],),
                )
                auto_closed += 1
            except Exception as exc:
                warnings.append(f"Failed to auto-close #{row['github_issue_number']}: {exc}")

    summary_parts = [
        f"Scanned {len(failures)} failure(s)",
        f"created {issues_created} issue(s)",
        f"updated {issues_updated}",
        f"skipped {issues_skipped}",
    ]
    if auto_closed:
        summary_parts.append(f"auto-closed {auto_closed}")
    if dry_run:
        summary_parts.append("(dry run)")

    return {
        "status": "success",
        "rows_processed": len(failures),
        "rows_written": issues_created,
        "summary": ", ".join(summary_parts) + ".",
        "warnings": warnings,
        "meta": {
            "issues_created": issues_created,
            "issues_updated": issues_updated,
            "issues_skipped": issues_skipped,
            "auto_closed": auto_closed,
            "failures_scanned": len(failures),
            "unique_patterns": len(grouped),
        },
    }
