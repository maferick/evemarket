#!/usr/bin/env bash
#
# SupplyCore Claude triage loop — picks the next open auto-log issue(s)
# and kicks off a Claude Code on GitHub session by posting an @claude
# mention.
#
# Runs on supplycore-claude-triage.timer, offset 30 min from the hourly
# loop so the two automations don't hit GitHub at the same second.
#
# Policy:
#   - Only issues labeled `auto-log` are eligible.
#   - The pinned tracker issue (label `hourly-loop`) is always skipped.
#   - Issues already marked `claude-triage-started` are skipped so we
#     never re-tag the same issue.
#   - Issues that already have a comment containing `@claude` (e.g. from
#     a manual tag) are labeled `claude-triage-started` and skipped.
#   - Remaining candidates are tagged oldest-first, up to
#     MAX_CLAUDE_INVOCATIONS_PER_RUN (default 1).
#
# This keeps the Claude Code on GitHub workload bounded — one issue per
# hour by default. Bump MAX_CLAUDE_INVOCATIONS_PER_RUN in
# /etc/default/supplycore-claude-triage to go faster.
#
# Environment:
#   GITHUB_TOKEN (required)
#   GITHUB_REPO  (default: maferick/SupplyCore)
#   MAX_CLAUDE_INVOCATIONS_PER_RUN (default: 1)
#   CLAUDE_TRIAGE_LABEL            (default: claude-triage-started)
#

set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT=${APP_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd)}

LOG_DIR="${APP_ROOT}/storage/logs/hourly-loop"
RUN_TS=$(date -u +'%Y%m%dT%H%M%SZ')
RUN_LOG="${LOG_DIR}/triage-${RUN_TS}.log"
LOG_RETENTION_DAYS=${CLAUDE_TRIAGE_LOG_RETENTION_DAYS:-7}

MAX_PER_RUN=${MAX_CLAUDE_INVOCATIONS_PER_RUN:-1}
CLAUDE_LABEL=${CLAUDE_TRIAGE_LABEL:-claude-triage-started}

mkdir -p "${LOG_DIR}"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" | tee -a "${RUN_LOG}"
}

# Load .env so GITHUB_TOKEN / GITHUB_REPO are picked up the same way as
# the hourly loop script.
if [[ -f "${APP_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1090,SC1091
  source "${APP_ROOT}/.env"
  set +a
fi

GITHUB_TOKEN=${GITHUB_TOKEN:-}
GITHUB_REPO=${GITHUB_REPO:-maferick/SupplyCore}

log "Claude triage run started (repo=${GITHUB_REPO} max_per_run=${MAX_PER_RUN} label=${CLAUDE_LABEL})"

if [[ -z "${GITHUB_TOKEN}" ]]; then
  log "GITHUB_TOKEN not set — cannot triage. Exiting cleanly."
  exit 0
fi

PYTHON_BIN=""
if [[ -x "${APP_ROOT}/.venv-orchestrator/bin/python" ]]; then
  PYTHON_BIN="${APP_ROOT}/.venv-orchestrator/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN=$(command -v python3)
fi

if [[ -z "${PYTHON_BIN}" ]]; then
  log "No python3 available — cannot triage. Exiting."
  exit 1
fi

REPO="${GITHUB_REPO}" TOKEN="${GITHUB_TOKEN}" MAX_PER_RUN="${MAX_PER_RUN}" \
  CLAUDE_LABEL="${CLAUDE_LABEL}" RUN_LOG_PATH="${RUN_LOG}" \
  "${PYTHON_BIN}" - <<'PY'
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import urlencode

repo = os.environ["REPO"]
token = os.environ["TOKEN"]
max_per_run = int(os.environ.get("MAX_PER_RUN", "1"))
claude_label = os.environ["CLAUDE_LABEL"]
log_path = os.environ["RUN_LOG_PATH"]

HEADERS = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "SupplyCore-ClaudeTriage/1.0",
    "Content-Type": "application/json",
}


def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def api(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        f"https://api.github.com{path}",
        data=data,
        headers=HEADERS,
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw) if raw else None


def has_existing_claude_mention(issue_number):
    """Return True if any comment on the issue already mentions @claude.

    Used to avoid double-tagging issues the operator manually asked Claude
    to look at — and to idempotently label them so we don't check again
    next hour.
    """
    try:
        comments = api(
            "GET",
            f"/repos/{repo}/issues/{issue_number}/comments?per_page=100",
        ) or []
    except urllib.error.HTTPError as exc:
        log(f"   WARN could not fetch comments for #{issue_number}: {exc.code}")
        return False
    for comment in comments:
        body = str(comment.get("body") or "")
        if "@claude" in body.lower():
            return True
    return False


# 1. Fetch open issues labeled auto-log, oldest first.
try:
    params = urlencode({
        "state": "open",
        "labels": "auto-log",
        "per_page": "100",
        "sort": "created",
        "direction": "asc",
    })
    issues = api("GET", f"/repos/{repo}/issues?{params}") or []
except urllib.error.HTTPError as exc:
    body = exc.read().decode("utf-8", errors="replace")[:300]
    log(f"Failed to list issues: {exc.code} {body}")
    sys.exit(1)

log(f"Fetched {len(issues)} open auto-log issue(s)")

# 2. Filter out PRs, the pinned hourly-loop tracker, and issues already
#    marked claude-triage-started.
candidates = []
for issue in issues:
    if "pull_request" in issue:
        continue
    labels = {str((label or {}).get("name") or "") for label in (issue.get("labels") or [])}
    if "hourly-loop" in labels:
        continue
    if claude_label in labels:
        continue
    candidates.append(issue)

log(f"After label filter: {len(candidates)} candidate(s)")

# 3. Walk candidates oldest-first until we've picked max_per_run.
#    If we hit one that already has a manual @claude mention, label it
#    and move on so we don't re-check it next hour.
picks = []
for issue in candidates:
    if len(picks) >= max_per_run:
        break
    num = int(issue["number"])
    if has_existing_claude_mention(num):
        log(f"   skipping #{num} — already has an @claude mention")
        try:
            api(
                "POST",
                f"/repos/{repo}/issues/{num}/labels",
                {"labels": [claude_label]},
            )
            log(f"   applied '{claude_label}' label to #{num} to suppress future checks")
        except urllib.error.HTTPError as exc:
            log(f"   WARN could not label #{num}: {exc.code}")
        continue
    picks.append(issue)

if not picks:
    log("No eligible issues to triage this run.")
    sys.exit(0)

pick_labels = ", ".join("#" + str(p["number"]) for p in picks)
log(f"Triaging {len(picks)} issue(s): {pick_labels}")

triaged = []
failed = []

for issue in picks:
    num = int(issue["number"])
    title = str(issue.get("title") or "")[:80]
    log(f"-> #{num}: {title}")

    branch_slug = f"claude/fix-auto-log-{num}"
    comment_body = (
        "@claude please investigate this auto-detected failure.\n\n"
        "**Instructions:**\n"
        "1. Read the error pattern and recent occurrences in the issue body above.\n"
        "2. Find the code path that produced this error — search the repo for the "
        "job name, error message, or any stack frame present in the log.\n"
        "3. Identify the root cause. If the issue body is not enough, check the "
        "`job_runs` / `sync_runs` tables or the relevant log file under "
        "`storage/logs/` on the host.\n"
        f"4. If you have **high confidence** in a minimal fix, open a PR on a new "
        f"branch `{branch_slug}` with the smallest change that addresses the root "
        "cause. Add a test or reproducer if feasible. Keep the change scoped — do "
        "not refactor surrounding code or add speculative defensive handling.\n"
        "5. If the root cause is **ambiguous** or would require a large refactor, "
        "post a comment here with your findings and what additional information "
        "you'd need to proceed — do NOT open a speculative PR.\n\n"
        "Do not close the issue yourself. The hourly `log_to_issues` worker will "
        "auto-close it 72 hours after the failure stops recurring, and will "
        "reopen it automatically if the failure returns.\n\n"
        "_Tagged automatically by `scripts/claude-triage.sh` — the hourly "
        "autonomous triage loop. See the pinned `[Auto] Hourly loop — run log` "
        "tracker issue for the full timeline of automated runs._"
    )

    try:
        api(
            "POST",
            f"/repos/{repo}/issues/{num}/comments",
            {"body": comment_body},
        )
        log(f"   OK posted @claude comment on #{num}")
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")[:300]
        log(f"   ERR comment failed on #{num}: {exc.code} {err_body}")
        failed.append(num)
        continue

    try:
        api(
            "POST",
            f"/repos/{repo}/issues/{num}/labels",
            {"labels": [claude_label]},
        )
        log(f"   OK applied '{claude_label}' label to #{num}")
    except urllib.error.HTTPError as exc:
        log(f"   WARN label add failed on #{num}: {exc.code} — next run may re-tag")

    triaged.append(num)

log(f"Triage complete: triaged={triaged} failed={failed}")
PY

# Prune old triage logs.
find "${LOG_DIR}" -name 'triage-*.log' -type f -mtime "+${LOG_RETENTION_DAYS}" -delete 2>/dev/null || true

log "Claude triage run finished"
