#!/usr/bin/env bash
#
# SupplyCore hourly loop — autonomous update + issue-triage cron.
#
# What this script does, once per hour (driven by supplycore-hourly-loop.timer):
#
#   1. Header the run log with the last N run summaries so Claude or a human
#      reading the log sees cross-run context without chasing files.
#   2. Run scripts/update-and-restart.sh --smart (git pull, sync units;
#      only restart services when git SHA or unit files actually changed).
#   3. Run the orchestrator log-to-issues worker directly (not via the
#      in-process scheduler), so failures still get filed as GitHub issues
#      even when the orchestrator itself is broken.
#   4. Build a combined JSON summary and append it to summary.jsonl.
#   5. Create (or reuse) a pinned GitHub tracker issue and post the summary
#      + run-log tail as a comment. That tracker issue is the durable,
#      cross-session memory: anyone (or any future Claude run) can read
#      the last comments to know what was tried, what failed, and whether
#      the latest fix stuck.
#   6. Prune run logs older than ${HOURLY_LOOP_LOG_RETENTION_DAYS:-7} days.
#
# The script is intentionally idempotent and safe to run manually for a
# dry-cycle before enabling the timer:
#
#   sudo /var/www/SupplyCore/scripts/hourly-loop.sh
#
# Environment:
#   GITHUB_TOKEN (optional but required to post to the tracker issue)
#   GITHUB_REPO  (default: maferick/SupplyCore)
#   APP_ROOT     (default: repo root relative to this script)
#   HOURLY_LOOP_CONTEXT_TAIL          (default: 5)
#   HOURLY_LOOP_LOG_RETENTION_DAYS    (default: 7)
#

set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT=${APP_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd)}

LOG_DIR="${APP_ROOT}/storage/logs/hourly-loop"
SUMMARY_FILE="${LOG_DIR}/summary.jsonl"
TRACKER_FILE="${LOG_DIR}/tracker_issue.txt"
RUN_TS=$(date -u +'%Y%m%dT%H%M%SZ')
RUN_LOG="${LOG_DIR}/run-${RUN_TS}.log"
CONTEXT_TAIL=${HOURLY_LOOP_CONTEXT_TAIL:-5}
RUN_LOG_RETENTION_DAYS=${HOURLY_LOOP_LOG_RETENTION_DAYS:-7}

mkdir -p "${LOG_DIR}"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

err() {
  printf '[%s] ERROR: %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

# ── Load .env (for GITHUB_TOKEN / GITHUB_REPO) ────────────────────────
if [[ -f "${APP_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1090,SC1091
  source "${APP_ROOT}/.env"
  set +a
fi

GITHUB_TOKEN=${GITHUB_TOKEN:-}
GITHUB_REPO=${GITHUB_REPO:-maferick/SupplyCore}

# ── Decide which branch the hourly loop should track ─────────────────
# Policy: default to main, but if the working tree is currently on a
# feature branch (claude/*, codex/*, anything non-main), stay on it —
# we cannot auto-merge feature branches back to main from this loop.
cd "${APP_ROOT}"
CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
if [[ -z "${CURRENT_BRANCH}" || "${CURRENT_BRANCH}" == "HEAD" ]]; then
  TARGET_BRANCH="main"
else
  TARGET_BRANCH="${CURRENT_BRANCH}"
fi

# ── Pick the python interpreter for orchestrator calls ───────────────
PYTHON_BIN=""
if [[ -x "${APP_ROOT}/.venv-orchestrator/bin/python" ]]; then
  PYTHON_BIN="${APP_ROOT}/.venv-orchestrator/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN=$(command -v python3)
fi

# ── Seed the run log with cross-run context ──────────────────────────
{
  echo "=== SupplyCore hourly loop ==="
  echo "run_ts=${RUN_TS}"
  echo "target_branch=${TARGET_BRANCH}"
  echo "current_branch=${CURRENT_BRANCH}"
  echo "app_root=${APP_ROOT}"
  echo "python_bin=${PYTHON_BIN:-<none>}"
  echo "github_repo=${GITHUB_REPO}"
  echo "github_token_set=$([[ -n "${GITHUB_TOKEN}" ]] && echo yes || echo no)"
  echo
  if [[ -s "${SUMMARY_FILE}" ]]; then
    echo "--- Previous ${CONTEXT_TAIL} run summaries (tail of summary.jsonl) ---"
    tail -n "${CONTEXT_TAIL}" "${SUMMARY_FILE}" || true
    echo
  else
    echo "--- No previous run summaries (first hourly run) ---"
    echo
  fi
  echo "--- update-and-restart.sh output ---"
} >"${RUN_LOG}"

log "Hourly loop started (run ${RUN_TS}, branch ${TARGET_BRANCH})" | tee -a "${RUN_LOG}"

# ── Step 1: update-and-restart (smart mode) ──────────────────────────
UPDATE_EXIT=0
set +e
"${APP_ROOT}/scripts/update-and-restart.sh" \
  --branch "${TARGET_BRANCH}" \
  --smart \
  2>&1 | tee -a "${RUN_LOG}"
UPDATE_EXIT=${PIPESTATUS[0]}
set -e

UPDATE_SUMMARY_LINE=$(grep '^HOURLY_LOOP_SUMMARY: ' "${RUN_LOG}" | tail -n1 || true)
UPDATE_SUMMARY_JSON="${UPDATE_SUMMARY_LINE#HOURLY_LOOP_SUMMARY: }"
if [[ -z "${UPDATE_SUMMARY_JSON}" ]]; then
  UPDATE_SUMMARY_JSON='{}'
fi

# ── Step 2: log-to-issues worker (direct, not via scheduler) ─────────
{
  echo
  echo "--- log_to_issues worker output ---"
} >>"${RUN_LOG}"

ISSUES_EXIT=0
ISSUES_SUMMARY_JSON='{}'
if [[ -n "${PYTHON_BIN}" ]]; then
  set +e
  "${PYTHON_BIN}" "${APP_ROOT}/bin/python_orchestrator.py" log-to-issues \
    --app-root "${APP_ROOT}" \
    2>&1 | tee -a "${RUN_LOG}"
  ISSUES_EXIT=${PIPESTATUS[0]}
  set -e

  # _print_cli_result prints a single JSON dict line with "command":"log-to-issues".
  # It's unique in the log because structured log entries use different keys.
  ISSUES_SUMMARY_LINE=$(grep -E '^\{"command": *"log-to-issues"' "${RUN_LOG}" | tail -n1 || true)
  if [[ -n "${ISSUES_SUMMARY_LINE}" ]]; then
    ISSUES_SUMMARY_JSON="${ISSUES_SUMMARY_LINE}"
  fi
else
  err "No python3 or .venv-orchestrator interpreter found — skipping log-to-issues" \
    | tee -a "${RUN_LOG}"
  ISSUES_EXIT=127
fi

# ── Step 3: combine into a single JSONL summary record ───────────────
COMBINED_JSON=$(
  UPDATE_SUMMARY_JSON="${UPDATE_SUMMARY_JSON}" \
  ISSUES_SUMMARY_JSON="${ISSUES_SUMMARY_JSON}" \
  RUN_TS="${RUN_TS}" \
  TARGET_BRANCH="${TARGET_BRANCH}" \
  UPDATE_EXIT="${UPDATE_EXIT}" \
  ISSUES_EXIT="${ISSUES_EXIT}" \
  RUN_LOG_NAME="$(basename "${RUN_LOG}")" \
  "${PYTHON_BIN:-python3}" - <<'PY'
import json, os

def safe_loads(s):
    s = (s or "").strip()
    if not s:
        return {}
    try:
        return json.loads(s)
    except Exception:
        return {"raw": s[:500]}

u = safe_loads(os.environ.get("UPDATE_SUMMARY_JSON", ""))
i = safe_loads(os.environ.get("ISSUES_SUMMARY_JSON", ""))

i_result = i.get("result", {}) if isinstance(i, dict) else {}
if not isinstance(i_result, dict):
    i_result = {}

out = {
    "ts": os.environ["RUN_TS"],
    "branch": os.environ["TARGET_BRANCH"],
    "run_log": os.environ["RUN_LOG_NAME"],
    "update_exit": int(os.environ.get("UPDATE_EXIT", "0") or 0),
    "issues_exit": int(os.environ.get("ISSUES_EXIT", "0") or 0),
    "update": u,
    "log_to_issues": {
        "status": i.get("status") if isinstance(i, dict) else None,
        "duration_ms": i.get("duration_ms") if isinstance(i, dict) else None,
        "rows_processed": i.get("rows_processed") if isinstance(i, dict) else None,
        "rows_written": i.get("rows_written") if isinstance(i, dict) else None,
        "summary": i_result.get("summary"),
        "meta": i_result.get("meta"),
        "warnings": i_result.get("warnings"),
    },
}
print(json.dumps(out, default=str))
PY
)

if [[ -z "${COMBINED_JSON}" ]]; then
  COMBINED_JSON="{\"ts\":\"${RUN_TS}\",\"error\":\"combine_failed\"}"
fi

echo "${COMBINED_JSON}" >>"${SUMMARY_FILE}"
{
  echo
  echo "--- combined summary ---"
  echo "${COMBINED_JSON}"
} >>"${RUN_LOG}"

# ── Step 4: post to GitHub tracker issue ─────────────────────────────
post_to_tracker() {
  local repo="$1" token="$2"

  if [[ -z "${token}" ]]; then
    log "GITHUB_TOKEN not set — skipping tracker post" | tee -a "${RUN_LOG}"
    return 0
  fi
  if [[ -z "${PYTHON_BIN}" ]]; then
    err "No python available for tracker API calls — skipping" | tee -a "${RUN_LOG}"
    return 0
  fi

  local tracker_number=""
  if [[ -f "${TRACKER_FILE}" ]]; then
    tracker_number=$(<"${TRACKER_FILE}")
    tracker_number="${tracker_number//[[:space:]]/}"
  fi

  # Render the comment body into a tempfile so we don't have to worry about
  # shell-escaping JSON/markdown.
  local body_file
  body_file=$(mktemp)
  {
    echo "## Run ${RUN_TS}"
    echo
    echo "- **Branch:** \`${TARGET_BRANCH}\`"
    echo "- **Update exit:** \`${UPDATE_EXIT}\`"
    echo "- **Log-to-issues exit:** \`${ISSUES_EXIT}\`"
    echo
    echo "### Combined summary"
    echo '```json'
    echo "${COMBINED_JSON}"
    echo '```'
    echo
    echo "### Run log tail (last 120 lines)"
    echo '```'
    tail -n 120 "${RUN_LOG}" | sed 's/```/ˋˋˋ/g'
    echo '```'
    echo
    echo "_Automated post by \`scripts/hourly-loop.sh\`. Full log on host: \`storage/logs/hourly-loop/${RUN_LOG##*/}\`._"
  } >"${body_file}"

  TRACKER_NUMBER="${tracker_number}" \
  TRACKER_FILE_PATH="${TRACKER_FILE}" \
  REPO="${repo}" \
  TOKEN="${token}" \
  BODY_FILE="${body_file}" \
    "${PYTHON_BIN}" - <<'PY'
import json, os, sys, urllib.request, urllib.error

repo = os.environ["REPO"]
token = os.environ["TOKEN"]
tracker_file = os.environ["TRACKER_FILE_PATH"]
tracker_number = os.environ.get("TRACKER_NUMBER", "").strip()
with open(os.environ["BODY_FILE"], "r", encoding="utf-8") as fh:
    body = fh.read()

HEADERS = {
    "Authorization": f"token {token}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "SupplyCore-HourlyLoop/1.0",
    "Content-Type": "application/json",
}


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


# Create the pinned tracker issue on first run.
if not tracker_number:
    try:
        created = api(
            "POST",
            f"/repos/{repo}/issues",
            {
                "title": "[Auto] Hourly loop — run log",
                "body": (
                    "**Pinned meta-issue — do not close manually.**\n\n"
                    "Each hourly run of `scripts/hourly-loop.sh` adds a comment here "
                    "with its summary, run-log tail, and links to any `auto-log` issues "
                    "filed during the run.\n\n"
                    "This issue is the cross-run memory for the automated "
                    "update / issue-triage loop. Claude Code and human operators can "
                    "read the last few comments to see what was tried, what failed, and "
                    "whether the latest fix is sticking.\n"
                ),
                "labels": ["auto-log", "hourly-loop"],
            },
        )
    except urllib.error.HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")[:300]
        sys.stderr.write(f"Failed to create tracker issue: {exc.code} {err_body}\n")
        sys.exit(2)
    tracker_number = str((created or {}).get("number") or "")
    if not tracker_number:
        sys.stderr.write(f"Tracker creation returned no issue number: {created}\n")
        sys.exit(2)
    with open(tracker_file, "w", encoding="utf-8") as fh:
        fh.write(tracker_number + "\n")
    print(f"Created tracker issue #{tracker_number}")

# Reopen if the tracker was accidentally closed.
try:
    issue = api("GET", f"/repos/{repo}/issues/{tracker_number}")
    if isinstance(issue, dict) and issue.get("state") == "closed":
        api("PATCH", f"/repos/{repo}/issues/{tracker_number}", {"state": "open"})
        print(f"Reopened tracker issue #{tracker_number}")
except urllib.error.HTTPError as exc:
    sys.stderr.write(f"Tracker fetch/reopen failed: {exc.code}\n")

# Post the run comment.
try:
    api(
        "POST",
        f"/repos/{repo}/issues/{tracker_number}/comments",
        {"body": body},
    )
    print(f"Posted comment to tracker issue #{tracker_number}")
except urllib.error.HTTPError as exc:
    err_body = exc.read().decode("utf-8", errors="replace")[:300]
    sys.stderr.write(f"Tracker comment failed: {exc.code} {err_body}\n")
    sys.exit(2)
PY
  local rc=$?
  rm -f "${body_file}"
  return ${rc}
}

if ! post_to_tracker "${GITHUB_REPO}" "${GITHUB_TOKEN}" 2>&1 | tee -a "${RUN_LOG}"; then
  err "Tracker post failed (non-fatal)" | tee -a "${RUN_LOG}"
fi

# ── Step 5: prune run logs older than retention window ───────────────
find "${LOG_DIR}" -name 'run-*.log' -type f -mtime "+${RUN_LOG_RETENTION_DAYS}" -delete 2>/dev/null || true

log "Hourly loop finished (update_exit=${UPDATE_EXIT} issues_exit=${ISSUES_EXIT})" \
  | tee -a "${RUN_LOG}"

# Exit non-zero so systemd flags a failure if either critical step broke.
if [[ ${UPDATE_EXIT} -ne 0 || ${ISSUES_EXIT} -ne 0 ]]; then
  exit 1
fi
exit 0
