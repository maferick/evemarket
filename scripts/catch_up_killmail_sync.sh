#!/usr/bin/env bash
#
# Continuously run character_killmail_sync until the backfill is caught up.
#
# After wiping the corrupt killmail_events rows with
# ``scripts/remediate_esi_cache_collision.py --apply``, the backlog for
# the last ~2 days needs to be re-fetched.  A single character_killmail_sync
# invocation runs within its ~55s budget and then returns — this script
# just keeps relaunching it until one of the stop conditions is met:
#
#   * The killmail queue is drained (``characters_processed == 0``)
#   * ``new_killmails == 0`` for N consecutive runs (default 3)
#   * The caller hits Ctrl-C
#   * The max run count is reached (default unlimited)
#
# Usage:
#   scripts/catch_up_killmail_sync.sh                # run until caught up
#   scripts/catch_up_killmail_sync.sh --idle 5       # require 5 quiet runs
#   scripts/catch_up_killmail_sync.sh --max 200      # cap at 200 iterations
#   scripts/catch_up_killmail_sync.sh --sleep 2      # pause 2s between runs
#   APP_ROOT=/var/www/SupplyCore scripts/catch_up_killmail_sync.sh
#
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT=${APP_ROOT:-$(cd -- "${SCRIPT_DIR}/.." && pwd)}
PYTHON_BIN=${PYTHON_BIN:-python}
JOB_KEY=${JOB_KEY:-character_killmail_sync}

IDLE_THRESHOLD=3
MAX_RUNS=0
SLEEP_BETWEEN=1

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Loops ``$JOB_KEY`` until the backlog is caught up.

Options:
  --idle N        Stop after N consecutive runs with 0 new killmails (default: ${IDLE_THRESHOLD})
  --max N         Stop after N total runs (default: unlimited)
  --sleep SECS    Pause between runs (default: ${SLEEP_BETWEEN}s)
  --job KEY       Job key to run (default: ${JOB_KEY})
  --python BIN    Python interpreter (default: ${PYTHON_BIN})
  -h, --help      Show this help

Environment:
  APP_ROOT        SupplyCore repository root (default: ${APP_ROOT})
USAGE
}

while (($# > 0)); do
  case "$1" in
    --idle)   IDLE_THRESHOLD=$2; shift 2 ;;
    --max)    MAX_RUNS=$2; shift 2 ;;
    --sleep)  SLEEP_BETWEEN=$2; shift 2 ;;
    --job)    JOB_KEY=$2; shift 2 ;;
    --python) PYTHON_BIN=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ ! -d "${APP_ROOT}/python" ]]; then
  echo "APP_ROOT does not contain python/: ${APP_ROOT}" >&2
  exit 1
fi

# Parse the final JSON line from ``python -m orchestrator.main run-job``'s
# stdout.  ``_print_cli_result`` prints a single-line JSON payload after
# any logging output, so ``tail -n1`` + python json is the cleanest path.
parse_result() {
  local raw=$1 field=$2
  "${PYTHON_BIN}" - "$raw" "$field" <<'PYEOF'
import json, sys
raw, field = sys.argv[1], sys.argv[2]
# Find the last line that parses as a JSON object.
payload = None
for line in reversed(raw.splitlines()):
    line = line.strip()
    if not line or not line.startswith("{"):
        continue
    try:
        payload = json.loads(line)
        break
    except Exception:
        continue
if payload is None:
    print("")
    sys.exit(0)
# Support dotted lookups: "result.new_killmails"
node = payload
for part in field.split("."):
    if isinstance(node, dict):
        node = node.get(part)
    else:
        node = None
        break
print("" if node is None else node)
PYEOF
}

TRAP_SIGNALED=0
trap 'TRAP_SIGNALED=1; echo; echo "Interrupted — stopping after current run."; ' INT TERM

RUN=0
IDLE_RUNS=0
TOTAL_NEW=0
TOTAL_WRITTEN=0
TOTAL_FETCHED=0
START_TS=$(date +%s)

printf '\n== Catch-up loop for %s ==\n' "${JOB_KEY}"
printf 'app_root=%s  python=%s  idle_threshold=%d  max_runs=%s  sleep=%ss\n\n' \
  "${APP_ROOT}" "${PYTHON_BIN}" "${IDLE_THRESHOLD}" \
  "$([[ ${MAX_RUNS} -eq 0 ]] && echo unlimited || echo "${MAX_RUNS}")" \
  "${SLEEP_BETWEEN}"

while :; do
  RUN=$((RUN + 1))
  if [[ ${MAX_RUNS} -gt 0 && ${RUN} -gt ${MAX_RUNS} ]]; then
    printf '[stop] reached --max %d, exiting\n' "${MAX_RUNS}"
    break
  fi

  RUN_STARTED=$(date +%s)
  printf '[run %4d] launching %s ...\n' "${RUN}" "${JOB_KEY}"

  OUTPUT=$(
    cd "${APP_ROOT}/python"
    "${PYTHON_BIN}" -m orchestrator run-job \
      --app-root "${APP_ROOT}" \
      --job-key "${JOB_KEY}" 2>&1
  ) || {
    EXIT=$?
    printf '[run %4d] FAILED (exit=%d)\n' "${RUN}" "${EXIT}"
    printf '%s\n' "${OUTPUT}" | tail -n 20 | sed 's/^/           /'
    if [[ ${TRAP_SIGNALED} -eq 1 ]]; then break; fi
    sleep "${SLEEP_BETWEEN}"
    continue
  }

  RUN_DURATION=$(($(date +%s) - RUN_STARTED))

  STATUS=$(parse_result "${OUTPUT}" "status")
  NEW=$(parse_result "${OUTPUT}" "result.new_killmails")
  WRITTEN=$(parse_result "${OUTPUT}" "result.written")
  FETCHED=$(parse_result "${OUTPUT}" "result.esi_fetched")
  CACHE_HITS=$(parse_result "${OUTPUT}" "result.esi_cache_hits")
  CHARS=$(parse_result "${OUTPUT}" "result.characters_processed")
  MSG=$(parse_result "${OUTPUT}" "result.message")

  NEW=${NEW:-0}
  WRITTEN=${WRITTEN:-0}
  FETCHED=${FETCHED:-0}
  CACHE_HITS=${CACHE_HITS:-0}
  CHARS=${CHARS:-0}

  TOTAL_NEW=$((TOTAL_NEW + NEW))
  TOTAL_WRITTEN=$((TOTAL_WRITTEN + WRITTEN))
  TOTAL_FETCHED=$((TOTAL_FETCHED + FETCHED))

  printf '[run %4d] %-7s  chars=%s  new=%s  written=%s  esi_fetched=%s  cache=%s  (%ds)\n' \
    "${RUN}" "${STATUS:-unknown}" "${CHARS}" "${NEW}" "${WRITTEN}" \
    "${FETCHED}" "${CACHE_HITS}" "${RUN_DURATION}"

  # Stop condition 1: queue drained.
  if [[ -n "${MSG}" && "${MSG}" == *"No characters pending"* ]]; then
    printf '[stop] queue drained ("%s") after %d runs\n' "${MSG}" "${RUN}"
    break
  fi

  # Stop condition 2: N consecutive quiet runs.
  if [[ "${NEW}" == "0" ]]; then
    IDLE_RUNS=$((IDLE_RUNS + 1))
    if [[ ${IDLE_RUNS} -ge ${IDLE_THRESHOLD} ]]; then
      printf '[stop] %d consecutive runs with new_killmails=0, backlog caught up\n' "${IDLE_RUNS}"
      break
    fi
  else
    IDLE_RUNS=0
  fi

  # Stop condition 3: caller hit Ctrl-C.
  if [[ ${TRAP_SIGNALED} -eq 1 ]]; then
    printf '[stop] interrupted by signal after %d runs\n' "${RUN}"
    break
  fi

  if [[ ${SLEEP_BETWEEN} -gt 0 ]]; then
    sleep "${SLEEP_BETWEEN}"
  fi
done

TOTAL_DURATION=$(($(date +%s) - START_TS))
printf '\n== Summary ==\n'
printf 'runs=%d  total_new=%d  total_written=%d  total_esi_fetched=%d  elapsed=%ds\n' \
  "${RUN}" "${TOTAL_NEW}" "${TOTAL_WRITTEN}" "${TOTAL_FETCHED}" "${TOTAL_DURATION}"
