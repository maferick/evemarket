#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT_DEFAULT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
APP_ROOT=${APP_ROOT:-${APP_ROOT_DEFAULT}}
PYTHON_BIN=${PYTHON_BIN:-python3}
ALLOW_LIVE=0
VERBOSE=0

SYNC_JOBS=(
  market_hub_current_sync
  alliance_current_sync
  market_hub_historical_sync
  alliance_historical_sync
  current_state_refresh_sync
  market_hub_local_history_sync
  doctrine_intelligence_sync
  market_comparison_summary_sync
  loss_demand_summary_sync
  dashboard_summary_sync
  forecasting_ai_sync
  analytics_bucket_1h_sync
  analytics_bucket_1d_sync
  deal_alerts_sync
)

# Jobs that have no safe dry-run interface today.
NO_DRY_RUN_JOBS=("${SYNC_JOBS[@]}")

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Options:
  --app-root PATH        SupplyCore repository root (default: ${APP_ROOT_DEFAULT})
  --python-bin PATH      Python interpreter to use (default: python3)
  --allow-live           Execute jobs without dry-run support (unsafe in prod)
  --verbose              Stream command output while running
  -h, --help             Show this help
USAGE
}

contains_job() {
  local needle=$1
  shift
  for candidate in "$@"; do
    [[ "$candidate" == "$needle" ]] && return 0
  done
  return 1
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --app-root)
        APP_ROOT=$2
        shift 2
        ;;
      --python-bin)
        PYTHON_BIN=$2
        shift 2
        ;;
      --allow-live)
        ALLOW_LIVE=1
        shift
        ;;
      --verbose)
        VERBOSE=1
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        usage
        exit 2
        ;;
    esac
  done
}

parse_args "$@"

if [[ ! -d "${APP_ROOT}" ]]; then
  echo "App root does not exist: ${APP_ROOT}" >&2
  exit 1
fi

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python interpreter not found: ${PYTHON_BIN}" >&2
  exit 1
fi

START_TS=$(date +%s)
printf "\n== SupplyCore sync job test runner ==\n"
printf "App root: %s\n" "${APP_ROOT}"
printf "Python:   %s\n" "${PYTHON_BIN}"
printf "Mode:     %s\n\n" "$([[ ${ALLOW_LIVE} -eq 1 ]] && echo 'live-allowed' || echo 'dry-run-only')"

declare -a SUMMARY=()
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

run_job() {
  local job_key=$1
  local started ended duration exit_code tmpfile
  tmpfile=$(mktemp)
  started=$(date +%s)

  if contains_job "${job_key}" "${NO_DRY_RUN_JOBS[@]}" && [[ ${ALLOW_LIVE} -ne 1 ]]; then
    echo "[SKIP] ${job_key} (no dry-run support; rerun with --allow-live to execute)"
    SUMMARY+=("${job_key}|SKIPPED|0|SKIPPED|dry-run unsupported")
    SKIP_COUNT=$((SKIP_COUNT + 1))
    rm -f "${tmpfile}"
    return
  fi

  local cmd=("${PYTHON_BIN}" -m orchestrator.main run-job --app-root "${APP_ROOT}" --job-key "${job_key}")
  echo "[RUN ] ${job_key}"
  if [[ ${VERBOSE} -eq 1 ]]; then
    (
      cd "${APP_ROOT}/python"
      "${cmd[@]}"
    ) | tee "${tmpfile}"
    exit_code=${PIPESTATUS[0]}
  else
    (
      cd "${APP_ROOT}/python"
      "${cmd[@]}"
    ) >"${tmpfile}" 2>&1 || exit_code=$?
    : "${exit_code:=0}"
  fi

  ended=$(date +%s)
  duration=$((ended - started))

  if [[ ${exit_code} -eq 0 ]]; then
    echo "[PASS] ${job_key} (${duration}s, exit=${exit_code})"
    SUMMARY+=("${job_key}|PASS|${duration}|${exit_code}|ok")
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    echo "[FAIL] ${job_key} (${duration}s, exit=${exit_code})"
    tail -n 40 "${tmpfile}" | sed 's/^/       /'
    SUMMARY+=("${job_key}|FAIL|${duration}|${exit_code}|command failed")
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi

  rm -f "${tmpfile}"
}

for job in "${SYNC_JOBS[@]}"; do
  run_job "${job}"
done

TOTAL_DURATION=$(( $(date +%s) - START_TS ))
printf "\n== Summary ==\n"
printf "%-34s | %-7s | %-10s | %-8s | %s\n" "job_key" "status" "duration_s" "exit" "notes"
printf -- "%.0s-" {1..95}
printf "\n"
for row in "${SUMMARY[@]}"; do
  IFS='|' read -r job status duration exit_code notes <<<"${row}"
  printf "%-34s | %-7s | %-10s | %-8s | %s\n" "${job}" "${status}" "${duration}" "${exit_code}" "${notes}"
done

printf "\nTotals: pass=%d fail=%d skipped=%d total_duration=%ss\n" "${PASS_COUNT}" "${FAIL_COUNT}" "${SKIP_COUNT}" "${TOTAL_DURATION}"

if [[ ${FAIL_COUNT} -gt 0 ]]; then
  exit 1
fi
