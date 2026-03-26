#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT_DEFAULT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
APP_ROOT=${APP_ROOT:-${APP_ROOT_DEFAULT}}
DRY_RUN=0
VERBOSE=0
REFRESH_DEPS=0
CLEAR_CACHE=0
BRANCH=""

SERVICES=(
  supplycore-sync-worker.service
  supplycore-compute-worker.service
  supplycore-zkill.service
  supplycore-orchestrator.service
)
RESTART_SERVICES=()

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Options:
  --app-root PATH        SupplyCore repository root (default: ${APP_ROOT_DEFAULT})
  --branch NAME          Optional branch to checkout before pull
  --refresh-deps         Run python dependency refresh (pip install --upgrade ./python)
  --clear-cache          Clear runtime cache files under storage/cache
  --service NAME         Add a service to restart (repeatable)
  --dry-run              Print actions without executing mutating commands
  --verbose              Print each command before executing
  -h, --help             Show this help
USAGE
}

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

run_cmd() {
  if [[ ${VERBOSE} -eq 1 || ${DRY_RUN} -eq 1 ]]; then
    log "CMD: $*"
  fi
  if [[ ${DRY_RUN} -eq 1 ]]; then
    return 0
  fi
  "$@"
}

service_exists() {
  local service_name=$1
  if [[ ${DRY_RUN} -eq 1 ]]; then
    return 0
  fi
  if systemctl list-unit-files --type=service --all --no-legend --plain 2>/dev/null | awk '{print $1}' | grep -Fxq "${service_name}"; then
    return 0
  fi
  if systemctl list-units --type=service --all --no-legend --plain 2>/dev/null | awk '{print $1}' | grep -Fxq "${service_name}"; then
    return 0
  fi
  return 1
}

parse_args() {
  while (($# > 0)); do
    case "$1" in
      --app-root)
        APP_ROOT=$2
        shift 2
        ;;
      --branch)
        BRANCH=$2
        shift 2
        ;;
      --refresh-deps)
        REFRESH_DEPS=1
        shift
        ;;
      --clear-cache)
        CLEAR_CACHE=1
        shift
        ;;
      --service)
        SERVICES+=("$2")
        shift 2
        ;;
      --dry-run)
        DRY_RUN=1
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

if [[ ! -d "${APP_ROOT}/.git" ]]; then
  echo "App root is not a git repository: ${APP_ROOT}" >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is required for service restart operations." >&2
  exit 1
fi

log "Starting update-and-restart"
log "app_root=${APP_ROOT} dry_run=${DRY_RUN} refresh_deps=${REFRESH_DEPS} clear_cache=${CLEAR_CACHE}"

cd "${APP_ROOT}"

if [[ -n "${BRANCH}" ]]; then
  run_cmd git checkout "${BRANCH}"
fi

run_cmd git fetch --all --prune
run_cmd git pull --ff-only

if [[ ${REFRESH_DEPS} -eq 1 ]]; then
  if [[ -x "${APP_ROOT}/.venv-orchestrator/bin/python" ]]; then
    run_cmd "${APP_ROOT}/.venv-orchestrator/bin/python" -m pip install --upgrade "${APP_ROOT}/python"
  else
    run_cmd python3 -m pip install --upgrade "${APP_ROOT}/python"
  fi
fi

if [[ ${CLEAR_CACHE} -eq 1 ]]; then
  run_cmd bash -lc "rm -rf '${APP_ROOT}/storage/cache/'*"
fi

run_cmd systemctl daemon-reload

for svc in "${SERVICES[@]}"; do
  if service_exists "${svc}"; then
    RESTART_SERVICES+=("${svc}")
  else
    log "Skipping restart for missing unit: ${svc}"
  fi
done

if [[ ${#RESTART_SERVICES[@]} -eq 0 ]]; then
  log "No configured services found on this host; nothing to restart."
fi

for svc in "${RESTART_SERVICES[@]}"; do
  run_cmd systemctl restart "${svc}"
done

log "Post-restart status checks"
for svc in "${RESTART_SERVICES[@]}"; do
  run_cmd systemctl --no-pager --full status "${svc}"
done

log "Completed update-and-restart"
