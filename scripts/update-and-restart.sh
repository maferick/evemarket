#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT_DEFAULT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
APP_ROOT=${APP_ROOT:-${APP_ROOT_DEFAULT}}
DRY_RUN=0
VERBOSE=0
REFRESH_DEPS=0
CLEAR_CACHE=0
RUN_MIGRATIONS=1
BRANCH=""
EXPLICIT_SERVICES=()

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Options:
  --app-root PATH        SupplyCore repository root (default: ${APP_ROOT_DEFAULT})
  --branch NAME          Optional branch to checkout before pull
  --refresh-deps         Run python dependency refresh (pip install --upgrade ./python)
  --clear-cache          Clear runtime cache files under storage/cache
  --service NAME         Restart only these services (repeatable; overrides auto-discovery)
  --no-migrations        Skip running database migrations
  --dry-run              Print actions without executing mutating commands
  --verbose              Print each command before executing
  -h, --help             Show this help

When no --service flags are given the script auto-discovers all active
supplycore-* systemd units (including templated instances like
supplycore-sync-worker@1.service) and restarts them.
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
        EXPLICIT_SERVICES+=("$2")
        shift 2
        ;;
      --no-migrations)
        RUN_MIGRATIONS=0
        shift
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

discover_services() {
  # Auto-discover all active/enabled supplycore-* services and timers.
  # This catches both plain units (supplycore-sync-worker.service) and
  # templated instances (supplycore-sync-worker@1.service).
  local -a discovered=()
  local unit

  while IFS= read -r unit; do
    [[ -n "${unit}" ]] && discovered+=("${unit}")
  done < <(systemctl list-units --type=service --all --no-legend --plain 2>/dev/null \
    | awk '{print $1}' \
    | grep -E '^supplycore-' \
    | grep -v '@\.service$' \
    || true)

  # Also discover active timers
  while IFS= read -r unit; do
    [[ -n "${unit}" ]] && discovered+=("${unit}")
  done < <(systemctl list-units --type=timer --all --no-legend --plain 2>/dev/null \
    | awk '{print $1}' \
    | grep -E '^supplycore-' \
    || true)

  printf '%s\n' "${discovered[@]}"
}

run_migrations() {
  local migrations_dir="${APP_ROOT}/database/migrations"
  if [[ ! -d "${migrations_dir}" ]]; then
    log "No migrations directory found at ${migrations_dir}; skipping."
    return 0
  fi

  local php_bin
  php_bin=$(command -v php 2>/dev/null || true)
  if [[ -z "${php_bin}" ]]; then
    log "php not found on PATH; skipping database migrations."
    return 0
  fi

  log "Running database migrations via PHP"
  run_cmd "${php_bin}" "${APP_ROOT}/bin/run-migrations.php"
}

# ------------------------------------------------------------------
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
log "app_root=${APP_ROOT} dry_run=${DRY_RUN} refresh_deps=${REFRESH_DEPS} clear_cache=${CLEAR_CACHE} run_migrations=${RUN_MIGRATIONS}"

cd "${APP_ROOT}"

# ------- Git update -------
if [[ -n "${BRANCH}" ]]; then
  run_cmd git checkout "${BRANCH}"
fi

run_cmd git fetch --all --prune
run_cmd git pull --ff-only

# ------- Python dependencies -------
if [[ ${REFRESH_DEPS} -eq 1 ]]; then
  if [[ -x "${APP_ROOT}/.venv-orchestrator/bin/python" ]]; then
    run_cmd "${APP_ROOT}/.venv-orchestrator/bin/python" -m pip install --upgrade "${APP_ROOT}/python"
  else
    run_cmd python3 -m pip install --upgrade "${APP_ROOT}/python"
  fi
fi

# ------- Cache -------
if [[ ${CLEAR_CACHE} -eq 1 ]]; then
  run_cmd bash -lc "rm -rf '${APP_ROOT}/storage/cache/'*"
fi

# ------- Database migrations -------
if [[ ${RUN_MIGRATIONS} -eq 1 ]]; then
  run_migrations
fi

# ------- Service discovery and restart -------
run_cmd systemctl daemon-reload

RESTART_SERVICES=()
if [[ ${#EXPLICIT_SERVICES[@]} -gt 0 ]]; then
  # User explicitly specified services — use only those
  RESTART_SERVICES=("${EXPLICIT_SERVICES[@]}")
  log "Using ${#RESTART_SERVICES[@]} explicitly specified service(s)"
else
  # Auto-discover all active supplycore-* units
  while IFS= read -r svc; do
    [[ -n "${svc}" ]] && RESTART_SERVICES+=("${svc}")
  done < <(discover_services)
  log "Auto-discovered ${#RESTART_SERVICES[@]} supplycore service(s)"
fi

if [[ ${#RESTART_SERVICES[@]} -eq 0 ]]; then
  log "No services found to restart."
fi

for svc in "${RESTART_SERVICES[@]}"; do
  log "Restarting ${svc}"
  run_cmd systemctl restart "${svc}"
done

log "Post-restart status checks"
for svc in "${RESTART_SERVICES[@]}"; do
  run_cmd systemctl --no-pager --full status "${svc}" || true
done

log "Completed update-and-restart"
