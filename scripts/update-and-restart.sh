#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT_DEFAULT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
APP_ROOT=${APP_ROOT:-${APP_ROOT_DEFAULT}}
SYSTEMD_DIR="/etc/systemd/system"
DRY_RUN=0
VERBOSE=0
REFRESH_DEPS=0
CLEAR_CACHE=0
RUN_MIGRATIONS=1
SYNC_UNITS=1
BRANCH=""
EXPLICIT_SERVICES=()

# Services that should always be installed if not already present.
# The orchestrator and legacy worker@ are opt-in only (installed via
# install-services.sh) — they are NOT auto-installed here.
CORE_UNITS=(
  supplycore-sync-worker.service
  supplycore-sync-worker@.service
  supplycore-compute-worker.service
  supplycore-compute-worker@.service
  supplycore-zkill.service
  supplycore-influx-rollup-export.service
  supplycore-influx-rollup-export.timer
)

# Units that are opt-in only — update if already installed, but don't install.
OPTIN_UNITS=(
  supplycore-orchestrator.service
  supplycore-worker@.service
)

# Known stale units that should be stopped, disabled, and removed.
STALE_UNITS=(
  supplycore-php-compute-worker.service
  supplycore-php-compute-worker@.service
)

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
  --no-sync-units        Skip syncing systemd unit files from ops/systemd/
  --dry-run              Print actions without executing mutating commands
  --verbose              Print each command before executing
  -h, --help             Show this help

This script:
  1. Pulls the latest code from git
  2. Syncs systemd unit files from ops/systemd/ to ${SYSTEMD_DIR}
  3. Removes known stale service units
  4. Runs database migrations
  5. Restarts all active supplycore-* services
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
      --no-sync-units)
        SYNC_UNITS=0
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

sync_systemd_units() {
  local src_dir="${APP_ROOT}/ops/systemd"
  if [[ ! -d "${src_dir}" ]]; then
    log "No ops/systemd directory found; skipping unit sync."
    return 0
  fi

  log "Syncing systemd unit files"

  # Always install/update core units
  for unit in "${CORE_UNITS[@]}"; do
    local src="${src_dir}/${unit}"
    local dest="${SYSTEMD_DIR}/${unit}"
    if [[ ! -f "${src}" ]]; then
      continue
    fi
    if [[ -f "${dest}" ]] && cmp -s "${src}" "${dest}"; then
      continue  # already up to date
    fi
    log "Installing ${unit}"
    run_cmd cp "${src}" "${dest}"
  done

  # Update opt-in units only if already installed
  for unit in "${OPTIN_UNITS[@]}"; do
    local src="${src_dir}/${unit}"
    local dest="${SYSTEMD_DIR}/${unit}"
    if [[ ! -f "${dest}" ]]; then
      continue  # not installed, skip
    fi
    if [[ ! -f "${src}" ]]; then
      continue
    fi
    if cmp -s "${src}" "${dest}"; then
      continue  # already up to date
    fi
    log "Updating opt-in unit ${unit}"
    run_cmd cp "${src}" "${dest}"
  done

  # Remove known stale units
  for unit in "${STALE_UNITS[@]}"; do
    local dest="${SYSTEMD_DIR}/${unit}"
    if [[ ! -f "${dest}" ]]; then
      continue
    fi
    log "Removing stale unit ${unit}"
    run_cmd systemctl stop "${unit}" 2>/dev/null || true
    run_cmd systemctl disable "${unit}" 2>/dev/null || true
    run_cmd rm -f "${dest}"
  done

  # Also clean up any stale templated instances of stale units
  for unit in "${STALE_UNITS[@]}"; do
    local base_name="${unit%.service}"
    while IFS= read -r instance; do
      [[ -z "${instance}" ]] && continue
      log "Stopping stale instance ${instance}"
      run_cmd systemctl stop "${instance}" 2>/dev/null || true
      run_cmd systemctl disable "${instance}" 2>/dev/null || true
    done < <(systemctl list-units --type=service --no-legend --plain 2>/dev/null \
      | awk '{print $1}' \
      | grep -E "^${base_name}@" \
      || true)
  done
}

discover_services() {
  # Auto-discover supplycore-* services and timers that are actually loaded.
  # Skips template definitions (@.service) and not-found/masked units.
  local -a discovered=()
  local unit load_state

  while read -r unit load_state _rest; do
    [[ -z "${unit}" ]] && continue
    [[ "${unit}" == *'@.service' ]] && continue
    [[ "${load_state}" == "not-found" ]] && continue
    [[ "${load_state}" == "masked" ]] && continue
    discovered+=("${unit}")
  done < <(systemctl list-units --type=service --no-legend --plain 2>/dev/null \
    | awk '/^supplycore-/ {print $1, $2}' \
    || true)

  while read -r unit load_state _rest; do
    [[ -z "${unit}" ]] && continue
    [[ "${load_state}" == "not-found" ]] && continue
    [[ "${load_state}" == "masked" ]] && continue
    discovered+=("${unit}")
  done < <(systemctl list-units --type=timer --no-legend --plain 2>/dev/null \
    | awk '/^supplycore-/ {print $1, $2}' \
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
log "app_root=${APP_ROOT} dry_run=${DRY_RUN} refresh_deps=${REFRESH_DEPS} clear_cache=${CLEAR_CACHE} run_migrations=${RUN_MIGRATIONS} sync_units=${SYNC_UNITS}"

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

# ------- Sync systemd units -------
if [[ ${SYNC_UNITS} -eq 1 ]]; then
  sync_systemd_units
fi

# ------- Database migrations -------
if [[ ${RUN_MIGRATIONS} -eq 1 ]]; then
  run_migrations
fi

# ------- Service discovery and restart -------
run_cmd systemctl daemon-reload

RESTART_SERVICES=()
if [[ ${#EXPLICIT_SERVICES[@]} -gt 0 ]]; then
  RESTART_SERVICES=("${EXPLICIT_SERVICES[@]}")
  log "Using ${#RESTART_SERVICES[@]} explicitly specified service(s)"
else
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
  run_cmd systemctl restart "${svc}" || log "WARNING: failed to restart ${svc}"
done

log "Post-restart status checks"
for svc in "${RESTART_SERVICES[@]}"; do
  run_cmd systemctl --no-pager --full status "${svc}" || true
done

log "Completed update-and-restart"
