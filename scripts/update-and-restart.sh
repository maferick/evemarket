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
BUILD_ASSETS=0
ANALYZE_TABLES=0
GRACEFUL_STOP=0
HEALTH_CHECK=1
SMART_MODE=0
BRANCH=""
EXPLICIT_SERVICES=()

# Smart-mode bookkeeping (tracked across phases, consumed at end for
# the HOURLY_LOOP_SUMMARY emission so scripts/hourly-loop.sh can adapt).
GIT_SHA_BEFORE=""
GIT_SHA_AFTER=""
GIT_CHANGED=0
UNITS_CHANGED=0
SMART_SKIP_RESTART=0
RESTART_SERVICES=()
FAILED_SERVICES=()
UNHEALTHY_SERVICES=()

# Timeout (seconds) to wait for a service to become active after restart.
HEALTH_CHECK_TIMEOUT=30

# Services that should always be installed if not already present.
# The orchestrator and legacy worker@ are opt-in only (installed via
# install-services.sh) — they are NOT auto-installed here.
#
# The hourly-loop unit files are synced here too, but the timer is NOT
# enabled automatically. Operators enable it with:
#   sudo systemctl enable --now supplycore-hourly-loop.timer
#   sudo systemctl enable --now supplycore-claude-triage.timer
# after reviewing one manual run.
CORE_UNITS=(
  supplycore-loop-runner.service
  supplycore-lane-realtime.service
  supplycore-lane-ingestion.service
  supplycore-lane-compute.service
  supplycore-lane-compute-bg.service
  supplycore-lane-maintenance.service
  supplycore-zkill.service
  supplycore-evewho-runner.service
  supplycore-backfill-runner.service
  supplycore-influx-rollup-export.service
  supplycore-influx-rollup-export.timer
  supplycore-hourly-loop.service
  supplycore-hourly-loop.timer
  supplycore-claude-triage.service
  supplycore-claude-triage.timer
)

# Units that are opt-in only — update if already installed, but don't install.
# Legacy queue-based workers are kept as opt-in so existing installs still get
# unit file updates, but new installs won't auto-install them.
OPTIN_UNITS=(
  supplycore-sync-worker.service
  supplycore-sync-worker@.service
  supplycore-compute-worker.service
  supplycore-compute-worker@.service
  supplycore-ai-worker.service
)

# Known stale units that should be stopped, disabled, and removed.
STALE_UNITS=(
  supplycore-php-compute-worker.service
  supplycore-php-compute-worker@.service
  supplycore-orchestrator.service
  supplycore-worker@.service
)

usage() {
  cat <<USAGE
Usage: $(basename "$0") [options]

Options:
  --app-root PATH        SupplyCore repository root (default: ${APP_ROOT_DEFAULT})
  --branch NAME          Optional branch to checkout before pull
  --refresh-deps         Run python dependency refresh (pip install --upgrade ./python)
  --build-assets         Rebuild Tailwind CSS assets (npm run build:css)
  --clear-cache          Clear runtime cache files under storage/cache
  --analyze-tables       Run ANALYZE TABLE PERSISTENT FOR ALL after migrations
  --graceful-stop        Stop services gracefully before pulling (avoids mid-job restarts)
  --service NAME         Restart only these services (repeatable; overrides auto-discovery)
  --no-migrations        Skip running database migrations
  --no-sync-units        Skip syncing systemd unit files from ops/systemd/
  --no-health-check      Skip post-restart health verification
  --smart                Smart mode: if neither the git HEAD SHA nor any
                         systemd unit file changed during this run, skip
                         migrations, daemon-reload, and service restarts.
                         Used by scripts/hourly-loop.sh to avoid hourly
                         restart churn when there is nothing to deploy.
  --dry-run              Print actions without executing mutating commands
  --verbose              Print each command before executing
  -h, --help             Show this help

This script:
  1. Optionally stops services gracefully (--graceful-stop)
  2. Pulls the latest code from git (stashing local changes if needed)
  3. Optionally refreshes Python dependencies and rebuilds CSS assets
  4. Syncs systemd unit files from ops/systemd/ to ${SYSTEMD_DIR}
  5. Removes known stale service units
  6. Runs database migrations
  7. Restarts all active supplycore-* services
  8. Verifies services are healthy after restart
USAGE
}

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

log_warn() {
  printf '[%s] WARNING: %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
}

log_error() {
  printf '[%s] ERROR: %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >&2
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
      --build-assets)
        BUILD_ASSETS=1
        shift
        ;;
      --clear-cache)
        CLEAR_CACHE=1
        shift
        ;;
      --analyze-tables)
        ANALYZE_TABLES=1
        shift
        ;;
      --graceful-stop)
        GRACEFUL_STOP=1
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
      --no-health-check)
        HEALTH_CHECK=0
        shift
        ;;
      --smart)
        SMART_MODE=1
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

preflight_checks() {
  if [[ ! -d "${APP_ROOT}/.git" ]]; then
    log_error "App root is not a git repository: ${APP_ROOT}"
    exit 1
  fi

  if ! command -v systemctl >/dev/null 2>&1; then
    log_error "systemctl is required for service restart operations."
    exit 1
  fi

  # Warn if not running as root (systemctl operations will fail)
  if [[ ${DRY_RUN} -eq 0 && $(id -u) -ne 0 ]]; then
    log_warn "Not running as root. Systemd operations may fail; consider: sudo $0 $*"
  fi
}

git_update() {
  cd "${APP_ROOT}"

  GIT_SHA_BEFORE=$(git rev-parse HEAD 2>/dev/null || echo "")

  # Stash any uncommitted changes before pull to avoid failures
  local stashed=0
  if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
    log "Stashing uncommitted local changes"
    run_cmd git stash push -m "update-and-restart auto-stash $(date -u +'%Y%m%dT%H%M%SZ')"
    stashed=1
  fi

  if [[ -n "${BRANCH}" ]]; then
    run_cmd git checkout "${BRANCH}"
  fi

  run_cmd git fetch --all --prune

  # Only attempt to pull if the current branch has an upstream configured.
  # Local-only branches (common during dev) have no tracking ref and would
  # fail both ff-only and merge pulls — skip cleanly instead.
  local current_branch
  current_branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")
  if git rev-parse --abbrev-ref "@{u}" >/dev/null 2>&1; then
    if ! run_cmd git pull --ff-only 2>/dev/null; then
      log_warn "Fast-forward pull failed (branch may have diverged). Attempting merge pull."
      run_cmd git pull --no-edit
    fi
  else
    log_warn "Current branch '${current_branch}' has no upstream; skipping git pull."
  fi

  # Restore stashed changes
  if [[ ${stashed} -eq 1 ]]; then
    log "Restoring stashed local changes"
    run_cmd git stash pop || log_warn "Could not restore stash cleanly. Check 'git stash list'."
  fi

  GIT_SHA_AFTER=$(git rev-parse HEAD 2>/dev/null || echo "")
  if [[ -n "${GIT_SHA_BEFORE}" && -n "${GIT_SHA_AFTER}" && "${GIT_SHA_BEFORE}" != "${GIT_SHA_AFTER}" ]]; then
    GIT_CHANGED=1
    log "Git HEAD changed: ${GIT_SHA_BEFORE:0:8} -> ${GIT_SHA_AFTER:0:8}"
  else
    log "Git HEAD unchanged at ${GIT_SHA_AFTER:0:8}"
  fi
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
    UNITS_CHANGED=1
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
    UNITS_CHANGED=1
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
    UNITS_CHANGED=1
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

  # Collect timer-triggered service names so we skip them (only restart the timer)
  local -a timer_triggered=()
  local triggers_line
  while read -r timer_unit _rest; do
    [[ -z "${timer_unit}" ]] && continue
    triggers_line=$(systemctl show -p Triggers --value "${timer_unit}" 2>/dev/null || true)
    if [[ -n "${triggers_line}" ]]; then
      timer_triggered+=("${triggers_line}")
    fi
  done < <(systemctl list-units --type=timer --no-legend --plain 2>/dev/null \
    | awk '/^supplycore-/ {print $1}' \
    || true)

  # Identify the service we're currently running under (if any) so we
  # never try to restart our own invoking unit. systemd sets INVOCATION_ID
  # for any process launched from a unit, and `systemctl status $PID`
  # resolves that PID back to the owning unit name.
  local self_unit=""
  if [[ -n "${INVOCATION_ID:-}" ]]; then
    self_unit=$(systemctl status "$$" --no-pager 2>/dev/null \
      | awk 'NR==1 && /\.service/ {for (i=1; i<=NF; i++) if ($i ~ /\.service$/) {print $i; exit}}' \
      || true)
  fi

  while read -r unit load_state _rest; do
    [[ -z "${unit}" ]] && continue
    [[ "${unit}" == *'@.service' ]] && continue
    [[ "${load_state}" == "not-found" ]] && continue
    [[ "${load_state}" == "masked" ]] && continue
    # Skip oneshot services — they are run-to-completion by design and
    # are typically driven by a timer. Restarting a oneshot while it is
    # still activating causes systemd to fail the job (classic case:
    # the hourly-loop service restarting itself from within its own
    # ExecStart). The timer entry is still restarted below, which is
    # the correct handle for rescheduling oneshots.
    local svc_type
    svc_type=$(systemctl show -p Type --value "${unit}" 2>/dev/null || true)
    if [[ "${svc_type}" == "oneshot" ]]; then
      continue
    fi
    # Belt-and-suspenders: never restart the unit that is currently
    # running this script (applies to non-oneshot self-invocations too).
    if [[ -n "${self_unit}" && "${unit}" == "${self_unit}" ]]; then
      continue
    fi
    # Skip services that are triggered by timers — we restart the timer instead
    local is_timer_triggered=false
    for tt in "${timer_triggered[@]+"${timer_triggered[@]}"}"; do
      if [[ "${tt}" == "${unit}" ]]; then
        is_timer_triggered=true
        break
      fi
    done
    if [[ ${is_timer_triggered} == true ]]; then
      continue
    fi
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
    log_warn "php not found on PATH; skipping database migrations."
    return 0
  fi

  log "Running database migrations via PHP"
  if ! run_cmd "${php_bin}" "${APP_ROOT}/bin/run-migrations.php"; then
    log_error "Database migrations failed! Services will NOT be restarted to avoid schema mismatch."
    log_error "Fix migration errors, then re-run this script."
    exit 1
  fi
}

run_analyze_tables() {
  local php_bin
  php_bin=$(command -v php 2>/dev/null || true)
  if [[ -z "${php_bin}" ]]; then
    log_warn "php not found; skipping ANALYZE TABLE."
    return 0
  fi

  log "Running ANALYZE TABLE PERSISTENT FOR ALL on key tables"
  # Use the migration SQL directly — it contains all ANALYZE TABLE statements
  local analyze_sql="${APP_ROOT}/database/migrations/20260413_mariadb_performance_tuning.sql"
  if [[ -f "${analyze_sql}" ]]; then
    local mysql_bin
    mysql_bin=$(command -v mysql 2>/dev/null || true)
    if [[ -n "${mysql_bin}" ]]; then
      # Extract just the ANALYZE TABLE lines and run them
      run_cmd bash -c "grep -i '^ANALYZE TABLE' '${analyze_sql}' | ${mysql_bin} --defaults-file=/etc/mysql/debian.cnf supplycore 2>/dev/null || ${mysql_bin} supplycore"
    else
      log_warn "mysql client not found; skipping ANALYZE TABLE."
    fi
  else
    log_warn "ANALYZE TABLE migration not found at ${analyze_sql}; skipping."
  fi
}

build_assets() {
  local npm_bin
  npm_bin=$(command -v npm 2>/dev/null || true)
  if [[ -z "${npm_bin}" ]]; then
    log_warn "npm not found on PATH; skipping asset build."
    return 0
  fi

  if [[ ! -f "${APP_ROOT}/package.json" ]]; then
    log "No package.json found; skipping asset build."
    return 0
  fi

  cd "${APP_ROOT}"

  # Install deps if node_modules is missing
  if [[ ! -d "${APP_ROOT}/node_modules" ]]; then
    log "Installing npm dependencies"
    run_cmd "${npm_bin}" ci --no-audit --no-fund 2>/dev/null || run_cmd "${npm_bin}" install --no-audit --no-fund
  fi

  log "Building Tailwind CSS assets"
  run_cmd "${npm_bin}" run build:css
}

stop_services_gracefully() {
  log "Stopping services gracefully before update"
  local -a services_to_stop=()

  if [[ ${#EXPLICIT_SERVICES[@]} -gt 0 ]]; then
    services_to_stop=("${EXPLICIT_SERVICES[@]}")
  else
    while IFS= read -r svc; do
      [[ -n "${svc}" ]] && services_to_stop+=("${svc}")
    done < <(discover_services)
  fi

  for svc in "${services_to_stop[@]}"; do
    log "Stopping ${svc}"
    run_cmd systemctl stop "${svc}" 2>/dev/null || log_warn "Could not stop ${svc}"
  done
}

verify_service_health() {
  local svc=$1
  local timeout=${HEALTH_CHECK_TIMEOUT}

  # Timers just need to be active (loaded), not "running"
  if [[ "${svc}" == *.timer ]]; then
    if systemctl is-active --quiet "${svc}" 2>/dev/null; then
      return 0
    fi
    return 1
  fi

  # For oneshot services, check they didn't fail
  local svc_type
  svc_type=$(systemctl show -p Type --value "${svc}" 2>/dev/null || true)
  if [[ "${svc_type}" == "oneshot" ]]; then
    local result
    result=$(systemctl show -p Result --value "${svc}" 2>/dev/null || true)
    [[ "${result}" == "success" || "${result}" == "" ]]
    return $?
  fi

  # For long-running services, wait up to timeout for active state
  local elapsed=0
  while [[ ${elapsed} -lt ${timeout} ]]; do
    if systemctl is-active --quiet "${svc}" 2>/dev/null; then
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done

  return 1
}

# ------------------------------------------------------------------
parse_args "$@"
preflight_checks

log "Starting update-and-restart"
log "app_root=${APP_ROOT} dry_run=${DRY_RUN} refresh_deps=${REFRESH_DEPS} build_assets=${BUILD_ASSETS} clear_cache=${CLEAR_CACHE} run_migrations=${RUN_MIGRATIONS} sync_units=${SYNC_UNITS} graceful_stop=${GRACEFUL_STOP} analyze_tables=${ANALYZE_TABLES}"

# ------- Graceful stop (optional) -------
if [[ ${GRACEFUL_STOP} -eq 1 ]]; then
  stop_services_gracefully
fi

# ------- Git update -------
git_update

# ------- Python dependencies -------
if [[ ${REFRESH_DEPS} -eq 1 ]]; then
  log "Refreshing Python dependencies"
  if [[ -x "${APP_ROOT}/.venv-orchestrator/bin/python" ]]; then
    run_cmd "${APP_ROOT}/.venv-orchestrator/bin/python" -m pip install --upgrade "${APP_ROOT}/python"
  elif command -v python3 >/dev/null 2>&1; then
    run_cmd python3 -m pip install --upgrade "${APP_ROOT}/python"
  else
    log_warn "No python3 found; skipping dependency refresh."
  fi
fi

# ------- Build CSS assets -------
if [[ ${BUILD_ASSETS} -eq 1 ]]; then
  build_assets
fi

# ------- Cache -------
if [[ ${CLEAR_CACHE} -eq 1 ]]; then
  log "Clearing runtime cache"
  run_cmd rm -rf "${APP_ROOT}/storage/cache/"*
fi

# ------- Storage permissions -------
fix_storage_permissions() {
  local storage_dir="${APP_ROOT}/storage"
  if [[ ! -d "${storage_dir}" ]]; then
    log "No storage directory found at ${storage_dir}; skipping permission fix."
    return 0
  fi
  log "Fixing storage ownership (www-data:www-data)"
  run_cmd chown -R www-data:www-data "${storage_dir}"
}
fix_storage_permissions

# ------- Sync systemd units -------
if [[ ${SYNC_UNITS} -eq 1 ]]; then
  sync_systemd_units
fi

# ------- Smart mode: decide whether to skip migrations + restart -------
if [[ ${SMART_MODE} -eq 1 && ${GIT_CHANGED} -eq 0 && ${UNITS_CHANGED} -eq 0 ]]; then
  SMART_SKIP_RESTART=1
  log "Smart mode: git HEAD unchanged and no systemd unit file updates — skipping migrations, daemon-reload, and service restarts."
fi

if [[ ${SMART_SKIP_RESTART} -eq 0 ]]; then
  # ------- Database migrations -------
  if [[ ${RUN_MIGRATIONS} -eq 1 ]]; then
    run_migrations
  fi

  # ------- ANALYZE TABLE (optional) -------
  if [[ ${ANALYZE_TABLES} -eq 1 ]]; then
    run_analyze_tables
  fi

  # ------- Reset overdue job schedules -------
  # After code update + migrations, reset any overdue next_due_at values so
  # the loop runner picks them up immediately when services restart.
  reset_overdue_jobs() {
    local python_bin=""
    if [[ -x "${APP_ROOT}/.venv-orchestrator/bin/python" ]]; then
      python_bin="${APP_ROOT}/.venv-orchestrator/bin/python"
    elif command -v python3 >/dev/null 2>&1; then
      python_bin="python3"
    fi
    if [[ -z "${python_bin}" ]]; then
      log_warn "No python3 found; skipping overdue job reset."
      return 0
    fi
    local reset_script="${APP_ROOT}/bin/reset_overdue_jobs.py"
    if [[ ! -f "${reset_script}" ]]; then
      log "No reset_overdue_jobs.py found; skipping."
      return 0
    fi
    log "Resetting overdue job schedules"
    run_cmd "${python_bin}" "${reset_script}" --app-root "${APP_ROOT}"
  }
  reset_overdue_jobs

  # ------- Service discovery and restart -------
  run_cmd systemctl daemon-reload

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
    if ! run_cmd systemctl restart "${svc}"; then
      log_warn "Failed to restart ${svc}"
      FAILED_SERVICES+=("${svc}")
    fi
  done

  # ------- Health verification -------
  if [[ ${HEALTH_CHECK} -eq 1 && ${#RESTART_SERVICES[@]} -gt 0 && ${DRY_RUN} -eq 0 ]]; then
    log "Verifying service health (timeout: ${HEALTH_CHECK_TIMEOUT}s per service)"
    # Short initial settle time
    sleep 2

    for svc in "${RESTART_SERVICES[@]}"; do
      # Skip already-failed services
      skip=false
      for failed in "${FAILED_SERVICES[@]+"${FAILED_SERVICES[@]}"}"; do
        if [[ "${failed}" == "${svc}" ]]; then
          skip=true
          break
        fi
      done
      [[ ${skip} == true ]] && continue

      if verify_service_health "${svc}"; then
        log "  OK: ${svc}"
      else
        log_warn "  UNHEALTHY: ${svc}"
        UNHEALTHY_SERVICES+=("${svc}")
      fi
    done

    if [[ ${#UNHEALTHY_SERVICES[@]} -gt 0 || ${#FAILED_SERVICES[@]} -gt 0 ]]; then
      log_warn "Some services are unhealthy after restart:"
      for svc in "${FAILED_SERVICES[@]+"${FAILED_SERVICES[@]}"}"; do
        log_warn "  FAILED:    ${svc}"
      done
      for svc in "${UNHEALTHY_SERVICES[@]+"${UNHEALTHY_SERVICES[@]}"}"; do
        log_warn "  UNHEALTHY: ${svc}"
        # Print the last few journal lines to aid debugging
        journalctl -u "${svc}" --no-pager -n 10 --since "-2 min" 2>/dev/null || true
      done
    fi
  else
    # Fallback: dump status for manual review
    for svc in "${RESTART_SERVICES[@]}"; do
      run_cmd systemctl --no-pager --full status "${svc}" || true
    done
  fi
fi

TOTAL_FAILED=$(( ${#FAILED_SERVICES[@]} + ${#UNHEALTHY_SERVICES[@]} ))

# ------- Machine-readable summary for scripts/hourly-loop.sh -------
# Emitted unconditionally so the hourly wrapper can extract it with a grep.
# Format: HOURLY_LOOP_SUMMARY: {JSON}
_bool_json() { [[ ${1} -eq 1 ]] && printf true || printf false; }
printf 'HOURLY_LOOP_SUMMARY: {"git_sha_before":"%s","git_sha_after":"%s","git_changed":%s,"units_changed":%s,"smart_mode":%s,"smart_skip_restart":%s,"services_restarted":%d,"services_failed":%d,"services_unhealthy":%d,"total_failed":%d,"branch":"%s"}\n' \
  "${GIT_SHA_BEFORE:0:40}" \
  "${GIT_SHA_AFTER:0:40}" \
  "$(_bool_json ${GIT_CHANGED})" \
  "$(_bool_json ${UNITS_CHANGED})" \
  "$(_bool_json ${SMART_MODE})" \
  "$(_bool_json ${SMART_SKIP_RESTART})" \
  "${#RESTART_SERVICES[@]}" \
  "${#FAILED_SERVICES[@]}" \
  "${#UNHEALTHY_SERVICES[@]}" \
  "${TOTAL_FAILED}" \
  "${BRANCH}"

if [[ ${TOTAL_FAILED} -gt 0 ]]; then
  log_warn "Completed update-and-restart with ${TOTAL_FAILED} issue(s)"
  exit 1
fi

log "Completed update-and-restart successfully"
