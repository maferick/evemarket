#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
APP_ROOT_DEFAULT=$(cd -- "${SCRIPT_DIR}/.." && pwd)
REPO_ROOT="${APP_ROOT_DEFAULT}"

if [[ ${EUID} -ne 0 ]]; then
  echo "This script must be run as root so it can write systemd units and manage services." >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "systemctl is required to install SupplyCore services." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Known stale units — stopped, disabled, and removed during installation.
# ---------------------------------------------------------------------------
STALE_UNITS=(
  supplycore-php-compute-worker.service
  supplycore-php-compute-worker@.service
  supplycore-orchestrator.service
  supplycore-worker@.service
)

# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------

prompt() {
  local message=$1
  local default=${2-}
  local value
  if [[ -n ${default} ]]; then
    read -r -p "${message} [${default}]: " value
    printf '%s\n' "${value:-$default}"
  else
    read -r -p "${message}: " value
    printf '%s\n' "${value}"
  fi
}

prompt_non_empty() {
  local message=$1
  local default=${2-}
  local value
  while true; do
    value=$(prompt "${message}" "${default}")
    if [[ -n ${value} ]]; then
      printf '%s\n' "${value}"
      return 0
    fi
    echo "A value is required." >&2
  done
}

prompt_secret() {
  local message=$1
  local confirm=${2:-false}
  local value
  local verify

  while true; do
    read -r -s -p "${message}: " value
    echo
    if [[ ${confirm} == true ]]; then
      read -r -s -p "Confirm ${message,,}: " verify
      echo
      if [[ ${value} != ${verify} ]]; then
        echo "Values did not match. Please try again." >&2
        continue
      fi
    fi
    printf '%s\n' "${value}"
    return 0
  done
}

prompt_yes_no() {
  local message=$1
  local default=${2:-Y}
  local reply
  local suffix="[y/N]"
  if [[ ${default^^} == Y ]]; then
    suffix="[Y/n]"
  fi

  while true; do
    read -r -p "${message} ${suffix}: " reply
    reply=${reply:-$default}
    case ${reply,,} in
      y|yes) return 0 ;;
      n|no) return 1 ;;
    esac
    echo "Please answer yes or no." >&2
  done
}

prompt_number() {
  local message=$1
  local default=${2:-1}
  local value
  while true; do
    value=$(prompt "${message}" "${default}")
    if [[ ${value} =~ ^[0-9]+$ ]]; then
      printf '%s\n' "${value}"
      return 0
    fi
    echo "Please enter a whole number." >&2
  done
}

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[\\&|]/\\&/g'
}

escape_php_single_quoted() {
  printf '%s' "$1" | sed -e "s/'/\\\\'/g"
}

# ---------------------------------------------------------------------------
# PHP local config
# ---------------------------------------------------------------------------

configure_local_php() {
  local local_config_path=$1
  local app_env=$2
  local app_base_url=$3
  local app_timezone=$4
  local db_host=$5
  local db_port=$6
  local db_name=$7
  local db_username=$8
  local db_password=$9
  local update_config=${10}

  if [[ ${update_config} != true ]]; then
    if [[ -f ${local_config_path} ]]; then
      echo "Keeping existing PHP config at ${local_config_path}"
    else
      echo "Skipping PHP config creation for ${local_config_path}"
    fi
    return 0
  fi

  local local_config_dir
  local app_env_escaped app_base_url_escaped app_timezone_escaped db_host_escaped db_name_escaped db_username_escaped db_password_escaped

  local_config_dir=$(dirname "${local_config_path}")
  install -d -m 0755 "${local_config_dir}"

  app_env_escaped=$(escape_php_single_quoted "${app_env}")
  app_base_url_escaped=$(escape_php_single_quoted "${app_base_url}")
  app_timezone_escaped=$(escape_php_single_quoted "${app_timezone}")
  db_host_escaped=$(escape_php_single_quoted "${db_host}")
  db_name_escaped=$(escape_php_single_quoted "${db_name}")
  db_username_escaped=$(escape_php_single_quoted "${db_username}")
  db_password_escaped=$(escape_php_single_quoted "${db_password}")

  cat > "${local_config_path}" <<PHP
<?php

declare(strict_types=1);

return [
    'app' => [
        'env' => '${app_env_escaped}',
        'base_url' => '${app_base_url_escaped}',
        'timezone' => '${app_timezone_escaped}',
    ],
    'db' => [
        'host' => '${db_host_escaped}',
        'port' => ${db_port},
        'database' => '${db_name_escaped}',
        'username' => '${db_username_escaped}',
        'password' => '${db_password_escaped}',
    ],
];
PHP

  chmod 0640 "${local_config_path}"
  chown "${RUN_USER}:${RUN_GROUP}" "${local_config_path}"
  echo "Wrote PHP config to ${local_config_path}"
}

# ---------------------------------------------------------------------------
# Systemd unit rendering
# ---------------------------------------------------------------------------

render_unit() {
  local template=$1
  local destination=$2
  local app_root_escaped user_escaped group_escaped venv_escaped env_file_escaped
  app_root_escaped=$(escape_sed_replacement "${APP_ROOT}")
  user_escaped=$(escape_sed_replacement "${RUN_USER}")
  group_escaped=$(escape_sed_replacement "${RUN_GROUP}")
  venv_escaped=$(escape_sed_replacement "${VENV_PATH}")
  env_file_escaped=$(escape_sed_replacement "${ENV_FILE}")

  sed \
    -e "s|/var/www/SupplyCore/.venv-orchestrator|${venv_escaped}|g" \
    -e "s|/var/www/SupplyCore|${app_root_escaped}|g" \
    -e "s|/etc/default/supplycore-worker|${env_file_escaped}|g" \
    -e "s|/etc/default/supplycore-influx-rollup-export|${env_file_escaped}|g" \
    -e "s|User=root|User=${user_escaped}|" \
    -e "s|Group=root|Group=${group_escaped}|" \
    "${template}" > "${destination}"
}

resolve_unit_template() {
  local requested_template=$1
  local fallback_template=${2-}

  if [[ -f ${requested_template} ]]; then
    printf '%s\n' "${requested_template}"
    return 0
  fi

  if [[ -n ${fallback_template} && -f ${fallback_template} ]]; then
    echo "Warning: missing unit template ${requested_template}; generating it from ${fallback_template}" >&2
    printf '%s\n' "${fallback_template}"
    return 0
  fi

  echo "Missing required unit template: ${requested_template}" >&2
  if [[ -n ${fallback_template} ]]; then
    echo "Fallback template was also unavailable: ${fallback_template}" >&2
  fi
  exit 1
}

render_sync_instance_unit() {
  local template
  local destination=$1

  template=$(resolve_unit_template \
    "${REPO_ROOT}/ops/systemd/supplycore-sync-worker@.service" \
    "${REPO_ROOT}/ops/systemd/supplycore-sync-worker.service")

  render_unit "${template}" "${destination}"
  if [[ ${template} != *"@.service" ]]; then
    sed -i \
      -e 's|Description=SupplyCore sync worker$|Description=SupplyCore sync worker %i|' \
      -e 's|--worker-id %H-sync --queues|--worker-id %H-sync-%i --queues|' \
      "${destination}"
  fi
}

render_compute_instance_unit() {
  local template
  local destination=$1

  template=$(resolve_unit_template \
    "${REPO_ROOT}/ops/systemd/supplycore-compute-worker@.service" \
    "${REPO_ROOT}/ops/systemd/supplycore-compute-worker.service")

  render_unit "${template}" "${destination}"
  if [[ ${template} != *"@.service" ]]; then
    sed -i \
      -e 's|Description=SupplyCore compute worker$|Description=SupplyCore compute worker %i|' \
      -e 's|--worker-id %H-compute --queues|--worker-id %H-compute-%i --queues|' \
      "${destination}"
  fi
}

# ---------------------------------------------------------------------------
# Stale unit cleanup
# ---------------------------------------------------------------------------

cleanup_stale_units() {
  local unit dest base_name
  for unit in "${STALE_UNITS[@]}"; do
    dest="${SYSTEMD_DIR}/${unit}"
    if [[ -f "${dest}" ]]; then
      echo "Removing stale unit ${unit}"
      systemctl stop "${unit}" 2>/dev/null || true
      systemctl disable "${unit}" 2>/dev/null || true
      rm -f "${dest}"
    fi
    # Also clean up templated instances
    base_name="${unit%.service}"
    while IFS= read -r instance; do
      [[ -z "${instance}" ]] && continue
      echo "Stopping stale instance ${instance}"
      systemctl stop "${instance}" 2>/dev/null || true
      systemctl disable "${instance}" 2>/dev/null || true
    done < <(systemctl list-units --type=service --no-legend --plain 2>/dev/null \
      | awk '{print $1}' \
      | grep -E "^${base_name}@" \
      || true)
  done
}

# ---------------------------------------------------------------------------
# Service enablement
# ---------------------------------------------------------------------------

start_services() {
  local service
  local -a services=("$@")

  systemctl daemon-reload
  if ((${#services[@]} == 0)); then
    return 0
  fi

  for service in "${services[@]}"; do
    echo "Enabling and starting ${service}"
    systemctl enable --now "${service}"
  done
}

# ===========================  Interactive prompts  =========================

APP_ROOT=$(prompt "SupplyCore app root" "${APP_ROOT_DEFAULT}")
RUN_USER=$(prompt "System user for services" "www-data")
RUN_GROUP=$(prompt "System group for services" "${RUN_USER}")
PYTHON_BIN=$(prompt "Python 3.11+ executable for the orchestrator venv" "python3")
VENV_PATH=$(prompt "Python virtualenv path" "${APP_ROOT}/.venv-orchestrator")
SYSTEMD_DIR=$(prompt "systemd unit directory" "/etc/systemd/system")
ENV_FILE=$(prompt "Worker environment file" "/etc/default/supplycore-worker")
LOCAL_CONFIG_PATH=$(prompt "PHP local config path" "${APP_ROOT}/src/config/local.php")
CONFIGURE_LOCAL_PHP=false
if [[ ! -f ${LOCAL_CONFIG_PATH} ]]; then
  if prompt_yes_no "Create PHP app config for this git clone (database is not created/imported by this installer)?" "Y"; then
    CONFIGURE_LOCAL_PHP=true
  fi
elif prompt_yes_no "Update existing PHP app config at ${LOCAL_CONFIG_PATH}?" "Y"; then
  CONFIGURE_LOCAL_PHP=true
fi

APP_ENV=production
APP_BASE_URL=https://supplycore.example.com
APP_TIMEZONE=UTC
DB_HOST=127.0.0.1
DB_PORT=3306
DB_NAME=supplycore
DB_USERNAME=supplycore
DB_PASSWORD=
if [[ ${CONFIGURE_LOCAL_PHP} == true ]]; then
  APP_ENV=$(prompt_non_empty "Application environment" "production")
  APP_BASE_URL=$(prompt_non_empty "Application base URL" "https://supplycore.example.com")
  APP_TIMEZONE=$(prompt_non_empty "Application timezone" "UTC")
  DB_HOST=$(prompt_non_empty "Database host" "127.0.0.1")
  DB_PORT=$(prompt_number "Database port" "3306")
  DB_NAME=$(prompt_non_empty "Database name (installer will not create/import it)" "supplycore")
  DB_USERNAME=$(prompt_non_empty "Database login name" "supplycore")
  DB_PASSWORD=$(prompt_secret "Database password" true)
fi

SYNC_COUNT=$(prompt_number "How many sync workers should be enabled?" "1")
COMPUTE_COUNT=$(prompt_number "How many compute workers should be enabled?" "1")
INSTALL_ZKILL=false
if prompt_yes_no "Install and enable the dedicated zKill worker?" "Y"; then
  INSTALL_ZKILL=true
fi
INSTALL_INFLUX=false
if prompt_yes_no "Install the InfluxDB rollup export service and timer?" "N"; then
  INSTALL_INFLUX=true
fi

# ===========================  Validation  =================================

if [[ ! -d ${APP_ROOT} ]]; then
  echo "App root does not exist: ${APP_ROOT}" >&2
  exit 1
fi

if [[ ! -f ${APP_ROOT}/python/pyproject.toml ]]; then
  echo "Expected Python package metadata at ${APP_ROOT}/python/pyproject.toml" >&2
  exit 1
fi

# ===========================  Setup  ======================================

install -d -m 0755 "${SYSTEMD_DIR}"
install -d -m 0755 "${APP_ROOT}/storage/logs" "${APP_ROOT}/storage/run"
chown -R "${RUN_USER}:${RUN_GROUP}" "${APP_ROOT}/storage"
chmod -R u+rwX "${APP_ROOT}/storage"

configure_local_php "${LOCAL_CONFIG_PATH}" "${APP_ENV}" "${APP_BASE_URL}" "${APP_TIMEZONE}" "${DB_HOST}" "${DB_PORT}" "${DB_NAME}" "${DB_USERNAME}" "${DB_PASSWORD}" "${CONFIGURE_LOCAL_PHP}"

# ===========================  Python venv  ================================

# The venv may exist from git (checked-in or pulled) but contain broken
# symlinks and paths from the machine it was originally created on.
# Always verify the venv's Python actually runs; recreate if it doesn't.
VENV_OK=false
if [[ -x ${VENV_PATH}/bin/python ]] && "${VENV_PATH}/bin/python" -c "import sys; sys.exit(0)" 2>/dev/null; then
  VENV_OK=true
fi

if [[ ${VENV_OK} == false ]]; then
  echo "Creating orchestrator virtualenv at ${VENV_PATH}"
  # Remove broken venv if it exists (e.g. pulled from git with wrong symlinks)
  if [[ -d ${VENV_PATH} ]]; then
    echo "Removing stale/broken virtualenv"
    rm -rf "${VENV_PATH}"
  fi
  "${PYTHON_BIN}" -m venv "${VENV_PATH}"
fi

# Ensure pip is available inside the venv (some distros create venvs without it)
if ! "${VENV_PATH}/bin/python" -m pip --version >/dev/null 2>&1; then
  echo "Bootstrapping pip inside virtualenv"
  "${VENV_PATH}/bin/python" -m ensurepip --upgrade
fi

"${VENV_PATH}/bin/python" -m pip install --upgrade pip
"${VENV_PATH}/bin/python" -m pip install --upgrade "${APP_ROOT}/python"
"${VENV_PATH}/bin/python" -m orchestrator worker-pool --help >/dev/null
"${VENV_PATH}/bin/python" -m orchestrator zkill-worker --help >/dev/null
"${VENV_PATH}/bin/python" -m orchestrator run-job --help >/dev/null

# ===========================  Stale unit cleanup  =========================

cleanup_stale_units

# ===========================  Env file  ===================================

if [[ ! -f ${ENV_FILE} ]]; then
  install -D -m 0644 "${REPO_ROOT}/ops/systemd/supplycore-worker.env.example" "${ENV_FILE}"
else
  echo "Keeping existing worker environment file at ${ENV_FILE}"
fi

# ===========================  Render and install units  ====================

render_unit "${REPO_ROOT}/ops/systemd/supplycore-sync-worker.service" "${SYSTEMD_DIR}/supplycore-sync-worker.service"
render_unit "${REPO_ROOT}/ops/systemd/supplycore-compute-worker.service" "${SYSTEMD_DIR}/supplycore-compute-worker.service"
render_sync_instance_unit "${SYSTEMD_DIR}/supplycore-sync-worker@.service"
render_compute_instance_unit "${SYSTEMD_DIR}/supplycore-compute-worker@.service"
render_unit "${REPO_ROOT}/ops/systemd/supplycore-zkill.service" "${SYSTEMD_DIR}/supplycore-zkill.service"
if [[ ${INSTALL_INFLUX} == true ]]; then
  render_unit "${REPO_ROOT}/ops/systemd/supplycore-influx-rollup-export.service" "${SYSTEMD_DIR}/supplycore-influx-rollup-export.service"
  cp "${REPO_ROOT}/ops/systemd/supplycore-influx-rollup-export.timer" "${SYSTEMD_DIR}/supplycore-influx-rollup-export.timer"
fi

# ===========================  Enable and start  ===========================

services_to_enable=()
if (( SYNC_COUNT == 1 )); then
  services_to_enable+=("supplycore-sync-worker.service")
elif (( SYNC_COUNT > 1 )); then
  for ((i = 1; i <= SYNC_COUNT; i++)); do
    services_to_enable+=("supplycore-sync-worker@${i}.service")
  done
fi

if (( COMPUTE_COUNT == 1 )); then
  services_to_enable+=("supplycore-compute-worker.service")
elif (( COMPUTE_COUNT > 1 )); then
  for ((i = 1; i <= COMPUTE_COUNT; i++)); do
    services_to_enable+=("supplycore-compute-worker@${i}.service")
  done
fi

if [[ ${INSTALL_ZKILL} == true ]]; then
  services_to_enable+=("supplycore-zkill.service")
fi

if [[ ${INSTALL_INFLUX} == true ]]; then
  services_to_enable+=("supplycore-influx-rollup-export.service")
  services_to_enable+=("supplycore-influx-rollup-export.timer")
fi

start_services "${services_to_enable[@]}"

# ===========================  Summary  ====================================

echo
echo "SupplyCore services installed successfully."
echo "Worker environment: ${ENV_FILE}"
echo "Virtualenv: ${VENV_PATH}"
if [[ -f ${LOCAL_CONFIG_PATH} ]]; then
  echo "PHP config: ${LOCAL_CONFIG_PATH}"
fi
echo
echo "Active services:"
if (( SYNC_COUNT == 1 )); then
  echo "  systemctl status supplycore-sync-worker.service"
elif (( SYNC_COUNT > 1 )); then
  for ((i = 1; i <= SYNC_COUNT; i++)); do
    echo "  systemctl status supplycore-sync-worker@${i}.service"
  done
fi
if (( COMPUTE_COUNT == 1 )); then
  echo "  systemctl status supplycore-compute-worker.service"
elif (( COMPUTE_COUNT > 1 )); then
  for ((i = 1; i <= COMPUTE_COUNT; i++)); do
    echo "  systemctl status supplycore-compute-worker@${i}.service"
  done
fi
if [[ ${INSTALL_ZKILL} == true ]]; then
  echo "  systemctl status supplycore-zkill.service"
fi
if [[ ${INSTALL_INFLUX} == true ]]; then
  echo "  systemctl status supplycore-influx-rollup-export.timer"
fi
echo
echo "Run any registered job manually:"
echo "  ${VENV_PATH}/bin/python -m orchestrator run-job --job-key <job_key>"
