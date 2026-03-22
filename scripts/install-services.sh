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
    -e "s|User=www-data|User=${user_escaped}|" \
    -e "s|Group=www-data|Group=${group_escaped}|" \
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

APP_ROOT=$(prompt "SupplyCore app root" "${APP_ROOT_DEFAULT}")
RUN_USER=$(prompt "System user for services" "www-data")
RUN_GROUP=$(prompt "System group for services" "${RUN_USER}")
PYTHON_BIN=$(prompt "Python 3.11+ executable for the orchestrator venv" "python3")
VENV_PATH=$(prompt "Python virtualenv path" "${APP_ROOT}/.venv-orchestrator")
SYSTEMD_DIR=$(prompt "systemd unit directory" "/etc/systemd/system")
ENV_FILE=$(prompt "Worker environment file" "/etc/default/supplycore-worker")
SYNC_COUNT=$(prompt_number "How many sync workers should be enabled?" "1")
COMPUTE_COUNT=$(prompt_number "How many compute workers should be enabled?" "1")
INSTALL_ZKILL=false
if prompt_yes_no "Install and enable the dedicated zKill worker?" "Y"; then
  INSTALL_ZKILL=true
fi
INSTALL_COMPAT_ORCHESTRATOR=false
if prompt_yes_no "Install the legacy compatibility worker-pool service (supplycore-orchestrator.service)?" "N"; then
  INSTALL_COMPAT_ORCHESTRATOR=true
fi

if [[ ! -d ${APP_ROOT} ]]; then
  echo "App root does not exist: ${APP_ROOT}" >&2
  exit 1
fi

if [[ ! -f ${APP_ROOT}/python/pyproject.toml ]]; then
  echo "Expected Python package metadata at ${APP_ROOT}/python/pyproject.toml" >&2
  exit 1
fi

install -d -m 0755 "${SYSTEMD_DIR}"
install -d -m 0755 "${APP_ROOT}/storage/logs" "${APP_ROOT}/storage/run"
chown -R "${RUN_USER}:${RUN_GROUP}" "${APP_ROOT}/storage"
chmod -R u+rwX "${APP_ROOT}/storage"

if [[ ! -x ${VENV_PATH}/bin/python ]]; then
  echo "Creating orchestrator virtualenv at ${VENV_PATH}"
  "${PYTHON_BIN}" -m venv "${VENV_PATH}"
fi

"${VENV_PATH}/bin/python" -m pip install --upgrade pip
"${VENV_PATH}/bin/python" -m pip install --upgrade "${APP_ROOT}/python"
"${VENV_PATH}/bin/python" -m orchestrator worker-pool --help >/dev/null
"${VENV_PATH}/bin/python" -m orchestrator zkill-worker --help >/dev/null

if [[ ! -f ${ENV_FILE} ]]; then
  install -D -m 0644 "${REPO_ROOT}/ops/systemd/supplycore-worker.env.example" "${ENV_FILE}"
else
  echo "Keeping existing worker environment file at ${ENV_FILE}"
fi

render_unit "${REPO_ROOT}/ops/systemd/supplycore-sync-worker.service" "${SYSTEMD_DIR}/supplycore-sync-worker.service"
render_unit "${REPO_ROOT}/ops/systemd/supplycore-compute-worker.service" "${SYSTEMD_DIR}/supplycore-compute-worker.service"
render_sync_instance_unit "${SYSTEMD_DIR}/supplycore-sync-worker@.service"
render_compute_instance_unit "${SYSTEMD_DIR}/supplycore-compute-worker@.service"
render_unit "${REPO_ROOT}/ops/systemd/supplycore-zkill.service" "${SYSTEMD_DIR}/supplycore-zkill.service"
if [[ ${INSTALL_COMPAT_ORCHESTRATOR} == true ]]; then
  render_unit "${REPO_ROOT}/ops/systemd/supplycore-orchestrator.service" "${SYSTEMD_DIR}/supplycore-orchestrator.service"
fi

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

if [[ ${INSTALL_COMPAT_ORCHESTRATOR} == true ]]; then
  services_to_enable+=("supplycore-orchestrator.service")
fi

start_services "${services_to_enable[@]}"

echo
echo "SupplyCore services installed successfully."
echo "Worker environment: ${ENV_FILE}"
echo "Virtualenv: ${VENV_PATH}"
echo "Helpful status commands:"
if (( SYNC_COUNT == 1 )); then
  echo "  systemctl status supplycore-sync-worker.service"
elif (( SYNC_COUNT > 1 )); then
  echo "  systemctl status supplycore-sync-worker@1.service"
fi
if (( COMPUTE_COUNT == 1 )); then
  echo "  systemctl status supplycore-compute-worker.service"
elif (( COMPUTE_COUNT > 1 )); then
  echo "  systemctl status supplycore-compute-worker@1.service"
fi
if [[ ${INSTALL_ZKILL} == true ]]; then
  echo "  systemctl status supplycore-zkill.service"
fi
