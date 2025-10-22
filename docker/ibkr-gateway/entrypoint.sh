#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '%s %s | %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "ibkr-entrypoint" "$*"
}

export DISPLAY="${DISPLAY:-:0}"
export XVFB_WHD="${XVFB_WHD:-1280x720x16}"
XVFB_BIN="${XVFB_BIN:-Xvfb}"

log "Starting Xvfb on ${DISPLAY} (${XVFB_WHD})"
"${XVFB_BIN}" "${DISPLAY}" -screen 0 "${XVFB_WHD}" -nolisten tcp -nolisten unix &
XVFB_PID=$!

cleanup() {
  if kill -0 "${XVFB_PID}" >/dev/null 2>&1; then
    log "Stopping Xvfb (${XVFB_PID})"
    kill "${XVFB_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

IBC_PATH="${IBC_PATH:-/opt/ibc}"
TWS_PATH="${TWS_PATH:-/opt/ibgateway}"
JAVA_PATH="${JAVA_PATH:-/usr/bin/java}"
CONFIG_DIR="${IBC_CONFIG_DIR:-/config}"
LOG_DIR="${LOG_PATH:-/var/log/ibgateway}"
mkdir -p "${CONFIG_DIR}" "${LOG_DIR}"

INI_PATH="${IBC_INI_PATH:-${CONFIG_DIR}/ibc.ini}"
if [ ! -f "${INI_PATH}" ]; then
  log "Writing IBC config to ${INI_PATH}"
  cat <<CONFIG >"${INI_PATH}"
[Config]
IbLoginId=${IBC_TWS_USERNAME:-}
IbPassword=${IBC_TWS_PASSWORD:-}
TradingMode=${IBC_TRADING_MODE:-paper}
ReadOnlyApi=${IBC_READONLY_API:-yes}
AcceptIncomingConnectionAction=${IBC_ACCEPT_INCOMING_ACTION:-accept}
ExistingSessionDetectedAction=${IBC_EXISTING_SESSION_ACTION:-primary}
StoreSettingsOnServer=no
SaveTwsSettings=yes
CONFIG
fi

PRIMARY_PORT="${IBC_GATEWAY_PORT:-4002}"
SECONDARY_PORT="${IBC_GATEWAY_LIVE_PORT:-4001}"

ARGS=(
  "${INI_PATH}"
  --gateway
  --ibc-path "${IBC_PATH}"
  --ibg-path "${TWS_PATH}"
  --mode "${IBC_TRADING_MODE:-paper}"
  --java-path "${JAVA_PATH}"
  --tws-settings-path "${CONFIG_DIR}"
  --logs-path "${LOG_DIR}"
  --launcher-login-id "${IBC_TWS_USERNAME:-}"
  --launcher-password "${IBC_TWS_PASSWORD:-}"
  --read-only-api "${IBC_READONLY_API:-yes}"
  --accept-incoming-action "${IBC_ACCEPT_INCOMING_ACTION:-accept}"
  --existing-session-action "${IBC_EXISTING_SESSION_ACTION:-primary}"
)

if [ -n "${TWS_CONFIG_PATH:-}" ]; then
  ARGS+=(--tws-config "${TWS_CONFIG_PATH}")
fi

if [ -n "${PRIMARY_PORT}" ] || [ -n "${SECONDARY_PORT}" ]; then
  ARGS+=(--gateway-ports "${PRIMARY_PORT}:${SECONDARY_PORT}")
fi

log "Launching IBC gateway controller"
exec "${IBC_PATH}/scripts/ibcstart.sh" "${ARGS[@]}"
