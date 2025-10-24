#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '%s %s | %s\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" "ibkr-entrypoint" "$*"
}

# -------- Xvfb ---------------------------------------------------------------
export DISPLAY="${DISPLAY:-:0}"
export XVFB_WHD="${XVFB_WHD:-1280x720x16}"
XVFB_BIN="${XVFB_BIN:-Xvfb}"

log "Starting Xvfb on ${DISPLAY} (${XVFB_WHD})"
"${XVFB_BIN}" "${DISPLAY}" -screen 0 "${XVFB_WHD}" -ac -nolisten tcp 2>&1 | tee /tmp/xvfb.log &
XVFB_PID=$!
cleanup() {
  if kill -0 "${XVFB_PID}" >/dev/null 2>&1; then
    log "Stopping Xvfb (${XVFB_PID})"
    kill "${XVFB_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sleep 1  # give Xvfb a moment to start
xdpyinfo >/dev/null 2>&1 || { log "ERROR: Xvfb failed to start"; cat /tmp/xvfb.log; exit 1; }

# -------- Config & paths -----------------------------------------------------
IBC_PATH="${IBC_PATH:-/opt/ibc}"
TWS_PATH="${TWS_PATH:-/opt/ibgateway/ibgateway}"            # parent folder (not including version)
CONFIG_DIR="${IBC_CONFIG_DIR:-/config}"
LOG_DIR="${LOG_PATH:-/var/log/ibgateway}"
IB_GATEWAY_VERSION="${IB_GATEWAY_VERSION:-1037}"  # e.g., 1037 for 10.37.x

# JAVA: script expects a *directory* containing 'java'
# Allow user to set JAVA_PATH as either '/usr/bin' or '/usr/bin/java'
RAW_JAVA_PATH="${JAVA_PATH:-/usr/bin}"
if [[ "${RAW_JAVA_PATH}" == */java ]]; then
  JAVA_DIR="$(dirname -- "${RAW_JAVA_PATH}")"
else
  JAVA_DIR="${RAW_JAVA_PATH}"
fi

mkdir -p "${CONFIG_DIR}" "${LOG_DIR}"

# -------- ibc.ini bootstrap (no secrets in logs) -----------------------------
INI_PATH="${IBC_INI_PATH:-${CONFIG_DIR}/ibc.ini}"
if [[ ! -f "${INI_PATH}" ]]; then
  log "Writing default IBC config to ${INI_PATH}"
  cat > "${INI_PATH}" <<'CONFIG'
[Config]
# These may be overridden by environment at runtime; keep secrets out of logs
IbLoginId=elijahcbrooks
IbPassword=lizardBreath34$7
TradingMode=paper
ReadOnlyApi=yes
AcceptIncomingConnectionAction=accept
ExistingSessionDetectedAction=primary
StoreSettingsOnServer=no
SaveTwsSettings=yes
TwsSettingsDir=/home/ibkr/Jts
CommandServerStart=yes
CommandServerPort=7462
CommandServerHost=127.0.0.1
LoginTimeoutSeconds=180
[LogSettings]
LogLevel=debug
CONFIG

  # Fill from env if provided (do not log)
  [[ -n "${IBC_TWS_USERNAME:-}" ]] && sed -i "s/^IbLoginId=.*/IbLoginId=${IBC_TWS_USERNAME//\//\\/}/" "${INI_PATH}"
  [[ -n "${IBC_TWS_PASSWORD:-}" ]] && sed -i "s/^IbPassword=.*/IbPassword=${IBC_TWS_PASSWORD//\//\\/}/" "${INI_PATH}"
  [[ -n "${IBC_TRADING_MODE:-}" ]] && sed -i "s/^TradingMode=.*/TradingMode=${IBC_TRADING_MODE}/" "${INI_PATH}"
  [[ -n "${IBC_READONLY_API:-}" ]] && sed -i "s/^ReadOnlyApi=.*/ReadOnlyApi=${IBC_READONLY_API}/" "${INI_PATH}"
  [[ -n "${IBC_ACCEPT_INCOMING_ACTION:-}" ]] && sed -i "s/^AcceptIncomingConnectionAction=.*/AcceptIncomingConnectionAction=${IBC_ACCEPT_INCOMING_ACTION}/" "${INI_PATH}"
  [[ -n "${IBC_EXISTING_SESSION_ACTION:-}" ]] && sed -i "s/^ExistingSessionDetectedAction=.*/ExistingSessionDetectedAction=${IBC_EXISTING_SESSION_ACTION}/" "${INI_PATH}"
fi

# -------- Preflight checks (helpful errors) ----------------------------------
[[ -x "${IBC_PATH}/scripts/ibcstart.sh" ]] || { log "ERROR: ${IBC_PATH}/scripts/ibcstart.sh not found or not executable"; exit 1; }
[[ -d "${TWS_PATH}" ]] || { log "ERROR: TWS_PATH does not exist: ${TWS_PATH}"; exit 1; }
[[ -f "${INI_PATH}" ]] || { log "ERROR: IBC ini not found at ${INI_PATH}"; exit 1; }
if [[ ! -x "${JAVA_DIR}/java" ]]; then
  log "WARN: No executable at ${JAVA_DIR}/java; attempting PATH fallback"
  if ! command -v java >/dev/null 2>&1; then
    log "ERROR: Java not found (expected ${JAVA_DIR}/java or on PATH)"
    exit 1
  fi
  JAVA_DIR="$(dirname -- "$(command -v java)")"
fi

# Optional: show effective config (no secrets)
log "Effective settings:"
log "  IB_GATEWAY_VERSION=${IB_GATEWAY_VERSION}"
log "  Program=Gateway"
log "  TWS_PATH=${TWS_PATH}"
log "  TWS_SETTINGS_PATH=${CONFIG_DIR}"
log "  IBC_PATH=${IBC_PATH}"
log "  IBC_INI=${INI_PATH}"
log "  JAVA_DIR=${JAVA_DIR}"
log "  MODE=${IBC_TRADING_MODE:-paper}"
log "  LOG_DIR=${LOG_DIR}"


# -------- VM options symlinks -----------------------------------------------
VER_DIR="${TWS_PATH}/${IB_GATEWAY_VERSION}"
PARENT="${TWS_PATH}"

if [ -f "${VER_DIR}/ibgateway.vmoptions" ]; then
  ln -sf "${VER_DIR}/ibgateway.vmoptions" "${VER_DIR}/tws.vmoptions"
  ln -sf "${VER_DIR}/ibgateway.vmoptions" "${PARENT}/ibgateway.vmoptions"
  ln -sf "${VER_DIR}/ibgateway.vmoptions" "${PARENT}/tws.vmoptions"
else
  log "ERROR: ${VER_DIR}/ibgateway.vmoptions not found"
  find "${TWS_PATH}" -maxdepth 3 -type f -name '*vmoptions' -printf '  %p\n' || true
  exit 1
fi


# -------- Build command (version first, flags as key=value) ------------------
CMD=( "${IBC_PATH}/scripts/ibcstart.sh" "${IB_GATEWAY_VERSION}" --gateway
  "--tws-path=${TWS_PATH}"
  "--tws-settings-path=/home/ibkr/Jts"
  "--ibc-path=${IBC_PATH}"
  "--ibc-ini=${INI_PATH}"
  "--java-path=${JAVA_DIR}"
  "--mode=${IBC_TRADING_MODE:-paper}"
)

log "Executing: ${CMD[*]}"
# exec "${CMD[@]}"

# # With:
"${CMD[@]}" || true
log "IBC exited — keeping container alive for debugging..."
tail -f /dev/null