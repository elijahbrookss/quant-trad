#!/usr/bin/env bash
# -----------------------------------------
# IBKR Gateway healthcheck
# Layers:
#   1. Socket listening check      (ss)
#   2. TCP handshake reachability  (nc)
#   3. API session usability       (ib_insync)
# -----------------------------------------

set -u
# we intentionally do NOT `set -e` so we can run all checks and summarize

### -------------------------
### Config
### -------------------------
IB_HOST="127.0.0.1"
IB_PORT="4002"
IB_CLIENT_ID="1"
PYTHON_BIN="python3"

### -------------------------
### Logging helpers
### -------------------------
timestamp() {
    date +"%Y-%m-%dT%H:%M:%S%z"
}

log_info() {
    echo "$(timestamp) [INFO] $*"
}

log_warn() {
    echo "$(timestamp) [WARN] $*" >&2
}

log_error() {
    echo "$(timestamp) [ERROR] $*" >&2
}

### -------------------------
### Check 1: Port exposure via ss
### -------------------------
check_ss() {
    log_info "=== CHECK 1: LISTEN SOCKET (ss) ==="
    log_info "Goal: Is IBKR Gateway actually LISTENING on ${IB_HOST}:${IB_PORT}?"
    log_info "If this fails: process may be down, crashed, still booting, or bound to a different interface/port."

    # get any LISTEN sockets on that port
    SS_OUTPUT="$(ss -ltnp 2>/dev/null | grep LISTEN | grep -E ":${IB_PORT}\b" || true)"

    if [[ -n "$SS_OUTPUT" ]]; then
        log_info "Result: FOUND listener on port ${IB_PORT}:"
        echo "$SS_OUTPUT"
        # warn if not bound to the expected host
        if ! echo "$SS_OUTPUT" | grep -q "${IB_HOST}:${IB_PORT}"; then
            log_warn "Note: Port ${IB_PORT} is listening, but not specifically on ${IB_HOST}. Binding may be 0.0.0.0 or a different iface."
        fi
        return 0
    else
        log_error "Result: No LISTEN socket found for port ${IB_PORT}."
        return 1
    fi
}

### -------------------------
### Check 2: TCP handshake via nc
### -------------------------
check_nc() {
    log_info "=== CHECK 2: TCP REACHABILITY (nc) ==="
    log_info "Goal: Can we complete a basic TCP handshake to ${IB_HOST}:${IB_PORT} from INSIDE this container?"
    log_info "If this fails: networking/firewall/container routing issues OR the process is wedged and not accepting new clients."

    # -z : don't send data
    # -v : verbose
    # -w 3 : small timeout
    NC_OUTPUT="$(nc -zv -w 3 "$IB_HOST" "$IB_PORT" 2>&1 >/dev/null)"
    NC_RC=$?

    if [[ $NC_RC -eq 0 ]]; then
        log_info "Result: SUCCESS. TCP handshake to ${IB_HOST}:${IB_PORT} succeeded."
        return 0
    else
        log_error "Result: FAIL. TCP handshake to ${IB_HOST}:${IB_PORT} failed."
        log_error "nc says: ${NC_OUTPUT}"
        return 1
    fi
}

### -------------------------
### Check 3: API session via ib_insync
### -------------------------
check_ib_insync() {
    log_info "=== CHECK 3: API SESSION (ib_insync) ==="
    log_info "Goal: Can we open an IB API session and get a sane response?"
    log_info "If this fails: Gateway may be up at TCP level but blocked on 2FA, daily restart, or API login rules."

    "$PYTHON_BIN" - "$IB_HOST" "$IB_PORT" "$IB_CLIENT_ID" <<'EOF'
import sys, time
from ib_insync import IB

def log(level, msg):
    ts = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    print(f"{ts} [{level}] [ib_insync] {msg}")

if len(sys.argv) != 4:
    log("ERROR", f"Bad args; expected HOST PORT CLIENT_ID, got {sys.argv}")
    sys.exit(2)

host = sys.argv[1]
port = int(sys.argv[2])
client_id = int(sys.argv[3])

log("INFO", f"Connecting to IB Gateway at {host}:{port} with clientId={client_id} ...")
ib = IB()

try:
    # 5s timeout so we're not hanging forever
    ib.connect(host, port, clientId=client_id, timeout=5)
except Exception as e:
    log("ERROR", f"API connect FAILED: {e.__class__.__name__}({e})")
    log("ERROR", "Meaning: TCP is reachable but IB API handshake did not complete.")
    log("ERROR", "Likely causes:")
    log("ERROR", "- Gateway stuck at login/2FA prompt")
    log("ERROR", "- Daily auto-restart window / mid-restart")
    log("ERROR", "- API disabled or clientId refused")
    sys.exit(1)

if not ib.isConnected():
    log("ERROR", "Connected call returned but ib.isConnected() is False.")
    sys.exit(1)

# Light ping to prove real API usability
try:
    srv_time = ib.reqCurrentTime()
    log("INFO", f"API ping OK. Server time: {srv_time}")
    log("INFO", "Meaning: Gateway is authenticated to IB and responding.")
    rc = 0
except Exception as e:
    log("WARN", f"API ping WARN: Connected but reqCurrentTime() raised {e.__class__.__name__}({e})")
    log("WARN", "Meaning: Session opened, but gateway is acting slow/unhappy.")
    rc = 2

# cleanup
try:
    ib.disconnect()
    log("INFO", "Disconnected cleanly.")
except Exception as e:
    log("WARN", f"Disconnect WARN: {e.__class__.__name__}({e}) (not fatal)")

sys.exit(rc)
EOF

    PY_RC=$?

    if [[ $PY_RC -eq 0 ]]; then
        log_info "Result: SUCCESS. ib_insync session is healthy."
        return 0
    elif [[ $PY_RC -eq 2 ]]; then
        log_warn "Result: DEGRADED. Connected to API but ping looked off."
        return 2
    else
        log_error "Result: FAIL. Could not establish a working IB API session."
        return 1
    fi
}

### -------------------------
### Run all checks
### -------------------------
main() {
    log_info "===== IBKR GATEWAY HEALTHCHECK START ====="
    log_info "Scope: This script runs INSIDE the IBKR Gateway container."
    log_info "Layers:"
    log_info "  1) Socket open (is anything listening?)"
    log_info "  2) TCP handshake (can we talk over the network stack?)"
    log_info "  3) API usability (can we actually speak IB API?)"
    echo

    check_ss
    SS_RC=$?
    echo

    check_nc
    NC_RC=$?
    echo

    check_ib_insync
    IB_RC=$?
    echo

    log_info "===== SUMMARY ====="
    log_info "Check1 ss   (listening socket) rc=${SS_RC}"
    log_info "Check2 nc   (tcp handshake)    rc=${NC_RC}"
    log_info "Check3 ib   (api session)      rc=${IB_RC}"

    # Final classification for exit code:
    # 0  -> fully healthy
    # 2  -> degraded (API reachable but unhappy)
    # 3+ -> critical
    #
    # Priority of diagnosis:
    #   1) If API works -> healthy/degraded
    #   2) Else if not listening -> startup/bind issue
    #   3) Else if tcp handshake failed -> network/socket issue
    #   4) Else -> API login/auth issue

    if [[ $IB_RC -eq 0 ]]; then
        log_info "OVERALL: HEALTHY. Gateway is up, authenticated, and responding."
        exit 0
    elif [[ $IB_RC -eq 2 ]]; then
        log_warn "OVERALL: DEGRADED. API session opened but ping wasn't clean. Investigate auth state / latency."
        exit 2
    else
        # API layer hard failed
        if [[ $SS_RC -ne 0 ]]; then
            log_error "OVERALL: CRITICAL. Nothing is LISTENING on ${IB_PORT}. Gateway likely down or mid-startup."
            exit 4
        elif [[ $NC_RC -ne 0 ]]; then
            log_error "OVERALL: CRITICAL. Port ${IB_PORT} is present but TCP handshake failed. Suspect firewall/iptables/socket exhaustion."
            exit 3
        else
            log_error "OVERALL: CRITICAL. Port is open and TCP works, but IB API login/handshake failed. Likely 2FA/restart block."
            exit 5
        fi
    fi
}

main "$@"
