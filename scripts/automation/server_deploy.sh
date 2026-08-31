#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
repo_root="$(cd "$script_dir/../.." >/dev/null 2>&1 && pwd)"
deployment_root="$(dirname "$repo_root")"
compose_file="${QT_SINGLE_NODE_COMPOSE_FILE:-$repo_root/docker/docker-compose.server.yml}"
alerting_compose_file="${QT_ALERTING_COMPOSE_FILE:-$repo_root/docker/docker-compose.alert-email.yml}"
env_file="${QT_SINGLE_NODE_ENV_FILE:-${QT_SERVER_ENV_FILE:-$deployment_root/secrets.env}}"

if [[ "$compose_file" != /* ]]; then
  compose_file="$repo_root/$compose_file"
fi
if [[ "$env_file" != /* ]]; then
  env_file="$repo_root/$env_file"
fi
if [[ "$alerting_compose_file" != /* ]]; then
  alerting_compose_file="$repo_root/$alerting_compose_file"
fi
export QT_SINGLE_NODE_ENV_FILE="$env_file"
export QT_SERVER_ENV_FILE="$env_file"

env_value() {
  local key="$1"
  local line=""
  if test -f "$env_file"; then
    line="$(grep -E "^${key}=" "$env_file" | tail -n 1 || true)"
  fi
  printf '%s' "${line#*=}"
}

first_value() {
  local value
  for value in "$@"; do
    if test -n "$value"; then
      printf '%s' "$value"
      return 0
    fi
  done
  return 0
}

data_root="$(first_value "${QT_MARKET_DATA_ROOT:-}" "$(env_value QT_MARKET_DATA_ROOT)" "/srv/quanttrad/market-structure")"
single_node_profiles="$(first_value "${QT_SINGLE_NODE_PROFILES:-}" "$(env_value QT_SINGLE_NODE_PROFILES)" "${QT_SERVER_PROFILES:-}" "$(env_value QT_SERVER_PROFILES)")"
state_root="$(first_value "${QT_SINGLE_NODE_STATE_ROOT:-}" "$(env_value QT_SINGLE_NODE_STATE_ROOT)" "$deployment_root/deploy-state")"
state_file="$state_root/release.env"
history_file="$state_root/release-history.ndjson"
alerts_enabled="$(first_value "$(env_value QT_ALERTS_ENABLED)" "false")"
export QT_MARKET_DATA_ROOT="$data_root"

usage() {
  cat <<'EOF'
Usage:
  scripts/automation/server_deploy.sh init-env
  scripts/automation/server_deploy.sh doctor
  scripts/automation/server_deploy.sh validate-alerts
  scripts/automation/server_deploy.sh apply-alerts
  scripts/automation/server_deploy.sh deploy [git-ref]
  scripts/automation/server_deploy.sh rollback [git-ref]
  scripts/automation/server_deploy.sh config
  scripts/automation/server_deploy.sh release
  scripts/automation/server_deploy.sh status
  scripts/automation/server_deploy.sh fleet
  scripts/automation/server_deploy.sh qt <qt-arguments...>
  scripts/automation/server_deploy.sh credentials-coinbase [setup-options]
  scripts/automation/server_deploy.sh logs [service]
  scripts/automation/server_deploy.sh stop

Deploy promotes one exact commit. Rollback without an argument promotes the
previously recorded commit. The default stack is the complete single-node
application and observability surface. Set QT_SINGLE_NODE_PROFILES=broker to
include the optional IBKR Gateway.
EOF
}

die() {
  echo "single-node deploy failed: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

initialize_operator_environment() {
  require_command python3
  test ! -e "$env_file" \
    || die "operator environment already exists; refusing to overwrite: $env_file"
  local environment_parent
  environment_parent="$(dirname "$env_file")"
  test -d "$environment_parent" \
    || die "operator environment parent does not exist: $environment_parent"
  python3 - "$env_file" "$repo_root" <<'PY'
import base64
import os
from pathlib import Path
import secrets
import sys

target = Path(sys.argv[1])
repo_root = Path(sys.argv[2]).resolve()
single_node_root = repo_root.parent

def password() -> str:
    # URL-safe values may be interpolated into the private SQLAlchemy DSN
    # without creating a second encoded-password authority.
    return secrets.token_urlsafe(32)

values = {
    "POSTGRES_DB": "quanttrad",
    "POSTGRES_USER": "quanttrad",
    "POSTGRES_PASSWORD": password(),
    # pgAdmin rejects special-use domains such as .local even when email
    # deliverability checks are disabled. This is only the local login name.
    "PGADMIN_DEFAULT_EMAIL": "admin@quanttrad.dev",
    "PGADMIN_DEFAULT_PASSWORD": password(),
    "GF_SECURITY_ADMIN_USER": "admin",
    "GF_SECURITY_ADMIN_PASSWORD": password(),
    "QT_SECURITY_PROVIDER_CREDENTIAL_KEY": base64.urlsafe_b64encode(
        secrets.token_bytes(32)
    ).decode("ascii"),
    "CHAINLINK_ARBITRUM_RPC_URL": "https://arb1.arbitrum.io/rpc",
    "QT_MARKET_DATA_ROOT": str(single_node_root / "market-structure"),
    "QT_SINGLE_NODE_STATE_ROOT": str(single_node_root / "deploy-state"),
    "QT_COMPOSE_PROJECT_NAME": "quant-trad-single-node",
    "QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA": "true",
    "QT_SINGLE_NODE_ENABLE_SCHEDULED_FACTS": "true",
    "QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS": "true",
    "QT_SINGLE_NODE_ENABLE_TRADE_STREAMS": "true",
    "QT_SINGLE_NODE_ENABLE_L2_STREAMS": "true",
    "QT_ALERTS_ENABLED": "false",
    "QT_ALERT_EMAILS": "",
}
payload = "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
try:
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
except BaseException:
    target.unlink(missing_ok=True)
    raise
PY
  echo "Generated private operator environment: $env_file"
  echo "No provider API credential was written; load it after deployment."
}

require_operator_value() {
  local key="$1"
  local value
  value="$(env_value "$key")"
  test -n "$value" || die "$key must be set in $env_file"
  case "$value" in
    *replace-with* | *change-me* | *example.com*)
      die "$key still contains an example value in $env_file"
      ;;
  esac
}

validate_boolean_value() {
  local key="$1"
  local value="$2"
  case "$value" in
    true | false)
      ;;
    *)
      die "$key must be true or false"
      ;;
  esac
}

validate_email_list() {
  local key="$1"
  local value="$2"
  if ! python3 - "$key" "$value" <<'PY'
from email.headerregistry import Address
import sys

key, raw_value = sys.argv[1:]
items = [item.strip() for item in raw_value.split(",")]
if not items or any(not item for item in items):
    raise SystemExit(f"{key} must be a comma-separated list of email addresses")
if len(set(items)) != len(items):
    raise SystemExit(f"{key} contains a duplicate email address")
for item in items:
    try:
        parsed = Address(addr_spec=item)
    except (TypeError, ValueError) as exc:
        raise SystemExit(f"{key} contains an invalid email address: {item}") from exc
    if not parsed.username or not parsed.domain or parsed.addr_spec != item:
        raise SystemExit(f"{key} contains an invalid email address: {item}")
PY
  then
    die "$key validation failed"
  fi
}

validate_alerting_environment() {
  local smtp_host smtp_port
  validate_boolean_value QT_ALERTS_ENABLED "$alerts_enabled"
  if test "$alerts_enabled" = "false"; then
    return 0
  fi

  test -f "$alerting_compose_file" \
    || die "alerting Compose overlay not found: $alerting_compose_file"
  require_operator_value QT_ALERT_EMAILS
  require_operator_value QT_ALERT_SMTP_HOST
  require_operator_value QT_ALERT_SMTP_USER
  require_operator_value QT_ALERT_SMTP_PASSWORD
  require_operator_value QT_ALERT_EMAIL_FROM
  validate_email_list QT_ALERT_EMAILS "$(env_value QT_ALERT_EMAILS)"
  validate_email_list QT_ALERT_EMAIL_FROM "$(env_value QT_ALERT_EMAIL_FROM)"

  smtp_host="$(env_value QT_ALERT_SMTP_HOST)"
  smtp_port="${smtp_host##*:}"
  test "${smtp_host%:*}" != "$smtp_host" \
    && test -n "${smtp_host%:*}" \
    && [[ "$smtp_port" =~ ^[0-9]+$ ]] \
    && (( smtp_port >= 1 && smtp_port <= 65535 )) \
    || die "QT_ALERT_SMTP_HOST must use host:port with a valid port"
}

profile_enabled() {
  local requested="$1"
  local profile
  local profiles="${single_node_profiles//,/ }"
  for profile in $profiles; do
    if test "$profile" = "$requested"; then
      return 0
    fi
  done
  return 1
}

validate_profiles() {
  local profile
  local profiles="${single_node_profiles//,/ }"
  for profile in $profiles; do
    test "$profile" = "broker" || die "unsupported single-node profile: $profile"
  done
}

validate_operator_environment() {
  local postgres_db postgres_user postgres_password credential_key
  require_operator_value POSTGRES_DB
  require_operator_value POSTGRES_USER
  require_operator_value POSTGRES_PASSWORD
  require_operator_value PGADMIN_DEFAULT_EMAIL
  require_operator_value PGADMIN_DEFAULT_PASSWORD
  require_operator_value GF_SECURITY_ADMIN_PASSWORD
  require_operator_value QT_SECURITY_PROVIDER_CREDENTIAL_KEY
  postgres_db="$(env_value POSTGRES_DB)"
  postgres_user="$(env_value POSTGRES_USER)"
  postgres_password="$(env_value POSTGRES_PASSWORD)"
  credential_key="$(env_value QT_SECURITY_PROVIDER_CREDENTIAL_KEY)"
  [[ "$postgres_db" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] \
    || die "POSTGRES_DB must be a simple PostgreSQL identifier"
  [[ "$postgres_user" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] \
    || die "POSTGRES_USER must be a simple PostgreSQL identifier"
  [[ "$postgres_password" =~ ^[A-Za-z0-9_-]{24,}$ ]] \
    || die "POSTGRES_PASSWORD must be at least 24 URL-safe characters"
  [[ "$credential_key" =~ ^[A-Za-z0-9_-]{43}=$ ]] \
    || die "QT_SECURITY_PROVIDER_CREDENTIAL_KEY must be a Fernet key"
  validate_profiles

  if profile_enabled broker; then
    require_operator_value IBKR_TWS_USERNAME
    require_operator_value IBKR_TWS_PASSWORD
  fi
  validate_alerting_environment
}

validate_private_env_file() {
  local mode
  mode="$(stat -c '%a' "$env_file")"
  [[ "$mode" =~ ^[0-7]{3,4}$ ]] || die "cannot determine permissions for $env_file"
  if (( (8#$mode) & 077 )); then
    die "operator environment must not be group/world accessible: $env_file mode=$mode"
  fi
}

validate_storage_root() {
  [[ "$data_root" = /* ]] || die "QT_MARKET_DATA_ROOT must be an absolute path"
  test "$data_root" != "/" || die "QT_MARKET_DATA_ROOT cannot be /"
  test -d "$data_root" || die "market-data directory not found: $data_root"
  test -w "$data_root" || die "market-data directory is not writable: $data_root"
  [[ "$state_root" = /* ]] || die "QT_SINGLE_NODE_STATE_ROOT must be an absolute path"
  test "$state_root" != "/" || die "QT_SINGLE_NODE_STATE_ROOT cannot be /"
}

require_runtime() {
  require_command docker
  require_command git
  require_command python3
  require_command stat
  docker compose version >/dev/null 2>&1 \
    || die "Docker Compose plugin is unavailable"
  docker info >/dev/null 2>&1 \
    || die "Docker Engine is unavailable to the deployment account"
  test -f "$compose_file" || die "Compose file not found: $compose_file"
  test -f "$env_file" || die "operator environment file not found: $env_file"
  validate_storage_root
  validate_private_env_file
  validate_operator_environment
}

require_clean_checkout() {
  if test -n "$(git -C "$repo_root" status --porcelain)"; then
    die "repository must be clean before deployment"
  fi
}

compute_release_material() {
  require_clean_checkout
  QT_RELEASE_REVISION="$(git -C "$repo_root" rev-parse --verify HEAD)"
  QT_SOURCE_TREE_HASH="$(
    python3 "$repo_root/scripts/provenance/source_tree_hash.py" \
      --root "$repo_root" \
      --git-revision "$QT_RELEASE_REVISION"
  )"
  export QT_RELEASE_REVISION QT_SOURCE_TREE_HASH
}

select_release() {
  local requested_ref="${1:-}"
  require_clean_checkout
  if test -n "$requested_ref"; then
    if git -C "$repo_root" remote get-url origin >/dev/null 2>&1; then
      git -C "$repo_root" fetch --prune origin
    fi
    local release_commit
    release_commit="$(git -C "$repo_root" rev-parse --verify "${requested_ref}^{commit}")" \
      || die "git ref does not resolve to a commit: $requested_ref"
    git -C "$repo_root" switch --detach "$release_commit"
  fi
  compute_release_material
}

compose() {
  local profile_args=()
  local compose_file_args=(--file "$compose_file")
  local profile
  local profiles="${single_node_profiles//,/ }"
  for profile in $profiles; do
    profile_args+=(--profile "$profile")
  done
  if test "$alerts_enabled" = "true"; then
    compose_file_args+=(--file "$alerting_compose_file")
  fi

  docker compose \
    --env-file "$env_file" \
    "${compose_file_args[@]}" \
    "${profile_args[@]}" \
    "$@"
}

build_release_images() {
  local rebuild_database_image
  rebuild_database_image="$(
    first_value \
      "${QT_REBUILD_DATABASE_IMAGE:-}" \
      "$(env_value QT_REBUILD_DATABASE_IMAGE)" \
      "0"
  )"
  compose pull --ignore-buildable

  if ! docker image inspect quanttrad-postgres:2.14.2-pg15 >/dev/null 2>&1 \
    || test "$rebuild_database_image" = "1"; then
    compose build --pull tsdb
  fi

  compose build --pull backend frontend frontend-v2
  if profile_enabled broker; then
    compose build --pull ibkr-gateway
  fi
}

verify_release_image() {
  local service="$1"
  compose exec -T "$service" sh -eu -c '
    actual_revision="${QT_IMAGE_SOURCE_REVISION:-}"
    actual_hash="${QT_IMAGE_SOURCE_TREE_HASH:-}"
    test "$actual_revision" = "$1" || {
      echo "image revision mismatch for $0: $actual_revision != $1" >&2
      exit 1
    }
    test "$actual_hash" = "$2" || {
      echo "image source hash mismatch for $0: $actual_hash != $2" >&2
      exit 1
    }
  ' "$service" "$QT_RELEASE_REVISION" "$QT_SOURCE_TREE_HASH"
}

verify_initializer() {
  local container_id
  local exit_code
  container_id="$(compose ps --all --quiet initialize)"
  test -n "$container_id" || die "single-node initializer container is missing"
  exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container_id")"
  test "$exit_code" = "0" \
    || die "single-node initializer failed with exit code $exit_code"
}

state_value() {
  local key="$1"
  local line=""
  if test -f "$state_file"; then
    line="$(grep -E "^${key}=" "$state_file" | tail -n 1 || true)"
  fi
  printf '%s' "${line#*=}"
}

record_release() {
  local prior_current prior_previous next_previous deployed_at temporary
  mkdir -p "$state_root"
  prior_current="$(state_value current_revision)"
  prior_previous="$(state_value previous_revision)"
  if test -n "$prior_current" && test "$prior_current" != "$QT_RELEASE_REVISION"; then
    next_previous="$prior_current"
  else
    next_previous="$prior_previous"
  fi
  deployed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  temporary="$(mktemp "$state_root/release.XXXXXX")"
  chmod 0600 "$temporary"
  {
    printf 'current_revision=%s\n' "$QT_RELEASE_REVISION"
    printf 'current_source_tree_hash=%s\n' "$QT_SOURCE_TREE_HASH"
    printf 'previous_revision=%s\n' "$next_previous"
    printf 'deployed_at=%s\n' "$deployed_at"
  } >"$temporary"
  mv "$temporary" "$state_file"
  printf '{"deployed_at":"%s","revision":"%s","source_tree_hash":"%s","previous_revision":"%s"}\n' \
    "$deployed_at" "$QT_RELEASE_REVISION" "$QT_SOURCE_TREE_HASH" "$next_previous" \
    >>"$history_file"
}

show_release() {
  if ! test -f "$state_file"; then
    echo "No successful release has been recorded at $state_file"
    return 0
  fi
  sed -n \
    -e 's/^current_revision=/current revision: /p' \
    -e 's/^current_source_tree_hash=/source tree hash: /p' \
    -e 's/^previous_revision=/previous revision: /p' \
    -e 's/^deployed_at=/deployed at: /p' \
    "$state_file"
}

run_doctor() {
  require_runtime
  compute_release_material
  compose config --quiet
  local available_bytes
  available_bytes="$(df -B1 --output=avail "$data_root" | tail -n 1 | tr -d ' ')"
  echo "Single-node deployment prerequisites are valid."
  echo "Revision: $QT_RELEASE_REVISION"
  echo "Source tree hash: $QT_SOURCE_TREE_HASH"
  echo "Market-data root: $data_root"
  echo "Market-data available bytes: $available_bytes"
  echo "State root: $state_root"
  if test "$(env_value QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA)" != "false"; then
    echo "Public Coinbase OI, funding, trade, and L2 definitions will be enrolled without credentials."
    echo "Provider credentials are optional unless an authenticated enrollment or operation is selected."
  fi
  if test "$alerts_enabled" = "true"; then
    echo "Operator email alerting: enabled"
  else
    echo "Operator email alerting: disabled"
  fi
}

apply_alerting_configuration() {
  local deployed_revision
  require_runtime
  compute_release_material
  deployed_revision="$(state_value current_revision)"
  test -n "$deployed_revision" \
    || die "no successful release is recorded; deploy a reviewed revision first"
  test "$deployed_revision" = "$QT_RELEASE_REVISION" \
    || die "checkout revision $QT_RELEASE_REVISION does not match deployed revision $deployed_revision"

  compose config --quiet
  compose up --detach --no-deps --force-recreate --wait \
    --wait-timeout "${QT_DEPLOY_WAIT_SECONDS:-600}" grafana
  compose ps grafana
  if test "$alerts_enabled" = "true"; then
    echo "Operator email alerting applied to Grafana (enabled)."
  else
    echo "Operator email alerting removed from Grafana (disabled)."
  fi
}

deploy_release() {
  local requested_ref="${1:-}"
  require_runtime
  select_release "$requested_ref"
  echo "Deploying Quant-Trad revision $QT_RELEASE_REVISION"
  echo "Source tree hash $QT_SOURCE_TREE_HASH"
  compose config --quiet
  build_release_images
  compose up --detach --remove-orphans --wait \
    --wait-timeout "${QT_DEPLOY_WAIT_SECONDS:-600}"
  verify_initializer
  verify_release_image backend
  verify_release_image market-data-collector
  verify_release_image frontend
  verify_release_image frontend-v2
  compose exec -T backend /app/scripts/qt data collectors fleet >/dev/null
  record_release
  compose ps
  show_release
}

action="${1:-}"
shift || true

case "$action" in
  init-env)
    initialize_operator_environment
    ;;
  doctor)
    run_doctor
    ;;
  validate-alerts)
    require_command python3
    require_command stat
    test -f "$env_file" || die "operator environment file not found: $env_file"
    validate_private_env_file
    validate_alerting_environment
    echo "Operator alert configuration is valid (enabled=$alerts_enabled)."
    ;;
  apply-alerts)
    apply_alerting_configuration
    ;;
  deploy)
    deploy_release "${1:-}"
    ;;
  rollback)
    rollback_ref="${1:-$(state_value previous_revision)}"
    test -n "$rollback_ref" \
      || die "no previous revision is recorded; supply an explicit git ref"
    deploy_release "$rollback_ref"
    ;;
  config)
    require_runtime
    compute_release_material
    compose config --quiet
    echo "Single-node Compose configuration is valid for $QT_RELEASE_REVISION"
    ;;
  release)
    show_release
    ;;
  status)
    require_runtime
    compute_release_material
    compose ps
    ;;
  fleet)
    require_runtime
    compute_release_material
    compose exec -T backend /app/scripts/qt data collectors fleet
    ;;
  qt)
    require_runtime
    compute_release_material
    test "$#" -gt 0 || die "qt action requires at least one qt argument"
    compose exec -T backend /app/scripts/qt "$@"
    ;;
  credentials-coinbase)
    require_runtime
    compute_release_material
    if test "$#" -eq 0; then
      compose exec backend /app/scripts/qt setup provider coinbase
    else
      compose exec -T backend /app/scripts/qt setup provider coinbase "$@"
    fi
    ;;
  logs)
    require_runtime
    compute_release_material
    if test -n "${1:-}"; then
      compose logs --tail "${QT_LOG_TAIL:-240}" --follow "$1"
    else
      compose logs --tail "${QT_LOG_TAIL:-240}" --follow
    fi
    ;;
  stop)
    require_runtime
    compute_release_material
    compose stop
    ;;
  *)
    usage
    exit 2
    ;;
esac
