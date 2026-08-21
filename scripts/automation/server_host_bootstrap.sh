#!/usr/bin/env bash
set -euo pipefail

die() {
  echo "server host bootstrap failed: $*" >&2
  exit 1
}

test -r /etc/os-release || die "/etc/os-release is unavailable"
# shellcheck disable=SC1091
. /etc/os-release
test "${ID:-}" = "ubuntu" || die "this bootstrap supports Ubuntu only"

deploy_user="${SUDO_USER:-${USER:-}}"
test -n "$deploy_user" || die "could not determine deployment account"
test "$deploy_user" != "root" || die "run this as the deployment account, not root"

install_root="${QT_SINGLE_NODE_ROOT:-/srv/quanttrad}"
[[ "$install_root" = /* ]] || die "QT_SINGLE_NODE_ROOT must be absolute"
test "$install_root" != "/" || die "QT_SINGLE_NODE_ROOT cannot be /"

conflicting_packages=(
  docker.io
  docker-compose
  docker-compose-v2
  docker-doc
  podman-docker
  containerd
  runc
)
for package in "${conflicting_packages[@]}"; do
  if dpkg-query -W -f='${db:Status-Abbrev}' "$package" 2>/dev/null \
    | grep -q '^ii '; then
    die "remove conflicting package before continuing: $package"
  fi
done

echo "One sudo authentication installs Docker and prepares $install_root."
sudo -v
sudo apt-get update
sudo apt-get install -y ca-certificates curl git
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

architecture="$(dpkg --print-architecture)"
codename="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
test -n "$codename" || die "could not determine Ubuntu codename"

repository_definition="$(mktemp)"
trap 'rm -f "$repository_definition"' EXIT
cat >"$repository_definition" <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $codename
Components: stable
Architectures: $architecture
Signed-By: /etc/apt/keyrings/docker.asc
EOF
sudo install -m 0644 "$repository_definition" /etc/apt/sources.list.d/docker.sources

sudo apt-get update
sudo apt-get install -y \
  docker-ce \
  docker-ce-cli \
  containerd.io \
  docker-buildx-plugin \
  docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$deploy_user"

deploy_group="$(id -gn "$deploy_user")"
sudo install -d -m 0750 -o "$deploy_user" -g "$deploy_group" \
  "$install_root" \
  "$install_root/app" \
  "$install_root/market-structure" \
  "$install_root/deploy-state" \
  "$install_root/backups"

echo "Host bootstrap complete at $install_root."
echo "Log out and back in so Docker group access applies."
