#!/bin/sh
set -eu

interval_seconds="${QT_DOCKER_CAPACITY_INTERVAL_SECONDS:-15}"
storage_mount="${QT_DOCKER_STORAGE_MOUNT:-/host-docker}"
case "$interval_seconds" in
  *[!0-9]*|"") interval_seconds=15 ;;
esac
if [ "$interval_seconds" -lt 5 ]; then
  interval_seconds=5
fi

emit_container_samples() {
  observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  container_ids="$(docker ps --filter label=loki.job=quanttrad --format '{{.ID}}')"
  if [ -z "$container_ids" ]; then
    return
  fi

  docker stats --no-stream --format '{{.Name}}	{{.CPUPerc}}	{{.MemPerc}}	{{.PIDs}}' $container_ids |
  while IFS="$(printf '\t')" read -r container_name cpu_percent memory_percent pids; do
    cpu_percent="${cpu_percent%%%}"
    memory_percent="${memory_percent%%%}"
    case "$cpu_percent" in *[!0-9.]*|"") cpu_percent=0 ;; esac
    case "$memory_percent" in *[!0-9.]*|"") memory_percent=0 ;; esac
    case "$pids" in *[!0-9]*|"") pids=0 ;; esac
    printf '{"observed_at":"%s","sample_kind":"container","container_name":"%s","cpu_percent":%s,"memory_percent":%s,"pids":%s}\n' \
      "$observed_at" "$container_name" "$cpu_percent" "$memory_percent" "$pids"
  done
}

emit_filesystem_sample() {
  observed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  operating_system="$(docker info --format '{{.OperatingSystem}}' 2>/dev/null || true)"
  kernel_version="$(docker info --format '{{.KernelVersion}}' 2>/dev/null || true)"
  engine_root_dir="$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || true)"
  runtime_probe="$(printf '%s %s' "$operating_system" "$kernel_version" | tr '[:upper:]' '[:lower:]')"
  runtime_kind="native_linux"
  capacity_authority="engine_storage_filesystem"
  physical_host_visible=true
  case "$runtime_probe" in
    *docker\ desktop*|*microsoft*|*wsl*)
      runtime_kind="virtualized_docker_desktop"
      capacity_authority="virtual_guest_storage"
      physical_host_visible=false
      ;;
    " ")
      runtime_kind="unknown"
      capacity_authority="unknown"
      physical_host_visible=false
      ;;
  esac
  if [ ! -d "$storage_mount" ]; then
    printf '{"observed_at":"%s","sample_kind":"capacity_unavailable","resource_id":"docker-engine-storage","capacity_scope":"docker_engine_storage","capacity_authority":"%s","physical_host_visible":%s,"runtime_kind":"%s","reason":"configured storage mount is unavailable"}\n' \
      "$observed_at" "$capacity_authority" "$physical_host_visible" "$runtime_kind"
    return
  fi
  set -- $(df -Pk "$storage_mount" | awk 'NR == 2 { print $2, $3, $4, $5 }')
  if [ "$#" -ne 4 ]; then
    return
  fi
  total_bytes=$(( $1 * 1024 ))
  used_bytes=$(( $2 * 1024 ))
  available_bytes=$(( $3 * 1024 ))
  used_percent="${4%%%}"
  printf '{"observed_at":"%s","sample_kind":"filesystem","resource_id":"docker-engine-storage","capacity_scope":"docker_engine_storage","capacity_authority":"%s","physical_host_visible":%s,"runtime_kind":"%s","engine_root_dir":"%s","total_bytes":%s,"used_bytes":%s,"available_bytes":%s,"used_percent":%s}\n' \
    "$observed_at" "$capacity_authority" "$physical_host_visible" "$runtime_kind" "$engine_root_dir" "$total_bytes" "$used_bytes" "$available_bytes" "$used_percent"
}

trap 'exit 0' INT TERM

while true; do
  emit_container_samples || printf '{"sample_kind":"sampler_error","stage":"docker_stats"}\n'
  emit_filesystem_sample || printf '{"sample_kind":"sampler_error","stage":"filesystem"}\n'
  sleep "$interval_seconds"
done
