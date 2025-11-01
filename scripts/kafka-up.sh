#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_dir="${repo_root}/__test__/integration"

if [[ ! -d "${compose_dir}" ]]; then
  echo "Error: expected integration directory at ${compose_dir}" >&2
  exit 1
fi

choose_compose() {
  if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    echo "docker compose"
    return
  fi

  if command -v docker-compose >/dev/null 2>&1; then
    echo "docker-compose"
    return
  fi

  if command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
    echo "podman compose"
    return
  fi

  if command -v podman-compose >/dev/null 2>&1; then
    echo "podman-compose"
    return
  fi

  echo ""
}

compose_cmd="$(choose_compose)"

if [[ -z "${compose_cmd}" ]]; then
  echo "Error: neither docker-compose, docker compose, podman compose, nor podman-compose is available." >&2
  exit 1
fi

IFS=' ' read -r -a compose_parts <<< "${compose_cmd}"

echo "Using '${compose_cmd}' to start Kafka test environment..."

if ! (cd "${compose_dir}" && "${compose_parts[@]}" up -d); then
  if [[ "${compose_parts[0]}" == podman* ]]; then
    echo "Failed to bring up containers with Podman. Ensure Podman is running (try 'podman machine start')." >&2
  fi
  exit 1
fi

echo "Kafka test environment is starting."
