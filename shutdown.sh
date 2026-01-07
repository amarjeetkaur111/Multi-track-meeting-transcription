#!/usr/bin/env bash
set -euo pipefail

# Find all docker compose projects that start with "whisper" and end with digits, then stop them.
mapfile -t PROJECTS < <(
  docker compose ls --format json \
  | jq -r '.[].Name' \
  | grep -E '^whisper[0-9]+$' \
  | sort -V
)

if [[ "${#PROJECTS[@]}" -eq 0 ]]; then
  echo "No whisper#!/usr/bin/env bash
set -euo pipefail

# Find all docker compose projects that start with "whisper" and end with digits, then stop them.
mapfile -t PROJECTS < <(
  docker compose ls --format json \
  | jq -r '.[].Name' \
  | grep -E '^whisper[0-9]+$' \
  | sort -V
)

if [[ "${#PROJECTS[@]}" -eq 0 ]]; then
  echo "No whisper<N> compose projects found."
  exit 0
fi

for p in "${PROJECTS[@]}"; do
  echo "Stopping project ${p}"
  docker compose -p "${p}" down --remove-orphans
done
<N> compose projects found."
  exit 0
fi

for p in "${PROJECTS[@]}"; do
  echo "Stopping project ${p}"
  docker compose -p "${p}" down --remove-orphans
done

