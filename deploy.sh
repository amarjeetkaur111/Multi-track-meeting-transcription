#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <num_containers>"
  echo "Example: $0 6"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

NUM_CONTAINERS="$1"
if ! [[ "$NUM_CONTAINERS" =~ ^[0-9]+$ ]] || [[ "$NUM_CONTAINERS" -le 0 ]]; then
  echo "Error: num_containers must be a positive integer"
  exit 1
fi

mapfile -t GPU_IDS < <(nvidia-smi --query-gpu=index --format=csv,noheader | tr -d ' ')

GPU_COUNT="${#GPU_IDS[@]}"
if [[ "$GPU_COUNT" -eq 0 ]]; then
  echo "Error: no GPUs found via nvidia-smi"
  exit 1
fi

# Launch NUM_CONTAINERS stacks, assigning GPUs in round-robin fashion.
for ((i=0; i<NUM_CONTAINERS; i++)); do
  GPU_ID="${GPU_IDS[$(( i % GPU_COUNT ))]}"
  STACK_NAME="whisper${i}"

  echo "Starting stack ${STACK_NAME} on GPU ${GPU_ID} (container $((i+1))/${NUM_CONTAINERS})"
  GPU_ID="$GPU_ID" docker compose -p "${STACK_NAME}" up -d
done

