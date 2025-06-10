#!/usr/bin/env bash

CHECK_INTERVAL=30  # Seconds between checks
MAX_RETRIES=3      # Max consecutive failures before restart
FAILURE_COUNT=0    # Track consecutive failures
CONTAINER_NAME="whisper-python-worker"  # Must match your container name
LOG_FILE="/app/gpu_watchdog.log"

# Ensure log file exists
touch "$LOG_FILE"

# Function to log messages with timestamp
log_message() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [GPU Watchdog] $message" | tee -a "$LOG_FILE"
}

while true; do
    # Check GPU availability using PyTorch
    python3 -c "import torch; exit(0 if torch.cuda.is_available() else 1)" 2>/dev/null
    GPU_OK=$?

    if [ "$GPU_OK" -eq 0 ]; then
        # GPU is available, reset failure count
        FAILURE_COUNT=0
        # Log success periodically (every 5th check to reduce noise)
        if [ $(( RANDOM % 5 )) -eq 0 ]; then
            log_message "GPU is available."
        fi
    else
        # GPU not available, increment failure count
        ((FAILURE_COUNT++))
        log_message "GPU not available! Failure $FAILURE_COUNT/$MAX_RETRIES"

        # Restart container if max retries reached
        if [ "$FAILURE_COUNT" -ge "$MAX_RETRIES" ]; then
            log_message "Max retries reached. Restarting container '$CONTAINER_NAME'..."
            docker restart "$CONTAINER_NAME"
            FAILURE_COUNT=0
        fi
    fi

    sleep "$CHECK_INTERVAL"
done
