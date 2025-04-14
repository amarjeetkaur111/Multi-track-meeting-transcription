#!/bin/bash

REDIS_CLI="redis-cli -h redis -p 6379"
LOCK_PREFIX="lock:"
QUEUE_HIGH="split_high"
QUEUE_LOW="split_low"
WHISPER_HIGH="whisper_high"
WHISPER_LOW="whisper_low"

while true; do
    # Try to pop from the high-priority queue first
    FILE_ID_HIGH=$($REDIS_CLI RPOP $QUEUE_HIGH 2>/dev/null)
    if [[ -n "$FILE_ID_HIGH" ]]; then
        FILE_ID="$FILE_ID_HIGH"
        QUEUE_FROM="$QUEUE_HIGH"
    else
        # If nothing from high, try low
        FILE_ID_LOW=$($REDIS_CLI RPOP $QUEUE_LOW 2>/dev/null)
        if [[ -n "$FILE_ID_LOW" ]]; then
            FILE_ID="$FILE_ID_LOW"
            QUEUE_FROM="$QUEUE_LOW"
        else
            # If no files in either queue, wait before retrying
            echo "No files to split."
            sleep 10
            continue
        fi
    fi

    # Acquire a lock, ensuring only one process handles a file at a time
    LOCK_KEY="${LOCK_PREFIX}${FILE_ID}"
    LOCK_ACQUIRED=$($REDIS_CLI SET "$LOCK_KEY" "locked" NX EX 300 2>/dev/null)

    if [[ "$LOCK_ACQUIRED" != "OK" ]]; then
        echo "File $FILE_ID is already being processed. Skipping."
        sleep 2  # Avoid rapid retries
        continue
    fi

    # Check if the audio file actually exists
    AUDIO_FILE="/app/queue/${FILE_ID}.ogg"
    if [[ ! -f "$AUDIO_FILE" ]]; then
        echo "Audio file $FILE_ID does not exist. Removing lock."
        $REDIS_CLI DEL "$LOCK_KEY" >/dev/null
        continue
    fi

    # Perform the splitting
    /app/split_audio.sh "$AUDIO_FILE"
    SPLIT_EXIT_CODE=$?

    if [[ $SPLIT_EXIT_CODE -eq 0 ]]; then
        # Decide which queue to move to based on the original queue
        if [[ "$QUEUE_FROM" == "$QUEUE_HIGH" ]]; then
            QUEUE_DEST="$WHISPER_HIGH"
        else
            QUEUE_DEST="$WHISPER_LOW"
        fi

        $REDIS_CLI LPUSH "$QUEUE_DEST" "$FILE_ID" >/dev/null
        echo "Splitting succeeded for $FILE_ID. Moved to $QUEUE_DEST"
    else
        echo "Splitting failed for $FILE_ID (exit code: $SPLIT_EXIT_CODE)."
        # Consider adding a retry mechanism or a failure queue
    fi

    # Remove the lock so another process can handle subsequent steps
    $REDIS_CLI DEL "$LOCK_KEY" >/dev/null
done
