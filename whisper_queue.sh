#!/bin/bash

REDIS_CLI="redis-cli -h redis -p 6379"
LOCK_PREFIX="lock:"
QUEUE_HIGH="whisper_high"
QUEUE_LOW="whisper_low"

while true; do
    # First, try to pop a file from the high-priority queue
    FILE_ID_HIGH=$($REDIS_CLI RPOP "$QUEUE_HIGH")
    if [ -n "$FILE_ID_HIGH" ]; then
        FILE_ID="$FILE_ID_HIGH"
        QUEUE_FROM="$QUEUE_HIGH"
    else
        # If no file from high, then pop from low
        FILE_ID_LOW=$($REDIS_CLI RPOP "$QUEUE_LOW")
        if [ -n "$FILE_ID_LOW" ]; then
            FILE_ID="$FILE_ID_LOW"
            QUEUE_FROM="$QUEUE_LOW"
        else
            echo "No files to transcribe."
            sleep 10
            continue
        fi
    fi

    # Acquire a lock to ensure only one process handles this file at a time
    LOCK_KEY="${LOCK_PREFIX}${FILE_ID}"
    LOCK_ACQUIRED=$($REDIS_CLI SET "$LOCK_KEY" "locked" NX EX 300)

    if [ "$LOCK_ACQUIRED" != "OK" ]; then
        echo "File $FILE_ID is already being transcribed. Skipping."
        continue
    fi

    # Verify the audio file exists
    AUDIO_FILE="/app/queue/${FILE_ID}.ogg"
    if [ ! -f "$AUDIO_FILE" ]; then
        echo "Audio file $FILE_ID does not exist. Removing lock."
        $REDIS_CLI DEL "$LOCK_KEY"
        continue
    fi

    # Perform the transcription (e.g., using Whisper)
    python3 /app/process_audio.py "$AUDIO_FILE"
    # this python3 command will take 10 minutes
    TRANSCRIBE_EXIT_CODE=$?

    if [ $TRANSCRIBE_EXIT_CODE -eq 0 ]; then
        echo "Successfully transcribed $FILE_ID (from $QUEUE_FROM)."
        # If you want to do anything specific based on QUEUE_FROM, you can add it here.
    else
        echo "Transcription failed for $FILE_ID (exit code: $TRANSCRIBE_EXIT_CODE)."
        # Optionally handle failures (e.g., push to 'failed' queue) as desired.
    fi

    echo "Running merge_speakers for $FILE_ID..."
    python3 /app/merge_speakers.py \
        "/raw/${FILE_ID}/events.xml" \
        "/transcripts/scripts/${FILE_ID}.txt" \
        "/transcripts/scripts/${FILE_ID}_speakers.txt"


    MERGE_EXIT_CODE=$?

    if [ $MERGE_EXIT_CODE -eq 0 ]; then
        echo "merge_speakers succeeded for $FILE_ID"
    else
        echo "merge_speakers failed for $FILE_ID (exit code: $MERGE_EXIT_CODE)"
    fi
    
    # Release the lock
    $REDIS_CLI DEL "$LOCK_KEY"
done
