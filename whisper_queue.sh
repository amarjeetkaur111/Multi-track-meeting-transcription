#!/bin/bash

REDIS_CLI="redis-cli -h redis -p 6379"
LOCK_PREFIX="lock:"
QUEUE_HIGH="whisper_high"
QUEUE_LOW="whisper_low"
PAUSE_FILE="/app/pause_flag"

process_once() {
    local BACKEND="$1"

    # --- PAUSE CHECK ---
    if [ -f "$PAUSE_FILE" ]; then
        echo "Whisper paused. No new tasks will be picked."
        sleep 5
        return
    fi
    # --- END PAUSE CHECK ---

    # First, try to pop a file from the high-priority queue
    FILE_ID_HIGH=$($REDIS_CLI RPOP "$QUEUE_HIGH")
    if [ -n "$FILE_ID_HIGH" ]; then
        FILE_ID="$FILE_ID_HIGH"
    else
        # If no file from high, then pop from low
        FILE_ID_LOW=$($REDIS_CLI RPOP "$QUEUE_LOW")
        if [ -n "$FILE_ID_LOW" ]; then
            FILE_ID="$FILE_ID_LOW"
        else
            echo "No files to transcribe."
            sleep 10
            return
        fi
    fi

    # Acquire a lock to ensure only one process handles this file at a time
    LOCK_KEY="${LOCK_PREFIX}${FILE_ID}"
    LOCK_ACQUIRED=$($REDIS_CLI SET "$LOCK_KEY" "locked" NX EX 600)

    if [ "$LOCK_ACQUIRED" != "OK" ]; then
        echo "File $FILE_ID is already locked. Skipping."
        return
    fi

    # Verify the audio file exists
    AUDIO_FILE="/app/queue/${FILE_ID}.ogg"
    TXT_FILE="/transcripts/scripts/${FILE_ID}.txt"
    SRT_FILE="/transcripts/scripts/${FILE_ID}.srt"
    SPEAKERS_FILE="/transcripts/scripts/${FILE_ID}_speakers.txt"
    SUMMARY_FILE="/transcripts/scripts/${FILE_ID}_summary.txt"

    if [ ! -f "$AUDIO_FILE" ]; then
        echo "Audio file $FILE_ID does not exist. Removing lock."
        $REDIS_CLI DEL "$LOCK_KEY"
        return
    fi

    # Split audio before processing
    /app/split_audio.sh "$AUDIO_FILE"
    if [ $? -ne 0 ]; then
        echo "Splitting failed for $FILE_ID. Removing lock."
        $REDIS_CLI DEL "$LOCK_KEY"
        return
    fi

    FORCE_PROCESS=$($REDIS_CLI GET "force_process:${FILE_ID}")

   # Step 1: Transcription
    TRANSCRIBE_EXIT_CODE=0
    if [ "$FORCE_PROCESS" == "1" ]; then
        echo "Force reprocessing $FILE_ID: doing fresh transcription..."
        WHISPER_BACKEND="$BACKEND" python3 /app/process_audio.py "$AUDIO_FILE" --force
        TRANSCRIBE_EXIT_CODE=$?
    elif [ -f "$TXT_FILE" ] && [ -f "$SRT_FILE" ]; then
        echo "Transcription files already exist for $FILE_ID, skipping transcription."
    else
        echo "transcribing using $BACKEND $FILE_ID..."
        WHISPER_BACKEND="$BACKEND" python3 /app/process_audio.py "$AUDIO_FILE"
        TRANSCRIBE_EXIT_CODE=$?
    fi

    if [ $TRANSCRIBE_EXIT_CODE -ne 0 ]; then
        echo "Transcription failed for $FILE_ID, skipping merging/summarizing."
        $REDIS_CLI DEL "$LOCK_KEY"
        return
    fi


    # Checking size of the TXT file
    if [ -f "$TXT_FILE" ]; then
        size_bytes=$(stat -c%s "$TXT_FILE") #  get size in bytes
        size_kb=$(awk "BEGIN { printf \"%.2f\", $size_bytes/1024 }") #convert to kilobytes (decimal KB)
        size_mb=$(awk "BEGIN { printf \"%.2f\", $size_bytes/1024/1024 }")  # convert to megabytes (decimal MB)
        echo "Transcript size for $FILE_ID: ${size_mb} MB (${size_kb} KB)"
    fi

   # Step 2: Merge Speakers
    MERGE_EXIT_CODE=0
    if [ "$FORCE_PROCESS" == "1" ]; then
        echo "Force reprocessing $FILE_ID: doing fresh speaker merging..."
        python3 /app/merge_speakers.py \
            "/raw/${FILE_ID}/events.xml" \
            "$TXT_FILE" \
            "$SPEAKERS_FILE" \
            "/transcripts/scripts/${FILE_ID}_chat.txt"
        MERGE_EXIT_CODE=$?
    elif [ -f "$SPEAKERS_FILE" ]; then
        echo "Speakers file already exists for $FILE_ID, skipping merging."
    else
        echo "Speakers file missing, merging speakers for $FILE_ID..."
        python3 /app/merge_speakers.py \
            "/raw/${FILE_ID}/events.xml" \
            "$TXT_FILE" \
            "$SPEAKERS_FILE" \
            "/transcripts/scripts/${FILE_ID}_chat.txt"
        MERGE_EXIT_CODE=$?
    fi


    # Step 3: GPT Summarization
    SUMMARY_EXIT_CODE=0
    if [ -f "$SUMMARY_FILE" ] && [ "$FORCE_PROCESS" != "1" ]; then
        echo "Summary file already exists for $FILE_ID, skipping summarization."
    else
        if [ -f "$SPEAKERS_FILE" ]; then
            word_count=$(wc -w < "$SPEAKERS_FILE")
        else
            word_count=0
        fi

        if [ "$word_count" -lt 500 ]; then
            echo "Transcript has only $word_count words. Skipping summarization."
            echo "File was too small to summarize." > "$SUMMARY_FILE"
            SUMMARY_EXIT_CODE=0
        else
            if [ "$FORCE_PROCESS" == "1" ]; then
                echo "Force reprocessing $FILE_ID: generating fresh summary..."
            else
                echo "Summary file missing, generating summary for $FILE_ID..."
            fi
            python3 /app/gpt_summary.py "$FILE_ID"
            SUMMARY_EXIT_CODE=$?
        fi
    fi

    if [ $SUMMARY_EXIT_CODE -eq 0 ]; then
        echo "✅ Summary generated for $FILE_ID."
    else
        echo "❌ Summary failed for $FILE_ID."
    fi

    # Release the lock
    $REDIS_CLI DEL "$LOCK_KEY"
    $REDIS_CLI DEL "force_process:${FILE_ID}"
}

worker_loop() {
    local backend="$1"
    while true; do
        if [ "$backend" = "azure" ]; then
            local low_len=$($REDIS_CLI LLEN "$QUEUE_LOW")
            if [ "$low_len" -le 20 ]; then
                sleep 10
                continue
            fi
        fi
        process_once "$backend"
    done
}

worker_loop open_source &
worker_loop azure &

wait -n
