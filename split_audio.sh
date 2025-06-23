#!/bin/bash

if ! command -v ffmpeg &> /dev/null
then
    echo "ffmpeg is not installed."
    exit 1
fi

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <audio_file>"
    exit 1
fi

input_file="$1"
base_name="$(basename -- "$input_file" .${input_file##*.})"
output_dir="/app/chunks/${base_name}"  # Updated chunk storage location
rm -rf "$output_dir"
mkdir -p "$output_dir"

# Maximum allowed file size in bytes (25MB)
MAX_SIZE=$((25 * 1024 * 1024))

# Determine how many parts to split a file into based on its size
calc_parts() {
    local size=$1
    if (( size > 75 * 1024 * 1024 )); then
        echo 4
    elif (( size > 50 * 1024 * 1024 )); then
        echo 3
    elif (( size > MAX_SIZE )); then
        echo 2
    else
        echo 1
    fi
}

# Split a chunk file into the specified number of parts using nearby 2s silence
split_file_parts() {
    local file="$1"
    local parts="$2"

    local base=$(basename "$file")
    local dir=$(dirname "$file")
    local start_ms=$(echo "$base" | cut -d'_' -f1)
    local idx=$(echo "$base" | cut -d'_' -f2 | cut -d'.' -f1)

    echo "Processing $file (split into $parts parts)"

    local duration=$(ffprobe -v error -show_entries format=duration \
        -of default=noprint_wrappers=1:nokey=1 "$file")
    local step=$(echo "$duration / $parts" | bc -l)

    # Detect silences and compute their mid points
    local silence_info=$(ffmpeg -y -i "$file" -af "silencedetect=noise=-25dB:d=2" \
        -f null - 2>&1 | grep -oP 'silence_(start|end): \K[0-9]+\.?[0-9]*')
    local silence_times=($silence_info)
    local mids=()
    for ((i=0; i<${#silence_times[@]}; i+=2)); do
        if [[ -n ${silence_times[i+1]} ]]; then
            mids+=( $(echo "(${silence_times[i]} + ${silence_times[i+1]})/2" | bc -l) )
        fi
    done

    local cut_times=()
    for ((n=1; n<parts; n++)); do
        local target=$(echo "$step * $n" | bc -l)
        local nearest=$target
        local mindiff=
        for m in "${mids[@]}"; do
            local diff=$(echo "$m - $target" | bc -l)
            diff=${diff#-}
            if [[ -z $mindiff || $(echo "$diff < $mindiff" | bc -l) -eq 1 ]]; then
                mindiff=$diff
                nearest=$m
            fi
        done
        cut_times+=( "$nearest" )
    done

    local start=0
    local chunk_num=1
    for ct in "${cut_times[@]}" "$duration"; do
        local part_dur=$(echo "$ct - $start" | bc -l)
        local out_start_ms=$(echo "$start_ms + ($start * 1000)" | bc | cut -d'.' -f1)
        local out_file="$dir/${out_start_ms}_${idx}_${chunk_num}.ogg"
        echo " - creating $out_file (start ${start}s, dur ${part_dur}s)"
        ffmpeg -y -i "$file" -ss "$start" -t "$part_dur" -c:a libvorbis "$out_file" >/dev/null 2>&1
        start=$ct
        ((chunk_num++))
    done
    rm -f "$file"
}

# Ensure a file does not exceed MAX_SIZE
ensure_size() {
    local f="$1"
    local size=$(stat -c%s "$f")
    local parts=$(calc_parts $size)
    if (( parts > 1 )); then
        echo "File $f is $size bytes - splitting into $parts parts"
        split_file_parts "$f" "$parts"
    fi
}

silence_info=$(ffmpeg -y -i "$input_file" -af "silencedetect=noise=-25dB:d=10" -f null - 2>&1 | \
    grep -oP 'silence_(start|end): \K[0-9]+\.?[0-9]*')

declare -a silence_times=($silence_info)
num_silence=${#silence_times[@]}

if (( num_silence % 2 != 0 )); then
    echo "Error: Unmatched silence start/end pairs detected."
    exit 1
fi

start_time=0
chunk_index=1
max_jobs=$(nproc)

job_count=0
for (( i=0; i<num_silence; i+=2 )); do
    silence_start=${silence_times[i]}
    silence_end=${silence_times[i+1]}
    duration=$(echo "$silence_start - $start_time" | bc)

    start_time_us=$(echo "$start_time * 1000" | bc | cut -d'.' -f1)  # Convert to milliseconds
    output_file="$output_dir/${start_time_us}_${chunk_index}.ogg"  # Correct file naming

    if (( $(echo "$duration > 0" | bc -l) )); then
        ffmpeg -y -i "$input_file" -ss "$start_time" -t "$duration" -c:a libvorbis "$output_file" &
        ((job_count++))
    fi

    if (( job_count >= max_jobs )); then
        wait -n
        ((job_count--))
    fi

    start_time=$silence_end
    ((chunk_index++))
done

final_duration=$(ffmpeg -i "$input_file" 2>&1 | grep "Duration" | awk '{print $2}' | tr -d , | awk -F: '{ print ($1 * 3600) + ($2 * 60) + $3 }')
remaining_duration=$(echo "$final_duration - $start_time" | bc)
start_time_us=$(echo "$start_time * 1000" | bc | cut -d'.' -f1)
output_file="$output_dir/${start_time_us}_${chunk_index}.ogg"

if (( $(echo "$remaining_duration > 0" | bc -l) )); then
    ffmpeg -y -i "$input_file" -ss "$start_time" -c:a libvorbis "$output_file" &
fi


wait

for f in "$output_dir"/*.ogg; do
    if [ ! -s "$f" ]; then
        echo "Warning: removing zero byte chunk $f"
        rm -f "$f"
        continue
    fi
    ensure_size "$f"
done

echo "Audio split successfully into '$output_dir'"
