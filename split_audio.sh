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
mkdir -p "$output_dir"

silence_info=$(ffmpeg -y -i "$input_file" -af "silencedetect=noise=-25dB:d=6" -f null - 2>&1 | \
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
        ffmpeg -i "$input_file" -ss "$start_time" -t "$duration" -c:a libvorbis "$output_file" &
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
    ffmpeg -i "$input_file" -ss "$start_time" -c:a libvorbis "$output_file" &
fi

wait

echo "Audio split successfully into '$output_dir'"