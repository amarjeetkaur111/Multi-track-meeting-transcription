import os
import sys
import subprocess
import srt
import datetime
import shutil
import re
import requests
import torch
from dotenv import load_dotenv
from pathlib import Path

load_dotenv() 

WHISPER_BACKEND = os.getenv("WHISPER_BACKEND", "open_source").lower()
print(f"Env Value: {WHISPER_BACKEND}")  

if WHISPER_BACKEND == "azure":
    from openai import AzureOpenAI
    print("Using Azure Whisper backend")
else:
    import whisper
    print("Using local Whisper backend")



if len(sys.argv) < 2:
    print("Usage: python process_audio.py <audio_file>")
    sys.exit(1)
    

# Ensure /transcripts/scripts/ exists before copying final files
os.makedirs("/transcripts/scripts", exist_ok=True)
os.makedirs("/transcripts/done", exist_ok=True)

input_file = sys.argv[1]
base_name = os.path.splitext(os.path.basename(input_file))[0]
chunk_dir = f"/app/chunks/{base_name}"  # Store chunks in /app/chunks/<audio_filename>/
output_srt = f"/app/scripts/{base_name}.srt"
output_txt = f"/app/scripts/{base_name}.txt"
queue_file = Path(os.getenv("TRANSCRIPTS_QUEUE")) /f"{base_name}.txt"
done_file = Path(os.getenv("TRANSCRIPTS_DONE")) /f"{base_name}.txt"
final_srt_transcripts = Path(os.getenv("TRANSCRIPTS_FOLDER")) /f"{base_name}.srt"
final_txt_transcripts = Path(os.getenv("TRANSCRIPTS_FOLDER")) /f"{base_name}.txt"


# Laravel webhook URL
# WEBHOOK_URL = "https://540c-86-98-4-252.ngrok-free.app/mobile/webhook/audio-processed"
# WEBHOOK_URL = "https://biggerbluebutton.com/mobile/webhook/audio-processed"

# Step 1: Downloaded audio file is already placed in /app/queue and tracked in /transcripts/queue

# Step 2: Split the audio file into chunks
# subprocess.run(["/app/split_audio.sh", input_file], check=True)

# Step 3: Process each chunk with Whisper AI or Azure Whisper
srt_files = []

if WHISPER_BACKEND == "azure":
    azure_client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_WHISPER_ENDPOINT", "").rstrip("/"),
        api_key=os.getenv("AZURE_WHISPER_KEY"),
        api_version=os.getenv("AZURE_WHISPER_API_VERSION"),
    )
    azure_deployment = os.getenv("AZURE_WHISPER_DEPLOYMENT")
else:
    model = whisper.load_model(
        "turbo",
        download_root="/root/.cache/whisper",
        device="cuda",
    )

for chunk in sorted(os.listdir(chunk_dir)):
    chunk_path = os.path.join(chunk_dir, chunk)
    chunk_base_name = os.path.splitext(chunk)[0]
    srt_path = os.path.join(chunk_dir, f"{chunk_base_name}.srt")  # Save SRT alongside chunk

    if not chunk.endswith(".ogg"):  # Skip non-audio files
        continue

    if WHISPER_BACKEND == "azure":
        with open(chunk_path, "rb") as audio_file:
            resp = azure_client.audio.transcriptions.create(
                file=audio_file,
                model=azure_deployment,
                response_format="verbose_json",
            )
        segments = resp.segments
    else:
        result = model.transcribe(chunk_path)
        segments = result["segments"]

    subs = []
    for i, segment in enumerate(segments):
        if isinstance(segment, dict):
            start = segment.get("start")
            end = segment.get("end")
            text = segment.get("text")
        else:
            start = getattr(segment, "start")
            end = getattr(segment, "end")
            text = getattr(segment, "text")

        start_time = datetime.timedelta(seconds=start)
        end_time = datetime.timedelta(seconds=end)
        subs.append(
            srt.Subtitle(index=i, start=start_time, end=end_time, content=text)
        )

    with open(srt_path, "w") as f:
        f.write(srt.compose(subs))

    srt_files.append(srt_path)

# Ensure there are SRT files before merging
if not srt_files:
    print(f"Error: No SRT files found for {base_name}. Skipping merge.")
    sys.exit(1)

# Sort SRT files only by timestamp (ignore index)
def extract_timestamp(filename):
    match = re.search(r"(\d+)_\d+\.srt$", filename)
    return int(match.group(1)) if match else float('inf')

srt_files = sorted(srt_files, key=lambda x: extract_timestamp(os.path.basename(x)))

# Debugging: Print sorted order to verify
print("Merging SRT files in order:", srt_files)

# Step 4: Merge all transcripts
subprocess.run(["python3", "/app/merge_transcripts.py", *srt_files, output_srt], check=True)

# Step 5: Convert SRT to TXT
def srt_to_custom_text(srt_file, output_file):
    with open(srt_file, 'r', encoding='utf-8') as file:
        content = file.read()

    srt_pattern = re.compile(r"(\d+)\n(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n(.+?)(?=\n\n|\Z)", re.DOTALL)
    formatted_lines = []

    for match in srt_pattern.finditer(content):
        start_time = match.group(2).replace(',', '.')
        end_time = match.group(3).replace(',', '.')
        text = ' '.join(match.group(4).splitlines())
        formatted_lines.append(f"[{start_time} {end_time}] {text}")

    with open(output_file, 'w', encoding='utf-8') as file:
        file.write("\n".join(formatted_lines))

srt_to_custom_text(output_srt, output_txt)

# Step 6: Copy final SRT and TXT to /transcripts/scripts
shutil.copy(output_srt, final_srt_transcripts)
shutil.copy(output_txt, final_txt_transcripts)

# Step 7: Move tracking file from /transcripts/queue to /transcripts/done
if os.path.exists(queue_file):
    shutil.move(queue_file, done_file)

print(f"Final transcript saved to {output_srt}")
print(f"Converted text file saved to {output_txt}")

# Step 8: Remove chunk directory after processing
if os.path.exists(chunk_dir):
    try:
        subprocess.run(['rm', '-rf', chunk_dir], check=True)  # Deletes the entire chunk directory
        print(f"Successfully removed chunk directory: {chunk_dir}")
    except Exception as e:
        print(f"Error removing chunk directory: {str(e)}")
    subprocess.run(['sync'])
