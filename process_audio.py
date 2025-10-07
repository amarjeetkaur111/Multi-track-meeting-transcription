import os
import subprocess
import srt
import datetime
import shutil
import re
from pathlib import Path
from typing import Optional

import torch
from dotenv import load_dotenv

from logger import log
from whisper_model import get_model

load_dotenv()


def process_file(
    input_file: str,
    model=None,
    *,
    destination_dir: Optional[Path] = None,
    final_basename: Optional[str] = None,
    finalize: bool = True,
    generate_txt: bool = True,
) -> Path:
    """Transcribe *input_file* using Whisper and write output files.

    Returns the path to the generated SRT file.  When *finalize* is False the
    caller is responsible for copying the results into their final location and
    no queue bookkeeping files are touched.
    """
    if model is None:
        model = get_model()

    base_name = os.path.splitext(os.path.basename(input_file))[0]
    final_base = final_basename or base_name
    log(f"Starting processing for {base_name}")

    chunk_dir = f"/app/chunks/{base_name}"
    scripts_dir = Path("/app/scripts")
    scripts_dir.mkdir(parents=True, exist_ok=True)
    output_srt = scripts_dir / f"{base_name}.srt"
    output_txt = scripts_dir / f"{base_name}.txt"

    final_dir = Path(destination_dir) if destination_dir else Path(os.getenv("TRANSCRIPTS_FOLDER", "/transcripts/scripts"))
    final_dir.mkdir(parents=True, exist_ok=True)
    final_srt_transcripts = final_dir / f"{final_base}.srt"
    final_txt_transcripts = final_dir / f"{final_base}.txt"

    queue_dir = Path(os.getenv("TRANSCRIPTS_QUEUE", "/transcripts/queue"))
    done_dir = Path(os.getenv("TRANSCRIPTS_DONE", "/transcripts/done"))
    queue_dir.mkdir(parents=True, exist_ok=True)
    done_dir.mkdir(parents=True, exist_ok=True)
    queue_file = queue_dir / f"{final_base}.txt"
    done_file = done_dir / f"{final_base}.txt"

    def transcribe_chunk(chunk_name: str):
        if not chunk_name.endswith(".ogg"):
            return None
        log(f"Transcribing {chunk_name}")
        chunk_path = os.path.join(chunk_dir, chunk_name)
        chunk_base = os.path.splitext(chunk_name)[0]
        srt_path = os.path.join(chunk_dir, f"{chunk_base}.srt")

        if os.path.getsize(chunk_path) == 0:
            log(f"Skipping zero-byte chunk {chunk_name}")
            return None

        try:
            result = model.transcribe(chunk_path)
        # ── differentiate fatal GPU errors from ordinary per‑chunk issues ──
        except (torch.cuda.CudaError,
                torch.cuda.OutOfMemoryError) as gpu_exc:
            # Let the caller decide; this will bubble up to run_pipeline,
            # which will force an os._exit(1) and restart the container.
            raise gpu_exc
        except Exception as exc:
            # Non‑GPU issue (e.g. corrupt audio) – just skip this chunk.
            log(f"Skipping corrupt chunk {chunk_name}: {exc}")
            return None
            
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
            subs.append(srt.Subtitle(index=i, start=start_time, end=end_time, content=text))

        with open(srt_path, "w") as f:
            f.write(srt.compose(subs))
        log(f"Generated {srt_path}")
        return srt_path

    srt_files = []
    chunk_list = [f for f in os.listdir(chunk_dir) if f.endswith(".ogg")]
    if not chunk_list:
        log(f"No audio chunks found for {base_name}")
        raise RuntimeError("no_audio")

    for chunk_name in sorted(chunk_list):
        result = transcribe_chunk(chunk_name)
        if result:
            srt_files.append(result)

    if not srt_files:
        log(f"Error: No SRT files found for {base_name}. Skipping merge.")
        raise RuntimeError("no_srt")

    def extract_timestamp(filename: str):
        match = re.search(r"(\d+)_\d+\.srt$", filename)
        return int(match.group(1)) if match else float("inf")

    srt_files = sorted(srt_files, key=lambda x: extract_timestamp(os.path.basename(x)))
    log(f"Merging SRT files in order: {srt_files}")

    rc = subprocess.run(["python3", "/app/merge_transcripts.py", *srt_files, str(output_srt)]).returncode
    if rc != 0:
        log(f"merge_transcripts.py failed with code {rc}")
        raise RuntimeError("merge_transcripts_failed")
    log("Merged transcripts")

    def srt_to_custom_text(srt_file: str, output_file: str):
        with open(srt_file, "r", encoding="utf-8") as file:
            content = file.read()

        pattern = re.compile(r"(\d+)\n(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n(.+?)(?=\n\n|\Z)", re.DOTALL)
        formatted = []
        for match in pattern.finditer(content):
            start_time = match.group(2).replace(",", ".")
            end_time = match.group(3).replace(",", ".")
            text = " ".join(match.group(4).splitlines())
            formatted.append(f"[{start_time} {end_time}] {text}")
        with open(output_file, "w", encoding="utf-8") as file:
            file.write("\n".join(formatted))

    if generate_txt:
        srt_to_custom_text(output_srt, output_txt)
        log("Converted SRT to TXT")

    shutil.copy(output_srt, final_srt_transcripts)
    log(f"Copied SRT to {final_srt_transcripts}")

    if generate_txt:
        shutil.copy(output_txt, final_txt_transcripts)
        log(f"Copied TXT to {final_txt_transcripts}")

    if finalize and queue_file.exists():
        shutil.move(queue_file, done_file)
        log(f"Moved {queue_file} to {done_file}")

    log(f"Final transcript saved to {final_srt_transcripts}")
    if generate_txt:
        log(f"Converted text file saved to {final_txt_transcripts}")

    if os.path.exists(chunk_dir):
        try:
            subprocess.run(["rm", "-rf", chunk_dir], check=True)
            log(f"Successfully removed chunk directory: {chunk_dir}")
        except Exception as exc:
            log(f"Error removing chunk directory: {exc}")
        subprocess.run(["sync"])

    return final_srt_transcripts


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python process_audio.py <audio_file>")
        sys.exit(5)
    try:
        process_file(sys.argv[1])
    except RuntimeError as exc:
        msg = str(exc)
        err_map = {
            "no_audio": 2,
            "no_srt": 3,
            "merge_transcripts_failed": 4,
        }
        sys.exit(err_map.get(msg, 1))
