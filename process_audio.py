import os
import subprocess
import srt
import datetime
import shutil
import re
import math
import traceback
from pathlib import Path
from typing import Optional

import torch
from dotenv import load_dotenv

from logger import log
from whisper_model import get_model
from audio_pre import preprocess_chunk

load_dotenv()

USE_WORD_TIMESTAMPS = os.getenv("WHISPER_WORD_TIMESTAMPS", "0").strip().lower() in ("1","true","yes","on")

log(f"word_timestamps enabled: {USE_WORD_TIMESTAMPS}")

# ======= knobs you can tune via env =======
PAUSE_THRESHOLD = float(os.getenv("SEG_PAUSE_S", "0.5"))       # split if >= this gap
MAX_SEG_LEN = float(os.getenv("SEG_MAX_S", "2.0"))              # hard cap for a caption
MIN_CAPTION_MS = int(os.getenv("SEG_MIN_MS", "800"))
PAD_GAP_MS = int(os.getenv("SEG_PAD_MS", "50"))
MAX_CHARS = int(os.getenv("SEG_MAX_CHARS", "84"))               # ~2 lines

def _field(x, name, default=None):
    if isinstance(x, dict):
        return x.get(name, default)
    return getattr(x, name, default)

def _to_sec(x, default=0.0):
    try:
        if x is None:
            return float(default)
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return float(default)
        return v
    except Exception:
        return float(default)

def _wrap_two_lines(txt: str, max_chars: int = MAX_CHARS) -> str:
    t = " ".join(txt.split())
    if len(t) <= max_chars:
        return t
    mid = max_chars // 2
    a = t.rfind(' ', 0, mid)
    b = t.find(' ', mid)
    split_at = a if a != -1 else b
    if split_at == -1:
        return t
    return t[:split_at].strip() + "\n" + t[split_at+1:].strip()

def _flush(subs, idx, start_s, end_s, text):
    start_s = _to_sec(start_s, 0.0)
    end_s   = _to_sec(end_s, start_s)
    if end_s <= start_s:
        end_s = start_s + (MIN_CAPTION_MS / 1000.0)
    text = " ".join((text or "").split())
    if not text:
        return idx
    subs.append(srt.Subtitle(
        index=idx,
        start=datetime.timedelta(seconds=start_s),
        end=datetime.timedelta(seconds=end_s),
        content=_wrap_two_lines(text, MAX_CHARS),
    ))
    return idx + 1

def probe_duration_seconds(path: str) -> float:
    try:
        # very fast probe via ffprobe
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path],
            stderr=subprocess.STDOUT,
        ).decode().strip()
        return float(out)
    except Exception:
        return 0.0

def ensure_wav(path: str) -> str:
    # if it's already wav, return; else re-mux to wav next to it
    if path.lower().endswith(".wav"):
        return path
    wav_path = os.path.splitext(path)[0] + ".wav"
    try:
        subprocess.check_call([
            "ffmpeg", "-y", "-nostats", "-loglevel", "error",
            "-i", path, "-vn", "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le", wav_path
        ])
        return wav_path
    except Exception:
        return path  # fall back; caller will handle failure

def shift_srt_inplace(path: Path, shift_ms: int) -> None:
    """Shift every cue by +shift_ms (can be negative)."""
    if not shift_ms:
        return
    with open(path, "r", encoding="utf-8") as f:
        subs = list(srt.parse(f.read()))
    delta = datetime.timedelta(milliseconds=int(shift_ms))
    fixed = []
    for sub in subs:
        start = sub.start + delta
        end   = sub.end   + delta
        # clamp to 0
        if start.total_seconds() < 0:
            start = datetime.timedelta(0)
            if end <= start:
                end = start + datetime.timedelta(milliseconds=MIN_CAPTION_MS)
        fixed.append(srt.Subtitle(index=sub.index, start=start, end=end, content=sub.content))
    with open(path, "w", encoding="utf-8") as f:
        f.write(srt.compose(list(srt.sort_and_reindex(fixed))))

def _segmentize_with_words(segments):
    subs, idx = [], 1
    for segment in segments:
        seg_start = _to_sec(_field(segment, "start"), 0.0)
        seg_end   = _to_sec(_field(segment, "end"), seg_start)
        words     = _field(segment, "words", None)

        if not words:
            # if words missing (unexpected), just make one caption for this segment
            idx = _flush(subs, idx, seg_start, seg_end, _field(segment, "text", ""))
            continue

        current_words, current_start, last_end = [], None, None
        for w in words:
            w_start = _to_sec(_field(w, "start"), seg_start)
            w_end   = _to_sec(_field(w, "end"),   w_start)
            w_text  = _field(w, "word", "") or ""
            if w_end < w_start:
                w_start, w_end = w_end, w_start
            if current_start is None:
                current_start = w_start

            split_pause  = (last_end is not None) and ((w_start - last_end) >= PAUSE_THRESHOLD)
            split_length = (w_end - current_start) >= MAX_SEG_LEN
            if split_pause or split_length:
                idx = _flush(subs, idx, current_start, last_end or w_start, " ".join(current_words))
                current_words = []
                current_start = w_start

            current_words.append(w_text)
            last_end = w_end

        idx = _flush(subs, idx, current_start if current_start is not None else seg_start,
                          last_end      if last_end      is not None else seg_end,
                          " ".join(current_words))
    return subs

_SENT_SPLIT = re.compile(r'([\.!\?]+)["”\']?\s+')

def _segmentize_without_words(segments):
    """
    Fallback when word_timestamps are unavailable/unstable.
    Split each segment by punctuation; enforce max duration by evenly slicing time.
    """
    subs, idx = [], 1
    for segment in segments:
        seg_start = _to_sec(_field(segment, "start"), 0.0)
        seg_end   = _to_sec(_field(segment, "end"), seg_start)
        seg_text  = (_field(segment, "text", "") or "").strip()
        if seg_end <= seg_start:
            seg_end = seg_start + (MIN_CAPTION_MS / 1000.0)

        # 1) split text into sentence-like chunks
        parts = []
        start = 0
        for m in _SENT_SPLIT.finditer(seg_text):
            end = m.end()
            parts.append(seg_text[start:end].strip())
            start = end
        tail = seg_text[start:].strip()
        if tail:
            parts.append(tail)
        if not parts:
            parts = [seg_text]

        # 2) allocate times across parts, then re-split overly long parts by time
        total = seg_end - seg_start
        per = total / max(1, len(parts))
        cur_t = seg_start
        for p in parts:
            p_start = cur_t
            p_end   = min(seg_end, cur_t + per)
            # further split long time spans to obey MAX_SEG_LEN
            span = p_end - p_start
            if span > MAX_SEG_LEN:
                n = int(math.ceil(span / MAX_SEG_LEN))
                step = span / n
                for i in range(n):
                    s = p_start + i * step
                    e = min(p_start + (i + 1) * step, p_end)
                    idx = _flush(subs, idx, s, e, p)
            else:
                idx = _flush(subs, idx, p_start, p_end, p)
            cur_t = p_end
    return subs

def process_file(
    input_file: str,
    model=None,
    *,
    destination_dir: Optional[Path] = None,
    final_basename: Optional[str] = None,
    finalize: bool = True,
    generate_txt: bool = True,
    intermediate_dir: Optional[Path] = None,
) -> Path:
    if model is None:
        model = get_model()

    base_name = os.path.splitext(os.path.basename(input_file))[0]
    final_base = final_basename or base_name
    log(f"Starting processing for {base_name}")

    chunk_dir = f"/app/chunks/{base_name}"
    scripts_dir = Path(intermediate_dir) if intermediate_dir else Path("/app/scripts")
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
    def dedupe_subs(subs, window_s: float = 3.0, prefer_longer: bool = True):
        """
        Remove consecutive near-duplicate captions within ~window_s.
        If duplicates found, keep the one with longer duration (or earlier if equal).
        """
        out = []
        last_by_text = {}  # text -> (index in out)
        for sub in subs:
            text = " ".join((sub.content or "").split())
            if not text:
                continue
            drop = False
            if text in last_by_text:
                j = last_by_text[text]
                prev = out[j]
                # If they’re very close in time, treat as duplicate
                gap = abs((sub.start - prev.end).total_seconds())
                overlap = (min(sub.end, prev.end) - max(sub.start, prev.start)).total_seconds()
                # duplicate if they abut/overlap or are close
                if gap <= window_s or overlap > 0:
                    dur_prev = (prev.end - prev.start).total_seconds()
                    dur_cur  = (sub.end  - sub.start ).total_seconds()
                    if prefer_longer and dur_cur > dur_prev + 0.05:
                        out[j] = sub  # replace previous with this longer one
                    # else keep previous and drop current
                    drop = True
            if not drop:
                last_by_text[text] = len(out)
                out.append(sub)
        # reindex
        return [srt.Subtitle(i+1, s.start, s.end, s.content) for i, s in enumerate(out)]

    def transcribe_chunk(chunk_name: str):
        ext = os.path.splitext(chunk_name)[1].lower()
        if ext not in (".wav", ".ogg"):
            return None
        log(f"Transcribing {chunk_name}")
        chunk_path = os.path.join(chunk_dir, chunk_name)
        chunk_base = os.path.splitext(chunk_name)[0]
        srt_path = os.path.join(chunk_dir, f"{chunk_base}.srt")

        if os.path.getsize(chunk_path) == 0:
            log(f"Skipping zero-byte chunk {chunk_name}")
            return None

        # ---- NEW: call RNNoise + Silero VAD here ----
        try:
            keep = preprocess_chunk(chunk_path)
        except Exception as e:
            log(f"Preprocess (RNNoise+VAD) failed on {chunk_name}: {e}")
            keep = True  # fail-open

        if not keep:
            log(f"VAD: Dropping non-speech chunk {chunk_name}")
            return None

        # ---- END NEW BLOCK ----
        # Build kwargs guardedly; some builds choke on these keys
        kwargs = {}
        if USE_WORD_TIMESTAMPS:
            kwargs.update(dict(
                word_timestamps=True,
                condition_on_previous_text=False,   # keep False here; we split by words anyway
                no_speech_threshold=0.6,
                logprob_threshold=-0.8,
                compression_ratio_threshold=2.4,
            ))
        else:
            # Let Whisper carry context INSIDE the chunk to reduce repeats.
            kwargs.update(dict(
                word_timestamps=False,
                condition_on_previous_text=False,
                no_speech_threshold=0.6,
                logprob_threshold=-0.8,
                compression_ratio_threshold=2.4,
            ))
        # quick sanity checks
        dur = probe_duration_seconds(chunk_path)
        if dur <= 0.05:
            log(f"Skipping too-short/unreadable chunk {chunk_name} (duration={dur:.3f}s)")
            return None


        # Call Whisper
        try:
            try:
                result = model.transcribe(chunk_path, **kwargs)
            except Exception as exc_first:
                log(f"Whisper failed on {chunk_name} (first pass): {exc_first}")
                # re-mux and retry
                retry_path = ensure_wav(chunk_path)
                if retry_path != chunk_path:
                    try:
                        result = model.transcribe(retry_path, **kwargs)
                    except Exception as exc_second:
                        log(f"Whisper failed on {chunk_name} after wav remux: {exc_second}")
                        return None
                else:
                    return None 
            except TypeError:
                # model doesn’t accept one or more kwargs
                if "word_timestamps" in kwargs:
                    # strip it and retry
                    kwargs.pop("word_timestamps", None)
                result = model.transcribe(chunk_path, **kwargs)
        except (torch.cuda.CudaError, torch.cuda.OutOfMemoryError):
            raise
        except Exception as exc:
            log(f"Whisper failed on {chunk_name}: {exc}\n{traceback.format_exc()}")
            return None

        try:
            segments = result["segments"]
        except Exception as exc:
            log(f"No segments for {chunk_name}: {exc}\n{traceback.format_exc()}")
            return None

        # ---- NEW: drop very low-confidence / noise-like segments ----
        NS_MAX = float(os.getenv("SEG_NO_SPEECH_MAX", "0.6"))   # if higher, probably noise
        LP_MIN = float(os.getenv("SEG_LOGPROB_MIN", "-0.9"))    # if lower, very uncertain

        filtered = []
        for seg in segments:
            ns = float(seg.get("no_speech_prob", 0.0))
            lp = float(seg.get("avg_logprob", 0.0))
            text = (seg.get("text") or "").strip()

            # Highly probable "no speech" or very low confidence → drop
            if ns > NS_MAX or lp < LP_MIN:
                log(
                    f"Dropping low-confidence seg in {chunk_name}: "
                    f"no_speech_prob={ns:.2f}, avg_logprob={lp:.2f}, text={text!r}"
                )
                continue

            filtered.append(seg)

        if not filtered:
            log(f"All segments filtered out as low-confidence / noise for {chunk_name}")
            return None

        segments = filtered
        # ---- END NEW BLOCK ----
        
        # Build subtitles
        try:
            if USE_WORD_TIMESTAMPS and any(_field(seg, "words", None) for seg in segments):
                subs = _segmentize_with_words(segments)
            else:
                subs = _segmentize_without_words(segments)
            subs = dedupe_subs(subs)
        except Exception as exc:
            log(f"Build SRT failed for {chunk_name}: {exc}\n{traceback.format_exc()}")
            return None

        with open(srt_path, "w", encoding="utf-8") as f:
            f.write(srt.compose(subs))
        log(f"Generated {srt_path}")
        return srt_path

    # --- Transcribe all chunks then merge ---
    srt_files = []
    chunk_list = [f for f in os.listdir(chunk_dir) if f.endswith((".wav", ".ogg"))]

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
        m = re.search(r"(\d+)_\d+\.srt$", filename)
        return int(m.group(1)) if m else float("inf")

    srt_files = sorted(srt_files, key=lambda x: extract_timestamp(os.path.basename(x)))
    log(f"Merging SRT files in order: {srt_files}")

    rc = subprocess.run(["python3", "/app/merge_transcripts.py", *srt_files, str(output_srt)]).returncode
    if rc != 0:
        log(f"merge_transcripts.py failed with code {rc}")
        raise RuntimeError("merge_transcripts_failed")
    log("Merged transcripts")
    
    shift_val = int(os.getenv("SRT_SHIFT_MS", "0"))
    if shift_val:
        log(f"Applying global SRT shift of {shift_val} ms")
        shift_srt_inplace(output_srt, shift_val)

    # Post-merge tidy
    def tidy_srt_inplace(path: Path, min_dur_ms: int = MIN_CAPTION_MS, pad_gap_ms: int = PAD_GAP_MS):
        with open(path, "r", encoding="utf-8") as f:
            items = list(srt.parse(f.read()))
        items.sort(key=lambda x: (x.start, x.end))

        # 1) fix overlaps + enforce min duration
        fixed = []
        for sub in items:
            if fixed and sub.start < fixed[-1].end:
                sub.start = fixed[-1].end + datetime.timedelta(milliseconds=pad_gap_ms)
                if sub.end <= sub.start:
                    sub.end = sub.start + datetime.timedelta(milliseconds=min_dur_ms)
            if (sub.end - sub.start).total_seconds() * 1000 < min_dur_ms:
                sub.end = sub.start + datetime.timedelta(milliseconds=min_dur_ms)
            fixed.append(sub)

        # 2) cross-chunk dedupe: collapse near-adjacent identical/near-identical lines
        def _norm(t: str) -> str:
            t = " ".join((t or "").split()).lower()
            # strip lightweight punctuation that causes false mismatches
            return re.sub(r"[^\w\s]", "", t)

        DEDUPE_WINDOW_S = 6.0  # bigger window to catch repeats with slightly different times
        deduped = []
        last_by_text = {}   # normalized_text -> (idx in deduped)

        for sub in fixed:
            nt = _norm(sub.content)
            if nt and nt in last_by_text:
                j = last_by_text[nt]
                prev = deduped[j]
                gap = (sub.start - prev.end).total_seconds()
                overlap = (min(sub.end, prev.end) - max(sub.start, prev.start)).total_seconds()
                if gap <= DEDUPE_WINDOW_S or overlap > 0:
                    # keep the longer one
                    if (sub.end - sub.start) > (prev.end - prev.start) + datetime.timedelta(milliseconds=50):
                        deduped[j] = sub
                    continue
            last_by_text[nt] = len(deduped)
            deduped.append(sub)

        with open(path, "w", encoding="utf-8") as f:
            f.write(srt.compose(list(srt.sort_and_reindex(deduped))))


    tidy_srt_inplace(output_srt)

    # TXT export
    def srt_to_custom_text(srt_file: Path, output_file: Path):
        with open(srt_file, "r", encoding="utf-8") as file:
            content = file.read()
        pattern = re.compile(
            r"(\d+)\n(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n(.+?)(?=\n\n|\Z)",
            re.DOTALL,
        )
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

    if output_srt.resolve() != final_srt_transcripts.resolve():
        shutil.copy(output_srt, final_srt_transcripts)
        log(f"Copied SRT to {final_srt_transcripts}")
    else:
        log(f"SRT already located at {final_srt_transcripts}; skipping copy")

    if generate_txt:
        if output_txt.resolve() != final_txt_transcripts.resolve():
            shutil.copy(output_txt, final_txt_transcripts)
            log(f"Copied TXT to {final_txt_transcripts}")
        else:
            log(f"TXT already located at {final_txt_transcripts}; skipping copy")

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
        err_map = {"no_audio": 2, "no_srt": 3, "merge_transcripts_failed": 4}
        sys.exit(err_map.get(msg, 1))
