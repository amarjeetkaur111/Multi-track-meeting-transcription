import sys
import srt
import os
import datetime
import re
from typing import Iterable, Optional, Tuple

from logger import log

def get_start_time_from_filename(filename):
    """Extracts the start timestamp from the filename (assuming format: timestamp_index.srt)."""
    try:
        match = re.search(r"(\d+)_\d+\.srt$", filename)
        if match:
            start_time_ms = int(match.group(1))  # Extract timestamp in ms
            return datetime.timedelta(milliseconds=start_time_ms)
    except ValueError:
        pass
    return datetime.timedelta(0)

def merge_srt(files, output_file):
    all_subs = []
    for file in files:
        if not os.path.exists(file) or os.stat(file).st_size == 0:
            log(f"Skipping empty or missing SRT file: {file}")
            continue
        with open(file, "r", encoding="utf-8") as f:
            subs = list(srt.parse(f.read()))
            start_time = get_start_time_from_filename(file)
            # Adjust each subtitle's time
            adjusted = [srt.Subtitle(i.index, start_time + i.start, start_time + i.end, i.content) for i in subs]
            all_subs.extend(adjusted)

    if not all_subs:
        log("Error: No valid SRT data found. Cannot merge.")
        sys.exit(1)

    # Sort by start time
    all_subs = sorted(all_subs, key=lambda x: x.start)

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(srt.compose(all_subs))


def merge_absolute_srts(
    files: Iterable[Tuple[str, Optional[str]]],
    output_file: str,
) -> None:
    """Merge already absolute-timestamp SRTs into *output_file*.

    *files* is an iterable of ``(path, speaker_name)`` tuples. ``speaker_name``
    may be ``None`` to leave the subtitle content untouched.
    """

    all_subs = []
    for path, speaker in files:
        if not os.path.exists(path) or os.stat(path).st_size == 0:
            log(f"Skipping empty or missing SRT file: {path}")
            continue

        with open(path, "r", encoding="utf-8") as handle:
            subtitles = list(srt.parse(handle.read()))

        for sub in subtitles:
            content = sub.content
            if speaker:
                content = f"{speaker}: {content}"
            all_subs.append(
                srt.Subtitle(index=sub.index, start=sub.start, end=sub.end, content=content)
            )

    if not all_subs:
        raise RuntimeError("no_subtitles")

    all_subs.sort(key=lambda subtitle: (subtitle.start, subtitle.end))

    renumbered = [
        srt.Subtitle(index=i, start=sub.start, end=sub.end, content=sub.content)
        for i, sub in enumerate(all_subs, start=1)
    ]

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as handle:
        handle.write(srt.compose(renumbered))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python merge_transcripts.py <srt_files> <output_file>")
        sys.exit(1)
    merge_srt(sys.argv[1:-1], sys.argv[-1])
