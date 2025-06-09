#!/usr/bin/env python3
"""
Merge speakers’ names into a BigBlueButton meeting transcript,
and optionally extract chat messages.

Usage
-----
# just speaker merge:
python merge_speakers.py <events.xml> <input_transcript.txt> <output_transcript.txt>

# + chat extraction:
python merge_speakers.py <events.xml> <input_transcript.txt> <output_transcript.txt> <chat_output.txt>
"""
import logging
import os
import re
import sys
import shutil
import xml.etree.ElementTree as ET
from typing import Dict, List, Tuple, Callable

# ---------------------------------------------------------------------------
# Tunables – adjust for your environment / tolerance
# ---------------------------------------------------------------------------
MIN_TALK_MS   = 300     # discard talking bursts shorter than this
MERGE_GAP_MS  = 200     # merge same-speaker intervals whose gap ≤ this
TIE_MARGIN    = 0.10    # when two speakers’ overlaps differ by <10 % of caption
LOW_CONF_FRAC = 0.25    # "dominance" threshold; below ⇒ look at context

# ---------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ---------------------------------------------------------------------------
# Helper: HH:MM:SS.mmm → ms
# ---------------------------------------------------------------------------
def parse_time_str_to_millis(time_str: str) -> int:
    hh, mm, ssms = time_str.split(":")
    ss, ms = ssms.split(".")
    return ((int(hh) * 3600) + (int(mm) * 60) + int(ss)) * 1000 + int(ms)

# ---------------------------------------------------------------------------
# Helper: ms → HH:MM:SS.mmm  (for chat timeline)
# ---------------------------------------------------------------------------
def ms_to_hhmmss(ms: int) -> str:
    hh, rem = divmod(ms, 3_600_000)
    mm, rem = divmod(rem,   60_000)
    ss, ms  = divmod(rem,    1_000)
    return f"{hh:02d}:{mm:02d}:{ss:02d}.{ms:03d}"

# ---------------------------------------------------------------------------
# Helper: build converter abs-timestamp ➜ recording-timeline ms
# ---------------------------------------------------------------------------
def build_rec_clock(events: List[ET.Element]) -> Callable[[int], int | None]:
    """
    Return a function convert(ts_abs) -> rec_ms | None
    that maps absolute BBB timestamps to the “viewer” timeline.
    """
    segments: List[Tuple[int, int, int]] = []  # (abs_start, abs_end, rec_start_ms)
    rec_on = False
    abs_start = None
    rec_ms_accum = 0

    for ev in events:
        ev_name = ev.get("eventname")
        ts_abs  = int(ev.get("timestamp", "0"))

        if ev_name == "RecordStatusEvent":
            status = (ev.findtext("status") or "").lower()
            if status == "true" and not rec_on:
                rec_on = True
                abs_start = ts_abs
            elif status != "true" and rec_on:
                segments.append((abs_start, ts_abs, rec_ms_accum))
                rec_ms_accum += ts_abs - abs_start
                rec_on = False
                abs_start = None

    # meeting ended with recording still on
    if rec_on and abs_start is not None:
        segments.append((abs_start, 2**63 - 1, rec_ms_accum))

    def convert(ts_abs: int) -> int | None:
        for seg_abs_start, seg_abs_end, seg_rec_start in segments:
            if seg_abs_start <= ts_abs < seg_abs_end:
                return seg_rec_start + (ts_abs - seg_abs_start)
        return None  # occurred while recording paused

    return convert

# ---------------------------------------------------------------------------
# 1. Extract talking intervals from events.xml
# ---------------------------------------------------------------------------
def parse_xml_with_recording_segments(xml_path: str):
    """Return (userId→displayName, userId→[(start_ms, end_ms), …])."""

    if not os.path.exists(xml_path):
        logging.error("events.xml not found: %s", xml_path)
        return {}, {}

    root = ET.parse(xml_path).getroot()
    events = root.findall("event")
    events.sort(key=lambda e: int(e.get("timestamp", "0")))

    user_map: Dict[str, str] = {}
    talk_intervals: Dict[str, List[Tuple[int, int]]] = {}

    is_recording = False
    rec_abs_start = None        # abs ts of current recording segment start
    rec_ms_accum  = 0           # total recorded ms so far (excluding pauses)
    ongoing: Dict[str, int] = {}  # userId→talk_start_ms (rec timeline)

    for ev in events:
        ev_name = ev.get("eventname")
        ts_abs  = int(ev.get("timestamp", "0"))

        # 1) participant joins ⇒ map userId→name
        if ev_name == "ParticipantJoinEvent":
            uid = ev.findtext("userId")
            name = ev.findtext("name")
            if uid and name:
                user_map[uid] = name
            continue

        # 2) recording on/off
        if ev_name == "RecordStatusEvent":
            status = (ev.findtext("status") or "").lower()
            if status == "true":      # recording started
                rec_abs_start = ts_abs
                is_recording = True
            else:                       # recording stopped
                if is_recording and rec_abs_start is not None:
                    rec_ms_accum += ts_abs - rec_abs_start
                    # close still-open talks in this segment
                    for uid, start in list(ongoing.items()):
                        if rec_ms_accum - start >= MIN_TALK_MS:
                            talk_intervals.setdefault(uid, []).append(
                                (start, rec_ms_accum)
                            )
                        del ongoing[uid]
                is_recording = False
                rec_abs_start = None
            continue

        # 3) talking on/off
        if ev_name == "ParticipantTalkingEvent" and is_recording and rec_abs_start is not None:
            uid = ev.findtext("participant")
            talking = (ev.findtext("talking") or "").lower() == "true"
            if not uid:
                continue
            rec_now = rec_ms_accum + (ts_abs - rec_abs_start)

            if talking:
                ongoing[uid] = rec_now
            else:
                start = ongoing.pop(uid, None)
                if start is not None and rec_now - start >= MIN_TALK_MS:
                    talk_intervals.setdefault(uid, []).append((start, rec_now))

    # flush at end of file
    for uid, start in ongoing.items():
        if rec_ms_accum - start >= MIN_TALK_MS:
            talk_intervals.setdefault(uid, []).append((start, rec_ms_accum))

    # ------------------------------------------------------------------
    # Merge close intervals for same speaker (gap ≤ MERGE_GAP_MS)
    # ------------------------------------------------------------------
    for uid, lst in talk_intervals.items():
        lst.sort()
        merged = []
        s, e = lst[0]
        for ns, ne in lst[1:]:
            if ns - e <= MERGE_GAP_MS:
                e = max(e, ne)
            else:
                merged.append((s, e))
                s, e = ns, ne
        merged.append((s, e))
        talk_intervals[uid] = merged

    return user_map, talk_intervals

# ---------------------------------------------------------------------------
# 2. Merge transcript with speaker labels – COLLAPSE consecutive lines
# ---------------------------------------------------------------------------
def merge_speakers_with_transcript(transcript_file: str,
                                   user_map: Dict[str, str],
                                   talk_intervals: Dict[str, List[Tuple[int, int]]],
                                   output_file: str):
    """
    Attach a speaker label to every caption line, but if the same speaker
    keeps talking, merge consecutive captions into one block:

        [start end] Speaker:
        line-1
        line-2
        …
    """
    total_ms = {u: sum(e - s for s, e in lst) for u, lst in talk_intervals.items()}
    flat: List[Tuple[str, int, int]] = [
        (u, s, e) for u, lst in talk_intervals.items() for (s, e) in lst
    ]
    flat.sort(key=lambda x: x[1])  # by interval start

    cap_rx = re.compile(
        r"^\[(\d{2}:\d{2}:\d{2}\.\d{3})\s+(\d{2}:\d{2}:\d{2}\.\d{3})\]\s+(.*)$"
    )

    def ms(ts: str) -> int:
        return parse_time_str_to_millis(ts)

    out_blocks: List[str] = []
    cur_uid = cur_name = None
    cur_t0 = cur_t1 = None
    cur_text_lines: List[str] = []
    prev_uid = None

    def flush_block():
        if cur_name is None:
            return
        header = f"[{cur_t0} {cur_t1}] {cur_name}:"
        out_blocks.append(header)
        out_blocks.extend(cur_text_lines)

    with open(transcript_file, encoding="utf-8") as fin:
        for raw in fin:
            raw = raw.rstrip("\n")
            m = cap_rx.match(raw)
            if not m:
                flush_block()
                cur_uid = cur_name = None
                cur_text_lines.clear()
                out_blocks.append(raw)
                continue

            s_str, e_str, text_body = m.groups()
            s_ms, e_ms = ms(s_str), ms(e_str)
            dur = max(1, e_ms - s_ms)

            # find overlaps
            overlaps: Dict[str, int] = {}
            for uid, s, e in flat:
                if e < s_ms:
                    continue
                if s > e_ms:
                    break
                ov = max(0, min(e, e_ms) - max(s, s_ms))
                if ov:
                    overlaps[uid] = overlaps.get(uid, 0) + ov

            # choose speaker
            speaker_uid = None
            if overlaps:
                ranked = sorted(overlaps.items(), key=lambda kv: kv[1], reverse=True)
                best_uid, best_ms = ranked[0]
                best_frac = best_ms / dur
                second_ms = ranked[1][1] if len(ranked) > 1 else 0
                close = (best_ms - second_ms) / dur < TIE_MARGIN
                low_conf = best_frac < LOW_CONF_FRAC

                if (close or low_conf) and prev_uid in overlaps:
                    speaker_uid = prev_uid
                elif close and len(ranked) > 1:
                    speaker_uid = max(ranked[:2],
                                      key=lambda kv: total_ms.get(kv[0], 0))[0]
                else:
                    speaker_uid = best_uid
            else:
                # no overlap: nearest‐interval fallback
                nearest_uid, nearest_dist = None, float("inf")
                for uid, s, e in flat:
                    dist = s - e_ms if s >= s_ms else s_ms - e
                    if dist < nearest_dist:
                        nearest_uid, nearest_dist = uid, dist
                    if s > e_ms and dist > nearest_dist:
                        break
                speaker_uid = nearest_uid or prev_uid

            speaker_name = user_map.get(speaker_uid, "Unknown Speaker")
            if speaker_name != "Unknown Speaker":
                prev_uid = speaker_uid

            # accumulate / flush blocks
            if cur_uid == speaker_uid:
                cur_t1 = e_str
                cur_text_lines.append(text_body)
            else:
                flush_block()
                cur_uid = speaker_uid
                cur_name = speaker_name
                cur_t0, cur_t1 = s_str, e_str
                cur_text_lines = [text_body]

    flush_block()

    with open(output_file, "w", encoding="utf-8") as fout:
        fout.write("\n".join(out_blocks))

    logging.info("Speaker-labeled transcript written to %s", output_file)

# ---------------------------------------------------------------------------
# 3. Extract chat messages with recording-timeline timestamps
# ---------------------------------------------------------------------------
def extract_chat_events(xml_path: str,
                        user_map: Dict[str, str],
                        chat_out_path: str) -> None:
    """
    Write chat lines using the same recording timeline as captions:
      [HH:MM:SS.mmm][Alice]: Hello …
    """
    if not os.path.exists(xml_path):
        logging.error("events.xml not found: %s", xml_path)
        return

    root = ET.parse(xml_path).getroot()
    events = root.findall("event")
    events.sort(key=lambda e: int(e.get("timestamp", "0")))

    rec_clock = build_rec_clock(events)  # build once
    chat_lines: List[str] = []

    for ev in events:
        if ev.get("eventname") != "PublicChatEvent":
            continue

        ts_abs = int(ev.get("timestamp", "0"))
        rec_ms = rec_clock(ts_abs)
        if rec_ms is None:  # happened while recording paused
            continue

        sender_id = ev.findtext("senderId") or ""
        name      = user_map.get(sender_id, sender_id or "Unknown")
        message   = ev.findtext("message") or ""

        chat_lines.append(f"[{ms_to_hhmmss(rec_ms)}][{name}]: {message}")

    with open(chat_out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(chat_lines))
    logging.info("Chat events written to %s", chat_out_path)

# ---------------------------------------------------------------------------
# main entrypoint
# ---------------------------------------------------------------------------
def main():
    # allow either 3 or 4 positional args
    if len(sys.argv) not in (4, 5):
        print(f"Usage: {sys.argv[0]} <events.xml> <input.txt> <output.txt> [<chat_output.txt>]")
        sys.exit(1)

    events_xml = sys.argv[1]
    transcript_in = sys.argv[2]
    transcript_out = sys.argv[3]
    chat_out = sys.argv[4] if len(sys.argv) == 5 else None

    logging.info("Parsing events.xml …")
    user_map, talk_ints = parse_xml_with_recording_segments(events_xml)
    logging.info("Users: %d, intervals: %d", len(user_map), len(talk_ints))

    if not os.path.exists(events_xml):
        logging.error("No events.xml at %s – skipping speaker merge.", events_xml)
        sys.exit(0)

    if not talk_ints:
        shutil.copy(transcript_in, transcript_out)
        logging.warning("No intervals found – transcript copied unchanged.")
    else:
        merge_speakers_with_transcript(transcript_in, user_map, talk_ints, transcript_out)

    if chat_out:
        logging.info("Extracting chat events …")
        extract_chat_events(events_xml, user_map, chat_out)

if __name__ == "__main__":
    main()
