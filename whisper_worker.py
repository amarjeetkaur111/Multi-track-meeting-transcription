#!/usr/bin/env python3
# heartbeat_async_whisper_worker.py
# ------------------------------------------------------------
#  * Async SelectConnection (auto heart-beat)
#  * Background thread for long job
#  * Per-meeting processing across microphone tracks
# ------------------------------------------------------------
import datetime
import os
import json
import threading
import subprocess
import sys
import re
import shutil
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pika
import srt
import torch
from dotenv import load_dotenv

from logger import log
from whisper_model import get_model, cuda_available

load_dotenv()

# Load Whisper model once and keep it in memory
MODEL = get_model()

# ─────────── ENV / RabbitMQ ───────────
RABBIT_HOST     = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_PORT     = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBIT_USER     = os.getenv("RABBITMQ_USER", "guest")
RABBIT_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
RABBIT_VHOST    = os.getenv("RABBITMQ_VHOST", "/")

TASK_QUEUE      = os.getenv("RABBITMQ_QUEUE",  "whisper_jobs")
RESULT_QUEUE    = os.getenv("RESULT_QUEUE",     "whisper_results")

HEARTBEAT       = int(os.getenv("RABBITMQ_HEARTBEAT", "30"))
HB_LOG_INTERVAL = HEARTBEAT                    # how often to print a tick
PREFETCH        = 1
JOB_TIMEOUT     = 60*60                        # 1 h
TEST_MODE       = False

CREDENTIALS     = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)

PARAMS = pika.ConnectionParameters(
    host     = RABBIT_HOST,
    port     = RABBIT_PORT,
    virtual_host = RABBIT_VHOST,
    credentials  = CREDENTIALS,
    heartbeat    = HEARTBEAT,
    blocked_connection_timeout = HEARTBEAT * 2,
)

FILE_TYPES = ["srt"]
RECORDINGS_ROOT = Path(os.getenv("RECORDINGS_ROOT", "/recordings"))

# ─────────── Metadata helpers ───────────

MICROPHONE_PATTERN = re.compile(r"microphone-(?P<user>[^-]+)-.*-(?P<ts>\d+)\.webm$", re.IGNORECASE)


@dataclass
class Participant:
    name: str
    join_ts: Optional[int]


@dataclass
class MeetingTimeline:
    segments: List[Tuple[int, int]]
    fallback_start: int = 0

    @classmethod
    def from_segments(
        cls,
        segments: List[Tuple[int, int]],
        fallback_start: int = 0,
    ) -> "MeetingTimeline":
        cleaned: List[Tuple[int, int]] = [
            (int(start), int(end))
            for start, end in segments
            if start is not None and end is not None and end >= start
        ]
        cleaned.sort(key=lambda pair: pair[0])
        merged: List[Tuple[int, int]] = []
        for start, end in cleaned:
            if not merged:
                merged.append((start, end))
                continue
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        return cls(segments=merged, fallback_start=fallback_start)

    @property
    def start_ms(self) -> int:
        if self.segments:
            return self.segments[0][0]
        return self.fallback_start

    def absolute_to_meeting(self, timestamp: Optional[int]) -> int:
        if timestamp is None:
            return 0
        ts = int(timestamp)
        if not self.segments:
            base = self.fallback_start
            return max(0, ts - base)
        total = 0
        for start, end in self.segments:
            if ts < start:
                return total
            if ts >= end:
                total += max(0, end - start)
                continue
            return total + max(0, ts - start)
        last_end = self.segments[-1][1]
        if ts <= last_end:
            return total
        return total + max(0, ts - last_end)


@dataclass
class TrackTiming:
    anchor_ms: int
    anchor_meeting_ms: int
    talk_windows: List[Tuple[int, int]]
    timeline: "MeetingTimeline"


def _safe_int(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def load_meeting_metadata(
    meeting_dir: Path,
) -> tuple[MeetingTimeline, Dict[str, Participant], Dict[str, List[Tuple[int, int]]]]:
    events_path = meeting_dir / "events.xml"
    participants: Dict[str, Participant] = {}
    first_recording_start: Optional[int] = None
    talk_windows: Dict[str, List[Tuple[int, int]]] = defaultdict(list)
    talking_state: Dict[str, Optional[int]] = {}
    last_timestamp_utc: Optional[int] = None
    recording_segments: List[Tuple[int, int]] = []
    recording_active = False
    current_record_start: Optional[int] = None

    if not events_path.exists():
        log(f"events.xml not found in {meeting_dir}; offsets will default to 0")
        timeline = MeetingTimeline.from_segments([], fallback_start=0)
        return timeline, participants, {}

    try:
        tree = ET.parse(events_path)
        root = tree.getroot()
    except ET.ParseError as exc:
        log(f"Failed to parse {events_path}: {exc}")
        timeline = MeetingTimeline.from_segments([], fallback_start=0)
        return timeline, participants, {}

    def _event_timestamp(event: ET.Element) -> int:
        value = event.attrib.get("timestamp")
        try:
            return int(value) if value is not None else 0
        except (TypeError, ValueError):
            return 0

    events = sorted(root.findall("event"), key=_event_timestamp)

    for event in events:
        event_name = event.attrib.get("eventname")
        module = event.attrib.get("module")
        timestamp_utc = _safe_int(event.findtext("timestampUTC"))

        if timestamp_utc is not None:
            if last_timestamp_utc is None or timestamp_utc > last_timestamp_utc:
                last_timestamp_utc = timestamp_utc

        if event_name == "RecordStatusEvent":
            status_text = (event.findtext("status") or "").strip().lower()
            current_ts = timestamp_utc
            if current_ts is None:
                current_ts = _safe_int(event.attrib.get("timestamp"))
            if current_ts is None:
                continue
            if status_text == "true":
                if not recording_active:
                    recording_active = True
                    current_record_start = current_ts
                    if first_recording_start is None:
                        first_recording_start = current_ts
            elif status_text == "false":
                if recording_active and current_record_start is not None:
                    end_ts = current_ts if current_ts >= current_record_start else current_record_start
                    recording_segments.append((current_record_start, end_ts))
                recording_active = False
                current_record_start = None
            continue

        if module == "PARTICIPANT" and event_name == "ParticipantJoinEvent":
            user_id = event.findtext("userId") or event.findtext("participant")
            if not user_id:
                continue
            name = event.findtext("name") or user_id
            participants[user_id] = Participant(name=name, join_ts=timestamp_utc)
            continue

        if module == "VOICE" and event_name == "ParticipantTalkingEvent":
            participant = event.findtext("participant")
            if not participant:
                continue
            talking = (event.findtext("talking") or "").strip().lower() == "true"
            current_ts = timestamp_utc
            if current_ts is None:
                current_ts = _safe_int(event.attrib.get("timestamp"))
            if current_ts is None:
                continue
            if talking:
                talking_state[participant] = current_ts
            else:
                start_ts = talking_state.pop(participant, None)
                if start_ts is None:
                    continue
                if current_ts < start_ts:
                    current_ts = start_ts
                talk_windows[participant].append((start_ts, current_ts))

    participants_summary = ", ".join(
        f"{user}:{info.join_ts}"
        for user, info in participants.items()
    ) or "<none>"
    if recording_active and current_record_start is not None:
        end_ts = (
            last_timestamp_utc
            if last_timestamp_utc is not None and last_timestamp_utc >= current_record_start
            else current_record_start
        )
        recording_segments.append((current_record_start, end_ts))

    # close any dangling talk intervals using the last known timestamp
    if talking_state:
        fallback_end = last_timestamp_utc
        for participant, start_ts in list(talking_state.items()):
            if start_ts is None:
                continue
            end_ts = fallback_end if fallback_end is not None else start_ts
            if end_ts < start_ts:
                end_ts = start_ts
            talk_windows[participant].append((start_ts, end_ts))

    normalised_windows: Dict[str, List[Tuple[int, int]]] = {}
    for participant, windows in talk_windows.items():
        cleaned = [
            (start, end)
            for start, end in windows
            if start is not None and end is not None and end >= start
        ]
        cleaned.sort(key=lambda pair: pair[0])
        merged: List[Tuple[int, int]] = []
        for start, end in cleaned:
            if not merged:
                merged.append((start, end))
                continue
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        if merged:
            normalised_windows[participant] = merged

    talk_summary = ", ".join(
        f"{user}:{len(windows)}"
        for user, windows in normalised_windows.items()
    ) or "<none>"
    timeline = MeetingTimeline.from_segments(
        recording_segments,
        fallback_start=first_recording_start or 0,
    )
    record_summary = ", ".join(
        f"[{start},{end}]"
        for start, end in timeline.segments
    ) or "<none>"
    log(
        f"Parsed metadata for meeting – recording_start={first_recording_start}, "
        f"participants={participants_summary}, talk_windows={talk_summary}, "
        f"record_segments={record_summary}"
    )
    return timeline, participants, normalised_windows


def parse_microphone_file(path: Path) -> tuple[Optional[str], Optional[int]]:
    match = MICROPHONE_PATTERN.match(path.name)
    if not match:
        return None, None
    user = match.group("user")
    ts = _safe_int(match.group("ts"))
    return user, ts


ALIGN_TOLERANCE_MS = 5000


def determine_track_timing(
    timeline: MeetingTimeline,
    join_ts: Optional[int],
    track_timestamp: Optional[int],
    talk_windows: List[Tuple[int, int]],
) -> TrackTiming:
    windows = list(talk_windows)
    windows.sort(key=lambda pair: pair[0])

    anchor = track_timestamp
    if anchor is not None and windows:
        for start, _ in windows:
            if start >= anchor - ALIGN_TOLERANCE_MS:
                if abs(start - anchor) <= ALIGN_TOLERANCE_MS:
                    anchor = min(start, anchor)
                break

    if anchor is None:
        if windows:
            anchor = windows[0][0]
        elif join_ts is not None:
            anchor = join_ts
        else:
            anchor = timeline.start_ms

    anchor_int = int(anchor)
    meeting_offset = timeline.absolute_to_meeting(anchor_int)

    return TrackTiming(
        anchor_ms=anchor_int,
        anchor_meeting_ms=int(meeting_offset),
        talk_windows=windows,
        timeline=timeline,
    )


def adjust_track_srt(
    srt_path: Path,
    timing: TrackTiming,
) -> None:
    try:
        content = srt_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        log(f"SRT not found for alignment: {srt_path}")
        return
    except OSError as exc:
        log(f"Failed to read {srt_path}: {exc}")
        return

    try:
        subtitles = list(srt.parse(content))
    except srt.SRTParseError as exc:
        log(f"Failed to parse SRT {srt_path}: {exc}")
        return

    if not subtitles:
        log(f"No subtitles to align for {srt_path}")
        return

    adjusted: List[srt.Subtitle] = []
    tolerance = ALIGN_TOLERANCE_MS
    anchor = timing.anchor_ms
    anchor_meeting = timing.anchor_meeting_ms
    windows = timing.talk_windows
    timeline = timing.timeline

    for subtitle in subtitles:
        rel_start_ms = round(subtitle.start.total_seconds() * 1000)
        rel_end_ms = round(subtitle.end.total_seconds() * 1000)

        abs_start = anchor + rel_start_ms
        abs_end = anchor + rel_end_ms
        if abs_end <= abs_start:
            abs_end = abs_start + 1

        if windows:
            best_window: Optional[Tuple[int, int]] = None
            best_overlap = -1
            for start, end in windows:
                if end + tolerance < abs_start:
                    continue
                if start - tolerance > abs_end:
                    break
                overlap = max(0, min(end, abs_end) - max(start, abs_start))
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_window = (start, end)
            if best_window:
                window_start, window_end = best_window
                abs_start = max(abs_start, window_start)
                abs_end = max(abs_start + 1, min(abs_end, window_end))

        rel_start_delta = max(timeline.absolute_to_meeting(abs_start), 0)
        rel_end_delta = timeline.absolute_to_meeting(abs_end)
        if rel_end_delta <= rel_start_delta:
            rel_end_delta = rel_start_delta + max(1, rel_end_ms - rel_start_ms)

        adjusted.append(
            srt.Subtitle(
                index=subtitle.index,
                start=datetime.timedelta(milliseconds=rel_start_delta),
                end=datetime.timedelta(milliseconds=rel_end_delta),
                content=subtitle.content.strip(),
            )
        )

    adjusted.sort(key=lambda sub: (sub.start, sub.end))
    renumbered = [
        srt.Subtitle(index=i, start=sub.start, end=sub.end, content=sub.content)
        for i, sub in enumerate(adjusted, start=1)
    ]

    try:
        srt_path.write_text(srt.compose(renumbered), encoding="utf-8")
    except OSError as exc:
        log(f"Failed to write aligned SRT {srt_path}: {exc}")
        return

    log(
        f"Aligned {srt_path.name} to meeting timeline "
        f"(anchor_abs={anchor}, anchor_meeting={anchor_meeting}, entries={len(renumbered)})"
    )


# ─────────── RabbitMQ connection check ───────────
def ensure_rabbitmq():
    """Exit the process if RabbitMQ is unreachable."""
    try:
        test = pika.BlockingConnection(PARAMS)
        test.close()
    except Exception as exc:
        log(f"RabbitMQ connection failed: {exc}")
        sys.exit(1)

# ─────────── heartbeat logger ───────────
def schedule_hb_log(conn, chan):
    def _log():
        log(f"HB tick – conn_open: {conn.is_open} "
            f"chan_open: {chan.is_open}")
        conn.ioloop.call_later(HB_LOG_INTERVAL, _log)
    conn.ioloop.call_later(HB_LOG_INTERVAL, _log)

# ─────────── RESULT_QUEUE notifier ───────────
def notify_file(channel, file_id, ftype, status, error=None):
    suffix_map = {
        "srt": f"{file_id}.txt",
    }
    suffix = suffix_map.get(ftype)

    content = None
    if status == "done" and suffix:
        try:
            script_path = Path(f"/transcripts_speaker_wise/{file_id}/{suffix}")
            content = script_path.read_text(encoding="utf-8")
            # log(f"Script {ftype} for {file_id}: {content}")
        except Exception as exc:
            log(f"Failed to read {ftype} for {file_id}: {exc}")


    body = {
        "file_id": file_id,
        "type": ftype,
        "status": status,
        "script": content,
        "error": error,
    }
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(body),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log(f"[notify] {file_id} {ftype} → {status}")

# ─────────── heavy job (split + Whisper) or dummy sleep ───────────
def run_pipeline(meeting_dir: Path, meeting_id: str) -> Path:
    """Transcribe every microphone track for *meeting_id* and merge the SRTs."""

    audio_dir = meeting_dir / "audio"
    if not audio_dir.exists() or not audio_dir.is_dir():
        log(f"Audio directory not found for meeting {meeting_id}: {audio_dir}")
        raise RuntimeError("no_audio")

    microphone_paths = sorted(audio_dir.glob("microphone-*.webm"))
    if not microphone_paths:
        log(f"No microphone recordings discovered under {audio_dir}")
        raise RuntimeError("no_audio")

    timeline, participants, talk_windows = load_meeting_metadata(meeting_dir)
    meeting_scripts_dir = Path("/app/scripts") / meeting_id
    meeting_scripts_dir.mkdir(parents=True, exist_ok=True)
    transcripts_output_dir = Path("/transcripts_speaker_wise") / meeting_id
    transcripts_output_dir.mkdir(parents=True, exist_ok=True)

    from process_audio import process_file
    from merge_transcripts import merge_absolute_srts

    per_track_srts: List[Tuple[str, Optional[str]]] = []

    for mic_path in microphone_paths:
        user_id, track_ts = parse_microphone_file(mic_path)
        if not user_id:
            log(f"Skipping unrecognised microphone file name: {mic_path.name}")
            continue

        participant = participants.get(user_id)
        speaker_name = participant.name if participant else user_id
        join_ts = participant.join_ts if participant else None
        user_windows = talk_windows.get(user_id, [])
        timing = determine_track_timing(timeline, join_ts, track_ts, user_windows)

        log(
            f"Processing track {mic_path.name}: speaker={speaker_name}, "
            f"join_ts={join_ts}, track_ts={track_ts}, offset={timing.anchor_meeting_ms}"
        )

        # Split audio with per-track offset; if split fails, skip this track
        with open("/logs/split.log", "a") as split_log:
            rc = subprocess.run(
                ["/app/split_audio.sh", str(mic_path), str(timing.anchor_meeting_ms)],
                stdout=split_log,
                stderr=subprocess.STDOUT,
            ).returncode

        if rc != 0:
            log(f"[split] skipping track (rc={rc}): {mic_path.name}")
            continue  # do NOT abort the whole meeting

        # Transcribe this track; if it yields no SRT, skip
        try:
            srt_path = process_file(
                str(mic_path),
                MODEL,
                destination_dir=meeting_scripts_dir,
                final_basename=mic_path.stem,
                finalize=False,
                generate_txt=False,
                intermediate_dir=meeting_scripts_dir,
            )
        except (torch.cuda.CudaError, torch.cuda.OutOfMemoryError):
            # GPU fatal: bail and let container restart
            raise
        except RuntimeError as exc:
            # Expected per-track “no audio / no srt / corrupt” → skip
            if str(exc) in {"no_audio", "no_srt", "merge_transcripts_failed"}:
                log(f"[transcribe] skipping track ({exc}): {mic_path.name}")
                continue
            # Unexpected → re-raise
            raise

        srt_path_obj = Path(srt_path) if srt_path else None
        if not srt_path_obj or not srt_path_obj.exists() or srt_path_obj.stat().st_size == 0:
            log(f"[transcribe] empty SRT; skipping: {mic_path.name}")
            continue

        target_srt_path = transcripts_output_dir / srt_path_obj.name
        try:
            if target_srt_path.exists():
                target_srt_path.unlink()
            shutil.move(str(srt_path_obj), target_srt_path)
            log(f"Moved {srt_path_obj} to {target_srt_path}")
            srt_path_obj = target_srt_path
        except Exception as exc:
            log(f"Failed to move SRT {srt_path_obj} to {target_srt_path}: {exc}")
            continue

        per_track_srts.append((str(srt_path_obj), speaker_name))


    if not per_track_srts:
        raise RuntimeError("no_srt")

    final_srt_path = transcripts_output_dir / f"{meeting_id}.srt"
    if final_srt_path.exists():
        final_srt_path.unlink()

    try:
        merge_absolute_srts(per_track_srts, str(final_srt_path))
    except RuntimeError as exc:
        raise RuntimeError(str(exc))

    log(f"Merged {len(per_track_srts)} track transcripts into {final_srt_path}")

    final_txt_path = transcripts_output_dir / f"{meeting_id}.txt"

    def write_txt_transcript(source: Path, target: Path) -> None:
        pattern = re.compile(
            r"(\d+)\n(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n(.+?)(?=\n\n|\Z)",
            re.DOTALL,
        )
        try:
            content = source.read_text(encoding="utf-8")
            formatted = []
            for match in pattern.finditer(content):
                start_time = match.group(2).replace(",", ".")
                end_time = match.group(3).replace(",", ".")
                text = " ".join(match.group(4).splitlines())
                formatted.append(f"[{start_time} {end_time}] {text}")
            target.write_text("\n".join(formatted), encoding="utf-8")
            log(f"Wrote TXT transcript to {target}")
        except Exception as exc:
            log(f"Failed to create TXT transcript {target}: {exc}")

    if final_srt_path.exists():
        write_txt_transcript(final_srt_path, final_txt_path)
    else:
        log(f"Final SRT {final_srt_path} missing; skipping TXT export")

    # Comment out the following block to retain per-track SRT files once the final transcript is ready.
    for track_path_str, _speaker in per_track_srts:
        track_path = Path(track_path_str)
        if track_path != final_srt_path and track_path.exists():
            try:
                track_path.unlink()
                log(f"Removed per-track SRT {track_path}")
            except Exception as exc:
                log(f"Failed to remove per-track SRT {track_path}: {exc}")

    try:
        shutil.rmtree(meeting_scripts_dir)
        log(f"Removed intermediate directory {meeting_scripts_dir}")
    except FileNotFoundError:
        pass


    return final_srt_path

def notify_op(channel, file_id: str, ftype: str,
              status: str, error: str | None = None):
    """
    Returns a lambda that will publish one status line to RESULT_QUEUE
    when executed in the I/O thread.
    """
    return lambda ch=channel: notify_file(ch, file_id, ftype, status, error)


# ─────────── worker thread ───────────
def spawn_worker(connection, channel, method, body: bytes):
    """Runs in its own *thread*.  Never touches the channel directly."""
    tag = method.delivery_tag
    enqueue = connection.ioloop.add_callback_threadsafe     # shorthand
    processed: set[str] = set()                             # file types done

    # ── helpers to ACK / NACK in the I/O thread ─────────────────────
    def ack():
        log(f"ACK tag={tag}")
        enqueue(lambda ch=channel: ch.basic_ack(tag))

    # ── 1. parse payload ────────────────────────────────────────────
    try:
        payload = json.loads(body)
        file_id = payload["file_id"]
    except Exception as exc:
        log(f"Bad payload: {exc}")
        ack()
        return

    if not cuda_available():
        log("CUDA device not available – exiting")
        os._exit(1)

    meeting_dir = RECORDINGS_ROOT / file_id
    if not meeting_dir.exists() or not meeting_dir.is_dir():
        log(f"Meeting directory not found: {meeting_dir}")
        enqueue(notify_op(channel, file_id, "srt", "error", "meeting_not_found"))
        ack()
        return

    # ── 3. watchdog (auto-ack after timeout) ────────────────────────
    def on_timeout():
        log(f"Job {file_id} timed out")
        for ft in (ft for ft in FILE_TYPES if ft not in processed):
            enqueue(notify_op(channel, file_id, ft, "error", "timeout"))
        ack()

    timer = threading.Timer(JOB_TIMEOUT, on_timeout)
    timer.start()

    try:
        # ── 4. heavy work (split + whisper or TEST_MODE sleep) ──────
        final_srt_path = run_pipeline(meeting_dir, file_id)

        if not final_srt_path.exists():
            enqueue(notify_op(channel, file_id, "srt", "error", "srt_failed"))
            ack()
            return
        enqueue(notify_op(channel, file_id, "srt", "done"))
        processed.add("srt")

        ack()

    except Exception as exc:
        err_map = {
            "no_audio": "No microphone recordings found",
            "no_srt": "No transcripts generated",
            "merge_transcripts_failed": "Could not merge transcripts",
            "invalid_audio": "Invalid or corrupt audio file",
            "meeting_not_found": "Meeting recordings directory missing",
            "no_subtitles": "No subtitles available to merge",
        }
        exc_key = str(exc)
        if exc_key.startswith("split_failed:"):
            failed_track = exc_key.split(":", 1)[1]
            msg = f"Audio split failed for {failed_track}"
        else:
            msg = err_map.get(exc_key, exc_key)
        log(f"Job {file_id} failed: {msg}")
        # notify failure only for the next expected file type
        def first_missing(done: set[str]):
            for ft in FILE_TYPES:
                if ft not in done:
                    return ft
            return None

        ft = first_missing(processed)
        if ft:
            enqueue(notify_op(channel, file_id, ft, "error", msg))
        ack()

    finally:
        timer.cancel()


# ─────────── SelectConnection callbacks ───────────
def on_message(ch, method, props, body):
    threading.Thread(
        target=spawn_worker,
        args=(ch.connection, ch, method, body),
        daemon=True
    ).start()

def on_channel_open(channel):
    log("Channel open – declaring queues")
    channel.queue_declare(queue=TASK_QUEUE, durable=True,
                          arguments={"x-max-priority": 10})
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=PREFETCH)
    channel.basic_consume(queue=TASK_QUEUE, on_message_callback=on_message)
    schedule_hb_log(channel.connection, channel)
    log("Consumer ready – waiting for jobs")

def on_connection_open(conn):
    conn.channel(on_open_callback=on_channel_open)

def on_connection_open_error(conn, exc):
    log(f"Conn open error: {exc}")
    sys.exit(1)

def on_connection_closed(conn, reason):
    log(f"Conn closed: {reason}")
    sys.exit(1)

# ─────────── main ───────────
def main():
    log(f"Connect {RABBIT_HOST}:{RABBIT_PORT} HB={HEARTBEAT}s "
        f"TEST_MODE={TEST_MODE}")
    ensure_rabbitmq()
    try:
        conn = pika.SelectConnection(
            parameters=PARAMS,
            on_open_callback=on_connection_open,
            on_open_error_callback=on_connection_open_error,
            on_close_callback=on_connection_closed
        )
    except Exception as exc:
        log(f"Failed to start SelectConnection: {exc}")
        sys.exit(1)
    try:
        conn.ioloop.start()
    except KeyboardInterrupt:
        log("Ctrl-C – exit")
        conn.close(); conn.ioloop.start()

if __name__ == "__main__":
    main()
