#!/usr/bin/env python3
# heartbeat_async_whisper_worker.py
# ------------------------------------------------------------
#  * Async SelectConnection (auto heart-beat)
#  * Background thread for long job
#  * Per-meeting processing across microphone tracks
# ------------------------------------------------------------
import os
import json
import threading
import subprocess
import sys
import re
import shutil
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pika
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


def _safe_int(value: Optional[str]) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def load_meeting_metadata(meeting_dir: Path) -> tuple[Optional[int], Dict[str, Participant]]:
    events_path = meeting_dir / "events.xml"
    participants: Dict[str, Participant] = {}
    recording_start: Optional[int] = None

    if not events_path.exists():
        log(f"events.xml not found in {meeting_dir}; offsets will default to 0")
        return recording_start, participants

    try:
        tree = ET.parse(events_path)
        root = tree.getroot()
    except ET.ParseError as exc:
        log(f"Failed to parse {events_path}: {exc}")
        return recording_start, participants

    for event in root.findall("event"):
        event_name = event.attrib.get("eventname")
        module = event.attrib.get("module")
        timestamp_utc = _safe_int(event.findtext("timestampUTC"))

        if event_name == "RecordStatusEvent" and event.findtext("status") == "true":
            if timestamp_utc is not None:
                if recording_start is None or timestamp_utc < recording_start:
                    recording_start = timestamp_utc
            continue

        if module == "PARTICIPANT" and event_name == "ParticipantJoinEvent":
            user_id = event.findtext("userId") or event.findtext("participant")
            if not user_id:
                continue
            name = event.findtext("name") or user_id
            participants[user_id] = Participant(name=name, join_ts=timestamp_utc)

    participants_summary = ", ".join(
        f"{user}:{info.join_ts}"
        for user, info in participants.items()
    ) or "<none>"
    log(
        f"Parsed metadata for meeting – recording_start={recording_start}, "
        f"participants={participants_summary}"
    )
    return recording_start, participants


def parse_microphone_file(path: Path) -> tuple[Optional[str], Optional[int]]:
    match = MICROPHONE_PATTERN.match(path.name)
    if not match:
        return None, None
    user = match.group("user")
    ts = _safe_int(match.group("ts"))
    return user, ts


def compute_track_offset(
    recording_start: Optional[int],
    join_ts: Optional[int],
    track_timestamp: Optional[int],
) -> int:
    if recording_start is None:
        return max(join_ts or track_timestamp or 0, 0)

    anchor = join_ts or track_timestamp or recording_start
    offset = anchor - recording_start
    return offset if offset >= 0 else 0


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
        "srt": f"{file_id}.srt",
    }
    suffix = suffix_map.get(ftype)

    content = None
    if status == "done" and suffix:
        try:
            script_path = Path(f"/transcripts/scripts/{suffix}")
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

    recording_start, participants = load_meeting_metadata(meeting_dir)
    staging_dir = Path("/app/staging") / meeting_id
    staging_dir.mkdir(parents=True, exist_ok=True)

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
        offset_ms = compute_track_offset(recording_start, join_ts, track_ts)

        log(
            f"Processing track {mic_path.name}: speaker={speaker_name}, "
            f"join_ts={join_ts}, track_ts={track_ts}, offset={offset_ms}"
        )

        with open("/logs/split.log", "a") as split_log:
            rc = subprocess.run(
                ["/app/split_audio.sh", str(mic_path), str(offset_ms)],
                stdout=split_log,
                stderr=subprocess.STDOUT,
            ).returncode
        if rc != 0:
            raise RuntimeError(f"split_failed:{mic_path.name}")

        try:
            srt_path = process_file(
                str(mic_path),
                MODEL,
                destination_dir=staging_dir,
                final_basename=mic_path.stem,
                finalize=False,
                generate_txt=False,
            )
        except (torch.cuda.CudaError, torch.cuda.OutOfMemoryError) as fatal:
            log(f"Fatal CUDA error: {fatal}; exiting so the container restarts")
            os._exit(1)
        except RuntimeError as exc:
            raise RuntimeError(str(exc))

        per_track_srts.append((str(srt_path), speaker_name))

    if not per_track_srts:
        raise RuntimeError("no_srt")

    transcripts_dir = Path("/app/scripts")
    transcripts_dir.mkdir(parents=True, exist_ok=True)
    final_srt_path = transcripts_dir / f"{meeting_id}.srt"

    try:
        merge_absolute_srts(per_track_srts, str(final_srt_path))
    except RuntimeError as exc:
        raise RuntimeError(str(exc))

    log(f"Merged {len(per_track_srts)} track transcripts into {final_srt_path}")

    # try:
    #     shutil.rmtree(staging_dir)
    #     log(f"Removed staging directory {staging_dir}")
    # except OSError:
    #     pass

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
