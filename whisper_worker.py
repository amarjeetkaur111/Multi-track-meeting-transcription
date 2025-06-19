import os
import sys
import subprocess
import json
import time
import threading
from pathlib import Path
import shutil
import requests

import redis

from logger import log


log("YAHOOO STARTED")
GROUP = "whisper-workers"
STREAM_HIGH = "whisper:jobs:high"
STREAM_LOW = "whisper:jobs:low"
STREAM_FAILED = "whisper:failed"
STREAM_FILES = "whisper:file"
FILE_TYPES = ["srt", "txt", "summary", "chat", "speakers"]
LOCK_PREFIX = "lock:"
CLAIM_IDLE_MS = int(os.getenv("CLAIM_IDLE_MS", "900000"))

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MACHINE_NUMBER = os.getenv("CONSUMER_NAME") 

BACKEND = sys.argv[1] if len(sys.argv) > 1 else os.getenv("WHISPER_BACKEND", "local_whisper")

CONSUMER =  f"{BACKEND}-{MACHINE_NUMBER}"

log(f"Backend: {BACKEND} | WhisperBackend : {os.getenv('WHISPER_BACKEND')} | CONSUMER: {CONSUMER}")

os.environ["WHISPER_BACKEND"] = BACKEND
BLOCK_MS = 5000

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
log(f"Worker {CONSUMER} connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")

def purge_stale_consumers(stream, group, max_idle_ms=3600_000):
    try:
        for c in r.xinfo_consumers(stream, group):
            if c['idle'] > max_idle_ms:
                r.xgroup_delconsumer(stream, group, c['name'])
    except Exception:
        pass

for stream in (STREAM_HIGH, STREAM_LOW):
    purge_stale_consumers(stream, GROUP)
    try:
        r.xgroup_create(stream, GROUP, id="0", mkstream=True)
        log(f"Ensured consumer group on {stream}")
    except redis.exceptions.ResponseError as e:
        log(str(e))
        if "BUSYGROUP" not in str(e):
            raise

log("Worker startup complete. Waiting for jobs...")

def run(cmd):
    return subprocess.run(cmd, check=False, env=os.environ).returncode

def extract_file_id(data: dict) -> str | None:
    """Return the file_id from a job entry.

    Messages may include the file_id directly or wrapped in a JSON
    string under the ``payload`` field as used by the Laravel producer.
    """

    fid = data.get("file_id")
    if fid:
        return fid
    payload = data.get("payload")
    if payload:
        try:
            obj = json.loads(payload)
            return obj.get("file_id")
        except Exception:
            return None
    return None

def extract_url(data: dict) -> str | None:
    """Return the audio URL from a job entry."""
    url = data.get("url")
    if url:
        return url
    payload = data.get("payload")
    if payload:
        try:
            obj = json.loads(payload)
            return obj.get("url")
        except Exception:
            return None
    return None

def notify_file(file_id: str, file_type: str, status: str, error: str | None = None) -> None:
    """Publish file processing status via Redis."""
    data = {
        "file_id": file_id,
        "type": file_type,
        "status": status,
    }
    base_url = os.getenv("BBB_URL")
    if status == "done" and base_url:
        base_url = base_url.rstrip("/")
        suffix = {
            "srt": f"{file_id}.srt",
            "txt": f"{file_id}.txt",
            "summary": f"{file_id}_summary.txt",
            "speakers": f"{file_id}_speakers.txt",
            "chat": f"{file_id}_chat.txt",
        }.get(file_type)
        if suffix:
            data["script"] = f"{base_url}/{suffix}"
    if error:
        data["error"] = error
    # Redis streams require string values
    r.xadd(STREAM_FILES, {k: str(v) for k, v in data.items()})
    r.publish(STREAM_FILES, json.dumps(data))

def process(file_id, url, stream, msg_id):
    lock_key = f"{LOCK_PREFIX}{file_id}"
    index_key = f"whisper:index:{file_id}"
    processed: set[str] = set()
    state = "error"

    def mark(ft: str, status: str, msg: str | None = None) -> None:
        notify_file(file_id, ft, status, msg)
        processed.add(ft)

    log(f"Starting job {file_id}")

    if not r.set(lock_key, "1", nx=True, ex=600):
        r.xack(stream, GROUP, msg_id)
        r.xadd(STREAM_FAILED, {"file_id": file_id, "error": "locked"})
        for ft in FILE_TYPES:
            mark(ft, "error", "locked")
        return

    r.hset(index_key, mapping={"state": "processing"})

    stop_extend = threading.Event()

    def extend_lock() -> None:
        while not stop_extend.wait(300):
            try:
                r.pexpire(lock_key, 1200)
            except Exception:
                pass

    threading.Thread(target=extend_lock, daemon=True).start()

    log(f"Processing audio file {file_id}.ogg")

    audio_dir = Path("/app/queue")
    audio_dir.mkdir(parents=True, exist_ok=True)   # guarantees directory exists
    audio = audio_dir / f"{file_id}.ogg"
    txt = f"/transcripts/scripts/{file_id}.txt"
    speakers = f"/transcripts/scripts/{file_id}_speakers.txt"
    chat = f"/transcripts/scripts/{file_id}_chat.txt"

    try:
        log(f"Downloading audio from {url}")
        try:
            resp = requests.get(url, stream=True, timeout=60)
            resp.raise_for_status()
            with open(audio, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        except Exception as e:
            log(f"Failed to download audio: {e}")
            r.xack(stream, GROUP, msg_id)
            r.xadd(STREAM_FAILED, {"file_id": file_id, "error": "download_failed"})
            for ft in FILE_TYPES:
                mark(ft, "error", "download_failed")
            return

        if not Path(audio).is_file():
            log("Audio file missing after download")
            r.xack(stream, GROUP, msg_id)
            r.xadd(STREAM_FAILED, {"file_id": file_id, "error": "missing_audio"})
            for ft in FILE_TYPES:
                mark(ft, "error", "missing_audio")
            return

        log("Splitting audio")
        if run(["/app/split_audio.sh", audio]):
            raise RuntimeError("split failed")

        force = r.get(f"force_process:{file_id}") == "1"

        env = os.environ.copy()
        env["WHISPER_BACKEND"] = BACKEND
        cmd = ["python3", "/app/process_audio.py", audio]
        if force:
            cmd.append("--force")
        log("Transcribing chunks")
        if run(cmd):
            mark("srt", "error", "transcribe failed")
            mark("txt", "error", "transcribe failed")
            raise RuntimeError("transcribe failed")
        mark("srt", "done")
        mark("txt", "done")

        merge_cmd = [
            "python3",
            "/app/merge_speakers.py",
            f"/raw/{file_id}/events.xml",
            txt,
            speakers,
            f"/transcripts/scripts/{file_id}_chat.txt",
        ]
        log("Merging speakers and chat")
        if run(merge_cmd):
            mark("speakers", "error", "merge failed")
            mark("chat", "error", "merge failed")
            raise RuntimeError("merge failed")
        mark("speakers", "done")
        if Path(chat).is_file():
            mark("chat", "done")

        log("Generating summary")
        if run(["python3", "/app/gpt_summary.py", file_id]):
            mark("summary", "error", "summary failed")
            raise RuntimeError("summary failed")
        mark("summary", "done")
        state = "done"
        r.xack(stream, GROUP, msg_id)
    except Exception as e:
        log(f"Job {file_id} failed: {e}")
        r.xack(stream, GROUP, msg_id)
        r.xadd(STREAM_FAILED, {"file_id": file_id, "error": str(e)})
        for ft in FILE_TYPES:
            if ft not in processed:
                mark(ft, "error", str(e))
    finally:
        r.hset(index_key, mapping={"state": state})
        stop_extend.set()
        r.delete(lock_key)
        r.delete(f"force_process:{file_id}")
        try:
            Path(audio).unlink(missing_ok=True)
        except Exception:
            pass
        try:
            shutil.rmtree(f"/app/chunks/{file_id}")
        except Exception:
            pass
        log(f"Finished job {file_id} with state {state}")

def next_job():
    log("Looking for next job...")
    for stream in (STREAM_HIGH, STREAM_LOW):
        # First try to claim messages left unacknowledged by crashed workers
        try:
            _, messages = r.xautoclaim(stream, GROUP, CONSUMER,
                                       CLAIM_IDLE_MS, "0-0", count=1)
            log(f"Any Pending Message: {messages} from {stream}")
        except Exception:
            messages = []
        if not messages:
            messages = []
            log(f"Reading From : Group:{GROUP} | Consumer: {CONSUMER} | Stream: {stream} | BlockMS: {BLOCK_MS} ")
            msgs = r.xreadgroup(GROUP, CONSUMER, {stream: ">"}, count=1, block=BLOCK_MS)
            log(msgs)
            if msgs:
                _, messages = msgs[0]
        if messages:
            msg_id, data = messages[0]
            log(f"Looking for message: {data} with messageId {msg_id}")
            file_id = extract_file_id(data)
            url = extract_url(data)
            if file_id:
                log(f"Claimed job {file_id} from {stream}")
                return stream, msg_id, file_id, url
    return None

while True:
    job = next_job()
    if not job:
        time.sleep(5)
        continue
    log(f"Processing file {job[2]}")
    process(job[2], job[3], job[0], job[1])
