import os
import sys
import subprocess
import json
import time
import threading
from pathlib import Path

import redis

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

BACKEND = sys.argv[1] if len(sys.argv) > 1 else os.getenv("WHISPER_BACKEND", "local_whisper")
CONSUMER = os.getenv("CONSUMER_NAME", f"{BACKEND}-{os.getpid()}")
os.environ["WHISPER_BACKEND"] = BACKEND
BLOCK_MS = 5000

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

for stream in (STREAM_HIGH, STREAM_LOW):
    try:
        r.xgroup_create(stream, GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise

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

def process(file_id, stream, msg_id):
    lock_key = f"{LOCK_PREFIX}{file_id}"
    index_key = f"whisper:index:{file_id}"
    processed: set[str] = set()
    state = "error"

    def mark(ft: str, status: str, msg: str | None = None) -> None:
        notify_file(file_id, ft, status, msg)
        processed.add(ft)

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

    audio = f"/app/queue/{file_id}.ogg"
    txt = f"/transcripts/scripts/{file_id}.txt"
    speakers = f"/transcripts/scripts/{file_id}_speakers.txt"
    chat = f"/transcripts/scripts/{file_id}_chat.txt"

    try:
        if not Path(audio).is_file():
            r.xack(stream, GROUP, msg_id)
            r.xadd(STREAM_FAILED, {"file_id": file_id, "error": "missing_audio"})
            for ft in FILE_TYPES:
                mark(ft, "error", "missing_audio")
            return

        if run(["/app/split_audio.sh", audio]):
            raise RuntimeError("split failed")

        force = r.get(f"force_process:{file_id}") == "1"

        env = os.environ.copy()
        env["WHISPER_BACKEND"] = BACKEND
        cmd = ["python3", "/app/process_audio.py", audio]
        if force:
            cmd.append("--force")
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
        if run(merge_cmd):
            mark("speakers", "error", "merge failed")
            mark("chat", "error", "merge failed")
            raise RuntimeError("merge failed")
        mark("speakers", "done")
        if Path(chat).is_file():
            mark("chat", "done")

        if run(["python3", "/app/gpt_summary.py", file_id]):
            mark("summary", "error", "summary failed")
            raise RuntimeError("summary failed")
        mark("summary", "done")
        state = "done"
        r.xack(stream, GROUP, msg_id)
    except Exception as e:
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

def next_job():
    for stream in (STREAM_HIGH, STREAM_LOW):
        # First try to claim messages left unacknowledged by crashed workers
        try:
            res = r.xautoclaim(stream, GROUP, CONSUMER, CLAIM_IDLE_MS, "0-0", count=1)
            messages = res[1] if isinstance(res, tuple) else res[0]
        except Exception:
            messages = []
        if not messages:
            messages = []
            msgs = r.xreadgroup(GROUP, CONSUMER, {stream: ">"}, count=1, block=BLOCK_MS)
            if msgs:
                _, messages = msgs[0]
        if messages:
            msg_id, data = messages[0]
            file_id = extract_file_id(data)
            if file_id:
                return stream, msg_id, file_id
    return None

while True:
    job = next_job()
    if not job:
        continue
    process(job[2], job[0], job[1])
