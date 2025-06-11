import os
import sys
import subprocess
from pathlib import Path

import redis

GROUP = "whisper-workers"
STREAM_HIGH = "whisper:jobs:high"
STREAM_LOW = "whisper:jobs:low"
STREAM_DONE = "whisper:done"
STREAM_FAILED = "whisper:failed"
LOCK_PREFIX = "lock:"

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

BACKEND = sys.argv[1] if len(sys.argv) > 1 else os.getenv("WHISPER_BACKEND", "open_source")
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

def process(file_id, stream, msg_id):
    lock_key = f"{LOCK_PREFIX}{file_id}"
    if not r.set(lock_key, "1", nx=True, ex=600):
        r.xack(stream, GROUP, msg_id)
        r.xadd(STREAM_FAILED, {"file_id": file_id, "error": "locked"})
        return

    audio = f"/app/queue/{file_id}.ogg"
    txt = f"/transcripts/scripts/{file_id}.txt"
    speakers = f"/transcripts/scripts/{file_id}_speakers.txt"

    try:
        if not Path(audio).is_file():
            r.xack(stream, GROUP, msg_id)
            r.xadd(STREAM_FAILED, {"file_id": file_id, "error": "missing_audio"})
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
            raise RuntimeError("transcribe failed")

        merge_cmd = [
            "python3",
            "/app/merge_speakers.py",
            f"/raw/{file_id}/events.xml",
            txt,
            speakers,
            f"/transcripts/scripts/{file_id}_chat.txt",
        ]
        if run(merge_cmd):
            raise RuntimeError("merge failed")

        if run(["python3", "/app/gpt_summary.py", file_id]):
            raise RuntimeError("summary failed")

        r.xack(stream, GROUP, msg_id)
        r.xadd(STREAM_DONE, {"file_id": file_id})
    except Exception as e:
        r.xack(stream, GROUP, msg_id)
        r.xadd(STREAM_FAILED, {"file_id": file_id, "error": str(e)})
    finally:
        r.delete(lock_key)
        r.delete(f"force_process:{file_id}")

def next_job():
    for stream in (STREAM_HIGH, STREAM_LOW):
        msgs = r.xreadgroup(GROUP, CONSUMER, {stream: ">"}, count=1, block=BLOCK_MS)
        if msgs:
            name, messages = msgs[0]
            msg_id, data = messages[0]
            file_id = data.get("file_id")
            if file_id:
                return stream, msg_id, file_id
    return None

while True:
    job = next_job()
    if not job:
        continue
    process(job[2], job[0], job[1])
