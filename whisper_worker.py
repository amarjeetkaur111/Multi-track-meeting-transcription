import os
import json
import subprocess
from pathlib import Path
import shutil
import requests
import pika

from logger import log

RABBIT_HOST = os.getenv("RABBITMQ_HOST")
RABBIT_PORT = int(os.getenv("RABBITMQ_PORT"))
RABBIT_USER = os.getenv("RABBITMQ_USER")
RABBIT_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
RABBIT_VHOST = os.getenv("RABBITMQ_VHOST")
TASK_QUEUE = os.getenv("RABBITMQ_QUEUE")
RESULT_QUEUE = os.getenv("RESULT_QUEUE")

os.environ["WHISPER_BACKEND"] = "local_whisper"

params = {
    "host": RABBIT_HOST,
    "port": RABBIT_PORT,
    "virtual_host": RABBIT_VHOST,
}
if RABBIT_USER:
    params["credentials"] = pika.PlainCredentials(
        RABBIT_USER, RABBIT_PASSWORD or ""
    )
connection = pika.BlockingConnection(pika.ConnectionParameters(**params))
channel = connection.channel()
channel.basic_qos(prefetch_count=1)

FILE_TYPES = ["srt", "txt", "summary", "chat", "speakers"]

log(f"Connected to RabbitMQ at {RABBIT_HOST}:{RABBIT_PORT}")


def run(cmd):
    return subprocess.run(cmd, check=False, env=os.environ).returncode


def parse_payload(body: bytes) -> dict:
    """Decode message body and unpack nested payload field if present."""
    try:
        data = json.loads(body)
        if isinstance(data, dict) and "payload" in data:
            try:
                nested = json.loads(data["payload"])
                if isinstance(nested, dict):
                    data.update(nested)
            except Exception:
                pass
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def notify_file(file_id: str, file_type: str, status: str, error: str | None = None) -> None:
    data = {"file_id": file_id, "type": file_type, "status": status}
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
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(data),
        properties=pika.BasicProperties(delivery_mode=2),
    )


def process(file_id: str, url: str) -> None:
    processed: set[str] = set()
    state = "error"

    def mark(ft: str, status: str, msg: str | None = None) -> None:
        notify_file(file_id, ft, status, msg)
        processed.add(ft)

    log(f"Starting job {file_id}")

    audio_dir = Path("/app/queue")
    audio_dir.mkdir(parents=True, exist_ok=True)
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
            for ft in FILE_TYPES:
                mark(ft, "error", "download_failed")
            return

        if not Path(audio).is_file():
            log("Audio file missing after download")
            for ft in FILE_TYPES:
                mark(ft, "error", "missing_audio")
            return

        log("Splitting audio")
        if run(["/app/split_audio.sh", audio]):
            raise RuntimeError("split failed")

        env = os.environ.copy()
        env["WHISPER_BACKEND"] = "local_whisper"
        cmd = ["python3", "/app/process_audio.py", audio]
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
    except Exception as e:
        log(f"Job {file_id} failed: {e}")
        for ft in FILE_TYPES:
            if ft not in processed:
                mark(ft, "error", str(e))
    finally:
        try:
            Path(audio).unlink(missing_ok=True)
        except Exception:
            pass
        try:
            shutil.rmtree(f"/app/chunks/{file_id}")
        except Exception:
            pass
        log(f"Finished job {file_id} with state {state}")


def on_message(ch, method, properties, body) -> None:
    try:
        data = parse_payload(body)
        file_id = data.get("file_id")
        url = data.get("url")
        if not file_id or not url:
            raise ValueError("Invalid payload")
        process(file_id, url)
    except Exception as e:
        log(f"Message processing failed: {e}")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


log("Worker waiting for jobs...")
channel.basic_consume(queue=TASK_QUEUE, on_message_callback=on_message)
channel.start_consuming()

