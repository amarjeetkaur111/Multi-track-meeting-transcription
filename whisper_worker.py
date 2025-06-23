#!/usr/bin/env python3
# whisper_worker.py  –  GPU Whisper worker with rock-solid heartbeat (Option B)

import os, json, subprocess, shutil, time, threading
from pathlib import Path

import requests, pika
from logger import log

# ────────────────  RabbitMQ / env  ────────────────
RABBIT_HOST     = os.getenv("RABBITMQ_HOST")
RABBIT_PORT     = int(os.getenv("RABBITMQ_PORT"))
RABBIT_USER     = os.getenv("RABBITMQ_USER")
RABBIT_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "")
RABBIT_VHOST    = os.getenv("RABBITMQ_VHOST")
TASK_QUEUE      = os.getenv("RABBITMQ_QUEUE")
RESULT_QUEUE    = os.getenv("RESULT_QUEUE")
HEARTBEAT       = int(os.getenv("RABBITMQ_HEARTBEAT", "60"))  # client proposal

os.environ["WHISPER_BACKEND"] = "local_whisper"

PARAMS = dict(
    host=RABBIT_HOST,
    port=RABBIT_PORT,
    virtual_host=RABBIT_VHOST,
    heartbeat=HEARTBEAT,
)
if RABBIT_USER:
    PARAMS["credentials"] = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)

FILE_TYPES = ["srt", "txt", "summary", "chat", "speakers"]

# ────────────────  connection + background heartbeat  ────────────────
def start_connection():
    conn = pika.BlockingConnection(pika.ConnectionParameters(**PARAMS))
    ch   = conn.channel()
    ch.queue_declare(queue=TASK_QUEUE,   durable=True,
                     arguments={"x-max-priority": 10})
    ch.queue_declare(queue=RESULT_QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)

    stop_evt = threading.Event()

    def _hb():
        while not stop_evt.is_set():
            try:
                conn.process_data_events()          # sends heartbeat
            except Exception:
                pass
            stop_evt.wait(30)                       # every 30 s

    threading.Thread(target=_hb, daemon=True).start()
    log(f"Connected to RabbitMQ at {RABBIT_HOST}:{RABBIT_PORT} "
        f"(heartbeat={HEARTBEAT})")
    return conn, ch, stop_evt


connection, channel, hb_evt = start_connection()

# ────────────────  helpers  ────────────────
def run(cmd, log_path: str | None = None) -> int:
    if log_path:
        with open(log_path, "a") as f:
            return subprocess.run(cmd, check=False, env=os.environ,
                                  stdout=f, stderr=subprocess.STDOUT).returncode
    return subprocess.run(cmd, check=False, env=os.environ).returncode


def parse_payload(body: bytes) -> dict:
    try:
        data = json.loads(body)
        if isinstance(data, dict) and "payload" in data:
            nested = json.loads(data["payload"])
            if isinstance(nested, dict):
                data.update(nested)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def notify_file(file_id: str, ftype: str, status: str,
                error: str | None = None) -> None:
    data      = {"file_id": file_id, "type": ftype, "status": status}
    base_url  = os.getenv("BBB_URL", "").rstrip("/")
    suffix    = {
        "srt":      f"{file_id}.srt",
        "txt":      f"{file_id}.txt",
        "summary":  f"{file_id}_summary.txt",
        "speakers": f"{file_id}_speakers.txt",
        "chat":     f"{file_id}_chat.txt",
    }.get(ftype)
    if status == "done" and base_url and suffix:
        data["script"] = f"{base_url}/{suffix}"
        log(f"File URL: {base_url}/{suffix}")
    elif base_url:
        log(f"File URL: {base_url}/{file_id} ({status})")
    if error:
        data["error"] = error
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(data),
        properties=pika.BasicProperties(delivery_mode=2),
    )

# ────────────────  main job pipeline  ────────────────
def process(file_id: str, url: str) -> None:
    processed, state = set(), "error"

    def mark(ft, st, msg=None):
        notify_file(file_id, ft, st, msg)
        processed.add(ft)

    log(f"Starting job {file_id}")

    audio_dir = Path("/app/queue");  audio_dir.mkdir(parents=True, exist_ok=True)
    audio     = audio_dir / f"{file_id}.ogg"
    txt       = f"/transcripts/scripts/{file_id}.txt"
    speakers  = f"/transcripts/scripts/{file_id}_speakers.txt"
    chat      = f"/transcripts/scripts/{file_id}_chat.txt"
    summary   = f"/transcripts/scripts/{file_id}_summary.txt"

    try:
        # ── 1. download ────────────────────────────────────────────────
        log(f"Downloading audio from {url}")
        try:
            with requests.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(audio, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        if chunk:
                            f.write(chunk)
        except Exception as e:
            log(f"Failed to download audio: {e}")
            for ft in FILE_TYPES: mark(ft, "error", "download_failed")
            return
        if not audio.exists():
            for ft in FILE_TYPES: mark(ft, "error", "missing_audio")
            return

        # ── 2. split audio ────────────────────────────────────────────
        log("Splitting audio")
        if run(["/app/split_audio.sh", audio], log_path="/app/logs/split.log"):
            raise RuntimeError("split failed")

        # ── 3. transcribe ─────────────────────────────────────────────
        log("Transcribing chunks")
        if run(["python3", "/app/process_audio.py", audio]):
            mark("srt", "error", "transcribe failed")
            mark("txt", "error", "transcribe failed")
            raise RuntimeError("transcribe failed")
        mark("srt", "done")
        mark("txt", "done")

        # ── 4. merge speakers + chat ──────────────────────────────────
        log("Merging speakers and chat")
        merge_cmd = [
            "python3", "/app/merge_speakers.py",
            f"/raw/{file_id}/events.xml", txt, speakers,
            f"/transcripts/scripts/{file_id}_chat.txt",
        ]
        if run(merge_cmd):
            mark("speakers", "error", "merge failed")
            mark("chat", "error", "merge failed")
            raise RuntimeError("merge failed")
        mark("speakers", "done")
        if Path(chat).is_file():
            mark("chat", "done")

        # ── 5. summarise (≥500 words) ─────────────────────────────────
        word_count = 0
        if Path(speakers).is_file():
            try:
                word_count = len(Path(speakers).read_text("utf-8").split())
            except Exception:
                pass

        if word_count < 500:
            log(f"Transcript has only {word_count} words → skip summary")
            Path(summary).write_text("File was too small to summarise.")
            mark("summary", "done")
        else:
            log("Generating summary")
            if run(["python3", "/app/gpt_summary.py", file_id]):
                mark("summary", "error", "summary failed")
                raise RuntimeError("summary failed")
            mark("summary", "done")

        state = "done"

    except Exception as e:
        log(f"Job {file_id} failed: {e}")
        for ft in (ft for ft in FILE_TYPES if ft not in processed):
            mark(ft, "error", str(e))

    finally:
        try:    audio.unlink(missing_ok=True)
        except Exception: pass
        try:    shutil.rmtree(f"/app/chunks/{file_id}")
        except Exception: pass
        log(f"Finished job {file_id} with state {state}")

# ────────────────  consumer callback  ────────────────
def on_message(ch, method, props, body):
    try:
        data    = parse_payload(body)
        file_id = data.get("file_id");  url = data.get("url")
        if not file_id or not url:
            raise ValueError("Invalid payload")
        process(file_id, url)
    except Exception as e:
        log(f"Message processing failed: {e}")
    finally:
        try:
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except pika.exceptions.ChannelWrongStateError as e:
            log(f"Ack failed (channel closed): {e}. Message will be re-queued.")

# ────────────────  main consume loop  ────────────────
def consume():
    global connection, channel, hb_evt
    while True:
        try:
            channel.basic_consume(queue=TASK_QUEUE, on_message_callback=on_message)
            log("Worker waiting for jobs…")
            channel.start_consuming()

        except (pika.exceptions.AMQPError,
                pika.exceptions.ChannelWrongStateError) as e:
            log(f"Connection lost: {e}. Re-connecting in 5 s…")
            hb_evt.set()         # stop old HB thread
            try: channel.close()
            except Exception: pass
            try: connection.close()
            except Exception: pass
            time.sleep(5)
            connection, channel, hb_evt = start_connection()

        except KeyboardInterrupt:
            hb_evt.set()
            break

if __name__ == "__main__":
    consume()
