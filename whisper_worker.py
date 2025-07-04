#!/usr/bin/env python3
# heartbeat_async_whisper_worker.py
# ------------------------------------------------------------
#  * Async SelectConnection (auto heart-beat)
#  * Background thread for long job
#  * Per-file notifications (srt, txt, summary, chat, speakers)
#  * TEST_MODE=1  -> just sleep(120) and emit fake files
# ------------------------------------------------------------
import os, json, threading, subprocess, shutil, time
from pathlib import Path
import pika, requests
from dotenv import load_dotenv
from logger import log

load_dotenv()

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

BBB_URL         = os.getenv("BBB_URL", "").rstrip("/")
CREDENTIALS     = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)

PARAMS = pika.ConnectionParameters(
    host     = RABBIT_HOST,
    port     = RABBIT_PORT,
    virtual_host = RABBIT_VHOST,
    credentials  = CREDENTIALS,
    heartbeat    = HEARTBEAT,
    blocked_connection_timeout = HEARTBEAT * 2,
)

FILE_TYPES = ["srt", "txt", "summary", "chat", "speakers"]

# ─────────── heartbeat logger ───────────
def schedule_hb_log(conn, chan):
    def _log():
        log(f"HB tick – conn_open: {conn.is_open} "
            f"chan_open: {chan.is_open}")
        conn.ioloop.call_later(HB_LOG_INTERVAL, _log)
    conn.ioloop.call_later(HB_LOG_INTERVAL, _log)

# ─────────── RESULT_QUEUE notifier ───────────
def notify_file(channel, file_id, ftype, status, error=None):
    body = {"file_id": file_id, "type": ftype, "status": status}
    if status == "done" and BBB_URL:
        suffix = {
            "srt":      f"{file_id}.srt",
            "txt":      f"{file_id}.txt",
            "summary":  f"{file_id}_summary.txt",
            "chat":     f"{file_id}_chat.txt",
            "speakers": f"{file_id}_speakers.txt",
        }.get(ftype)
        if suffix:
            body["script"] = f"{BBB_URL}/{suffix}"
    if error:
        body["error"] = error
    channel.basic_publish(
        exchange="",
        routing_key=RESULT_QUEUE,
        body=json.dumps(body),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log(f"[notify] {file_id} {ftype} → {status}")

# ─────────── heavy job (split + Whisper) or dummy sleep ───────────
def run_pipeline(audio_path, file_id):
    if TEST_MODE:
        log("TEST_MODE=1 → sleeping 60 s instead of real processing")
        time.sleep(60)
        # create two tiny dummy files so downstream steps see something
        Path(f"/transcripts/scripts/{file_id}.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nTEST\n")
        Path(f"/transcripts/scripts/{file_id}.txt").write_text("TEST")
        return

    # real path: split then process_audio.py
    if subprocess.run(["/app/split_audio.sh", audio_path]).returncode != 0:
        raise RuntimeError("split_audio.sh failed")
    rc = subprocess.run(["python3", "/app/process_audio.py", audio_path]).returncode
    if rc != 0:
        raise RuntimeError(f"process_audio.py exited {rc}")

def merge_speakers_chat(file_id: str) -> None:
    """Run merge_speakers.py to add speaker labels and extract chat."""
    txt = f"/transcripts/scripts/{file_id}.txt"
    speakers = f"/transcripts/scripts/{file_id}_speakers.txt"
    chat = f"/transcripts/scripts/{file_id}_chat.txt"
    events = f"/raw/{file_id}/events.xml"
    rc = subprocess.run([
        "python3", "/app/merge_speakers.py",
        events, txt, speakers, chat,
    ]).returncode
    if rc != 0:
        raise RuntimeError(f"merge_speakers.py exited {rc}")


def generate_summary(file_id: str) -> None:
    """Run gpt_summary.py for *file_id*."""
    rc = subprocess.run(["python3", "/app/gpt_summary.py", file_id]).returncode
    if rc != 0:
        raise RuntimeError(f"gpt_summary.py exited {rc}")

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

    def nack(requeue=True):
        log(f"NACK tag={tag} requeue={requeue}")
        enqueue(lambda ch=channel: ch.basic_nack(tag, requeue=requeue))

    # ── 1. parse payload ────────────────────────────────────────────
    try:
        payload = json.loads(body)
        file_id, url = payload["file_id"], payload["url"]
    except Exception as exc:
        log(f"Bad payload: {exc}")
        nack(False)
        return

    # ── 2. download audio ───────────────────────────────────────────
    audio_tmp = f"/app/queue/{file_id}.ogg"
    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(audio_tmp, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
    except Exception as exc:
        log(f"Download failed: {exc}")
        # queue one “error” message per file type, then requeue job
        for ft in FILE_TYPES:
            enqueue(notify_op(channel, file_id, ft, "error", "download_failed"))
        nack(True)
        return

    # ── 3. watchdog (auto-nack after timeout) ───────────────────────
    timer = threading.Timer(JOB_TIMEOUT, lambda: nack(True))
    timer.start()

    try:
        # ── 4. heavy work (split + whisper or TEST_MODE sleep) ──────
        run_pipeline(audio_tmp, file_id)

        # notify srt + txt first
        for ft in ("srt", "txt"):
            enqueue(notify_op(channel, file_id, ft, "done"))
            processed.add(ft)

        # ── merge speakers + chat ─────────────────────────────────
        try:
            merge_speakers_chat(file_id)
            enqueue(notify_op(channel, file_id, "speakers", "done"))
            processed.add("speakers")
            enqueue(notify_op(channel, file_id, "chat", "done"))
            processed.add("chat")
        except Exception as exc_merge:
            log(f"Merge failed: {exc_merge}")
            for ft in ("speakers", "chat", "summary"):
                enqueue(notify_op(channel, file_id, ft, "error", str(exc_merge)))
                processed.add(ft)
            ack()
            return

        # ── GPT summary ───────────────────────────────────────────
        try:
            generate_summary(file_id)
            enqueue(notify_op(channel, file_id, "summary", "done"))
            processed.add("summary")
        except Exception as exc_sum:
            log(f"Summary failed: {exc_sum}")
            enqueue(notify_op(channel, file_id, "summary", "error", str(exc_sum)))
            processed.add("summary")
            ack()
            return

        ack()

    except Exception as exc:
        log(f"Job {file_id} failed: {exc}")
        # send “error” for anything not already marked done
        for ft in (ft for ft in FILE_TYPES if ft not in processed):
            enqueue(notify_op(channel, file_id, ft, "error", str(exc)))
        nack(True)

    finally:
        timer.cancel()
        try:
            Path(audio_tmp).unlink(missing_ok=True)
        except Exception:
            pass


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

# ─────────── main ───────────
def main():
    log(f"Connect {RABBIT_HOST}:{RABBIT_PORT} HB={HEARTBEAT}s "
        f"TEST_MODE={TEST_MODE}")
    conn = pika.SelectConnection(
        parameters=PARAMS,
        on_open_callback=on_connection_open,
        on_open_error_callback=lambda c,e: log(f"Conn open error: {e}"),
        on_close_callback=lambda c,rc,txt: log(f"Conn closed: {rc} {txt}")
    )
    try:
        conn.ioloop.start()
    except KeyboardInterrupt:
        log("Ctrl-C – exit")
        conn.close(); conn.ioloop.start()

if __name__ == "__main__":
    main()
