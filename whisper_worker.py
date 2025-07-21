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
    suffix = {
        "srt":      f"{file_id}.srt",
        "txt":      f"{file_id}.txt",
        "summary":  f"{file_id}_summary.txt",
        "chat":     f"{file_id}_chat.txt",
        "speakers": f"{file_id}_speakers.txt",
    }.get(ftype)

    body = {
        "file_id": file_id,
        "type": ftype,
        "status": status,
        "script": (
            f"{BBB_URL}/{suffix}" if status == "done" and BBB_URL and suffix else None
        ),
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
def run_pipeline(audio_path, file_id):
    if TEST_MODE:
        log("TEST_MODE=1 → sleeping 60 s instead of real processing")
        time.sleep(60)
        # create two tiny dummy files so downstream steps see something
        Path(f"/transcripts/scripts/{file_id}.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nTEST\n")
        Path(f"/transcripts/scripts/{file_id}.txt").write_text("TEST")
        return

    # real path: split then process_audio.py
    with open("/logs/split.log", "a") as split_log:
        rc = subprocess.run(
            ["/app/split_audio.sh", audio_path],
            stdout=split_log,
            stderr=subprocess.STDOUT,
        ).returncode
    if rc != 0:
        raise RuntimeError("split_audio.sh failed")
    rc = subprocess.run(["python3", "/app/process_audio.py", audio_path]).returncode
    if rc == 2:
        raise RuntimeError("no_audio")
    if rc == 3:
        raise RuntimeError("no_srt")
    if rc == 4:
        raise RuntimeError("merge_transcripts_failed")
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
        raise RuntimeError("merge_speakers_failed")


def generate_summary(file_id: str) -> None:
    """Run gpt_summary.py for *file_id*."""
    rc = subprocess.run(["python3", "/app/gpt_summary.py", file_id]).returncode
    if rc == 2:
        raise RuntimeError("transcript_not_found")
    if rc == 3:
        raise RuntimeError("file_too_small")
    if rc != 0:
        raise RuntimeError("gpt_summary_failed")

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
        file_id, url = payload["file_id"], payload["url"]
    except Exception as exc:
        log(f"Bad payload: {exc}")
        ack()
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
        # queue one “error” message per file type and stop processing
        # only notify failure for the first expected file (srt)
        enqueue(notify_op(channel, file_id, "srt", "error", "download_failed"))
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
        run_pipeline(audio_tmp, file_id)

        srt_path = Path(f"/transcripts/scripts/{file_id}.srt")
        txt_path = Path(f"/transcripts/scripts/{file_id}.txt")

        if not srt_path.exists():
            enqueue(notify_op(channel, file_id, "srt", "error", "srt_failed"))
            ack()
            return
        enqueue(notify_op(channel, file_id, "srt", "done"))
        processed.add("srt")

        if not txt_path.exists():
            enqueue(notify_op(channel, file_id, "txt", "error", "txt_failed"))
            ack()
            return
        enqueue(notify_op(channel, file_id, "txt", "done"))
        processed.add("txt")

        # ── merge speakers + chat ─────────────────────────────────
        merge_err = None
        try:
            merge_speakers_chat(file_id)
        except Exception as exc_merge:
            merge_err = exc_merge
            log(f"Merge failed: {exc_merge}")

        speakers_path = Path(f"/transcripts/scripts/{file_id}_speakers.txt")
        chat_path = Path(f"/transcripts/scripts/{file_id}_chat.txt")

        if not speakers_path.exists():
            enqueue(notify_op(channel, file_id, "speakers", "error",
                             str(merge_err) if merge_err else "speakers_failed"))
            ack()
            return
        enqueue(notify_op(channel, file_id, "speakers", "done"))
        processed.add("speakers")

        if chat_path.exists() and chat_path.stat().st_size > 0:
            enqueue(notify_op(channel, file_id, "chat", "done"))
            processed.add("chat")
        elif merge_err is not None:
            enqueue(notify_op(channel, file_id, "chat", "error", str(merge_err)))

        # ── GPT summary ───────────────────────────────────────────
        sum_err = None
        try:
            generate_summary(file_id)
        except Exception as exc_sum:
            sum_err = exc_sum
            log(f"Summary failed: {exc_sum}")

        summary_path = Path(f"/transcripts/scripts/{file_id}_summary.txt")
        if not summary_path.exists():
            enqueue(notify_op(channel, file_id, "summary", "error",
                             str(sum_err) if sum_err else "summary_failed"))
        else:
            enqueue(notify_op(channel, file_id, "summary", "done"))
            processed.add("summary")

        ack()

    except Exception as exc:
        err_map = {
            "no_audio": "No Audio in meeting",
            "no_srt": "No srt file for the meeting",
            "merge_transcripts_failed": "Could not merge transcripts",
            "merge_speakers_failed": "Speaker merge failed",
            "file_too_small": "File too small for summarization",
            "gpt_summary_failed": "Summary generation failed",
            "transcript_not_found": "Transcript not found",
        }
        msg = err_map.get(str(exc), str(exc))
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
        on_close_callback=lambda c,reason: log(f"Conn closed: {reason}")
    )
    try:
        conn.ioloop.start()
    except KeyboardInterrupt:
        log("Ctrl-C – exit")
        conn.close(); conn.ioloop.start()

if __name__ == "__main__":
    main()
