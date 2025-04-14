from flask import Flask, request, jsonify
import os
import re
import redis
import requests

from webhook_utils import async_send_webhook  # <-- import here

app = Flask(__name__)

# Environment Variables
AUTH_KEY = "JcviID7zUMoEuFmSN0GT043XisDda4eppRRyCCS3y0"
REDIS_HOST = os.getenv("REDIS_HOST", "redis")  # Redis runs as a separate container
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# File Paths
UPLOAD_FOLDER = "/app/queue"
TRANSCRIPTS_QUEUE_FOLDER = "/transcripts/queue"
TRANSCRIPTS_SCRIPT_FOLDER = "/transcripts/scripts"
# WEBHOOK_URL = "https://biggerbluebutton.com/mobile/webhook/audio-processed"
# WEBHOOK_URL = "https://540c-86-98-4-252.ngrok-free.app/mobile/webhook/audio-processed"


os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TRANSCRIPTS_QUEUE_FOLDER, exist_ok=True)
os.makedirs(TRANSCRIPTS_SCRIPT_FOLDER, exist_ok=True)

@app.route("/", methods=["GET"])
def index():
    return "Hello, this is the root route!", 200

@app.route("/process", methods=["POST"])
def process_audio():
    """
    1. Validate API Key
    2. Parse file_id from URL
    3. Check if file is already processed; if so, send webhook and return.
    4. Check if file is locked; if so, return that it's in process.
    5. Check if file is in any Redis queue:
       a) If it's in a low queue and we want low priority => return "already queued in X".
       b) If it's in a low queue and we want high priority => move it to the corresponding high queue.
       c) If it's in a high queue already => return "already queued in X".
    6. If not in any queue, download and add to either split_high/split_low.
    """
    try:
        data = request.json
        api_key = data.get("api_key")
        full_url = data.get("url")
        priority = data.get("priority", "low").lower()

        if api_key != AUTH_KEY:
            return jsonify({"status": "unauthorized", "error": "Invalid API key"}), 401
        
        if not full_url:
            return jsonify({"status": "bad_request", "error": "No URL provided"}), 400
        
        match = re.search(r"podcast/([a-fA-F0-9-]+)/audio.ogg", full_url)
        if not match:
            return jsonify({"status": "bad_request", "error": "Invalid URL format"}), 400

        file_id = match.group(1)

        # File Paths
        srt_file_path = os.path.join(TRANSCRIPTS_SCRIPT_FOLDER, f"{file_id}.srt")
        txt_file_path = os.path.join(TRANSCRIPTS_SCRIPT_FOLDER, f"{file_id}.txt")

        # Check if already processed
        if os.path.exists(srt_file_path) and os.path.exists(txt_file_path):
            webhook_data = {
                "file_id": file_id,
                "script": f"https://biggerbluebutton.com/playback/transcripts/{file_id}.txt",
                "status": "done"
            }
            async_send_webhook(webhook_data)
            return jsonify({"status": "done", "message": "File already processed"}), 200

        # 2) Check if file is locked => means it is currently being split or transcribed
        lock_key = f"lock:{file_id}"
        if redis_client.exists(lock_key):
            return jsonify({"status": "in_process", "message": "File is currently processing"}), 409
        
        # 3) Check if file is in ANY Redis queue
        #    We'll handle each scenario based on the requested logic.
        all_queues = ["split_high", "split_low", "whisper_high", "whisper_low"]
        for q in all_queues:
            queue_items = redis_client.lrange(q, 0, -1)
            if file_id in queue_items:
                # If the file is already in a queue:
                if priority == "low":
                    # Condition 3: If file is already in split_low or whisper_low and new priority is low
                    # => "File already queued in <queue>"
                    # Or if it's in a high queue, we also inform that it's already in a queue.
                    return jsonify({"status": "in_process", 
                                    "message": f"File already queued in {q}"}), 200
                else:
                    # priority == "high"
                    # Condition 4: If file is in split_low but new priority is high
                    # => move from split_low to split_high
                    if q == "split_low":
                        redis_client.lrem("split_low", 0, file_id)
                        redis_client.lpush("split_high", file_id)
                        return jsonify({"status": "requeued",
                                        "message": "File queue changed from split_low to split_high"}), 200

                    # Condition 5: If file is in whisper_low but new priority is high and not locked
                    # => move from whisper_low to whisper_high
                    elif q == "whisper_low":
                        redis_client.lrem("whisper_low", 0, file_id)
                        redis_client.lpush("whisper_high", file_id)
                        return jsonify({"status": "requeued",
                                        "message": "File queue changed from whisper_low to whisper_high"}), 200

                    else:
                        # If it's already in a high queue (split_high or whisper_high),
                        # we just return that it's already queued.
                        return jsonify({"status": "in_process",
                                        "message": f"File already queued in {q}"}), 200

        # 4) If we reach here, the file is not processed yet, not locked, and not in a queue
        # Download the file
        file_path = os.path.join(UPLOAD_FOLDER, f"{file_id}.ogg")
        if not download_file(full_url, file_path):
            return jsonify({"status": "error", "error": "Failed to download audio file"}), 500

        # 5) Push to the correct queue based on priority
        if priority == "high":
            redis_client.lpush("split_high", file_id)
            return jsonify({"status": "processing", "message": "File queued in split_high"}), 200
        else:
            redis_client.lpush("split_low", file_id)
            return jsonify({"status": "processing", "message": "File queued in split_low"}), 200
    
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500


def download_file(url, path):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except requests.RequestException:
        return False


# def send_webhook(data):
#     try:
#         response = requests.post(WEBHOOK_URL, json=data, headers={"Content-Type": "application/json"})
#         response.raise_for_status()
#     except requests.RequestException as e:
#         print(f"Failed to send webhook: {str(e)}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
