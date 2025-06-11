from flask import Flask, request, jsonify
import os
import re
import redis
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv() 

from webhook_utils import async_send_webhook  # <-- import here

app = Flask(__name__)

# Environment Variables
AUTH_KEY = os.getenv("AUTH_KEY")
REDIS_HOST = os.getenv("REDIS_HOST")  # Redis runs as a separate container
REDIS_PORT = int(os.getenv("REDIS_PORT"))

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

# File Paths
UPLOAD_FOLDER = Path(os.getenv("UPLOAD_FOLDER"))
TRANSCRIPTS_SCRIPT_FOLDER = Path(os.getenv("TRANSCRIPTS_FOLDER"))

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TRANSCRIPTS_SCRIPT_FOLDER, exist_ok=True)

def file_exists(file_path):
    return os.path.exists(file_path)

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
    6. If not in any queue, download and add to either whisper_high/whisper_low.
    """
    try:
        data = request.json
        api_key = data.get("api_key")
        full_url = data.get("url")
        priority = data.get("priority", "low").lower()
        retry_all = data.get("retry_all", False)


        if api_key != AUTH_KEY:
            return jsonify({"status": "unauthorized", "error": "Invalid API key"}), 401
        
        if not full_url:
            return jsonify({"status": "bad_request", "error": "No URL provided"}), 400
        
        match = re.search(r"podcast/([a-fA-F0-9-]+)/audio.ogg", full_url)
        if not match:
            return jsonify({"status": "bad_request", "error": "Invalid URL format"}), 400

        file_id = match.group(1)

        print(f"[Request] Received meeting_id: {file_id}")
        
        # File Paths
        summary_file_path = os.path.join(TRANSCRIPTS_SCRIPT_FOLDER, f"{file_id}_summary.txt")
        txt_file_path = os.path.join(TRANSCRIPTS_SCRIPT_FOLDER, f"{file_id}.txt")

        if not retry_all:
        # Check if already processed
            if file_exists(txt_file_path) and file_exists(summary_file_path):
                webhook_data = {
                    "file_id": file_id,
                    "script":  str(Path(os.getenv("BBB_URL")) / f"{file_id}.txt"),
                    "status": "done"
                }
                async_send_webhook(webhook_data)
                return jsonify({"status": "done", "message": "File already processed"}), 200

        # Check if file is locked => means it is currently being split or transcribed
        lock_key = f"lock:{file_id}"
        if redis_client.exists(lock_key) and not retry_all:
            return jsonify({"status": "in_process", "message": "File is currently processing"}), 409
        
        # Manage retry
        if retry_all:
            redis_client.set(f"force_process:{file_id}", "1", ex=3600)

        file_path = os.path.join(UPLOAD_FOLDER, f"{file_id}.ogg")
        if not os.path.exists(file_path) or retry_all:
            if not download_file(full_url, file_path):
                return jsonify({"status": "error", "error": "Failed to download audio file"}), 500

        # Check if file is already queued for processing
        all_queues = ["whisper_high", "whisper_low"]
        for q in all_queues:
            queue_items = redis_client.lrange(q, 0, -1)
            if file_id in queue_items:
                # If the file is already in a queue:
                if priority == "high" and q == "whisper_low":
                    redis_client.lrem("whisper_low", 0, file_id)
                    redis_client.lpush("whisper_high", file_id)
                    return jsonify({"status": "requeued",
                                    "message": "File queue changed from whisper_low to whisper_high"}), 200
                else:
                    return jsonify({"status": "in_process",
                                    "message": f"File already queued in {q}"}), 200

        # Push to the correct whisper queue based on priority
        if priority == "high":
            redis_client.lpush("whisper_high", file_id)
            return jsonify({"status": "processing", "message": "File queued in whisper_high"}), 200
        else:
            redis_client.lpush("whisper_low", file_id)
            return jsonify({"status": "processing", "message": "File queued in whisper_low"}), 200
    
    
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
