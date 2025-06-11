from flask import Flask, request, jsonify
import os
import re
import redis
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv() 


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
