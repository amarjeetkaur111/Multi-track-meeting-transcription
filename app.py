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

@app.route("/", methods=["GET"])
def index():
    return "Hello, this is the root route!", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
