# BiggerBlueButton Whisper

This repository provides Docker configuration and scripts for running a Whisper transcription service with optional GPU acceleration.

## Requirements
- Docker and Docker Compose
- (Optional) NVIDIA GPU drivers and the Docker NVIDIA runtime for GPU acceleration

## Building and Running
1. Copy `.env.example` to `.env` and adjust any environment variables as needed.
2. Build and start the containers:
   ```bash
   docker-compose up --build
   ```
   Whisper will be available on port `5000`.

## Logs
Application and watchdog logs are written to `./supervisor-logs` inside the repository.
- `app.log` – Flask application output
- `split.log` – audio splitting queue
- `whisper.log` – transcription queue
- `gpu_watchdog.log` – GPU availability monitoring

## GPU Watchdog
The `gpu_watchdog.sh` script checks for GPU availability. If the GPU is unavailable for a configured number of retries, the script will restart the container using Docker to recover automatically.


