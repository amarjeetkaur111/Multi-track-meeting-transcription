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
   The worker listens for jobs on Redis Streams and no longer exposes an HTTP API.

## Logs
Application logs are written to `./supervisor-logs`.
- `whisper_open.log` / `whisper_azure.log` – worker output
- `gpu_watchdog.log` – GPU availability monitoring

## GPU Watchdog
The `gpu_watchdog.sh` script checks for GPU availability. If the GPU is unavailable for a configured number of retries, the script will restart the container using Docker to recover automatically.


