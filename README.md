# BiggerBlueButton Whisper

This repository provides Docker configuration and scripts for running a Whisper transcription service with optional GPU acceleration. The local backend now leverages **faster-whisper** for significantly improved performance when processing audio on the GPU.

## Requirements
- Docker and Docker Compose
- (Optional) NVIDIA GPU drivers and the Docker NVIDIA runtime for GPU acceleration

## Building and Running
1. Copy `.env.example` to `.env` and adjust any environment variables as needed.
2. Build and start the containers:
   ```bash
   docker-compose up --build
   ```
   The worker listens for jobs on the `whisper-tasks` RabbitMQ queue and no longer exposes an HTTP API.
    It publishes file processing status updates on the `whisper-result` queue.
    Configure the connection using these environment variables:
    `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`,
   `RABBITMQ_VHOST`, `RABBITMQ_QUEUE`, `RESULT_QUEUE` and `RABBITMQ_HEARTBEAT`.
   The default heartbeat is disabled (0) to avoid disconnects during long jobs.
    Each job message must include a `file_id` field and an audio `url`.
    The `WHISPER_MODEL` variable controls which Faster-Whisper model is loaded
    for local transcription (defaults to `large-v3`).
    Use `WHISPER_BATCH_SIZE` to set how many audio frames the model processes at
    once (defaults to `16`). When supported by the installed Faster-Whisper
    version, this helps keep the GPU busy during transcription. The worker will
    also set `num_workers` to the available CPU cores if the API supports it.
    `WHISPER_CONCURRENT_CHUNKS` controls how many audio chunks are transcribed in
    parallel (defaults to `2`) so the GPU remains active while multiple threads
    feed data to the model.

## Logs
- Application logs are written to `./supervisor-logs`.
- `whisper.log` – worker output
- `gpu_watchdog.log` – GPU availability monitoring

## GPU Watchdog
The `gpu_watchdog.sh` script checks for GPU availability. If the GPU is unavailable for a configured number of retries, the script will restart the container using Docker to recover automatically.


