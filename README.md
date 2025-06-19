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
   The worker listens for jobs on the `whisper-tasks` RabbitMQ queue and no longer exposes an HTTP API.
    It publishes file processing status updates on the `whisper-result` queue.
    Configure the connection using these environment variables:
    `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`,
    `RABBITMQ_VHOST`, `RABBITMQ_QUEUE` and `RESULT_QUEUE`.
    Each job message must include a `file_id` field and an audio `url`.

## Logs
Application logs are written to `./supervisor-logs`.
- `whisper_open.log` / `whisper_azure.log` – worker output
- `gpu_watchdog.log` – GPU availability monitoring

## GPU Watchdog
The `gpu_watchdog.sh` script checks for GPU availability. If the GPU is unavailable for a configured number of retries, the script will restart the container using Docker to recover automatically.


