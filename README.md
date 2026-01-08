# Whisper

This repository provides Docker configuration and scripts for running a Whisper transcription service with optional GPU acceleration. The worker transcribes audio locally using the open source **Whisper** model from OpenAI.

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
  The `WHISPER_MODEL` variable selects which Whisper model to load (defaults to `large-v3`).
    `WHISPER_BATCH_SIZE` sets the `batch_size` used during transcription (defaults to `16`).
    `WHISPER_DEVICE` chooses the Torch device (defaults to automatically using the
    CUDA GPU with the most free memory) and `WHISPER_COMPUTE_TYPE` controls
    whether the model is converted to `float16` when supported (defaults to
    `float16`).

## Logs
- Application logs are written to `./logs`.
- `whisper.log` – worker output (rotated at 10MB, up to 10 files)
- `split.log` – audio splitting details (rotated at 10MB, up to 10 files)


