# Multi-track meeting transcription (Whisper + RabbitMQ)

An internal service that takes separate mic recordings (`webm` per microphone) and produces a single **merged, time-synced transcript** as **SRT** (subtitles) and **TXT**.

**Role:** Tech lead 
**Stack:** Python, Whisper (open-source turbo), RabbitMQ, Docker, ffmpeg, [storage], [logging/metrics]
**Infra:** 64-core CPU + 2×32GB GPUs + 6container for continous transcription of live meetings (GPU-first inference, CPU for decode + orchestration)
---

## Why
Meetings were recorded as multiple mic tracks. If you transcribe them independently, the output looks “right” per track but the final transcript is messy: speakers are out of order, timestamps don’t line up, and interruptions/crosstalk get weird.

We needed a pipeline that could process tracks in parallel **and** reliably merge them into one timeline and have subtitles for recorded meetings.

---

## How it works (high level)
1. **Ingest**: receive `meeting_id` + N `webm` tracks  
2. **Queue**: publish one job per track to RabbitMQ  
3. **Transcribe**: workers run Whisper and output timestamped segments  
4. **Sync + merge**: normalize timestamps (offset/drift), then merge segments into one ordered stream  
5. **Render**: write `meeting.srt` + `meeting.txt` (+ optional per-track transcripts)  
6. **Harden**: retries, DLQ for broken media, idempotent processing

---

## Contribution
- Built the **multi-track merge** logic to keep ordering correct even with overlaps/interruptions  
- Added **timestamp normalization** (offset/drift handling) so SRT stays in sync  
- Made workers **idempotent** and production-safe (retries + DLQ + backpressure)  
- Containerized and tuned concurrency for predictable throughput

---

- Built an internal **async transcription pipeline** (Python + Whisper + RabbitMQ) to convert per-mic `webm` recordings into merged, time-aligned **SRT/TXT**.
- Implemented **sync + merge** logic for multi-track audio, keeping speaker ordering correct during overlaps and interruptions.
- Scaled processing to 100+ hours of audio/day by tuning consumer concurrency/prefetch and Dockerized deployments.
