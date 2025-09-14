# Use NVIDIA CUDA base image
FROM nvidia/cuda:12.8.1-cudnn-runtime-ubuntu22.04

# Set environment variables
# Basics
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    # Ensure CUDA extensions you build target Blackwell (sm_100) and include PTX
    TORCH_CUDA_ARCH_LIST="10.0+PTX" \
    NVIDIA_VISIBLE_DEVICES=all \
    NVIDIA_DRIVER_CAPABILITIES=compute,utility,video

WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-dev python3-venv \
    ffmpeg git curl bc sudo \
 && rm -rf /var/lib/apt/lists/*

# Standardize python/pip names
RUN ln -sf /usr/bin/python3 /usr/bin/python && ln -sf /usr/bin/pip3 /usr/bin/pip

# Upgrade pip toolchain
RUN python -m pip install --upgrade pip setuptools wheel




# ---- PyTorch built for CUDA 12.8 (Blackwell-ready) ----
# Note: use the cu128 index so wheels match the 12.8 runtime.
RUN pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu128



# Whisper implementations + utilities
# (Quote openai requirement to avoid shell redirection)
RUN pip install --no-cache-dir \
    "git+https://github.com/openai/whisper.git" \
    faster-whisper \
    "openai>=1.14" \
    flask flask-cors requests srt pika pysrt tiktoken python-dotenv

# Ensure necessary directories exist inside the container
RUN mkdir -p /app/queue /app/scripts /logs

CMD ["python3", "/app/whisper_worker.py"]

