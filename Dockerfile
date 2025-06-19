# Use NVIDIA CUDA base image
FROM nvidia/cuda:12.6.3-cudnn-runtime-ubuntu22.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /app

# Install Python, dependencies, and system libraries
RUN apt-get update && apt-get install -y \
    sudo \
    python3.9 \
    python3-distutils \
    python3-pip \
    python3-dev \
    ffmpeg \
    curl \
    bc \
    git \
    supervisor \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Set Python as default
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install Whisper dependencies (Torch with CUDA)
RUN pip install --no-cache-dir torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

# Install OpenAI Whisper from GitHub (ensures latest updates)
RUN pip install --no-cache-dir git+https://github.com/openai/whisper.git

# Install additional dependencies
RUN pip install -U flask flask-cors requests srt pika

# Optional: Install Faster-Whisper for better performance
RUN pip install --no-cache-dir openai>=1.14 faster-whisper

# Install OpenAI and other dependencies
RUN pip install --upgrade pysrt tiktoken python-dotenv

# Ensure necessary directories exist inside the container
RUN mkdir -p /app/queue /app/scripts

COPY ./supervisord.conf /etc/supervisor/conf.d/supervisord.conf
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf", "-n"]

