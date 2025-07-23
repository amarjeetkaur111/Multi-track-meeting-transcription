import os
from logger import log

try:
    import whisper
except ImportError as e:
    log("openai-whisper package not available")
    raise

def cuda_available() -> bool:
    try:
        import torch
        return torch.cuda.is_available()
    except Exception as exc:
        log(f"CUDA check failed: {exc}")
        return False


_model = None

def get_model():
    """Load and return a singleton Whisper model."""
    global _model
    if _model is not None:
        return _model

    device = os.getenv("WHISPER_DEVICE", "cuda")
    model_size = os.getenv("WHISPER_MODEL", "turbo")

    if device == "cuda" and not cuda_available():
        log("CUDA device requested but not available – exiting")
        raise RuntimeError("cuda_unavailable")

    log(f"Loading Whisper model {model_size} on {device}")
    _model = whisper.load_model(
        model_size,
        device=device,
        download_root="/root/.cache/whisper",
    )
    return _model
