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


def _select_device(device: str) -> str:
    """Return a Torch device, auto-selecting the GPU with most free memory."""
    if device != "cuda" or not cuda_available():
        return device
    try:
        import torch

        gpu_count = torch.cuda.device_count()
        if gpu_count <= 1:
            return device

        free_memories = []
        for idx in range(gpu_count):
            free_mem, _ = torch.cuda.mem_get_info(idx)
            free_memories.append((free_mem, idx))

        best_free, best_idx = max(free_memories)
        log(
            f"Auto-selected cuda:{best_idx} with {best_free / 1024 ** 2:.0f}MB free"
        )
        return f"cuda:{best_idx}"
    except Exception as exc:
        log(f"GPU auto-selection failed: {exc}")
        return device


_model = None


def get_model():
    """Load and return a singleton Whisper model."""
    global _model
    if _model is not None:
        return _model

    device = _select_device(os.getenv("WHISPER_DEVICE", "cuda"))
    model_size = os.getenv("WHISPER_MODEL", "turbo")

    if device.startswith("cuda") and not cuda_available():
        log("CUDA device requested but not available – exiting")
        raise RuntimeError("cuda_unavailable")

    log(f"Loading Whisper model {model_size} on {device}")
    _model = whisper.load_model(
        model_size,
        device=device,
        download_root="/root/.cache/whisper",
    )
    return _model
