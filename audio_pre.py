# audio_pre.py
import os
import subprocess
import torch

from pyrnnoise import RNNoise  # <-- RNNoise binding

# ---- Silero VAD setup ----
model, utils = torch.hub.load(
    repo_or_dir='snakers4/silero-vad',
    model='silero_vad',
    force_reload=False,
    trust_repo=True,
)
(get_speech_ts, save_audio, read_audio, VADIterator, collect_chunks) = utils

SAMPLE_RATE = 16000
MIN_SPEECH_SEC = float(os.getenv("VAD_MIN_SPEECH_SEC", "0.6"))

# ---- RNNoise setup ----
# RNNoise natively works at 48kHz, pyrnnoise will resample as needed.
_rnnoise = RNNoise(sample_rate=48000)


def denoise_with_rnnoise(in_path: str) -> str:
    """
    Run RNNoise (pyrnnoise) on a wav chunk.
    We process into a temp file then overwrite the original.
    """
    base, ext = os.path.splitext(in_path)
    out_path = base + "_dn" + ext

    try:
        # denoise_wav processes the file and writes out_path
        # it yields speech_prob per frame; we don't need them here.
        for _ in _rnnoise.denoise_wav(in_path, out_path):
            pass

        os.replace(out_path, in_path)
        return in_path
    except Exception as e:
        print(f"[audio_pre] RNNoise failed on {in_path}: {e}")
        if os.path.exists(out_path):
            try:
                os.remove(out_path)
            except Exception:
                pass
        return in_path


def has_enough_speech(in_path: str) -> bool:
    """
    Use Silero VAD to check if this chunk contains enough speech.
    """
    try:
        wav = read_audio(in_path, sampling_rate=SAMPLE_RATE)
        speech_ts = get_speech_ts(wav, model, sampling_rate=SAMPLE_RATE)
    except Exception as e:
        print(f"[audio_pre] VAD failed on {in_path}: {e}")
        return True  # fail-open

    if not speech_ts:
        return False

    total_speech_samples = sum(t['end'] - t['start'] for t in speech_ts)
    total_speech_sec = total_speech_samples / SAMPLE_RATE
    return total_speech_sec >= MIN_SPEECH_SEC


def preprocess_chunk(in_path: str) -> bool:
    """
    Full pipeline for each chunk:
      1) RNNoise denoise (file-based, no PulseAudio)
      2) Silero VAD gate

    Returns True = keep chunk → Whisper
            False = drop chunk (noise-only)
    """
    # 1) Denoise in place
    denoise_with_rnnoise(in_path)

    # 2) VAD filtering
    return has_enough_speech(in_path)
