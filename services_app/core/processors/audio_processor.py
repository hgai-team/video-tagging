import asyncio
import random
import numpy as np
import librosa
from pathlib import Path
from pydub import AudioSegment
from ..constants.audio_constants import (
    PITCHES_SHARP, FLAT_TO_SHARP, KS_MAJOR, KS_MINOR,
    MIN_BPM, MAX_BPM, HOP_LENGTH, CENTER_TRIM
)

class AudioProcessor:
    """Handles audio format processing and music analysis."""
    
    def __init__(self):
        pass

    async def process_audio(self, input_path: str, output_path: str) -> bool:
        """Process audio file asynchronously."""
        try:
            if not Path(input_path).exists():
                return False
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: self._shrink_audio(input_path, output_path))
            
            return result
            
        except Exception:
            return False

    def _shrink_audio(self, input_path: str, output_path: str) -> bool:
        """
        Shrink audio using pydub with specified algorithm:
        - Convert to 22050 Hz mono
        - Sample 7-second segments from 25-second windows
        - Concatenate segments
        """
        try:
            # 1. Load and downsample to 22050 Hz
            audio = AudioSegment.from_file(input_path)
            audio = audio.set_frame_rate(22050).set_channels(1)
            
            duration_ms = len(audio)
            segment_ms = 25 * 1000   # 25s = 25000 ms
            sample_ms = 7 * 1000     # 7s = 7000 ms
            
            # Sample audio segments
            sampled_segments = []
            for start_ms in range(0, duration_ms, segment_ms):
                end_ms = min(start_ms + segment_ms, duration_ms)
                window_duration = end_ms - start_ms
                
                # Skip final segment if too short
                if window_duration < sample_ms:
                    continue
                    
                # Choose random position within window to cut 7-second segment
                offset = random.randint(0, window_duration - sample_ms)
                seg = audio[start_ms + offset : start_ms + offset + sample_ms]
                sampled_segments.append(seg)
            
            # Check if we have enough segments to concatenate
            if not sampled_segments:
                raise ValueError("Không có đủ dữ liệu để sample")
            
            # 2. Concatenate segments
            output = sampled_segments[0]
            for seg in sampled_segments[1:]:
                output += seg
            
            # 3. Export WAV file
            output.export(output_path, format='wav')
            return True
            
        except Exception:
            return False
        
    def _to_sharp(self, note: str) -> str:
        return FLAT_TO_SHARP.get(note, note)

    def _l2(self, v: np.ndarray) -> np.ndarray:
        return v / (np.linalg.norm(v) + 1e-9)

    def _estimate_bpm(self, y: np.ndarray, sr: int, *, hop_length: int = HOP_LENGTH) -> int:
        onset = librosa.onset.onset_strength(y=y, sr=sr, hop_length=hop_length)
        temps = librosa.beat.tempo(onset_envelope=onset, sr=sr, hop_length=hop_length, aggregate=None)
        if temps.size == 0:
            raise ValueError("Không tìm thấy nhịp")
        xs = []
        for t in temps:
            while t < MIN_BPM: t *= 2
            while t > MAX_BPM: t /= 2
            xs.append(float(t))
        return int(round(float(np.median(xs))))

    def _estimate_key_essentia(self, y: np.ndarray, sr: int, *, profile: str = "edma"):
        try:
            import essentia.standard as es
        except Exception as e:
            raise RuntimeError("Essentia chưa cài đặt") from e
        y32 = np.asarray(y, dtype=np.float32)
        ke = es.KeyExtractor(profileType=profile, hpcpSize=36, sampleRate=float(sr), averageDetuningCorrection=True)
        key, scale, strength = ke(y32)
        return self._to_sharp(key), scale.lower(), float(strength), "essentia"

    def _estimate_key_librosa(self, y: np.ndarray, sr: int, *, hop_length: int = HOP_LENGTH, center_trim: float = CENTER_TRIM):
        y_h, _ = librosa.effects.hpss(y)
        tune = librosa.estimate_tuning(y=y_h, sr=sr)
        chroma = librosa.feature.chroma_cens(y=y_h, sr=sr, hop_length=hop_length, tuning=tune)

        _, beats = librosa.beat.beat_track(y=y, sr=sr, hop_length=hop_length)
        if beats.size > 0:
            beats = librosa.util.fix_frames(beats, x_min=0, x_max=chroma.shape[1]-1)
            chroma = librosa.util.sync(chroma, beats, aggregate=np.median)

        if chroma.shape[1] >= 10 and 0.0 < center_trim < 0.5:
            T = chroma.shape[1]; a = int(T*center_trim); b = T - a
            if b - a >= 4:
                chroma = chroma[:, a:b]

        v = self._l2(chroma.mean(axis=1))
        pmaj = self._l2(KS_MAJOR); pmin = self._l2(KS_MINOR)
        scores = []
        for k in range(12):
            scores.append(("major", k, float(v @ np.roll(pmaj, k))))
            scores.append(("minor", k, float(v @ np.roll(pmin, k))))
        scores.sort(key=lambda x: x[2], reverse=True)
        mode, k, s1 = scores[0]; s2 = scores[1][2]
        return PITCHES_SHARP[k], mode, float(s1 - s2), "librosa"

    def _estimate_key(self, y: np.ndarray, sr: int, *, engine: str = "auto", profile: str = "edma"):
        if engine in ("auto","essentia"):
            try:
                return self._estimate_key_essentia(y, sr, profile=profile)
            except Exception:
                if engine == "essentia":
                    raise
        return self._estimate_key_librosa(y, sr)

    def analyze_file(self, input_path: str, *, engine: str = "auto", profile: str = "edma", hop_length: int = HOP_LENGTH):
        """Analyze audio file for BPM, key, and scale."""
        y, sr = librosa.load(str(input_path), mono=True)
        bpm = self._estimate_bpm(y, sr, hop_length=hop_length)
        pitch, scale, strength, used = self._estimate_key(y, sr, engine=engine, profile=profile)
        return {
            "bpm": int(bpm),
            "tone": pitch,
            "scale": scale,
            "key_strength": round(float(strength), 4),
            "engine": used,
        }