# ---------- Constants Over Magic Numbers ----------
INTRO_SEC = 20
OUTRO_SEC = 20
WAV_FMT = "wav"
TARGET_SR = 22050
TARGET_CH = 1

# ---------- Imports ----------
# Smart comments (why): keep the full logic here so the main app only calls this service
import asyncio
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pydub import AudioSegment
from tempfile import NamedTemporaryFile
from chord_extractor.extractors import Chordino
from pathlib import Path

app = FastAPI(title="Chord Service (py39)")

# ---------- Schemas ----------
class ExtractRequest(BaseModel):
    input_path: str  # absolute or service-local path

# ---------- Helpers ----------
async def _cut_wav_segments(input_path: str):
    # why: normalize to WAV mono 22.05kHz to avoid codec/plugin pitfalls
    loop = asyncio.get_event_loop()
    
    # Run heavy audio processing in executor
    def _process_audio():
        audio = AudioSegment.from_file(input_path)
        dur = len(audio)
        intro_ms = INTRO_SEC * 1000
        outro_ms = OUTRO_SEC * 1000

        intro_seg = audio[:min(intro_ms, dur)].set_frame_rate(TARGET_SR).set_channels(TARGET_CH)
        start_outro = max(0, dur - outro_ms)
        outro_seg = audio[start_outro:].set_frame_rate(TARGET_SR).set_channels(TARGET_CH)

        intro_tmp = NamedTemporaryFile(suffix=f".{WAV_FMT}", delete=False)
        outro_tmp = NamedTemporaryFile(suffix=f".{WAV_FMT}", delete=False)
        intro_seg.export(intro_tmp.name, format=WAV_FMT)
        outro_seg.export(outro_tmp.name, format=WAV_FMT)
        return intro_tmp.name, outro_tmp.name
    
    return await loop.run_in_executor(None, _process_audio)

async def _extract_chords(path: str):
    # why: chord-extractor returns ChordChange list; keep names only
    loop = asyncio.get_event_loop()
    
    def _process_chords():
        c = Chordino()
        changes = c.extract(path) or []
        return [ch.chord for ch in changes if getattr(ch, "chord", None)]
    
    return await loop.run_in_executor(None, _process_chords)

async def _cleanup_temp_files(*file_paths):
    # why: async cleanup of temporary files
    loop = asyncio.get_event_loop()
    
    def _remove_files():
        for file_path in file_paths:
            try:
                if file_path and os.path.exists(file_path):
                    os.unlink(file_path)
            except OSError:
                pass  # Ignore cleanup errors
    
    await loop.run_in_executor(None, _remove_files)

# ---------- Endpoint ----------
@app.post("/extract-20s-path")
async def extract_20s_path(req: ExtractRequest):
    p = Path(req.input_path)
    if not p.exists():
        raise HTTPException(status_code=404, detail="Input file not found on chord service host")
    
    intro_path = None
    outro_path = None
    
    try:
        # Cut audio segments (async)
        intro_path, outro_path = await _cut_wav_segments(str(p))
        
        # Extract chords from both segments concurrently
        intro_chords, outro_chords = await asyncio.gather(
            _extract_chords(intro_path),
            _extract_chords(outro_path)
        )
        
        return {
            "success": True,
            "data": {
                "intro_chords": intro_chords,
                "outro_chords": outro_chords
            },
            "error": None
        }
        
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "error": str(e)
        }
        
    finally:
        # Always cleanup temp files
        if intro_path or outro_path:
            await _cleanup_temp_files(intro_path, outro_path)
