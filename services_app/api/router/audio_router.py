import asyncio
from fastapi import APIRouter, HTTPException
from pathlib import Path
from core import audio_pro, audio_tag
import requests
from config.settings import get_settings
import logging
import os
logger = logging.getLogger(__name__)

CHORD_ENDPOINT = "/extract-20s-path"

app = APIRouter(
    prefix='/audio',
    tags=['Audio']
)

@app.post('/download')
async def download_audio(url: str, output_path: str):
    try:
        result = await audio_pro.download_audio(url=url, output_path=output_path)
        if not result:
            raise HTTPException(status_code=400, detail="Download failed")

        return {
            "success": True,
            "data": {"message": "Audio downloaded", "path": output_path},
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/process')
async def process_audio(input_path: str, output_path: str):
    try:
        result = await audio_pro.process_audio(input_path=input_path, output_path=output_path)
        if not result:
            raise HTTPException(status_code=400, detail="Audio processing failed")
        return {
            "success": True,
            "data": {"message": "Audio processed", "path": output_path},
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/tagging')
async def audio_tagging(input_path: str):
    try:
        path = Path(input_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="Input file not found")

        # Read file bytes asynchronously to prevent blocking
        audio_bytes = await asyncio.to_thread(path.read_bytes)
        tags = await audio_tag.chat(audio_bytes=audio_bytes)
        return {
            "success": True,
            "data": tags,
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/delete')
async def delete_audio(input_path: str):
    try:
        path = Path(input_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="File not found")

        # Delete file asynchronously to prevent blocking
        await asyncio.to_thread(path.unlink)
        return {
            "success": True,
            "data": {"message": "File deleted"},
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post('/analyze-metrics')
async def analyze_audio(input_path: str):
    try:
        info = audio_pro.analyze_file(input_path)
        return {
            "success": True,
            "data": {
                "bpm": info["bpm"],
                "tone": info["tone"],
                "scale": info["scale"]
            },
            "error": None
        }
    except HTTPException:
        raise
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Input file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Audio metrics analysis failed: {str(e)}")
    
import requests
from config.settings import get_settings

@app.post('/chords-progression')
async def chords_progression(input_path: str):
    try:
        # Chuyển đổi sang đường dẫn tuyệt đối
        abs_path = os.path.abspath(input_path)
        logger.info(f"Đường dẫn tuyệt đối: {abs_path}")
        
        path = Path(abs_path)
        if not path.exists():
            logger.error(f"File không tồn tại: {abs_path}")
            return {"success": True, "data": {"intro_chords": [], "outro_chords": []}}
            
        chord_service_url = get_settings().CHORD_SERVICE_URL
        url = f"{chord_service_url.rstrip('/')}{CHORD_ENDPOINT}"
        
        logger.info(f"Gọi chord service URL: {url} với file: {abs_path}")
        
        # Gửi cả đường dẫn tuyệt đối và tương đối để phòng hờ
        resp = requests.post(
            url, 
            json={"input_path": abs_path, "relative_path": input_path}, 
            timeout=60
        )
        
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.error(f"Chord service lỗi: {resp.status_code} - {resp.text}")
            return {"success": True, "data": {"intro_chords": [], "outro_chords": []}}
            
    except Exception as e:
        logger.error(f"Lỗi chord progression: {str(e)}")
        return {"success": True, "data": {"intro_chords": [], "outro_chords": []}}