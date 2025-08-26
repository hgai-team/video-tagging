import asyncio
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Optional
from pathlib import Path

from core import video_pro, video_tag, video_detect

app = APIRouter(
    prefix='/video',
    tags=['Video']
)

class ResponseModel(BaseModel):
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None

@app.post('/download', response_model=ResponseModel)
async def download_video(url: str, output_path: str):
    try:
        result = await video_pro.download_video(url=url, output_path=output_path)
        if not result:
            raise HTTPException(status_code=400, detail="Download failed")

        return {
            "success": True,
            "data": {"message": "Video downloaded", "path": output_path},
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/shrink', response_model=ResponseModel)
async def shrink_video(input_path: str, output_path: str):
    try:
        result = await video_pro.shrink_video(input_path=input_path, output_path=output_path)
        if not result:
            raise HTTPException(status_code=400, detail="Shrink operation failed")
        return {
            "success": True,
            "data": {"message": "Video shrunk", "path": output_path},
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/tagging', response_model=ResponseModel)
async def video_tagging(input_path: str):
    try:
        path = Path(input_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="Input file not found")

        # Read file bytes asynchronously to prevent blocking
        video_bytes = await asyncio.to_thread(path.read_bytes)
        tags = await video_tag.chat(video_bytes=video_bytes)
        return {
            "success": True,
            "data": tags,
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post('/delete', response_model=ResponseModel)
async def delete_video(input_path: str):
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
    
@app.post('/detect-real', response_model=ResponseModel)
async def detect_real_or_ai(input_path: str):
    try:
        path = Path(input_path)
        if not path.exists():
            raise HTTPException(status_code=404, detail="File not found")

        # Read file bytes asynchronously to prevent blocking
        video_bytes = await asyncio.to_thread(path.read_bytes)
        detection_result = await video_detect.detect(video_bytes=video_bytes)
        
        return {
            "success": True,
            "data": detection_result,
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
