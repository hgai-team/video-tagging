from fastapi import APIRouter, HTTPException
from pathlib import Path
from core import audio_pro, audio_tag

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

        audio_bytes = path.read_bytes()
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

        path.unlink()
        return {
            "success": True,
            "data": {"message": "File deleted"},
            "error": None
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))