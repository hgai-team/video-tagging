import asyncio
import aiohttp
import tempfile
import os
import subprocess
import logging
from typing import Optional, Dict, Any
from time import perf_counter
import random
from pathlib import Path

from google import genai
from google.genai import types
from models import OutputSchema, ProcessingResult
from config import GEMINI_API_KEY, PROMPT_TEXT

logger = logging.getLogger(__name__)

class VideoProcessor:
    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=120)  # 2 minute timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def download_video(self, url: str, output_path: str) -> bool:
        """Download video from URL to local path"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    with open(output_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                    return True
                else:
                    logger.error(f"Failed to download video: HTTP {response.status}")
                    return False
        except Exception as e:
            logger.error(f"Download error: {e}")
            return False
    
    def shrink_video(self, input_path: str, output_path: str, 
                    width: int = 800, bitrate: str = '1000k', target_frames: int = 59) -> bool:
        """Process video to 59 frames (adapted from original code)"""
        try:
            # Get video duration
            result = subprocess.run([
                'ffprobe', '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                input_path
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            
            if result.returncode != 0:
                logger.error(f"ffprobe failed: {result.stderr}")
                return False
                
            duration = float(result.stdout.strip())
            temp_dir = tempfile.mkdtemp()
            
            try:
                if duration <= target_frames:
                    # Short video: 1 frame per second
                    cmd = [
                        'ffmpeg', '-y',
                        '-i', input_path,
                        '-vf', f'fps=1,scale={width}:-2',
                        '-an',
                        f'{temp_dir}/frame_%03d.png'
                    ]
                    subprocess.run(cmd, check=True)
                else:
                    # Long video: random frame from each segment
                    segment_duration = duration / target_frames
                    
                    for i in range(target_frames):
                        random_offset = random.uniform(0, segment_duration - 0.1)
                        timestamp = i * segment_duration + random_offset
                        
                        cmd = [
                            'ffmpeg', '-y',
                            '-ss', f'{timestamp:.6f}',
                            '-i', input_path,
                            '-frames:v', '1',
                            '-vf', f'scale={width}:-2',
                            f'{temp_dir}/frame_{i+1:03d}.png'
                        ]
                        subprocess.run(cmd, check=True)
                
                # Create concat file
                concat_file = os.path.join(temp_dir, 'frames.txt')
                with open(concat_file, 'w') as f:
                    for i in range(1, target_frames + 1):
                        frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')
                        if os.path.exists(frame_path):
                            f.write(f"file '{frame_path}'\n")
                            f.write("duration 1\n")
                
                # Create final video
                cmd_final = [
                    'ffmpeg', '-y',
                    '-f', 'concat',
                    '-safe', '0',
                    '-i', concat_file,
                    '-c:v', 'libx264',
                    '-b:v', bitrate,
                    '-preset', 'slow',
                    '-pix_fmt', 'yuv420p',
                    output_path
                ]
                subprocess.run(cmd_final, check=True)
                
                return True
                
            finally:
                # Cleanup temp directory
                for file in os.listdir(temp_dir):
                    os.remove(os.path.join(temp_dir, file))
                os.rmdir(temp_dir)
                
        except Exception as e:
            logger.error(f"Video processing error: {e}")
            return False
    
    async def analyze_video(self, video_path: str) -> Optional[Dict[str, Any]]:
        """Analyze video with Gemini AI (adapted from original code)"""
        try:
            with open(video_path, "rb") as f:
                video_bytes = f.read()
            
            start = perf_counter()
            resp = self.client.models.generate_content(
                model="gemini-2.0-flash",
                contents=types.Content(parts=[
                    types.Part(inline_data=types.Blob(data=video_bytes, mime_type="video/mp4")),
                    types.Part(text=PROMPT_TEXT)
                ]),
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=OutputSchema
                )
            )
            duration = perf_counter() - start
            
            usage = resp.usage_metadata
            token_usage = {
                "total_tokens": getattr(usage, "total_token_count", None),
                "prompt_tokens": getattr(usage, "prompt_token_count", None),
                "output_tokens": getattr(usage, "candidates_token_count", None)
            }
            
            return {
                "tags": OutputSchema.model_validate_json(resp.text),
                "processing_time": duration,
                "token_usage": token_usage
            }
            
        except Exception as e:
            logger.error(f"AI analysis error: {e}")
            return None
    
    async def process_video_with_retry(self, file_id: str, video_url: str, max_retries: int = 3) -> Optional[ProcessingResult]:
        """Process video with retry logic"""
        last_error = None
        
        for attempt in range(max_retries):
            try:
                with tempfile.TemporaryDirectory() as temp_dir:
                    original_path = os.path.join(temp_dir, f"{file_id}_original.mp4")
                    processed_path = os.path.join(temp_dir, f"{file_id}_processed.mp4")
                    
                    # Download video
                    if not await self.download_video(video_url, original_path):
                        raise Exception("Failed to download video")
                    
                    # Process video
                    if not self.shrink_video(original_path, processed_path):
                        raise Exception("Failed to process video")
                    
                    # Analyze with AI
                    analysis_result = await self.analyze_video(processed_path)
                    if not analysis_result:
                        raise Exception("Failed to analyze video")
                    
                    return ProcessingResult(
                        file_id=file_id,
                        tags=analysis_result["tags"],
                        processing_time=analysis_result["processing_time"],
                        token_usage=analysis_result["token_usage"]
                    )
                    
            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt + 1} failed for {file_id}: {e}")
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(10)  # 10 second delay
        
        logger.error(f"All {max_retries} attempts failed for {file_id}: {last_error}")
        return None