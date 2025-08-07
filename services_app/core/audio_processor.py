import os
import requests
import logging
import asyncio
import random
from pathlib import Path
from pydub import AudioSegment

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("./logs/audio_processor.log"), logging.StreamHandler()]
)

class AudioProcessor:
    def __init__(self, timeout: int = 3600, subprocess_timeout: int = 3600):
        self.timeout = timeout
        self.subprocess_timeout = subprocess_timeout

    async def download_audio(self, url: str, output_path: str) -> bool:
        """Download audio from URL to local path (sync, blocking)."""
        try:
            response = requests.get(url, timeout=self.timeout, stream=True, verify=False)
            
            if response.status_code != 200:
                logger.error(f"Download failed: HTTP {response.status_code}")
                raise RuntimeError(f"Download failed: HTTP {response.status_code}")
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  
                        f.write(chunk)
            return True
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error during download: {str(e)}")
            if os.path.exists(output_path):
                os.remove(output_path)
            raise RuntimeError(f"Download error: {str(e)}")    
            
    async def process_audio(self, input_path: str, output_path: str) -> bool:
        """
        Process audio file:
        - Convert to 22050 Hz mono WAV
        - Sample segments theo thuật toán định trước
        
        Returns True if successful, False otherwise.
        """
        try:
            if not Path(input_path).exists():
                logger.error(f"Input file not found: {input_path}")
                return False
            
            # Sử dụng asyncio để chạy xử lý audio không đồng bộ
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: self._shrink_audio(input_path, output_path))
            
            return result
            
        except Exception as e:
            logger.error(f"Audio processing error: {str(e)}")
            return False

    def _shrink_audio(self, input_path: str, output_path: str) -> bool:
        """
        Shrink audio using pydub with specified algorithm:
        - Convert to 22050 Hz mono
        - Sample 7-second segments from 25-second windows
        - Concatenate segments
        """
        try:
            # 1. Load and downsample về 22050 Hz
            audio = AudioSegment.from_file(input_path)
            audio = audio.set_frame_rate(22050).set_channels(1)
            
            duration_ms = len(audio)
            segment_ms = 25 * 1000   # 25s = 25000 ms
            sample_ms = 7 * 1000     # 7s = 7000 ms
            
            # Lấy mẫu các đoạn audio
            sampled_segments = []
            for start_ms in range(0, duration_ms, segment_ms):
                end_ms = min(start_ms + segment_ms, duration_ms)
                window_duration = end_ms - start_ms
                
                # Bỏ qua đoạn cuối nếu không đủ dài
                if window_duration < sample_ms:
                    continue
                    
                # Chọn vị trí ngẫu nhiên trong cửa sổ để cắt đoạn 7 giây
                offset = random.randint(0, window_duration - sample_ms)
                seg = audio[start_ms + offset : start_ms + offset + sample_ms]
                sampled_segments.append(seg)
            
            # Kiểm tra xem có đủ đoạn để ghép không
            if not sampled_segments:
                logger.error("Không có đủ dữ liệu để sample")
                raise ValueError("Không có đủ dữ liệu để sample")
            
            # 2. Ghép các đoạn lại
            output = sampled_segments[0]
            for seg in sampled_segments[1:]:
                output += seg
            
            # 3. Xuất file WAV
            output.export(output_path, format='wav')
            
            logger.info(f"Xuất thành công: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error shrinking audio: {str(e)}")
            return False