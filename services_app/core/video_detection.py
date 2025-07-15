import asyncio
import logging

from google import genai
from google.genai import types

from config.settings import get_settings
from utils.prompts import VIDEO_DETECTION_PROMPT
from utils.parser import json_parser

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("./logs/video_detection.log"), logging.StreamHandler()]
)

class VideoDetection:
    def __init__(self):
        self.client = genai.Client(
            api_key=get_settings().GOOGLEAI_API_KEY
        )

    async def detect(self, video_bytes):
        try:
            response = await asyncio.to_thread(
                self.client.models.generate_content,
                model=get_settings().GOOGLEAI_DETECTION_MODEL,
                contents=types.Content(
                    parts=[
                        types.Part(
                            inline_data=types.Blob(
                                data=video_bytes,
                                mime_type="video/mp4",
                            )
                        ),
                        types.Part(
                            text=VIDEO_DETECTION_PROMPT
                        )
                    ]
                )
            )
            
            if not response.candidates:
                raise RuntimeError("Không có candidates trong response")
                
            part = response.candidates[0].content.parts
            if not part:
                raise RuntimeError("Không có content parts")
                
            text = part[0].text
            
            # Parse kết quả JSON
            result = json_parser(text)
            
            # Xử lý kết quả theo yêu cầu
            # {"is_real_life": true, "has_ai_elements": false} => is_real = 1
            # Các trường hợp khác => is_real = 0
            is_real_life = result.get("is_real_life")
            has_ai_elements = result.get("has_ai_elements")
            
            # Kiểm tra is_real_life là True/true và has_ai_elements là False/false
            is_real_life_true = is_real_life is True or is_real_life == "true" or is_real_life == "True"
            has_ai_elements_false = has_ai_elements is False or has_ai_elements == "false" or has_ai_elements == "False"
            
            # Nếu điều kiện thỏa mãn thì is_real = 1, còn lại is_real = 0
            is_real = 1 if is_real_life_true and has_ai_elements_false else 0
            
            return {
                "is_real": is_real,
                "raw_result": result  
            }
            
        except Exception as e:
            logger.error(f"Lỗi trong quá trình phát hiện video: {str(e)}", exc_info=True)
            raise RuntimeError(f"Phát hiện video thất bại: {str(e)}")
