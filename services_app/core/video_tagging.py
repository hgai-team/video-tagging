import os
import asyncio
import logging

from google import genai
from google.genai import types

from settings import get_settings

from .prompts import VIDEO_TAGGING_PROMPT
from .parser import json_parser

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("./logs/video_tagging.log"), logging.StreamHandler()]
)

class VideoTagging:
    def __init__(self):
        self.client = genai.Client(
            api_key=get_settings().GOOGLEAI_API_KEY
        )

    async def chat(self, video_bytes):
        
        response = await asyncio.to_thread(
            self.client.models.generate_content,
            model=get_settings().GOOGLEAI_MODEL,
            contents=types.Content(
                parts=[
                    types.Part(
                        inline_data=types.Blob(
                            data=video_bytes,
                            mime_type="video/mp4",
                        )
                    ),
                    types.Part(
                        text=VIDEO_TAGGING_PROMPT
                    )
                ]
            )
        )
        if not response.candidates:
            raise RuntimeError("No candidates in response")
        part = response.candidates[0].content.parts
        if not part:
            raise RuntimeError("No content parts")
        text = part[0].text

        return json_parser(text)