import asyncio
import logging

from google import genai
from google.genai import types

from config.settings import get_settings

from utils.prompts import AUDIO_TAGGING_PROMPT
from utils.parser import json_parser

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("./logs/audio_tagging.log"), logging.StreamHandler()]
)

class AudioTagging:
    def __init__(self):
        self.client = genai.Client(
            api_key=get_settings().GOOGLEAI_API_KEY
        )

    async def chat(self, audio_bytes):
        response = await asyncio.to_thread(
            self.client.models.generate_content,
            model=get_settings().GOOGLEAI_AUDIO,
            contents=types.Content(
                parts=[
                    types.Part(
                        inline_data=types.Blob(
                            data=audio_bytes,
                            mime_type="audio/wav",
                        )
                    ),
                    types.Part(
                        text=AUDIO_TAGGING_PROMPT
                    )
                ]
            )
        )
        
        if not response.candidates:
            raise RuntimeError("No candidates in response")
            
        part = response.candidates[0].content.parts
        if not part:
            raise RuntimeError("No content parts in response")
            
        text = part[0].text
        
        return json_parser(text)  