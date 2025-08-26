import asyncio
from google import genai
from google.genai import types

from config.settings import get_settings
from utils.prompts import VIDEO_TAGGING_PROMPT
from utils.parser import json_parser


class VideoAnalyzer:
    """Handles AI-powered video content analysis."""
    
    def __init__(self):
        self.client = genai.Client(
            api_key=get_settings().GOOGLEAI_API_KEY
        )

    async def analyze(self, video_bytes):
        """Analyze video content and extract tags."""
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