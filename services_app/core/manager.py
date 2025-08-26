# New refactored core modules
from .downloaders import VideoDownloader, AudioDownloader
from .processors import VideoProcessor as NewVideoProcessor, AudioProcessor as NewAudioProcessor  
from .llm_analyzer import VideoAnalyzer, AudioAnalyzer, VideoDetector
from .storage import VectorStorage
from .constants import *

class VideoProcessor:
    """Combined video processor with download + processing capabilities."""
    
    def __init__(self, timeout: int = 3600, subprocess_timeout: int = 3600):
        self.downloader = VideoDownloader(timeout)
        self.processor = NewVideoProcessor(subprocess_timeout)
        self.timeout = timeout
        self.subprocess_timeout = subprocess_timeout

    async def download_video(self, url: str, output_path: str) -> bool:
        return await self.downloader.download_video(url, output_path)
    
    async def shrink_video(self, input_path: str, output_path: str, hd_limit: int = 720) -> bool:
        return await self.processor.shrink_video(input_path, output_path, hd_limit)


class AudioProcessor:
    """Combined audio processor with download + processing + analysis capabilities."""
    
    def __init__(self, timeout: int = 3600, subprocess_timeout: int = 3600):
        self.downloader = AudioDownloader(timeout)
        self.processor = NewAudioProcessor()
        self.timeout = timeout
        self.subprocess_timeout = subprocess_timeout

    async def download_audio(self, url: str, output_path: str) -> tuple[bool, str]:
        return await self.downloader.download_audio(url, output_path)
    
    async def process_audio(self, input_path: str, output_path: str) -> bool:
        return await self.processor.process_audio(input_path, output_path)
    
    def analyze_file(self, input_path: str, *, engine: str = "auto", profile: str = "edma", hop_length: int = 512):
        return self.processor.analyze_file(input_path, engine=engine, profile=profile, hop_length=hop_length)


class VideoTagging:
    """Video AI analysis wrapper."""
    
    def __init__(self):
        self.analyzer = VideoAnalyzer()

    async def chat(self, video_bytes):
        return await self.analyzer.analyze(video_bytes)


class AudioTagging:
    """Audio AI analysis wrapper."""
    
    def __init__(self):
        self.analyzer = AudioAnalyzer()

    async def chat(self, audio_bytes):
        return await self.analyzer.analyze(audio_bytes)


class VideoDetection:
    """Video detection wrapper."""
    
    def __init__(self):
        self.detector = VideoDetector()

    async def detect(self, video_bytes):
        return await self.detector.detect(video_bytes)


class PointProcessor:
    """Vector storage wrapper."""
    
    def __init__(self):
        self.storage = VectorStorage()

    async def embedding(self, text: str):
        return await self.storage.embedding(text)
    
    def detect_media_type(self, point):
        return self.storage.detect_media_type(point)
    
    async def handle_point(self, point, id: str, media_type: str = None):
        return await self.storage.handle_point(point, id, media_type)
    
    async def create_collection(self, collection_name: str, media_type: str):
        return await self.storage.create_collection(collection_name, media_type)
    
    async def upsert_points(self, collection_name: str, points, ids, media_type: str = None):
        return await self.storage.upsert_points(collection_name, points, ids, media_type)
    
    async def delete_points(self, collection_name: str, ids):
        return await self.storage.delete_points(collection_name, ids)