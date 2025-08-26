# Backward compatibility imports
from .manager import (
    VideoProcessor, AudioProcessor, VideoTagging, 
    AudioTagging, VideoDetection, PointProcessor
)

# Create singleton instances for backward compatibility
video_pro = VideoProcessor()
video_tag = VideoTagging()
video_detect = VideoDetection()

audio_pro = AudioProcessor()
audio_tag = AudioTagging()

point_pro = PointProcessor()

# Also expose the new modular components for advanced usage
from . import downloaders, processors, llm_analyzer, storage, constants

__all__ = [
    # Backward compatible classes
    "VideoProcessor", "AudioProcessor", "VideoTagging", 
    "AudioTagging", "VideoDetection", "PointProcessor",
    
    # Singleton instances
    "video_pro", "video_tag", "video_detect",
    "audio_pro", "audio_tag", "point_pro",
    
    # New modular components
    "downloaders", "processors", "llm_analyzer", "storage", "constants"
]