from .video_processor import VideoProcessor

from .video_tagging import VideoTagging

from .point_processor import PointProcessor

from .video_detection import VideoDetection

from .audio_processor import AudioProcessor

from .audio_tagging import AudioTagging

video_pro = VideoProcessor()
video_tag = VideoTagging()
video_detect = VideoDetection()

audio_pro = AudioProcessor()
audio_tag =AudioTagging()

point_pro = PointProcessor()