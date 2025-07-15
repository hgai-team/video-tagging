from .parser import json_parser

from .video_processor import VideoProcessor

from .video_tagging import VideoTagging

from .point_processor import PointProcessor

from .video_detection import VideoDetection

video_pro = VideoProcessor()
video_tag = VideoTagging()
point_pro = PointProcessor()
video_detect = VideoDetection()
