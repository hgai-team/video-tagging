from dataclasses import dataclass
from pathlib import Path

# --- Cấu hình chung ---
BASE_MEDIA_DIR = Path("./temp_media")
BASE_VIDEO_DIR = BASE_MEDIA_DIR / "videos"
BASE_AUDIO_DIR = BASE_MEDIA_DIR / "audios"
TAG_VERSION = "v3"
MIN_VIDEO_SIZE_MB = 0.2
MIN_AUDIO_SIZE_MB = 0.01
COLLECTION_NAME_VIDEO = "render_video_pro_collection"
COLLECTION_NAME_AUDIO = "render_audio_dev_collection"
LOG_LEVEL = "INFO"

@dataclass
class PipelineConfig:
    """Cấu hình cho một pipeline cụ thể."""
    name: str
    collection_name: str
    tag_version: str
    media_type: int
    temp_dir: Path
    max_concurrent_downloads: int
    max_concurrent_processing: int
    min_media_size_mb: float

UNLABELED_CONFIG_VIDEO = PipelineConfig(
    name="unlabeled_VIDEO",
    collection_name=COLLECTION_NAME_VIDEO,
    tag_version=TAG_VERSION,
    media_type=2,
    temp_dir=BASE_VIDEO_DIR / "unlabeled",
    max_concurrent_downloads=20,
    max_concurrent_processing=20,
    min_media_size_mb=MIN_VIDEO_SIZE_MB
)

UNLABELED_CONFIG_AUDIO = PipelineConfig(
    name="unlabeled_AUDIO",
    collection_name=COLLECTION_NAME_AUDIO,
    tag_version=TAG_VERSION,
    media_type=1,
    temp_dir=BASE_AUDIO_DIR / "unlabeled",
    max_concurrent_downloads=10,
    max_concurrent_processing=10,
    min_media_size_mb=MIN_AUDIO_SIZE_MB,
)

OLD_VERSION_CONFIG_VIDEO = PipelineConfig(
    name="old_version_VIDEO",
    collection_name=COLLECTION_NAME_VIDEO,
    tag_version=TAG_VERSION,
    media_type=2,
    temp_dir=BASE_VIDEO_DIR / "old_version",
    max_concurrent_downloads=10,
    max_concurrent_processing=10,
    min_media_size_mb=MIN_VIDEO_SIZE_MB,
)

OLD_VERSION_CONFIG_AUDIO = PipelineConfig(
    name="old_version_AUDIO",
    collection_name=COLLECTION_NAME_AUDIO,
    tag_version=TAG_VERSION,
    media_type=1,
    temp_dir=BASE_AUDIO_DIR / "old_version",
    max_concurrent_downloads=10,
    max_concurrent_processing=10,
    min_media_size_mb=MIN_AUDIO_SIZE_MB,
)

MISS_UPSERT_CONFIG_VIDEO = PipelineConfig(
    name="miss_upsert_VIDEO",
    collection_name=COLLECTION_NAME_VIDEO,
    tag_version=TAG_VERSION,
    media_type=2,
    temp_dir=BASE_VIDEO_DIR / "miss_upsert_VIDEO",
    max_concurrent_downloads=10,
    max_concurrent_processing=10,
    min_media_size_mb=MIN_VIDEO_SIZE_MB,
)

MISS_UPSERT_CONFIG_AUDIO = PipelineConfig(
    name="miss_upsert_AUDIO",
    collection_name=COLLECTION_NAME_AUDIO,
    tag_version=TAG_VERSION,
    media_type=1,
    temp_dir=BASE_AUDIO_DIR / "miss_upsert",
    max_concurrent_downloads=10,
    max_concurrent_processing=10,
    min_media_size_mb=MIN_AUDIO_SIZE_MB,
)

REAL_DETECTION_CONFIG = PipelineConfig(
    name="real_detection_VIDEO",
    collection_name=COLLECTION_NAME_VIDEO,
    tag_version=TAG_VERSION,
    media_type=2,
    temp_dir=BASE_VIDEO_DIR / "real_detection_VIDEO",
    max_concurrent_downloads=20,
    max_concurrent_processing=20,
    min_media_size_mb=MIN_VIDEO_SIZE_MB,
)