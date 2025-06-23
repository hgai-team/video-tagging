# services_app/config.py
from dataclasses import dataclass
from pathlib import Path

# --- Cấu hình chung ---
BASE_TEMP_DIR = Path("./temp_videos")
TAG_VERSION = "v3"
MIN_VIDEO_SIZE_MB = 2
COLLECTION_NAME = "render_video_pro_collection"
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
    min_video_size_mb: int

# --- Định nghĩa các cấu hình cho từng pipeline ---

UNLABELED_CONFIG = PipelineConfig(
    name="unlabeled",
    collection_name=COLLECTION_NAME,
    tag_version=TAG_VERSION,
    media_type=2,
    temp_dir=BASE_TEMP_DIR / "unlabled",
    max_concurrent_downloads=30,
    max_concurrent_processing=30,
    min_video_size_mb=MIN_VIDEO_SIZE_MB,
)

OLD_VERSION_CONFIG = PipelineConfig(
    name="old_version",
    collection_name=COLLECTION_NAME,
    tag_version=TAG_VERSION,
    media_type=2,
    temp_dir=BASE_TEMP_DIR / "old_version",
    max_concurrent_downloads=10,
    max_concurrent_processing=10,
    min_video_size_mb=MIN_VIDEO_SIZE_MB,
)

MISS_UPSERT_CONFIG = PipelineConfig(
    name="miss_upsert",
    collection_name=COLLECTION_NAME,
    tag_version=TAG_VERSION,
    media_type=2,
    temp_dir=BASE_TEMP_DIR / "miss_upsert",
    max_concurrent_downloads=10,
    max_concurrent_processing=10,
    min_video_size_mb=MIN_VIDEO_SIZE_MB,
)
