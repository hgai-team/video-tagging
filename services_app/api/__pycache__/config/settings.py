from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    TAG_API_KEY: str
    TAG_DOMAIN: str

    GOOGLEAI_API_KEY: str
    GOOGLEAI_MODEL: str
    GOOGLEAI_AUDIO: str
    GOOGLEAI_DETECTION_MODEL: str 

    MODEL_PATH: str
    MAX_CONCURRENT_TASKS: int
    MAX_RETRIES: int
    RETRY_DELAY: int

    QDRANT_HOST: str
    QDRANT_PORT: str
    VECTOR_SIZE: int

    REMOVED_URL: str
    UNLABEL_URL: str
    OLD_VERSION_URL: str
    DOWNLOAD_URL: str
    TAGS_URL: str
    FILE_INFO_TAG_VERSION: str
    CHORD_SERVICE_URL: str

@lru_cache
def get_settings():
    return Settings()
