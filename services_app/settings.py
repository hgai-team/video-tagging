from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    GOOGLEAI_API_KEY: str
    GOOGLEAI_MODEL: str

    MODEL_PATH: str

    MAX_CONCURRENT_TASKS: int

    MAX_RETRIES: int
    RETRY_DELAY: int

    QDRANT_HOST: str
    QDRANT_PORT: str
    VECTOR_SIZE: int

    CRON_LIST: str

    UNLABEL_URL: str
    OLD_VERSION_URL: str
    DOWNLOAD_URL: str
    TAGS_URL: str

@lru_cache
def get_settings():
    return Settings()
