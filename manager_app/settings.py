from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    GOOGLEAI_API_KEY: str
    GOOGLEAI_MODEL: str

    MAX_CONCURRENT_TASKS: int

    MAX_RETRIES: int
    RETRY_DELAY: int

@lru_cache
def get_settings():
    return Settings()
