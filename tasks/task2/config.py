from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal


class Settings(BaseSettings):

    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    GITHUB_ACCESS_TOKEN: str
    TOP_REPOSITORIES_LIMIT: int
    MAX_CONCURRENT_REQUESTS: int
    REQUESTS_PER_SECOND: int

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()


