from pydantic import PositiveInt, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal


class Settings(BaseSettings):
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

    CLICKHOUSE_HOST: str
    CLICKHOUSE_USER: str
    CLICKHOUSE_PASSWORD: str
    CLICKHOUSE_DATABASE: str

    BATCH_SIZE: PositiveInt = Field(default=100, gt=0, le=1000)

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()