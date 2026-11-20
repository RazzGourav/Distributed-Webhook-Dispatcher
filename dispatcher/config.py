from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: SecretStr = SecretStr(
        "postgresql://dispatcher:dispatcher@localhost:5432/dispatcher"
    )
    api_key: SecretStr = Field(min_length=16)
    embedded_worker: bool = True
    demo_enabled: bool = False
    allow_private_networks: bool = False  # local development/tests ONLY
    worker_concurrency: int = Field(default=8, ge=1, le=32)
    request_timeout: float = Field(default=12, gt=0, le=60)
    domain_rate: float = Field(default=2, gt=0, le=100)
    domain_burst: int = Field(default=1, ge=1, le=100)
    max_body_bytes: int = Field(default=65536, ge=1024, le=1048576)
    retention_days: int = Field(default=7, ge=1, le=30)
    log_level: str = "INFO"
