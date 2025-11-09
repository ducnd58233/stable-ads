import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field, computed_field
from langchain_core.globals import set_debug, set_verbose
from functools import lru_cache

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE_PATH = os.path.join(BASE_DIR, ".env")

class LLMSettings(BaseModel):
    model: str = "gpt-4o-mini"
    api_key: str | None = None
    base_url: str | None = None

class DiffuserSettings(BaseModel):
    backend: str
    device: str
    dtype: str
    width: int
    height: int
    fps: int
    seed: int
    cpu_offload: bool
    # SVD
    sdxl_model_id: str | None = None
    svd_model_id: str | None = None
    svd_num_frames: int | None = None
    svd_num_steps: int | None = None
    svd_motion_bucket: int | None = None
    svd_decode_chunk: int | None = None
    # AnimateDiff
    ad_base_model_id: str | None = None
    ad_motion_adapter_id: str | None = None
    ad_steps: int | None = None
    ad_frames: int | None = None
    ad_guidance: float | None = None

class DBSettings(BaseModel):
    driver: str = "postgresql"  # postgresql, mysql, sqlite, etc.
    host: str = "localhost"
    port: int = 5432
    db: str = "text2video"
    user: str = "postgres"
    password: str = "postgres"
    
    @computed_field
    @property
    def url(self) -> str:
        """Generate database connection URL based on driver."""
        if self.driver == "postgresql":
            return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        elif self.driver == "mysql":
            return f"mysql+aiomysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        elif self.driver == "sqlite":
            return f"sqlite+aiosqlite:///{self.db}"
        else:
            raise ValueError(f"Unsupported database driver: {self.driver}")

class StorageSettings(BaseModel):
    driver: str = "minio"  # minio, s3, azure, etc.
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    secure: bool = False
    region: str | None = None  # For S3/Azure
    
    @computed_field
    @property
    def url(self) -> str:
        """Generate storage connection URL."""
        protocol = "https" if self.secure else "http"
        if self.driver == "minio":
            return f"{protocol}://{self.endpoint}"
        elif self.driver == "s3":
            return f"https://s3.{self.region or 'us-east-1'}.amazonaws.com"
        elif self.driver == "azure":
            return f"https://{self.endpoint}.blob.core.windows.net"
        else:
            raise ValueError(f"Unsupported storage driver: {self.driver}")

class MQSettings(BaseModel):
    driver: str = "kafka"  # kafka, rabbitmq, redis, etc.
    broker: str = "localhost:9092"  # For Kafka: broker, for RabbitMQ: amqp://..., etc.
    group_renderer: str = "renderer-workers"
    default_partitions: int = 3
    default_replication_factor: int = 1
    
    @computed_field
    @property
    def url(self) -> str:
        """Generate message queue connection URL."""
        if self.driver == "kafka":
            return f"kafka://{self.broker}"
        elif self.driver == "rabbitmq":
            return self.broker if self.broker.startswith("amqp://") else f"amqp://{self.broker}"
        elif self.driver == "redis":
            return self.broker if self.broker.startswith("redis://") else f"redis://{self.broker}"
        else:
            raise ValueError(f"Unsupported MQ driver: {self.driver}")

class Settings(BaseSettings):
    llm: LLMSettings = Field(default_factory=LLMSettings)
    diffuser: DiffuserSettings = Field(default_factory=DiffuserSettings)
    db: DBSettings = Field(default_factory=DBSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    mq: MQSettings = Field(default_factory=MQSettings)

    model_config = SettingsConfigDict(
        env_file=ENV_FILE_PATH,
        env_nested_delimiter="_",
        env_nested_max_split=1, 
        case_sensitive=False,
        extra="ignore",
    )

set_verbose(True)
set_debug(True)

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()