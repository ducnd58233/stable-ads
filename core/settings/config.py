import json
import os
from pathlib import Path
from typing import Any
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field, computed_field, field_validator
from functools import lru_cache

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE_PATH = os.path.join(BASE_DIR, ".env")

class MLflowSettings(BaseModel):
    tracking_uri: str = Field(
        default_factory=lambda: os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"),
    )
    experiment_name: str = Field(
        default_factory=lambda: os.getenv("MLFLOW_EXPERIMENT_NAME", "purchase-prediction"),
    )
    registry_uri: str | None = Field(
        default_factory=lambda: os.getenv("MLFLOW_REGISTRY_URI"),
    )

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

    @field_validator('device')
    @classmethod
    def normalize_device(cls, v: str) -> str:
        v = v.strip().lower()
        if v not in ('cpu', 'cuda', 'mps'):
            raise ValueError(f"Device must be 'cpu', 'cuda', or 'mps', got: {v}")
        return v

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
    driver: str = Field(default="kafka", description="kafka | rabbitmq | redis | nats")
    # For clustered drivers (kafka / nats):
    brokers: str
    # For single-URL drivers (rabbitmq / redis / nats):
    url: str | None = None
    group_renderer: str = "renderer-workers"
    default_partitions: int = 3
    default_replication_factor: int = 1

    @computed_field
    @property
    def bootstrap(self) -> str:
        return self.brokers.split(",")


class CacheSettings(BaseModel):
    driver: str = "redis"
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: str | None = None

    @computed_field
    @property
    def url(self) -> str:
        if self.driver == "redis":
            return f"redis://{self.host}:{self.port}/{self.db}"
        else:
            raise ValueError(f"Unsupported cache driver: {self.driver}")

class MLPredictionSettings(BaseModel):
    purchase_probability_threshold: float = Field(
        default=0.7,
        ge=0.0,
        le=1.0,
        description="Threshold for purchase probability to trigger ad generation"
    )
    model_cache_ttl_seconds: int = Field(
        default=3600,
        ge=0,
        description="Time to live for cached models in seconds"
    )
    batch_prediction_size: int = Field(
        default=1000,
        ge=1,
        description="Batch size for batch predictions"
    )
    new_customer_lookback_days: int = Field(
        default=30,
        ge=1,
        description="Number of days to look back for new customer feature computation"
    )
    realtime_feature_cache_ttl_seconds: int = Field(
        default=300,
        ge=0,
        description="Time to live for cached real-time features in seconds (5 minutes)"
    )


class Settings(BaseSettings):
    llm: LLMSettings = Field(default_factory=LLMSettings)
    diffuser: DiffuserSettings = Field(default_factory=DiffuserSettings)
    db: DBSettings = Field(default_factory=DBSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    mq: MQSettings = Field(default_factory=MQSettings)
    mlflow: MLflowSettings = Field(default_factory=MLflowSettings)
    cache: CacheSettings = Field(default_factory=CacheSettings)
    ml_prediction: MLPredictionSettings = Field(default_factory=MLPredictionSettings)

    model_config = SettingsConfigDict(
        env_file=ENV_FILE_PATH,
        env_nested_delimiter="_",
        env_nested_max_split=1, 
        case_sensitive=False,
        extra="ignore",
    )

try:
    from langchain_core.globals import set_debug, set_verbose
    set_verbose(True)
    set_debug(True)
except ImportError:
    pass

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()