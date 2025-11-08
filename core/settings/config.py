import os
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel, Field
from langchain_core.globals import set_debug, set_verbose
from typing import Literal
from functools import lru_cache

BASE_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE_PATH = os.path.join(BASE_DIR, ".env")

class LLMSettings(BaseModel):
    provider: Literal["openai", "ollama"] = "openai"
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

class Settings(BaseSettings):
    llm: LLMSettings = Field(default_factory=LLMSettings)
    diffuser: DiffuserSettings = Field(default_factory=DiffuserSettings)

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