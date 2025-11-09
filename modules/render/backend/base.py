from abc import ABC, abstractmethod
from core.settings.config import DiffuserSettings, get_settings
from typing import Callable, Type
from .dto import RenderRequest
from pathlib import Path


class VideoBackend(ABC):
    def __init__(self, cfg: DiffuserSettings):
        self._cfg = cfg

    def _get_model_cache_dir(self, model_type: str, model_name: str) -> str:
        """
        Returns the cache directory for a specific model.
        Structure: runs/models/video-render/<model_type>/<model_name>
        
        Args:
            model_type: Type of model (e.g., 'animatediff', 'svd', 'text-to-image')
            model_name: Name/ID of the model (extracted from model_id)
        """
        # Extract model name from model_id (e.g., "stabilityai/stable-diffusion-xl-base-1.0" -> "stable-diffusion-xl-base-1.0")
        # Handle both "org/model" and plain "model" formats
        if "/" in model_name:
            model_name = model_name.split("/")[-1]
        
        cache_base = Path("runs/models/video-render")
        cache_dir = cache_base / model_type / model_name
        cache_dir.mkdir(parents=True, exist_ok=True)
        return str(cache_dir)

    @abstractmethod
    def render(self, req: RenderRequest, out_path: str) -> str: ...

    def close(self) -> None:
        pass

_REGISTRY: dict[str, Type[VideoBackend]] = {}

def register_backend(name: str) -> Callable[[Type[VideoBackend]], Type[VideoBackend]]:
    key = name.strip().lower()
    def _wrap(cls: Type[VideoBackend]) -> Type[VideoBackend]:
        _REGISTRY[key] = cls
        return cls
    return _wrap

def list_backends() -> list[str]:
    return sorted(_REGISTRY.keys())

_SINGLETON: dict[str, VideoBackend] = {}

def get_backend() -> VideoBackend:
    cfg = get_settings().diffuser
    name = cfg.backend.strip().lower()
    if name not in _REGISTRY:
        raise ValueError(f"Video backend {name} not found")
    cache_key = f"{name}:{cfg.device}"
    if cache_key not in _SINGLETON:
        _SINGLETON[cache_key] = _REGISTRY[name](cfg)
    return _SINGLETON[cache_key]
    