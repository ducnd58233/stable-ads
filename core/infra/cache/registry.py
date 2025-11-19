from typing import Callable, Final, Type
from core.infra.cache.interface import AsyncCache
from core.settings.config import get_settings

_REGISTRY: Final[dict[str, Type[AsyncCache]]] = {}
_SINGLETON: Final[dict[str, AsyncCache]] = {}

def register_cache(name: str) -> Callable[[Type[AsyncCache]], Type[AsyncCache]]:
    key = name.strip().lower()
    def _wrap(cls: Type[AsyncCache]) -> Type[AsyncCache]:
        _REGISTRY[key] = cls
        return cls
    return _wrap

def get_cache(name: str) -> AsyncCache:
    cfg = get_settings().cache
    cache_name = cfg.driver.strip().lower()
    if cache_name not in _REGISTRY:
        raise ValueError(f"Unknown cache: {cache_name}")
    if cache_name not in _SINGLETON:
        _SINGLETON[cache_name] = _REGISTRY[cache_name](cfg)
    return _SINGLETON[cache_name]
