from typing import Callable, Type
from core.infra.blob.interface import AsyncBlobStorage
from core.settings.config import get_settings


_REG: dict[str, Type[AsyncBlobStorage]] = {}
_SINGLETON: dict[str, AsyncBlobStorage] = {}

def register_blob(name: str) -> Callable[[Type[AsyncBlobStorage]], Type[AsyncBlobStorage]]:
    key = name.strip().lower()
    def _wrap(cls: Type[AsyncBlobStorage]) -> Type[AsyncBlobStorage]:
        _REG[key] = cls
        return cls
    return _wrap

def get_blob() -> AsyncBlobStorage:
    cfg = get_settings().storage
    name = cfg.backend.strip().lower()
    if name not in _REG:
        raise ValueError(f"Blob storage {name} not found")
    if name not in _SINGLETON:
        _SINGLETON[name] = _REG[name](cfg)
    return _SINGLETON[name]