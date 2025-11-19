from typing import Callable, Final, Type
from core.infra.blob.interface import AsyncBlobStorage
from core.settings.config import get_settings


_REG: Final[dict[str, Type[AsyncBlobStorage]]] = {}
_SINGLETON: Final[dict[str, AsyncBlobStorage]] = {}

def register_blob(name: str) -> Callable[[Type[AsyncBlobStorage]], Type[AsyncBlobStorage]]:
    key = name.strip().lower()
    def _wrap(cls: Type[AsyncBlobStorage]) -> Type[AsyncBlobStorage]:
        _REG[key] = cls
        return cls
    return _wrap

def get_blob() -> AsyncBlobStorage:
    cfg = get_settings().storage
    name = cfg.driver.strip().lower()
    if name not in _REG:
        raise ValueError(f"Blob storage {name} not found")
    if name not in _SINGLETON:
        _SINGLETON[name] = _REG[name](cfg)
    return _SINGLETON[name]