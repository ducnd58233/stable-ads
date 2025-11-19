from typing import Callable, Final, Type

from .interfaces import AsyncPublisher, AsyncSubscriber, Marshaler

_PUB_REG: Final[dict[str, Type[AsyncPublisher]]] = {}
_SUB_REG: Final[dict[str, Type[AsyncSubscriber]]] = {}
_MARSHALER_REG: Final[dict[str, Type[Marshaler]]] = {}

def register_marshaler(name: str) -> Callable[[Type[Marshaler]], Type[Marshaler]]:
    def _wrap(cls: Type[Marshaler]):
        _MARSHALER_REG[name.lower()] = cls
        return cls
    return _wrap

def register_publisher(name: str) -> Callable[[Type[AsyncPublisher]], Type[AsyncPublisher]]:
    def _wrap(cls: Type[AsyncPublisher]):
        _PUB_REG[name.lower()] = cls
        return cls
    return _wrap

def register_subscriber(name: str) -> Callable[[Type[AsyncSubscriber]], Type[AsyncSubscriber]]:
    def _wrap(cls: Type[AsyncSubscriber]):
        _SUB_REG[name.lower()] = cls
        return cls
    return _wrap


def create_marshaler(name: str, **kwargs) -> Marshaler:
    k = name.lower()
    if k not in _MARSHALER_REG:
        raise ValueError(f"Marshaler '{name}' not registered. Have: {sorted(_MARSHALER_REG)}")
    return _MARSHALER_REG[k](**kwargs)

def create_publisher(name: str, **kwargs) -> AsyncPublisher:
    k = name.lower()
    if k not in _PUB_REG:
        raise ValueError(f"Publisher '{name}' not registered. Have: {sorted(_PUB_REG)}")
    return _PUB_REG[k](**kwargs) 

def create_subscriber(name: str, **kwargs) -> AsyncSubscriber:
    k = name.lower()
    if k not in _SUB_REG:
        raise ValueError(f"Subscriber '{name}' not registered. Have: {sorted(_SUB_REG)}")
    return _SUB_REG[k](**kwargs)