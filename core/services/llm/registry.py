from typing import Callable, Type
from core.services.llm.interfaces import LLMClient

_REGISTRY: dict[str, Type[LLMClient]] = {}

def register_provider(name: str) -> Callable[[Type[LLMClient]], Type[LLMClient]]:
    def decorator(cls: Type[LLMClient]) -> Type[LLMClient]:
        _REGISTRY[name] = cls
        return cls
    return decorator

def get_provider_cls(name: str) -> Type[LLMClient]:
    if name not in _REGISTRY:
        raise ValueError(f"LLM provider {name} not found")
    return _REGISTRY[name]

def list_providers() -> list[str]:
    return sorted(_REGISTRY.keys())