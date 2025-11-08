from core.services.llm.interfaces import LLMClient
from core.services.llm.registry import get_provider_cls
from core.settings.config import get_settings
from .providers import *
from .tools import *

def get_client() -> LLMClient:
    cfg = get_settings().llm
    provider_cls = get_provider_cls(cfg.provider)
    return provider_cls(model=cfg.model)