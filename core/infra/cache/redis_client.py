# core/infra/cache/redis_client.py
import json
import logging
from typing import Any
from redis.asyncio import Redis
from core.infra.cache.interface import AsyncCache
from core.settings.config import CacheSettings
from core.infra.cache.registry import register_cache

logger = logging.getLogger(__name__)

@register_cache("redis")    
class RedisClient(AsyncCache):
    def __init__(self, cfg: CacheSettings):
        self._host = cfg.host
        self._port = cfg.port
        self._db = cfg.db
        self._password = cfg.password
        self._client: Redis | None = None
    
    async def start(self) -> None:
        self._client = Redis(
            host=self._host,
            port=self._port,
            db=self._db,
            password=self._password,
            decode_responses=False,
        )
        await self._client.ping()
        logger.info("Redis client connected")
    
    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
    
    async def get(self, key: str) -> bytes | None:
        if not self._client:
            raise RuntimeError("Redis client not initialized")
        result = await self._client.get(key)
        if result is None:
            return None
        # Ensure bytes return type
        if isinstance(result, str):
            return result.encode("utf-8")
        return result
    
    async def set(self, key: str, value: bytes, ttl: int | None = None) -> None:
        if not self._client:
            raise RuntimeError("Redis client not initialized")
        if ttl:
            await self._client.setex(key, ttl, value)
        else:
            await self._client.set(key, value)
    
    async def delete(self, key: str) -> None:
        if not self._client:
            raise RuntimeError("Redis client not initialized")
        await self._client.delete(key)
    
    async def exists(self, key: str) -> bool:
        if not self._client:
            raise RuntimeError("Redis client not initialized")
        result = await self._client.exists(key)
        return bool(result)
    
    async def set_json(self, key: str, value: dict[str, Any], ttl: int | None = None) -> None:
        json_bytes = json.dumps(value).encode("utf-8")
        await self.set(key, json_bytes, ttl)
    
    async def get_json(self, key: str) -> dict[str, Any] | None:
        result = await self.get(key)
        if result is None:
            return None
        return json.loads(result.decode("utf-8"))