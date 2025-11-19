from abc import ABC, abstractmethod

class AsyncCache(ABC):
    @abstractmethod
    async def start(self) -> None:
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        pass
    
    @abstractmethod
    async def get(self, key: str) -> bytes | None:
        pass
    
    @abstractmethod
    async def set(self, key: str, value: bytes, ttl: int | None = None) -> None:
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        pass