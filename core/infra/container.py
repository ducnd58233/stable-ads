from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, AsyncSession
from core.infra.blob.interface import AsyncBlobStorage
from core.infra.mq import AsyncPublisher, Marshaler

@dataclass(slots=True)
class Infra:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    blob: AsyncBlobStorage
    publisher: AsyncPublisher
    marshaler: Marshaler