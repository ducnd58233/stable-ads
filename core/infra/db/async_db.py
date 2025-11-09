from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncEngine
from core.settings.config import get_settings

class AsyncDB:
    def __init__(self):
        cfg = get_settings().db
        self._url = cfg.url

        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    @property
    def engine(self) -> AsyncEngine:
        assert self._engine is not None
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        assert self._session_factory is not None
        return self._session_factory

    async def start(self):
        self._engine: AsyncEngine = create_async_engine(self._url)
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(self._engine)

    async def stop(self):
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
