from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from .repository import DataIngestionRepository
from .model import DataIngestion
from .domain import IngestionStatus

class DataIngestionService:
    
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory
        self._repo = DataIngestionRepository()
    
    async def create(
        self,
        ingestion_id: str,
        file_type: str,
        fields: list[str],
        raw_bucket: str,
        raw_key: str,
        raw_bytes: int
    ) -> DataIngestion:
        ingestion = DataIngestion(
            id=ingestion_id,
            file_path=raw_key,
            file_type=file_type,
            fields=fields,
            status=IngestionStatus.PENDING.value,
            raw_bucket=raw_bucket,
            raw_key=raw_key,
            raw_bytes=raw_bytes,
        )
        
        async with self._sf() as s, s.begin():
            return await self._repo.create(s, ingestion)
    
    async def get(self, ingestion_id: str) -> DataIngestion | None:
        async with self._sf() as s:
            return await self._repo.get(s, ingestion_id)
    
    async def get_pending_ingestions(self, limit: int) -> list[DataIngestion]:
        async with self._sf() as s:
            return await self._repo.get_pending_ingestions(s, limit)
    
    async def update_status(
        self,
        ingestion_id: str,
        status: IngestionStatus,
        *,
        fields: list[str] | None = None,
        error_message: str | None = None,
    ) -> None:
        async with self._sf() as s, s.begin():
            await self._repo.set_status_with_fields(
                s,
                ingestion_id,
                status,
                fields=fields,
                error_message=error_message,
            )
    
    async def set_failed(self, ingestion_id: str, error_message: str) -> None:
        await self.update_status(
            ingestion_id,
            IngestionStatus.FAILED,
            error_message=error_message,
        )