from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from .model import DataIngestion
from .domain import IngestionStatus

class DataIngestionRepository:
    async def create(self, s: AsyncSession, ingestion: DataIngestion) -> DataIngestion:
        s.add(ingestion)
        await s.flush()
        s.expunge(ingestion)
        return ingestion
    
    async def get(self, s: AsyncSession, ingestion_id: str) -> DataIngestion | None:
        return await s.get(DataIngestion, ingestion_id)
    
    async def set_status(
        self, 
        s: AsyncSession, 
        ingestion_id: str, 
        status: IngestionStatus,
        error_message: str | None = None
    ) -> None:
        stmt = (
            update(DataIngestion)
            .where(DataIngestion.id == ingestion_id)
            .values(status=status.value, error_message=error_message)
        )
        await s.execute(stmt)
    
    async def set_storage(
        self,
        s: AsyncSession,
        ingestion_id: str,
        *,
        bucket: str,
        key: str,
        nbytes: int
    ) -> None:
        stmt = (
            update(DataIngestion)
            .where(DataIngestion.id == ingestion_id)
            .values(raw_bucket=bucket, raw_key=key, raw_bytes=nbytes)
        )
        await s.execute(stmt)