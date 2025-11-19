from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update, select
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
    
    async def get_pending_ingestions(
        self,
        s: AsyncSession,
        limit: int,
    ) -> list[DataIngestion]:
        """Get pending ingestions ordered by created_at."""
        stmt = (
            select(DataIngestion)
            .where(DataIngestion.status == IngestionStatus.PENDING.value)
            .order_by(DataIngestion.created_at.asc())
            .limit(limit)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
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
    
    async def set_status_with_fields(
        self,
        s: AsyncSession,
        ingestion_id: str,
        status: IngestionStatus,
        *,
        fields: list[str] | None = None,
        error_message: str | None = None,
    ) -> None:
        """Update status and optionally fields."""
        values = {
            "status": status.value,
            "error_message": error_message,
        }
        if fields is not None:
            values["fields"] = fields
        
        stmt = (
            update(DataIngestion)
            .where(DataIngestion.id == ingestion_id)
            .values(**values)
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