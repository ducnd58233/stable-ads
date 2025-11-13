# modules/warehouse/repository.py
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from .model import WarehouseData

class WarehouseDataRepository:
    async def create(self, s: AsyncSession, warehouse_data: WarehouseData) -> WarehouseData:
        """Create a single warehouse_data record."""
        s.add(warehouse_data)
        await s.flush()
        s.expunge(warehouse_data)
        return warehouse_data
    
    async def bulk_create(
        self,
        s: AsyncSession,
        warehouse_data_list: list[WarehouseData],
    ) -> int:
        if not warehouse_data_list:
            return 0
        
        s.add_all(warehouse_data_list)
        await s.flush()
        return len(warehouse_data_list)
    
    async def get(self, s: AsyncSession, record_id: int) -> WarehouseData | None:
        return await s.get(WarehouseData, record_id)
    
    async def get_by_ingestion_id(
        self,
        s: AsyncSession,
        ingestion_id: str,
    ) -> list[WarehouseData]:
        stmt = (
            select(WarehouseData)
            .where(WarehouseData.ingestion_id == ingestion_id)
            .order_by(WarehouseData.event_time)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def get_by_user_id(
        self,
        s: AsyncSession,
        user_id: int,
        limit: Optional[int] = None,
    ) -> list[WarehouseData]:
        stmt = (
            select(WarehouseData)
            .where(WarehouseData.user_id == user_id)
            .order_by(WarehouseData.event_time.desc())
        )
        if limit:
            stmt = stmt.limit(limit)
        result = await s.execute(stmt)
        return list(result.scalars().all())