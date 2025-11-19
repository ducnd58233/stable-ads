from datetime import datetime
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, case, asc
from .model import WarehouseData
from .domain import UserAggregate

class WarehouseDataRepository:
    async def create(self, s: AsyncSession, warehouse_data: WarehouseData) -> WarehouseData:
        s.add(warehouse_data)
        await s.flush()
        s.expunge(warehouse_data)
        return warehouse_data
    
    async def bulk_create(
        self,
        s: AsyncSession,
        warehouse_data_list: list[WarehouseData],
        batch_size: int = 10000,
    ) -> int:
        if not warehouse_data_list:
            return 0
        
        total_inserted = 0
        for i in range(0, len(warehouse_data_list), batch_size):
            batch = warehouse_data_list[i:i + batch_size]
            s.add_all(batch)
            await s.flush()
            total_inserted += len(batch)
        
        return total_inserted
    
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

    async def get_events_in_window(
        self,
        s: AsyncSession,
        window_start: datetime,
        window_end: datetime,
    ) -> list[WarehouseData]:
        stmt = (
            select(WarehouseData)
            .where(
                WarehouseData.event_time >= window_start,
                WarehouseData.event_time < window_end,
            )
            .order_by(WarehouseData.user_id, WarehouseData.event_time)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def get_purchased_users_in_window(
        self,
        s: AsyncSession,
        window_start: datetime,
        window_end: datetime,
    ) -> set[int]:
        stmt = (
            select(WarehouseData.user_id)
            .where(
                WarehouseData.event_type == "purchase",
                WarehouseData.event_time >= window_start,
                WarehouseData.event_time < window_end,
            )
            .distinct()
        )
        result = await s.execute(stmt)
        return {row[0] for row in result.fetchall() if row[0] is not None}
    
    async def get_user_events(
        self,
        s: AsyncSession,
        user_id: int,
        window_start: datetime,
        window_end: datetime,
    ) -> list[WarehouseData]:
        stmt = (
            select(WarehouseData)
            .where(
                WarehouseData.user_id == user_id,
                WarehouseData.event_time >= window_start,
                WarehouseData.event_time < window_end,
            )
            .order_by(WarehouseData.event_time)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def get_user_aggregates_in_window(
        self,
        s: AsyncSession,
        window_start: datetime,
        window_end: datetime,
        batch_size: int = 10000,
        offset: int = 0,
    ) -> list[UserAggregate]:
        stmt = (
            select(
                WarehouseData.user_id,
                func.count(func.distinct(WarehouseData.user_session)).label("session_count"),
                func.min(WarehouseData.event_time).label("first_event_time"),
                func.max(WarehouseData.event_time).label("last_event_time"),
                func.sum(
                    case((WarehouseData.event_type == "purchase", WarehouseData.price), else_=0)
                ).label("total_spend"),
                func.count(
                    case((WarehouseData.event_type == "purchase", 1), else_=None)
                ).label("purchase_count"),
                func.mode().within_group(asc(WarehouseData.category_code)).label("top_category_1"),
                func.mode().within_group(asc(WarehouseData.brand)).label("top_brand_1"),
                func.min(WarehouseData.price).label("price_range_min"),
                func.max(WarehouseData.price).label("price_range_max"),
                func.avg(WarehouseData.price).label("price_range_avg"),
            )
            .where(
                WarehouseData.event_time >= window_start,
                WarehouseData.event_time < window_end,
                WarehouseData.user_id.isnot(None),
            )
            .group_by(WarehouseData.user_id)
            .order_by(WarehouseData.user_id)
            .limit(batch_size)
            .offset(offset)
        )
        
        result = await s.execute(stmt)
        rows = result.fetchall()
        
        return [
            UserAggregate(
                user_id=row.user_id,
                session_count=row.session_count,
                first_event_time=row.first_event_time,
                last_event_time=row.last_event_time,
                total_spend=float(row.total_spend) if row.total_spend else 0.0,
                purchase_count=row.purchase_count or 0,
                top_category_1=row.top_category_1,
                top_brand_1=row.top_brand_1,
                price_range_min=float(row.price_range_min) if row.price_range_min is not None else None,
                price_range_max=float(row.price_range_max) if row.price_range_max is not None else None,
                price_range_avg=float(row.price_range_avg) if row.price_range_avg is not None else None,
            )
            for row in rows
        ]

    async def get_user_events_batch(
        self,
        s: AsyncSession,
        user_ids: list[int],
        window_start: datetime,
        window_end: datetime,
    ) -> dict[int, list[WarehouseData]]:
        if not user_ids:
            return {}
        
        stmt = (
            select(WarehouseData)
            .where(
                WarehouseData.user_id.in_(user_ids),
                WarehouseData.event_time >= window_start,
                WarehouseData.event_time < window_end,
            )
            .order_by(WarehouseData.user_id, WarehouseData.event_time)
        )
        result = await s.execute(stmt)
        events = list(result.scalars().all())
        
        user_events_map: dict[int, list[WarehouseData]] = {}
        for event in events:
            if event.user_id is not None:
                if event.user_id not in user_events_map:
                    user_events_map[event.user_id] = []
                user_events_map[event.user_id].append(event)
        
        return user_events_map

    async def get_event_time_range(
        self,
        s: AsyncSession,
    ) -> tuple[datetime | None, datetime | None]:
        stmt = (
            select(
                func.min(WarehouseData.event_time).label("min_time"),
                func.max(WarehouseData.event_time).label("max_time"),
            )
            .where(WarehouseData.user_id.isnot(None))
        )
        result = await s.execute(stmt)
        row = result.fetchone()
        
        if row:
            return (row.min_time, row.max_time)
        return (None, None)
    
    async def get_user_events_recent(
        self,
        s: AsyncSession,
        user_id: int,
        lookback_days: int = 30,
    ) -> list[WarehouseData]:
        """
        Get recent events for a user within lookback window.
        
        Args:
            s: Database session
            user_id: User identifier
            lookback_days: Number of days to look back from current time
        
        Returns:
            List of warehouse events ordered by event_time
        """
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        
        window_end = datetime.now(ZoneInfo("UTC"))
        window_start = window_end - timedelta(days=lookback_days)
        
        stmt = (
            select(WarehouseData)
            .where(
                WarehouseData.user_id == user_id,
                WarehouseData.event_time >= window_start,
                WarehouseData.event_time <= window_end,
            )
            .order_by(WarehouseData.event_time)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def get_user_events_count(
        self,
        s: AsyncSession,
        user_id: int,
        lookback_days: int = 30,
    ) -> int:
        """
        Get count of recent events for a user within lookback window.
        Quick existence check without loading full data.
        
        Args:
            s: Database session
            user_id: User identifier
            lookback_days: Number of days to look back from current time
        
        Returns:
            Count of events
        """
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        
        window_end = datetime.now(ZoneInfo("UTC"))
        window_start = window_end - timedelta(days=lookback_days)
        
        stmt = (
            select(func.count(WarehouseData.id))
            .where(
                WarehouseData.user_id == user_id,
                WarehouseData.event_time >= window_start,
                WarehouseData.event_time <= window_end,
            )
        )
        result = await s.execute(stmt)
        count = result.scalar_one()
        return count or 0
    
    async def get_session_events(
        self,
        s: AsyncSession,
        session_id: str,
    ) -> list[WarehouseData]:
        """
        Get all events for a session (for anonymous users).
        
        Args:
            s: Database session
            session_id: Session identifier
        
        Returns:
            List of warehouse events for the session ordered by event_time
        """
        stmt = (
            select(WarehouseData)
            .where(WarehouseData.user_session == session_id)
            .order_by(WarehouseData.event_time)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def get_session_events_count(
        self,
        s: AsyncSession,
        session_id: str,
    ) -> int:
        """
        Get count of events for a session.
        Quick existence check without loading full data.
        
        Args:
            s: Database session
            session_id: Session identifier
        
        Returns:
            Count of events
        """
        stmt = (
            select(func.count(WarehouseData.id))
            .where(WarehouseData.user_session == session_id)
        )
        result = await s.execute(stmt)
        count = result.scalar_one()
        return count or 0