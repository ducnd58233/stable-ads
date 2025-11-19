import logging
from datetime import datetime, timedelta
from typing import AsyncIterator
from sqlalchemy.ext.asyncio import AsyncSession
from modules.warehouse.repository import WarehouseDataRepository
from modules.warehouse.model import WarehouseData
from modules.feature_store.dto import UserFeatureDTO

logger = logging.getLogger(__name__)

class PurchaseFeatureBuilder:
    def __init__(self, session: AsyncSession):
        self._session = session
        self._warehouse_repo = WarehouseDataRepository()
    
    @staticmethod
    def _compute_features_from_events(
        user_id: int,
        user_events: list[WarehouseData],
        feature_window_end: datetime,
        purchased_users: set[int] | None = None,
    ) -> UserFeatureDTO:
        """
        Compute user features from a list of warehouse events.
        
        Args:
            user_id: User identifier
            user_events: List of warehouse events for the user
            feature_window_end: End of feature window (for computing days_since_*)
            purchased_users: Optional set of user IDs who purchased (for label)
        
        Returns:
            UserFeatureDTO with computed features
        """
        if not user_events:
            raise ValueError("user_events cannot be empty")
        
        purchased_users = purchased_users or set()
        
        sessions = {e.user_session for e in user_events if e.user_session}
        session_count = len(sessions)
        
        session_durations = []
        page_views_per_session = []
        
        for session in sessions:
            session_events = [e for e in user_events if e.user_session == session]
            if len(session_events) > 1:
                duration = (
                    max(e.event_time for e in session_events) - 
                    min(e.event_time for e in session_events)
                ).total_seconds() / 3600.0
                session_durations.append(duration)
            page_views_per_session.append(len(session_events))
        
        session_duration_avg = (
            sum(session_durations) / len(session_durations) 
            if session_durations else 0.0
        )
        page_views_avg = (
            sum(page_views_per_session) / len(page_views_per_session)
            if page_views_per_session else 0.0
        )
        
        purchase_events = [e for e in user_events if e.event_type == "purchase"]
        total_spend = sum(e.price for e in purchase_events if e.price) or 0.0
        purchase_count = len(purchase_events)
        avg_order_value = (
            total_spend / purchase_count 
            if purchase_count > 0 else 0.0
        )
        
        days_since_last_event = (
            feature_window_end - max(e.event_time for e in user_events)
        ).days
        days_since_first_event = (
            feature_window_end - min(e.event_time for e in user_events)
        ).days
        
        last_purchase_time = (
            max(e.event_time for e in purchase_events) 
            if purchase_events else None
        )
        days_since_last_purchase = (
            (feature_window_end - last_purchase_time).days
            if last_purchase_time else None
        )
        
        category_counts = {}
        brand_counts = {}
        for e in user_events:
            if e.category_code:
                category_counts[e.category_code] = category_counts.get(e.category_code, 0) + 1
            if e.brand:
                brand_counts[e.brand] = brand_counts.get(e.brand, 0) + 1
        
        top_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)[:2]
        top_category_1 = top_categories[0][0] if len(top_categories) > 0 else None
        top_category_2 = top_categories[1][0] if len(top_categories) > 1 else None
        
        top_brands = sorted(brand_counts.items(), key=lambda x: x[1], reverse=True)[:2]
        top_brand_1 = top_brands[0][0] if len(top_brands) > 0 else None
        top_brand_2 = top_brands[1][0] if len(top_brands) > 1 else None
        
        valid_prices = [e.price for e in user_events if e.price is not None]
        if valid_prices:
            price_range_min = float(min(valid_prices))
            price_range_max = float(max(valid_prices))
            price_range_avg = float(sum(valid_prices) / len(valid_prices))
        else:
            price_range_min = None
            price_range_max = None
            price_range_avg = None
        
        purchased = 1 if user_id in purchased_users else 0
        
        return UserFeatureDTO(
            user_id=user_id,
            session_count=session_count,
            session_duration_avg=session_duration_avg,
            page_views_per_session=page_views_avg,
            total_spend=total_spend,
            purchase_count=purchase_count,
            avg_order_value=avg_order_value,
            days_since_last_purchase=days_since_last_purchase,
            days_since_last_event=days_since_last_event,
            days_since_first_event=days_since_first_event,
            top_category_1=top_category_1,
            top_category_2=top_category_2,
            top_brand_1=top_brand_1,
            top_brand_2=top_brand_2,
            price_range_min=price_range_min,
            price_range_max=price_range_max,
            price_range_avg=price_range_avg,
            purchased=purchased,
        )
    
    async def build_features(
        self,
        feature_window_start: datetime,
        feature_window_end: datetime,
        label_horizon_days: int = 7,
        batch_size: int = 10000,
    ) -> AsyncIterator[UserFeatureDTO]:
        label_end = feature_window_end + timedelta(days=label_horizon_days)
        
        purchased_users = await self._warehouse_repo.get_purchased_users_in_window(
            self._session,
            feature_window_end,
            label_end,
        )
        
        offset = 0
        
        while True:
            user_aggregates = await self._warehouse_repo.get_user_aggregates_in_window(
                self._session,
                feature_window_start,
                feature_window_end,
                batch_size,
                offset,
            )
            
            if not user_aggregates:
                break
            
            user_ids = [agg.user_id for agg in user_aggregates]
            
            user_events_map = await self._warehouse_repo.get_user_events_batch(
                self._session,
                user_ids,
                feature_window_start,
                feature_window_end,
            )
            
            for aggregate in user_aggregates:
                user_id = aggregate.user_id
                user_events = user_events_map.get(user_id, [])
                
                if not user_events:
                    continue
                
                feature_dto = self._compute_features_from_events(
                    user_id=user_id,
                    user_events=user_events,
                    feature_window_end=feature_window_end,
                    purchased_users=purchased_users,
                )
                
                yield feature_dto
            
            offset += batch_size
            
            if len(user_aggregates) < batch_size:
                break