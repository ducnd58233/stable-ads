from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class UserAggregate:
    user_id: int
    session_count: int
    first_event_time: Optional[datetime]
    last_event_time: Optional[datetime]
    total_spend: float
    purchase_count: int
    top_category_1: Optional[str]
    top_brand_1: Optional[str]
    price_range_min: Optional[float]
    price_range_max: Optional[float]
    price_range_avg: Optional[float]
