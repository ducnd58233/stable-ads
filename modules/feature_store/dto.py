from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class UserFeatureDTO(BaseModel):
    user_id: int = Field(..., description="User identifier")
    session_count: Optional[int] = Field(None, description="Number of sessions")
    session_duration_avg: Optional[float] = Field(None, description="Average session duration in hours")
    page_views_per_session: Optional[float] = Field(None, description="Average page views per session")
    total_spend: Optional[float] = Field(None, description="Total spending amount")
    purchase_count: Optional[int] = Field(None, description="Number of purchases")
    avg_order_value: Optional[float] = Field(None, description="Average order value")
    days_since_last_purchase: Optional[int] = Field(None, description="Days since last purchase")
    days_since_last_event: Optional[int] = Field(None, description="Days since last event")
    days_since_first_event: Optional[int] = Field(None, description="Days since first event")
    top_category_1: Optional[str] = Field(None, description="Most frequent category")
    top_category_2: Optional[str] = Field(None, description="Second most frequent category")
    top_brand_1: Optional[str] = Field(None, description="Most frequent brand")
    top_brand_2: Optional[str] = Field(None, description="Second most frequent brand")
    price_range_min: Optional[float] = Field(None, description="Minimum price")
    price_range_max: Optional[float] = Field(None, description="Maximum price")
    price_range_avg: Optional[float] = Field(None, description="Average price")
    purchased: int = Field(..., description="Purchase label (0 or 1)")

class FeatureMaterializationResultDTO(BaseModel):
    feature_version_id: str = Field(..., description="Feature version identifier")
    version: str = Field(..., description="Feature version string")
    total_users: int = Field(..., description="Total number of users with features")
    total_records: int = Field(..., description="Total number of feature records")
    feature_window_start: datetime = Field(..., description="Start of feature window")
    feature_window_end: datetime = Field(..., description="End of feature window")
