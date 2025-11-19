from sqlalchemy import String, BigInteger, Double, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from core.infra.db.model import Base

class PurchasePrediction(Base):
    __tablename__ = "purchase_predictions"
    
    id: Mapped[str] = mapped_column(String, primary_key=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    session_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    purchase_probability: Mapped[float] = mapped_column(Double, nullable=False)
    will_purchase: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)
    model_version: Mapped[str] = mapped_column(String, nullable=False, index=True)
    feature_version_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    model_path: Mapped[str] = mapped_column(String, nullable=False)
    ad_job_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

