from sqlalchemy import String, BigInteger, Double, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from core.infra.db.model import Base

class WarehouseData(Base):
    __tablename__ = "warehouse_data"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ingestion_id: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    event_time: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    event_type: Mapped[str | None] = mapped_column(String, nullable=True)
    product_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    category_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    category_code: Mapped[str | None] = mapped_column(String, nullable=True)
    brand: Mapped[str | None] = mapped_column(String, nullable=True)
    price: Mapped[float | None] = mapped_column(Double, nullable=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)
    user_session: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    inserted_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())