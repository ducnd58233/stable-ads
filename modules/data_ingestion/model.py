from sqlalchemy import String, BigInteger, JSON, DateTime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from core.infra.db.model import Base
from .domain import IngestionStatus

class DataIngestion(Base):
    __tablename__ = "data_ingestions"
    
    id: Mapped[str] = mapped_column(String, primary_key=True)
    file_path: Mapped[str] = mapped_column(String)
    file_type: Mapped[str] = mapped_column(String)
    fields: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String, default=IngestionStatus.PENDING.value)
    
    raw_bucket: Mapped[str | None] = mapped_column(String, nullable=True)
    raw_key: Mapped[str | None] = mapped_column(String, nullable=True)
    raw_bytes: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())