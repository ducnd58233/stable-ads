from sqlalchemy import JSON, Integer, String
from sqlalchemy.orm import Mapped, mapped_column
from core.infra.db.model import Base
from .domain import JobStatus

class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String, default=JobStatus.PENDING.value)
    progress: Mapped[int] = mapped_column(Integer, default=0)
    script_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # storage metadata (no persisted URI)
    video_bucket: Mapped[str | None] = mapped_column(String, nullable=True)
    video_key: Mapped[str | None] = mapped_column(String, nullable=True)
    video_mime: Mapped[str | None] = mapped_column(String, nullable=True)   # e.g., "video/mp4"
    video_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True) # size