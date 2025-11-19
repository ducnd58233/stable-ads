from sqlalchemy import String, BigInteger, Double, DateTime, Integer, Boolean, JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func
from core.infra.db.model import Base
from .domain import FeatureVersionStatus, ModelStatus

class FeatureVersion(Base):
    __tablename__ = "feature_versions"
    
    id: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String, unique=True, index=True)
    status: Mapped[str] = mapped_column(String, default=FeatureVersionStatus.PENDING.value)
    feature_window_start: Mapped[DateTime] = mapped_column(DateTime(timezone=True))
    feature_window_end: Mapped[DateTime] = mapped_column(DateTime(timezone=True))
    label_horizon_days: Mapped[int] = mapped_column(Integer, default=7)
    total_users: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_records: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checkpoint_path: Mapped[str | None] = mapped_column(String, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

class MLTrainingRun(Base):
    __tablename__ = "ml_training_runs"
    
    id: Mapped[str] = mapped_column(String, primary_key=True)
    feature_version_id: Mapped[str] = mapped_column(String, index=True)
    model_version: Mapped[str] = mapped_column(String, unique=True, index=True)
    mlflow_run_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    status: Mapped[str] = mapped_column(String, default=ModelStatus.TRAINING.value)
    train_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    val_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    test_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metrics: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    model_path: Mapped[str | None] = mapped_column(String, nullable=True)
    is_production: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    error_message: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    completed_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

class ProductionModel(Base):
    __tablename__ = "production_models"
    
    id: Mapped[str] = mapped_column(String, primary_key=True)
    model_version: Mapped[str] = mapped_column(String, unique=True, index=True)
    feature_version_id: Mapped[str] = mapped_column(String, index=True)
    mlflow_run_id: Mapped[str] = mapped_column(String, index=True)
    model_path: Mapped[str] = mapped_column(String)
    metrics: Mapped[dict] = mapped_column(JSON)
    activated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, index=True)

class OfflineFeature(Base):
    __tablename__ = "offline_features"
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    feature_version_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    session_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    session_duration_avg: Mapped[float | None] = mapped_column(Double, nullable=True)
    page_views_per_session: Mapped[float | None] = mapped_column(Double, nullable=True)
    total_spend: Mapped[float | None] = mapped_column(Double, nullable=True)
    purchase_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    avg_order_value: Mapped[float | None] = mapped_column(Double, nullable=True)
    days_since_last_purchase: Mapped[int | None] = mapped_column(Integer, nullable=True)
    days_since_last_event: Mapped[int | None] = mapped_column(Integer, nullable=True)
    days_since_first_event: Mapped[int | None] = mapped_column(Integer, nullable=True)
    top_category_1: Mapped[str | None] = mapped_column(String, nullable=True)
    top_category_2: Mapped[str | None] = mapped_column(String, nullable=True)
    top_brand_1: Mapped[str | None] = mapped_column(String, nullable=True)
    top_brand_2: Mapped[str | None] = mapped_column(String, nullable=True)
    price_range_min: Mapped[float | None] = mapped_column(Double, nullable=True)
    price_range_max: Mapped[float | None] = mapped_column(Double, nullable=True)
    price_range_avg: Mapped[float | None] = mapped_column(Double, nullable=True)
    purchased: Mapped[int] = mapped_column(Integer, nullable=False)
    used_for_training: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    training_run_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())

class TrainingDatasetSplit(Base):
    __tablename__ = "training_dataset_splits"
    
    id: Mapped[str] = mapped_column(String, primary_key=True)
    training_run_id: Mapped[str] = mapped_column(String, unique=True, index=True)
    feature_version_id: Mapped[str] = mapped_column(String, index=True)
    train_user_ids: Mapped[list[int]] = mapped_column(JSON, nullable=False)  # Store as JSON array
    val_user_ids: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    test_user_ids: Mapped[list[int]] = mapped_column(JSON, nullable=False)
    train_split: Mapped[float] = mapped_column(Double, nullable=False)
    val_split: Mapped[float] = mapped_column(Double, nullable=False)
    test_split: Mapped[float] = mapped_column(Double, nullable=False)
    random_seed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())