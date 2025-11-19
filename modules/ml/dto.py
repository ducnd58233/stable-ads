from pydantic import BaseModel, Field
from typing import Any

class TrainingResultDTO(BaseModel):
    model_version: str = Field(..., description="Model version identifier")
    feature_version_id: str = Field(..., description="Feature version used for training")
    training_run_id: str = Field(..., description="Training run ID")
    metrics: dict[str, float] = Field(..., description="Model evaluation metrics")
    model_path: str = Field(..., description="Path to saved model")
    mlflow_run_id: str = Field(..., description="MLflow run ID")
    features_marked: int = Field(..., ge=0, description="Number of features marked as used")
    train_size: int = Field(..., ge=0, description="Training dataset size")
    val_size: int = Field(..., ge=0, description="Validation dataset size")
    test_size: int = Field(..., ge=0, description="Test dataset size")
