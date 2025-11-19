from pydantic import BaseModel, Field

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

class PurchasePredictionDTO(BaseModel):
    user_id: int | None = Field(None, description="User identifier (None for anonymous users)")
    session_id: str | None = Field(None, description="Session identifier (for anonymous users)")
    purchase_probability: float = Field(..., ge=0.0, le=1.0, description="Predicted purchase probability")
    will_purchase: bool = Field(..., description="Whether user is predicted to purchase (based on threshold)")
    model_version: str = Field(..., description="Model version used for prediction")
    feature_version_id: str = Field(..., description="Feature version used for prediction")
    model_path: str = Field(..., description="Path to model used for prediction")