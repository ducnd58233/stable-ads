import logging
import math
import torch
from modules.ml.inference.model_loader import ModelLoader
from modules.feature_store.dto import UserFeatureDTO
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

logger = logging.getLogger(__name__)

class PurchasePredictor:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._sf = session_factory
        self._model_loader = ModelLoader(session_factory)
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
    
    def _encode_categorical(self, value: str | None) -> int:
        if value is None:
            return 0
        if not isinstance(value, str):
            return 0
        if not value:
            return 0
        return abs(hash(value)) % (2**31)
    
    def _safe_float(self, value: any) -> float:
        if value is None:
            return 0.0
        try:
            val = float(value)
            if math.isnan(val) or math.isinf(val):
                return 0.0
            return val
        except (ValueError, TypeError):
            return 0.0
    
    def _preprocess_features(
        self,
        feature_dto: UserFeatureDTO,
        feature_cols: list[str],
    ) -> torch.Tensor:
        """
        Preprocess user features to match training pipeline.
        
        This must match the preprocessing in PurchaseDataset.
        """
        feature_dict = feature_dto.model_dump(exclude={"user_id", "session_id", "purchased", "feature_version_id"})
        
        processed_features = []
        for col in feature_cols:
            if col.endswith("_encoded"):
                original_col = col.replace("_encoded", "")
                value = feature_dict.get(original_col)
                encoded = self._encode_categorical(value)
                processed_features.append(float(encoded))
            else:
                value = feature_dict.get(col)
                processed_features.append(self._safe_float(value))
        
        tensor = torch.tensor([processed_features], dtype=torch.float32)
        return tensor
    
    def _get_feature_columns(self) -> list[str]:
        """
        Get the expected feature columns in the same order as training.
        
        This should match the order used in PurchaseDataset.
        """
        return [
            "session_count",
            "session_duration_avg",
            "page_views_per_session",
            "total_spend",
            "purchase_count",
            "avg_order_value",
            "days_since_last_purchase",
            "days_since_last_event",
            "days_since_first_event",
            "top_category_1_encoded",  # Categorical encoded
            "top_category_2_encoded",  # Categorical encoded
            "top_brand_1_encoded",     # Categorical encoded
            "top_brand_2_encoded",     # Categorical encoded
            "price_range_min",
            "price_range_max",
            "price_range_avg",
        ]
    
    async def predict(
        self,
        feature_dto: UserFeatureDTO,
        model_version: str | None = None,
    ) -> tuple[float, str, str]:
        """
        Predict purchase probability for a user.
        
        Args:
            feature_dto: User features
            model_version: Specific model version to use. If None, uses latest production model.
        
        Returns:
            Tuple of (probability, model_version, model_path)
        """
        model, model_version, model_path = await self._model_loader.load_production_model(model_version)
        model = model.to(self._device)
        
        feature_cols = self._get_feature_columns()
        
        feature_tensor = self._preprocess_features(feature_dto, feature_cols)
        feature_tensor = feature_tensor.to(self._device)
        
        model.eval()
        with torch.no_grad():
            probability = model(feature_tensor).item()
        
        # Ensure probability is in [0, 1] range
        probability = max(0.0, min(1.0, probability))
        
        logger.debug(
            "Prediction for user %s: probability=%.4f, model=%s",
            feature_dto.user_id,
            probability,
            model_version,
        )
        
        return probability, model_version, model_path

