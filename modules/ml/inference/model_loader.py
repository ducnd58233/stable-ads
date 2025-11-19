import logging
import tempfile
from pathlib import Path
import torch
from core.infra.blob.registry import get_blob
from core.infra.blob.buckets import Buckets
from modules.ml.models.purchase_model import PurchasePredictionModel
from modules.feature_store.repository import FeatureStoreRepository
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

logger = logging.getLogger(__name__)

class ModelLoader:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._sf = session_factory
        self._repo = FeatureStoreRepository()
        self._cached_model: PurchasePredictionModel | None = None
        self._cached_model_version: str | None = None
        self._cached_model_path: str | None = None
    
    async def load_production_model(
        self,
        model_version: str | None = None,
    ) -> tuple[PurchasePredictionModel, str, str]:
        """
        Load production model from blob storage.
        
        Args:
            model_version: Specific model version to load. If None, loads latest production model.
        
        Returns:
            Tuple of (model, model_version, model_path)
        """
        model_path: str | None = None
        
        async with self._sf() as s:
            if model_version is None:
                prod_model = await self._repo.get_production_model(s)
                if prod_model:
                    model_version = prod_model.model_version
                    model_path = prod_model.model_path
                    logger.info("Using production model: %s", model_version)
                else:
                    training_run = await self._repo.get_latest_completed_training_run(s)
                    if not training_run or not training_run.model_path:
                        raise ValueError(
                            "No production model or completed training run found. "
                            "Please train a model first or specify a model_version."
                        )
                    model_version = training_run.model_version
                    model_path = training_run.model_path
                    logger.info("Using latest completed training run: %s", model_version)
            else:
                prod_model = await self._repo.get_production_model(s)
                if prod_model and prod_model.model_version == model_version:
                    model_path = prod_model.model_path
                    logger.info("Found production model: %s", model_version)
                else:
                    # Try training run
                    training_run = await self._repo.get_training_run_by_model_version(s, model_version)
                    if not training_run or not training_run.model_path:
                        raise ValueError(
                            f"Model version {model_version} not found or has no model_path. "
                            "Please check the model version or train a new model."
                        )
                    model_path = training_run.model_path
                    logger.info("Found training run: %s", model_version)
        
        if not model_path:
            raise ValueError(f"Model path not found for version {model_version}")
        
        if (
            self._cached_model is not None
            and self._cached_model_version == model_version
            and self._cached_model_path == model_path
        ):
            logger.debug("Using cached model: %s", model_version)
            return self._cached_model, self._cached_model_version, self._cached_model_path
        
        logger.info("Loading model from blob storage: %s", model_path)
        blob = get_blob()
        await blob.start()
        
        try:
            bucket = Buckets.ML_MODELS
            key = model_path
            
            model_data = await blob.get_object(bucket, key)
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pt") as tmp_file:
                tmp_path = Path(tmp_file.name)
                tmp_path.write_bytes(model_data)
            
            try:
                checkpoint = torch.load(tmp_path, map_location="cpu")
                
                model_config = checkpoint.get("model_config", {})
                input_dim = model_config.get("input_dim")
                if input_dim is None:
                    raise ValueError("Model config missing input_dim")
                
                model = PurchasePredictionModel(input_dim=input_dim)
                model.load_state_dict(checkpoint["model_state_dict"])
                model.eval()
                
                self._cached_model = model
                self._cached_model_version = model_version
                self._cached_model_path = model_path
                
                logger.info("Successfully loaded model: %s", model_version)
                return model, model_version, model_path
                
            finally:
                if tmp_path.exists():
                    tmp_path.unlink()
                    
        except Exception as e:
            logger.error("Failed to load model %s: %s", model_version, str(e), exc_info=True)
            raise
        finally:
            await blob.stop()
    
    def clear_cache(self) -> None:
        """Clear the cached model."""
        self._cached_model = None
        self._cached_model_version = None
        self._cached_model_path = None
        logger.debug("Model cache cleared")

