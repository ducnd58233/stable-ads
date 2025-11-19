import logging
import uuid
from datetime import datetime
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from torch.utils.data import DataLoader
import mlflow
from core.settings.config import get_settings
from core.infra.blob.registry import get_blob
from core.infra.blob.buckets import Buckets
from modules.ml.datasets.purchase import PurchaseDataLoader
from modules.ml.models.purchase_model import PurchasePredictionModel
from modules.ml.training.purchase_trainer import PurchaseTrainer
from modules.feature_store.repository import FeatureStoreRepository
from modules.feature_store.model import MLTrainingRun
from modules.feature_store.domain import ModelStatus
from modules.ml.dto import TrainingResultDTO

logger = logging.getLogger(__name__)

class MLService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory
        self._repo = FeatureStoreRepository()
        self._settings = get_settings()
    
    async def train_model(
        self,
        feature_version_id: str | None = None,
        epochs: int = 10,
        batch_size: int = 64,
        reuse_training_run_id: str | None = None,
        train_split: float = 0.7,
        val_split: float = 0.15,
        test_split: float = 0.15,
        random_seed: int = 42,
    ) -> TrainingResultDTO:
        async with self._sf() as s:
            if feature_version_id:
                feature_version = await self._repo.get_feature_version(s, feature_version_id)
            else:
                feature_version = await self._repo.get_latest_completed_feature_version(s)
        
        if not feature_version:
            raise ValueError("No completed feature version found")
        
        logger.info("Using feature version: %s", feature_version.version)
        
        data_loader = PurchaseDataLoader(self._sf)
        train_ds, val_ds, test_ds = await data_loader.load_features(
            feature_version_id=feature_version.id,
            train_split=train_split,
            val_split=val_split,
            test_split=test_split,
            random_seed=random_seed,
            reuse_training_run_id=reuse_training_run_id,
        )
        
        train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_ds, batch_size=batch_size, shuffle=False)
        test_loader = DataLoader(test_ds, batch_size=batch_size, shuffle=False)
        
        input_dim = len(train_ds.feature_cols)
        model = PurchasePredictionModel(input_dim=input_dim)
        
        model_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        training_run_id = str(uuid.uuid4())
        
        trainer = PurchaseTrainer(model)
        history = trainer.train(train_loader, val_loader, epochs=epochs)
        
        test_metrics = trainer.evaluate(test_loader)
        logger.info("Test metrics: %s", test_metrics)
        
        model_path = await trainer.save_model(model_version, feature_version.id)
        
        mlflow_run_id: str | None = None
        try:
            mlflow.set_tracking_uri(self._settings.mlflow.tracking_uri)
            mlflow.set_experiment(self._settings.mlflow.experiment_name)
            
            with mlflow.start_run(run_name=f"purchase-prediction-{model_version}"):
                mlflow.log_params({
                    "feature_version_id": feature_version.id,
                    "feature_version": feature_version.version,
                    "epochs": epochs,
                    "batch_size": batch_size,
                    "input_dim": input_dim,
                    "train_size": len(train_ds),
                    "val_size": len(val_ds),
                    "test_size": len(test_ds),
                    "reused_training_run_id": reuse_training_run_id or "none",
                    "model_path": model_path,
                })
                
                mlflow.log_metrics(test_metrics)
                
                if test_metrics["auc"] > 0.7:
                    mlflow.set_tag("production", "true")
                
                mlflow_run_id = mlflow.active_run().info.run_id
                logger.info("MLflow tracking successful: run_id=%s", mlflow_run_id)
        except Exception as mlflow_error:
            logger.warning(
                "MLflow tracking failed (non-critical): %s. Continuing without MLflow tracking.",
                str(mlflow_error),
            )
        
        training_run = MLTrainingRun(
            id=training_run_id,
            feature_version_id=feature_version.id,
            model_version=model_version,
            mlflow_run_id=mlflow_run_id,
            status=ModelStatus.EVALUATING.value,
            train_size=len(train_ds),
            val_size=len(val_ds),
            test_size=len(test_ds),
            metrics=test_metrics,
            model_path=model_path,
        )
        
        async with self._sf() as s, s.begin():
            s.add(training_run)
            await s.flush()
            
            from modules.feature_store.model import TrainingDatasetSplit
            dataset_split = TrainingDatasetSplit(
                id=training_run_id,
                training_run_id=training_run_id,
                feature_version_id=feature_version.id,
                train_user_ids=[int(uid) for uid in train_ds.features_df["user_id"].tolist()],
                val_user_ids=[int(uid) for uid in val_ds.features_df["user_id"].tolist()],
                test_user_ids=[int(uid) for uid in test_ds.features_df["user_id"].tolist()],
                train_split=train_split,
                val_split=val_split,
                test_split=test_split,
                random_seed=random_seed if not reuse_training_run_id else None,
            )
            await self._repo.create_training_dataset_split(s, dataset_split)
            
            marked_count = await self._repo.mark_features_as_used_for_training(
                s,
                feature_version.id,
                training_run_id,
            )
            logger.info(
                "Marked %s features as used for training (run: %s)",
                marked_count,
                training_run_id,
            )
        
        if test_metrics["auc"] > 0.7:
            async with self._sf() as s, s.begin():
                await self._repo.set_production_model(
                    s,
                    model_version,
                    feature_version.id,
                    mlflow_run_id or "no-mlflow",
                    model_path,
                    test_metrics,
                )
            
            logger.info("Promoted model %s to production", model_version)
        
        return TrainingResultDTO(
            model_version=model_version,
            feature_version_id=feature_version.id,
            training_run_id=training_run_id,
            metrics=test_metrics,
            model_path=model_path,
            mlflow_run_id=mlflow_run_id or "",
            features_marked=marked_count,
            train_size=len(train_ds),
            val_size=len(val_ds),
            test_size=len(test_ds),
        )
        