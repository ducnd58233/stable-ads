from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, text, func, delete
from .model import FeatureVersion, MLTrainingRun, ProductionModel, OfflineFeature, TrainingDatasetSplit
from .domain import FeatureVersionStatus, ModelStatus
from datetime import datetime

class FeatureStoreRepository:
    async def create_feature_version(
        self, s: AsyncSession, feature_version: FeatureVersion
    ) -> FeatureVersion:
        s.add(feature_version)
        await s.flush()
        s.expunge(feature_version)
        return feature_version
    
    async def get_feature_version(
        self, s: AsyncSession, version_id: str
    ) -> FeatureVersion | None:
        return await s.get(FeatureVersion, version_id)
    
    async def get_latest_completed_feature_version(
        self,
        s: AsyncSession,
    ) -> FeatureVersion | None:
        stmt = (
            select(FeatureVersion)
            .where(FeatureVersion.status == FeatureVersionStatus.COMPLETED.value)
            .order_by(FeatureVersion.created_at.desc())
            .limit(1)
        )
        result = await s.execute(stmt)
        return result.scalar_one_or_none()
    
    async def update_feature_version_status(
        self,
        s: AsyncSession,
        version_id: str,
        status: FeatureVersionStatus,
        *,
        total_users: int | None = None,
        total_records: int | None = None,
        checkpoint_path: str | None = None,
        error_message: str | None = None,
    ) -> None:
        values = {"status": status.value}
        if total_users is not None:
            values["total_users"] = total_users
        if total_records is not None:
            values["total_records"] = total_records
        if checkpoint_path is not None:
            values["checkpoint_path"] = checkpoint_path
        if error_message is not None:
            values["error_message"] = error_message
        if status == FeatureVersionStatus.COMPLETED:
            values["completed_at"] = text("NOW()")
        
        stmt = (
            update(FeatureVersion)
            .where(FeatureVersion.id == version_id)
            .values(**values)
        )
        await s.execute(stmt)
    
    async def get_offline_features(
        self,
        s: AsyncSession,
        feature_version_id: str | None = None,
    ) -> list[OfflineFeature]:
        stmt = select(OfflineFeature)
        if feature_version_id:
            stmt = stmt.where(OfflineFeature.feature_version_id == feature_version_id)
        stmt = stmt.order_by(OfflineFeature.user_id)
        
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def bulk_insert_offline_features(
        self,
        s: AsyncSession,
        features: list[OfflineFeature],
    ) -> int:
        if not features:
            return 0
        
        s.add_all(features)
        await s.flush()
        return len(features)
    
    async def create_training_run(
        self, s: AsyncSession, training_run: MLTrainingRun
    ) -> MLTrainingRun:
        s.add(training_run)
        await s.flush()
        s.expunge(training_run)
        return training_run
    
    async def get_production_model(
        self, s: AsyncSession
    ) -> ProductionModel | None:
        stmt = (
            select(ProductionModel)
            .where(ProductionModel.is_active == True)
            .order_by(ProductionModel.activated_at.desc())
            .limit(1)
        )
        result = await s.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_training_run_by_model_version(
        self, s: AsyncSession, model_version: str
    ) -> MLTrainingRun | None:
        stmt = (
            select(MLTrainingRun)
            .where(MLTrainingRun.model_version == model_version)
            .limit(1)
        )
        result = await s.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_latest_completed_training_run(
        self, s: AsyncSession
    ) -> MLTrainingRun | None:
        stmt = (
            select(MLTrainingRun)
            .where(
                MLTrainingRun.model_path.isnot(None),
                MLTrainingRun.completed_at.isnot(None),
                MLTrainingRun.status != ModelStatus.TRAINING.value,
            )
            .order_by(MLTrainingRun.completed_at.desc())
            .limit(1)
        )
        result = await s.execute(stmt)
        return result.scalar_one_or_none()
    
    async def set_production_model(
        self,
        s: AsyncSession,
        model_version: str,
        feature_version_id: str,
        mlflow_run_id: str,
        model_path: str,
        metrics: dict,
    ) -> None:
        await s.execute(
            update(ProductionModel)
            .values(is_active=False)
        )
        
        prod_model = ProductionModel(
            id=f"prod_{model_version}",
            model_version=model_version,
            feature_version_id=feature_version_id,
            mlflow_run_id=mlflow_run_id,
            model_path=model_path,
            metrics=metrics,
            is_active=True,
        )
        s.add(prod_model)
        await s.flush()

    async def mark_features_as_used_for_training(
        self,
        s: AsyncSession,
        feature_version_id: str,
        training_run_id: str,
    ) -> int:
        stmt = (
            update(OfflineFeature)
            .where(OfflineFeature.feature_version_id == feature_version_id)
            .values(
                used_for_training=True,
                training_run_id=training_run_id,
            )
        )
        result = await s.execute(stmt)
        return result.rowcount

    async def get_incomplete_feature_version_for_window(
        self,
        s: AsyncSession,
        feature_window_start: datetime,
        feature_window_end: datetime,
    ) -> FeatureVersion | None:
        """Get incomplete feature version for the same window to resume from."""
        stmt = (
            select(FeatureVersion)
            .where(
                FeatureVersion.feature_window_start == feature_window_start,
                FeatureVersion.feature_window_end == feature_window_end,
                FeatureVersion.status.in_([
                    FeatureVersionStatus.PENDING.value,
                    FeatureVersionStatus.COMPUTING.value,
                ])
            )
            .order_by(FeatureVersion.created_at.desc())
            .limit(1)
        )
        result = await s.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_completed_feature_version_for_window(
        self,
        s: AsyncSession,
        feature_window_start: datetime,
        feature_window_end: datetime,
    ) -> FeatureVersion | None:
        """Check if feature version already exists and is completed for this window."""
        stmt = (
            select(FeatureVersion)
            .where(
                FeatureVersion.feature_window_start == feature_window_start,
                FeatureVersion.feature_window_end == feature_window_end,
                FeatureVersion.status == FeatureVersionStatus.COMPLETED.value,
            )
            .order_by(FeatureVersion.created_at.desc())
            .limit(1)
        )
        result = await s.execute(stmt)
        return result.scalar_one_or_none()
    
    async def count_offline_features_for_version(
        self,
        s: AsyncSession,
        feature_version_id: str,
    ) -> int:
        """Count how many features have been inserted for a version."""
        stmt = (
            select(func.count(OfflineFeature.id))
            .where(OfflineFeature.feature_version_id == feature_version_id)
        )
        result = await s.execute(stmt)
        return result.scalar_one() or 0
    
    async def delete_offline_features_for_version(
        self,
        s: AsyncSession,
        feature_version_id: str,
    ) -> int:
        """Delete all offline features for a version (for cleanup)."""
        stmt = (
            delete(OfflineFeature)
            .where(OfflineFeature.feature_version_id == feature_version_id)
        )
        result = await s.execute(stmt)
        return result.rowcount

    async def get_latest_incomplete_feature_version(
        self,
        s: AsyncSession,
    ) -> FeatureVersion | None:
        stmt = (
            select(FeatureVersion)
            .where(
                FeatureVersion.status.in_([
                    FeatureVersionStatus.PENDING.value,
                    FeatureVersionStatus.COMPUTING.value,
                ])
            )
            .order_by(FeatureVersion.created_at.desc())
            .limit(1)
        )
        result = await s.execute(stmt)
        return result.scalar_one_or_none()

    async def get_existing_user_ids_for_version(
        self,
        s: AsyncSession,
        feature_version_id: str,
    ) -> set[int]:
        stmt = (
            select(OfflineFeature.user_id)
            .where(OfflineFeature.feature_version_id == feature_version_id)
            .distinct()
        )
        result = await s.execute(stmt)
        return set(result.scalars().all())

    async def create_training_dataset_split(
        self,
        s: AsyncSession,
        split: TrainingDatasetSplit,
    ) -> TrainingDatasetSplit:
        s.add(split)
        await s.flush()
        s.expunge(split)
        return split
    
    async def get_training_dataset_split(
        self,
        s: AsyncSession,
        training_run_id: str,
    ) -> TrainingDatasetSplit | None:
        return await s.get(TrainingDatasetSplit, training_run_id)
    
    async def get_training_dataset_split_by_feature_version(
        self,
        s: AsyncSession,
        feature_version_id: str,
        latest_only: bool = True,
    ) -> TrainingDatasetSplit | None:
        stmt = (
            select(TrainingDatasetSplit)
            .where(TrainingDatasetSplit.feature_version_id == feature_version_id)
        )
        if latest_only:
            stmt = stmt.order_by(TrainingDatasetSplit.created_at.desc()).limit(1)
        result = await s.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_offline_feature_by_user_id(
        self,
        s: AsyncSession,
        user_id: int,
        feature_version_id: str | None = None,
    ) -> OfflineFeature | None:
        stmt = select(OfflineFeature).where(OfflineFeature.user_id == user_id)
        if feature_version_id:
            stmt = stmt.where(OfflineFeature.feature_version_id == feature_version_id)
        stmt = stmt.order_by(OfflineFeature.created_at.desc()).limit(1)
        result = await s.execute(stmt)
        return result.scalar_one_or_none()