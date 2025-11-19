import json
import uuid
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from core.infra.blob.registry import get_blob
from core.infra.blob.buckets import Buckets
from core.infra.cache.registry import get_cache
from .repository import FeatureStoreRepository
from .model import FeatureVersion, OfflineFeature
from .domain import FeatureVersionStatus
from .dto import FeatureMaterializationResultDTO, UserFeatureDTO
from .purchase_features import PurchaseFeatureBuilder
from modules.warehouse.repository import WarehouseDataRepository
import logging
import asyncio
import pendulum

logger = logging.getLogger(__name__)

FEATURE_BATCH_INSERT_SIZE = 5000
FEATURE_LOOKBACK_DAYS = 30
LABEL_HORIZON_DAYS = 7

class FeatureStoreService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory
        self._repo = FeatureStoreRepository()
        self._warehouse_repo = WarehouseDataRepository()
    
    async def determine_feature_window(
        self,
        lookback_days: int = 30,
        fallback_to_current_time: bool = True,
        check_incomplete_first: bool = True,
    ) -> tuple[datetime, datetime, FeatureVersion | None]:
        """
        Determine the feature window based on actual data in warehouse.
        Returns (feature_window_start, feature_window_end, incomplete_version_if_found).
        
        Args:
            lookback_days: Number of days to look back from the max event_time
            fallback_to_current_time: If True, use current time if no data found
            check_incomplete_first: If True, check for incomplete versions first and use their window
        
        Returns:
            Tuple of (feature_window_start, feature_window_end, incomplete_version) as timezone-aware datetimes
        """
        incomplete_version = None
        
        if check_incomplete_first:
            async with self._sf() as s:
                incomplete_version = await self._repo.get_latest_incomplete_feature_version(s)
            
            if incomplete_version:
                logger.info(
                    "Found incomplete feature version: %s with window %s to %s. Using its window for resume.",
                    incomplete_version.version,
                    incomplete_version.feature_window_start,
                    incomplete_version.feature_window_end,
                )
                return (
                    incomplete_version.feature_window_start,
                    incomplete_version.feature_window_end,
                    incomplete_version,
                )
        
        async with self._sf() as s:
            min_time, max_time = await self._warehouse_repo.get_event_time_range(s)
        
        if max_time is not None:
            if isinstance(max_time, str):
                feature_window_end = pendulum.parse(max_time)
            else:
                feature_window_end = pendulum.instance(max_time)
            
            if not feature_window_end.timezone:
                feature_window_end = feature_window_end.in_timezone("UTC")
            
            label_horizon_days = LABEL_HORIZON_DAYS
            adjusted_end = feature_window_end - timedelta(days=label_horizon_days)
            
            if adjusted_end > feature_window_start:
                feature_window_end = adjusted_end
                logger.info(
                    "Adjusted feature_window_end from %s to %s to account for %s-day label horizon",
                    max_time,
                    feature_window_end,
                    label_horizon_days,
                )
            
            feature_window_start = feature_window_end - timedelta(days=lookback_days)
            
            logger.info(
                "Using data-driven feature window: %s to %s (based on actual event_time range)",
                feature_window_start,
                feature_window_end,
            )
            
            return (feature_window_start, feature_window_end, None)
        
        # Fallback: Use current time
        if fallback_to_current_time:
            feature_window_end = pendulum.now("UTC")
            feature_window_start = feature_window_end - timedelta(days=lookback_days)
            
            logger.info(
                "Using execution-time feature window: %s to %s (no data found in warehouse)",
                feature_window_start,
                feature_window_end,
            )
            
            return (feature_window_start, feature_window_end, None)
        
        raise ValueError("No data found in warehouse and fallback_to_current_time is False")
    
    async def materialize_features(
        self,
        feature_window_start: datetime,
        feature_window_end: datetime,
        label_horizon_days: int = 7,
        resume: bool = True,
    ) -> FeatureMaterializationResultDTO:
        if feature_window_start >= feature_window_end:
            raise ValueError(
                f"Invalid feature window: start ({feature_window_start}) must be before end ({feature_window_end})"
            )
        
        if feature_window_start.tzinfo is None:
            raise ValueError("feature_window_start must be timezone-aware")
        if feature_window_end.tzinfo is None:
            raise ValueError("feature_window_end must be timezone-aware")
        
        version_id: str | None = None
        version_str: str | None = None
        feature_version: FeatureVersion | None = None
        
        try:
            async with self._sf() as s:
                if resume:
                    existing_completed = await self._repo.get_completed_feature_version_for_window(
                        s, feature_window_start, feature_window_end
                    )
                    if existing_completed:
                        logger.info(
                            "Feature version already completed for window %s to %s: %s",
                            feature_window_start,
                            feature_window_end,
                            existing_completed.version,
                        )
                        return FeatureMaterializationResultDTO(
                            feature_version_id=existing_completed.id,
                            version=existing_completed.version,
                            total_users=existing_completed.total_users or 0,
                            total_records=existing_completed.total_records or 0,
                            feature_window_start=feature_window_start,
                            feature_window_end=feature_window_end,
                        )
                    
                    incomplete_version = await self._repo.get_incomplete_feature_version_for_window(
                        s, feature_window_start, feature_window_end
                    )
                    
                    if not incomplete_version:
                        incomplete_version = await self._repo.get_latest_incomplete_feature_version(s)
                        if incomplete_version:
                            logger.warning(
                                "Found incomplete version %s with different window (%s to %s). "
                                "Using provided window (%s to %s) instead.",
                                incomplete_version.version,
                                incomplete_version.feature_window_start,
                                incomplete_version.feature_window_end,
                                feature_window_start,
                                feature_window_end,
                            )
                    
                    if incomplete_version:
                        logger.info(
                            "Resuming incomplete feature version: %s (status: %s)",
                            incomplete_version.version,
                            incomplete_version.status,
                        )
                        feature_version = incomplete_version
                        version_id = incomplete_version.id
                        version_str = incomplete_version.version
                        
                        if incomplete_version.feature_window_start and incomplete_version.feature_window_end:
                            feature_window_start = incomplete_version.feature_window_start
                            feature_window_end = incomplete_version.feature_window_end
                            logger.info(
                                "Using incomplete version's window: %s to %s",
                                feature_window_start,
                                feature_window_end,
                            )
                        
                        existing_count = await self._repo.count_offline_features_for_version(
                            s, version_id
                        )
                        if existing_count > 0:
                            logger.info(
                                "Found %s existing features, will skip to resume",
                                existing_count,
                            )
                else:
                    incomplete_version = await self._repo.get_incomplete_feature_version_for_window(
                        s, feature_window_start, feature_window_end
                    )
                    if incomplete_version:
                        logger.warning(
                            "Incomplete version exists but resume=False. Cleaning up: %s",
                            incomplete_version.id,
                        )
                        await self._repo.delete_offline_features_for_version(
                            s, incomplete_version.id
                        )
                        async with s.begin():
                            await self._repo.update_feature_version_status(
                                s,
                                incomplete_version.id,
                                FeatureVersionStatus.FAILED,
                                error_message="Replaced by new run",
                            )
            
            if not feature_version:
                version_id = str(uuid.uuid4())
                short_uuid = str(uuid.uuid4())[:8]
                version_str = f"v{feature_window_end.strftime('%Y%m%d_%H%M%S')}_{short_uuid}"
                
                feature_version = FeatureVersion(
                    id=version_id,
                    version=version_str,
                    status=FeatureVersionStatus.PENDING.value,
                    feature_window_start=feature_window_start,
                    feature_window_end=feature_window_end,
                    label_horizon_days=label_horizon_days,
                )
                
                async with self._sf() as s, s.begin():
                    await self._repo.create_feature_version(s, feature_version)
            
            async with self._sf() as s:
                existing_user_ids = await self._repo.get_existing_user_ids_for_version(
                    s, version_id
                )
                existing_count = len(existing_user_ids)
                
                if existing_count > 0:
                    logger.info(
                        "Found %s existing features for %s users, will skip those users",
                        existing_count,
                        len(existing_user_ids),
                    )
                
                builder = PurchaseFeatureBuilder(s)
                features_dto_list: list[UserFeatureDTO] = []
                offline_features_batch: list[OfflineFeature] = []
                total_inserted = 0
                features_yielded = 0
                features_skipped = 0
                
                async for feature_dto in builder.build_features(
                    feature_window_start,
                    feature_window_end,
                    label_horizon_days,
                ):
                    features_yielded += 1
                    
                    if feature_dto.user_id in existing_user_ids:
                        features_skipped += 1
                        logger.debug(
                            "Skipping feature for user %s (already exists)",
                            feature_dto.user_id,
                        )
                        continue
                    
                    features_dto_list.append(feature_dto)
                    
                    offline_feature = OfflineFeature(
                        feature_version_id=version_id,
                        user_id=feature_dto.user_id,
                        session_count=feature_dto.session_count,
                        session_duration_avg=feature_dto.session_duration_avg,
                        page_views_per_session=feature_dto.page_views_per_session,
                        total_spend=feature_dto.total_spend,
                        purchase_count=feature_dto.purchase_count,
                        avg_order_value=feature_dto.avg_order_value,
                        days_since_last_purchase=feature_dto.days_since_last_purchase,
                        days_since_last_event=feature_dto.days_since_last_event,
                        days_since_first_event=feature_dto.days_since_first_event,
                        top_category_1=feature_dto.top_category_1,
                        top_category_2=feature_dto.top_category_2,
                        top_brand_1=feature_dto.top_brand_1,
                        top_brand_2=feature_dto.top_brand_2,
                        price_range_min=feature_dto.price_range_min,
                        price_range_max=feature_dto.price_range_max,
                        price_range_avg=feature_dto.price_range_avg,
                        purchased=feature_dto.purchased,
                    )
                    offline_features_batch.append(offline_feature)
                    
                    if len(offline_features_batch) >= FEATURE_BATCH_INSERT_SIZE:
                        async with self._sf() as s, s.begin():
                            await self._repo.update_feature_version_status(
                                s,
                                version_id,
                                FeatureVersionStatus.COMPUTING,
                            )
                            batch_count = await self._repo.bulk_insert_offline_features(
                                s, offline_features_batch
                            )
                            total_inserted += batch_count
                            logger.info(
                                "Inserted batch of %s features (total inserted this run: %s, skipped: %s, yielded: %s)",
                                batch_count,
                                total_inserted,
                                features_skipped,
                                features_yielded,
                            )
                        offline_features_batch.clear()
                
                if offline_features_batch:
                    async with self._sf() as s, s.begin():
                        await self._repo.update_feature_version_status(
                            s,
                            version_id,
                            FeatureVersionStatus.COMPUTING,
                        )
                        batch_count = await self._repo.bulk_insert_offline_features(
                            s, offline_features_batch
                        )
                        total_inserted += batch_count
                        logger.info(
                            "Inserted final batch of %s features (total inserted this run: %s, skipped: %s, yielded: %s)",
                            batch_count,
                            total_inserted,
                            features_skipped,
                            features_yielded,
                        )
                
                total_features = existing_count + total_inserted
                
                if features_yielded == 0:
                    logger.warning(
                        "No features yielded from builder for window %s to %s. "
                        "This may indicate no data matches the window or a query issue.",
                        feature_window_start,
                        feature_window_end,
                    )
                    raise ValueError("No features generated")
                
                if total_inserted == 0 and existing_count > 0:
                    logger.info(
                        "No new features to insert (all %s features already exist)",
                        existing_count,
                    )
                
                logger.info(
                    "Feature materialization complete: %s total features (%s existing + %s new), %s skipped, %s yielded",
                    total_features,
                    existing_count,
                    total_inserted,
                    features_skipped,
                    features_yielded,
                )
                
                checkpoint_key: str | None = None
                blob = get_blob()
                try:
                    await blob.start()
                    checkpoint_key = f"features/checkpoints/{version_str}.json"
                    checkpoint_data = json.dumps({
                        "version_id": version_id,
                        "version": version_str,
                        "feature_window_start": feature_window_start.isoformat(),
                        "feature_window_end": feature_window_end.isoformat(),
                        "total_users": total_features,
                        "total_records": total_features,
                    }).encode("utf-8")
                    
                    await blob.upload(
                        Buckets.TRANSFORMED_DATA,
                        checkpoint_key,
                        checkpoint_data,
                        content_type="application/json",
                    )
                    logger.info("Checkpoint uploaded to %s", checkpoint_key)
                except Exception as blob_error:
                    logger.warning(
                        "Failed to upload checkpoint (non-critical): %s. Status will still be marked as COMPLETED.",
                        str(blob_error),
                    )
                    checkpoint_key = None
                finally:
                    try:
                        await blob.stop()
                    except Exception:
                        pass
                
                async with self._sf() as s, s.begin():
                    await self._repo.update_feature_version_status(
                        s,
                        version_id,
                        FeatureVersionStatus.COMPLETED,
                        total_users=total_features,
                        total_records=total_features,
                        checkpoint_path=checkpoint_key,
                    )
                
                logger.info(
                    "Feature version %s marked as COMPLETED with %s total features",
                    version_str,
                    total_features,
                )
            
            return FeatureMaterializationResultDTO(
                feature_version_id=version_id,
                version=version_str,
                total_users=total_features,
                total_records=total_features,
                feature_window_start=feature_window_start,
                feature_window_end=feature_window_end,
            )
        
        except Exception as e:
            if version_id:
                try:
                    async with self._sf() as s, s.begin():
                        await self._repo.update_feature_version_status(
                            s,
                            version_id,
                            FeatureVersionStatus.FAILED,
                            error_message=f"Materialization failed: {str(e)[:500]}",
                        )
                    logger.error(
                        "Feature materialization failed for version %s: %s",
                        version_id,
                        str(e),
                        exc_info=True,
                    )
                except Exception as cleanup_error:
                    logger.error(
                        "Failed to update feature version status to FAILED: %s",
                        cleanup_error,
                        exc_info=True,
                    )
            raise
    
    async def publish_online_features(
        self, feature_version_id: str
    ) -> None:
        async with self._sf() as s:
            offline_features = await self._repo.get_offline_features(s, feature_version_id)
            
            if not offline_features:
                return
            
            cache = get_cache("redis")
            await cache.start()
            try:
                batch_size = 100
                for i in range(0, len(offline_features), batch_size):
                    batch = offline_features[i:i + batch_size]
                    tasks = []
                    for feature in batch:
                        key = f"features:user:{feature.user_id}"
                        feature_dict = {
                            "user_id": feature.user_id,
                            "session_count": feature.session_count,
                            "session_duration_avg": feature.session_duration_avg,
                            "page_views_per_session": feature.page_views_per_session,
                            "total_spend": feature.total_spend,
                            "purchase_count": feature.purchase_count,
                            "avg_order_value": feature.avg_order_value,
                            "days_since_last_purchase": feature.days_since_last_purchase,
                            "days_since_last_event": feature.days_since_last_event,
                            "days_since_first_event": feature.days_since_first_event,
                            "top_category_1": feature.top_category_1,
                            "top_category_2": feature.top_category_2,
                            "top_brand_1": feature.top_brand_1,
                            "top_brand_2": feature.top_brand_2,
                            "price_range_min": feature.price_range_min,
                            "price_range_max": feature.price_range_max,
                            "price_range_avg": feature.price_range_avg,
                            "feature_version_id": feature.feature_version_id,
                        }
                        tasks.append(cache.set_json(key, feature_dict, ttl=86400))
                    
                    await asyncio.gather(*tasks)
                    
                logger.info("Published %s features to online store", len(offline_features))
            except Exception as e:
                logger.error(f"Error publishing features to cache: {e}")
                raise
            finally:
                await cache.stop()