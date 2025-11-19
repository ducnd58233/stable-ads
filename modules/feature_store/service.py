import json
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
                feature_window_end = datetime.fromisoformat(max_time.replace("Z", "+00:00"))
            else:
                feature_window_end = max_time
            
            if feature_window_end.tzinfo is None:
                feature_window_end = feature_window_end.replace(tzinfo=ZoneInfo("UTC"))
            elif feature_window_end.tzinfo != ZoneInfo("UTC"):
                feature_window_end = feature_window_end.astimezone(ZoneInfo("UTC"))
            
            label_horizon_days = LABEL_HORIZON_DAYS
            feature_window_end = feature_window_end - timedelta(days=label_horizon_days)
            logger.info(
                "Adjusted feature_window_end to %s to account for %s-day label horizon",
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
        
        if fallback_to_current_time:
            feature_window_end = datetime.now(ZoneInfo("UTC"))
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
            
            cache = get_cache()
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
    
    async def get_user_features_for_prediction(
        self,
        user_id: int,
    ) -> UserFeatureDTO | None:
        """
        Get user features for prediction, checking cache first, then database.
        
        Args:
            user_id: User identifier
        
        Returns:
            UserFeatureDTO if found, None otherwise
        """
        # Try cache first
        cache = get_cache()
        await cache.start()
        try:
            key = f"features:user:{user_id}"
            cached_data = await cache.get_json(key)
            
            if cached_data:
                logger.debug("Found user %s features in cache", user_id)
                return UserFeatureDTO(
                    user_id=cached_data["user_id"],
                    feature_version_id=cached_data.get("feature_version_id"),
                    session_count=cached_data.get("session_count"),
                    session_duration_avg=cached_data.get("session_duration_avg"),
                    page_views_per_session=cached_data.get("page_views_per_session"),
                    total_spend=cached_data.get("total_spend"),
                    purchase_count=cached_data.get("purchase_count"),
                    avg_order_value=cached_data.get("avg_order_value"),
                    days_since_last_purchase=cached_data.get("days_since_last_purchase"),
                    days_since_last_event=cached_data.get("days_since_last_event"),
                    days_since_first_event=cached_data.get("days_since_first_event"),
                    top_category_1=cached_data.get("top_category_1"),
                    top_category_2=cached_data.get("top_category_2"),
                    top_brand_1=cached_data.get("top_brand_1"),
                    top_brand_2=cached_data.get("top_brand_2"),
                    price_range_min=cached_data.get("price_range_min"),
                    price_range_max=cached_data.get("price_range_max"),
                    price_range_avg=cached_data.get("price_range_avg"),
                    purchased=0,
                )
        except Exception as e:
            logger.warning("Error reading from cache for user %s: %s", user_id, str(e))
        finally:
            await cache.stop()
        
        async with self._sf() as s:
            feature = await self._repo.get_offline_feature_by_user_id(s, user_id)
            
            if not feature:
                logger.debug("User %s not found in feature store", user_id)
                return None
            
            logger.debug("Found user %s features in database", user_id)
            return UserFeatureDTO(
                user_id=feature.user_id,
                feature_version_id=feature.feature_version_id,
                session_count=feature.session_count,
                session_duration_avg=feature.session_duration_avg,
                page_views_per_session=feature.page_views_per_session,
                total_spend=feature.total_spend,
                purchase_count=feature.purchase_count,
                avg_order_value=feature.avg_order_value,
                days_since_last_purchase=feature.days_since_last_purchase,
                days_since_last_event=feature.days_since_last_event,
                days_since_first_event=feature.days_since_first_event,
                top_category_1=feature.top_category_1,
                top_category_2=feature.top_category_2,
                top_brand_1=feature.top_brand_1,
                top_brand_2=feature.top_brand_2,
                price_range_min=feature.price_range_min,
                price_range_max=feature.price_range_max,
                price_range_avg=feature.price_range_avg,
                purchased=0,
            )
    
    async def compute_features_for_user(
        self,
        user_id: int,
        lookback_days: int = 30,
    ) -> UserFeatureDTO | None:
        """
        Compute features for a user on-the-fly from warehouse data.
        Used for new customers or when features are not in feature store.
        
        Args:
            user_id: User identifier
            lookback_days: Number of days to look back for events
        
        Returns:
            UserFeatureDTO if events found, None if no events exist
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from core.settings.config import get_settings
        from modules.feature_store.purchase_features import PurchaseFeatureBuilder
        
        settings = get_settings()
        cache_ttl = settings.ml_prediction.realtime_feature_cache_ttl_seconds
        
        # Check cache first
        cache = get_cache()
        await cache.start()
        try:
            cache_key = f"realtime_features:user:{user_id}:{lookback_days}"
            cached_data = await cache.get_json(cache_key)
            
            if cached_data:
                logger.debug("Found real-time features in cache for user %s", user_id)
                return UserFeatureDTO(
                    user_id=cached_data["user_id"],
                    feature_version_id=None,  # Real-time features don't have version
                    session_count=cached_data.get("session_count"),
                    session_duration_avg=cached_data.get("session_duration_avg"),
                    page_views_per_session=cached_data.get("page_views_per_session"),
                    total_spend=cached_data.get("total_spend"),
                    purchase_count=cached_data.get("purchase_count"),
                    avg_order_value=cached_data.get("avg_order_value"),
                    days_since_last_purchase=cached_data.get("days_since_last_purchase"),
                    days_since_last_event=cached_data.get("days_since_last_event"),
                    days_since_first_event=cached_data.get("days_since_first_event"),
                    top_category_1=cached_data.get("top_category_1"),
                    top_category_2=cached_data.get("top_category_2"),
                    top_brand_1=cached_data.get("top_brand_1"),
                    top_brand_2=cached_data.get("top_brand_2"),
                    price_range_min=cached_data.get("price_range_min"),
                    price_range_max=cached_data.get("price_range_max"),
                    price_range_avg=cached_data.get("price_range_avg"),
                    purchased=0,
                )
        except Exception as e:
            logger.warning("Error reading real-time features cache for user %s: %s", user_id, str(e))
        finally:
            await cache.stop()
        
        async with self._sf() as s:
            event_count = await self._warehouse_repo.get_user_events_count(s, user_id, lookback_days)
            
            if event_count == 0:
                logger.debug("User %s has no events in last %s days", user_id, lookback_days)
                return None
            
            user_events = await self._warehouse_repo.get_user_events_recent(s, user_id, lookback_days)
            
            if not user_events:
                logger.debug("User %s has no events after query", user_id)
                return None
            
            feature_window_end = datetime.now(ZoneInfo("UTC"))
            builder = PurchaseFeatureBuilder(s)
            
            try:
                feature_dto = builder._compute_features_from_events(
                    user_id=user_id,
                    user_events=user_events,
                    feature_window_end=feature_window_end,
                    purchased_users=None,
                )
                
                cache = get_cache()
                await cache.start()
                try:
                    cache_key = f"realtime_features:user:{user_id}:{lookback_days}"
                    feature_dict = feature_dto.model_dump(exclude={"purchased", "feature_version_id"})
                    feature_dict["user_id"] = user_id
                    await cache.set_json(cache_key, feature_dict, ttl=cache_ttl)
                    logger.debug("Cached real-time features for user %s", user_id)
                except Exception as e:
                    logger.warning("Error caching real-time features for user %s: %s", user_id, str(e))
                finally:
                    await cache.stop()
                
                logger.info("Computed real-time features for user %s from %s events", user_id, len(user_events))
                return feature_dto
                
            except ValueError as e:
                logger.warning("Failed to compute features for user %s: %s", user_id, str(e))
                return None
    
    async def compute_features_for_session(
        self,
        session_id: str,
    ) -> UserFeatureDTO | None:
        """
        Compute features for an anonymous user session on-the-fly from warehouse data.
        Used for users who haven't logged in yet (no user_id).
        
        Args:
            session_id: Session identifier
        
        Returns:
            UserFeatureDTO if events found, None if no events exist
        """
        from datetime import datetime
        from zoneinfo import ZoneInfo
        from core.settings.config import get_settings
        from modules.feature_store.purchase_features import PurchaseFeatureBuilder
        
        settings = get_settings()
        cache_ttl = settings.ml_prediction.realtime_feature_cache_ttl_seconds
        
        # Check cache first
        cache = get_cache()
        await cache.start()
        try:
            cache_key = f"realtime_features:session:{session_id}"
            cached_data = await cache.get_json(cache_key)
            
            if cached_data:
                logger.debug("Found real-time features in cache for session %s", session_id)
                return UserFeatureDTO(
                    user_id=None,  # Anonymous user
                    feature_version_id=None,
                    session_count=cached_data.get("session_count"),
                    session_duration_avg=cached_data.get("session_duration_avg"),
                    page_views_per_session=cached_data.get("page_views_per_session"),
                    total_spend=cached_data.get("total_spend"),
                    purchase_count=cached_data.get("purchase_count"),
                    avg_order_value=cached_data.get("avg_order_value"),
                    days_since_last_purchase=cached_data.get("days_since_last_purchase"),
                    days_since_last_event=cached_data.get("days_since_last_event"),
                    days_since_first_event=cached_data.get("days_since_first_event"),
                    top_category_1=cached_data.get("top_category_1"),
                    top_category_2=cached_data.get("top_category_2"),
                    top_brand_1=cached_data.get("top_brand_1"),
                    top_brand_2=cached_data.get("top_brand_2"),
                    price_range_min=cached_data.get("price_range_min"),
                    price_range_max=cached_data.get("price_range_max"),
                    price_range_avg=cached_data.get("price_range_avg"),
                    purchased=0,
                )
        except Exception as e:
            logger.warning("Error reading real-time features cache for session %s: %s", session_id, str(e))
        finally:
            await cache.stop()
        
        async with self._sf() as s:
            event_count = await self._warehouse_repo.get_session_events_count(s, session_id)
            
            if event_count == 0:
                logger.debug("Session %s has no events", session_id)
                return None
            
            user_events = await self._warehouse_repo.get_session_events(s, session_id)
            
            if not user_events:
                logger.debug("Session %s has no events after query", session_id)
                return None
            
            feature_window_end = datetime.now(ZoneInfo("UTC"))
            builder = PurchaseFeatureBuilder(s)
            
            try:
                feature_dto = builder._compute_features_from_events(
                    user_id=0,
                    user_events=user_events,
                    feature_window_end=feature_window_end,
                    purchased_users=None,
                )
                
                feature_dto.user_id = None
                
                cache = get_cache()
                await cache.start()
                try:
                    cache_key = f"realtime_features:session:{session_id}"
                    feature_dict = feature_dto.model_dump(exclude={"purchased", "feature_version_id", "user_id"})
                    await cache.set_json(cache_key, feature_dict, ttl=cache_ttl)
                    logger.debug("Cached real-time features for session %s", session_id)
                except Exception as e:
                    logger.warning("Error caching real-time features for session %s: %s", session_id, str(e))
                finally:
                    await cache.stop()
                
                logger.info("Computed real-time features for session %s from %s events", session_id, len(user_events))
                return feature_dto
                
            except ValueError as e:
                logger.warning("Failed to compute features for session %s: %s", session_id, str(e))
                return None