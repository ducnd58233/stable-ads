from datetime import timedelta
import logging
from typing import Any, Final
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from core.infra.db.async_db import AsyncDB
from core.infra.db.model import Base
from core.utils.run_in_thread import run_async_from_sync
from modules.data_ingestion import DataIngestionService
from modules.warehouse import WarehouseDataService
from modules.feature_store import FeatureStoreService

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

logger = logging.getLogger(__name__)

for module_name in ['modules.warehouse', 'modules.feature_store', 'modules.ml', 'modules.data_ingestion']:
    module_logger = logging.getLogger(module_name)
    module_logger.setLevel(logging.INFO)
    module_logger.propagate = True

BATCH_SIZE = int(Variable.get("DATA_INGESTION_BATCH_SIZE", default_var=10))
LARGE_FILE_THRESHOLD_MB = int(Variable.get("LARGE_FILE_THRESHOLD_MB", default_var=100))
CHUNK_SIZE = int(Variable.get("DATA_INGESTION_CHUNK_SIZE", default_var=10000))
FEATURE_LOOKBACK_DAYS = int(Variable.get("FEATURE_LOOKBACK_DAYS", default_var=30))
LABEL_HORIZON_DAYS = int(Variable.get("LABEL_HORIZON_DAYS", default_var=7))

default_args = {
    "owner": "stable-ads-data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

def init_db(**context) -> None:
    async def _start():
        db = AsyncDB()
        await db.start()
        try:
            async with db.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Database tables initialized successfully")
        finally:
            await db.stop()
    
    run_async_from_sync(_start())

def ingest_data(**context) -> dict[str, Any]:
    
    async def _ingest():
        db = AsyncDB()
        await db.start()
        try:
            ingestion_service = DataIngestionService(db.session_factory)
            warehouse_service = WarehouseDataService(db.session_factory)
            
            ingestions = await ingestion_service.get_pending_ingestions(BATCH_SIZE)
            
            if not ingestions:
                logger.info("No pending ingestions found")
                return {
                    "ingestions_processed": 0,
                    "rows_inserted": 0,
                    "has_data": False,
                }
            
            large_file_threshold_bytes = LARGE_FILE_THRESHOLD_MB * 1024 * 1024
            
            logger.info("Processing %s ingestions", len(ingestions))
            
            summary = await warehouse_service.process_ingestions(
                ingestions,
                large_file_threshold_bytes,
                CHUNK_SIZE,
            )
            
            if summary.failure_count > 0:
                error_details = "; ".join([
                    f"{f.ingestion_id}: {f.error[:100]}"
                    for f in summary.failed[:5]
                ])
                if summary.failure_count > 5:
                    error_details += f" ... and {summary.failure_count - 5} more failures"
                
                logger.error(
                    "Data ingestion failed: %s/%s ingestions failed. Failures: %s",
                    summary.failure_count,
                    summary.total,
                    error_details,
                )
                raise RuntimeError(
                    f"Data ingestion failed: {summary.failure_count}/{summary.total} ingestions failed. "
                    f"Failures: {error_details}"
                )
            
            total_rows = sum(s.rows_inserted for s in summary.successful)
            logger.info(
                "Ingestion complete: %s successful, %s rows inserted",
                summary.success_count,
                total_rows,
            )
            
            return {
                "ingestions_processed": summary.total,
                "rows_inserted": total_rows,
                "has_data": total_rows > 0,
            }
        finally:
            await db.stop()
    
    result: Final[dict[str, Any]] = run_async_from_sync(_ingest())
    context["ti"].xcom_push(key="ingestion_result", value=result)
    return result

def materialize_features(**context) -> dict[str, Any]:
    
    ingestion_result = context["ti"].xcom_pull(
        key="ingestion_result", task_ids="ingest_data"
    ) or {}
    
    has_data = ingestion_result.get("has_data", False)
    if not has_data:
        logger.info("No data ingested, skipping feature materialization")
        return {
            "features_computed": False,
            "feature_version_id": None,
        }
    
    async def _materialize():
        db = AsyncDB()
        await db.start()
        try:
            feature_service = FeatureStoreService(db.session_factory)
            
            feature_window_start, feature_window_end, incomplete_version = await feature_service.determine_feature_window(
                lookback_days=FEATURE_LOOKBACK_DAYS,
                fallback_to_current_time=True,
                check_incomplete_first=True,
            )
            
            if incomplete_version:
                logger.info(
                    "Resuming from incomplete version %s with window %s to %s",
                    incomplete_version.version,
                    feature_window_start,
                    feature_window_end,
                )
            
            if feature_window_start >= feature_window_end:
                raise ValueError(
                    f"Invalid feature window: start ({feature_window_start}) must be before end ({feature_window_end})"
                )
            
            logger.info(
                "Computing features for window: %s to %s (lookback: %s days)",
                feature_window_start,
                feature_window_end,
                FEATURE_LOOKBACK_DAYS,
            )
            
            try:
                result_dto = await feature_service.materialize_features(
                    feature_window_start,
                    feature_window_end,
                    LABEL_HORIZON_DAYS,
                )
                
                logger.info(
                    "Features computed: version %s, %s users",
                    result_dto.version,
                    result_dto.total_users,
                )
                
                return {
                    "features_computed": True,
                    "feature_version_id": result_dto.feature_version_id,
                    "feature_version": result_dto.version,
                    "total_users": result_dto.total_users,
                    "total_records": result_dto.total_records,
                }
            except ValueError as e:
                if "No features generated" in str(e):
                    logger.warning(
                        "No features generated for window %s to %s. This may be normal if there's no data yet.",
                        feature_window_start,
                        feature_window_end,
                    )
                    return {
                        "features_computed": False,
                        "feature_version_id": None,
                    }
                raise
        finally:
            await db.stop()
    
    result: Final[dict[str, Any]] = run_async_from_sync(_materialize())
    context["ti"].xcom_push(key="feature_result", value=result)
    return result

def publish_online_features(**context) -> dict[str, Any]:
    
    feature_result = context["ti"].xcom_pull(
        key="feature_result", task_ids="materialize_features"
    ) or {}
    
    features_computed = feature_result.get("features_computed", False)
    feature_version_id = feature_result.get("feature_version_id")
    
    if not features_computed or not feature_version_id:
        logger.info("No features to publish, skipping online feature publish")
        return feature_result
    
    async def _publish():
        db = AsyncDB()
        await db.start()
        try:
            feature_service = FeatureStoreService(db.session_factory)
            await feature_service.publish_online_features(feature_version_id)
            logger.info("Published features to online store")
            return feature_result
        finally:
            await db.stop()
    
    result: Final[dict[str, Any]] = run_async_from_sync(_publish())
    return result

with DAG(
    "data_ingestion_pipeline",
    default_args=default_args,
    description="ETL pipeline: Ingest → Transform → Load → Feature Engineering",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_lake", "data_warehouse", "etl", "feature_store"],
    max_active_runs=3,
    max_active_tasks=10,
    doc_md="""
    # Data Ingestion Pipeline
    
    Processes data ingestion and feature engineering in separate tasks:
    1. **Ingest Data**: Get pending ingestions, validate, transform, load into warehouse
    2. **Materialize Features**: Compute aggregated features from warehouse data
    3. **Publish Online Features**: Push latest features to Redis for low-latency inference
    """,
) as dag:
    init_db_task = PythonOperator(
        task_id="init_db",
        python_callable=init_db,
    )
    
    ingest_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
    )
    
    materialize_task = PythonOperator(
        task_id="materialize_features",
        python_callable=materialize_features,
    )
    
    publish_task = PythonOperator(
        task_id="publish_online_features",
        python_callable=publish_online_features,
    )
    
    init_db_task >> ingest_task >> materialize_task >> publish_task