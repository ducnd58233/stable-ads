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
from modules.data_ingestion import DataIngestion, DataIngestionService
from modules.warehouse import WarehouseDataService

logger = logging.getLogger(__name__)

BATCH_SIZE = int(Variable.get("DATA_INGESTION_BATCH_SIZE", default_var=10))
LARGE_FILE_THRESHOLD_MB = int(Variable.get("LARGE_FILE_THRESHOLD_MB", default_var=100))
CHUNK_SIZE = int(Variable.get("DATA_INGESTION_CHUNK_SIZE", default_var=10000))

default_args = {
    "owner": "stable-ads-data-ingestion-team",
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
        async with db.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await db.stop()
    
    run_async_from_sync(_start())
    logger.info("Database initialized successfully")


def get_pending_ingestions(**context) -> list[dict[str, Any]]:
    async def _get_pending() -> list[dict[str, Any]]:
        db = AsyncDB()
        await db.start()
        try:
            service = DataIngestionService(db.session_factory)
            ingestions = await service.get_pending_ingestions(BATCH_SIZE)
            
            result = [
                {
                    "id": ing.id,
                    "file_type": ing.file_type,
                    "fields": ing.fields,
                    "raw_bucket": ing.raw_bucket,
                    "raw_key": ing.raw_key,
                    "raw_bytes": ing.raw_bytes,
                }
                for ing in ingestions
            ]
            
            logger.info("Found %s pending ingestions", len(result))
            return result
        finally:
            await db.stop()
    
    ingestions: Final[list[dict[str, Any]]] = run_async_from_sync(_get_pending())
    context["ti"].xcom_push(key="pending_ingestions", value=ingestions)
    return ingestions


def validate_and_transform_data(**context) -> dict[str, Any]:
    upload_results = context["ti"].xcom_pull(
        key="pending_ingestions", task_ids="get_pending_ingestions"
    ) or []
    
    async def _process_ingestions() -> dict[str, Any]:
        db = AsyncDB()
        await db.start()
        try:
            ingestions = [
                DataIngestion(
                    id=item["id"],
                    file_type=item["file_type"],
                    fields=item.get("fields"),
                    raw_bucket=item.get("raw_bucket"),
                    raw_key=item.get("raw_key"),
                    raw_bytes=item.get("raw_bytes"),
                )
                for item in upload_results
            ]
            
            service = WarehouseDataService(db.session_factory)
            large_file_threshold_bytes = LARGE_FILE_THRESHOLD_MB * 1024 * 1024
            
            summary = await service.process_ingestions(
                ingestions,
                large_file_threshold_bytes,
                CHUNK_SIZE,
            )
            
            if summary["failure_count"] > 0:
                error_details = "; ".join([
                    f"{f['ingestion_id']}: {f['error'][:100]}"
                    for f in summary["failed"][:5]
                ])
                if summary["failure_count"] > 5:
                    error_details += f" ... and {summary['failure_count'] - 5} more failures"
                
                raise RuntimeError(
                    f"Data ingestion pipeline failed: {summary['failure_count']}/{summary['total']} ingestions failed. "
                    f"Failures: {error_details}"
                )
            
            return summary
        finally:
            await db.stop()
    
    summaries: Final[dict[str, Any]] = run_async_from_sync(_process_ingestions())
    context["ti"].xcom_push(key="processed_ingestions", value=summaries)
    return summaries


def load_to_warehouse(**context) -> dict[str, Any]:
    summary = context["ti"].xcom_pull(
        key="processed_ingestions", task_ids="validate_and_transform_data"
    ) or {}
    
    if not summary or not isinstance(summary, dict):
        logger.info("No ingestions processed in previous step.")
        return {}
    
    total = summary.get("total", 0)
    success_count = summary.get("success_count", 0)
    failure_count = summary.get("failure_count", 0)
    successful = summary.get("successful", [])
    
    logger.info(
        "Warehouse load summary: %s total, %s successful, %s failed",
        total,
        success_count,
        failure_count,
    )
    
    for item in successful:
        logger.info(
            "Ingestion %s inserted %s rows into warehouse_data",
            item.get("ingestion_id"),
            item.get("rows_inserted"),
        )
    
    return summary


with DAG(
    "data_ingestion_pipeline",
    default_args=default_args,
    description="ETL pipeline for data ingestion: Validate → Transform → Load",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["data_lake", "data_warehouse", "etl"],
    max_active_runs=3,
    max_active_tasks=10,
    doc_md="""
    # Data Ingestion ETL Pipeline

    Processes data ingestion requests:
    1. **Extract**: Get pending ingestions from database
    2. **Validate**: Download from MinIO, validate fields exist
    3. **Transform**: Clean and transform data using parsers
    4. **Load**: Insert into data warehouse
    """,
) as dag:
    init_db_task = PythonOperator(
        task_id="init_db", python_callable=init_db
    )
    
    get_pending_task = PythonOperator(
        task_id="get_pending_ingestions", python_callable=get_pending_ingestions
    )
    
    validate_and_transform_task = PythonOperator(
        task_id="validate_and_transform_data", python_callable=validate_and_transform_data
    )
    
    load_task = PythonOperator(
        task_id="load_to_warehouse", python_callable=load_to_warehouse
    )
    
    init_db_task >> get_pending_task >> validate_and_transform_task >> load_task