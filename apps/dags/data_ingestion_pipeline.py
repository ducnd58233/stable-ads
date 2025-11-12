from datetime import timedelta
import json
import logging
import os
import tempfile
from typing import Any, Final

import numpy as np
import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from core.infra.blob.buckets import Buckets
from core.infra.blob.registry import get_blob
from core.infra.db.async_db import AsyncDB
from core.utils.run_in_thread import run_async_from_sync
from modules.data_ingestion.domain import IngestionStatus
from modules.data_ingestion.parsers.registry import get_parser
from sqlalchemy import text

logger = logging.getLogger(__name__)

BATCH_SIZE = int(Variable.get("DATA_INGESTION_BATCH_SIZE", default_var=10))
LARGE_FILE_THRESHOLD_MB = int(Variable.get("LARGE_FILE_THRESHOLD_MB", default_var=100))
CHUNK_SIZE = int(Variable.get("DATA_INGESTION_CHUNK_SIZE", default_var=10000))

WAREHOUSE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS warehouse_data (
    id BIGSERIAL PRIMARY KEY,
    ingestion_id TEXT NOT NULL,
    event_time TIMESTAMPTZ,
    event_type TEXT,
    product_id BIGINT,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price DOUBLE PRECISION,
    user_id BIGINT,
    user_session TEXT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);
"""

WAREHOUSE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_warehouse_data_ingestion_event
ON warehouse_data (ingestion_id, event_time);
"""

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


def get_pending_ingestions(**context) -> list[dict[str, Any]]:
    async def _get_pending() -> list[dict[str, Any]]:
        db = AsyncDB()
        await db.start()
        try:
            async with db.engine.begin() as conn:
                result = await conn.execute(
                    text(
                        """
                        SELECT id, file_type, fields, raw_bucket, raw_key, raw_bytes
                        FROM data_ingestions
                        WHERE status = :pending_status
                        ORDER BY created_at ASC
                        LIMIT :limit
                        """
                    ),
                    {"limit": BATCH_SIZE, "pending_status": IngestionStatus.PENDING.value},
                )
                rows = result.fetchall()
                ingestions = [
                    {
                        "id": row[0],
                        "file_type": row[1],
                        "fields": row[2],
                        "raw_bucket": row[3],
                        "raw_key": row[4],
                        "raw_bytes": row[5],
                    }
                    for row in rows
                ]
            logger.info("Found %s pending ingestions", len(ingestions))
            return ingestions
        finally:
            await db.stop()

    ingestions: Final[list[dict[str, Any]]] = run_async_from_sync(_get_pending())
    context["ti"].xcom_push(key="pending_ingestions", value=ingestions)
    return ingestions


def validate_and_transform_data(**context) -> dict[str, Any]:
    """Validate fields, stream chunks into the warehouse, and capture summaries.
    
    Returns a summary dict with success/failure counts. Raises RuntimeError
    if any ingestion fails to ensure the Airflow task fails.
    """
    upload_results = context["ti"].xcom_pull(
        key="pending_ingestions", task_ids="get_pending_ingestions"
    ) or []

    async def _process_ingestions() -> dict[str, Any]:
        """Process all pending ingestions and return summary with success/failure counts.
        
        Returns:
            dict with keys:
                - successful: list of successful ingestion summaries
                - failed: list of failed ingestion details
                - total: total number of ingestions processed
                - success_count: number of successful ingestions
                - failure_count: number of failed ingestions
        """
        db = AsyncDB()
        await db.start()
        blob = get_blob()
        await blob.start()

        successful_ingestions: list[dict[str, Any]] = []
        failed_ingestions: list[dict[str, str]] = []

        try:
            async with db.engine.begin() as conn:
                await conn.execute(text(WAREHOUSE_TABLE_DDL))
                await conn.execute(text(WAREHOUSE_INDEX_DDL))

            async def _update_status(
                ingestion_id: str,
                status: IngestionStatus,
                *,
                fields: list[str] | None = None,
                error_message: str | None = None,
            ) -> None:
                payload = {
                    "id": ingestion_id,
                    "status": status.value,
                    "error": error_message,
                    "fields": json.dumps(fields) if fields is not None else None,
                }
                async with db.engine.begin() as conn:
                    if fields is not None:
                        await conn.execute(
                            text(
                                """
                                UPDATE data_ingestions
                                SET status = :status,
                                    fields = CAST(:fields AS JSONB),
                                    error_message = :error
                                WHERE id = :id
                                """
                            ),
                            payload,
                        )
                    else:
                        await conn.execute(
                            text(
                                """
                                UPDATE data_ingestions
                                SET status = :status,
                                    error_message = :error
                                WHERE id = :id
                                """
                            ),
                            payload,
                        )

            async def _insert_chunk(chunk_df: pd.DataFrame, ingestion_id: str) -> int:
                """Insert a chunk of transformed data into the warehouse.
                
                Handles invalid numeric values (NaN, inf, -inf) by converting them to None,
                which becomes NULL in PostgreSQL.
                """
                if chunk_df.empty:
                    return 0

                chunk_df = chunk_df.copy()
                chunk_df["ingestion_id"] = ingestion_id
                
                chunk_df = chunk_df.replace({
                    np.nan: None,
                    pd.NA: None,
                    np.inf: None,
                    -np.inf: None,
                    float('inf'): None,
                    float('-inf'): None,
                })

                records = chunk_df.to_dict("records")
                if not records:
                    return 0

                columns = list(chunk_df.columns)
                placeholders = ", ".join(f":{col}" for col in columns)
                columns_str = ", ".join(columns)

                async with db.engine.begin() as conn:
                    await conn.execute(
                        text(
                            f"""
                            INSERT INTO warehouse_data ({columns_str})
                            VALUES ({placeholders})
                            ON CONFLICT DO NOTHING
                            """
                        ),
                        records,
                    )
                return len(records)

            for ingestion in upload_results:
                ingestion_id = ingestion["id"]
                raw_key = ingestion["raw_key"]
                file_type = ingestion["file_type"]
                requested_fields = ingestion["fields"]
                file_size = ingestion.get("raw_bytes", 0) or 0
                rows_inserted = 0

                try:
                    await _update_status(ingestion_id, IngestionStatus.VALIDATING)

                    file_data = await blob.get_object(Buckets.RAW_DATA, raw_key)

                    temp_file_path = None
                    try:
                        temp_file = tempfile.NamedTemporaryFile(
                            delete=False,
                            suffix=f".{file_type}",
                            prefix=f"ingestion-{ingestion_id}-",
                        )
                        temp_file.write(file_data)
                        temp_file_path = temp_file.name
                        temp_file.close()

                        parser = get_parser(temp_file_path)
                        file_headers = parser.get_headers(temp_file_path)

                        if not file_headers:
                            raise ValueError("File has no headers or is empty")

                        if requested_fields:
                            missing_fields = [f for f in requested_fields if f not in file_headers]
                            if missing_fields:
                                raise ValueError(
                                    f"Missing fields in file: {', '.join(missing_fields)}"
                                )
                            fields_to_use = requested_fields
                        else:
                            fields_to_use = file_headers

                        await _update_status(
                            ingestion_id,
                            IngestionStatus.UPLOADING,
                            fields=fields_to_use,
                            error_message=None,
                        )

                        large_file_threshold = LARGE_FILE_THRESHOLD_MB * 1024 * 1024

                        if file_size > large_file_threshold:
                            logger.info("Processing large file %s in chunks", ingestion_id)
                            for chunk_df in parser.read_dataframe_chunks(
                                temp_file_path,
                                fields=fields_to_use,
                                chunksize=CHUNK_SIZE,
                            ):
                                transformed_chunk = parser.transform_chunk(
                                    chunk_df, fields_to_use
                                )
                                rows_inserted += await _insert_chunk(
                                    transformed_chunk, ingestion_id
                                )
                        else:
                            df = parser.read_dataframe(temp_file_path, fields=fields_to_use)
                            transformed = parser.transform(df, fields_to_use)
                            rows_inserted += await _insert_chunk(transformed, ingestion_id)

                        logger.info(
                            "Validation and load successful for %s. Rows inserted: %s",
                            ingestion_id,
                            rows_inserted,
                        )

                    finally:
                        if temp_file_path and os.path.exists(temp_file_path):
                            os.unlink(temp_file_path)

                except Exception as exc:
                    error_message = str(exc)
                    logger.exception(
                        "Validation/Transform failed for %s: %s", ingestion_id, error_message
                    )
                    await _update_status(
                        ingestion_id,
                        IngestionStatus.FAILED,
                        error_message=f"Validation/Transform error: {error_message}",
                    )
                    failed_ingestions.append({
                        "ingestion_id": ingestion_id,
                        "error": error_message,
                    })
                    continue

                await _update_status(ingestion_id, IngestionStatus.SUCCEEDED, error_message=None)
                successful_ingestions.append({
                    "ingestion_id": ingestion_id,
                    "rows_inserted": rows_inserted,
                })

        finally:
            await blob.stop()
            await db.stop()

        total_processed = len(upload_results)
        success_count = len(successful_ingestions)
        failure_count = len(failed_ingestions)

        summary = {
            "successful": successful_ingestions,
            "failed": failed_ingestions,
            "total": total_processed,
            "success_count": success_count,
            "failure_count": failure_count,
        }

        # Log summary
        logger.info(
            "Processing complete: %s total, %s successful, %s failed",
            total_processed,
            success_count,
            failure_count,
        )

        # Raise exception if any ingestion failed to ensure the task fails
        if failed_ingestions:
            error_details = "; ".join([
                f"{f['ingestion_id']}: {f['error'][:100]}"  # Truncate long errors
                for f in failed_ingestions[:5]  # Show first 5 failures
            ])
            if failure_count > 5:
                error_details += f" ... and {failure_count - 5} more failures"
            
            raise RuntimeError(
                f"Data ingestion pipeline failed: {failure_count}/{total_processed} ingestions failed. "
                f"Failures: {error_details}"
            )

        return summary

    summaries = run_async_from_sync(_process_ingestions())
    context["ti"].xcom_push(key="processed_ingestions", value=summaries)
    return summaries


def load_to_warehouse(**context) -> dict[str, Any]:
    """Summarize the work already written to the warehouse.
    
    This task runs after validate_and_transform_data, which already inserts
    data into the warehouse. This task just logs the summary.
    """
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
    get_pending_task = PythonOperator(
        task_id="get_pending_ingestions", python_callable=get_pending_ingestions
    )

    validate_and_transform_task = PythonOperator(
        task_id="validate_and_transform_data", python_callable=validate_and_transform_data
    )

    load_task = PythonOperator(
        task_id="load_to_warehouse", python_callable=load_to_warehouse
    )

    get_pending_task >> validate_and_transform_task >> load_task