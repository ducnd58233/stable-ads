from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import asyncio
import os
import logging
import tempfile
from typing import Any, Final

import pendulum
import pandas as pd
from core.infra.blob.registry import get_blob
from core.infra.blob.buckets import Buckets
from core.infra.db.async_db import AsyncDB
from modules.data_ingestion.parsers.registry import get_parser
from sqlalchemy import text

logger = logging.getLogger(__name__)

BATCH_SIZE = int(Variable.get("DATA_INGESTION_BATCH_SIZE", default_var=10))
LARGE_FILE_THRESHOLD_MB = int(Variable.get("LARGE_FILE_THRESHOLD_MB", default_var=100))
CHUNK_SIZE = int(Variable.get("DATA_INGESTION_CHUNK_SIZE", default_var=10000))

default_args = {
    'owner': 'stable-ads-data-ingestion-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(hours=1),
}

with DAG(
    'data_ingestion_pipeline',
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

    def get_pending_ingestions(**context) -> list[dict[str, Any]]:
        async def _get_pending() -> list[dict[str, Any]]:
            db = AsyncDB()
            await db.start()
            
            try:
                async with db.engine.begin() as conn:
                    result = await conn.execute(text("""
                        SELECT id, file_type, fields, raw_bucket, raw_key, raw_bytes
                        FROM data_ingestions
                        WHERE status = 'PENDING'
                        ORDER BY created_at ASC
                        LIMIT :limit
                    """), {'limit': BATCH_SIZE})
                    
                    rows = result.fetchall()
                    ingestions = [
                        {
                            'id': row[0],
                            'file_type': row[1],
                            'fields': row[2],
                            'raw_bucket': row[3],
                            'raw_key': row[4],
                            'raw_bytes': row[5],
                        }
                        for row in rows
                    ]
                
                logger.info(f"Found {len(ingestions)} pending ingestions")
                return ingestions
            finally:
                await db.stop()
        
        ingestions: Final[list[dict[str, Any]]] = asyncio.run(_get_pending())
        context['ti'].xcom_push(key='pending_ingestions', value=ingestions)
        return ingestions

    def validate_and_transform_data(**context):
        """Validate fields and transform data from MinIO"""
        upload_results = context['ti'].xcom_pull(key='pending_ingestions', task_ids='get_pending_ingestions')
        
        async def _validate_and_transform():
            db = AsyncDB()
            await db.start()
            blob = get_blob()
            await blob.start()
            
            transformed_data = []
            
            try:
                for ingestion in upload_results:
                    ingestion_id = ingestion['id']
                    raw_key = ingestion['raw_key']
                    file_type = ingestion['file_type']
                    requested_fields = ingestion['fields']
                    file_size = ingestion.get('raw_bytes', 0) or 0
                    
                    try:
                        async with db.engine.begin() as conn:
                            await conn.execute(text("""
                                UPDATE data_ingestions
                                SET status = 'VALIDATING'
                                WHERE id = :id
                            """), {'id': ingestion_id})
                        
                        file_data = await blob.get_object(Buckets.RAW_DATA, raw_key)
                        
                        temp_file_path = None
                        try:
                            temp_file = tempfile.NamedTemporaryFile(
                                delete=False,
                                suffix=f".{file_type}",
                                prefix=f'ingestion-{ingestion_id}-'
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
                                    raise ValueError(f"Missing fields in file: {', '.join(missing_fields)}")
                                fields_to_use = requested_fields
                            else:
                                fields_to_use = file_headers
                            
                            large_file_threshold = LARGE_FILE_THRESHOLD_MB * 1024 * 1024
                            
                            if file_size > large_file_threshold:
                                logger.info(f"Processing large file {ingestion_id} in chunks")
                                chunk_count = 0
                                for chunk_df in parser.read_dataframe_chunks(
                                    temp_file_path,
                                    fields=fields_to_use,
                                    chunksize=CHUNK_SIZE
                                ):
                                    transformed_chunk = parser.transform_chunk(chunk_df, fields_to_use)
                                    transformed_data.append(transformed_chunk)
                                    chunk_count += 1
                            else:
                                df = parser.read_dataframe(temp_file_path, fields=fields_to_use)
                                transformed = parser.transform(df, fields_to_use)
                                transformed_data.append(transformed)
                            
                            async with db.engine.begin() as conn:
                                await conn.execute(text("""
                                    UPDATE data_ingestions
                                    SET status = 'TRANSFORMED',
                                        fields = :fields
                                    WHERE id = :id
                                """), {'id': ingestion_id, 'fields': fields_to_use})
                            
                            logger.info(f"Validation and transformation successful for {ingestion_id}")
                            
                        finally:
                            if temp_file_path and os.path.exists(temp_file_path):
                                os.unlink(temp_file_path)
                    
                    except Exception as e:
                        logger.error(f"Validation/Transform failed for {ingestion_id}: {str(e)}")
                        async with db.engine.begin() as conn:
                            await conn.execute(text("""
                                UPDATE data_ingestions
                                SET status = 'FAILED',
                                    error_message = :error
                                WHERE id = :id
                            """), {'error': f'Validation/Transform error: {str(e)}', 'id': ingestion_id})
            
            finally:
                await blob.stop()
                await db.stop()
            
            return transformed_data
        
        transformed_data = asyncio.run(_validate_and_transform())
        context['ti'].xcom_push(key='transformed_data', value=transformed_data)
        return transformed_data

    def load_to_warehouse(**context):
        """Load transformed data into PostgreSQL data warehouse"""
        transformed_data = context['ti'].xcom_pull(key='transformed_data', task_ids='validate_and_transform_data')
        upload_results = context['ti'].xcom_pull(key='pending_ingestions', task_ids='get_pending_ingestions')
        
        async def _load():
            db = AsyncDB()
            await db.start()
            
            try:
                ingestion_ids = [ing['id'] for ing in upload_results]
                
                for data_chunk in transformed_data:
                    if isinstance(data_chunk, pd.DataFrame) and not data_chunk.empty:
                        records = data_chunk.to_dict('records')
                        
                        if records:
                            columns = list(data_chunk.columns)
                            placeholders = ', '.join([f':{col}' for col in columns])
                            columns_str = ', '.join(columns)
                            
                            async with db.engine.begin() as conn:
                                await conn.execute(
                                    text(f"""
                                        INSERT INTO warehouse_data ({columns_str})
                                        VALUES ({placeholders})
                                        ON CONFLICT DO NOTHING
                                    """),
                                    records
                                )
                
                for ingestion_id in ingestion_ids:
                    async with db.engine.begin() as conn:
                        result = await conn.execute(text("""
                            SELECT status FROM data_ingestions WHERE id = :id
                        """), {'id': ingestion_id})
                        row = result.fetchone()
                        
                        if row and row[0] == 'TRANSFORMED':
                            await conn.execute(text("""
                                UPDATE data_ingestions
                                SET status = 'SUCCEEDED'
                                WHERE id = :id
                            """), {'id': ingestion_id})
            
            finally:
                await db.stop()
        
        asyncio.run(_load())

    # Define tasks
    get_pending_task = PythonOperator(
        task_id='get_pending_ingestions',
        python_callable=get_pending_ingestions,
    )

    validate_and_transform_task = PythonOperator(
        task_id='validate_and_transform_data',
        python_callable=validate_and_transform_data,
    )

    load_task = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse,
    )

    # Set task dependencies
    get_pending_task >> validate_and_transform_task >> load_task