import logging
import os
import tempfile
import time
from typing import Any
import pandas as pd
import numpy as np
import asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from core.infra.blob.registry import get_blob
from core.infra.blob.buckets import Buckets
from modules.data_ingestion.parsers.registry import get_parser
from modules.data_ingestion.domain import IngestionStatus
from modules.data_ingestion.service import DataIngestionService
from modules.data_ingestion.model import DataIngestion
from modules.warehouse.dto import (
    FailedIngestionResult,
    SuccessfulIngestionResult,
    IngestionProcessingSummary,
)
from .repository import WarehouseDataRepository
from .model import WarehouseData

logger = logging.getLogger(__name__)

class WarehouseDataService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory
        self._warehouse_repo = WarehouseDataRepository()
        self._ingestion_service = DataIngestionService(session_factory)
    
    def _clean_record(self, record: dict) -> dict:
        cleaned_record = {}
        for key, value in record.items():
            if isinstance(value, (float, int)):
                if pd.isna(value) or np.isinf(value) or np.isnan(value):
                    cleaned_record[key] = None
                else:
                    cleaned_record[key] = value
            else:
                cleaned_record[key] = value
        return cleaned_record
    
    def _clean_records(self, records: list[dict]) -> list[dict]:    
        return [self._clean_record(record) for record in records]
    
    def _convert_to_warehouse_data(
        self,
        records: list[dict],
        ingestion_id: str,
    ) -> list[WarehouseData]:
        warehouse_data_list = []
        for record in records:
            record["ingestion_id"] = ingestion_id
            warehouse_data = WarehouseData(**record)
            warehouse_data_list.append(warehouse_data)
        return warehouse_data_list
    
    async def insert_chunk(
        self,
        chunk_df: pd.DataFrame,
        ingestion_id: str,
        batch_size: int = 10000,
    ) -> int:
        if chunk_df.empty:
            return 0
        
        records = []
        for row in chunk_df.itertuples(index=False):
            record = row._asdict()
            record["ingestion_id"] = ingestion_id
            records.append(record)
        
        if not records:
            return 0
        
        cleaned_records = self._clean_records(records)
        
        async with self._sf() as s, s.begin():
            total_inserted = 0
            
            for i in range(0, len(cleaned_records), batch_size):
                batch = cleaned_records[i:i + batch_size]
                warehouse_data_list = self._convert_to_warehouse_data(batch, ingestion_id)
                
                batch_count = await self._warehouse_repo.bulk_create(s, warehouse_data_list)
                total_inserted += batch_count
            
            return total_inserted
    
    async def _process_single_ingestion(
        self,
        ingestion: DataIngestion,
        blob: Any,
        large_file_threshold_bytes: int,
        chunk_size: int,
        max_chunk_concurrent: int = 10,
    ) -> SuccessfulIngestionResult | FailedIngestionResult:
        ingestion_id = ingestion.id
        start_time = time.time()
        rows_inserted = 0
        
        try:
            logger.info(
                "Starting ingestion processing",
                extra={
                    "ingestion_id": ingestion_id,
                    "file_type": ingestion.file_type,
                    "file_size_bytes": ingestion.raw_bytes or 0,
                }
            )
            
            await self._ingestion_service.update_status(
                ingestion_id,
                IngestionStatus.VALIDATING,
            )
            
            if not ingestion.raw_bucket or not ingestion.raw_key:
                raise ValueError(f"Missing raw_bucket or raw_key for ingestion {ingestion_id}")
            
            file_data = await blob.get_object(
                Buckets.RAW_DATA if ingestion.raw_bucket == Buckets.RAW_DATA.value else Buckets(ingestion.raw_bucket),
                ingestion.raw_key,
            )
            
            temp_file_path = None
            try:
                temp_file = tempfile.NamedTemporaryFile(
                    delete=False,
                    suffix=f".{ingestion.file_type}",
                    prefix=f"ingestion-{ingestion_id}-",
                )
                temp_file.write(file_data)
                temp_file_path = temp_file.name
                temp_file.close()
                
                parser = get_parser(temp_file_path)
                if not parser:
                    raise ValueError(f"No parser available for file type: {ingestion.file_type}")
                
                file_headers = parser.get_headers(temp_file_path)
                
                if not file_headers:
                    raise ValueError("File has no headers or is empty")
                
                requested_fields = ingestion.fields or []
                if requested_fields:
                    missing_fields = [f for f in requested_fields if f not in file_headers]
                    if missing_fields:
                        raise ValueError(
                            f"Missing fields in file: {', '.join(missing_fields)}"
                        )
                    fields_to_use = requested_fields
                else:
                    fields_to_use = file_headers
                
                await self._ingestion_service.update_status(
                    ingestion_id,
                    IngestionStatus.UPLOADING,
                    fields=fields_to_use,
                )
                
                file_size = ingestion.raw_bytes or os.path.getsize(temp_file_path)
                is_large_file = file_size >= large_file_threshold_bytes
                
                if is_large_file:
                    logger.info(
                        "Processing large file in parallel chunks",
                        extra={
                            "ingestion_id": ingestion_id,
                            "chunk_size": chunk_size,
                            "max_concurrent_chunks": max_chunk_concurrent,
                        }
                    )
                    
                    chunk_semaphore = asyncio.Semaphore(max_chunk_concurrent)
                    
                    async def process_chunk(chunk_df: pd.DataFrame) -> int:
                        async with chunk_semaphore:
                            transformed_chunk = parser.transform_chunk(chunk_df, fields_to_use)
                            return await self.insert_chunk(transformed_chunk, ingestion_id)
                    
                    chunk_tasks = []
                    for chunk_df in parser.read_dataframe_chunks(
                        temp_file_path,
                        fields=fields_to_use,
                        chunksize=chunk_size,
                    ):
                        chunk_tasks.append(process_chunk(chunk_df))
                    
                    chunk_results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
                    
                    for result in chunk_results:
                        if isinstance(result, Exception):
                            logger.error(
                                "Chunk processing failed",
                                extra={"ingestion_id": ingestion_id, "error": str(result)},
                                exc_info=True,
                            )
                            raise result
                        rows_inserted += result
                else:
                    df = parser.read_dataframe(temp_file_path, fields=fields_to_use)
                    transformed = parser.transform(df, fields=fields_to_use)
                    rows_inserted = await self.insert_chunk(transformed, ingestion_id)
                
                await self._ingestion_service.update_status(
                    ingestion_id,
                    IngestionStatus.SUCCEEDED,
                )
                
                return SuccessfulIngestionResult(
                    ingestion_id=ingestion_id,
                    rows_inserted=rows_inserted,
                    processing_time_seconds=round(time.time() - start_time, 2),
                )
            finally:
                if temp_file_path and os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
        
        except Exception as exc:
            error_message = str(exc)
            processing_time = time.time() - start_time
            
            logger.error(
                "Ingestion processing failed",
                extra={
                    "ingestion_id": ingestion_id,
                    "error": error_message,
                    "processing_time_seconds": round(processing_time, 2),
                },
                exc_info=True,
            )
            
            await self._ingestion_service.update_status(
                ingestion_id,
                IngestionStatus.FAILED,
                error_message=f"Validation/Transform error: {error_message}",
            )
            
            return FailedIngestionResult(
                ingestion_id=ingestion_id,
                error=error_message,
                processing_time_seconds=round(processing_time, 2),
            )
    
    async def process_ingestions(
        self,
        ingestions: list[DataIngestion],
        large_file_threshold_bytes: int,
        chunk_size: int,
        max_concurrent: int = 5,
    ) -> IngestionProcessingSummary:
        """Process all ingestions with parallel processing.
        
        Args:
            ingestions: List of ingestions to process
            large_file_threshold_bytes: Threshold for chunked processing
            chunk_size: Size of chunks for large files
            max_concurrent: Maximum number of concurrent ingestion processing tasks
        
        Returns:
            IngestionProcessingSummary DTO with processing results
        """
        if not ingestions:
            return IngestionProcessingSummary(
                successful=[],
                failed=[],
                total=0,
                success_count=0,
                failure_count=0,
                total_processing_time_seconds=0.0,
                total_rows_inserted=0,
            )
        
        start_time = time.time()
        successful_ingestions: list[SuccessfulIngestionResult] = []
        failed_ingestions: list[FailedIngestionResult] = []
        
        blob = get_blob()
        await blob.start()
        
        try:
            logger.info(
                "Starting batch ingestion processing",
                extra={
                    "total_ingestions": len(ingestions),
                    "max_concurrent": max_concurrent,
                }
            )
            
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def process_with_semaphore(ingestion: DataIngestion) -> SuccessfulIngestionResult | FailedIngestionResult:
                async with semaphore:
                    return await self._process_single_ingestion(
                        ingestion,
                        blob,
                        large_file_threshold_bytes,
                        chunk_size,
                        max_chunk_concurrent=10,
                    )
            
            results = await asyncio.gather(
                *[process_with_semaphore(ing) for ing in ingestions],
                return_exceptions=True,
            )
            
            for result in results:
                if isinstance(result, Exception):
                    logger.error(
                        "Unexpected error during ingestion processing",
                        extra={"error": str(result)},
                        exc_info=True,
                    )
                    failed_ingestions.append(
                        FailedIngestionResult(
                            ingestion_id="unknown",
                            error=f"Unexpected error: {str(result)}",
                            processing_time_seconds=0.0,
                        )
                    )
                elif isinstance(result, SuccessfulIngestionResult):
                    successful_ingestions.append(result)
                elif isinstance(result, FailedIngestionResult):
                    failed_ingestions.append(result)
                else:
                    logger.warning(
                        "Unexpected result type",
                        extra={"result_type": type(result).__name__},
                    )
                    failed_ingestions.append(
                        FailedIngestionResult(
                            ingestion_id="unknown",
                            error="Unexpected result type",
                            processing_time_seconds=0.0,
                        )
                    )
        
        finally:
            await blob.stop()
        
        total_processing_time = time.time() - start_time
        total_rows_inserted = sum(r.rows_inserted for r in successful_ingestions)
        
        summary = IngestionProcessingSummary(
            successful=successful_ingestions,
            failed=failed_ingestions,
            total=len(ingestions),
            success_count=len(successful_ingestions),
            failure_count=len(failed_ingestions),
            total_processing_time_seconds=round(total_processing_time, 2),
            total_rows_inserted=total_rows_inserted,
        )
        
        logger.info(
            "Batch ingestion processing complete",
            extra={
                "total": summary.total,
                "success_count": summary.success_count,
                "failure_count": summary.failure_count,
                "total_processing_time_seconds": summary.total_processing_time_seconds,
                "total_rows_inserted": summary.total_rows_inserted,
                "average_time_per_ingestion_seconds": summary.average_time_per_ingestion_seconds,
                "throughput_rows_per_second": summary.throughput_rows_per_second,
                "success_rate": summary.success_rate,
            }
        )
        
        return summary