import logging
import os
import tempfile
from typing import Any
import pandas as pd
import numpy as np
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from core.infra.blob.registry import get_blob
from core.infra.blob.buckets import Buckets
from modules.data_ingestion.parsers.registry import get_parser
from modules.data_ingestion.domain import IngestionStatus
from modules.data_ingestion.service import DataIngestionService
from modules.data_ingestion.model import DataIngestion
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
    ) -> int:
        if chunk_df.empty:
            return 0
        
        chunk_df = chunk_df.copy()
        records = chunk_df.to_dict("records")
        
        if not records:
            return 0
        
        cleaned_records = self._clean_records(records)
        
        warehouse_data_list = self._convert_to_warehouse_data(
            cleaned_records,
            ingestion_id,
        )
        
        async with self._sf() as s, s.begin():
            return await self._warehouse_repo.bulk_create(s, warehouse_data_list)
    
    async def process_ingestions(
        self,
        ingestions: list[DataIngestion],
        large_file_threshold_bytes: int,
        chunk_size: int,
    ) -> dict[str, Any]:
        """Process all ingestions and return summary.
        
        Handles: file processing, validation, transformation.
        Manages transactions for each ingestion.
        
        Returns:
            dict with keys:
                - successful: list of successful ingestion summaries
                - failed: list of failed ingestion details
                - total: total number of ingestions processed
                - success_count: number of successful ingestions
                - failure_count: number of failed ingestions
        """
        successful_ingestions: list[dict[str, Any]] = []
        failed_ingestions: list[dict[str, str]] = []
        
        blob = get_blob()
        await blob.start()
        
        try:
            for ingestion in ingestions:
                ingestion_id = ingestion.id
                rows_inserted = 0
                
                try:
                    await self._ingestion_service.update_status(
                        ingestion_id,
                        IngestionStatus.VALIDATING,
                    )
                    
                    file_data = await blob.get_object(Buckets.RAW_DATA, ingestion.raw_key)
                    
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
                        
                        file_size = ingestion.raw_bytes or 0
                        
                        if file_size > large_file_threshold_bytes:
                            logger.info("Processing large file %s in chunks", ingestion_id)
                            for chunk_df in parser.read_dataframe_chunks(
                                temp_file_path,
                                fields=fields_to_use,
                                chunksize=chunk_size,
                            ):
                                transformed_chunk = parser.transform_chunk(
                                    chunk_df, fields_to_use
                                )
                                rows_inserted += await self.insert_chunk(
                                    transformed_chunk, ingestion_id
                                )
                        else:
                            df = parser.read_dataframe(temp_file_path, fields=fields_to_use)
                            transformed = parser.transform(df, fields_to_use)
                            rows_inserted += await self.insert_chunk(transformed, ingestion_id)
                        
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
                    await self._ingestion_service.update_status(
                        ingestion_id,
                        IngestionStatus.FAILED,
                        error_message=f"Validation/Transform error: {error_message}",
                    )
                    failed_ingestions.append({
                        "ingestion_id": ingestion_id,
                        "error": error_message,
                    })
                    continue
                
                await self._ingestion_service.update_status(
                    ingestion_id,
                    IngestionStatus.SUCCEEDED,
                )
                successful_ingestions.append({
                    "ingestion_id": ingestion_id,
                    "rows_inserted": rows_inserted,
                })
        
        finally:
            await blob.stop()
        
        total_processed = len(ingestions)
        success_count = len(successful_ingestions)
        failure_count = len(failed_ingestions)
        
        summary = {
            "successful": successful_ingestions,
            "failed": failed_ingestions,
            "total": total_processed,
            "success_count": success_count,
            "failure_count": failure_count,
        }
        
        logger.info(
            "Processing complete: %s total, %s successful, %s failed",
            total_processed,
            success_count,
            failure_count,
        )
        
        return summary