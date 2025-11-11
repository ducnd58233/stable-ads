from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, File, Form
from core.infra.container import Infra
from core.infra.blob.buckets import Buckets
from .dto import CreateDataIngestionResponse, DataIngestionStatusResponse
from .service import DataIngestionService
import uuid
import json
from typing import Optional

router = APIRouter(prefix="/v1/data-ingestion", tags=["data-ingestion"])

def get_infra(req: Request) -> Infra:
    return req.app.state.infra

@router.post("", response_model=CreateDataIngestionResponse)
async def create_data_ingestion(
    file: UploadFile = File(..., description="File to ingest (CSV, etc.)"),
    fields: Optional[str] = Form(None, description='JSON array string, e.g., ["field1", "field2"]. If empty, all fields will be used.'),
    infra: Infra = Depends(get_infra)
) -> CreateDataIngestionResponse:
    svc = DataIngestionService(infra.session_factory)
    
    fields_list = []
    if fields:
        try:
            parsed = json.loads(fields)
            if isinstance(parsed, list):
                fields_list = [str(f).strip() for f in parsed if f and str(f).strip()]
        except (json.JSONDecodeError, TypeError) as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid JSON format for fields. Expected JSON array string, got: {fields}. Error: {str(e)}"
            )
    
    try:
        file_bytes = await file.read()
        file_size = len(file_bytes)
        
        file_type = "unknown"
        if file.filename:
            file_ext = file.filename.split(".")[-1].lower() if "." in file.filename else ""
            file_type = file_ext
        
        now = datetime.utcnow()
        time_partition = f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}"
        
        ingestion_id = str(uuid.uuid4())
        filename = file.filename or f"upload_{ingestion_id}"
        key = f"raw/{time_partition}/{ingestion_id}/{filename}"
        
        # Upload to MinIO
        await infra.blob.upload(
            Buckets.RAW_DATA,
            key,
            file_bytes,
            content_type=file.content_type or "application/octet-stream"
        )
        
        ingestion = await svc.create(
            ingestion_id=ingestion_id,
            file_type=file_type,
            fields=fields_list,
            raw_bucket=Buckets.RAW_DATA.value,
            raw_key=key,
            raw_bytes=file_size
        )
        
        return CreateDataIngestionResponse(
            ingestion_id=ingestion.id,
            status=ingestion.status,
            message="File uploaded successfully. Airflow will validate and process the ingestion."
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload error: {str(e)}")

@router.get("/{ingestion_id}", response_model=DataIngestionStatusResponse)
async def get_ingestion_status(
    ingestion_id: str,
    infra: Infra = Depends(get_infra)
) -> DataIngestionStatusResponse:
    svc = DataIngestionService(infra.session_factory)
    ingestion = await svc.get(ingestion_id)
    
    if not ingestion:
        raise HTTPException(status_code=404, detail="Ingestion not found")
    
    return DataIngestionStatusResponse(
        id=ingestion.id,
        status=ingestion.status,
        raw_bucket=ingestion.raw_bucket,
        raw_key=ingestion.raw_key,
        error_message=ingestion.error_message,
    )