from pydantic import BaseModel, Field

class CreateDataIngestionRequest(BaseModel):
    fields: list[str] = Field(
        default_factory=list, 
        description="List of field names to extract from the file. If empty, all fields will be used."
    )

class CreateDataIngestionResponse(BaseModel):
    ingestion_id: str
    status: str
    message: str

class DataIngestionStatusResponse(BaseModel):
    id: str
    status: str
    raw_bucket: str | None = None
    raw_key: str | None = None
    error_message: str | None = None