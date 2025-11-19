from pydantic import BaseModel, Field


class SuccessfulIngestionResult(BaseModel):
    ingestion_id: str = Field(..., description="Unique identifier for the ingestion")
    rows_inserted: int = Field(..., ge=0, description="Number of rows inserted into warehouse")
    processing_time_seconds: float = Field(..., ge=0, description="Time taken to process this ingestion")


class FailedIngestionResult(BaseModel):
    ingestion_id: str = Field(..., description="Unique identifier for the ingestion")
    error: str = Field(..., description="Error message describing the failure")
    processing_time_seconds: float = Field(..., ge=0, description="Time taken before failure occurred")


class IngestionProcessingSummary(BaseModel):
    successful: list[SuccessfulIngestionResult] = Field(
        default_factory=list,
        description="List of successfully processed ingestions"
    )
    failed: list[FailedIngestionResult] = Field(
        default_factory=list,
        description="List of failed ingestions with error details"
    )
    total: int = Field(..., ge=0, description="Total number of ingestions processed")
    success_count: int = Field(..., ge=0, description="Number of successful ingestions")
    failure_count: int = Field(..., ge=0, description="Number of failed ingestions")
    total_processing_time_seconds: float = Field(
        ..., ge=0, description="Total time taken to process all ingestions"
    )
    total_rows_inserted: int = Field(
        ..., ge=0, description="Total rows inserted across all successful ingestions"
    )
    
    @property
    def average_time_per_ingestion_seconds(self) -> float:
        """Calculate average processing time per ingestion."""
        if self.total == 0:
            return 0.0
        return round(self.total_processing_time_seconds / self.total, 2)
    
    @property
    def throughput_rows_per_second(self) -> float:
        """Calculate throughput in rows per second."""
        if self.total_processing_time_seconds == 0:
            return 0.0
        return round(self.total_rows_inserted / self.total_processing_time_seconds, 2)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as a percentage."""
        if self.total == 0:
            return 0.0
        return round((self.success_count / self.total) * 100, 2)