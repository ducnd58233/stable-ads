from pydantic import BaseModel, Field

class CreateJobRequest(BaseModel):
    prompt: str = Field(..., min_length=3)
    style: str | None = "neutral"
    duration_sec: int = Field(ge=5, le=600, default=30)

class CreateJobResponse(BaseModel):
    job_id: str

class JobStatusResponse(BaseModel):
    id: str
    status: str
    progress: int
    script: dict | None = None
    presigned_url: str | None = None
