from fastapi import APIRouter, Depends, HTTPException
from fastapi import Request

from core.infra.blob.buckets import Buckets
from core.infra.container import Infra
from modules.jobs.dto import CreateJobRequest, CreateJobResponse, JobStatusResponse
from modules.jobs.service import JobService

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])

def get_infra(req: Request) -> Infra:
    return req.app.state.infra

@router.post("", response_model=CreateJobResponse)
async def create_job(req: CreateJobRequest, infra: Infra = Depends(get_infra)) -> CreateJobResponse:
    svc = JobService(infra.session_factory)
    job = await svc.create(req)
    await svc.publish(job, infra.publisher, infra.marshaler)
    return CreateJobResponse(job_id=job.id)

@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job(job_id: str, infra: Infra = Depends(get_infra)) -> JobStatusResponse:
    svc = JobService(infra.session_factory)
    job = await svc.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Not found")

    presigned: str | None = None
    if job.video_bucket and job.video_key:
        presigned = await infra.blob.get_presigned_url(Buckets.VIDEOS, job.video_key, expires_seconds=3600)

    return JobStatusResponse(
        id=job.id,
        status=job.status,
        progress=job.progress,              # kept for backward compat; or remove from DTO
        script=job.script_json,
        presigned_url=presigned,
    )