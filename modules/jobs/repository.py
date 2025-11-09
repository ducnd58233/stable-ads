from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import update
from .model import Job

class JobRepository:
    async def create(self, s: AsyncSession, job: Job) -> Job:
        s.add(job)
        await s.flush()
        s.expunge(job)
        return job

    async def get(self, s: AsyncSession, job_id: str) -> Job | None:
        return await s.get(Job, job_id)

    async def save_script(self, s: AsyncSession, job_id: str, script: dict) -> None:
        stmt = update(Job).where(Job.id == job_id).values(script_json=script)
        await s.execute(stmt)

    async def set_status(self, s: AsyncSession, job_id: str, *, status: str, progress: int | None = None) -> None:
        stmt = (
            update(Job).where(Job.id == job_id)
            .values(status=status, progress=progress if progress is not None else Job.progress)
        )
        await s.execute(stmt)

    async def set_storage(
        self,
        s: AsyncSession,
        job_id: str,
        *,
        bucket: str,
        key: str,
        mime: str,
        nbytes: int
    ) -> None:
        stmt = (
            update(Job).where(Job.id == job_id)
            .values(video_bucket=bucket, video_key=key, video_mime=mime, video_bytes=nbytes)
        )
        await s.execute(stmt)