from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from modules.feature_store import UserFeatureDTO
from modules.ml import PurchasePredictionDTO
from .repository import JobRepository
from .model import Job
from .domain import JobStatus
from .dto import CreateJobRequest
from core.infra.mq import Envelope, Marshaler, AsyncPublisher, Topics
import uuid

class JobService:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory
        self._repo = JobRepository()
        self._topic = Topics.RENDER_REQUESTS.value

    async def create(self, req: CreateJobRequest) -> Job:
        job = Job(
            id=str(uuid.uuid4()),
            params=req.model_dump(),
            status=JobStatus.PENDING.value,
            progress=0,
        )
        async with self._sf() as s, s.begin():
            await self._repo.create(s, job)
        return job

    async def publish(self, job: Job, publisher: AsyncPublisher, marshaler: Marshaler) -> None:
        job_id = job.id
        job_params = job.params
        env: Envelope = marshaler.dumps(
            {"job_id": job_id, "prompt": job_params["prompt"], "style": job_params.get("style"), "duration_sec": job_params.get("duration_sec")},
            topic=self._topic,
        )
        await publisher.publish(env)

    async def get(self, job_id: str) -> Job | None:
        async with self._sf() as s:
            return await self._repo.get(s, job_id)

    async def save_script(self, job_id: str, script: dict) -> None:
        async with self._sf() as s, s.begin():
            await self._repo.save_script(s, job_id, script)

    async def set_running(self, job_id: str, progress: int = 0) -> None:
        async with self._sf() as s, s.begin():
            await self._repo.set_status(s, job_id, status=JobStatus.RUNNING.value, progress=progress)

    async def set_succeeded_storage(self, job_id: str, *, bucket: str, key: str, mime: str, nbytes: int) -> None:
        async with self._sf() as s, s.begin():
            await self._repo.set_storage(s, job_id, bucket=bucket, key=key, mime=mime, nbytes=nbytes)
            await self._repo.set_status(s, job_id, status=JobStatus.SUCCEEDED.value, progress=100)

    async def set_failed(self, job_id: str) -> None:
        async with self._sf() as s, s.begin():
            await self._repo.set_status(s, job_id, status=JobStatus.FAILED.value)
    
    async def create_from_prediction(
        self,
        user_id: int,
        prediction: PurchasePredictionDTO,
        user_features: UserFeatureDTO,
    ) -> Job:
        """
        Create a job from purchase prediction with personalized prompt.
        
        Args:
            user_id: User identifier
            prediction: Purchase prediction result
            user_features: User feature data for personalization
        
        Returns:
            Created Job
        """
        from modules.orchestrator.purchase_orchestrator import PurchaseOrchestrator
        orchestrator = PurchaseOrchestrator(self._sf)
        
        # Generate personalized prompt
        prompt = orchestrator._generate_personalized_prompt(user_features, prediction)
        
        # Create job
        from modules.jobs.dto import CreateJobRequest
        job_request = CreateJobRequest(
            prompt=prompt,
            style="neutral",
            duration_sec=30,
        )
        
        return await self.create(job_request)