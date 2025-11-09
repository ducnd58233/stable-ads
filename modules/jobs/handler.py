import asyncio
from typing import Final
from core.infra.blob.buckets import Buckets
from core.infra.container import Infra
from modules.orchestrator import AgentState, ScriptSchema, to_render_request
from modules.render.video_renderer import VideoRenderer
from modules.orchestrator.graph import get_graph


class JobsHandler:
    _MIME: Final[str] = "video/mp4"

    def __init__(self, infra: Infra) -> None:
        self._infra = infra
        self._renderer = VideoRenderer()
        from core.settings.config import get_settings
        self._fps = get_settings().diffuser.fps
        self._graph = get_graph()

    async def close(self) -> None:
        self._renderer.close()

    async def handle(self, msg: dict) -> None:
        from .service import JobService
        svc = JobService(self._infra.session_factory)

        job_id: str = msg["job_id"]
        prompt: str = msg["prompt"]
        style: str = (msg.get("style") or "neutral")
        duration_sec: int = int(msg.get("duration_sec") or 30)

        # 1) Run agent
        state = AgentState(job_id=job_id, prompt=prompt, style=style, duration_sec=duration_sec)
        out = await self._graph.ainvoke(state)
        if not out.script:
            await svc.set_failed(job_id)
            return
        script = ScriptSchema.model_validate(out.script)
        await svc.save_script(job_id, script.model_dump())
        await svc.set_running(job_id, progress=60)

        # 2) Render to bytes (no local persistent file)
        req = to_render_request(script, fps=self._fps)
        data: bytes = await asyncio.to_thread(self._renderer.render, req)

        # 3) Upload to blob
        bucket = Buckets.VIDEOS.value
        key = f"videos/{job_id}.mp4"     # id-based name for idempotency
        await self._infra.blob.upload(Buckets.VIDEOS, key, data, content_type=self._MIME)

        # 4) Persist storage metadata
        await svc.set_succeeded_storage(job_id, bucket=bucket, key=key, mime=self._MIME, nbytes=len(data))