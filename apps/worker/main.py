import asyncio, contextlib
from fastapi import FastAPI
from contextlib import asynccontextmanager
from core.infra.db.async_db import AsyncDB
from core.infra.db.model import Base
from core.infra.blob.registry import get_blob
from core.infra.cache.registry import get_cache
from core.infra.mq import create_subscriber, create_marshaler, AsyncSubscriber, mw_retry_dlq, Topics
from core.infra.container import Infra
from core.settings.config import get_settings
from modules.jobs.handler import JobsHandler

class RT:
    infra: Infra
    sub: AsyncSubscriber
    loop_task: asyncio.Task | None = None
    handler: JobsHandler | None = None
    db: AsyncDB

@asynccontextmanager
async def lifespan(app: FastAPI):
    s = get_settings()
    rt = RT()

    rt.db = AsyncDB()
    await rt.db.start()
    async with rt.db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    blob = get_blob(); await blob.start()
    cache = get_cache()
    await cache.start()
    marshaler = create_marshaler("json", version="v1")

    app.state.infra = Infra(
        engine=rt.db.engine,
        session_factory=rt.db.session_factory,
        blob=blob,
        publisher=None,
        marshaler=marshaler,
        cache=cache,
    )

    rt.handler = JobsHandler(app.state.infra)
    rt.sub = create_subscriber(s.mq.driver, group_id=s.mq.group_renderer)
    await rt.sub.start([Topics.RENDER_REQUESTS.value, Topics.RENDER_RESULTS.value])

    async def _loop() -> None:
        try:
            while True:
                env = await rt.sub.poll(1.0)
                if not env:
                    continue
                try:
                    payload = marshaler.loads(env)
                    await rt.handler.handle(payload)
                    await rt.sub.commit(env)
                except Exception as e:
                    print(f"Error handling message: {e}")
                    await mw_retry_dlq(rt.infra.publisher, Topics.RENDER_DLQ.value)
        except asyncio.CancelledError:
            pass

    rt.loop_task = asyncio.create_task(_loop())
    app.state.rt = rt

    try:
        yield
    finally:
        if rt.loop_task:
            rt.loop_task.cancel()
            with contextlib.suppress(Exception):
                await rt.loop_task
        await rt.sub.stop()
        if rt.handler:
            await rt.handler.close()
        await blob.stop()
        await cache.stop()
        await rt.db.stop()

app = FastAPI(title="stable-ads-worker", lifespan=lifespan)

@app.get("/healthz")
async def healthz() -> dict[str, bool]:
    return {"ok": True}
