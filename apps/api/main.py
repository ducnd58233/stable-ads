from fastapi import FastAPI
from contextlib import asynccontextmanager
from core.infra.db.async_db import AsyncDB
from core.infra.db.model import Base
from core.infra.blob.registry import get_blob 
from core.infra.mq import create_publisher, create_marshaler
from core.infra.container import Infra
from core.settings.config import get_settings
from modules.jobs.api import router as jobs_router
from modules.data_ingestion.api import router as data_ingestion_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    s = get_settings()

    db = AsyncDB()
    await db.start()
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    blob = get_blob()
    await blob.start()

    pub = create_publisher(s.mq.driver)
    await pub.start()

    marshaler = create_marshaler("json", version="v1")

    app.state.infra = Infra(
        engine=db.engine,
        session_factory=db.session_factory,
        blob=blob,
        publisher=pub,
        marshaler=marshaler,
    )

    try:
        yield
    finally:
        await pub.stop()
        await blob.stop()
        await db.stop()

app = FastAPI(title="stable-ads-api", lifespan=lifespan)

app.include_router(jobs_router)
app.include_router(data_ingestion_router)

@app.get("/healthz")
async def healthz() -> dict[str, bool]:
    return {"ok": True}
