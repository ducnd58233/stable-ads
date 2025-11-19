from fastapi import FastAPI
from contextlib import asynccontextmanager
from core.infra.db.async_db import AsyncDB
from core.infra.db.model import Base
from core.infra.blob.registry import get_blob 
from core.infra.cache.registry import get_cache
from core.infra.mq import create_publisher, create_marshaler
from core.infra.container import Infra
from core.settings.config import get_settings
from modules.jobs import Job
from modules.data_ingestion import DataIngestion
from modules.ml import PurchasePrediction
from modules.feature_store import FeatureVersion, OfflineFeature, MLTrainingRun, ProductionModel
from modules.warehouse import WarehouseData
from modules.jobs import jobs_router
from modules.data_ingestion import data_ingestion_router
from modules.ml import ml_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    s = get_settings()

    db = AsyncDB()
    await db.start()
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    cache = get_cache()
    await cache.start()

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
        cache=cache,
    )

    try:
        yield
    finally:
        await pub.stop()
        await blob.stop()
        await db.stop()
        await cache.stop()
        
app = FastAPI(title="stable-ads-api", lifespan=lifespan)

app.include_router(jobs_router)
app.include_router(data_ingestion_router)
app.include_router(ml_router)

@app.get("/healthz")
async def healthz() -> dict[str, bool]:
    return {"ok": True}
