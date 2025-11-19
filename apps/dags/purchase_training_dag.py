from datetime import timedelta
import logging
from typing import Any
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from core.infra.db.async_db import AsyncDB
from core.infra.db.model import Base
from core.utils.run_in_thread import run_async_from_sync
from modules.ml import MLService

logger = logging.getLogger(__name__)

TRAIN_EPOCHS = int(Variable.get("TRAIN_EPOCHS", default_var=10))
BATCH_SIZE = int(Variable.get("TRAIN_BATCH_SIZE", default_var=64))
REUSE_TRAINING_RUN_ID = Variable.get("REUSE_TRAINING_RUN_ID", default_var=None)

default_args = {
    "owner": "stable-ads-ml-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

def init_db(**context) -> None:
    async def _start():
        db = AsyncDB()
        await db.start()
        try:
            async with db.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("ML training tables initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing database: {e}", exc_info=True)
            raise
        finally:
            await db.stop()
    
    try:
        run_async_from_sync(_start())
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}", exc_info=True)
        raise

def train_model(**context) -> dict[str, Any]:
    """Train purchase prediction model and mark features as used."""
    
    async def _train() -> dict[str, Any]:
        db = AsyncDB()
        await db.start()
        try:
            service = MLService(db.session_factory)
            result_dto = await service.train_model(
                epochs=TRAIN_EPOCHS,
                batch_size=BATCH_SIZE,
                reuse_training_run_id=REUSE_TRAINING_RUN_ID,
            )
            
            logger.info(
                "Training completed: model %s, feature_version %s, metrics: %s",
                result_dto.model_version,
                result_dto.feature_version_id,
                result_dto.metrics,
            )
            
            return result_dto.model_dump()
        except ValueError as e:
            logger.error(f"Training failed: {e}", exc_info=True)
            raise
        finally:
            await db.stop()
    
    try:
        result = run_async_from_sync(_train())
        context["ti"].xcom_push(key="training_result", value=result)
        return result
    except Exception as e:
        logger.error(f"Failed to train model: {e}", exc_info=True)
        raise

with DAG(
    "purchase_training_dag",
    default_args=default_args,
    description="Train purchase prediction model using computed features",
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ml", "training"],
    max_active_runs=1,
) as dag:
    init_db_task = PythonOperator(
        task_id="init_db",
        python_callable=init_db,
    )
    
    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )
    
    init_db_task >> train_task