from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from .model import PurchasePrediction

class MLRepository:
    async def create(
        self,
        s: AsyncSession,
        prediction: PurchasePrediction,
    ) -> PurchasePrediction:
        s.add(prediction)
        await s.flush()
        s.expunge(prediction)
        return prediction
    
    async def get_by_user_id(
        self,
        s: AsyncSession,
        user_id: int,
        limit: int = 10,
    ) -> list[PurchasePrediction]:
        stmt = (
            select(PurchasePrediction)
            .where(PurchasePrediction.user_id == user_id)
            .order_by(desc(PurchasePrediction.created_at))
            .limit(limit)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def get_recent_predictions(
        self,
        s: AsyncSession,
        limit: int = 100,
        will_purchase: bool | None = None,
    ) -> list[PurchasePrediction]:
        stmt = select(PurchasePrediction)
        if will_purchase is not None:
            stmt = stmt.where(PurchasePrediction.will_purchase == will_purchase)
        stmt = stmt.order_by(desc(PurchasePrediction.created_at)).limit(limit)
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def get_by_session_id(
        self,
        s: AsyncSession,
        session_id: str,
        limit: int = 10,
    ) -> list[PurchasePrediction]:
        """
        Get predictions by session ID (for anonymous users).
        
        Args:
            s: Database session
            session_id: Session identifier
            limit: Maximum number of predictions to return
        
        Returns:
            List of predictions ordered by most recent first
        """
        stmt = (
            select(PurchasePrediction)
            .where(PurchasePrediction.session_id == session_id)
            .order_by(desc(PurchasePrediction.created_at))
            .limit(limit)
        )
        result = await s.execute(stmt)
        return list(result.scalars().all())
    
    async def update_ad_job_id(
        self,
        s: AsyncSession,
        prediction_id: str,
        ad_job_id: str,
    ) -> None:
        prediction = await s.get(PurchasePrediction, prediction_id)
        if prediction:
            prediction.ad_job_id = ad_job_id
            await s.flush()

