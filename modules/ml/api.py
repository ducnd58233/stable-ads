from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi import Request
from core.infra.container import Infra
from modules.ml.service import MLService
from modules.ml.dto import PurchasePredictionDTO

router = APIRouter(prefix="/v1/ml", tags=["ml"])

def get_infra(req: Request) -> Infra:
    return req.app.state.infra

@router.post("/predict/user/{user_id}", response_model=PurchasePredictionDTO)
async def predict_purchase(
    user_id: int,
    model_version: str | None = Query(None, description="Specific model version to use"),
    lookback_days: int | None = Query(None, description="Number of days to look back for new customers (default from settings)"),
    infra: Infra = Depends(get_infra),
) -> PurchasePredictionDTO:
    """
    Predict purchase probability for a user.
    
    Supports both existing customers (from feature store) and new customers (on-the-fly feature computation).
    For new customers, features are computed from recent warehouse events within the lookback window.
    
    Args:
        user_id: User identifier
        model_version: Optional specific model version to use. If not provided, uses latest production model.
        lookback_days: Optional number of days to look back for new customers. 
                      If not provided, uses default from settings (typically 30 days).
                      Only used when user is not found in feature store.
    
    Returns:
        PurchasePredictionDTO with prediction results including:
        - purchase_probability: Predicted probability (0.0 to 1.0)
        - will_purchase: Boolean indicating if probability >= threshold
        - model_version: Model version used for prediction
        - feature_version_id: Feature version ID (or "realtime" for new customers)
        - model_path: Path to model file
    
    Raises:
        404: If user has no data available (no features and no events)
        500: If prediction fails due to system error
    """
    try:
        service = MLService(infra.session_factory)
        prediction = await service.predict_purchase(user_id, model_version, lookback_days)
        return prediction
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

@router.post("/predict/session/{session_id}", response_model=PurchasePredictionDTO)
async def predict_purchase_from_session(
    session_id: str,
    model_version: str | None = Query(None, description="Specific model version to use"),
    infra: Infra = Depends(get_infra),
) -> PurchasePredictionDTO:
    """
    Predict purchase probability for an anonymous user session.
    
    Used for users who haven't logged in yet (no user_id). Features are computed
    on-the-fly from session events in the warehouse.
    
    Args:
        session_id: Session identifier (for anonymous users)
        model_version: Optional specific model version to use. If not provided, uses latest production model.
    
    Returns:
        PurchasePredictionDTO with prediction results including:
        - purchase_probability: Predicted probability (0.0 to 1.0)
        - will_purchase: Boolean indicating if probability >= threshold
        - model_version: Model version used for prediction
        - feature_version_id: Feature version ID (typically "realtime_session")
        - model_path: Path to model file
        - session_id: Session identifier
        - user_id: None (for anonymous users)
    
    Raises:
        404: If session has no data available (no events)
        500: If prediction fails due to system error
    """
    try:
        service = MLService(infra.session_factory)
        prediction = await service.predict_purchase_from_session(session_id, model_version)
        return prediction
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

