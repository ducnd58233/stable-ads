import logging
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from modules.ml.service import MLService
from modules.ml.dto import PurchasePredictionDTO
from modules.jobs.service import JobService
from modules.jobs.model import Job
from modules.feature_store.service import FeatureStoreService
from modules.feature_store.dto import UserFeatureDTO
from core.settings.config import get_settings

logger = logging.getLogger(__name__)

class PurchaseOrchestrator:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._sf = session_factory
        self._ml_service = MLService(session_factory)
        self._job_service = JobService(session_factory)
        self._feature_service = FeatureStoreService(session_factory)
        self._settings = get_settings()
    
    def _generate_personalized_prompt(
        self,
        user_features: UserFeatureDTO,
        prediction: PurchasePredictionDTO,
    ) -> str:
        """
        Generate a personalized prompt for video ad based on user features.
        Optimized for video generation models with clear, visual, action-oriented language.
        
        Args:
            user_features: User feature data
            prediction: Purchase prediction result
        
        Returns:
            Personalized prompt string suitable for video ad generation
        """
        # Build visual and action-oriented prompt parts
        visual_elements = []
        product_focus = []
        style_context = []
        
        # Product categories (visual focus)
        if user_features.top_category_1:
            product_focus.append(user_features.top_category_1)
        if user_features.top_category_2 and user_features.top_category_2 != user_features.top_category_1:
            product_focus.append(user_features.top_category_2)
        
        # Brand preferences
        brands = []
        if user_features.top_brand_1:
            brands.append(user_features.top_brand_1)
        if user_features.top_brand_2 and user_features.top_brand_2 != user_features.top_brand_1:
            brands.append(user_features.top_brand_2)
        
        # Price range and quality context
        if user_features.price_range_avg:
            if user_features.price_range_avg > 100:
                style_context.append("premium")
                visual_elements.append("luxury aesthetic")
            elif user_features.price_range_avg < 50:
                style_context.append("affordable")
                visual_elements.append("value-focused presentation")
            else:
                style_context.append("mid-range")
        
        # Purchase behavior and intent
        if user_features.purchase_count and user_features.purchase_count > 0:
            style_context.append("returning customer")
            visual_elements.append("trust-building elements")
        else:
            style_context.append("new customer")
            visual_elements.append("welcoming introduction")
        
        if prediction.purchase_probability >= 0.8:
            visual_elements.append("high-intent showcase")
            style_context.append("conversion-focused")
        elif prediction.purchase_probability >= 0.7:
            visual_elements.append("engaging presentation")
        
        prompt_sections = []
        
        # Main product focus
        if product_focus:
            products = " and ".join(product_focus)
            prompt_sections.append(f"Showcase {products} products")
        else:
            prompt_sections.append("Showcase featured products")
        
        if brands:
            brand_text = " and ".join(brands)
            prompt_sections.append(f"from {brand_text}")
        
        if visual_elements:
            visual_text = ", ".join(visual_elements[:2])
            prompt_sections.append(f"with {visual_text}")
        
        if style_context:
            style_text = ", ".join(style_context[:2])
            prompt_sections.append(f"tailored for {style_text}")
        
        prompt = ". ".join(prompt_sections) + "."
        
        prompt += " Dynamic product shots, smooth transitions, clear call-to-action."
        
        logger.debug(
            "Generated personalized video ad prompt for user %s (probability=%.2f): %s",
            user_features.user_id,
            prediction.purchase_probability,
            prompt,
        )
        
        return prompt
    
    async def orchestrate_purchase_prediction_and_ad(
        self,
        user_id: int,
        model_version: str | None = None,
        lookback_days: int | None = None,
    ) -> Job | None:
        """
        Orchestrate purchase prediction and video ad generation.
        
        Flow:
        1. Predict purchase probability (supports new and existing customers)
        2. If will_purchase=True (probability >= threshold):
           - Get user features (for prompt generation)
           - Generate personalized prompt
           - Create video ad generation job
           - Link prediction to job
           - Return job
        3. If will_purchase=False:
           - Return None (no ad generated)
        
        Args:
            user_id: User identifier
            model_version: Optional specific model version to use
            lookback_days: Optional lookback days for new customers
        
        Returns:
            Job if ad generation was triggered, None otherwise
        """
        from modules.ml.repository import MLRepository
        
        try:
            prediction = await self._ml_service.predict_purchase(
                user_id, 
                model_version,
                lookback_days,
            )
            
            logger.info(
                "Prediction for user %s: probability=%.4f, will_purchase=%s",
                user_id,
                prediction.purchase_probability,
                prediction.will_purchase,
            )
            
            if not prediction.will_purchase:
                logger.info(
                    "User %s prediction (%.4f) below threshold (%.4f), skipping ad generation",
                    user_id,
                    prediction.purchase_probability,
                    self._settings.ml_prediction.purchase_probability_threshold,
                )
                return None
            
            user_features = await self._feature_service.get_user_features_for_prediction(user_id)
            if not user_features:
                lookback_days = lookback_days or self._settings.ml_prediction.new_customer_lookback_days
                user_features = await self._feature_service.compute_features_for_user(user_id, lookback_days)
            
            if not user_features:
                logger.warning(
                    "User %s has no features available for prompt generation, using default prompt",
                    user_id,
                )
                from modules.feature_store.dto import UserFeatureDTO
                user_features = UserFeatureDTO(
                    user_id=user_id,
                    feature_version_id=None,
                    session_count=None,
                    session_duration_avg=None,
                    page_views_per_session=None,
                    total_spend=None,
                    purchase_count=None,
                    avg_order_value=None,
                    days_since_last_purchase=None,
                    days_since_last_event=None,
                    days_since_first_event=None,
                    top_category_1=None,
                    top_category_2=None,
                    top_brand_1=None,
                    top_brand_2=None,
                    price_range_min=None,
                    price_range_max=None,
                    price_range_avg=None,
                    purchased=0,
                )
            
            # Generate personalized prompt
            prompt = self._generate_personalized_prompt(user_features, prediction)
            
            # Create job with personalized parameters
            from modules.jobs.dto import CreateJobRequest
            job_request = CreateJobRequest(
                prompt=prompt,
                style="neutral",
                duration_sec=30,
            )
            
            job = await self._job_service.create(job_request)
            
            ml_repo = MLRepository()
            async with self._sf() as s, s.begin():
                predictions = await ml_repo.get_by_user_id(s, user_id, limit=1)
                if predictions:
                    await ml_repo.update_ad_job_id(s, predictions[0].id, job.id)
                    logger.debug(
                        "Linked prediction %s to ad job %s for user %s",
                        predictions[0].id,
                        job.id,
                        user_id,
                    )
            
            logger.info(
                "Created video ad job %s for user %s (prediction: %.4f, will_purchase=%s)",
                job.id,
                user_id,
                prediction.purchase_probability,
                prediction.will_purchase,
            )
            
            return job
            
        except Exception as e:
            logger.error(
                "Error orchestrating prediction and ad for user %s: %s",
                user_id,
                str(e),
                exc_info=True,
            )
            raise

