import logging
import pandas as pd
import torch
from torch.utils.data import Dataset
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sklearn.model_selection import train_test_split
from modules.feature_store.repository import FeatureStoreRepository

logger = logging.getLogger(__name__)

class PurchaseDataset(Dataset):
    def __init__(
        self,
        features_df: pd.DataFrame,
        target_col: str = "purchased",
    ):
        self.features_df = features_df.copy()
        self.target_col = target_col
        
        feature_cols = [
            col for col in self.features_df.columns 
            if col not in ["user_id", target_col, "feature_version_id"]
        ]
        
        self.feature_cols = []
        for col in feature_cols:
            if self.features_df[col].dtype == "object":
                self.features_df[f"{col}_encoded"] = pd.Categorical(
                    self.features_df[col]
                ).codes
                self.feature_cols.append(f"{col}_encoded")
            else:
                self.feature_cols.append(col)
        
        self.features_df[self.feature_cols] = (
            self.features_df[self.feature_cols].fillna(0)
        )
        
        self.X = torch.tensor(
            self.features_df[self.feature_cols].values,
            dtype=torch.float32,
        )
        self.y = torch.tensor(
            self.features_df[target_col].values,
            dtype=torch.float32,
        )
        
        logger.info(
            "Dataset created: %s samples, %s features, positive class: %.2f%%",
            len(self.X),
            len(self.feature_cols),
            (self.y.sum().item() / len(self.y)) * 100 if len(self.y) > 0 else 0,
        )
    
    def __len__(self) -> int:
        return len(self.X)
    
    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.X[idx], self.y[idx]

class PurchaseDataLoader:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self._sf = session_factory
        self._repo = FeatureStoreRepository()
    
    async def load_features(
        self,
        feature_version_id: str | None = None,
        train_split: float = 0.7,
        val_split: float = 0.15,
        test_split: float = 0.15,
        random_seed: int = 42,
        reuse_training_run_id: str | None = None,
    ) -> tuple[PurchaseDataset, PurchaseDataset, PurchaseDataset]:
        async with self._sf() as s:
            offline_features = await self._repo.get_offline_features(
                s,
                feature_version_id,
            )
            
            if not offline_features:
                raise ValueError("No features found")
            
            features_data = [
                {
                    "user_id": f.user_id,
                    "session_count": f.session_count,
                    "session_duration_avg": f.session_duration_avg,
                    "page_views_per_session": f.page_views_per_session,
                    "total_spend": f.total_spend,
                    "purchase_count": f.purchase_count,
                    "avg_order_value": f.avg_order_value,
                    "days_since_last_purchase": f.days_since_last_purchase,
                    "days_since_last_event": f.days_since_last_event,
                    "days_since_first_event": f.days_since_first_event,
                    "top_category_1": f.top_category_1,
                    "top_category_2": f.top_category_2,
                    "top_brand_1": f.top_brand_1,
                    "top_brand_2": f.top_brand_2,
                    "price_range_min": f.price_range_min,
                    "price_range_max": f.price_range_max,
                    "price_range_avg": f.price_range_avg,
                    "purchased": f.purchased,
                }
                for f in offline_features
            ]
            features_df = pd.DataFrame(features_data)
        
        if reuse_training_run_id:
            async with self._sf() as s:
                existing_split = await self._repo.get_training_dataset_split(
                    s, reuse_training_run_id
                )
                if not existing_split:
                    raise ValueError(
                        f"Training run {reuse_training_run_id} not found or has no saved split"
                    )
                
                if existing_split.feature_version_id != feature_version_id:
                    logger.warning(
                        "Reusing split from different feature_version_id: %s (current: %s)",
                        existing_split.feature_version_id,
                        feature_version_id,
                    )
                
                train_df = features_df[features_df["user_id"].isin(existing_split.train_user_ids)]
                val_df = features_df[features_df["user_id"].isin(existing_split.val_user_ids)]
                test_df = features_df[features_df["user_id"].isin(existing_split.test_user_ids)]
                
                logger.info(
                    "Reused dataset split from training_run %s: train=%s (pos: %.2f%%), val=%s (pos: %.2f%%), test=%s (pos: %.2f%%)",
                    reuse_training_run_id,
                    len(train_df),
                    (train_df["purchased"].sum() / len(train_df) * 100) if len(train_df) > 0 else 0,
                    len(val_df),
                    (val_df["purchased"].sum() / len(val_df) * 100) if len(val_df) > 0 else 0,
                    len(test_df),
                    (test_df["purchased"].sum() / len(test_df) * 100) if len(test_df) > 0 else 0,
                )
        else:
            assert abs(train_split + val_split + test_split - 1.0) < 1e-6
            
            train_size = train_split
            temp_size = val_split + test_split
            
            train_df, temp_df = train_test_split(
                features_df,
                test_size=temp_size,
                random_state=random_seed,
                stratify=features_df["purchased"],
            )
            
            val_size = val_split / temp_size
            
            val_df, test_df = train_test_split(
                temp_df,
                test_size=(1 - val_size),
                random_state=random_seed,
                stratify=temp_df["purchased"],
            )
            
            logger.info(
                "Created new stratified dataset split: train=%s (pos: %.2f%%), val=%s (pos: %.2f%%), test=%s (pos: %.2f%%)",
                len(train_df),
                (train_df["purchased"].sum() / len(train_df) * 100) if len(train_df) > 0 else 0,
                len(val_df),
                (val_df["purchased"].sum() / len(val_df) * 100) if len(val_df) > 0 else 0,
                len(test_df),
                (test_df["purchased"].sum() / len(test_df) * 100) if len(test_df) > 0 else 0,
            )
        
        train_ds = PurchaseDataset(train_df)
        val_ds = PurchaseDataset(val_df)
        test_ds = PurchaseDataset(test_df)
        
        return train_ds, val_ds, test_ds    