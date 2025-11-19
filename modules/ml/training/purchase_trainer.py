import logging
from pathlib import Path
import torch
import torch.nn as nn
from torch.optim import Adam
from torch.utils.data import DataLoader
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score
import numpy as np
from core.infra.blob.registry import get_blob
from core.infra.blob.buckets import Buckets
from modules.ml.models.purchase_model import PurchasePredictionModel

logger = logging.getLogger(__name__)

class PurchaseTrainer:
    def __init__(
        self,
        model: PurchasePredictionModel,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
    ):
        self.model = model.to(device)
        self.device = device
        self.criterion = nn.BCELoss()
        self.optimizer = Adam(self.model.parameters(), lr=0.001)
    
    def train_epoch(
        self,
        train_loader: DataLoader,
    ) -> float:
        self.model.train()
        total_loss = 0.0
        n_batches = 0
        
        for X, y in train_loader:
            X = X.to(self.device)
            y = y.to(self.device)
            
            self.optimizer.zero_grad()
            outputs = self.model(X)
            loss = self.criterion(outputs, y)
            loss.backward()
            self.optimizer.step()
            
            total_loss += loss.item()
            n_batches += 1
        
        return total_loss / n_batches if n_batches > 0 else 0.0
    
    def evaluate(
        self,
        val_loader: DataLoader,
    ) -> dict[str, float]:
        self.model.eval()
        all_preds = []
        all_labels = []
        total_loss = 0.0
        n_batches = 0
        
        with torch.no_grad():
            for X, y in val_loader:
                X = X.to(self.device)
                y = y.to(self.device)
                
                outputs = self.model(X)
                loss = self.criterion(outputs, y)
                
                all_preds.extend(outputs.cpu().numpy())
                all_labels.extend(y.cpu().numpy())
                total_loss += loss.item()
                n_batches += 1
        
        avg_loss = total_loss / n_batches if n_batches > 0 else 0.0
        
        all_preds = np.array(all_preds)
        all_labels = np.array(all_labels)
        pred_binary = (all_preds > 0.5).astype(int)
        
        unique_labels = np.unique(all_labels)
        has_both_classes = len(unique_labels) > 1
        
        if has_both_classes:
            auc = float(roc_auc_score(all_labels, all_preds))
        else:
            auc = 0.0
            logger.warning(
                "Only one class present in evaluation set (class=%s). AUC set to 0.0.",
                int(unique_labels[0]) if len(unique_labels) > 0 else "unknown",
            )
        
        metrics = {
            "loss": float(avg_loss),
            "auc": auc,
            "accuracy": float(accuracy_score(all_labels, pred_binary)),
            "precision": float(precision_score(all_labels, pred_binary, zero_division=0)),
            "recall": float(recall_score(all_labels, pred_binary, zero_division=0)),
        }
        
        return metrics
    
    def train(
        self,
        train_loader: DataLoader,
        val_loader: DataLoader,
        epochs: int = 10,
    ) -> dict[str, list[float]]:
        history = {
            "train_loss": [],
            "val_loss": [],
            "val_auc": [],
        }
        
        best_val_auc = 0.0
        
        for epoch in range(epochs):
            train_loss = self.train_epoch(train_loader)
            val_metrics = self.evaluate(val_loader)
            
            history["train_loss"].append(train_loss)
            history["val_loss"].append(val_metrics["loss"])
            history["val_auc"].append(val_metrics["auc"])
            
            logger.info(
                "Epoch %s/%s: train_loss=%.4f, val_loss=%.4f, val_auc=%.4f",
                epoch + 1,
                epochs,
                train_loss,
                val_metrics["loss"],
                val_metrics["auc"],
            )
            
            if val_metrics["auc"] > best_val_auc:
                best_val_auc = val_metrics["auc"]
        
        return history
    
    async def save_model(
        self,
        model_version: str,
        feature_version_id: str,
    ) -> str:
        temp_path = Path(f"/tmp/model_{model_version}.pt")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        
        torch.save({
            "model_state_dict": self.model.state_dict(),
            "model_config": {
                "input_dim": self.model.network[0].in_features,
            },
        }, temp_path)
        
        blob = get_blob()
        await blob.start()
        try:
            model_key = f"models/purchase/{model_version}.pt"
            with open(temp_path, "rb") as f:
                model_data = f.read()
            
            await blob.upload(
                Buckets.ML_MODELS,
                model_key,
                model_data,
                content_type="application/octet-stream",
            )
            
            temp_path.unlink()
            return model_key
        finally:
            await blob.stop()