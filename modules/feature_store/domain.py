from enum import Enum

class FeatureVersionStatus(str, Enum):
    PENDING = "PENDING"
    COMPUTING = "COMPUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

class ModelStatus(str, Enum):
    TRAINING = "TRAINING"
    EVALUATING = "EVALUATING"
    PRODUCTION = "PRODUCTION"
    ARCHIVED = "ARCHIVED"