from enum import Enum


class Buckets(str, Enum):
    VIDEOS = "videos"
    RAW_DATA = "raw-data"
    TRANSFORMED_DATA = "transformed-data"
    FEATURES = "features"
    ML_MODELS = "models"
    CHECKPOINTS = "checkpoints"