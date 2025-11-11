from enum import Enum

class IngestionStatus(str, Enum):
    PENDING = "PENDING"
    VALIDATING = "VALIDATING"
    UPLOADING = "UPLOADING"
    PUBLISHED = "PUBLISHED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

class FileType(str, Enum):
    CSV = "csv"