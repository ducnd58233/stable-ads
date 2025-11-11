from enum import Enum


class Topics(str, Enum):
    RENDER_REQUESTS = "render-requests"
    RENDER_RESULTS = "render-results"
    RENDER_DLQ = "render-dlq"

    DATA_INGESTION_REQUESTS = "data-ingestion-requests"