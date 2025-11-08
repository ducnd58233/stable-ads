from pydantic import BaseModel, Field
from typing import Any

class AgentState(BaseModel):
    job_id: str
    prompt: str
    style: str = "neutral"
    duration_sec: int = Field(ge=5, le=600)
    messages: list[Any] = Field(default_factory=list)
    rounds: int = 0
    script: dict | None = None