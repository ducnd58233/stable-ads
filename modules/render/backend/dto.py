from pydantic import BaseModel, Field


class BeatSpec(BaseModel):
    scene: str
    narration: str
    broll_hint: str | None = None
    seconds: int = Field(ge=1, le=120)

class RenderRequest(BaseModel):
    title: str
    style: str | None = "neutral"
    beats: list[BeatSpec] = Field(min_length=1, default_factory=list)
    fps: int = Field(ge=4, le=60)