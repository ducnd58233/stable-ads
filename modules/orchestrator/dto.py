from pydantic import BaseModel, Field

class ScriptBeat(BaseModel):
    scene: str
    narration: str
    broll_hint: str | None = None
    seconds: int

class ScriptSchema(BaseModel):
    title: str
    style: str | None = None
    total_seconds: int = Field(ge=5, le=1800)
    beats: list[ScriptBeat]
    camera: str | None = None