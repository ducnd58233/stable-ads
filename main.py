import os
from contextlib import asynccontextmanager
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from apps.orchestrator.graph import build_graph
from apps.orchestrator.mapper import to_render_request
from apps.orchestrator.schema import ScriptSchema
from apps.orchestrator.state import AgentState
from core.services.render import VideoRenderer
from core.services.render.backend import RenderRequest
from core.settings.config import get_settings

class ServicesRuntime(BaseModel):
    graph: object
    renderer: VideoRenderer
    fps: int

    model_config = ConfigDict(
        arbitrary_types_allowed=True
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("generated", exist_ok=True)
    os.makedirs("generated/videos", exist_ok=True)
    graph = build_graph()
    renderer = VideoRenderer()
    fps = get_settings().diffuser.fps

    app.state.runtime = ServicesRuntime(graph=graph, renderer=renderer, fps=fps)

    try:
        yield
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        renderer.close()

app = FastAPI(
    title="Stable Ads",
    description="Stable Ads",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

def runtime_dep() -> ServicesRuntime:
    return app.state.runtime

class GenerateRequest(BaseModel):
    prompt: str = Field(..., min_length=3)
    style: str | None = "neutral"
    duration_sec: int = Field(ge=5, le=600, default=30)

class GenerateResponse(BaseModel):
    script: dict
    video_path: str


@app.post("/v1/generate", response_model=GenerateResponse)
def generate_script(req: GenerateRequest, runtime: ServicesRuntime = Depends(runtime_dep)):
    # 1) Build initial agent state
    state = AgentState(job_id="test-inline", prompt=req.prompt, style=req.style or "neutral", duration_sec=req.duration_sec)

    # 2) Run the graph synchronously
    out_dict = runtime.graph.invoke(state)
    out = AgentState(**out_dict)
    if not out.script:
        raise HTTPException(500, "No script produced by agent")
    script = ScriptSchema.model_validate(out.script)

    # 3) Map agent schema -> renderer DTO (service-local)
    render_req: RenderRequest = to_render_request(script, fps=runtime.fps)

    # 4) Render a single combined clip (all beats concatenated)
    video_path = runtime.renderer.render(render_req)

    return GenerateResponse(script=script.model_dump(), video_path=video_path)
