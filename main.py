from contextlib import asynccontextmanager
from langchain_core.tools import tool
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from core.services.llm import get_client

@asynccontextmanager
async def lifespan(_: FastAPI):
    yield

app = FastAPI(
    title="Stable Ads",
    description="Stable Ads",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

SYSTEM_PROMPT = (
    "You are a senior video scriptwriter. Use tools only if needed to verify facts, "
    "then produce a concise, voice-over friendly script with clear beats and b-roll hints."
)

@tool("fetch_time", return_direct=False)
def fetch_time(timezone: str | None = None) -> str:
    """Return current time; optional IANA timezone."""
    import datetime, zoneinfo
    try:
        now = datetime.datetime.now(zoneinfo.ZoneInfo(timezone)) if timezone else datetime.datetime.utcnow()
        return now.isoformat()
    except Exception as e:
        return f"error: {e}"

class ScriptBeat(BaseModel):
    scene: str
    narration: str
    broll_hint: str | None = None
    seconds: int

class ScriptSchema(BaseModel):
    title: str
    style: str | None = None
    total_seconds: int = Field(ge=5, le=600)
    beats: list[ScriptBeat]

class ScriptRequest(BaseModel):
    prompt: str = Field(min_length=3)
    style: str | None = None
    duration_sec: int = Field(default=30, ge=5, le=600)

class ScriptResponse(BaseModel):
    script: ScriptSchema
    provider: str


@app.post("/v1/script", response_model=ScriptResponse)
def generate_script(req: ScriptRequest):
    """
    Two-phase flow:
    1) Let model optionally call tools (single pass, simple demo)
    2) Ask again for structured ScriptSchema
    """
    try:
        client = get_client(model="gpt-4o-mini")

        tools = [fetch_time]
        tool_map = {tool.name: tool for tool in tools}

        # Phase 1: tool call opportunity (single pass for demo)
        user = (
            f"Topic: {req.prompt}\n"
            f"Style: {req.style or 'neutral'}\n"
            f"Target duration: {req.duration_sec} seconds.\n"
            "If you need timing, call the fetch_time tool first."
        )
        _text, tool_calls = client.chat_with_tools(
            system_prompt=SYSTEM_PROMPT,
            user_prompt=user,
            tools=tools,
        )
        # (Optional) You can execute the tool_calls here and iterate, but we keep it one-shot.
        for tool_call in tool_calls:
            tool_obj = tool_map.get(tool_call.tool_name)
            if tool_obj:
                result = tool_obj.invoke(tool_call.arguments)
                print(f"tool result: {result}")

        # Phase 2: structured JSON output
        user2 = (
            f"Now produce ONLY a JSON object matching ScriptSchema.\n"
            f"Topic: {req.prompt}\n"
            f"Style: {req.style or 'neutral'}\n"
            f"Target duration: {req.duration_sec}\n"
            "Keep 3–6 beats; short narration lines; include broll_hint and per-beat seconds."
        )
        script: ScriptSchema = client.structured(ScriptSchema, SYSTEM_PROMPT, user2)
        return ScriptResponse(script=script, provider=type(client).__name__)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
