from functools import lru_cache
from .dto import ScriptSchema
from .domain import AgentState
from langchain_core.messages import SystemMessage, HumanMessage, ToolMessage
from modules.llm import LLMProvider, fetch_time
from langgraph.graph import StateGraph, END
from core.utils.run_in_thread import to_thread

SYSTEM_PROMPT = (
    "You are a senior video scriptwriter. Use tools to check facts or timing if needed. "
    "Keep outputs concise and voice-over friendly."
)

def node_init(state: AgentState) -> AgentState:
    state.messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=f"Topic: {state.prompt}\nStyle: {state.style}\n"
                             f"Target duration: {state.duration_sec}s.\nIf you need facts/timing, call tools.")
    ]
    state.rounds = 0
    return state

async def node_tools(state: AgentState) -> AgentState:
    client = LLMProvider()
    tools = [fetch_time]
    tool_map = {tool.name: tool for tool in tools}

    for _ in range(3):
        ai_msg, tool_calls = await client.achat_with_tools(state.messages, tools)
        state.messages.append(ai_msg)
        state.rounds += 1
        if not tool_calls:
            break
        for tool_call in tool_calls:
            tool_obj = tool_map.get(tool_call.tool_name)
            if tool_obj:
                result = await to_thread(tool_obj.invoke, tool_call.arguments)
                state.messages.append(ToolMessage(content=str(result), tool_call_id=tool_call.tool_call_id))
    return state

async def node_structured(state: AgentState) -> AgentState:
    client = LLMProvider()
    script: ScriptSchema = await client.astructured(ScriptSchema, state.messages)
    state.script = script.model_dump()
    return state

def build_graph():
    g = StateGraph(AgentState)
    g.add_node("init", node_init)
    g.add_node("tools", node_tools)
    g.add_node("structured", node_structured)
    g.set_entry_point("init")
    g.add_edge("init", "tools")
    g.add_edge("tools", "structured")
    g.add_edge("structured", END)
    return g.compile()

@lru_cache(maxsize=1)
def get_graph():
    return build_graph()