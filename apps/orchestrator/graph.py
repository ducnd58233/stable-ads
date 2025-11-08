from .schema import ScriptSchema
from .state import AgentState
from langchain_core.messages import AIMessage, SystemMessage, HumanMessage, ToolMessage
from core.services.llm import get_client, fetch_time
from langgraph.graph import StateGraph, END

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

def node_tools(state: AgentState) -> AgentState:
    client = get_client()
    tools = [fetch_time]
    tool_map = {tool.name: tool for tool in tools}

    for _ in range(3):
        ai_msg, tool_calls = client.chat_with_tools(state.messages, tools)
        state.messages.append(ai_msg)
        state.rounds += 1
        if not tool_calls:
            break
        for tool_call in tool_calls:
            tool_obj = tool_map.get(tool_call.tool_name)
            if tool_obj:
                result = tool_obj.invoke(tool_call.arguments)
                state.messages.append(ToolMessage(content=str(result), tool_call_id=tool_call.tool_call_id))
    return state

def node_structured(state: AgentState) -> AgentState:
    client = get_client()
    script: ScriptSchema = client.structured(ScriptSchema, state.messages)
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