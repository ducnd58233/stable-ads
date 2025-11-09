from typing import Iterable, Sequence, Type, TypeVar

from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import BaseTool
from pydantic import BaseModel

from core.settings.config import get_settings
from langchain.chat_models import init_chat_model

T = TypeVar("T", bound=BaseModel)

class LLMToolSpec(BaseModel):
    name: str
    description: str
    schema: dict 

class ToolCall(BaseModel):
    tool_name: str
    arguments: dict
    tool_call_id: str

class LLMProvider:
    def __init__(self):
        cfg = get_settings().llm
        self.model = init_chat_model(
            model=cfg.model,
            temperature=0.0,
            timeout=10.0,
            api_key=cfg.api_key,
            base_url=cfg.base_url,
        )

    async def astructured(self, schema: Type[T], messages: Sequence[BaseMessage], **kwargs) -> T:
        prompt = ChatPromptTemplate.from_messages(messages)
        chain = prompt | self.model.with_structured_output(schema=schema)
        return await chain.ainvoke({})

    async def achat_with_tools(self, messages: Sequence[BaseMessage], tools: Iterable[BaseTool], **kwargs) -> tuple[AIMessage, list[ToolCall]]:
        prompt = ChatPromptTemplate.from_messages(messages)
        model = self.model.bind_tools(tools)
        ai_msg: AIMessage = await (prompt | model).ainvoke({})
        calls: list[ToolCall] = []
        for tc in ai_msg.tool_calls or []:
            calls.append(ToolCall(tool_name=tc["name"], arguments=tc["args"], tool_call_id=tc["id"]))
        return ai_msg, calls
