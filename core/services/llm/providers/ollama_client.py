from typing import Iterable, Type, TypeVar
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.tools import BaseTool
from pydantic import BaseModel
from langchain_ollama import ChatOllama
from ..interfaces import LLMClient, ToolCall
from ..registry import register_provider
from core.settings.config import get_settings
from langchain_core.prompts import ChatPromptTemplate

T = TypeVar("T", bound=BaseModel)

@register_provider("ollama")
class OllamaProvider(LLMClient):
    def __init__(self, model: str = "llama3.1:8b"):
        cfg = get_settings().llm
        self.model = ChatOllama(model=model, base_url=cfg.base_url)

    def structured(self, schema: Type[T], messages: list[BaseMessage], **kwargs) -> T:
        prompt = ChatPromptTemplate.from_messages(messages)
        chain = prompt | self.model.with_structured_output(schema=schema)
        return chain.invoke(kwargs.get("input", {}))

    def chat_with_tools(
        self, 
        messages: list[BaseMessage], 
        tools: Iterable[BaseTool], 
        **kwargs
    ) -> tuple[AIMessage, list[ToolCall]]:
        prompt = ChatPromptTemplate.from_messages(messages)
        model_with_tools = self.model.bind_tools(tools)

        ai_msg: AIMessage = (prompt | model_with_tools).invoke({})
        calls: list[ToolCall] = []
        for tc in ai_msg.tool_calls or []:
            calls.append(ToolCall(tool_name=tc["name"], arguments=tc["args"], tool_call_id=tc["id"]))
        return ai_msg, calls