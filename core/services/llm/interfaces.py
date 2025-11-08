from typing import Type, TypeVar, Any, Protocol, Iterable
from pydantic import BaseModel
from langchain_core.tools import BaseTool
from langchain_core.messages import AIMessage, BaseMessage

T = TypeVar("T", bound=BaseModel)

class ToolCall(BaseModel):
    tool_name: str
    arguments: dict[str, Any]
    tool_call_id: str

class LLMClient(Protocol):
    def structured(self, schema: Type[T], messages: list[BaseMessage], **kwargs) -> T:
        ...

    def chat_with_tools(self, messages: list[BaseMessage], tools: Iterable[BaseTool], **kwargs) -> tuple[AIMessage, list[ToolCall]]:
        ...
