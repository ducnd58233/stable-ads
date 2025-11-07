from typing import Type, TypeVar, Any, Protocol, Iterable
from pydantic import BaseModel
from langchain_core.tools import BaseTool

T = TypeVar("T", bound=BaseModel)

class ToolCall(BaseModel):
    tool_name: str
    arguments: dict[str, Any]

class LLMClient(Protocol):
    def structured(self, schema: Type[T], system_prompt: str, user_prompt: str, **kwargs) -> T:
        ...

    def chat_with_tools(self, system_prompt: str, user_prompt: str, tools: Iterable[BaseTool], **kwargs) -> tuple[str, list[ToolCall]]:
        ...
