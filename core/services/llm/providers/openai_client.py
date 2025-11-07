from typing import Iterable, Type, TypeVar
from langchain_core.tools import BaseTool
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from ..interfaces import LLMClient, ToolCall
from ..registry import register_provider
from core.settings.config import get_settings
from langchain_core.prompts import ChatPromptTemplate

T = TypeVar("T", bound=BaseModel)

@register_provider("openai")
class OpenAIProvider(LLMClient):
    def __init__(self, model: str = "gpt-4o-mini"):
        cfg = get_settings().llm
        self.model = ChatOpenAI(
            model=model,
            temperature=0.0,
            timeout=10.0,
            api_key=cfg.api_key,
            base_url=cfg.base_url,
        )

    def structured(self, schema: Type[T], system_prompt: str, user_prompt: str, **kwargs) -> T:
        prompt = ChatPromptTemplate.from_messages([SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)])
        chain = prompt | self.model.with_structured_output(schema=schema)
        return chain.invoke(kwargs.get("input", {}))

    def chat_with_tools(
        self, 
        system_prompt: str, 
        user_prompt: str, 
        tools: Iterable[BaseTool], 
        **kwargs
    ) -> tuple[str, list[ToolCall]]:
        prompt = ChatPromptTemplate.from_messages([SystemMessage(content=system_prompt), HumanMessage(content=user_prompt)])
        model_with_tools = self.model.bind_tools(tools)

        ai_msg: AIMessage = (prompt | model_with_tools).invoke({})
        text = ai_msg.content if isinstance(ai_msg.content, str) else ""
        calls: list[ToolCall] = []
        for tc in ai_msg.tool_calls or []:
            calls.append(ToolCall(tool_name=tc["name"], arguments=tc["args"]))
        return text, calls