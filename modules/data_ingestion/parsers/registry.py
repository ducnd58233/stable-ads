from typing import Callable, Type
from .base import FileParser
from .csv_parser import CSVParser

_PARSER_REGISTRY: dict[str, Type[FileParser]] = {}

def register_parser(name: str) -> Callable[[Type[FileParser]], Type[FileParser]]:
    def _wrap(cls: Type[FileParser]) -> Type[FileParser]:
        _PARSER_REGISTRY[name] = cls
        return cls
    return _wrap

def get_parser(file_path: str) -> FileParser:
    for parser_cls in _PARSER_REGISTRY.values():
        parser = parser_cls()
        if parser.can_parse(file_path):
            return parser
    raise ValueError(f"No parser found for file: {file_path}")

register_parser("csv")(CSVParser)