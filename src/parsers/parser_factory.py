"""
Parser factory – resolves the correct parser for a given file.

Design:
  - Open/Closed Principle: adding a new format only requires registering a new
    parser class; no existing code changes.
  - Detection is performed by each parser's classmethod, so the factory has no
    format-specific logic.
"""
from __future__ import annotations

import structlog

from src.parsers.base_parser import BaseParser
from src.parsers.nem12_parser import NEM12Parser
from src.parsers.nem13_parser import NEM13Parser

logger = structlog.get_logger(__name__)

# Registry order matters: first match wins.
_REGISTRY: list[type[BaseParser]] = [
    NEM12Parser,
    NEM13Parser,
]


class ParserFactory:
    """
    Factory that maps an input file to the appropriate parser.

    Usage::

        parser = ParserFactory.get_parser("/data/meters.csv")
        result = parser.parse("/data/meters.csv")
    """

    @staticmethod
    def get_parser(file_path: str) -> BaseParser:
        """
        Detect the file format and return an instantiated parser.

        Raises:
            ValueError: When no registered parser recognises the file format.
        """
        for cls in _REGISTRY:
            if cls.detect(file_path):
                logger.info(
                    "parser_factory.detected",
                    format=cls.__name__,
                    file=file_path,
                )
                return cls()

        raise ValueError(
            f"Unrecognised file format: '{file_path}'. "
            f"Supported formats: {[c().format_name for c in _REGISTRY]}"
        )

    @staticmethod
    def get_format(file_path: str) -> str:
        """Return the format name without constructing a full parser."""
        for cls in _REGISTRY:
            if cls.detect(file_path):
                # instantiate temporarily just to read format_name
                return cls().format_name
        return "UNKNOWN"

    @staticmethod
    def register(parser_class: type[BaseParser]) -> None:
        """
        Register an additional parser at runtime.

        Useful for plugins or test doubles.
        """
        if parser_class not in _REGISTRY:
            _REGISTRY.insert(0, parser_class)
            logger.info(
                "parser_factory.registered",
                parser=parser_class.__name__,
            )
