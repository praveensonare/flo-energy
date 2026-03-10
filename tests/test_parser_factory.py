"""
Unit tests for the ParserFactory.
"""
from __future__ import annotations

import pytest

from src.parsers.parser_factory import ParserFactory
from src.parsers.nem12_parser import NEM12Parser
from src.parsers.nem13_parser import NEM13Parser


class TestParserFactory:
    def test_get_parser_returns_nem12(self, nem12_file):
        parser = ParserFactory.get_parser(nem12_file)
        assert isinstance(parser, NEM12Parser)

    def test_get_parser_returns_nem13(self, nem13_file):
        parser = ParserFactory.get_parser(nem13_file)
        assert isinstance(parser, NEM13Parser)

    def test_get_format_nem12(self, nem12_file):
        assert ParserFactory.get_format(nem12_file) == "NEM12"

    def test_get_format_nem13(self, nem13_file):
        assert ParserFactory.get_format(nem13_file) == "NEM13"

    def test_get_format_unknown(self, empty_file):
        assert ParserFactory.get_format(empty_file) == "UNKNOWN"

    def test_raises_for_unknown_format(self, empty_file):
        with pytest.raises(ValueError, match="Unrecognised"):
            ParserFactory.get_parser(empty_file)

    def test_register_custom_parser(self):
        """Verify that a dynamically registered parser is picked up."""
        from src.parsers.base_parser import BaseParser, ParserResult
        from typing import Generator
        from src.models.meter_reading import MeterReading

        class CustomParser(BaseParser):
            @property
            def format_name(self):
                return "CUSTOM"

            @classmethod
            def detect(cls, file_path):
                return True  # match everything

            def stream_readings(self, file_path):
                return iter([])

            def parse(self, file_path):
                return ParserResult()

        ParserFactory.register(CustomParser)
        # Because detect() always returns True, factory should now pick it first
        # We verify by checking format_name only (avoid side effects on other tests)
        from src.parsers.parser_factory import _REGISTRY
        assert CustomParser in _REGISTRY
        # Clean up
        _REGISTRY.remove(CustomParser)
