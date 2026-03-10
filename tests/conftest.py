"""
Shared test fixtures and configuration.
"""
from __future__ import annotations

import os
import tempfile
from decimal import Decimal
from typing import Generator

import pytest

# ---------------------------------------------------------------------------
# Sample NEM12 data
# ---------------------------------------------------------------------------

NEM12_SAMPLE = """100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.461,0.810,0.568,1.234,1.353,1.507,1.344,1.773,0.848,1.271,0.895,1.327,1.013,1.793,0.988,0.985,0.876,0.555,0.760,0.938,0.566,0.512,0.970,0.760,0.731,0.615,0.886,0.531,0.774,0.712,0.598,0.670,0.587,0.657,0.345,0.231,A,,,20050310121004,20050310182204
300,20050302,0,0,0,0,0,0,0,0,0,0,0,0,0.235,0.567,0.890,1.123,1.345,1.567,1.543,1.234,0.987,1.123,0.876,1.345,1.145,1.173,1.265,0.987,0.678,0.998,0.768,0.954,0.876,0.845,0.932,0.786,0.999,0.879,0.777,0.578,0.709,0.772,0.625,0.653,0.543,0.599,0.432,0.432,A,,,20050310121004,20050310182204
500,O,S01009,20050310121004,
200,NEM1201010,E1E2,2,E2,,01009,kWh,30,20050610
300,20050301,0,0,0,0,0,0,0,0,0,0,0,0,0.154,0.460,0.770,1.003,1.059,1.750,1.423,1.200,0.980,1.111,0.800,1.403,1.145,1.173,1.065,1.187,0.900,0.998,0.768,1.432,0.899,1.211,0.873,0.786,1.504,0.719,0.817,0.780,0.709,0.700,0.565,0.655,0.543,0.786,0.430,0.432,A,,,20050310121004,
900
"""

NEM12_MINIMAL = """100,NEM12,200506081149,UNITEDDP,NEMMCO
200,NEM0000001,E1,1,E1,N1,00001,kWh,30,20050610
300,20050301,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,1.000,A,,,20050310121004,
900
"""

NEM13_SAMPLE = """100,NEM13,200506081149,UNITEDDP,NEMMCO
250,NEM1301001,E1,1,kWh,001,20050301
350,20050201,20050301,123.456,20050301,130.456,F,
900
"""


@pytest.fixture
def nem12_file(tmp_path) -> str:
    """Write sample NEM12 data to a temp file and return its path."""
    p = tmp_path / "sample_nem12.csv"
    p.write_text(NEM12_SAMPLE)
    return str(p)


@pytest.fixture
def nem12_minimal_file(tmp_path) -> str:
    """Write minimal NEM12 data (1 NMI, 1 day, all 1.000) to temp file."""
    p = tmp_path / "minimal_nem12.csv"
    p.write_text(NEM12_MINIMAL)
    return str(p)


@pytest.fixture
def nem13_file(tmp_path) -> str:
    """Write sample NEM13 header to a temp file and return its path."""
    p = tmp_path / "sample_nem13.csv"
    p.write_text(NEM13_SAMPLE)
    return str(p)


@pytest.fixture
def empty_file(tmp_path) -> str:
    p = tmp_path / "empty.csv"
    p.write_text("")
    return str(p)


@pytest.fixture
def malformed_file(tmp_path) -> str:
    content = "100,NEM12,20050101,X,Y\n200,TOOLONGFORNMI,E1,1,E1,N1,001,kWh,30,20050610\n"
    p = tmp_path / "malformed.csv"
    p.write_text(content)
    return str(p)
