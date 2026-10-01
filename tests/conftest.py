"""Test Configuration."""

import sys

import pytest


def pytest_configure(config: pytest.Config):
    if sys.version_info < (3, 11):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3.10 reached its end of life on 2026-10:FutureWarning",
        )

    elif sys.version_info < (3, 12):
        config.addinivalue_line(
            "filterwarnings",
            "once:Python 3.11 will reach its end of life on 2027-10:FutureWarning",
        )
