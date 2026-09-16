"""Shared fixtures. ``pythonpath = ["src"]`` in pyproject makes `tafw_ingest`
importable without an install."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures" / "dayforce"


@pytest.fixture
def load_fixture():
    def _load(name: str):
        return json.loads((FIXTURES / name).read_text(encoding="utf-8"))

    return _load


@pytest.fixture
def approved_entries(load_fixture):
    return load_fixture("tafw_EMP-001_approved.json")["Data"]
