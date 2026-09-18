from __future__ import annotations

import pytest
from pydantic import ValidationError

from tafw_ingest.config import Settings


def test_dayforce_max_rps_rejects_above_documented_ceiling():
    Settings(dayforce_max_rps=10)  # ok, at the ceiling
    with pytest.raises(ValidationError):
        Settings(dayforce_max_rps=11)


def test_dayforce_max_rpm_rejects_above_documented_ceiling():
    Settings(dayforce_max_rpm=100)  # ok, at the ceiling
    with pytest.raises(ValidationError):
        Settings(dayforce_max_rpm=101)
