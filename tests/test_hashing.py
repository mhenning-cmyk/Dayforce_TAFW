"""Locks the FROZEN record-hash spec. If one of these breaks, a hash-spec
change slipped in and every staging row is about to be re-keyed - stop."""

from __future__ import annotations

import datetime as _dt
import hashlib

import pytest

from tafw_ingest.hashing import canonical_string, normalize_hours, record_hash

# Golden values - computed once, must never move without a HASH_SPEC_VERSION bump.
GOLDEN = {
    ("ABC123", "2026-03-14", "VACATION", 8): (
        "ABC123|2026-03-14|VACATION|8.00",
        "aaeadb0cbd49c96ac306861c948719e3c0079acde65afdbde61e129dc1798c24",
    ),
    ("EMP-001", "2026-07-01", "PERSONAL", "4.5"): (
        "EMP-001|2026-07-01|PERSONAL|4.50",
        "edd791cbc8a8bf352a75e412eb41458347aa2654bfd4b899f249195d40da547b",
    ),
}


@pytest.mark.parametrize(("args", "expected"), list(GOLDEN.items()))
def test_golden_values(args, expected):
    canonical, digest = expected
    assert canonical_string(*args) == canonical
    assert record_hash(*args) == digest
    # digest really is sha256 of that exact string
    assert hashlib.sha256(canonical.encode()).hexdigest() == digest


def test_normalization_is_format_insensitive():
    base = record_hash("ABC123", "2026-03-14", "VACATION", 8)
    variants = [
        ("  abc123 ", "2026-03-14", "vacation", 8),
        ("ABC123", _dt.date(2026, 3, 14), "VACATION", "8"),
        ("ABC123", "2026-03-14T09:30:00Z", "VACATION", 8.0),
        ("ABC123", "2026-03-14T00:00:00", " Vacation ", "8.000"),
    ]
    for v in variants:
        assert record_hash(*v) == base


def test_material_change_changes_hash():
    a = record_hash("ABC123", "2026-03-14", "VACATION", 8)
    assert record_hash("ABC123", "2026-03-14", "VACATION", 4) != a  # hours
    assert record_hash("ABC123", "2026-03-14", "PERSONAL", 8) != a  # type
    assert record_hash("ABC123", "2026-03-15", "VACATION", 8) != a  # date
    assert record_hash("ABC124", "2026-03-14", "VACATION", 8) != a  # employee


def test_hours_rules():
    assert normalize_hours(8) == "8.00"
    assert normalize_hours("8.5") == "8.50"
    assert normalize_hours(0) == "0.00"
    with pytest.raises(ValueError):
        normalize_hours(-1)
    with pytest.raises(ValueError):
        normalize_hours("not-a-number")
