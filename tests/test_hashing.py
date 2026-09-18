"""Locks the FROZEN record-hash spec. If one of these breaks, a hash-spec
change slipped in and every staging row is about to be re-keyed - stop."""

from __future__ import annotations

import datetime as _dt
import hashlib

import pytest

from tafw_ingest.hashing import canonical_string, normalize_hours, record_hash
from tafw_ingest.normalize import normalize_entry

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


def test_unique_hash_per_day_for_multi_day_request_with_daylist():
    """A single multi-day TAFW request (explicit DayList) yields one distinct
    record_hash per day, each independently recomputable from its own fields."""
    entry = {
        "TimeStart": "2026-03-16T00:00:00",
        "TimeEnd": "2026-03-18T00:00:00",
        "NetHours": 24.0,
        "ReasonName": "Vacation",
        "PayAdjustmentCodeName": "VAC",
        "DayList": [
            {"Date": "2026-03-16", "NetHours": 8.0},
            {"Date": "2026-03-17", "NetHours": 8.0},
            {"Date": "2026-03-18", "NetHours": 8.0},
        ],
    }
    records = normalize_entry("H5JN767", entry)

    assert [r.pto_date.isoformat() for r in records] == [
        "2026-03-16",
        "2026-03-17",
        "2026-03-18",
    ]
    hashes = [r.record_hash for r in records]
    assert len(hashes) == len(set(hashes))  # every day in the request is unique
    for r in records:
        assert r.record_hash == record_hash(
            r.employee_xref, r.pto_date, r.type_code, r.hours
        )


def test_unique_hash_per_day_for_multi_day_request_expanded():
    """Same, but via the expand_multi_day=True even-split fallback (no DayList)."""
    entry = {
        "TimeStart": "2026-03-16T00:00:00",
        "TimeEnd": "2026-03-18T00:00:00",
        "NetHours": 24.0,
        "ReasonName": "Vacation",
        "PayAdjustmentCodeName": "VAC",
    }
    records = normalize_entry("H5JN767", entry, expand_multi_day=True)

    assert len(records) == 3
    hashes = [r.record_hash for r in records]
    assert len(hashes) == len(set(hashes))


def test_hash_is_deterministic_across_repeated_ingests():
    """Re-normalizing the same request (e.g. seen again in the next sync
    cycle's overlapping window) must reproduce identical hashes, or the
    staging MERGE would treat unchanged days as new/changed records."""
    entry = {
        "TimeStart": "2026-03-16T00:00:00",
        "TimeEnd": "2026-03-18T00:00:00",
        "NetHours": 24.0,
        "ReasonName": "Vacation",
        "PayAdjustmentCodeName": "VAC",
        "DayList": [
            {"Date": "2026-03-16", "NetHours": 8.0},
            {"Date": "2026-03-17", "NetHours": 8.0},
            {"Date": "2026-03-18", "NetHours": 8.0},
        ],
    }
    first = [r.record_hash for r in normalize_entry("H5JN767", entry)]
    second = [r.record_hash for r in normalize_entry("H5JN767", entry)]
    assert first == second


def test_different_requests_for_same_employee_dont_collide():
    """Two distinct TAFW requests (different reason, non-overlapping dates)
    for the same employee must never share a hash for any of their days."""
    vacation = normalize_entry(
        "H5JN767",
        {
            "TimeStart": "2026-03-16T00:00:00",
            "TimeEnd": "2026-03-17T00:00:00",
            "NetHours": 16.0,
            "ReasonName": "Vacation",
            "PayAdjustmentCodeName": "VAC",
            "DayList": [
                {"Date": "2026-03-16", "NetHours": 8.0},
                {"Date": "2026-03-17", "NetHours": 8.0},
            ],
        },
    )
    sick = normalize_entry(
        "H5JN767",
        {
            "TimeStart": "2026-04-01T00:00:00",
            "TimeEnd": "2026-04-01T00:00:00",
            "NetHours": 8.0,
            "ReasonName": "Sick",
            "PayAdjustmentCodeName": "SICK",
            "DayList": [{"Date": "2026-04-01", "NetHours": 8.0}],
        },
    )
    vacation_hashes = {r.record_hash for r in vacation}
    sick_hashes = {r.record_hash for r in sick}
    assert vacation_hashes.isdisjoint(sick_hashes)
