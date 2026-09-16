from __future__ import annotations

import datetime as _dt
from decimal import Decimal

from tafw_ingest.normalize import dedupe_by_hash, normalize_entry, normalize_events


def test_single_day_entry(approved_entries):
    records = normalize_entry("EMP-001", approved_entries[0])
    assert len(records) == 1
    r = records[0]
    assert r.employee_xref == "EMP-001"
    assert r.pto_date == _dt.date(2026, 3, 16)
    assert r.type_code == "VAC"  # PayAdjustmentCodeName wins over ReasonName
    assert r.hours == Decimal("8.00")
    assert r.status == "APPROVED"
    assert r.reason_name == "Vacation"


def test_daylist_expands_to_one_record_per_day(approved_entries):
    records = normalize_entry("EMP-001", approved_entries[1])
    assert [r.pto_date for r in records] == [
        _dt.date(2026, 4, 6),
        _dt.date(2026, 4, 7),
        _dt.date(2026, 4, 8),
    ]
    assert all(r.hours == Decimal("8.00") for r in records)
    assert all(r.type_code == "PERS" for r in records)
    # every day-record has a distinct identity
    assert len({r.record_hash for r in records}) == 3


def test_expand_multi_day_even_split_without_daylist():
    entry = {
        "TimeStart": "2026-05-04T00:00:00",
        "TimeEnd": "2026-05-06T00:00:00",
        "NetHours": 12.0,
        "ReasonName": "Sick",
        "Status": "APPROVED",
    }
    one = normalize_entry("E1", entry, expand_multi_day=False)
    assert len(one) == 1 and one[0].hours == Decimal("12.00")

    many = normalize_entry("E1", entry, expand_multi_day=True)
    assert [r.hours for r in many] == [Decimal("4.00"), Decimal("4.00"), Decimal("4.00")]
    assert sum(r.hours for r in many) == Decimal("12.00")


def test_missing_type_code_is_skipped_not_fatal():
    bad = {"TimeStart": "2026-01-01T00:00:00", "NetHours": 8.0, "Status": "APPROVED"}
    good = {
        "TimeStart": "2026-01-02T00:00:00",
        "NetHours": 8.0,
        "ReasonName": "Vacation",
        "Status": "APPROVED",
    }
    out = normalize_events("E1", [bad, good])
    assert len(out) == 1
    assert out[0].pto_date == _dt.date(2026, 1, 2)


def test_dedupe_by_hash_keeps_first():
    entry = {
        "TimeStart": "2026-02-02T00:00:00",
        "NetHours": 8.0,
        "ReasonName": "Vacation",
        "Status": "APPROVED",
    }
    a = normalize_entry("E1", entry)
    b = normalize_entry("E1", dict(entry))
    deduped = dedupe_by_hash([*a, *b])
    assert len(deduped) == 1
