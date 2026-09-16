from __future__ import annotations

import datetime as _dt

from tafw_ingest.models import DayRecord
from tafw_ingest.reconcile import StoredRecord, reconcile


def rec(xref: str, day: str, type_code: str, hours) -> DayRecord:
    return DayRecord.from_fields(
        employee_xref=xref,
        pto_date=_dt.date.fromisoformat(day),
        type_code=type_code,
        hours=hours,
    )


def stored(r: DayRecord) -> StoredRecord:
    return StoredRecord(r.record_hash, r.employee_xref, r.pto_date)


def test_all_new_when_no_history():
    cur = [rec("E1", "2026-03-01", "VAC", 8), rec("E1", "2026-03-02", "VAC", 8)]
    result = reconcile(cur, {})
    assert result.summary() == {
        "new": 2,
        "changed": 0,
        "unchanged": 0,
        "cancelled": 0,
        "superseded": 0,
    }
    assert result.upserts == cur


def test_unchanged_when_identical():
    r = rec("E1", "2026-03-01", "VAC", 8)
    result = reconcile([r], {r.record_hash: stored(r)})
    assert result.summary()["unchanged"] == 1
    assert not result.upserts


def test_changed_hours_is_change_not_new_plus_cancel():
    old = rec("E1", "2026-03-01", "VAC", 8)
    new = rec("E1", "2026-03-01", "VAC", 4)  # same employee+date, different hours
    result = reconcile([new], {old.record_hash: stored(old)})
    assert result.summary() == {
        "new": 0,
        "changed": 1,
        "unchanged": 0,
        "cancelled": 0,
        "superseded": 1,
    }
    assert result.changed == [new]
    assert result.cancelled == []


def test_disappeared_record_is_cancelled():
    kept = rec("E1", "2026-03-01", "VAC", 8)
    gone = rec("E1", "2026-03-09", "VAC", 8)
    prev = {kept.record_hash: stored(kept), gone.record_hash: stored(gone)}
    result = reconcile([kept], prev)
    assert result.summary() == {
        "new": 0,
        "changed": 0,
        "unchanged": 1,
        "cancelled": 1,
        "superseded": 0,
    }
    assert result.cancelled == [gone.record_hash]


def test_mixed_cycle():
    unchanged = rec("E1", "2026-03-01", "VAC", 8)
    changed_old = rec("E1", "2026-03-02", "VAC", 8)
    changed_new = rec("E1", "2026-03-02", "PERS", 8)
    cancelled = rec("E2", "2026-03-03", "VAC", 8)
    brand_new = rec("E3", "2026-03-04", "SICK", 8)

    prev = {r.record_hash: stored(r) for r in (unchanged, changed_old, cancelled)}
    result = reconcile([unchanged, changed_new, brand_new], prev)
    assert result.summary() == {
        "new": 1,
        "changed": 1,
        "unchanged": 1,
        "cancelled": 1,
        "superseded": 1,
    }
    assert set(result.cancelled) == {cancelled.record_hash}
