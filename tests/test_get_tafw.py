from __future__ import annotations

from tafw_ingest.get_tafw import expand_tafw_records_to_days
from tafw_ingest.hashing import record_hash


def _record(*, start: str, end: str, net_hours: float | None, status: str = "APPROVED", **extra):
    record = {
        "TimeStart": start,
        "TimeEnd": end,
        "ReasonName": "Vacation",
        "PayAdjShortName": "VAC",
        "_XRefCode": "H5JN095",
        "_Status": status,
    }
    if net_hours is not None:
        record["NetHours"] = net_hours
    record.update(extra)
    return record


def test_full_week_request_splits_evenly_across_weekdays():
    """3 weekdays, NetHours=24 - the historical hardcoded-8 assumption,
    kept as a regression check now that hours are computed instead."""
    records = [
        _record(start="2026-03-16T00:00:00", end="2026-03-19T00:00:00", net_hours=24.0)
    ]
    df = expand_tafw_records_to_days(records)

    assert len(df) == 3
    assert list(df["Date"].astype(str)) == ["2026-03-16", "2026-03-17", "2026-03-18"]
    assert (df["Hours"] == 8.0).all()


def test_partial_day_request_uses_actual_net_hours_not_hardcoded_eight():
    """The bug: a single weekday, 5-hour partial-day request (e.g. an
    employee's TAFW hours were edited down from 8 to 5) must produce a row
    with Hours=5.0, not a hardcoded 8.0."""
    records = [
        _record(start="2026-09-14T09:00:00", end="2026-09-14T14:00:00", net_hours=5.0)
    ]
    df = expand_tafw_records_to_days(records)

    assert len(df) == 1
    assert df.iloc[0]["Date"].isoformat() == "2026-09-14"
    assert df.iloc[0]["Hours"] == 5.0


def test_hours_change_produces_a_different_record_hash():
    """A record's identity hash is built from (employee, date, type, hours) -
    an hours edit must change the hash so a downstream reconcile/merge can
    detect it as a changed record rather than silently keeping the old value."""
    hours_8 = _record(start="2026-09-14T09:00:00", end="2026-09-14T17:00:00", net_hours=8.0)
    hours_5 = _record(start="2026-09-14T09:00:00", end="2026-09-14T14:00:00", net_hours=5.0)

    df_8 = expand_tafw_records_to_days([hours_8])
    df_5 = expand_tafw_records_to_days([hours_5])

    assert df_8.iloc[0]["RecordHash"] != df_5.iloc[0]["RecordHash"]
    assert df_5.iloc[0]["RecordHash"] == record_hash("H5JN095", "2026-09-14", "VAC", 5.0)


def test_weekend_only_span_contributes_no_rows():
    # 2026-09-19 is a Saturday, 2026-09-21 is a Monday (exclusive end).
    records = [
        _record(start="2026-09-19T00:00:00", end="2026-09-21T00:00:00", net_hours=16.0)
    ]
    df = expand_tafw_records_to_days(records)
    assert df.empty


def test_missing_net_hours_falls_back_to_eight_per_day():
    records = [
        _record(start="2026-09-14T00:00:00", end="2026-09-15T00:00:00", net_hours=None)
    ]
    df = expand_tafw_records_to_days(records)
    assert df.iloc[0]["Hours"] == 8.0
