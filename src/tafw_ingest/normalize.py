"""Raw Dayforce ``TimeAwayFromWork`` entries -> :class:`DayRecord` list.

A Dayforce TAFW entry looks roughly like::

    {
      "TimeStart": "2026-03-16T00:00:00",
      "TimeEnd":   "2026-03-18T00:00:00",
      "NetHours":  24.0,
      "ReasonName": "Vacation",
      "PayAdjustmentCodeName": "VAC",
      "DateOfRequest": "2026-02-01T09:12:00",
      "AllDay": true,
      "Status": "APPROVED",
      "DayList": [                       # <- present on some tenants only
        {"Date": "2026-03-16", "NetHours": 8.0},
        {"Date": "2026-03-17", "NetHours": 8.0},
        {"Date": "2026-03-18", "NetHours": 8.0}
      ]
    }

Rules:

* ``type_code`` comes from ``PayAdjustmentCodeName`` if present, else
  ``ReasonName`` (upper-cased by the hashing spec).
* If the entry carries an explicit per-day breakdown (``DayList`` /
  ``Days`` / ``TAFWDays``), one :class:`DayRecord` is emitted per day using
  that day's hours.
* Otherwise, with ``expand_multi_day=False`` (default) a single record is
  emitted on the ``TimeStart`` date carrying the full ``NetHours``. With
  ``expand_multi_day=True`` the entry is spread evenly across the calendar
  days from ``TimeStart`` to ``TimeEnd`` inclusive.

The exact payload shape is a TODO to confirm against a real tenant response
(see tests/fixtures/dayforce/). ``_DAY_LIST_KEYS`` / ``_DAY_DATE_KEYS`` /
``_DAY_HOURS_KEYS`` make that cheap to adjust.
"""

from __future__ import annotations

import datetime as _dt
import logging
from collections.abc import Iterable, Sequence
from decimal import Decimal
from typing import Any

from tafw_ingest.models import DayRecord

logger = logging.getLogger(__name__)

__all__ = ["normalize_entry", "normalize_events", "dedupe_by_hash"]

_TYPE_CODE_KEYS = ("PayAdjustmentCodeName", "PayAdjustmentCodeXRefCode", "ReasonName")
_DAY_LIST_KEYS = ("DayList", "Days", "TAFWDays", "TimeAwayFromWorkDays")
_DAY_DATE_KEYS = ("Date", "DateTime", "WorkDate")
_DAY_HOURS_KEYS = ("NetHours", "Hours", "HoursRequested")


def _first(entry: dict[str, Any], keys: Sequence[str]) -> Any:
    for key in keys:
        value = entry.get(key)
        if value not in (None, ""):
            return value
    return None


def _date_only(value: Any) -> _dt.date:
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    return _dt.date.fromisoformat(str(value)[:10])


def _day_breakdown(entry: dict[str, Any]) -> list[tuple[_dt.date, Any]] | None:
    raw = _first(entry, _DAY_LIST_KEYS)
    if not isinstance(raw, list) or not raw:
        return None
    out: list[tuple[_dt.date, Any]] = []
    for day in raw:
        if not isinstance(day, dict):
            return None
        d = _first(day, _DAY_DATE_KEYS)
        h = _first(day, _DAY_HOURS_KEYS)
        if d is None or h is None:
            return None
        out.append((_date_only(d), h))
    return out


def _spread_even(entry: dict[str, Any]) -> list[tuple[_dt.date, Decimal]]:
    start = _date_only(entry["TimeStart"])
    end = _date_only(entry.get("TimeEnd") or entry["TimeStart"])
    if end < start:
        start, end = end, start
    days = [start + _dt.timedelta(days=i) for i in range((end - start).days + 1)]
    total = Decimal(str(entry.get("NetHours") or 0))
    if len(days) == 1:
        return [(days[0], total)]
    per = (total / len(days)).quantize(Decimal("0.01"))
    out = [(d, per) for d in days[:-1]]
    out.append((days[-1], (total - per * (len(days) - 1)).quantize(Decimal("0.01"))))
    return out


def normalize_entry(
    employee_xref: str,
    entry: dict[str, Any],
    *,
    status: str = "APPROVED",
    expand_multi_day: bool = False,
) -> list[DayRecord]:
    """Turn one raw TAFW entry into one or more :class:`DayRecord`."""
    type_code = _first(entry, _TYPE_CODE_KEYS)
    if type_code is None:
        logger.warning(
            "TAFW entry for %s has no type/reason code: %r", employee_xref, entry
        )
        return []

    breakdown = _day_breakdown(entry)
    if breakdown is None:
        if expand_multi_day and entry.get("TimeEnd"):
            breakdown = _spread_even(entry)
        else:
            breakdown = [(_date_only(entry["TimeStart"]), entry.get("NetHours") or 0)]

    records: list[DayRecord] = []
    for pto_date, hours in breakdown:
        records.append(
            DayRecord.from_fields(
                employee_xref=employee_xref,
                pto_date=pto_date,
                type_code=str(type_code),
                hours=hours,
                raw=entry,
                status=str(entry.get("Status") or status),
            )
        )
    return records


def normalize_events(
    employee_xref: str,
    entries: Iterable[dict[str, Any]],
    *,
    status: str = "APPROVED",
    expand_multi_day: bool = False,
) -> list[DayRecord]:
    """Normalize every entry for one employee, skipping malformed ones."""
    out: list[DayRecord] = []
    for entry in entries:
        try:
            out.extend(
                normalize_entry(
                    employee_xref,
                    entry,
                    status=status,
                    expand_multi_day=expand_multi_day,
                )
            )
        except Exception:  # noqa: BLE001 - one bad entry must not sink the cycle
            logger.exception(
                "Could not normalize TAFW entry for %s: %r", employee_xref, entry
            )
    return out


def dedupe_by_hash(records: Iterable[DayRecord]) -> list[DayRecord]:
    """Collapse records that share a ``record_hash`` (same identity seen twice).

    Dayforce can surface the same underlying day under overlapping query
    windows. First occurrence wins; order is otherwise preserved.
    """
    seen: set[str] = set()
    out: list[DayRecord] = []
    for record in records:
        if record.record_hash in seen:
            continue
        seen.add(record.record_hash)
        out.append(record)
    return out
