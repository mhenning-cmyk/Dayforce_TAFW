"""Fetch + expand TAFW requests for a selection of employees into a DataFrame.

Consolidates the ad hoc logic that used to live duplicated in
``tests/test_tafw_approved.py`` and ``tests/test_tafw_deleted.py`` into
reusable functions that take a *list* of employee XRefCodes, an arbitrary
start/end date range, and one or more statuses (APPROVED and CANCELED by
default).

    client = DayforceClient.from_settings(settings)
    day_df = get_tafw_days(client, ["H5JN767", "EMP-002"], start, end)

``get_tafw_records_for_employee`` issues one request per (employee, status)
pair for the whole date range and follows ``Paging.Next`` for large result
sets - no date-window chunking here. An earlier version of this module
assumed the TAFW endpoint capped queries at 31 days (inherited, uncited,
from the original ``bluedrop-mavenlink-sync`` client), but live testing
against a full year showed that limit doesn't hold.

Each TAFW request is expanded into one row per weekday off, 8 hours each -
``TimeEnd`` is treated as the start of the first day back (exclusive),
matching the Dayforce convention seen in practice: a Mon-Wed request off
returns TimeStart=Mon 00:00, TimeEnd=Thu 00:00, NetHours=24 (3 weekdays x
8h). Weekend days in the span are skipped since they carry no PTO hours.

Each expanded day row also carries a ``RecordHash`` - the same deterministic
per-day identity hash used by the production pipeline
(:mod:`tafw_ingest.hashing`), built from employee/date/type/hours only, never
status. That is deliberate: this module fetches APPROVED and CANCELED
separately and tags the ``Status`` column itself, so a request that flips
status keeps the same day-identity and a comparison across cycles can
distinguish "still there" from "added" / "removed" by hash presence alone.
"""

from __future__ import annotations

import datetime as _dt
import logging
from collections.abc import Sequence
from typing import Any

import pandas as pd

from tafw_ingest.dayforce_client import DayforceClient
from tafw_ingest.hashing import record_hash

logger = logging.getLogger(__name__)

__all__ = [
    "DEFAULT_STATUSES",
    "fetch_tafw_records",
    "expand_tafw_records_to_days",
    "get_tafw_days",
]

DEFAULT_STATUSES = (DayforceClient.STATUS_APPROVED, DayforceClient.STATUS_CANCELED)


def fetch_tafw_records(
    client: DayforceClient,
    xref_codes: Sequence[str],
    start_date: _dt.datetime,
    end_date: _dt.datetime,
    *,
    statuses: Sequence[str] = DEFAULT_STATUSES,
) -> list[dict[str, Any]]:
    """Fetch raw TAFW entries for every (employee, status) combination.

    Each returned dict is tagged with ``_XRefCode`` / ``_Status`` so
    :func:`expand_tafw_records_to_days` doesn't need those threaded through
    separately.
    """
    records: list[dict[str, Any]] = []
    for xref in xref_codes:
        for status in statuses:
            entries = client.get_tafw_records_for_employee(
                xref, status, start_date, end_date
            )
            for entry in entries:
                tagged = dict(entry)
                tagged["_XRefCode"] = xref
                tagged["_Status"] = status
                records.append(tagged)
    logger.info(
        "Fetched %d TAFW record(s) for %d employee(s), statuses=%s",
        len(records),
        len(xref_codes),
        list(statuses),
    )
    return records


def expand_tafw_records_to_days(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Expand tagged TAFW requests (see :func:`fetch_tafw_records`) into one
    row per weekday off, 8 hours each, deduped by ``RecordHash``.
    """
    rows: list[dict[str, Any]] = []
    for record in records:
        xref_code = record["_XRefCode"]
        status = record["_Status"]
        start = _dt.datetime.fromisoformat(record["TimeStart"])
        end = _dt.datetime.fromisoformat(record["TimeEnd"])
        type_code = record.get("PayAdjShortName") or record.get("ReasonName")
        day = start
        while day < end:
            if day.weekday() < 5:  # Mon-Fri
                rows.append(
                    {
                        "XRefCode": xref_code,
                        "Date": day.date(),
                        "Hours": 8.0,
                        "ReasonName": record.get("ReasonName"),
                        "PayAdjShortName": record.get("PayAdjShortName"),
                        "Status": str(status).title(),
                        "RecordHash": record_hash(xref_code, day.date(), type_code, 8.0),
                    }
                )
            day += _dt.timedelta(days=1)
    day_df = pd.DataFrame(rows)
    if not day_df.empty:
        day_df = day_df.drop_duplicates(subset="RecordHash").reset_index(drop=True)
    return day_df


def get_tafw_days(
    client: DayforceClient,
    xref_codes: Sequence[str],
    start_date: _dt.datetime,
    end_date: _dt.datetime,
    *,
    statuses: Sequence[str] = DEFAULT_STATUSES,
) -> pd.DataFrame:
    """Fetch + expand in one call: employees x statuses x date-range -> DataFrame."""
    records = fetch_tafw_records(
        client, xref_codes, start_date, end_date, statuses=statuses
    )
    return expand_tafw_records_to_days(records)
