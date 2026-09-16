"""Fetch the full Dayforce employee roster into a pandas DataFrame.

``GET /Employees`` is paginated (the client follows ``Paging.Next``); this
module flattens the accumulated rows into a DataFrame for exploration,
roster diffing, or feeding the TAFW sync's employee loop.

Typical use::

    from tafw_ingest.config import Settings
    from tafw_ingest.dayforce_client import DayforceClient
    from tafw_ingest.employees import fetch_employees_dataframe

    client = DayforceClient.from_settings(Settings.from_env("conf/settings.dev.yaml"))
    df = fetch_employees_dataframe(client)          # all employees, all pages
    df = fetch_employees_dataframe(client, filterHireStartDate="2020-01-01T00:00:00Z")
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:  # pragma: no cover
    from tafw_ingest.dayforce_client import DayforceClient

__all__ = ["employees_to_dataframe", "fetch_employees_dataframe"]

#: Columns pulled to the front when present; everything else follows, sorted.
_PREFERRED_ORDER = [
    "XRefCode",
    "EmployeeNumber",
    "FirstName",
    "LastName",
    "DisplayName",
    "HireDate",
    "TerminationDate",
    "Status",
]


def employees_to_dataframe(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Flatten raw Dayforce employee objects into a DataFrame.

    Nested objects/arrays are flattened with dotted column names
    (``pandas.json_normalize``). ``XRefCode`` and other common identity fields
    are ordered first; the frame is de-duplicated on ``XRefCode`` if that
    column exists. An empty input yields an empty frame with an ``XRefCode``
    column so downstream code can rely on it.
    """
    records = list(rows)
    if not records:
        return pd.DataFrame(columns=["XRefCode"])

    df = pd.json_normalize(records, sep=".")

    if "XRefCode" in df.columns:
        df = df.drop_duplicates(subset="XRefCode").reset_index(drop=True)

    front = [c for c in _PREFERRED_ORDER if c in df.columns]
    rest = sorted(c for c in df.columns if c not in front)
    return df[front + rest]


def fetch_employees_dataframe(
    client: DayforceClient, *, page_size: int | None = None, **filters: Any
) -> pd.DataFrame:
    """Page through ``GET /Employees`` and return the roster as a DataFrame.

    ``filters`` are passed straight through as Dayforce query parameters
    (e.g. ``contextDate``, ``filterHireStartDate``, ``filterTerminationStartDate``).
    """
    rows = client.iter_employees(page_size=page_size, **filters)
    return employees_to_dataframe(rows)
