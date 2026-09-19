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

from tafw_ingest.department_map import DepartmentMap

if TYPE_CHECKING:  # pragma: no cover
    from tafw_ingest.dayforce_client import DayforceClient

__all__ = [
    "employees_to_dataframe",
    "fetch_employees_dataframe",
    "get_employee_department",
    "get_employee_display_name",
    "fetch_employee_department",
    "get_department_project_id",
    "get_department_task_id",
]

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

#: Only employees whose XRefCode starts with this prefix are in scope for
#: this service - everyone else is dropped in employees_to_dataframe.
_IN_SCOPE_XREF_PREFIX = "H5JN"


def employees_to_dataframe(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Flatten raw Dayforce employee objects into a DataFrame.

    Only employees whose ``XRefCode`` starts with ``"H5JN"`` are kept - those
    are the only employees this service is concerned with; everyone else is
    dropped before flattening. Nested objects/arrays are flattened with
    dotted column names (``pandas.json_normalize``). ``XRefCode`` and other
    common identity fields are ordered first; the frame is de-duplicated on
    ``XRefCode`` if that column exists. Empty input (or an input with no
    in-scope employees) yields an empty frame with an ``XRefCode`` column so
    downstream code can rely on it.
    """
    records = [
        row
        for row in rows
        if str(row.get("XRefCode", "")).strip().upper().startswith(_IN_SCOPE_XREF_PREFIX)
    ]
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


def get_employee_department(employee: dict[str, Any]) -> str | None:
    """Pull the ``Department.XRefCode`` off an ``expand=WorkAssignments`` record.

    ``WorkAssignments.Items`` can hold more than one assignment; the item
    flagged ``IsPrimary`` is preferred, falling back to the first item.
    Returns ``None`` if there is no work assignment or department attached.
    The XRefCode (e.g. ``"BTSI_SIMULATION_PRODUCT"``) is what
    :class:`~tafw_ingest.department_map.DepartmentMap` and
    :func:`get_department_project_id` key on - not the human-readable
    ``ShortName``.
    """
    work_assignments = employee.get("WorkAssignments")
    items = work_assignments.get("Items") if isinstance(work_assignments, dict) else None
    if not items:
        return None

    primary = next(
        (item for item in items if isinstance(item, dict) and item.get("IsPrimary")),
        items[0],
    )
    if not isinstance(primary, dict):
        return None

    position = primary.get("Position") or {}
    department = position.get("Department") or {} if isinstance(position, dict) else {}
    return department.get("XRefCode") if isinstance(department, dict) else None


def get_employee_display_name(employee: dict[str, Any]) -> str | None:
    """Pull ``DisplayName`` (e.g. ``"Woodland, Jason"``) off an employee record.

    Dayforce's field-restricted role for this integration returns
    ``DisplayName`` on the per-employee ``expand=WorkAssignments`` payload
    (see :func:`fetch_employee_department`) even though the bulk
    ``GET /Employees`` roster doesn't reliably populate ``FirstName``/
    ``LastName`` - so this is the source of truth for an employee's name,
    not the roster DataFrame from :func:`employees_to_dataframe`.
    """
    return employee.get("DisplayName")


def fetch_employee_department(client: DayforceClient, xref_code: str) -> pd.DataFrame:
    """Look up one employee's work assignment and return their name/department.

    Calls ``GET /Employees/{xref}?expand=WorkAssignments`` and returns a
    one-row DataFrame with ``XRefCode``, ``DisplayName``, and
    ``DepartmentXRefCode`` (the assignment's ``Department.XRefCode``, or
    ``None`` if the employee has no work assignment on file).
    """
    employee = client.get_employee_work_assignments(xref_code)
    return pd.DataFrame(
        [
            {
                "XRefCode": employee.get("XRefCode", xref_code),
                "DisplayName": get_employee_display_name(employee),
                "DepartmentXRefCode": get_employee_department(employee),
            }
        ]
    )


def get_department_project_id(
    department_xref: str, dept_map: DepartmentMap | None = None
) -> int | None:
    """NetSuite ``project_id`` for a Department XRefCode, or ``None`` if out of scope.

    Resolved via :class:`~tafw_ingest.department_map.DepartmentMap` (backed by
    ``conf/department_project_task.yaml`` - the same source of truth
    :mod:`tafw_ingest.roster` uses), not a hardcoded rule, so it stays in sync
    with the Dayforce report if the mapping changes. Pass an already-loaded
    ``dept_map`` to avoid re-reading the YAML file on every call.
    """
    dept_map = dept_map or DepartmentMap.load()
    resolved = dept_map.resolve(department_xref)
    return resolved.project_id if resolved else None


def get_department_task_id(
    department_xref: str, dept_map: DepartmentMap | None = None
) -> int | None:
    """NetSuite ``task_id`` for a Department XRefCode, or ``None`` if out of scope.

    Same ``DepartmentMap`` lookup as :func:`get_department_project_id`, just
    the ``task_id`` half of the pair - i.e. the CASE-over-``Department.XRefCode``
    rule mirrored in ``conf/department_project_task.yaml``.
    """
    dept_map = dept_map or DepartmentMap.load()
    resolved = dept_map.resolve(department_xref)
    return resolved.task_id if resolved else None
