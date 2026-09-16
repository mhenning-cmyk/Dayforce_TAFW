"""Narrow the Dayforce roster to employees in the in-scope departments.

The TAFW pull is per-employee, so filtering *before* the pull is the biggest
cost lever. This module resolves each employee's Department XRefCode from their
``GET /Employees/{xref}`` detail and keeps only those whose department is in the
:class:`~tafw_ingest.department_map.DepartmentMap` (i.e. rolls up to one of the
target projects).

Where "Department" lives in the employee payload varies by tenant config and
which ``expand`` you request, so :data:`_DEPARTMENT_ACCESSORS` lists the
candidate locations in priority order. Run one real ``GET /Employees/{xref}?expand=...``
and confirm which one hits (a warning is logged for every employee where none do).
"""

from __future__ import annotations

import datetime as _dt
import logging
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from tafw_ingest.department_map import DepartmentMap

if TYPE_CHECKING:  # pragma: no cover
    from tafw_ingest.dayforce_client import DayforceClient

logger = logging.getLogger(__name__)

__all__ = [
    "EMPLOYEE_EXPAND",
    "InScopeEmployee",
    "department_xref_of",
    "iter_in_scope_employees",
    "build_in_scope_roster",
]

#: Sub-collections to expand on GET /Employees/{xref} so the org assignment is
#: present. Only valid Dayforce expand tokens - an unknown one makes the whole
#: call 400. (HomeOrganization is a native field, not an expand.)
EMPLOYEE_EXPAND = "WorkAssignments,EmploymentStatuses"

#: Candidate paths to the Department XRefCode, most specific first. Each entry is
#: ("collection_key" | None, (path, into, the, item | scalar)). A ``None``
#: collection means the path is read straight off the employee object.
_DEPARTMENT_ACCESSORS: tuple[tuple[str | None, tuple[str, ...]], ...] = (
    ("WorkAssignments", ("Department", "XRefCode")),
    ("WorkAssignments", ("DepartmentXRefCode",)),
    ("WorkAssignments", ("Position", "Department", "XRefCode")),
    ("EmploymentStatuses", ("Department", "XRefCode")),
    ("EmploymentStatuses", ("OrgUnitXRefCode",)),
    (None, ("HomeOrganization", "XRefCode")),
    (None, ("DepartmentXRefCode",)),
)


@dataclass(frozen=True)
class InScopeEmployee:
    xref: str
    department_xref: str
    project_id: int
    task_id: int


# --------------------------------------------------------------------------- #
# Payload parsing                                                             #
# --------------------------------------------------------------------------- #
def _as_items(value: Any) -> list[dict[str, Any]]:
    """Dayforce expand collections come back as ``[...]`` or ``{"Items": [...]}``."""
    if isinstance(value, dict):
        value = value.get("Items", value.get("items"))
    return [v for v in value or [] if isinstance(v, dict)]


def _dig(obj: Any, path: tuple[str, ...]) -> Any:
    for key in path:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def _parse_date(value: Any) -> _dt.date | None:
    if not value:
        return None
    try:
        return _dt.date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _effective_item(
    items: list[dict[str, Any]], as_of: _dt.date
) -> dict[str, Any] | None:
    """Assignment covering ``as_of`` (prefer primary), else the latest-started."""
    covering = []
    for item in items:
        start = _parse_date(item.get("EffectiveStart") or item.get("EffectiveStartDate"))
        end = _parse_date(item.get("EffectiveEnd") or item.get("EffectiveEndDate"))
        if (start is None or start <= as_of) and (end is None or end >= as_of):
            covering.append((start or _dt.date.min, item))
    pool = covering or [
        (_parse_date(i.get("EffectiveStart")) or _dt.date.min, i) for i in items
    ]
    if not pool:
        return None
    primary = [it for _, it in pool if it.get("IsPrimary") or it.get("IsPrimaryWork")]
    if primary:
        return primary[0]
    return max(pool, key=lambda t: t[0])[1]


def department_xref_of(
    employee: dict[str, Any], *, as_of: _dt.date | None = None
) -> str | None:
    """Best-effort Department XRefCode for an employee, effective ``as_of``."""
    as_of = as_of or _dt.date.today()
    for collection, path in _DEPARTMENT_ACCESSORS:
        if collection is None:
            value = _dig(employee, path)
        else:
            item = _effective_item(_as_items(employee.get(collection)), as_of)
            value = _dig(item, path) if item else None
        if value:
            return str(value).strip().upper()
    return None


# --------------------------------------------------------------------------- #
# Roster narrowing                                                            #
# --------------------------------------------------------------------------- #
def iter_in_scope_employees(
    client: DayforceClient,
    dept_map: DepartmentMap,
    *,
    xrefs: Iterable[str] | None = None,
    expand: str = EMPLOYEE_EXPAND,
    as_of: _dt.date | None = None,
) -> Iterator[InScopeEmployee]:
    """Yield an :class:`InScopeEmployee` for each employee whose department is mapped.

    ``xrefs`` limits which employees to check (defaults to the full roster).
    One ``GET /Employees/{xref}`` per employee - this is the fan-out the filter
    exists to shrink, so cache/persist the result rather than re-deriving it
    every cycle.
    """
    codes = list(xrefs) if xrefs is not None else client.list_employee_xrefs()
    logger.info("Resolving department for %d employee(s)", len(codes))

    kept = unmapped = unresolved = 0
    for xref in codes:
        employee = client.get_employee(xref, expand=expand)
        dept = department_xref_of(employee, as_of=as_of)
        if dept is None:
            unresolved += 1
            logger.warning("No Department XRefCode found for employee %s", xref)
            continue
        pt = dept_map.resolve(dept)
        if pt is None:
            unmapped += 1
            continue
        kept += 1
        yield InScopeEmployee(str(xref), dept, pt.project_id, pt.task_id)

    logger.info(
        "In-scope roster: kept=%d, out-of-scope dept=%d, dept unresolved=%d",
        kept,
        unmapped,
        unresolved,
    )


def build_in_scope_roster(
    client: DayforceClient,
    dept_map: DepartmentMap,
    *,
    xrefs: Iterable[str] | None = None,
    expand: str = EMPLOYEE_EXPAND,
    as_of: _dt.date | None = None,
) -> dict[str, InScopeEmployee]:
    """``{employee_xref: InScopeEmployee}`` for every in-scope employee."""
    return {
        e.xref: e
        for e in iter_in_scope_employees(
            client, dept_map, xrefs=xrefs, expand=expand, as_of=as_of
        )
    }
