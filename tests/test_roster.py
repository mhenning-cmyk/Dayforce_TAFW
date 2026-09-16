from __future__ import annotations

import datetime as _dt

from tafw_ingest.department_map import DepartmentMap
from tafw_ingest.roster import (
    build_in_scope_roster,
    department_xref_of,
    iter_in_scope_employees,
)

MAP = DepartmentMap.load()


class FakeClient:
    """Minimal stand-in for DayforceClient: roster + per-employee detail."""

    def __init__(self, employees: dict[str, dict]):
        self._employees = employees

    def list_employee_xrefs(self):
        return list(self._employees)

    def get_employee(self, xref, *, expand=None, **_):
        return self._employees[xref]


# --- department_xref_of --------------------------------------------------------
def test_reads_department_from_work_assignment_nested():
    emp = {
        "WorkAssignments": {
            "Items": [
                {
                    "IsPrimary": True,
                    "EffectiveStart": "2020-01-01",
                    "Department": {"XRefCode": "BTSI_SERVICES"},
                }
            ]
        }
    }
    assert department_xref_of(emp) == "BTSI_SERVICES"


def test_reads_department_from_scalar_fallback():
    assert (
        department_xref_of({"DepartmentXRefCode": "btsi_learninglogics"})
        == "BTSI_LEARNINGLOGICS"
    )


def test_picks_assignment_effective_on_the_as_of_date():
    emp = {
        "WorkAssignments": [
            {
                "EffectiveStart": "2019-01-01",
                "EffectiveEnd": "2024-12-31",
                "DepartmentXRefCode": "BTSI_CORPORATE",
            },
            {
                "EffectiveStart": "2025-01-01",
                "DepartmentXRefCode": "BTSI_SERVICES",
            },
        ]
    }
    assert department_xref_of(emp, as_of=_dt.date(2023, 6, 1)) == "BTSI_CORPORATE"
    assert department_xref_of(emp, as_of=_dt.date(2026, 6, 1)) == "BTSI_SERVICES"


def test_returns_none_when_no_department_anywhere():
    assert department_xref_of({"XRefCode": "E1", "FirstName": "Ada"}) is None


# --- iter_in_scope_employees ------------------------------------------------
def test_filters_roster_to_mapped_departments_and_tags_project_task():
    client = FakeClient(
        {
            "E1": {"DepartmentXRefCode": "BTSI_LEARNINGLOGICS"},
            "E2": {"DepartmentXRefCode": "BTSI_SERVICES"},
            "E3": {"DepartmentXRefCode": "BTSI_CORPORATE"},  # out of scope
            "E4": {"FirstName": "no department here"},  # unresolved
            "E5": {"DepartmentXRefCode": "BTSI_SIMULATION_PRODUCT"},
        }
    )
    roster = build_in_scope_roster(client, MAP)

    assert set(roster) == {"E1", "E2", "E5"}
    assert (roster["E1"].project_id, roster["E1"].task_id) == (96731, 3431179)
    assert (roster["E2"].project_id, roster["E2"].task_id) == (96732, 3431175)
    assert (roster["E5"].project_id, roster["E5"].task_id) == (96731, 3431179)
    assert roster["E1"].department_xref == "BTSI_LEARNINGLOGICS"


def test_xrefs_argument_limits_which_employees_are_checked():
    client = FakeClient(
        {
            "E1": {"DepartmentXRefCode": "BTSI_SERVICES"},
            "E2": {"DepartmentXRefCode": "BTSI_SERVICES"},
        }
    )
    out = list(iter_in_scope_employees(client, MAP, xrefs=["E2"]))
    assert [e.xref for e in out] == ["E2"]
