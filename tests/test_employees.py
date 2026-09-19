from __future__ import annotations

import pandas as pd
import pytest

from tafw_ingest.dayforce_client import DayforceClient
from tafw_ingest.department_map import DepartmentMap, ProjectTask
from tafw_ingest.employees import (
    employees_to_dataframe,
    fetch_employee_department,
    fetch_employees_dataframe,
    get_department_project_id,
    get_department_task_id,
    get_employee_department,
    get_employee_display_name,
)

BASE = "https://dayforcehcm.com/Api/bluedrop/v1/Employees"


@pytest.fixture
def client():
    return DayforceClient(
        "u",
        "p",
        "bluedrop",
        base_uri="https://dayforcehcm.com/Api",
        throttle_seconds=0,
        max_rpm=1000,
    )


def test_iter_employees_follows_paging_next(client, requests_mock):
    page2 = f"{BASE}?pageSize=2&cursor=abc"
    page3 = f"{BASE}?pageSize=2&cursor=def"
    requests_mock.get(
        BASE,
        json={"Data": [{"XRefCode": "A"}, {"XRefCode": "B"}], "Paging": {"Next": page2}},
    )
    requests_mock.get(
        page2,
        json={"Data": [{"XRefCode": "C"}, {"XRefCode": "D"}], "Paging": {"Next": page3}},
    )
    requests_mock.get(page3, json={"Data": [{"XRefCode": "E"}], "Paging": {"Next": ""}})

    rows = client.get_all_employees(page_size=2)
    assert [r["XRefCode"] for r in rows] == ["A", "B", "C", "D", "E"]
    assert requests_mock.call_count == 3
    # page size / filters only on the first request; Next carries its own query
    assert requests_mock.request_history[0].qs["pagesize"] == ["2"]


def test_single_page_when_no_paging(client, requests_mock):
    requests_mock.get(BASE, json={"Data": [{"XRefCode": "A"}, {"XRefCode": "B"}]})
    assert client.list_employee_xrefs() == ["A", "B"]
    assert requests_mock.call_count == 1


def test_filters_passed_through_as_query_params(client, requests_mock):
    requests_mock.get(BASE, json={"Data": []})
    client.get_all_employees(filterHireStartDate="2020-01-01T00:00:00Z")
    assert requests_mock.last_request.qs["filterhirestartdate"] == [
        "2020-01-01t00:00:00z"
    ]


def test_employees_to_dataframe_flattens_and_orders():
    rows = [
        {
            "XRefCode": "H5JN2",
            "FirstName": "Alan",
            "LastName": "Turing",
            "HomeOrganization": {"XRefCode": "ORG-1", "ShortName": "R&D"},
        },
        {"XRefCode": "H5JN1", "FirstName": "Ada", "LastName": "Lovelace"},
    ]
    df = employees_to_dataframe(rows)

    assert list(df.columns[:3]) == ["XRefCode", "FirstName", "LastName"]
    assert "HomeOrganization.ShortName" in df.columns
    assert len(df) == 2
    assert set(df["XRefCode"]) == {"H5JN1", "H5JN2"}


def test_dataframe_dedupes_on_xrefcode():
    rows = [
        {"XRefCode": "H5JN1", "FirstName": "a"},
        {"XRefCode": "H5JN1", "FirstName": "a"},
    ]
    assert len(employees_to_dataframe(rows)) == 1


def test_empty_roster_yields_empty_frame_with_xrefcode_column():
    df = employees_to_dataframe([])
    assert df.empty
    assert list(df.columns) == ["XRefCode"]


def test_dataframe_drops_employees_outside_the_h5jn_prefix():
    """Only H5JN* employees are in scope for this service - everyone else
    (a different xref series, or something malformed) is dropped."""
    rows = [
        {"XRefCode": "H5JN1", "FirstName": "In"},
        {"XRefCode": "EMP-2", "FirstName": "Out"},
        {"XRefCode": "h5jn3", "FirstName": "In (lowercase)"},  # case-insensitive
        {"XRefCode": ""},
        {},  # no XRefCode at all
    ]
    df = employees_to_dataframe(rows)
    assert set(df["XRefCode"]) == {"H5JN1", "h5jn3"}


def test_dataframe_empty_when_no_employees_in_scope():
    df = employees_to_dataframe([{"XRefCode": "EMP-1"}, {"XRefCode": "EMP-2"}])
    assert df.empty
    assert list(df.columns) == ["XRefCode"]


def test_fetch_employees_dataframe_end_to_end(client, requests_mock):
    requests_mock.get(
        BASE,
        json={
            "Data": [{"XRefCode": "H5JN1", "FirstName": "Ada"}],
            "Paging": {"Next": f"{BASE}?cursor=2"},
        },
    )
    requests_mock.get(
        f"{BASE}?cursor=2",
        json={
            "Data": [
                {"XRefCode": "H5JN2", "FirstName": "Alan"},
                {"XRefCode": "EMP-3", "FirstName": "OutOfScope"},
            ]
        },
    )
    df = fetch_employees_dataframe(client)
    assert isinstance(df, pd.DataFrame)
    assert list(df["XRefCode"]) == ["H5JN1", "H5JN2"]


def _work_assignment(department: dict, *, is_primary: bool | None = None) -> dict:
    item: dict = {"Position": {"Department": department}}
    if is_primary is not None:
        item["IsPrimary"] = is_primary
    return item


def test_get_employee_department_single_assignment():
    employee = {
        "XRefCode": "H5JN995",
        "WorkAssignments": {
            "Items": [
                _work_assignment(
                    {
                        "XRefCode": "BTSI_SIMULATION_PRODUCT",
                        "ShortName": "BTSI - Simulation Product",
                        "LongName": "Simulation Product",
                    }
                )
            ]
        },
    }
    assert get_employee_department(employee) == "BTSI_SIMULATION_PRODUCT"


def test_get_employee_department_prefers_primary_assignment():
    employee = {
        "WorkAssignments": {
            "Items": [
                _work_assignment({"XRefCode": "SECONDARY_DEPT"}, is_primary=False),
                _work_assignment({"XRefCode": "PRIMARY_DEPT"}, is_primary=True),
            ]
        },
    }
    assert get_employee_department(employee) == "PRIMARY_DEPT"


def test_get_employee_department_missing_work_assignments():
    assert get_employee_department({"XRefCode": "H5JN1"}) is None
    assert get_employee_department({"WorkAssignments": {"Items": []}}) is None


def test_fetch_employee_department(client, requests_mock):
    requests_mock.get(
        f"{BASE}/H5JN995",
        json={
            "Data": {
                "EmployeeNumber": "H5JN995",
                "XRefCode": "H5JN995",
                "DisplayName": "Woodland, Jason",
                "WorkAssignments": {
                    "Items": [
                        _work_assignment(
                            {
                                "XRefCode": "BTSI_SIMULATION_PRODUCT",
                                "ShortName": "BTSI - Simulation Product",
                                "LongName": "Simulation Product",
                            }
                        )
                    ]
                },
            }
        },
    )

    df = fetch_employee_department(client, "H5JN995")

    assert requests_mock.last_request.qs["expand"] == ["workassignments"]
    assert list(df.columns) == ["XRefCode", "DisplayName", "DepartmentXRefCode"]
    assert df.iloc[0]["XRefCode"] == "H5JN995"
    assert df.iloc[0]["DisplayName"] == "Woodland, Jason"
    assert df.iloc[0]["DepartmentXRefCode"] == "BTSI_SIMULATION_PRODUCT"


def test_get_employee_display_name():
    assert get_employee_display_name({"DisplayName": "Woodland, Jason"}) == "Woodland, Jason"
    assert get_employee_display_name({"XRefCode": "H5JN1"}) is None


def test_get_department_project_id_resolves_via_department_map():
    dept_map = DepartmentMap(
        {
            "BTSI_LEARNINGLOGICS": ProjectTask(96731, 3431179),
            "BTSI_SERVICES": ProjectTask(96732, 3431175),
            "BTSI_SIMULATION_PRODUCT": ProjectTask(96731, 3431179),
        }
    )

    assert get_department_project_id("BTSI_LEARNINGLOGICS", dept_map) == 96731
    assert get_department_project_id("BTSI_SIMULATION_PRODUCT", dept_map) == 96731
    assert get_department_project_id("BTSI_SERVICES", dept_map) == 96732
    assert get_department_project_id("btsi_services", dept_map) == 96732
    assert get_department_project_id("SOME_OTHER_DEPT", dept_map) is None


def test_get_department_project_id_uses_default_map_when_none_given():
    # conf/department_project_task.yaml is the real, checked-in mapping.
    assert get_department_project_id("BTSI_SERVICES") == 96732
    assert get_department_project_id("NOT_A_REAL_DEPARTMENT") is None


def test_get_department_task_id_resolves_via_department_map():
    dept_map = DepartmentMap(
        {
            "BTSI_LEARNINGLOGICS": ProjectTask(96731, 3431179),
            "BTSI_SERVICES": ProjectTask(96732, 3431175),
            "BTSI_SIMULATION_PRODUCT": ProjectTask(96731, 3431179),
        }
    )

    assert get_department_task_id("BTSI_LEARNINGLOGICS", dept_map) == 3431179
    assert get_department_task_id("BTSI_SIMULATION_PRODUCT", dept_map) == 3431179
    assert get_department_task_id("BTSI_SERVICES", dept_map) == 3431175
    assert get_department_task_id("btsi_services", dept_map) == 3431175
    assert get_department_task_id("SOME_OTHER_DEPT", dept_map) is None


def test_get_department_task_id_uses_default_map_when_none_given():
    # conf/department_project_task.yaml is the real, checked-in mapping.
    assert get_department_task_id("BTSI_SERVICES") == 3431175
    assert get_department_task_id("NOT_A_REAL_DEPARTMENT") is None
