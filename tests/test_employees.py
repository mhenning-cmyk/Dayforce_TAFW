from __future__ import annotations

import pandas as pd
import pytest

from tafw_ingest.dayforce_client import DayforceClient
from tafw_ingest.employees import employees_to_dataframe, fetch_employees_dataframe

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
            "XRefCode": "EMP-2",
            "FirstName": "Alan",
            "LastName": "Turing",
            "HomeOrganization": {"XRefCode": "ORG-1", "ShortName": "R&D"},
        },
        {"XRefCode": "EMP-1", "FirstName": "Ada", "LastName": "Lovelace"},
    ]
    df = employees_to_dataframe(rows)

    assert list(df.columns[:3]) == ["XRefCode", "FirstName", "LastName"]
    assert "HomeOrganization.ShortName" in df.columns
    assert len(df) == 2
    assert set(df["XRefCode"]) == {"EMP-1", "EMP-2"}


def test_dataframe_dedupes_on_xrefcode():
    rows = [{"XRefCode": "E1", "FirstName": "a"}, {"XRefCode": "E1", "FirstName": "a"}]
    assert len(employees_to_dataframe(rows)) == 1


def test_empty_roster_yields_empty_frame_with_xrefcode_column():
    df = employees_to_dataframe([])
    assert df.empty
    assert list(df.columns) == ["XRefCode"]


def test_fetch_employees_dataframe_end_to_end(client, requests_mock):
    requests_mock.get(
        BASE,
        json={
            "Data": [{"XRefCode": "A", "FirstName": "Ada"}],
            "Paging": {"Next": f"{BASE}?cursor=2"},
        },
    )
    requests_mock.get(
        f"{BASE}?cursor=2", json={"Data": [{"XRefCode": "B", "FirstName": "Alan"}]}
    )
    df = fetch_employees_dataframe(client)
    assert isinstance(df, pd.DataFrame)
    assert list(df["XRefCode"]) == ["A", "B"]
