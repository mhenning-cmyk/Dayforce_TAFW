from __future__ import annotations

import datetime as _dt

import pytest
import requests

from tafw_ingest.dayforce_client import DayforceApiError, DayforceClient


@pytest.fixture
def client():
    return DayforceClient(
        "svc-user",
        "secret",
        "bluedrop",
        base_uri="https://dayforcehcm.com/Api",
        throttle_seconds=0,  # keep tests fast
        max_rpm=1000,
        lookback_days=30,
        horizon_days=90,
        window_days=30,
    )


def test_api_base_built_correctly(client):
    assert client.api_base == "https://dayforcehcm.com/Api/bluedrop/v1/"


def test_windows_cover_the_whole_span_in_le_31_day_chunks(client):
    now = _dt.datetime(2026, 6, 15, tzinfo=_dt.UTC)
    windows = client._windows(now)
    assert windows[0][0].date() == _dt.date(2026, 5, 16)  # 30 days back
    assert windows[-1][1].date() >= _dt.date(2026, 9, 13)  # 90 days forward
    for start, end in windows:
        assert (end - start).days <= 31
    # windows are contiguous, no gaps
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:], strict=False):
        assert (next_start.date() - prev_end.date()).days == 1


def test_list_employee_xrefs(client, requests_mock):
    requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees",
        json={"Data": [{"XRefCode": "EMP-001"}, {"XRefCode": "EMP-002"}, {"NoXRef": 1}]},
    )
    assert client.list_employee_xrefs() == ["EMP-001", "EMP-002"]


def test_tafw_fan_out_issues_one_call_per_window(client, requests_mock):
    m = requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork",
        json={"Data": [{"TimeStart": "2026-06-01T00:00:00", "NetHours": 8}]},
    )
    now = _dt.datetime(2026, 6, 15, tzinfo=_dt.UTC)
    out = client.get_tafw_records_for_employee_in_timeframe(
        "EMP-001", "APPROVED", now=now
    )
    assert m.call_count == len(client._windows(now))
    assert len(out) == m.call_count  # one row echoed per window
    q = m.last_request.qs
    assert q["status"] == ["approved"]
    assert "filtertafwstartdate" in q and "filtertafwenddate" in q


def test_http_error_raises_dayforce_api_error(client, requests_mock):
    requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees/BAD/TimeAwayFromWork",
        status_code=403,
        json={"Message": "Forbidden"},
    )
    with pytest.raises(DayforceApiError) as exc:
        client.get_tafw_records_for_employee("BAD", "APPROVED", _now(), _now())
    assert exc.value.status == 403


def test_auth_header_survives_cross_host_redirect(client, requests_mock):
    base = "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork"
    regional = "https://us-west-2.dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork"
    requests_mock.get(base, status_code=302, headers={"Location": regional})
    captured = {}

    def _regional(request, context):
        captured["auth"] = request.headers.get("Authorization")
        return {"Data": []}

    requests_mock.get(regional, json=_regional)
    client.get_tafw_records_for_employee("EMP-001", "APPROVED", _now(), _now())
    assert captured["auth"] is not None and captured["auth"].startswith("Basic ")


def _now() -> _dt.datetime:
    return _dt.datetime(2026, 6, 1, tzinfo=_dt.UTC)


def test_transport_error_is_wrapped(client, requests_mock):
    requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork",
        exc=requests.exceptions.ConnectTimeout,
    )
    with pytest.raises(DayforceApiError):
        client.get_tafw_records_for_employee("EMP-001", "APPROVED", _now(), _now())
