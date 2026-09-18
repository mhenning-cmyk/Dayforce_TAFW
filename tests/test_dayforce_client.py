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
        max_rps=10,  # Dayforce's documented ceiling, so tests never queue/sleep
        max_rpm=100,
        lookback_days=30,
        horizon_days=90,
    )


def test_api_base_built_correctly(client):
    assert client.api_base == "https://dayforcehcm.com/Api/bluedrop/v1/"


def test_limiter_enforces_both_per_second_and_per_minute_rates():
    client = DayforceClient(
        "svc-user", "secret", "bluedrop", max_rps=8, max_rpm=90
    )
    rates = {r.limit: r.interval for r in client._limiter.buckets()[0].rates}
    assert rates == {8: 1000, 90: 60_000}  # 8/sec, 90/min


def test_max_rps_and_max_rpm_are_clamped_to_dayforce_ceiling():
    """Dayforce's documented ceiling is 10/sec, 100/min - a caller passing a
    higher value (e.g. a misconfigured setting) must not be able to exceed it."""
    client = DayforceClient(
        "svc-user", "secret", "bluedrop", max_rps=999, max_rpm=999
    )
    assert client.max_rps == 10
    assert client.max_rpm == 100


def test_lookback_horizon_span_covers_the_whole_range(client):
    now = _dt.datetime(2026, 6, 15, tzinfo=_dt.UTC)
    start, end = client._lookback_horizon_span(now)
    assert start.date() == _dt.date(2026, 5, 16)  # 30 days back
    assert end.date() == _dt.date(2026, 9, 13)  # 90 days forward


def test_list_employee_xrefs(client, requests_mock):
    requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees",
        json={"Data": [{"XRefCode": "EMP-001"}, {"XRefCode": "EMP-002"}, {"NoXRef": 1}]},
    )
    assert client.list_employee_xrefs() == ["EMP-001", "EMP-002"]


def test_tafw_in_timeframe_issues_one_call_for_the_whole_span(client, requests_mock):
    m = requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork",
        json={"Data": [{"TimeStart": "2026-06-01T00:00:00", "NetHours": 8}]},
    )
    now = _dt.datetime(2026, 6, 15, tzinfo=_dt.UTC)
    out = client.get_tafw_records_for_employee_in_timeframe(
        "EMP-001", "APPROVED", now=now
    )
    assert m.call_count == 1  # no date-window chunking
    assert len(out) == 1
    q = m.last_request.qs
    assert q["status"] == ["approved"]
    assert "filtertafwstartdate" in q and "filtertafwenddate" in q


def test_get_tafw_records_for_employee_follows_paging_next(client, requests_mock):
    page1_url = "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork"
    page2_url = "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork?cursor=2"
    requests_mock.get(
        page1_url,
        json={
            "Data": [{"TimeStart": "2026-06-01T00:00:00", "NetHours": 8}],
            "Paging": {"Next": page2_url},
        },
    )
    requests_mock.get(
        page2_url,
        json={"Data": [{"TimeStart": "2026-06-02T00:00:00", "NetHours": 8}]},
    )
    out = client.get_tafw_records_for_employee("EMP-001", "APPROVED", _now(), _now())
    assert len(out) == 2
    assert [r["TimeStart"] for r in out] == [
        "2026-06-01T00:00:00",
        "2026-06-02T00:00:00",
    ]


def test_recovers_from_a_rate_limit_denial_disguised_as_an_ok_response(
    client, requests_mock, monkeypatch
):
    """Dayforce's guide doesn't promise a 429 for a denial - just 'a message
    noting that the limit has been reached' - so an ok-looking (200) response
    carrying that message must be retried, not trusted as real data."""
    monkeypatch.setattr("tafw_ingest.dayforce_client.time.sleep", lambda _: None)
    responses = [
        {"json": {"Message": "Rate limit exceeded, please try again"}, "status_code": 200},
        {"json": {"Data": [{"TimeStart": "2026-06-01T00:00:00", "NetHours": 8}]}},
    ]
    m = requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork",
        responses,
    )
    out = client.get_tafw_records_for_employee("EMP-001", "APPROVED", _now(), _now())
    assert len(out) == 1
    assert m.call_count == 2


def test_gives_up_after_repeated_rate_limit_denials(client, requests_mock, monkeypatch):
    monkeypatch.setattr("tafw_ingest.dayforce_client.time.sleep", lambda _: None)
    requests_mock.get(
        "https://dayforcehcm.com/Api/bluedrop/v1/Employees/EMP-001/TimeAwayFromWork",
        json={"Message": "Requests exceed the allowed threshold"},
    )
    with pytest.raises(DayforceApiError, match="rate-limited"):
        client.get_tafw_records_for_employee("EMP-001", "APPROVED", _now(), _now())


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
