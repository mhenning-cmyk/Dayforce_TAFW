"""End-to-end cycle with a fake Dayforce client + the in-memory writer.

Exercises everything except the real HTTP transport and the real MERGE:
fetch -> normalize -> hash -> reconcile -> staging apply, across two cycles.
"""

from __future__ import annotations

from tafw_ingest.config import Settings
from tafw_ingest.pipeline import run_cycle
from tafw_ingest.staging import InMemoryStagingWriter


class FakeDayforceClient:
    def __init__(self, roster, entries_by_xref):
        self._roster = roster
        self._entries = entries_by_xref

    def list_employee_xrefs(self):
        return list(self._roster)

    def get_tafw_records_for_employee_in_timeframe(self, xref, status, *, now=None):
        return list(self._entries.get(xref, []))


def _settings(**over):
    base = dict(
        dayforce_username="u",
        dayforce_password="p",
        writer="memory",
        tafw_statuses=["APPROVED"],
    )
    base.update(over)
    return Settings(**base)


def _entry(start, hours, code="VAC", reason="Vacation", status="APPROVED", end=None):
    return {
        "TimeStart": f"{start}T00:00:00",
        "TimeEnd": f"{end or start}T00:00:00",
        "NetHours": hours,
        "ReasonName": reason,
        "PayAdjustmentCodeName": code,
        "DateOfRequest": "2026-01-15T00:00:00",
        "AllDay": True,
        "Status": status,
    }


def test_first_cycle_inserts_everything():
    client = FakeDayforceClient(
        ["EMP-001", "EMP-002"],
        {
            "EMP-001": [_entry("2026-03-16", 8), _entry("2026-03-17", 8)],
            "EMP-002": [_entry("2026-03-16", 4, code="PERS", reason="Personal")],
        },
    )
    writer = InMemoryStagingWriter()
    result = run_cycle(_settings(), df_client=client, writer=writer)

    assert result.employees == 2
    assert result.day_records == 3
    assert result.reconcile.summary() == {
        "new": 3,
        "changed": 0,
        "unchanged": 0,
        "cancelled": 0,
        "superseded": 0,
    }
    assert result.merge.inserted == 3
    assert len(writer.load_active_records()) == 3


def test_second_cycle_detects_change_and_cancellation():
    writer = InMemoryStagingWriter()
    settings = _settings()

    c1 = FakeDayforceClient(
        ["EMP-001"],
        {"EMP-001": [_entry("2026-03-16", 8), _entry("2026-03-20", 8)]},
    )
    run_cycle(settings, df_client=c1, writer=writer)

    # cycle 2: 03-16 hours changed 8 -> 4; 03-20 disappeared; 03-25 is new
    c2 = FakeDayforceClient(
        ["EMP-001"],
        {"EMP-001": [_entry("2026-03-16", 4), _entry("2026-03-25", 8)]},
    )
    result = run_cycle(settings, df_client=c2, writer=writer)

    assert result.reconcile.summary() == {
        "new": 1,
        "changed": 1,
        "unchanged": 0,
        "cancelled": 1,
        "superseded": 1,
    }
    active = writer.load_active_records()
    assert len(active) == 2  # 03-16 (new hash) + 03-25
    # both the truly-cancelled row (03-20) and the superseded old 03-16 row are
    # retained but inactive - that soft-delete history is the audit trail.
    inactive = [r for r in writer.rows.values() if not r["is_active"]]
    assert len(inactive) == 2
    assert all(r["deleted_at"] is not None for r in inactive)


def test_idempotent_when_nothing_changes():
    writer = InMemoryStagingWriter()
    settings = _settings()
    client = FakeDayforceClient(["EMP-001"], {"EMP-001": [_entry("2026-03-16", 8)]})

    run_cycle(settings, df_client=client, writer=writer)
    result = run_cycle(settings, df_client=client, writer=writer)

    assert result.reconcile.summary() == {
        "new": 0,
        "changed": 0,
        "unchanged": 1,
        "cancelled": 0,
        "superseded": 0,
    }
    assert result.merge.inserted == 0


def test_roster_override_skips_employee_listing():
    class Boom(FakeDayforceClient):
        def list_employee_xrefs(self):  # must not be called
            raise AssertionError("roster override should prevent Employees call")

    client = Boom(["ignored"], {"EMP-009": [_entry("2026-03-16", 8)]})
    settings = _settings(tafw_employee_xrefs=["EMP-009"])
    result = run_cycle(settings, df_client=client, writer=InMemoryStagingWriter())
    assert result.day_records == 1
