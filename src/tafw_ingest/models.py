"""Typed TAFW day-record - the unit written to the staging table.

A :class:`DayRecord` is produced by :mod:`tafw_ingest.normalize` from one raw
Dayforce ``TimeAwayFromWork`` payload entry (optionally expanded to one record
per calendar day). It is immutable and self-hashing: :attr:`DayRecord.record_hash`
is derived from the four identity fields via :mod:`tafw_ingest.hashing`.

The staging Delta table columns map 1:1 onto :meth:`DayRecord.to_staging_row`.
"""

from __future__ import annotations

import datetime as _dt
import json
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from tafw_ingest.hashing import (
    normalize_employee_xref,
    normalize_hours,
    normalize_type_code,
    record_hash,
)

__all__ = ["DayRecord"]


class DayRecord(BaseModel):
    """One approved TAFW day, ready to MERGE into ``staging.tafw_day_record``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # --- identity fields (feed the hash) ---
    employee_xref: str
    pto_date: _dt.date
    type_code: str = Field(description="pay-code / reason type, e.g. VACATION")
    hours: Decimal

    # --- passthrough / audit (not hashed) ---
    status: str = "APPROVED"
    reason_name: str | None = None
    time_start: str | None = None
    time_end: str | None = None
    all_day: bool | None = None
    date_of_request: str | None = None
    source_payload: str = "{}"

    @field_validator("employee_xref")
    @classmethod
    def _norm_xref(cls, v: str) -> str:
        return normalize_employee_xref(v)

    @field_validator("type_code")
    @classmethod
    def _norm_type(cls, v: str) -> str:
        return normalize_type_code(v)

    @field_validator("hours")
    @classmethod
    def _norm_hours(cls, v: Decimal) -> Decimal:
        # Reuse the hashing spec's parser/quantizer so the stored value and the
        # hashed value can never diverge.
        return Decimal(normalize_hours(v))

    @computed_field  # type: ignore[prop-decorator]
    @property
    def record_hash(self) -> str:
        """Deterministic SHA-256 identity (see :mod:`tafw_ingest.hashing`)."""
        return record_hash(self.employee_xref, self.pto_date, self.type_code, self.hours)

    # ------------------------------------------------------------------ #
    def to_staging_row(
        self, *, run_id: str, now: _dt.datetime | None = None
    ) -> dict[str, Any]:
        """Return a dict keyed by staging-table column name.

        ``first_seen_at`` / ``last_seen_at`` / ``is_active`` are set for an
        INSERT; the MERGE keeps ``first_seen_at`` on an existing row.
        """
        ts = (now or _dt.datetime.now(_dt.UTC)).replace(microsecond=0)
        return {
            "record_hash": self.record_hash,
            "employee_xref": self.employee_xref,
            "pto_date": self.pto_date,
            "approval_type_code": self.type_code,
            "hours_requested": self.hours,
            "reason_name": self.reason_name,
            "status": self.status,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "all_day": self.all_day,
            "date_of_request": self.date_of_request,
            "source_payload": self.source_payload,
            "first_seen_at": ts,
            "last_seen_at": ts,
            "is_active": True,
            "deleted_at": None,
            "ingest_run_id": run_id,
        }

    @classmethod
    def from_fields(
        cls,
        *,
        employee_xref: str,
        pto_date: Any,
        type_code: str,
        hours: Any,
        raw: dict[str, Any] | None = None,
        status: str = "APPROVED",
    ) -> DayRecord:
        """Convenience builder used by :mod:`tafw_ingest.normalize` and tests."""
        raw = raw or {}
        if not isinstance(pto_date, _dt.date):
            pto_date = _dt.date.fromisoformat(str(pto_date)[:10])
        return cls(
            employee_xref=employee_xref,
            pto_date=pto_date,
            type_code=type_code,
            hours=Decimal(str(hours)),
            status=status,
            reason_name=raw.get("ReasonName"),
            time_start=raw.get("TimeStart"),
            time_end=raw.get("TimeEnd"),
            all_day=_as_bool(raw.get("AllDay")),
            date_of_request=raw.get("DateOfRequest"),
            source_payload=json.dumps(raw, sort_keys=True, default=str),
        )


def _as_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "t"}
