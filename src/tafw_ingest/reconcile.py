"""Compare this cycle's day-records against what is already in staging.

Given the current :class:`DayRecord` list and the set of records currently
*active* in the staging table (keyed by hash), classify each into:

* **new**       - hash never seen before, and no prior record for the same
                  (employee, date) natural key
* **changed**   - hash is new, but a prior active record for the same
                  (employee, date) has now disappeared (hours or type changed)
* **unchanged** - hash present in both cycles
* **cancelled** - hash was active last cycle, absent this cycle, and not
                  explained by a "changed" pairing -> the request was
                  cancelled or denied in Dayforce

``new + changed`` are the rows to upsert; ``cancelled`` hashes get soft-deleted
(``is_active = false``). ``unchanged`` rows only get their ``last_seen_at``
bumped so the table can show sync liveness.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

from tafw_ingest.models import DayRecord

__all__ = ["StoredRecord", "ReconcileResult", "reconcile"]


@dataclass(frozen=True)
class StoredRecord:
    """The subset of a staging row needed to reconcile against it."""

    record_hash: str
    employee_xref: str
    pto_date: _dt.date


@dataclass
class ReconcileResult:
    new: list[DayRecord] = field(default_factory=list)
    changed: list[DayRecord] = field(default_factory=list)
    unchanged: list[DayRecord] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)  # hashes gone for good
    superseded: list[str] = field(default_factory=list)  # old hashes of `changed`

    @property
    def upserts(self) -> list[DayRecord]:
        return [*self.new, *self.changed]

    @property
    def deactivations(self) -> list[str]:
        """Hashes to soft-delete: truly cancelled + replaced-by-a-change."""
        return [*self.cancelled, *self.superseded]

    def summary(self) -> dict[str, int]:
        return {
            "new": len(self.new),
            "changed": len(self.changed),
            "unchanged": len(self.unchanged),
            "cancelled": len(self.cancelled),
            "superseded": len(self.superseded),
        }


def reconcile(
    current: Iterable[DayRecord],
    previous_active: Mapping[str, StoredRecord],
) -> ReconcileResult:
    # Materialize once; keep caller order for stable, reviewable output.
    current_list = list(current)
    current_hashes = {r.record_hash for r in current_list}
    previous_hashes = set(previous_active)
    gone_hashes = [h for h in previous_active if h not in current_hashes]

    # Natural key -> the prior hash that had it, for the ones that vanished.
    prev_gone_by_key: dict[tuple[str, _dt.date], str] = {
        (previous_active[h].employee_xref, previous_active[h].pto_date): h
        for h in gone_hashes
    }

    result = ReconcileResult()
    changed_gone: set[str] = set()

    for rec in current_list:
        if rec.record_hash in previous_hashes:
            result.unchanged.append(rec)
            continue
        prior = prev_gone_by_key.get((rec.employee_xref, rec.pto_date))
        if prior is not None:
            result.changed.append(rec)
            changed_gone.add(prior)
        else:
            result.new.append(rec)

    result.superseded = [h for h in gone_hashes if h in changed_gone]
    result.cancelled = [h for h in gone_hashes if h not in changed_gone]
    return result
