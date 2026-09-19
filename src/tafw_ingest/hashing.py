"""Deterministic record-identity hash for TAFW day-records.

Per the Bluedrop hybrid-architecture design, every TAFW *day-record* is
identified by a fingerprint built from exactly four fields:

    EmployeeXRefCode  |  PTO date  |  approval/type code  |  hours requested

Comparing fingerprints between sync cycles is the whole change-detection
mechanism:

* a hash that is new this cycle          -> record was created
* a hash present last cycle but not now  -> record was cancelled / denied
* same natural key, different hash        -> record was changed (hours or type)

This is a deliberate simplification of the old ``bluedrop-mavenlink-sync``
approach (SHA-1 over the whole scrubbed event payload). Keeping the input to
four normalized fields makes the hash easy to reproduce by hand, in SQL, or in
a spreadsheet when troubleshooting.

THE SPEC BELOW IS FROZEN. Changing the field list, order, separator,
normalization rules, or digest algorithm re-keys every row already in the
staging table. If a change is ever unavoidable, bump
:data:`HASH_SPEC_VERSION`, store it alongside the hash, and plan a migration.

v2 (:data:`HASH_SPEC_VERSION` = 2): truncated the SHA-256 hex digest from 64
to :data:`RECORD_HASH_LENGTH` (16) characters for a more concise column value.
16 hex chars = 64 bits of the digest; by the birthday bound, P(any collision)
is ~2.7e-8 at 1 million rows and ~2.7e-6 at 10 million - this table's
realistic scale (a few hundred employees x TAFW days) is orders of magnitude
below where that risk becomes worth worrying about. Because it's a straight
prefix of the v1 digest, existing v1 rows can be migrated in place with
``UPDATE {table} SET RecordHash = substring(RecordHash, 1, 16)`` rather than
needing a full re-key.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
from decimal import Decimal, InvalidOperation
from typing import Any

__all__ = [
    "HASH_SPEC_VERSION",
    "FIELD_SEPARATOR",
    "RECORD_HASH_LENGTH",
    "normalize_employee_xref",
    "normalize_pto_date",
    "normalize_type_code",
    "normalize_hours",
    "canonical_string",
    "record_hash",
]

HASH_SPEC_VERSION = 2

#: Joins the four normalized fields. A pipe cannot occur in any of them after
#: normalization (dates are ISO, hours are numeric, codes are single tokens).
FIELD_SEPARATOR = "|"

#: Hex characters kept from the SHA-256 digest - see the v2 note above.
RECORD_HASH_LENGTH = 16


# --------------------------------------------------------------------------- #
# Field normalization - applied before hashing, never after data lands        #
# --------------------------------------------------------------------------- #
def normalize_employee_xref(value: Any) -> str:
    """Trim and upper-case the employee XRefCode.

    Dayforce XRefCodes are case-insensitive tokens; normalizing avoids a
    spurious hash change if the API ever varies casing or pads whitespace.
    """
    text = str(value).strip().upper()
    if not text:
        raise ValueError("employee_xref is empty after normalization")
    return text


def normalize_pto_date(value: Any) -> str:
    """Return the PTO day as ``YYYY-MM-DD`` with no time component.

    Accepts a :class:`datetime.date`, :class:`datetime.datetime`, or a string.
    Strings may be a bare date or any ISO-8601 timestamp (``...T..``, trailing
    ``Z`` tolerated); only the calendar date is kept.
    """
    if isinstance(value, _dt.datetime):
        return value.date().isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()

    text = str(value).strip()
    if not text:
        raise ValueError("pto_date is empty")
    text = text.replace("Z", "+00:00")
    # Fast path: already a bare date.
    try:
        return _dt.date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        pass
    try:
        return _dt.datetime.fromisoformat(text).date().isoformat()
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"cannot parse pto_date {value!r}") from exc


def normalize_type_code(value: Any) -> str:
    """Trim and upper-case the approval / pay-code / reason type.

    This is the *kind* of time away (e.g. ``VACATION``, ``PERSONAL``,
    ``VOLUNTEER``), not the workflow status - the staging feed only carries
    APPROVED records, and a status transition shows up as the hash
    disappearing, not as a type change.
    """
    text = str(value).strip().upper()
    if not text:
        raise ValueError("type_code is empty after normalization")
    return text


def normalize_hours(value: Any) -> str:
    """Return hours as a fixed-point string with exactly two decimals.

    ``8`` / ``8.0`` / ``"8"`` / ``Decimal("8")`` all normalize to ``"8.00"``
    so numeric-format noise from the API never changes the hash. A genuine
    change (``8.00`` -> ``4.00``) still does.
    """
    try:
        quantized = Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"cannot parse hours {value!r}") from exc
    if quantized < 0:
        raise ValueError(f"hours is negative: {value!r}")
    return f"{quantized:.2f}"


# --------------------------------------------------------------------------- #
# The hash                                                                    #
# --------------------------------------------------------------------------- #
def canonical_string(
    employee_xref: Any, pto_date: Any, type_code: Any, hours: Any
) -> str:
    """Return the exact string that gets hashed. Handy in tests and logs."""
    return FIELD_SEPARATOR.join(
        (
            normalize_employee_xref(employee_xref),
            normalize_pto_date(pto_date),
            normalize_type_code(type_code),
            normalize_hours(hours),
        )
    )


def record_hash(employee_xref: Any, pto_date: Any, type_code: Any, hours: Any) -> str:
    """Truncated SHA-256 hex digest identifying one TAFW day-record.

    >>> record_hash("abc123", "2026-03-14T00:00:00Z", "vacation", 8)
    '...'  # stable 16-char hex string; see tests/test_hashing.py for goldens
    """
    canonical = canonical_string(employee_xref, pto_date, type_code, hours)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:RECORD_HASH_LENGTH]
