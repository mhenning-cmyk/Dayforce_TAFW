"""Live Dayforce smoke test - manual, not part of the pytest suite.

The filename matches pytest's collection pattern but this script is meant to
be run by hand against a real Dayforce tenant:

    python tests/test_tafw_window.py

It uses the tenant in ``conf/settings.dev.yaml`` and the credentials in the OS
keyring (service ``dayforce-tafw``), and pulls both APPROVED and CANCELED
TimeAwayFromWork requests for a selection of employees over a rolling window -
one month back to three months ahead of today - split into 30-day chunks,
mirroring this Postman call per employee/status/chunk:

    GET {base}/Employees/{xref}/TimeAwayFromWork
        ?filterTAFWStartDate=...
        &filterTAFWEndDate=...
        &status=APPROVED   (or CANCELED)

Fetching both statuses together (rather than the separate APPROVED-only /
CANCELED-only scripts this replaces) matches how :mod:`tafw_ingest.get_tafw`
is meant to be used: each day-row keeps the same identity hash regardless of
status, so comparing hash sets across runs can tell "still there" from
"added"/"removed" - see that module's docstring.

The 30-day chunking here is not required by the API itself - live testing
showed the TAFW endpoint has no real window-size cap (see
``tafw_ingest.dayforce_client``'s module docstring) - it's done because this
script's caller wants results broken into 30-day periods. Chunk boundaries
overlap by one instant, so a request that happens to span one duplicates
across two raw fetches; ``expand_tafw_records_to_days`` dedupes by
``RecordHash`` before it reaches the final DataFrame.

All fetch/expand/hash logic lives in :mod:`tafw_ingest.get_tafw`; this script
is just the manual harness (auth, windowing, printing).

Exit codes: 0 = ok, 1 = request/auth failed, 2 = credentials missing.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

from tafw_ingest.config import Settings
from tafw_ingest.dayforce_client import DayforceApiError, DayforceClient
from tafw_ingest.get_tafw import (
    DEFAULT_STATUSES,
    expand_tafw_records_to_days,
    fetch_tafw_records,
)

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "conf" / "settings.dev.yaml"

# Hardcoded to match the known-good Postman call the scripts this replaces
# were built from; add more xrefs to pull several employees in one run.
XREF_CODES = ["H5JN767"]
STATUSES = DEFAULT_STATUSES  # (APPROVED, CANCELED)

WINDOW_LOOKBACK_DAYS = 30  # ~1 month back from today
WINDOW_HORIZON_DAYS = 90  # ~3 months ahead of today
CHUNK_DAYS = 30


def _chunk_window(
    start: datetime, end: datetime, chunk_days: int
) -> Iterator[tuple[datetime, datetime]]:
    """Split ``[start, end]`` into consecutive ``chunk_days``-wide windows."""
    step = timedelta(days=chunk_days)
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + step, end)
        yield cursor, chunk_end
        cursor = chunk_end


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log = logging.getLogger("dayforce-tafw-window-test")

    settings = Settings.from_env(config_path=CONFIG)
    endpoint_root = (
        f"{settings.dayforce_base_uri.rstrip('/')}/"
        f"{settings.dayforce_company}/{settings.dayforce_api_version}"
    )

    now = datetime.now(UTC)
    start_date = now - timedelta(days=WINDOW_LOOKBACK_DAYS)
    end_date = now + timedelta(days=WINDOW_HORIZON_DAYS)
    windows = list(_chunk_window(start_date, end_date, CHUNK_DAYS))

    print("=" * 68)
    print("Dayforce TAFW (APPROVED + CANCELED) window test")
    print("-" * 68)
    print(f"  endpoint : {endpoint_root}/Employees/{{xref}}/TimeAwayFromWork")
    print(f"  employees: {XREF_CODES}")
    print(f"  statuses : {list(STATUSES)}")
    print(
        f"  window   : {start_date.date().isoformat()} .. {end_date.date().isoformat()}"
        f" ({len(windows)} x {CHUNK_DAYS}d chunk(s))"
    )
    print(f"  username : {settings.dayforce_username or '(MISSING)'}")
    print(f"  password : {'(set)' if settings.dayforce_password else '(MISSING)'}")
    print(f"  test_mode: {settings.dayforce_test_mode}")
    print("=" * 68)

    if not (settings.dayforce_username and settings.dayforce_password):
        log.error(
            "Credentials not found. Set them with:\n"
            "    keyring set dayforce-tafw username\n"
            "    keyring set dayforce-tafw password"
        )
        return 2

    print("Authenticating to Dayforce")
    client = DayforceClient.from_settings(settings)
    print("Authentication Worked.")

    all_records = []
    try:
        for i, (chunk_start, chunk_end) in enumerate(windows, start=1):
            print(
                f"\nRequesting {list(STATUSES)} TAFW records for {XREF_CODES} "
                f"[{chunk_start.date()} .. {chunk_end.date()}] ({i}/{len(windows)})..."
            )
            chunk_records = fetch_tafw_records(
                client, XREF_CODES, chunk_start, chunk_end, statuses=STATUSES
            )
            print(f"  -> {len(chunk_records)} record(s)")
            all_records.extend(chunk_records)
    except DayforceApiError as err:
        if err.status in (401, 403):
            log.error("AUTH FAILED - HTTP %s. Body: %s", err.status, err.body)
            log.error("Check the username/password in keyring and the tenant/company.")
        elif err.status is not None:
            log.error("Request failed - HTTP %s. Body: %s", err.status, err.body)
        else:
            log.error("Could not reach Dayforce (network/TLS/proxy?): %s", err)
        return 1

    print(
        f"\n[1] TAFW OK - {len(all_records)} record(s) returned across "
        f"{len(windows)} window(s)."
    )
    for i, record in enumerate(all_records, start=1):
        print(f"\n  record {i}:")
        print(json.dumps(record, indent=2, default=str))

    day_df = expand_tafw_records_to_days(all_records)
    print(
        f"\n[2] Expanded to {len(day_df)} weekday-off row(s) "
        "(APPROVED + CANCELED combined, deduped by RecordHash)."
    )
    print(day_df.to_string(index=False))

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
