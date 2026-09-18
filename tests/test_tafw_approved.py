"""Live Dayforce smoke test - manual, not part of the pytest suite.

The filename ``test_tafw_approved.py`` matches pytest's collection pattern but
this script is meant to be run by hand against a real Dayforce tenant:

    python tests/test_tafw_approved.py

It uses the tenant in ``conf/settings.dev.yaml`` and the credentials in the OS
keyring (service ``dayforce-tafw``), and pulls APPROVED TimeAwayFromWork
requests for a selection of employees, mirroring this Postman call per
employee:

    GET {base}/Employees/{xref}/TimeAwayFromWork
        ?filterTAFWStartDate=...
        &filterTAFWEndDate=...
        &status=APPROVED

All fetch/expand/hash logic lives in :mod:`tafw_ingest.get_tafw`; this script
is just the manual harness (auth, printing).

Exit codes: 0 = ok, 1 = request/auth failed, 2 = credentials missing.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from tafw_ingest.config import Settings
from tafw_ingest.dayforce_client import DayforceApiError, DayforceClient
from tafw_ingest.get_tafw import expand_tafw_records_to_days, fetch_tafw_records

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / "conf" / "settings.dev.yaml"

# Hardcoded to match the known-good Postman call above; add more xrefs to
# pull several employees in one run.
XREF_CODES = ["H5JN767"]
STATUS = DayforceClient.STATUS_APPROVED
START_DATE = datetime(2026, 1, 1, tzinfo=UTC)
END_DATE = datetime(2026, 12, 31, tzinfo=UTC)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log = logging.getLogger("dayforce-tafw-approved-test")

    settings = Settings.from_env(config_path=CONFIG)
    endpoint_root = (
        f"{settings.dayforce_base_uri.rstrip('/')}/"
        f"{settings.dayforce_company}/{settings.dayforce_api_version}"
    )

    print("=" * 68)
    print("Dayforce TAFW (APPROVED) test")
    print("-" * 68)
    print(f"  endpoint : {endpoint_root}/Employees/{{xref}}/TimeAwayFromWork")
    print(f"  employees: {XREF_CODES}")
    print(f"  status   : {STATUS}")
    print(
        f"  window   : {START_DATE.date().isoformat()} .. {END_DATE.date().isoformat()}"
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

    try:
        print(f"\nRequesting {STATUS} TAFW records for {XREF_CODES}...")
        records = fetch_tafw_records(
            client, XREF_CODES, START_DATE, END_DATE, statuses=[STATUS]
        )
    except DayforceApiError as err:
        if err.status in (401, 403):
            log.error("AUTH FAILED - HTTP %s. Body: %s", err.status, err.body)
            log.error("Check the username/password in keyring and the tenant/company.")
        elif err.status is not None:
            log.error("Request failed - HTTP %s. Body: %s", err.status, err.body)
        else:
            log.error("Could not reach Dayforce (network/TLS/proxy?): %s", err)
        return 1

    print(f"\n[1] TAFW OK - {len(records)} {STATUS} record(s) returned.")
    for i, record in enumerate(records, start=1):
        print(f"\n  record {i}:")
        print(json.dumps(record, indent=2, default=str))

    day_df = expand_tafw_records_to_days(records)
    print(f"\n[2] Expanded to {len(day_df)} weekday-off row(s).")
    print(day_df.to_string(index=False))

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
