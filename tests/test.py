"""Live Dayforce smoke test - manual, not part of the pytest suite.

The filename ``test.py`` does not match pytest's ``test_*.py`` collection
pattern, so ``pytest`` ignores it. Run it by hand to check a real Dayforce
tenant end to end:

    python tests/test.py

It uses the tenant in ``conf/settings.dev.yaml`` and the credentials in the OS
keyring (service ``dayforce-tafw``), and does two things:

  1. Authenticate  - a 200 from an auth-required endpoint proves the creds.
  2. Employees     - page through ``GET /Employees`` and load a DataFrame.

Exit codes: 0 = ok, 1 = request/auth failed, 2 = credentials missing.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

try:
    ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # Databricks serverless compute does not set __file__.
    ROOT = Path("/Workspace/Users/mhenning@modelpath.net/Dayforce_TAFW")
sys.path.insert(0, str(ROOT / "src"))

import pandas as pd
from dateutil.relativedelta import relativedelta

from tafw_ingest.config import Settings
from tafw_ingest.dayforce_client import DayforceApiError, DayforceClient
from tafw_ingest.department_map import DepartmentMap
from tafw_ingest.employees import fetch_employees_dataframe
from tafw_ingest.roster import (
    EMPLOYEE_EXPAND,
    department_xref_of,
    iter_in_scope_employees,
)

CONFIG = ROOT / "conf" / "settings.dev.yaml"

# Step 4 probes per-employee detail (1 GET each); cap it so the smoke test stays quick.
ROSTER_PROBE_SAMPLE = 25


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    log = logging.getLogger("dayforce-smoke-test")

    settings = Settings.from_env(config_path=CONFIG)
    endpoint_root = (
        f"{settings.dayforce_base_uri.rstrip('/')}/"
        f"{settings.dayforce_company}/{settings.dayforce_api_version}"
    )

    print("=" * 68)
    print("Dayforce smoke test")
    print("-" * 68)
    print(f"  endpoint : {endpoint_root}/Employees")
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

    # if client worked print successful or client auth failed
    if client:
        print("Authentication Worked.")

    else:
        print("Authenticating Failed.")

    # --- 1. Authenticate ------------------------------------------------------
    try:
        print("Step 1. Get all employee unique Ids.")
        xrefs = client.list_employee_xrefs()
        print("Done")
    except DayforceApiError as err:
        if err.status in (401, 403):
            log.error("AUTH FAILED - HTTP %s. Body: %s", err.status, err.body)
            log.error("Check the username/password in keyring and the tenant/company.")
        elif err.status is not None:
            log.error("Request failed - HTTP %s. Body: %s", err.status, err.body)
        else:
            log.error("Could not reach Dayforce (network/TLS/proxy?): %s", err)
        return 1

    print(f"\n[1] AUTH OK - Employees returned {len(xrefs)} XRefCode(s).")
    if xrefs:
        print(f"    sample: {', '.join(xrefs[:5])}")

    # --- 2. Full roster -> pandas DataFrame ---------------------------------
    try:
        print("Converting Employee Roster List do Dataframe")
        df = fetch_employees_dataframe(client)
        print("Done")

        # Print the top 5 rows
        df.head(n=5)
    except DayforceApiError as err:
        log.error("Employees paging failed - HTTP %s. Body: %s", err.status, err.body)
        return 1

    pd.set_option("display.max_columns", 12)
    pd.set_option("display.width", 160)
    print(f"\n[2] Roster DataFrame: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"    columns: {list(df.columns)[:12]}{' ...' if df.shape[1] > 12 else ''}")
    print(df.head(5).to_string(index=False))

    # --- 3. TAFW date range: today - 1 month .. today + 3 months ------------
    # The Dayforce TAFW endpoint only accepts a <= 31-day window, so the range
    # is split into an array of (start, end) windows to iterate over.
    print("\nCreating date-range array for TAFW requests")
    window_days = settings.tafw_window_days  # 30; Dayforce caps at 31
    today = date.today()
    range_start = today - relativedelta(months=1)
    range_end = today + relativedelta(months=3)

    tafw_windows: list[tuple[date, date]] = []
    cursor = range_start
    while cursor <= range_end:
        win_end = min(cursor + timedelta(days=window_days - 1), range_end)
        tafw_windows.append((cursor, win_end))
        cursor = win_end + timedelta(days=1)

    print(
        f"\n[3] TAFW range: {range_start.isoformat()} -> {range_end.isoformat()}  "
        f"({len(tafw_windows)} windows, <= {window_days}d each)"
    )
    for i, (start, end) in enumerate(tafw_windows, start=1):
        print(f"    window {i}: {start.isoformat()} .. {end.isoformat()}")

    # --- 4. Narrow the roster to in-scope departments ----------------------
    # Only employees whose Department rolls up to projects 96731 / 96732 matter.
    # First: confirm WHERE the department lives in the employee payload.
    dept_map = DepartmentMap.load()
    print(
        f"\nResolving department for a sample of {ROSTER_PROBE_SAMPLE} employees "
        f"(in-scope departments: {sorted(dept_map.in_scope_departments)})"
    )

    # Prefer a numeric xref for the probe (the H5JN* codes look like non-employees).
    probe_xref = next((x for x in xrefs if x.isdigit()), xrefs[0])
    detail: dict = {}
    for exp in (EMPLOYEE_EXPAND, "WorkAssignments", "EmploymentStatuses", None):
        try:
            detail = client.get_employee(probe_xref, expand=exp)
            print(
                f"\n[4a] GET /Employees/{probe_xref}"
                f"{f'?expand={exp}' if exp else ' (no expand)'}  -> OK"
            )
            break
        except DayforceApiError as err:
            print(f"\n[4a] expand={exp!r} -> HTTP {err.status}: {err.body}")
    print(f"     top-level keys: {sorted(detail.keys())}")
    for key in (
        "WorkAssignments",
        "EmploymentStatuses",
        "HomeOrganization",
        "DepartmentXRefCode",
        "OrgUnitInfos",
    ):
        if key in detail:
            print(f"     {key}: {json.dumps(detail[key], default=str)[:600]}")
    print(f"     department_xref_of() -> {department_xref_of(detail)!r}")

    sample = xrefs[:ROSTER_PROBE_SAMPLE]
    in_scope = list(iter_in_scope_employees(client, dept_map, xrefs=sample))
    print(f"\n[4b] {len(in_scope)}/{len(sample)} sampled employees are in scope:")
    for e in in_scope[:10]:
        print(
            f"     {e.xref:>10}  {e.department_xref:<24}  "
            f"project={e.project_id} task={e.task_id}"
        )

    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
