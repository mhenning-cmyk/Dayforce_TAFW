###### Dayforce Integration Service #########

#Import python libaries
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
import pandas as pd

# Databricks notebooks get `dbutils` injected as a global automatically; it
# never exists locally (script, REPL, or an interactive/Jupyter cell), so its
# presence is how we tell the two environments apart. This has to run before
# the `tafw_ingest` imports below: locally the package is on sys.path via
# `pip install -e .`, but a Databricks Workspace notebook doesn't get that for
# free, so `import tafw_ingest` 404s unless its `src/` dir is added first.
IN_DATABRICKS = "dbutils" in globals()

if IN_DATABRICKS:
    # Fixed Workspace path this repo is synced to - update if it ever moves.
    DATABRICKS_REPO_ROOT = Path("/Workspace/Users/mhenning@modelpath.net/Dayforce_TAFW")
    sys.path.insert(0, str(DATABRICKS_REPO_ROOT / "src"))

    # Unlike local (where `pip install -e .` already populated .venv),
    # Databricks compute doesn't come with this repo's dependencies
    # pre-installed - install them here, before anything below imports
    # tafw_ingest or the third-party packages (e.g. pydantic-settings) it
    # depends on.
    import subprocess

    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-q", "-r", str(DATABRICKS_REPO_ROOT / "requirements.txt")]
    )

#Import custom python libraries
from tafw_ingest import employees
from tafw_ingest.config import Settings
from tafw_ingest.dayforce_client import DayforceClient
from tafw_ingest.department_map import DepartmentMap
from tafw_ingest.employee_department_queries import MERGE_EMPLOYEE_DEPARTMENT_SQL
from tafw_ingest.get_tafw import DEFAULT_STATUSES, expand_tafw_records_to_days, fetch_tafw_records
from tafw_ingest.tafw_day_record_queries import MERGE_TAFW_DAY_RECORD_SQL

if IN_DATABRICKS:
    # Databricks: non-secret defaults (base_uri, company, ...) come from the
    # synced repo's conf/settings.dev.yaml, same file `from_env` reads
    # locally. Credentials come from the `dayforce` secret scope; everything
    # else from job widgets (falls back to Settings' own defaults for any
    # widget that isn't declared).
    settings = Settings.from_databricks(  # noqa: F821
        dbutils, secret_scope="dayforce", config_path=DATABRICKS_REPO_ROOT / "conf" / "settings.dev.yaml"
    )
else:
    # Local (script, REPL, or an interactive cell with no `__file__`): find
    # the repo root by walking up from the current file/directory until
    # conf/settings.dev.yaml turns up, then load non-secret defaults from it;
    # credentials come from the OS keyring or the environment.
    try:
        _here = Path(__file__).resolve()
    except NameError:  # no __file__ in an interactive/notebook cell
        _here = Path.cwd()
    ROOT = next(
        (p for p in (_here, *_here.parents) if (p / "conf" / "settings.dev.yaml").is_file()),
        _here,
    )
    CONFIG = ROOT / "conf" / "settings.dev.yaml"
    settings = Settings.from_env(config_path=CONFIG)

client = DayforceClient.from_settings(settings)

############################# Employees ############################# 
#Step 1. Get all employees from dayforce.
print('Getting employees from dayforce')
employees_df = employees.employees_to_dataframe(client.iter_employees())
print(employees_df.head(n=10))

print('Done')

#### Testing select a subset 
employees_df = employees_df.iloc[0:9]


#Step 2, Get the department and projects for all of these employees
dept_map = DepartmentMap.load()  # load once - each lookup below would otherwise re-read the YAML

employee_department_rows = []
for xRefCode in employees_df["XRefCode"]:
    #Get the department for this xrefcode
    department_df = employees.fetch_employee_department(client, xRefCode)
    department_xref = department_df.iloc[0]["DepartmentXRefCode"]

    employee_department_rows.append(
        {
            "XRefCode": xRefCode,
            "DepartmentXRefCode": department_xref,
            "ProjectId": employees.get_department_project_id(department_xref, dept_map),
            "TaskId": employees.get_department_task_id(department_xref, dept_map),
        }
    )

# One row per employee - de-dupe defensively in case the roster ever repeats an XRefCode.
# This list (not the display DataFrame below) is what Step 3 uploads.
employee_department_rows = list(
    {row["XRefCode"]: row for row in employee_department_rows}.values()
)

# Int64 (nullable) rather than plain int, purely for display - an out-of-scope
# department resolves to None, which plain int can't hold.
employee_department_df = pd.DataFrame(employee_department_rows).astype(
    {"ProjectId": "Int64", "TaskId": "Int64"}
)

#Remove any records where the Project ID is null
employee_department_df = employee_department_df[employee_department_df["ProjectId"].notna()].reset_index(drop = True)
print(employee_department_df)

#Step 3. Upsert to employees table in databricks
EMPLOYEE_DEPARTMENT_TABLE = "tafw.tafw_records.employees"

if IN_DATABRICKS:
    # `spark` is injected by Databricks, same as `dbutils` above - never
    # defined locally, so these are expected "undefined name" warnings.
    # Built from the raw dicts (plain int/None), not the pandas Int64 column -
    # Spark's Arrow-based conversion doesn't reliably support pandas' nullable
    # extension dtypes, and an explicit schema avoids relying on inference.
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("XRefCode", StringType(), False),
            StructField("DepartmentXRefCode", StringType(), True),
            StructField("ProjectId", IntegerType(), True),
            StructField("TaskId", IntegerType(), True),
        ]
    )
    source = spark.createDataFrame(employee_department_rows, schema=schema)  # noqa: F821
    source.createOrReplaceTempView("_employee_department_updates")

    merge_sql = MERGE_EMPLOYEE_DEPARTMENT_SQL.format(
        table=EMPLOYEE_DEPARTMENT_TABLE, source="_employee_department_updates"
    )
    spark.sql(merge_sql)  # noqa: F821
    spark.catalog.dropTempView("_employee_department_updates")  # noqa: F821
    print(f"Merged {len(employee_department_rows)} row(s) into {EMPLOYEE_DEPARTMENT_TABLE}")
else:
    # No Databricks/Spark session available locally - skip the upload but show
    # what would have been merged so the rest of the script stays runnable.
    print(
        f"Skipping Databricks upload (not running in Databricks) - "
        f"would merge {len(employee_department_rows)} row(s) into {EMPLOYEE_DEPARTMENT_TABLE}:"
    )
    print(employee_department_df)

############################# TAFW #############################
# Step 4. Get TAFW (Time Away From Work) requests for these employees.
# Only employees with a resolved ProjectId (employee_department_df, already
# filtered in Step 2) are in scope - a TAFW record for an employee with no
# project/task mapping can't be attributed anywhere downstream.
tafw_xrefs = employee_department_df["XRefCode"].tolist()

#### Testing select a subset
tafw_xrefs = tafw_xrefs[0:3]

# Rolling window: one month back to three months ahead of today, split into
# 30-day chunks (a caller preference, not an API requirement - see
# tafw_ingest.get_tafw's module docstring).
tafw_now = datetime.now(UTC)
TAFW_START_DATE = tafw_now - timedelta(days=30)
TAFW_END_DATE = tafw_now + timedelta(days=90)
TAFW_CHUNK_DAYS = 30

print(
    f"Getting {list(DEFAULT_STATUSES)} TAFW records for {len(tafw_xrefs)} employee(s), "
    f"{TAFW_START_DATE.date()} .. {TAFW_END_DATE.date()}"
)
tafw_records = fetch_tafw_records(
    client,
    tafw_xrefs,
    TAFW_START_DATE,
    TAFW_END_DATE,
    statuses=DEFAULT_STATUSES,
    chunk_days=TAFW_CHUNK_DAYS,
)

# One row per weekday off, APPROVED + CANCELED combined, deduped by RecordHash.
tafw_day_df = expand_tafw_records_to_days(tafw_records)
print(f"Expanded to {len(tafw_day_df)} weekday-off row(s).")
print(tafw_day_df)

#Step 5. Upsert to tafw_day_records table in databricks
TAFW_DAY_RECORD_TABLE = "tafw.tafw_records.tafw_day_records"

if IN_DATABRICKS:
    # `spark` is injected by Databricks, same as `dbutils` above - never
    # defined locally, so these are expected "undefined name" warnings.
    # Built from the raw dicts (plain str/float/date), not the pandas
    # DataFrame directly - explicit schema avoids relying on inference, same
    # reasoning as the employee upsert in Step 3.
    from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

    tafw_schema = StructType(
        [
            StructField("XRefCode", StringType(), False),
            StructField("Date", DateType(), False),
            StructField("Hours", DecimalType(6, 2), False),
            StructField("ReasonName", StringType(), True),
            StructField("PayAdjShortName", StringType(), True),
            StructField("Status", StringType(), False),
            StructField("RecordHash", StringType(), False),
        ]
    )
    tafw_source = spark.createDataFrame(  # noqa: F821
        tafw_day_df.to_dict("records"), schema=tafw_schema
    )
    tafw_source.createOrReplaceTempView("_tafw_day_record_updates")

    tafw_merge_sql = MERGE_TAFW_DAY_RECORD_SQL.format(
        table=TAFW_DAY_RECORD_TABLE, source="_tafw_day_record_updates"
    )
    spark.sql(tafw_merge_sql)  # noqa: F821
    spark.catalog.dropTempView("_tafw_day_record_updates")  # noqa: F821
    print(f"Merged {len(tafw_day_df)} row(s) into {TAFW_DAY_RECORD_TABLE}")
else:
    # No Databricks/Spark session available locally - skip the upload but show
    # what would have been merged so the rest of the script stays runnable.
    print(
        f"Skipping Databricks upload (not running in Databricks) - "
        f"would merge {len(tafw_day_df)} row(s) into {TAFW_DAY_RECORD_TABLE}:"
    )
    print(tafw_day_df)
