## Dayforce Integration Service ##

#Import python libaries
import sys
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

#Import custom python libraries
from tafw_ingest import employees
from tafw_ingest.config import Settings
from tafw_ingest.dayforce_client import DayforceClient
from tafw_ingest.department_map import DepartmentMap
from tafw_ingest.employee_department_queries import (
    CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL,
    MERGE_EMPLOYEE_DEPARTMENT_SQL,
)

if IN_DATABRICKS:
    # Databricks: no local repo file to rely on - secrets come from the
    # `dayforce` secret scope, everything else from job widgets (falls back
    # to Settings' own defaults for any widget that isn't declared).
    settings = Settings.from_databricks(dbutils, secret_scope="dayforce")  # noqa: F821
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

######## Testing Only: Filter for hte top 3 rows ###########
employees_df = employees_df.iloc[0:3]

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
print(employee_department_df)

#Step 3. Upsert to employees table in databricks
EMPLOYEE_DEPARTMENT_TABLE = "staging.employee_department_task"

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

    create_table_sql = CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL.format(
        table=EMPLOYEE_DEPARTMENT_TABLE
    )
    merge_sql = MERGE_EMPLOYEE_DEPARTMENT_SQL.format(
        table=EMPLOYEE_DEPARTMENT_TABLE, source="_employee_department_updates"
    )
    spark.sql(create_table_sql)  # noqa: F821
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
