"""SQL for the employee -> department/project/task staging table.

Kept separate from the notebook (rather than inline f-strings) so the queries
can be read, reviewed, and changed in one place without editing notebook
cells. Mirrors the ``{table}``-templated style of
:data:`tafw_ingest.staging.STAGING_DDL` / its ``_MERGE_SQL``.

Typical use (see ``notebooks/dayforce_integration_service.py``)::

    spark.sql(CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL.format(table=EMPLOYEE_DEPARTMENT_TABLE))
    existing = spark.table(EMPLOYEE_DEPARTMENT_TABLE).columns
    for stmt in add_missing_employee_department_columns_sql(EMPLOYEE_DEPARTMENT_TABLE, existing):
        spark.sql(stmt)
    spark.sql(
        MERGE_EMPLOYEE_DEPARTMENT_SQL.format(
            table=EMPLOYEE_DEPARTMENT_TABLE, source="_employee_department_updates"
        )
    )
"""

from __future__ import annotations

from collections.abc import Iterable

__all__ = [
    "ADDED_EMPLOYEE_DEPARTMENT_COLUMNS",
    "CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL",
    "MERGE_EMPLOYEE_DEPARTMENT_SQL",
    "add_missing_employee_department_columns_sql",
]

#: ``{table}`` is filled in by the caller.
CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    XRefCode           STRING NOT NULL,
    employee_name      STRING,
    DepartmentXRefCode STRING,
    ProjectId          INT,
    TaskId             INT,
    employment_status  STRING,
    UpdatedDateTime    TIMESTAMP
) USING DELTA
"""

#: Columns added after the table was first created. ``CREATE TABLE IF NOT
#: EXISTS`` leaves an existing table's schema alone, so these are ALTERed in by
#: :func:`add_missing_employee_department_columns_sql`. Order matters - they are
#: appended in this order, matching the CREATE above.
ADDED_EMPLOYEE_DEPARTMENT_COLUMNS: dict[str, str] = {
    "employment_status": "STRING",
    "UpdatedDateTime": "TIMESTAMP",
}

#: ``{table}`` is the target Delta table; ``{source}`` is the temp view holding
#: the batch of employee/department/project/task/status rows to upsert. One row
#: per employee - matched on ``XRefCode``.
#:
#: Only new or changed employees are written: a matched row whose values are all
#: identical to the source (and already has an ``UpdatedDateTime``) is skipped
#: entirely, so a run where nothing changed in Dayforce touches no rows. ``<=>``
#: is null-safe equality, so NULL -> 'ACTIVE' counts as a change.
#:
#: ``UpdatedDateTime`` is the ingestion time, stamped by Databricks rather than the
#: source rows: set on insert, and on update ONLY when ``employment_status``
#: changed (or was never stamped - that's what backfills rows that predate the
#: column). A name/department/project/task change rewrites the row but keeps its
#: ``UpdatedDateTime``. ``current_timestamp()`` is evaluated once per query, so
#: every row stamped by one run gets the same value.
MERGE_EMPLOYEE_DEPARTMENT_SQL = """
MERGE INTO {table} AS t
USING {source} AS s
ON t.XRefCode = s.XRefCode
WHEN MATCHED AND (
    NOT (t.employee_name <=> s.employee_name)
    OR NOT (t.DepartmentXRefCode <=> s.DepartmentXRefCode)
    OR NOT (t.ProjectId <=> s.ProjectId)
    OR NOT (t.TaskId <=> s.TaskId)
    OR NOT (t.employment_status <=> s.employment_status)
    OR t.UpdatedDateTime IS NULL
) THEN UPDATE SET
    t.employee_name = s.employee_name,
    t.DepartmentXRefCode = s.DepartmentXRefCode,
    t.ProjectId = s.ProjectId,
    t.TaskId = s.TaskId,
    t.UpdatedDateTime = CASE
        WHEN t.employment_status <=> s.employment_status
             AND t.UpdatedDateTime IS NOT NULL THEN t.UpdatedDateTime
        ELSE current_timestamp()
    END,
    t.employment_status = s.employment_status
WHEN NOT MATCHED THEN INSERT (
    XRefCode, employee_name, DepartmentXRefCode, ProjectId, TaskId,
    employment_status, UpdatedDateTime
) VALUES (
    s.XRefCode, s.employee_name, s.DepartmentXRefCode, s.ProjectId, s.TaskId,
    s.employment_status, current_timestamp()
)
"""


def add_missing_employee_department_columns_sql(
    table: str, existing_columns: Iterable[str]
) -> list[str]:
    """``ALTER TABLE ... ADD COLUMNS`` for each added column ``table`` lacks.

    ``existing_columns`` is the table's current column list (e.g.
    ``spark.table(table).columns``). Returns ``[]`` once the table is up to date,
    so it's safe to run every time. Comparison is case-insensitive, as Delta's is.
    """
    have = {c.lower() for c in existing_columns}
    return [
        f"ALTER TABLE {table} ADD COLUMNS ({name} {sql_type})"
        for name, sql_type in ADDED_EMPLOYEE_DEPARTMENT_COLUMNS.items()
        if name.lower() not in have
    ]
