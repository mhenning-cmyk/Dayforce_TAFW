"""SQL for the employee -> department/project/task staging table.

Kept separate from the notebook (rather than inline f-strings) so the queries
can be read, reviewed, and changed in one place without editing notebook
cells. Mirrors the ``{table}``-templated style of
:data:`tafw_ingest.staging.STAGING_DDL` / its ``_MERGE_SQL``.

Typical use (see ``notebooks/dayforce_integration_service.py``)::

    spark.sql(CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL.format(table=EMPLOYEE_DEPARTMENT_TABLE))
    spark.sql(
        MERGE_EMPLOYEE_DEPARTMENT_SQL.format(
            table=EMPLOYEE_DEPARTMENT_TABLE, source="_employee_department_updates"
        )
    )
"""

from __future__ import annotations

__all__ = [
    "CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL",
    "MERGE_EMPLOYEE_DEPARTMENT_SQL",
]

#: ``{table}`` is filled in by the caller.
CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    XRefCode           STRING NOT NULL,
    DepartmentXRefCode STRING,
    ProjectId          INT,
    TaskId             INT
) USING DELTA
"""

#: ``{table}`` is the target Delta table; ``{source}`` is the temp view holding
#: the batch of employee/department/project/task rows to upsert. One row per
#: employee - matched on ``XRefCode``.
MERGE_EMPLOYEE_DEPARTMENT_SQL = """
MERGE INTO {table} AS t
USING {source} AS s
ON t.XRefCode = s.XRefCode
WHEN MATCHED THEN UPDATE SET
    t.DepartmentXRefCode = s.DepartmentXRefCode,
    t.ProjectId = s.ProjectId,
    t.TaskId = s.TaskId
WHEN NOT MATCHED THEN INSERT *
"""
