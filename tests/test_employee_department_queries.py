from __future__ import annotations

from tafw_ingest.employee_department_queries import (
    CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL,
    MERGE_EMPLOYEE_DEPARTMENT_SQL,
    add_missing_employee_department_columns_sql,
)


def test_create_table_sql_has_status_and_updated_date():
    sql = CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL.format(table="tafw.tafw_records.employees")
    assert "CREATE TABLE IF NOT EXISTS tafw.tafw_records.employees" in sql
    assert "employment_status  STRING" in sql
    assert "UpdatedDateTime    TIMESTAMP" in sql


def test_merge_sql_only_moves_updated_date_when_status_changes():
    sql = MERGE_EMPLOYEE_DEPARTMENT_SQL.format(table="t_tbl", source="s_view")
    assert "MERGE INTO t_tbl AS t" in sql
    assert "USING s_view AS s" in sql
    update = sql.split("THEN UPDATE SET", 1)[1].split("WHEN NOT MATCHED", 1)[0]
    assert "WHEN t.employment_status <=> s.employment_status" in update
    assert "AND t.UpdatedDateTime IS NOT NULL THEN t.UpdatedDateTime" in update
    assert "ELSE current_timestamp()" in update
    assert "t.employment_status = s.employment_status" in update


def test_merge_sql_skips_unchanged_rows():
    """WHEN MATCHED must be conditional - an unconditional update rewrites
    every existing employee on every run even when nothing changed."""
    sql = MERGE_EMPLOYEE_DEPARTMENT_SQL.format(table="t", source="s")
    assert "WHEN MATCHED THEN" not in sql
    condition = sql.split("WHEN MATCHED AND (", 1)[1].split(") THEN UPDATE SET", 1)[0]
    for column in (
        "employee_name",
        "DepartmentXRefCode",
        "ProjectId",
        "TaskId",
        "employment_status",
    ):
        assert f"NOT (t.{column} <=> s.{column})" in condition
    assert "t.UpdatedDateTime IS NULL" in condition


def test_merge_sql_stamps_updated_date_on_insert():
    sql = MERGE_EMPLOYEE_DEPARTMENT_SQL.format(table="t", source="s")
    insert = sql.split("WHEN NOT MATCHED THEN INSERT", 1)[1]
    assert "UpdatedDateTime" in insert
    assert "current_timestamp()" in insert
    assert "INSERT *" not in sql


def test_add_missing_columns_on_pre_existing_table():
    existing = ["XRefCode", "employee_name", "DepartmentXRefCode", "ProjectId", "TaskId"]
    assert add_missing_employee_department_columns_sql("tbl", existing) == [
        "ALTER TABLE tbl ADD COLUMNS (employment_status STRING)",
        "ALTER TABLE tbl ADD COLUMNS (UpdatedDateTime TIMESTAMP)",
    ]


def test_add_missing_columns_noop_when_up_to_date_case_insensitive():
    existing = [
        "XRefCode",
        "employee_name",
        "DepartmentXRefCode",
        "ProjectId",
        "TaskId",
        "Employment_Status",
        "updateddatetime",
    ]
    assert add_missing_employee_department_columns_sql("tbl", existing) == []
