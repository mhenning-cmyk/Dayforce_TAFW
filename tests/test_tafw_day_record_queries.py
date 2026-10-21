from __future__ import annotations

from tafw_ingest.tafw_day_record_queries import (
    CREATE_TAFW_DAY_RECORD_TABLE_SQL,
    MERGE_TAFW_DAY_RECORD_SQL,
    sql_string_list,
)


def test_sql_string_list_quotes_and_joins():
    assert sql_string_list(["H5JN095", "H5JN002"]) == "'H5JN095', 'H5JN002'"


def test_sql_string_list_escapes_embedded_quotes():
    assert sql_string_list(["O'Brien"]) == "'O''Brien'"


def test_sql_string_list_empty_renders_empty_string():
    # `AND t.XRefCode IN ()` is invalid SQL, but that only matters if a
    # caller passes an empty xref list into the MERGE - not this helper's job.
    assert sql_string_list([]) == ""


def test_create_table_sql_still_formats():
    sql = CREATE_TAFW_DAY_RECORD_TABLE_SQL.format(table="tafw.tafw_records.tafw_day_records")
    assert "CREATE TABLE IF NOT EXISTS tafw.tafw_records.tafw_day_records" in sql
    assert "RecordHash      STRING       NOT NULL" in sql


def test_merge_sql_matches_and_updates_on_record_hash():
    sql = MERGE_TAFW_DAY_RECORD_SQL.format(
        table="t",
        source="s",
        xref_list=sql_string_list(["H5JN095"]),
        window_start="2026-08-23",
        window_end="2026-12-21",
    )
    assert "ON t.RecordHash = s.RecordHash" in sql
    assert ") THEN UPDATE SET" in sql
    assert "t.Hours = s.Hours" in sql
    assert "INSERT *" not in sql


def test_merge_sql_cancels_stale_rows_scoped_to_run():
    """The bug this closes: an hours-only edit changes RecordHash, so the old
    row can never be re-matched and updated by the WHEN MATCHED branch - it
    must be flipped to Canceled instead (not deleted, so its history is kept),
    scoped to only the XRefCodes/date-window this run actually covers."""
    sql = MERGE_TAFW_DAY_RECORD_SQL.format(
        table="t",
        source="s",
        xref_list=sql_string_list(["H5JN095", "H5JN002"]),
        window_start="2026-08-23",
        window_end="2026-12-21",
    )
    assert "WHEN NOT MATCHED BY SOURCE" in sql
    assert "AND t.XRefCode IN ('H5JN095', 'H5JN002')" in sql
    assert "AND t.Date BETWEEN DATE '2026-08-23' AND DATE '2026-12-21'" in sql
    cancel = sql.split("WHEN NOT MATCHED BY SOURCE", 1)[1]
    assert "t.Status = 'Canceled'" in cancel
    assert "t.UpdatedDateTime = current_timestamp()" in cancel
    assert "THEN DELETE" not in sql


def _merge_sql() -> str:
    return MERGE_TAFW_DAY_RECORD_SQL.format(
        table="t",
        source="s",
        xref_list=sql_string_list(["H5JN095"]),
        window_start="2026-08-23",
        window_end="2026-12-21",
    )


def test_create_table_sql_has_updated_datetime():
    sql = CREATE_TAFW_DAY_RECORD_TABLE_SQL.format(table="x")
    assert "UpdatedDateTime TIMESTAMP" in sql


def test_merge_sql_stamps_updated_datetime_on_insert():
    insert = _merge_sql().split("WHEN NOT MATCHED THEN INSERT", 1)[1]
    insert = insert.split("WHEN NOT MATCHED BY SOURCE", 1)[0]
    assert "UpdatedDateTime" in insert
    assert "current_timestamp()" in insert


def test_merge_sql_skips_unchanged_matched_rows():
    """Unchanged requests must keep their original UpdatedDateTime, so the
    matched branch only fires when something mutable actually differs."""
    sql = _merge_sql()
    assert "WHEN MATCHED THEN" not in sql
    condition = sql.split("WHEN MATCHED AND (", 1)[1].split(") THEN UPDATE SET", 1)[0]
    for column in ("Status", "ReasonName", "PayAdjShortName"):
        assert f"NOT (t.{column} <=> s.{column})" in condition
    assert "t.UpdatedDateTime IS NULL" in condition
    update = sql.split(") THEN UPDATE SET", 1)[1].split("WHEN NOT MATCHED", 1)[0]
    assert "t.UpdatedDateTime = current_timestamp()" in update
