"""SQL for the TAFW day-record staging table (Step 4's ``tafw_day_df``).

Kept separate from the notebook (rather than inline f-strings) so the queries
can be read, reviewed, and changed in one place without editing notebook
cells. Mirrors the ``{table}``-templated style of
:data:`tafw_ingest.employee_department_queries.CREATE_EMPLOYEE_DEPARTMENT_TABLE_SQL`.

Column names/types match :func:`tafw_ingest.get_tafw.expand_tafw_records_to_days`'s
output frame exactly:

    XRefCode         STRING        - Dayforce employee XRefCode
    Date             DATE          - the weekday off (one row per weekday, not per request)
    Hours            DECIMAL(6,2)  - hours off that day (currently always 8.00)
    ReasonName       STRING, null  - Dayforce's TAFW reason
    PayAdjShortName  STRING, null  - Dayforce's pay-adjustment short name
    Status           STRING        - "Approved" or "Canceled" (title-cased)
    RecordHash       STRING        - 16-char truncated SHA-256 hex digest
                                     day-record identity (tafw_ingest.hashing
                                     .record_hash); unique per row after
                                     expand_tafw_records_to_days's own
                                     drop_duplicates, and the MERGE match key
                                     here.

This is a distinct, simpler table from :data:`tafw_ingest.staging.STAGING_DDL`,
which backs the separate reconcile/active-tracking pipeline
(:mod:`tafw_ingest.pipeline`) with its own soft-delete history. Don't conflate
the two unless/until Step 4 is migrated onto that pipeline.

Typical use (see ``notebooks/dayforce_integration_service.py``)::

    spark.sql(CREATE_TAFW_DAY_RECORD_TABLE_SQL.format(table=TAFW_DAY_RECORD_TABLE))
    spark.sql(
        MERGE_TAFW_DAY_RECORD_SQL.format(
            table=TAFW_DAY_RECORD_TABLE, source="_tafw_day_record_updates"
        )
    )
"""

from __future__ import annotations

__all__ = [
    "CREATE_TAFW_DAY_RECORD_TABLE_SQL",
    "MERGE_TAFW_DAY_RECORD_SQL",
]

#: ``{table}`` is filled in by the caller.
CREATE_TAFW_DAY_RECORD_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    XRefCode        STRING       NOT NULL,
    Date            DATE         NOT NULL,
    Hours           DECIMAL(6,2) NOT NULL,
    ReasonName      STRING,
    PayAdjShortName STRING,
    Status          STRING       NOT NULL,
    RecordHash      STRING       NOT NULL
) USING DELTA
"""

#: ``{table}`` is the target Delta table; ``{source}`` is the temp view holding
#: the batch of expanded TAFW day-rows to upsert. Matched on ``RecordHash`` -
#: the deterministic per-day identity hash - not ``XRefCode`` + ``Date``,
#: since one employee can have multiple day-records on different dates and
#: the hash is what stays stable across re-fetches of the same request.
MERGE_TAFW_DAY_RECORD_SQL = """
MERGE INTO {table} AS t
USING {source} AS s
ON t.RecordHash = s.RecordHash
WHEN MATCHED THEN UPDATE SET
    t.XRefCode = s.XRefCode,
    t.Date = s.Date,
    t.Hours = s.Hours,
    t.ReasonName = s.ReasonName,
    t.PayAdjShortName = s.PayAdjShortName,
    t.Status = s.Status
WHEN NOT MATCHED THEN INSERT *
"""
