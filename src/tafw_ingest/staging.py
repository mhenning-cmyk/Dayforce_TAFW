"""Where reconciled TAFW day-records land: the staging system of record.

One :class:`StagingWriter` interface, three implementations:

===================  ==========================  ================================
Implementation       When                         Backing store
===================  ==========================  ================================
InMemoryStagingWriter unit tests / dry runs        a dict
LocalDeltaStagingWriter laptop integration tests   local Delta table (pyspark)
DatabricksStagingWriter the notebook, on a cluster  Unity Catalog Delta + MERGE
===================  ==========================  ================================

Everything upstream (fetch -> normalize -> hash -> reconcile) is framework-free
and fully testable; only :class:`DatabricksStagingWriter` needs a live Spark
session, and it is a thin wrapper over one ``MERGE INTO`` statement.
"""

from __future__ import annotations

import datetime as _dt
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from tafw_ingest.models import DayRecord
from tafw_ingest.reconcile import StoredRecord

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

__all__ = [
    "MergeStats",
    "StagingWriter",
    "InMemoryStagingWriter",
    "LocalDeltaStagingWriter",
    "DatabricksStagingWriter",
    "STAGING_DDL",
]

# Column contract shared by every writer. ``{table}`` is filled in per writer.
STAGING_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    record_hash         STRING       NOT NULL,
    employee_xref       STRING       NOT NULL,
    pto_date            DATE         NOT NULL,
    approval_type_code  STRING       NOT NULL,
    hours_requested     DECIMAL(6,2) NOT NULL,
    reason_name         STRING,
    status              STRING,
    time_start          STRING,
    time_end            STRING,
    all_day             BOOLEAN,
    date_of_request     STRING,
    source_payload      STRING,
    first_seen_at       TIMESTAMP    NOT NULL,
    last_seen_at        TIMESTAMP    NOT NULL,
    is_active           BOOLEAN      NOT NULL,
    deleted_at          TIMESTAMP,
    ingest_run_id       STRING       NOT NULL
) USING DELTA
"""


@dataclass
class MergeStats:
    inserted: int = 0
    updated: int = 0
    reactivated: int = 0
    touched: int = 0
    cancelled: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "inserted": self.inserted,
            "updated": self.updated,
            "reactivated": self.reactivated,
            "touched": self.touched,
            "cancelled": self.cancelled,
        }


class StagingWriter(Protocol):
    """Contract the pipeline depends on."""

    def load_active_records(self) -> dict[str, StoredRecord]:
        """Return ``{record_hash: StoredRecord}`` for every ``is_active`` row."""

    def apply(
        self,
        *,
        upserts: Iterable[DayRecord],
        seen_hashes: Iterable[str],
        cancelled_hashes: Iterable[str],
        run_id: str,
        now: _dt.datetime | None = None,
    ) -> MergeStats:
        """Insert/reactivate ``upserts``, bump ``last_seen_at`` for ``seen_hashes``,
        soft-delete ``cancelled_hashes``. Idempotent for a given cycle."""


# --------------------------------------------------------------------------- #
# In-memory                                                                   #
# --------------------------------------------------------------------------- #
class InMemoryStagingWriter:
    """Dict-backed writer. The default for local dev and unit tests."""

    def __init__(self, seed: Mapping[str, dict[str, Any]] | None = None) -> None:
        self.rows: dict[str, dict[str, Any]] = {
            k: dict(v) for k, v in (seed or {}).items()
        }

    def load_active_records(self) -> dict[str, StoredRecord]:
        return {
            h: StoredRecord(h, row["employee_xref"], row["pto_date"])
            for h, row in self.rows.items()
            if row.get("is_active", True)
        }

    def apply(
        self,
        *,
        upserts: Iterable[DayRecord],
        seen_hashes: Iterable[str],
        cancelled_hashes: Iterable[str],
        run_id: str,
        now: _dt.datetime | None = None,
    ) -> MergeStats:
        now = (now or _dt.datetime.now(_dt.UTC)).replace(microsecond=0)
        stats = MergeStats()

        for record in upserts:
            row = record.to_staging_row(run_id=run_id, now=now)
            existing = self.rows.get(record.record_hash)
            if existing is None:
                self.rows[record.record_hash] = row
                stats.inserted += 1
            else:
                if not existing.get("is_active", True):
                    stats.reactivated += 1
                else:
                    stats.updated += 1
                row["first_seen_at"] = existing.get("first_seen_at", row["first_seen_at"])
                self.rows[record.record_hash] = row

        for h in set(seen_hashes):
            stored_row = self.rows.get(h)
            if stored_row is None or not stored_row.get("is_active", True):
                continue
            if stored_row.get("last_seen_at") != now:
                stored_row["last_seen_at"] = now
                stats.touched += 1

        for h in set(cancelled_hashes):
            stored_row = self.rows.get(h)
            if stored_row is not None and stored_row.get("is_active", True):
                stored_row["is_active"] = False
                stored_row["deleted_at"] = now
                stats.cancelled += 1

        return stats


# --------------------------------------------------------------------------- #
# Spark-backed writers                                                        #
# --------------------------------------------------------------------------- #
_MERGE_SQL = """
MERGE INTO {table} AS t
USING {source} AS s
ON t.record_hash = s.record_hash
WHEN MATCHED THEN UPDATE SET
    t.employee_xref = s.employee_xref,
    t.pto_date = s.pto_date,
    t.approval_type_code = s.approval_type_code,
    t.hours_requested = s.hours_requested,
    t.reason_name = s.reason_name,
    t.status = s.status,
    t.time_start = s.time_start,
    t.time_end = s.time_end,
    t.all_day = s.all_day,
    t.date_of_request = s.date_of_request,
    t.source_payload = s.source_payload,
    t.last_seen_at = s.last_seen_at,
    t.is_active = true,
    t.deleted_at = NULL,
    t.ingest_run_id = s.ingest_run_id
WHEN NOT MATCHED THEN INSERT *
"""

# Column order for the Spark upsert frame - matches STAGING_DDL and the keys of
# DayRecord.to_staging_row() exactly.
_STAGING_COLUMNS: tuple[str, ...] = (
    "record_hash",
    "employee_xref",
    "pto_date",
    "approval_type_code",
    "hours_requested",
    "reason_name",
    "status",
    "time_start",
    "time_end",
    "all_day",
    "date_of_request",
    "source_payload",
    "first_seen_at",
    "last_seen_at",
    "is_active",
    "deleted_at",
    "ingest_run_id",
)


def _staging_spark_schema() -> Any:
    """Explicit Spark schema for the upsert frame (no reliance on type inference
    for Decimal precision / date vs timestamp / nullability)."""
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DecimalType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    s, ts = StringType(), TimestampType()
    return StructType(
        [
            StructField("record_hash", s, False),
            StructField("employee_xref", s, False),
            StructField("pto_date", DateType(), False),
            StructField("approval_type_code", s, False),
            StructField("hours_requested", DecimalType(6, 2), False),
            StructField("reason_name", s, True),
            StructField("status", s, True),
            StructField("time_start", s, True),
            StructField("time_end", s, True),
            StructField("all_day", BooleanType(), True),
            StructField("date_of_request", s, True),
            StructField("source_payload", s, True),
            StructField("first_seen_at", ts, False),
            StructField("last_seen_at", ts, False),
            StructField("is_active", BooleanType(), False),
            StructField("deleted_at", ts, True),
            StructField("ingest_run_id", s, False),
        ]
    )


class _SparkStagingWriterBase:
    """Shared MERGE / soft-delete logic for the two Spark-backed writers."""

    def __init__(self, spark: SparkSession, table: str) -> None:
        self.spark = spark
        self.table = table

    def ensure_table(self) -> None:
        self.spark.sql(STAGING_DDL.format(table=self.table))

    def load_active_records(self) -> dict[str, StoredRecord]:
        df = self.spark.sql(
            f"SELECT record_hash, employee_xref, pto_date "  # noqa: S608 - table from trusted config
            f"FROM {self.table} WHERE is_active = true"
        )
        return {
            row["record_hash"]: StoredRecord(
                row["record_hash"], row["employee_xref"], row["pto_date"]
            )
            for row in df.collect()
        }

    def apply(
        self,
        *,
        upserts: Iterable[DayRecord],
        seen_hashes: Iterable[str],
        cancelled_hashes: Iterable[str],
        run_id: str,
        now: _dt.datetime | None = None,
    ) -> MergeStats:
        now = (now or _dt.datetime.now(_dt.UTC)).replace(microsecond=0)
        self.ensure_table()
        stats = MergeStats()

        upsert_rows = [r.to_staging_row(run_id=run_id, now=now) for r in upserts]
        if upsert_rows:
            tuples = [tuple(row[c] for c in _STAGING_COLUMNS) for row in upsert_rows]
            source = self.spark.createDataFrame(tuples, schema=_staging_spark_schema())
            view = f"_tafw_upserts_{run_id}"
            source.createOrReplaceTempView(view)
            self.spark.sql(_MERGE_SQL.format(table=self.table, source=view))
            self.spark.catalog.dropTempView(view)
            stats.inserted = len(upsert_rows)  # MERGE does not split the count for us

        seen = sorted(set(seen_hashes) - {r["record_hash"] for r in upsert_rows})
        if seen:
            self._exec_in(
                f"UPDATE {self.table} SET last_seen_at = %(now)s "  # noqa: S608
                f"WHERE is_active = true AND record_hash IN (%(hashes)s)",
                now=now,
                hashes=seen,
            )
            stats.touched = len(seen)

        cancelled = sorted(set(cancelled_hashes))
        if cancelled:
            self._exec_in(
                f"UPDATE {self.table} SET is_active = false, deleted_at = %(now)s "  # noqa: S608
                f"WHERE is_active = true AND record_hash IN (%(hashes)s)",
                now=now,
                hashes=cancelled,
            )
            stats.cancelled = len(cancelled)

        return stats

    def _exec_in(self, template: str, *, now: _dt.datetime, hashes: list[str]) -> None:
        """Run an ``... IN (...)`` UPDATE, chunked to keep the statement small."""
        for i in range(0, len(hashes), 500):
            chunk = hashes[i : i + 500]
            in_list = ", ".join("'" + h.replace("'", "''") + "'" for h in chunk)
            sql = template.replace("%(hashes)s", in_list).replace(
                "%(now)s", "TIMESTAMP '" + now.strftime("%Y-%m-%d %H:%M:%S") + "'"
            )
            self.spark.sql(sql)


class DatabricksStagingWriter(_SparkStagingWriterBase):
    """Runs inside the Databricks notebook against a Unity Catalog Delta table."""


class LocalDeltaStagingWriter(_SparkStagingWriterBase):
    """Local Delta table for laptop integration tests (needs pyspark + delta-spark)."""

    @classmethod
    def create(cls, table: str, warehouse_path: str) -> LocalDeltaStagingWriter:
        try:
            from delta import configure_spark_with_delta_pip
            from pyspark.sql import SparkSession
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "LocalDeltaStagingWriter needs the 'localspark' extra: "
                "pip install -r requirements-dev.txt"
            ) from exc

        builder = (
            SparkSession.builder.appName("tafw-ingest-local")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", warehouse_path)
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return cls(spark, table)
