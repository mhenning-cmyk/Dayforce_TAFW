"""Orchestration: one ingestion cycle end to end.

    roster -> per-employee TAFW fetch -> normalize -> hash -> reconcile -> MERGE

:func:`run_cycle` is what the Databricks notebook calls. It is also runnable
from the CLI (``python -m tafw_ingest.pipeline`` / the ``tafw-ingest`` script)
for local dry runs against the in-memory or local-Delta writer.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from tafw_ingest.config import Settings
from tafw_ingest.dayforce_client import DayforceClient
from tafw_ingest.normalize import dedupe_by_hash, normalize_events
from tafw_ingest.reconcile import ReconcileResult, reconcile
from tafw_ingest.staging import (
    InMemoryStagingWriter,
    MergeStats,
    StagingWriter,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

__all__ = ["CycleResult", "run_cycle", "build_writer", "main"]


@dataclass
class CycleResult:
    run_id: str
    started_at: _dt.datetime
    finished_at: _dt.datetime
    employees: int
    fetched_entries: int
    day_records: int
    reconcile: ReconcileResult
    merge: MergeStats

    @property
    def duration_ms(self) -> int:
        return int((self.finished_at - self.started_at).total_seconds() * 1000)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "duration_ms": self.duration_ms,
            "employees": self.employees,
            "fetched_entries": self.fetched_entries,
            "day_records": self.day_records,
            "reconcile": self.reconcile.summary(),
            "merge": self.merge.as_dict(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)

    def summary(self) -> str:
        d = self.to_dict()
        r, m = d["reconcile"], d["merge"]
        return (
            f"run {self.run_id} in {d['duration_ms']} ms | "
            f"{d['employees']} employees, {d['fetched_entries']} entries -> "
            f"{d['day_records']} day-records | "
            f"new={r['new']} changed={r['changed']} unchanged={r['unchanged']} "
            f"cancelled={r['cancelled']} | "
            f"inserted={m['inserted']} touched={m['touched']} cancelled={m['cancelled']}"
        )


def build_writer(settings: Settings, spark: SparkSession | None = None) -> StagingWriter:
    """Pick a writer: explicit ``spark`` -> Databricks; else per ``settings.writer``."""
    if spark is not None:
        from tafw_ingest.staging import DatabricksStagingWriter

        return DatabricksStagingWriter(spark, settings.staging_table)
    if settings.writer == "databricks":
        raise RuntimeError("writer='databricks' requires a Spark session")
    if settings.writer == "local_delta":
        from tafw_ingest.staging import LocalDeltaStagingWriter

        return LocalDeltaStagingWriter.create(
            settings.staging_table, settings.local_delta_path
        )
    return InMemoryStagingWriter()


def run_cycle(
    settings: Settings,
    *,
    spark: SparkSession | None = None,
    df_client: DayforceClient | None = None,
    writer: StagingWriter | None = None,
    now: _dt.datetime | None = None,
) -> CycleResult:
    """Run one full reconcile pass and return its metrics."""
    started = now or _dt.datetime.now(_dt.UTC)
    run_id = uuid.uuid4().hex

    df_client = df_client or DayforceClient.from_settings(settings)
    writer = writer or build_writer(settings, spark)

    xrefs = settings.tafw_employee_xrefs or df_client.list_employee_xrefs()
    logger.info(
        "Cycle %s: %d employee(s), statuses=%s",
        run_id,
        len(xrefs),
        settings.tafw_statuses,
    )

    fetched = 0
    day_records = []
    for xref in xrefs:
        for status in settings.tafw_statuses:
            entries = df_client.get_tafw_records_for_employee_in_timeframe(xref, status)
            fetched += len(entries)
            day_records.extend(
                normalize_events(
                    xref,
                    entries,
                    status=status,
                    expand_multi_day=settings.tafw_expand_multi_day,
                )
            )

    current = dedupe_by_hash(day_records)
    previous_active = writer.load_active_records()
    result = reconcile(current, previous_active)

    merge = writer.apply(
        upserts=result.upserts,
        seen_hashes=[r.record_hash for r in current],
        cancelled_hashes=result.deactivations,
        run_id=run_id,
        now=started,
    )

    finished = _dt.datetime.now(_dt.UTC)
    cycle = CycleResult(
        run_id=run_id,
        started_at=started,
        finished_at=finished,
        employees=len(xrefs),
        fetched_entries=fetched,
        day_records=len(current),
        reconcile=result,
        merge=merge,
    )
    logger.info("Cycle complete: %s", cycle.summary())
    return cycle


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one Dayforce TAFW ingestion cycle.")
    parser.add_argument(
        "--config", default="conf/settings.dev.yaml", help="YAML defaults file"
    )
    parser.add_argument(
        "--run-mode", default="incremental", help="reserved; informational"
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    settings = Settings.from_env(config_path=args.config)
    missing = [
        name
        for name in ("dayforce_username", "dayforce_password")
        if not getattr(settings, name)
    ]
    if missing and not settings.tafw_employee_xrefs:
        logger.warning(
            "No Dayforce credentials set (%s) - only a mocked run will work.",
            ", ".join(missing),
        )

    result = run_cycle(settings)
    print(result.summary())
    print(result.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
