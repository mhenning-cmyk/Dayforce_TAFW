# Databricks notebook source
# MAGIC %md
# MAGIC # Dayforce TAFW → Databricks Staging Ingestion
# MAGIC
# MAGIC Runs on a schedule as a Databricks **Job** (this notebook is the task).
# MAGIC All logic lives in the `tafw_ingest` package; this notebook only wires
# MAGIC widgets + secrets into `run_cycle` and reports the result.
# MAGIC
# MAGIC | Step | Module |
# MAGIC |---|---|
# MAGIC | fetch (paced, retried) | `tafw_ingest.dayforce_client` |
# MAGIC | normalize → day-records | `tafw_ingest.normalize` |
# MAGIC | deterministic hash | `tafw_ingest.hashing` |
# MAGIC | new / changed / cancelled | `tafw_ingest.reconcile` |
# MAGIC | MERGE into staging Delta | `tafw_ingest.staging.DatabricksStagingWriter` |

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Install the package
# MAGIC Point this at the wheel built + uploaded by the Asset Bundle, or use a
# MAGIC `%pip install -e` against a Repos checkout while developing.

# COMMAND ----------
# MAGIC %pip install -r ../requirements.txt
# MAGIC %pip install -e ..
# dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Job parameters

# COMMAND ----------
dbutils.widgets.text("run_mode", "incremental")
dbutils.widgets.text("lookback_days", "30")
dbutils.widgets.text("horizon_days", "90")
dbutils.widgets.text("statuses", "APPROVED")
dbutils.widgets.text("employee_xrefs", "")           # empty = full roster
dbutils.widgets.text("staging_table", "main.staging.tafw_day_record")
dbutils.widgets.text("max_rpm", "90")
dbutils.widgets.text("expand_multi_day", "false")
dbutils.widgets.text("test_mode", "false")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Build settings (widgets + secret scope `dayforce`) and run

# COMMAND ----------
import logging

from tafw_ingest.config import Settings
from tafw_ingest.pipeline import run_cycle

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

settings = Settings.from_databricks(dbutils, secret_scope="dayforce")
result = run_cycle(settings, spark=spark)

print(result.summary())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Quick look at what changed this cycle

# COMMAND ----------
display(
    spark.sql(
        f"""
        SELECT is_active, count(*) AS rows, max(last_seen_at) AS last_seen
        FROM {settings.staging_table}
        GROUP BY is_active ORDER BY is_active DESC
        """
    )
)

# COMMAND ----------
# Surface metrics to the Job run UI / downstream tasks.
dbutils.notebook.exit(result.to_json())
