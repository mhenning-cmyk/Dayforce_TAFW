# Databricks notebook source
# MAGIC %md
# MAGIC # Dayforce smoke test
# MAGIC Runs `tests/test.py` from this Repos checkout, pulling Dayforce
# MAGIC credentials from the `dayforce` secret scope instead of the local
# MAGIC OS keyring that `Settings.from_env()` uses for local dev.

# COMMAND ----------
REPO_ROOT = "/Workspace/Users/mhenning@modelpath.net/Dayforce_TAFW"

# COMMAND ----------
# MAGIC %pip install -r {REPO_ROOT}/requirements.txt
# MAGIC %pip install -e {REPO_ROOT}

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import os
import subprocess
import sys

REPO_ROOT = "/Workspace/Users/mhenning@modelpath.net/Dayforce_TAFW"

os.environ["DAYFORCE_USERNAME"] = dbutils.secrets.get("dayforce", "username")
os.environ["DAYFORCE_PASSWORD"] = dbutils.secrets.get("dayforce", "password")

result = subprocess.run(
    [sys.executable, "tests/test.py"],
    cwd=REPO_ROOT,
    env=os.environ,
)
print(f"exit code: {result.returncode}")
