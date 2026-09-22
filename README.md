# Dayforce TAFW → Databricks Ingestion Service

Local development guide for `dayforce_integration_service`, the Python service that
syncs **Time Away From Work (TAFW)** data from the Dayforce REST API into a
Databricks **staging Delta table**, which Celigo then reads to create / update /
delete PTO records in NetSuite.

This README covers **how to build and test the service locally before Databricks
environment access is available**, and how the workflow changes once it is.

> Looking for a plain-language explanation of what this project does, with no
> coding background required? See [HOW_IT_WORKS.md](HOW_IT_WORKS.md).

---

## 1. What this service does

```
Dayforce TAFW API                Ingestion service (this repo)              Databricks              Celigo → NetSuite
─────────────────                ────────────────────────────              ─────────              ────────────────
per-employee GET  ──▶  1. fetch      (paced ≤ ~100 req/min, ret/backoff)
                       2. normalize  raw JSON → flat day-records
                       3. hash       deterministic fingerprint per record
                       4. reconcile  compare to last cycle → new / changed / cancelled
                       5. write      MERGE into  staging.tafw_day_record  ──▶  Delta table  ──▶  Celigo reads
```

**Design rules that shape the code**

| Rule | Consequence for local dev |
|---|---|
| Runs as a **Databricks Job** (notebook task) on a schedule | Notebook is a *thin wrapper*; all logic lives in importable modules under `src/` so it can be unit-tested without Spark or Databricks. |
| Dayforce has **no bulk endpoint** — one call per employee | The client must handle pagination, pacing, and partial-failure retry. These are unit-testable with recorded fixtures. |
| Record identity is a **deterministic hash** of 4 fields | Hashing is pure Python — 100% testable offline, and the spec must be frozen (see §7). |
| Staging table is the **system of record** | The only part that truly needs Databricks is the final `MERGE`. It sits behind a `StagingWriter` interface with a local fake, so everything else runs on a laptop. |

---

## 2. Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | **3.11.x** | Matches Databricks Runtime 15.4 LTS. Already installed via `winget install Python.Python.3.11`; `.venv` is built from it. |
| Git Bash | any recent | Shell used for the commands below. |
| VS Code | latest | Plus the extensions in §4. |
| Java (Temurin) 17 | optional, Phase 1 | Only needed if you want to run **local Spark + Delta** instead of the in-memory fake writer. `winget install EclipseAdoptium.Temurin.17.JDK` |
| Databricks CLI | latest | **Phase 2 only** (needs workspace access). `winget install Databricks.DatabricksCLI` |

---

## 3. Repository layout

Everything below the divider now exists (scaffolded, tests green). `databricks.yml`
is the one Phase-2 addition still to come.

```
Dayforce_TAFW/
├── README.md                       ← this file
├── pyproject.toml                  ← package metadata + pytest/ruff/mypy config
├── requirements.txt                ← runtime deps (already created)
├── requirements-dev.txt            ← `-e .` + local Spark/Delta + notebook kernel
├── .env.example                    ← template for local secrets (copy to .env, never commit .env)
├── .gitignore
├── databricks.yml                  ← Asset Bundle definition (Phase 2 — not yet created)
├── conf/
│   └── settings.dev.yaml           ← non-secret local config (page size, base URL, table name)
├── notebooks/
│   └── dayforce_integration_service.py   ← Databricks notebook *source format* (see §6)
├── src/
│   └── tafw_ingest/
│       ├── __init__.py
│       ├── config.py               ← pydantic-settings: YAML + keyring + env + widgets
│       ├── dayforce_client.py      ← auth, pagination (Paging.Next), rate limiting, retry
│       ├── employees.py            ← paginate GET /Employees → pandas DataFrame
│       ├── models.py               ← pydantic models for TAFW day-records
│       ├── normalize.py            ← raw API JSON → list[DayRecord]
│       ├── hashing.py              ← deterministic record hash (frozen spec)
│       ├── reconcile.py            ← diff current vs. previous cycle (new/changed/cancelled/superseded)
│       ├── staging.py              ← StagingWriter: InMemory / LocalDelta / Databricks
│       └── pipeline.py             ← run_cycle(): orchestrates 1–5, called by the notebook + CLI
└── tests/
    ├── conftest.py
    ├── test.py                     ← MANUAL live smoke test (auth + roster); pytest skips it
    ├── fixtures/dayforce/          ← recorded API responses (sanitized, no real PII)
    ├── test_hashing.py
    ├── test_normalize.py
    ├── test_reconcile.py
    ├── test_employees.py
    ├── test_dayforce_client.py
    └── test_pipeline.py
```

> The file currently in the repo is `dayforce_integration_service.ipyb` (empty, and the
> extension is a typo). Recommended: delete it and use
> `notebooks/dayforce_integration_service.py` in **Databricks notebook source format**
> — it is plain text, diffs cleanly in git, imports `src/tafw_ingest`, and both the
> Databricks extension and `databricks bundle` treat it as a notebook.

---

## 4. VS Code extensions

| Extension | ID | Purpose |
|---|---|---|
| Databricks | `databricks.databricks` | Phase 2: attach cluster, "Run file on Databricks", bundle deploy. Harmless to install now. |
| Python | `ms-python.python` | Interpreter + test runner. |
| Pylance | `ms-python.vscode-pylance` | Types / completions. |
| Jupyter | `ms-toolsai.jupyter` | Run notebook cells locally against the `.venv` kernel. |
| Ruff | `charliermarsh.ruff` | Lint + format (matches `ruff` pin in requirements). |
| Data Wrangler | `ms-toolsai.datawrangler` | Inspect DataFrames while developing `normalize` / `reconcile`. |
| YAML | `redhat.vscode-yaml` | Editing `databricks.yml` / `conf/*.yaml`. |

Install from the terminal:

```bash
for ext in databricks.databricks ms-python.python ms-python.vscode-pylance \
           ms-toolsai.jupyter charliermarsh.ruff ms-toolsai.datawrangler redhat.vscode-yaml; do
  code --install-extension "$ext"
done
```

---

## 5. One-time local setup

```bash
cd "C:/Users/mhenn/OneDrive/Desktop/Dayforce_TAFW"

# 1. Virtual environment (already done — recreate only if needed)
py -3.11 -m venv .venv
source .venv/Scripts/activate          # Git Bash on Windows: Scripts/, not bin/
python --version                        # -> 3.11.9

# 2. Runtime + dev dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt     # local pyspark + delta-spark + ipykernel (see §8)

# 3. Register the venv as a Jupyter kernel for VS Code / notebooks
python -m ipykernel install --user --name tafw-ingest --display-name "Python 3.11 (tafw-ingest)"

# 4. Local secrets
cp .env.example .env                    # then fill in Dayforce dev credentials

# 5. VS Code: Ctrl+Shift+P → "Python: Select Interpreter" → .venv\Scripts\python.exe
```

`.gitignore` must contain at least:

```
.venv/
.env
*.pyc
__pycache__/
.pytest_cache/
.databricks/
spark-warehouse/
metastore_db/
```

### Notebook-friendly git config (run once per clone)

`notebooks/dayforce_integration_service.ipynb` gets edited from two places -
here, and live inside Databricks (running cells, and occasionally real code
edits) - so plain git, which diffs `.ipynb` as raw JSON text, turns "someone
just ran it" or "someone edited a different cell" into a merge conflict on
the whole file almost every time. Two things fix that, and both need a
one-time local registration (`.gitattributes` declares the mapping and is
already committed, but the actual filter/driver *commands* they point at are
deliberately not something a repo can auto-install for you - that's a git
security boundary, not an oversight):

```bash
# 1. Notebook-aware diff/merge (compares cell-by-cell instead of raw JSON lines)
pip install -r requirements-dev.txt   # includes nbdime
python -m nbdime config-git --enable

# 2. Strip Databricks' execution_count/outputs before every commit, so
#    re-running a notebook with no real changes produces no diff at all
git config filter.strip-notebook-output.clean "python scripts/strip_notebook_output.py"
git config filter.strip-notebook-output.smudge cat
git config filter.strip-notebook-output.required false
```

This doesn't replace pulling before you start editing in either place - it
just means the conflicts that do happen are real (both sides changed the
same cell) rather than noise, and nbdime resolves the non-overlapping ones
automatically instead of handing you a raw JSON conflict to untangle by hand.

---

## 6. Notebook source format

> **Note:** this section describes the original plan. In practice the actual
> notebook (`notebooks/dayforce_integration_service.ipynb`) is authored as a
> real `.ipynb` file, not a `.py` with magic comments, so it can be edited
> and run directly in Databricks' notebook UI. See §5 above for how to keep
> that from causing constant merge pain, and [HOW_IT_WORKS.md](HOW_IT_WORKS.md)
> for what it actually does.

Author the notebook as a `.py` file so it version-controls cleanly. Databricks
recognises these magic comments:

```python
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Dayforce TAFW Ingestion — runs on a schedule as a Databricks Job

# COMMAND ----------
dbutils.widgets.text("run_mode", "incremental")     # job parameter
dbutils.widgets.text("lookback_days", "90")

# COMMAND ----------
from tafw_ingest.pipeline import run_cycle
from tafw_ingest.config import Settings

settings = Settings.from_databricks(dbutils, spark)   # pulls secrets from a secret scope
result = run_cycle(settings, spark)
print(result.summary())                               # counts: fetched / new / changed / cancelled

# COMMAND ----------
# MAGIC %md Exit with metrics so the Job UI shows them
dbutils.notebook.exit(result.to_json())
```

Keep cells thin. Anything worth testing goes in `src/tafw_ingest/` and is imported.

- **Run locally in VS Code:** the Jupyter extension executes `# COMMAND ----------`
  cells as regular cells using the `tafw-ingest` kernel. `spark` / `dbutils` are
  **not** defined locally — the pipeline is designed so `run_cycle` accepts them as
  arguments, and Phase 1 passes fakes (see §8).
- **Run on a cluster (Phase 2):** Databricks extension → "Run File on Databricks".

---

## 7. Deterministic hash — frozen spec

Per the architecture doc, every TAFW day-record's identity is:

```
hash = sha256( "{employee_xref}|{pto_date}|{approval_type_code}|{hours_requested}" ).hexdigest()
```

Normalization rules (must be applied *before* hashing, and never changed once data
is in staging):

| Field | Source | Normalization |
|---|---|---|
| `employee_xref` | `EmployeeXRefCode` | trim, uppercase |
| `pto_date` | PTO day | ISO `YYYY-MM-DD`, no time component |
| `approval_type_code` | approval / type code | trim, uppercase |
| `hours_requested` | hours for that day | fixed-point string, 2 decimals (`"8.00"`) |

- If any of the 4 values changes → hash changes → treated as an **update**.
- A hash present last cycle but absent this cycle → record was **cancelled/denied**
  → soft-deleted in staging (`is_active = false`, `deleted_at = now`).
- `test_hashing.py` locks the exact byte string and expected digests with golden
  values so the spec cannot drift accidentally.

---

## 8. Local development strategy (Phase 1 — no Databricks yet)

The goal: exercise steps **1–4 fully**, and step **5** against a local stand-in.

### `StagingWriter` abstraction (`src/tafw_ingest/staging.py`)

```python
class StagingWriter(Protocol):
    def load_previous_hashes(self) -> dict[str, str]: ...
    def merge(self, upserts: list[DayRecord], cancellations: list[str]) -> MergeStats: ...
```

| Implementation | When | Backing store |
|---|---|---|
| `InMemoryStagingWriter` | unit tests | a dict |
| `LocalDeltaStagingWriter` | integration tests on a laptop | local Delta table via `pyspark` + `delta-spark` (needs Java 17) |
| `DatabricksStagingWriter` | Phase 2, in the notebook | `spark` session → `MERGE INTO staging.tafw_day_record` |

`run_cycle(settings, spark=None, writer=None)` picks `DatabricksStagingWriter` when a
real `spark` is passed, otherwise uses the injected `writer`.

### Mocking the Dayforce API

- Sanitized sample responses live under `tests/fixtures/dayforce/` (roster +
  single/multi-day approved TAFW). Add more (empty result, 429) as needed.
- `test_dayforce_client.py` stubs HTTP with `requests-mock` — including the
  cross-host redirect and the window fan-out.
- `test_pipeline.py` runs the whole cycle against a `FakeDayforceClient` + the
  in-memory writer, across two cycles (change + cancellation + idempotency).
- Optional next step: a tiny `Flask` fake that replays fixtures, so the notebook
  can run end-to-end locally by pointing `DAYFORCE_BASE_URI` at `http://localhost:8080`.

### Dependencies for local dev

[requirements-dev.txt](requirements-dev.txt) already exists: `-e .` (editable install),
`ipykernel`, and `pyspark==3.5.3` / `delta-spark==3.2.0` for `LocalDeltaStagingWriter`.
`databricks-connect` is deliberately **absent** — it conflicts with `pyspark` and needs
live workspace auth; in Phase 2 it replaces the two Spark pins.

### Status of the offline build (Phase 1)

- [x] `dayforce_client` — redirect-safe Basic auth, urllib3 retry, `pyrate_limiter` pacing, window fan-out
- [x] `models` + `normalize` — raw JSON → `DayRecord` (incl. `DayList` per-day expansion + even-split)
- [x] `hashing` — frozen 4-field SHA-256 spec + golden-value tests
- [x] `reconcile` — new / changed / unchanged / cancelled / **superseded** diff
- [x] `pipeline.run_cycle` — orchestration + CLI (`python -m tafw_ingest.pipeline`)
- [x] `staging` — `InMemory` (tested) + `LocalDelta` / `Databricks` (coded, share one MERGE path)
- [x] 26 `pytest` green, `ruff` clean, `mypy` clean
- [ ] Exercise `LocalDeltaStagingWriter` against a real local Delta table (needs JDK 17)
- [ ] Confirm real Dayforce TAFW payload shape → adjust `normalize._DAY_LIST_KEYS` etc.
- [ ] Phase 2: secret scope, cluster/runtime pin, Job schedule, `databricks.yml` bundle

---

## 9. Running things

```bash
source .venv/Scripts/activate

# Unit + integration tests
pytest -q
pytest -q --cov=tafw_ingest --cov-report=term-missing

# Lint / format / types
ruff check .
ruff format .
mypy src

# Run one ingestion cycle from the CLI (uses conf/settings.dev.yaml + .env).
# With real creds it hits Dayforce; set TAFW_EMPLOYEE_XREFS to a couple of codes first.
python -m tafw_ingest.pipeline --config conf/settings.dev.yaml --log-level DEBUG

# Run the notebook locally: open notebooks/dayforce_integration_service.py in VS Code,
# select the "Python 3.11 (tafw-ingest)" kernel, Run All.
```

---

## 10. Configuration & secrets

| Setting | Local (Phase 1) | Databricks (Phase 2) |
|---|---|---|
| Dayforce base URL / company / version, windows, table name | `conf/settings.dev.yaml` (committed, non-secret) | notebook widgets / job params |
| Dayforce username + password | **OS keyring** service `dayforce-tafw` (or `.env` to override) | **secret scope** `dayforce`, via `dbutils.secrets.get` |
| Target table | `staging.tafw_day_record` (local Delta path in dev) | Unity Catalog `catalog.staging.tafw_day_record` |

`config.py` exposes one `Settings` model with two constructors: `Settings.from_env(config_path=...)`
(YAML defaults → **keyring** for creds → env / `.env` → kwargs) and
`Settings.from_databricks(dbutils, secret_scope="dayforce")` (notebook widgets + secret
scope). Nothing else reads env vars or `dbutils` directly — pass a `Settings` instance in.
List-valued vars (`TAFW_STATUSES`, `TAFW_EMPLOYEE_XREFS`) take a plain comma-separated
string, not JSON.

Store the credentials once (Windows Credential Locker / macOS Keychain):

```bash
keyring set dayforce-tafw username     # svc_celigo_integration
keyring set dayforce-tafw password
```

Non-secret config already has committed defaults in
[conf/settings.dev.yaml](conf/settings.dev.yaml) — the CANTest261 test tenant:

```yaml
dayforce_base_uri: https://cantest261-services.dayforcehcm.com/api
dayforce_company:  bluedrop
dayforce_api_version: v1
# -> https://cantest261-services.dayforcehcm.com/api/bluedrop/v1/Employees
```

Override any of it (and creds, for CI) via env / [.env](.env.example):
`DAYFORCE_*`, `TAFW_STATUSES`, `TAFW_EMPLOYEE_XREFS`, `TAFW_WRITER`, `TAFW_STAGING_TABLE`, …

---

## 11. Staging table schema

The authoritative DDL is `STAGING_DDL` in [src/tafw_ingest/staging.py](src/tafw_ingest/staging.py);
every writer runs it. Shape:

```sql
CREATE TABLE IF NOT EXISTS {table} (
    record_hash         STRING       NOT NULL,   -- deterministic id (§7)
    employee_xref       STRING       NOT NULL,
    pto_date            DATE         NOT NULL,
    approval_type_code  STRING       NOT NULL,
    hours_requested     DECIMAL(6,2) NOT NULL,
    -- passthrough / audit (not hashed)
    reason_name         STRING,
    status              STRING,
    time_start          STRING,
    time_end            STRING,
    all_day             BOOLEAN,
    date_of_request     STRING,
    source_payload      STRING,                  -- raw entry JSON for troubleshooting
    first_seen_at       TIMESTAMP    NOT NULL,
    last_seen_at        TIMESTAMP    NOT NULL,
    is_active           BOOLEAN      NOT NULL,   -- false = cancelled / denied / superseded
    deleted_at          TIMESTAMP,
    ingest_run_id       STRING       NOT NULL
) USING DELTA;
```

**Per-cycle write (`StagingWriter.apply`), matched on `record_hash`**

- **new** hash → `INSERT` (`is_active = true`, `first_seen_at = last_seen_at = now`)
- **changed** (same employee+date, new hash) → new row `INSERT`ed; the old hash's row is
  soft-deleted (`is_active = false`, `deleted_at = now`) — `reconcile` reports it as `superseded`
- hash **still present** → bump `last_seen_at` (reactivate if it was inactive)
- hash **gone** this cycle → `is_active = false, deleted_at = now` (`cancelled`; Celigo turns
  this into a NetSuite delete)

The full-history / audit trail requirement is satisfied by keeping rows (soft delete)
and by Delta time-travel; a companion `staging.tafw_day_record_history` append table
can be added if a flat audit query is preferred over `DESCRIBE HISTORY`.

---

## 12. Phase 2 — the day Databricks access arrives

1. `databricks auth login --host https://<workspace>.azuredatabricks.net` → creates a CLI profile.
2. In VS Code, Databricks panel → select profile → select cluster (confirm DBR version).
3. **Confirm the runtime** (`spark.version`, Python version) and adjust pins:
   - `requirements-dev.txt`: replace `pyspark` / `delta-spark` with
     `databricks-connect==<DBR major.minor>.*`
   - `pip uninstall pyspark delta-spark`
4. Create the secret scope and add Dayforce credentials:
   `databricks secrets create-scope dayforce` then `put-secret`.
5. `databricks bundle init` / fill in `databricks.yml`: the notebook task, schedule
   (matches DNS-03 sync frequency), job cluster spec, and the `src/` wheel or
   `--include` path.
6. `databricks bundle validate` → `databricks bundle deploy -t dev`.
7. `databricks bundle run dayforce_tafw_ingest -t dev` → check the Job run output.
8. Point `normalize` / `reconcile` fixtures at a **real** (sanitized) Dayforce response
   and re-verify the hash golden values.
9. Promote: `-t staging` → `-t prod` targets in `databricks.yml`.

---

## 13. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `source .venv/bin/activate` → *No such file or directory* | Windows venv uses `Scripts/`: `source .venv/Scripts/activate` |
| `pip install` pulls source builds / fails on `pyarrow`, `pyspark` | venv is on Python 3.14. Rebuild with `py -3.11 -m venv .venv`. |
| `pyspark` errors after adding `databricks-connect` | They conflict. `pip uninstall pyspark delta-spark` — Connect provides its own. |
| Local Delta test: `JAVA_HOME is not set` / `UnsupportedClassVersion` | Install Temurin 17, set `JAVA_HOME`. Not needed if using `InMemoryStagingWriter`. |
| `databricks-connect` session fails at startup | Client `major.minor` must equal the cluster's DBR. Re-pin and reinstall. |
| `NameError: spark` / `dbutils` running the notebook in VS Code | Expected — that notebook targets a Databricks cluster. Locally, develop against the `tafw_ingest` package + `pytest`, or a `run_cycle(settings, df_client=..., writer=InMemoryStagingWriter())` scratch cell. |
| Notebook cells won't run in VS Code | Wrong kernel — pick "Python 3.11 (tafw-ingest)". |
| Hitting Dayforce 429s | Lower `DAYFORCE_MAX_RPM` (feeds the `pyrate_limiter` bucket); urllib3 `Retry` already honours `Retry-After` on 429. |
| `SettingsError: error parsing value for field "tafw_..."` | List env vars are plain CSV (`TAFW_STATUSES=APPROVED,DENIED`), not JSON. |

---

## 14. Status

- [x] Python 3.11 venv + `requirements.txt` installed and verified
- [x] `pyproject.toml` + `requirements-dev.txt` (editable install, pytest/ruff/mypy config)
- [x] `src/tafw_ingest/` package scaffolded — config, dayforce_client, models, normalize,
      hashing, reconcile, staging, pipeline
- [x] Notebook source file (`notebooks/dayforce_integration_service.py`)
- [x] Test suite + fixtures — 26 tests, `ruff` clean, `mypy` clean
- [ ] Exercise `LocalDeltaStagingWriter` against a real local Delta table (JDK 17)
- [ ] Validate `normalize` against a real Dayforce TAFW payload
- [ ] Databricks workspace access → Phase 2 (`databricks.yml` bundle, secret scope, schedule)
