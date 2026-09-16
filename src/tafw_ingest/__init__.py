"""Dayforce TAFW -> Databricks staging ingestion service.

Public surface:

* :mod:`tafw_ingest.config`      - :class:`Settings` (env / YAML / notebook widgets)
* :mod:`tafw_ingest.dayforce_client` - :class:`DayforceClient` REST wrapper
* :mod:`tafw_ingest.employees`   - paginate ``GET /Employees`` -> pandas DataFrame
* :mod:`tafw_ingest.department_map` - Department XRefCode -> (project_id, task_id)
* :mod:`tafw_ingest.roster`      - narrow the roster to in-scope departments
* :mod:`tafw_ingest.normalize`   - raw TAFW JSON -> :class:`DayRecord` list
* :mod:`tafw_ingest.hashing`     - the frozen deterministic record-hash spec
* :mod:`tafw_ingest.reconcile`   - new / changed / cancelled diff
* :mod:`tafw_ingest.staging`     - :class:`StagingWriter` (mem / local-Delta / dbx)
* :mod:`tafw_ingest.pipeline`    - :func:`run_cycle`, the orchestration entrypoint
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
