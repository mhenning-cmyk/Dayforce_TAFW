"""Department -> (project_id, task_id) resolution.

Bluedrop's Dayforce Report Writer report derives ``Project ID`` and ``Task ID``
with CASE expressions over ``Department.XRefCode``. The TAFW ingestion service
needs the same rule, for two reasons:

1. **Scope filter** - only employees whose department is in the map are in
   scope (the report's ``ELSE NULL`` branch drops everyone else).
2. **Enrichment** - every retained TAFW day-record is tagged with the
   ``project_id`` / ``task_id`` NetSuite expects.

The mapping data lives in ``conf/department_project_task.yaml`` so it can be
kept in step with the Dayforce report without a code change.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

__all__ = ["ProjectTask", "DepartmentMap", "DEFAULT_MAP_PATH"]

DEFAULT_MAP_PATH = (
    Path(__file__).resolve().parents[2] / "conf" / "department_project_task.yaml"
)


@dataclass(frozen=True)
class ProjectTask:
    project_id: int
    task_id: int


class DepartmentMap:
    """Immutable lookup from a normalized Department XRefCode to a ProjectTask."""

    def __init__(self, mapping: dict[str, ProjectTask]) -> None:
        # Department XRefCodes are case-insensitive tokens; normalize on the way in.
        self._map = {self._norm(k): v for k, v in mapping.items()}

    @staticmethod
    def _norm(xref: Any) -> str:
        return str(xref).strip().upper()

    # ------------------------------------------------------------------ #
    @classmethod
    def load(cls, path: str | Path | None = None) -> DepartmentMap:
        """Load the mapping from YAML (default: ``conf/department_project_task.yaml``)."""
        path = Path(path) if path else DEFAULT_MAP_PATH
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        mapping: dict[str, ProjectTask] = {}
        for dept, spec in raw.items():
            if (
                not isinstance(spec, dict)
                or "project_id" not in spec
                or "task_id" not in spec
            ):
                raise ValueError(
                    f"{path}: entry {dept!r} must have 'project_id' and 'task_id'"
                )
            mapping[dept] = ProjectTask(int(spec["project_id"]), int(spec["task_id"]))
        if not mapping:
            raise ValueError(f"{path}: department map is empty")
        return cls(mapping)

    # ------------------------------------------------------------------ #
    @property
    def in_scope_departments(self) -> frozenset[str]:
        """Normalized Department XRefCodes that are in scope."""
        return frozenset(self._map)

    def is_in_scope(self, department_xref: Any) -> bool:
        return self._norm(department_xref) in self._map

    def resolve(self, department_xref: Any) -> ProjectTask | None:
        """Return the ProjectTask for a department, or ``None`` if out of scope."""
        return self._map.get(self._norm(department_xref))

    @property
    def project_ids(self) -> frozenset[int]:
        return frozenset(pt.project_id for pt in self._map.values())

    def __len__(self) -> int:
        return len(self._map)

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        pairs = ", ".join(
            f"{d}->({pt.project_id},{pt.task_id})" for d, pt in self._map.items()
        )
        return f"DepartmentMap({pairs})"
