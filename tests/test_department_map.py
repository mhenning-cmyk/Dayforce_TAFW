from __future__ import annotations

import pytest

from tafw_ingest.department_map import DEFAULT_MAP_PATH, DepartmentMap, ProjectTask


def test_loads_committed_yaml_and_matches_the_report():
    m = DepartmentMap.load()
    assert m.resolve("BTSI_LEARNINGLOGICS") == ProjectTask(96731, 3431179)
    assert m.resolve("BTSI_SERVICES") == ProjectTask(96732, 3431175)
    assert m.resolve("BTSI_SIMULATION_PRODUCT") == ProjectTask(96731, 3431179)
    assert m.project_ids == frozenset({96731, 96732})
    assert len(m) == 3
    assert DEFAULT_MAP_PATH.is_file()


def test_department_xref_is_case_and_whitespace_insensitive():
    m = DepartmentMap.load()
    assert m.is_in_scope("  btsi_services ")
    assert m.resolve("Btsi_Services") == ProjectTask(96732, 3431175)


def test_out_of_scope_department_resolves_to_none():
    m = DepartmentMap.load()
    assert m.resolve("BTSI_CORPORATE") is None
    assert not m.is_in_scope("BTSI_CORPORATE")
    assert m.resolve(None) is None


def test_in_scope_set():
    m = DepartmentMap.load()
    assert m.in_scope_departments == frozenset(
        {"BTSI_LEARNINGLOGICS", "BTSI_SERVICES", "BTSI_SIMULATION_PRODUCT"}
    )


def test_load_rejects_malformed_entries(tmp_path):
    bad = tmp_path / "m.yaml"
    bad.write_text("BTSI_X:\n  project_id: 1\n", encoding="utf-8")  # missing task_id
    with pytest.raises(ValueError, match="task_id"):
        DepartmentMap.load(bad)


def test_load_rejects_empty_map(tmp_path):
    empty = tmp_path / "m.yaml"
    empty.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        DepartmentMap.load(empty)
