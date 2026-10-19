"""Pre-commit check: refuse to commit an .ipynb Databricks can't open.

A notebook with leftover merge-conflict markers, or otherwise broken JSON,
shows up in Databricks as "Notebook failed to load" - and the clean filter
(scripts/strip_notebook_output.py) passes such a file through untouched.
This checks the *staged* copy of every .ipynb being committed.

Install once per clone (see README.md):

    cp scripts/pre-commit .git/hooks/pre-commit
"""

from __future__ import annotations

import json
import subprocess
import sys

CONFLICT_MARKERS = (b"<<<<<<< ", b"\n=======\n", b">>>>>>> ")


def _staged_notebooks() -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR", "-z", "--", "*.ipynb"],
        check=True,
        capture_output=True,
    ).stdout
    return [p.decode("utf-8") for p in out.split(b"\0") if p]


def _problem(raw: bytes) -> str | None:
    if any(marker in raw for marker in CONFLICT_MARKERS):
        return "contains merge-conflict markers"
    try:
        notebook = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        return f"is not valid UTF-8 ({exc})"
    except json.JSONDecodeError as exc:
        return f"is not valid JSON ({exc})"
    if not isinstance(notebook, dict) or notebook.get("nbformat") != 4:
        return "is not an nbformat 4 notebook"
    if not isinstance(notebook.get("cells"), list):
        return "has no cells list"
    return None


def main() -> int:
    failed = False
    for path in _staged_notebooks():
        raw = subprocess.run(["git", "show", f":{path}"], check=True, capture_output=True).stdout
        problem = _problem(raw)
        if problem:
            failed = True
            sys.stderr.write(f"check_notebooks: {path} {problem} - Databricks can't load it.\n")
    if failed:
        sys.stderr.write("Fix the notebook(s) above, `git add` them again, then commit.\n")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
