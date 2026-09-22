"""Git clean filter: strip run-to-run execution noise from .ipynb files.

Every time a notebook is *run* in Databricks (even with no code changes),
each executed cell's ``execution_count`` and ``outputs`` change - a plain
git diff can't tell that apart from an actual edit, so "someone just ran it"
and "someone changed the code" look identical and both trigger a merge.
This filter strips those two fields (never source, never cell metadata/
identity) before the file is written to a git commit, so re-running a
notebook with no real changes produces no diff at all.

One-time setup per clone (see README.md) - `.gitattributes` declares the
mapping, but the filter *command* itself has to be registered locally:

    git config filter.strip-notebook-output.clean "python scripts/strip_notebook_output.py"
    git config filter.strip-notebook-output.smudge cat
    git config filter.strip-notebook-output.required false

Read via stdin, written to stdout - the standard git clean-filter contract.
Anything that isn't a valid, cells-bearing notebook passes through
unchanged rather than risk corrupting a file this was never meant to touch.
"""

from __future__ import annotations

import json
import sys


def _strip(notebook: dict) -> dict:
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") == "code":
            cell["execution_count"] = None
            cell["outputs"] = []
    return notebook


def main() -> int:
    raw = sys.stdin.read()
    try:
        notebook = json.loads(raw)
    except json.JSONDecodeError:
        sys.stdout.write(raw)
        return 0

    if not isinstance(notebook, dict) or "cells" not in notebook or "nbformat" not in notebook:
        sys.stdout.write(raw)
        return 0

    json.dump(_strip(notebook), sys.stdout, indent=1, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
