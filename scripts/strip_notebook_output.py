"""Git clean filter: commit .ipynb files in exactly the form Databricks writes.

This repo's notebooks are edited in two places - VS Code/Jupyter locally, and
inside a Databricks Git folder - and the two serialize the same notebook
differently (kernelspec, language_info, execution_count, per-cell Databricks
metadata, trailing newline). Whenever the committed file differs from what
Databricks would write, Databricks rewrites it as soon as the notebook is
opened or run, leaving an uncommitted change in the Git folder. The next pull
then stashes that change, pulls, and re-applies the stash - and when that
conflicts, git writes ``<<<<<<< Updated upstream`` / ``>>>>>>> Stashed
changes`` markers straight into the JSON, and Databricks shows "Notebook
failed to load" (commit a58f3af is one of those).

So before a notebook is written to a local commit, this filter:

* strips outputs and sets ``execution_count`` to 0, as Databricks does - so
  "someone just ran it" never shows up as a change;
* gives every cell Databricks' ``application/vnd.databricks.v1+cell``
  metadata (a cell added in VS Code has none, so Databricks would add it);
* cell ``id`` fields: a notebook with no ids is pinned to ``nbformat_minor``
  0 (Databricks' own value). A notebook where any cell has an id gets one on
  every cell (the cell's nuid) and ``nbformat_minor`` 5 - ids are only valid
  from 4.5, and a 4.0 file carrying them is an invalid notebook;
* replaces kernelspec / language_info with Databricks' values;
* serializes like Databricks: 1-space indent, sorted keys, raw UTF-8, no
  trailing newline.

One-time setup per clone (see README.md) - the mapping lives in
`.git/info/attributes`, and the filter *command* has to be registered locally:

    git config filter.strip-notebook-output.clean "python scripts/strip_notebook_output.py"
    git config filter.strip-notebook-output.smudge cat
    git config filter.strip-notebook-output.required false

Read via stdin, written to stdout - the standard git clean-filter contract.
Anything that isn't a valid, cells-bearing notebook passes through unchanged
(with a warning on stderr) rather than risk corrupting a file this was never
meant to touch; scripts/check_notebooks.py is what blocks committing one.
"""

from __future__ import annotations

import json
import sys
import uuid

DATABRICKS_KERNELSPEC = {"display_name": "Python 3", "language": "python", "name": "python3"}
DATABRICKS_LANGUAGE_INFO = {"name": "python"}

# Fixed namespace so a generated nuid is deterministic: git may run the clean
# filter many times on the same content, and it must give the same bytes back.
_NUID_NAMESPACE = uuid.UUID("5b7c2f0e-3d1a-4c8e-9f6b-2a4d8e1c7b30")


def _source_text(cell: dict) -> str:
    source = cell.get("source", "")
    return "".join(source) if isinstance(source, list) else str(source)


def _source_lines(text: str) -> list[str]:
    return text.splitlines(keepends=True)


def _databricks_cell_metadata(cell: dict, index: int) -> None:
    metadata = cell.setdefault("metadata", {})
    dbx = metadata.setdefault("application/vnd.databricks.v1+cell", {})
    if not dbx.get("nuid"):
        seed = f"{index}\n{cell.get('cell_type')}\n{_source_text(cell)}"
        dbx["nuid"] = str(uuid.uuid5(_NUID_NAMESPACE, seed))
    is_code = cell.get("cell_type") == "code"
    dbx.setdefault("cellMetadata", {"byteLimit": 2048000, "rowLimit": 10000} if is_code else {})
    dbx.setdefault("inputWidgets", {})
    dbx.setdefault("showTitle", False)
    dbx.setdefault("tableResultSettingsMap", {})
    dbx.setdefault("title", "")


def _normalize(notebook: dict) -> dict:
    cells = notebook.get("cells", [])
    with_ids = any(cell.get("id") for cell in cells)
    seen_ids: set[str] = set()
    for index, cell in enumerate(cells):
        cell["source"] = _source_lines(_source_text(cell))
        if cell.get("cell_type") == "code":
            cell["execution_count"] = 0
            cell["outputs"] = []
        _databricks_cell_metadata(cell, index)
        if with_ids:
            # Keep an existing id; a missing or duplicate one (e.g. a cell
            # copy-pasted in VS Code) becomes the cell's nuid.
            cell_id = cell.get("id")
            if not cell_id or cell_id in seen_ids:
                cell_id = cell["metadata"]["application/vnd.databricks.v1+cell"]["nuid"]
            cell["id"] = cell_id
            seen_ids.add(cell_id)
        else:
            cell.pop("id", None)

    metadata = notebook.setdefault("metadata", {})
    metadata["kernelspec"] = dict(DATABRICKS_KERNELSPEC)
    metadata["language_info"] = dict(DATABRICKS_LANGUAGE_INFO)
    notebook["nbformat"] = 4
    notebook["nbformat_minor"] = 5 if with_ids else 0
    return notebook


def main() -> int:
    # Raw bytes in and out: sys.stdin/stdout default to the locale codepage on
    # Windows (cp1252), which crashes on some UTF-8 sequences and writes CRLF -
    # and a crashed filter makes git silently commit the unfiltered file.
    raw = sys.stdin.buffer.read()
    try:
        notebook = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"strip_notebook_output: not valid notebook JSON ({exc}); left as-is\n")
        sys.stdout.buffer.write(raw)
        return 0

    if not isinstance(notebook, dict) or "cells" not in notebook or "nbformat" not in notebook:
        sys.stdout.buffer.write(raw)
        return 0

    text = json.dumps(_normalize(notebook), indent=1, sort_keys=True, ensure_ascii=False)
    sys.stdout.buffer.write(text.encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
