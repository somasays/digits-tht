"""Execute the analysis notebook against the local warehouse.

Kept out of the notebook itself so the source remains clean and a separate executed copy
contains the outputs produced from the local marts.
"""
from __future__ import annotations

import sys
from pathlib import Path

SOURCE = Path("notebooks/taxi_analysis.ipynb")
RENDERED = Path("notebooks/taxi_analysis_executed.ipynb")


def main() -> int:
    import nbformat
    from nbclient import NotebookClient

    notebook = nbformat.read(SOURCE, as_version=4)
    # Run from the repository root, because the warehouse path and `src` are relative to it.
    NotebookClient(notebook, timeout=1800, kernel_name="python3",
                   resources={"metadata": {"path": "."}}).execute()

    RENDERED.parent.mkdir(exist_ok=True)
    nbformat.write(notebook, RENDERED)
    print(f"executed {SOURCE} -> {RENDERED}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
