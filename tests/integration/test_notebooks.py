#
# SPDX-License-Identifier: Apache-2.0
#

import subprocess
import sys
from pathlib import Path

import pytest
from omnimalloc.common.directories import NOTEBOOKS_DIR

NOTEBOOK_FILES = sorted(NOTEBOOKS_DIR.glob("*.ipynb"))
TIMEOUT_SECONDS = 300  # 5 minutes


@pytest.mark.parametrize("notebook_file", NOTEBOOK_FILES, ids=lambda p: p.name)
def test_notebooks(notebook_file: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "nbconvert",
            "--execute",
            "--to",
            "notebook",
            "--stdout",
            str(notebook_file),
        ],
        cwd=NOTEBOOKS_DIR.parent,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )

    if result.returncode != 0:
        print(f"\n=== STDOUT ===\n{result.stdout}")
        print(f"\n=== STDERR ===\n{result.stderr}")
        pytest.fail(
            f"Notebook {notebook_file.name} failed with code {result.returncode}"
        )
