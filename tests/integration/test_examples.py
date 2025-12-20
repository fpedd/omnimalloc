#
# SPDX-License-Identifier: Apache-2.0
#

import subprocess
import sys
from pathlib import Path

import pytest
from omnimalloc.common.directories import EXAMPLES_DIR

EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.py"))
TIMEOUT_SECONDS = 300  # 5 minutes


@pytest.mark.parametrize("example_file", EXAMPLE_FILES, ids=lambda p: p.name)
def test_examples(example_file: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(example_file)],
        cwd=EXAMPLES_DIR.parent,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )

    if result.returncode != 0:
        print(f"\n=== STDOUT ===\n{result.stdout}")
        print(f"\n=== STDERR ===\n{result.stderr}")
        pytest.fail(f"Example {example_file.name} failed with code {result.returncode}")
