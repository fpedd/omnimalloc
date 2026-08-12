#
# SPDX-License-Identifier: Apache-2.0
#

import subprocess
import sys
from pathlib import Path

import pytest

from tests.markers import needs_examples
from tests.paths import EXAMPLES_DIR

pytestmark = needs_examples

EXAMPLE_FILES = sorted(EXAMPLES_DIR.glob("*.py"))
TIMEOUT_SECONDS = 600  # 10 minutes
RATE_LIMIT_MARKERS = ("429 Too Many Requests", "huggingface.co")


@pytest.mark.parametrize("example_file", EXAMPLE_FILES, ids=lambda p: p.name)
def test_examples(example_file: Path, tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(example_file)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=TIMEOUT_SECONDS,
        check=False,
    )

    if result.returncode != 0:
        if all(marker in result.stderr for marker in RATE_LIMIT_MARKERS):
            pytest.skip("Hugging Face Hub rate limited the request")
        print(f"\n=== STDOUT ===\n{result.stdout}")
        print(f"\n=== STDERR ===\n{result.stderr}")
        pytest.fail(f"Example {example_file.name} failed with code {result.returncode}")
