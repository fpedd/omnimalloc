#
# SPDX-License-Identifier: Apache-2.0
#

import shutil
from pathlib import Path

import pytest


@pytest.fixture  # type: ignore[misc]
def artifacts_dir(request: pytest.FixtureRequest) -> Path:
    artifacts_root = Path(__file__).parent / "artifacts"
    test_name = request.node.name
    test_file = Path(request.node.fspath).stem
    test_dir = artifacts_root / test_file / test_name

    if test_dir.exists():
        shutil.rmtree(test_dir)

    test_dir.mkdir(parents=True)

    return Path(test_dir)
