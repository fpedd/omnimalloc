#
# SPDX-License-Identifier: Apache-2.0
#

import shutil
from pathlib import Path

import pytest

# Headless backend: plot_allocation(path=None) displays the figure, which
# must never block the test run on an interactive backend.
try:
    import matplotlib as mpl

    mpl.use("Agg")
except ImportError:
    pass


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
