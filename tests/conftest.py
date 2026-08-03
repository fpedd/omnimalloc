#
# SPDX-License-Identifier: Apache-2.0
#

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from omnimalloc.allocators import BaseAllocator
from omnimalloc.benchmark.sources import BaseSource

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


@pytest.fixture(autouse=True)  # type: ignore[misc]
def isolated_registries() -> Iterator[None]:
    # Defining a Registered subclass registers it process-wide, so a
    # throwaway allocator or source declared inside one test would otherwise
    # be picked up by every later test that sweeps the registry.
    snapshots = [(cls, cls.registry()) for cls in (BaseAllocator, BaseSource)]
    yield
    for cls, snapshot in snapshots:
        cls._registry.clear()
        cls._registry.update(snapshot)
