#
# SPDX-License-Identifier: Apache-2.0
#

from collections.abc import Iterator

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
