#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.allocators.supermalloc import SupermallocAllocator
from omnimalloc.io import load_allocation
from omnimalloc.validate import validate_allocation

CHALLENGING_DIR = (
    Path(__file__).resolve().parents[2] / "external" / "minimalloc" / "challenging"
)

# Instances that must reach a proved optimum within a second on any thread
# count; before node-budgeted rounds, 1-4 threads stalled on the first member
OPTIMA = {"C": 1039360, "G": 1048576, "I": 1048576, "K": 1048576}


@pytest.mark.skipif(not CHALLENGING_DIR.is_dir(), reason="needs a checkout")
@pytest.mark.parametrize("num_threads", [1, 8])
@pytest.mark.parametrize("name", sorted(OPTIMA))
def test_challenging_reaches_a_proved_optimum(name: str, num_threads: int) -> None:
    pool = load_allocation(CHALLENGING_DIR / f"{name}.1048576.csv")
    allocator = SupermallocAllocator(timeout=10.0, num_threads=num_threads)
    result = allocator.solve(pool.allocations)
    validate_allocation(result.allocations)
    assert result.peak == OPTIMA[name]
    assert result.proved_optimal
