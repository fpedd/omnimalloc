#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

from omnimalloc.allocators import SupermallocAllocator
from omnimalloc.analysis import antichain_pressure
from omnimalloc.io import load_allocation
from omnimalloc.validate import validate_allocation


def test_supermalloc_proves_deeploy_microllama_optimal() -> None:
    # The largest L3 pool from Deeploy's microLlama1 CI test: every greedy
    # order lands 32 B above the bound; only some search heuristics close it
    path = Path(__file__).with_name("deeploy_microllama.csv")
    allocations = load_allocation(path).allocations
    # One thread per default heuristic, so the outcome is core-count independent
    solved = SupermallocAllocator(num_threads=3).solve(allocations)
    validate_allocation(solved.allocations)
    assert solved.proved_optimal
    assert solved.peak == antichain_pressure(allocations) == 3904
