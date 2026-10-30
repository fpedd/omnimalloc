#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.allocators import SupermallocAllocator
from omnimalloc.analysis import antichain_pressure
from omnimalloc.io import load_allocation
from omnimalloc.validate import validate_allocation

# Pools from pulp-platform/Deeploy codegen (ETH Zurich and University of Bologna)
DEEPLOY = Path(__file__).with_name("deeploy")


@pytest.mark.parametrize(
    ("name", "lower_bound"),
    [
        ("microllama2_siracusa_l2_db", 4672),
        ("microllama32_siracusa_l3_db", 37552),
    ],
)
def test_supermalloc_proves_deeploy_pool_optimal(name: str, lower_bound: int) -> None:
    allocations = load_allocation(DEEPLOY / f"{name}.csv").allocations
    # One thread per default heuristic, so the outcome is core-count independent
    solved = SupermallocAllocator(num_threads=3).solve(allocations)
    validate_allocation(solved.allocations)
    assert solved.proved_optimal
    assert solved.peak == antichain_pressure(allocations) == lower_bound
