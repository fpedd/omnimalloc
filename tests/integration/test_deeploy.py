#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.allocators import SupermallocAllocator
from omnimalloc.analysis import antichain_pressure
from omnimalloc.io import load_allocation
from omnimalloc.validate import validate_allocation

# Largest L2/L3 pools Deeploy's tiler hands its allocator, dumped from
# pulp-platform/Deeploy codegen and named model_platform_level[_db] (double
# buffered); only ids, sizes and lifetimes of its test models (CC-BY-ND-4.0,
# ETH Zurich and University of Bologna) are kept. Omni or supermalloc's own
# greedy start misses the bound on most, and narrowing the default heuristics
# leaves some unproved
DEEPLOY = Path(__file__).with_name("deeploy")


@pytest.mark.parametrize(
    ("name", "lower_bound"),
    [
        ("microllama1_neureka_wmem_l3_db", 3904),
        ("microllama2_neureka_wmem_l3_db", 4928),
        ("microllama2_siracusa_l2_db", 4672),
        ("microllama4_neureka_l3", 6976),
        ("microllama8_siracusa_l2_db", 10864),
        ("microllama8_parallel_siracusa_l3_db", 21248),
        ("microllama16_neureka_l3", 19440),
        ("microllama32_siracusa_l3_db", 37552),
        ("microllama64_neureka_l3", 73904),
    ],
)
def test_supermalloc_proves_deeploy_pool_optimal(name: str, lower_bound: int) -> None:
    allocations = load_allocation(DEEPLOY / f"{name}.csv").allocations
    # One thread per default heuristic, so the outcome is core-count independent
    solved = SupermallocAllocator(num_threads=3).solve(allocations)
    validate_allocation(solved.allocations)
    assert solved.proved_optimal
    assert solved.peak == antichain_pressure(allocations) == lower_bound
