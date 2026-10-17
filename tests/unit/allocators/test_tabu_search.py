#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc._cpp import tabu_search_place
from omnimalloc.allocators.tabu_search import TabuSearchAllocator
from omnimalloc.primitives import Allocation


@pytest.mark.parametrize(
    "parameter", ["max_iterations", "neighborhood_size", "tabu_tenure"]
)
def test_tabu_search_rejects_non_positive(parameter: str) -> None:
    with pytest.raises(ValueError, match=f"{parameter} must be positive"):
        TabuSearchAllocator(**{parameter: 0})


def test_tabu_search_cpp_boundary_rejects_zero_timeout() -> None:
    allocations = (
        Allocation(id=1, size=100, start=0, end=10),
        Allocation(id=2, size=100, start=5, end=15),
    )
    with pytest.raises(ValueError, match="timeout must be positive"):
        tabu_search_place(
            allocations,
            seed=0,
            max_iterations=10,
            neighborhood_size=5,
            tabu_tenure=3,
            timeout=0.0,
        )
