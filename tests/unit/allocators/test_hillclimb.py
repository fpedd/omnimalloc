#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.hillclimb import HillClimbAllocator
from omnimalloc.primitives import Allocation
from omnimalloc.validate import validate_allocation


def test_hillclimb_rejects_non_positive_iterations() -> None:
    with pytest.raises(ValueError, match="max_iterations must be positive"):
        HillClimbAllocator(max_iterations=0)


def test_hillclimb_survives_rejected_step_undo() -> None:
    allocations = (
        Allocation(id=0, size=50, start=4, end=7),
        Allocation(id=1, size=40, start=2, end=6),
        Allocation(id=2, size=30, start=2, end=4),
        Allocation(id=3, size=40, start=4, end=5),
        Allocation(id=4, size=70, start=2, end=3),
    )
    validate_allocation(HillClimbAllocator(max_iterations=6).allocate(allocations))


def test_hillclimb_repr_omits_the_pinned_annealing_knobs() -> None:
    assert "temperature" not in repr(HillClimbAllocator())
    assert "cooling_rate" not in repr(HillClimbAllocator())
