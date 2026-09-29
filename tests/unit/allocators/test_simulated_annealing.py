#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators.simulated_annealing import SimulatedAnnealingAllocator


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_iterations": 0}, "max_iterations must be positive"),
        ({"initial_temperature": -1.0}, "initial_temperature must be non-negative"),
        ({"cooling_rate": 0.0}, "cooling_rate must be in"),
        ({"cooling_rate": 1.5}, "cooling_rate must be in"),
    ],
)
def test_simulated_annealing_rejects(kwargs: dict[str, float], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        SimulatedAnnealingAllocator(**kwargs)


def test_simulated_annealing_repr_shows_flat_kwargs() -> None:
    allocator = SimulatedAnnealingAllocator(seed=7, max_iterations=10)
    assert repr(allocator) == (
        "SimulatedAnnealingAllocator(seed=7, max_iterations=10, "
        "initial_temperature=3.0, cooling_rate=0.998, timeout=3.0)"
    )
