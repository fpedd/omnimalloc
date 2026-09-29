#
# SPDX-License-Identifier: Apache-2.0
#
"""The contract every registered allocator keeps, driven by its class flags."""

import inspect
import random

import pytest
from omnimalloc import validate_allocation
from omnimalloc.allocators import BaseAllocator, available_allocators
from omnimalloc.analysis import antichain_pressure, placement_pressure
from omnimalloc.benchmark.sources import HighContentionSource, SyncPatternSource
from omnimalloc.primitives import Allocation

# Small budgets: the contract does not depend on how hard an allocator searches
FAST = {
    "genetic": {"population_size": 10, "max_generations": 3},
    "hill_climb": {"max_iterations": 100},
    "random": {"num_trials": 10},
    "simulated_annealing": {"max_iterations": 200},
    "tabu_search": {"max_iterations": 30},
}

NAMES = sorted(
    name
    for name in available_allocators()
    if BaseAllocator.get(name).__module__.startswith("omnimalloc.")
)
VECTOR = [name for name in NAMES if BaseAllocator.get(name).supports_vector_time]
PINNED = [name for name in NAMES if BaseAllocator.get(name).supports_pinned]
SCALAR_ONLY = sorted(set(NAMES) - set(VECTOR))
UNPINNED = sorted(set(NAMES) - set(PINNED))
SEARCHES = ("genetic", "hill_climb", "simulated_annealing", "tabu_search", "telamalloc")


def build(name: str, **kwargs: object) -> BaseAllocator:
    try:
        return BaseAllocator.get(name)(**kwargs)
    except ImportError as error:  # an optional dependency is missing
        pytest.skip(str(error))


def fast(name: str) -> BaseAllocator:
    return build(name, **FAST.get(name, {}))


def _takes(name: str, parameter: str) -> bool:
    return parameter in inspect.signature(BaseAllocator.get(name)).parameters


def _scalar(seed: int = 0, count: int = 40) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(count):
        start = rng.randint(0, 30)
        size, duration = rng.randint(1, 512), rng.randint(1, 8)
        allocations.append(
            Allocation(id=i, size=size, start=start, end=start + duration)
        )
    return tuple(allocations)


def _vector(seed: int = 0, count: int = 24, dim: int = 3) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(count):
        start = tuple(rng.randint(0, 6) for _ in range(dim))
        delta = [rng.randint(0, 3) for _ in range(dim)]
        delta[rng.randrange(dim)] += 1
        end = tuple(s + d for s, d in zip(start, delta, strict=True))
        allocations.append(
            Allocation(id=i, size=rng.randint(1, 256), start=start, end=end)
        )
    return tuple(allocations)


SCALAR_INSTANCES = {
    "random": _scalar(),
    "contention": HighContentionSource(
        num_allocations=40, time_window=10, seed=1
    ).get_allocations(),
}
VECTOR_INSTANCES = {
    "random_vector": _vector(),
    "sync_pattern": SyncPatternSource(
        num_allocations=40, num_threads=3, pattern="sparse", seed=2
    ).get_allocations(),
}
CASES = [
    *((name, instance) for name in NAMES for instance in SCALAR_INSTANCES),
    *((name, instance) for name in VECTOR for instance in VECTOR_INSTANCES),
]
INSTANCES = SCALAR_INSTANCES | VECTOR_INSTANCES


def _offsets(placed: tuple[Allocation, ...]) -> list[int | None]:
    return [alloc.offset for alloc in placed]


@pytest.mark.parametrize("name", NAMES)
def test_nothing_places_as_nothing(name: str) -> None:
    assert fast(name).allocate(()) == ()


@pytest.mark.parametrize("name", NAMES)
def test_a_single_allocation_lands_at_zero(name: str) -> None:
    placed = fast(name).allocate((Allocation(id=1, size=100, start=0, end=10),))
    assert _offsets(placed) == [0]


# naive stacks by design and never reuses an address
@pytest.mark.parametrize("name", sorted(set(NAMES) - {"naive"}))
def test_disjoint_lifetimes_share_offset_zero(name: str) -> None:
    allocations = tuple(
        Allocation(id=i, size=10 * (i + 1), start=i, end=i + 1) for i in range(4)
    )
    assert _offsets(fast(name).allocate(allocations)) == [0, 0, 0, 0]


@pytest.mark.parametrize("name", NAMES)
def test_identical_lifetimes_stack(name: str) -> None:
    allocations = tuple(Allocation(id=i, size=100, start=0, end=10) for i in range(5))
    placed = fast(name).allocate(allocations)
    validate_allocation(placed)
    assert placement_pressure(placed) == 500


@pytest.mark.parametrize(("name", "instance"), CASES)
def test_placement_is_valid_bounded_and_in_input_order(
    name: str, instance: str
) -> None:
    allocations = INSTANCES[instance]
    placed = fast(name).allocate(allocations)
    validate_allocation(placed)
    assert [(a.id, a.size, a.start, a.end) for a in placed] == [
        (a.id, a.size, a.start, a.end) for a in allocations
    ]
    bound = antichain_pressure(allocations, work_budget=None)
    assert bound <= placement_pressure(placed) <= sum(a.size for a in allocations)


@pytest.mark.parametrize("name", NAMES)
def test_placement_is_deterministic(name: str) -> None:
    allocations = SCALAR_INSTANCES["random"]
    allocator = fast(name)
    first = _offsets(allocator.allocate(allocations))
    assert _offsets(allocator.allocate(allocations)) == first
    assert _offsets(fast(name).allocate(allocations)) == first


@pytest.mark.parametrize("name", PINNED)
def test_a_pin_is_honored(name: str) -> None:
    allocations = (
        Allocation(id="pinned", size=10, start=0, end=5, offset=100),
        Allocation(id="free", size=10, start=0, end=5),
    )
    pinned, free = fast(name).allocate(allocations)
    assert pinned.offset == 100
    assert free.offset != 100


@pytest.mark.parametrize("name", UNPINNED)
def test_a_pin_is_rejected(name: str) -> None:
    allocations = (Allocation(id=1, size=10, start=0, end=5, offset=0),)
    with pytest.raises(ValueError, match="cannot honor pinned offsets"):
        fast(name).allocate(allocations)


@pytest.mark.parametrize("name", SCALAR_ONLY)
def test_vector_time_is_rejected(name: str) -> None:
    with pytest.raises(ValueError, match=r"requires scalar .* 3-dim vector clocks"):
        fast(name).allocate(VECTOR_INSTANCES["random_vector"])


def test_only_the_interval_solvers_need_scalar_time_and_no_pins() -> None:
    assert SCALAR_ONLY == UNPINNED == ["minimalloc", "supermalloc", "telamalloc"]


@pytest.mark.parametrize("name", SEARCHES)
def test_a_search_never_loses_to_greedy_by_size(name: str) -> None:
    allocations = _scalar(seed=3)
    baseline = placement_pressure(fast("greedy_by_size").allocate(allocations))
    assert placement_pressure(fast(name).allocate(allocations)) <= baseline


@pytest.mark.parametrize("name", [name for name in NAMES if _takes(name, "seed")])
@pytest.mark.parametrize("seed", [-1, 2**64])
def test_an_out_of_range_seed_is_rejected(name: str, seed: int) -> None:
    with pytest.raises(ValueError, match="seed must be in"):
        build(name, seed=seed)


@pytest.mark.parametrize("name", [name for name in NAMES if _takes(name, "timeout")])
@pytest.mark.parametrize("timeout", [0, -1.0])
def test_a_non_positive_timeout_is_rejected(name: str, timeout: float) -> None:
    with pytest.raises(ValueError, match="timeout must be positive or None"):
        build(name, timeout=timeout)
