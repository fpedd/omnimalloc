#
# SPDX-License-Identifier: Apache-2.0
#

import itertools
import random

from omnimalloc import (
    Allocation,
    Pool,
    allocate,
    validate_allocation,
)
from omnimalloc.allocators import BaseAllocator, available_allocators
from omnimalloc.analysis import (
    antichain_pressure,
    closure_pressure,
    conflict_degrees,
    conflict_graph,
    conflicts,
    placement_pressure,
    try_linearize,
)
from omnimalloc.benchmark.sources import (
    ConcurrentTilingSource,
    HighContentionSource,
    PinwheelSource,
    RandomSource,
    SyncPatternSource,
    TilingSource,
)

SEEDS = (0, 1, 2, 3)

PERMUTATION_SEEDS = (0, 1, 2, 3, 4, 5, 6, 7)

REORDERING_SENSITIVE = (
    Allocation(id=0, size=6, start=0, end=3),
    Allocation(id=1, size=5, start=2, end=5),
    Allocation(id=2, size=8, start=3, end=4),
    Allocation(id=3, size=6, start=1, end=2),
)

LANE_SENSITIVE = (
    Allocation(id=0, size=2, start=(0, 0), end=(1, 2)),
    Allocation(id=1, size=8, start=(1, 0), end=(2, 1)),
    Allocation(id=2, size=5, start=(3, 1), end=(5, 3)),
    Allocation(id=3, size=9, start=(2, 2), end=(4, 3)),
    Allocation(id=4, size=4, start=(1, 0), end=(3, 0)),
)


def _scalar_instance(seed: int, count: int = 40) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(count):
        start = rng.randint(0, 30)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 512),
                start=start,
                end=start + rng.randint(1, 8),
            )
        )
    return tuple(allocations)


def _vector_instance(
    seed: int, count: int = 24, dim: int = 3
) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(count):
        start = tuple(rng.randint(0, 6) for _ in range(dim))
        delta = [rng.randint(0, 3) for _ in range(dim)]
        if sum(delta) == 0:
            delta[rng.randrange(dim)] = 1
        end = tuple(s + d for s, d in zip(start, delta, strict=True))
        allocations.append(
            Allocation(id=i, size=rng.randint(1, 256), start=start, end=end)
        )
    return tuple(allocations)


def _lockstep_instance(seed: int, count: int = 30) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=alloc.id,
            size=alloc.size,
            start=(alloc.start, alloc.start, 0),
            end=(alloc.end, alloc.end, 0),
        )
        for alloc in _scalar_instance(seed, count)
    )


def _staircase_instance(count: int = 30) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(id=i, size=8 + i, start=(i, 2 * i), end=(i + 2, 2 * i + 3))
        for i in range(count)
    )


def _source_instances(seed: int) -> dict[str, tuple[Allocation, ...]]:
    return {
        "random": RandomSource(num_allocations=60, seed=seed).get_allocations(),
        "contention": HighContentionSource(
            num_allocations=60, time_window=10, seed=seed
        ).get_allocations(),
        "tiling": TilingSource(num_allocations=64, seed=seed).get_allocations(),
        "pinwheel": PinwheelSource(num_allocations=65, seed=seed).get_allocations(),
        "sync": SyncPatternSource(
            num_allocations=48, num_threads=3, pattern="sparse", seed=seed
        ).get_allocations(),
        "concurrent_tiling": ConcurrentTilingSource(
            num_allocations=48, num_threads=4, capacity=1 << 20, seed=seed
        ).get_allocations(),
    }


def _allocator_classes() -> tuple[type[BaseAllocator], ...]:
    registered = (BaseAllocator.get(name) for name in available_allocators())
    return tuple(
        allocator_cls
        for allocator_cls in registered
        if allocator_cls.__module__.startswith("omnimalloc.")
    )


def _allocators() -> tuple[BaseAllocator, ...]:
    instances = []
    for allocator_cls in _allocator_classes():
        try:
            instances.append(allocator_cls())
        except ImportError:
            continue
    return tuple(instances)


def _peak(allocations: tuple[Allocation, ...]) -> int:
    return placement_pressure(allocations)


def _offsets(allocations: tuple[Allocation, ...]) -> dict[int | str, int | None]:
    return {alloc.id: alloc.offset for alloc in allocations}


def _shuffled(allocations: tuple[Allocation, ...], seed: int) -> tuple[Allocation, ...]:
    reordered = list(allocations)
    random.Random(seed).shuffle(reordered)
    return tuple(reordered)


def _scaled(allocations: tuple[Allocation, ...], factor: int) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=alloc.id,
            size=factor * alloc.size,
            start=alloc.start,
            end=alloc.end,
        )
        for alloc in allocations
    )


def _dilated(
    allocations: tuple[Allocation, ...], factor: int
) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=alloc.id,
            size=alloc.size,
            start=tuple(factor * t for t in alloc.start),
            end=tuple(factor * t for t in alloc.end),
        )
        for alloc in allocations
    )


def _padded(allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=alloc.id,
            size=alloc.size,
            start=(*alloc.start, 0),
            end=(*alloc.end, 0),
        )
        for alloc in allocations
    )


def _lane_permuted(
    allocations: tuple[Allocation, ...], lanes: tuple[int, ...]
) -> tuple[Allocation, ...]:
    return tuple(
        Allocation(
            id=alloc.id,
            size=alloc.size,
            start=tuple(alloc.start[lane] for lane in lanes),
            end=tuple(alloc.end[lane] for lane in lanes),
        )
        for alloc in allocations
    )


def _optimum(allocations: tuple[Allocation, ...]) -> int:
    return min(
        _peak(allocate([allocations[i] for i in order], "greedy"))
        for order in itertools.permutations(range(len(allocations)))
    )


def _tiny_instance(rng: random.Random, count: int = 6) -> tuple[Allocation, ...]:
    allocations = []
    for i in range(count):
        start = rng.randint(0, 6)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 50),
                start=start,
                end=start + rng.randint(1, 4),
            )
        )
    return tuple(allocations)


def test_every_allocator_places_a_valid_pool() -> None:
    for seed in SEEDS[:3]:
        for name, allocations in _source_instances(seed).items():
            pool = Pool(id=name, allocations=allocations)
            for allocator in _allocators():
                if not allocator.supports(allocations):
                    continue
                validate_allocation(allocate(pool, allocator))


def test_every_allocator_places_generated_instances_validly() -> None:
    for seed in SEEDS:
        for allocations in (_scalar_instance(seed), _vector_instance(seed)):
            for allocator in _allocators():
                if not allocator.supports(allocations):
                    continue
                validate_allocation(allocate(allocations, allocator))


def test_placement_pressure_never_drops_below_the_antichain_bound() -> None:
    for seed in SEEDS:
        for allocations in (_scalar_instance(seed), _vector_instance(seed)):
            bound = antichain_pressure(allocations, work_budget=None)
            for allocator in _allocators():
                if not allocator.supports(allocations):
                    continue
                placed = allocate(allocations, allocator)
                assert placement_pressure(placed) >= bound


def test_closure_bound_stays_under_the_antichain_bound_and_the_peak() -> None:
    for seed in SEEDS:
        for allocations in (_scalar_instance(seed), _vector_instance(seed)):
            closure = closure_pressure(allocations, closure_cap=None)
            antichain = antichain_pressure(allocations, work_budget=None)
            peak = _peak(allocate(allocations, "omni"))
            assert closure <= antichain <= peak


def test_placement_preserves_ids_sizes_and_lifetimes() -> None:
    for seed in SEEDS:
        for allocations in (_scalar_instance(seed), _vector_instance(seed)):
            expected = {a.id: (a.size, a.start, a.end) for a in allocations}
            for allocator in _allocators():
                if not allocator.supports(allocations):
                    continue
                placed = allocate(allocations, allocator)
                assert {a.id: (a.size, a.start, a.end) for a in placed} == expected


def test_placement_leaves_no_allocation_unplaced() -> None:
    for seed in SEEDS:
        for allocations in (_scalar_instance(seed), _vector_instance(seed)):
            for allocator in _allocators():
                if not allocator.supports(allocations):
                    continue
                placed = allocate(allocations, allocator)
                assert len(placed) == len(allocations)
                assert all(a.offset is not None and a.offset >= 0 for a in placed)


def test_repeated_calls_on_one_instance_return_identical_offsets() -> None:
    for allocations in (_scalar_instance(1), _vector_instance(1)):
        for allocator in _allocators():
            if not allocator.supports(allocations):
                continue
            first = allocate(allocations, allocator)
            second = allocate(allocations, allocator)
            assert _offsets(first) == _offsets(second)


def test_a_fresh_instance_returns_the_offsets_of_a_reused_one() -> None:
    for allocations in (_scalar_instance(2), _vector_instance(2)):
        for allocator_cls in _allocator_classes():
            try:
                allocator = allocator_cls()
            except ImportError:
                continue
            if not allocator.supports(allocations):
                continue
            reused = allocate(allocations, allocator)
            fresh = allocate(allocations, allocator_cls())
            assert _offsets(reused) == _offsets(fresh)


def test_a_placed_result_keeps_its_offsets_when_allocated_again() -> None:
    allocations = _scalar_instance(3)
    placed = allocate(allocations, "omni")
    assert _offsets(allocate(placed, "omni")) == _offsets(placed)


def test_reordering_the_input_keeps_the_omni_peak_on_random_instances() -> None:
    for seed in PERMUTATION_SEEDS:
        allocations = _scalar_instance(seed)
        shuffled = _shuffled(allocations, seed + 100)
        assert _peak(allocate(shuffled, "omni")) == _peak(allocate(allocations, "omni"))


def test_reordering_the_input_can_change_the_omni_peak() -> None:
    reordered = tuple(REORDERING_SENSITIVE[i] for i in (2, 1, 3, 0))
    original = allocate(REORDERING_SENSITIVE, "omni")
    permuted = allocate(reordered, "omni")
    validate_allocation(permuted)
    assert _peak(permuted) > _peak(original)
    assert _peak(original) == antichain_pressure(REORDERING_SENSITIVE, work_budget=None)


def test_reordering_the_input_changes_the_greedy_peak() -> None:
    for seed in PERMUTATION_SEEDS:
        allocations = _scalar_instance(seed)
        shuffled = _shuffled(allocations, seed + 100)
        assert _peak(allocate(shuffled, "greedy")) != _peak(
            allocate(allocations, "greedy")
        )


def test_zero_thread_padding_preserves_the_conflict_relation() -> None:
    for seed in SEEDS:
        allocations = _vector_instance(seed)
        padded = _padded(allocations)
        assert conflicts(padded, None) == conflicts(allocations, None)
        assert conflict_degrees(padded, None) == conflict_degrees(allocations, None)


def test_zero_thread_padding_preserves_the_pressures_and_the_peak() -> None:
    for seed in SEEDS:
        allocations = _vector_instance(seed)
        padded = _padded(allocations)
        assert antichain_pressure(padded, work_budget=None) == antichain_pressure(
            allocations, work_budget=None
        )
        assert _peak(allocate(padded, "omni")) == _peak(allocate(allocations, "omni"))


def test_thread_permutation_preserves_the_conflict_relation() -> None:
    lanes = (2, 0, 1)
    for seed in SEEDS:
        allocations = _vector_instance(seed)
        permuted = _lane_permuted(allocations, lanes)
        assert conflicts(permuted, None) == conflicts(allocations, None)


def test_thread_permutation_preserves_the_pressure_bounds() -> None:
    lanes = (2, 0, 1)
    for seed in SEEDS:
        allocations = _vector_instance(seed)
        permuted = _lane_permuted(allocations, lanes)
        assert antichain_pressure(permuted, work_budget=None) == antichain_pressure(
            allocations, work_budget=None
        )
        assert closure_pressure(permuted, closure_cap=None) == closure_pressure(
            allocations, closure_cap=None
        )


def test_thread_permutation_keeps_the_omni_peak_under_every_relabelling() -> None:
    for seed in SEEDS:
        allocations = _vector_instance(seed)
        peak = _peak(allocate(allocations, "omni"))
        for lanes in itertools.permutations(range(3)):
            permuted = _lane_permuted(allocations, lanes)
            assert _peak(allocate(permuted, "omni")) == peak


def test_thread_permutation_keeps_the_peak_of_a_lane_sensitive_instance() -> None:
    swapped = _lane_permuted(LANE_SENSITIVE, (1, 0))
    assert conflicts(swapped, None) == conflicts(LANE_SENSITIVE, None)
    original = allocate(LANE_SENSITIVE, "omni")
    permuted = allocate(swapped, "omni")
    validate_allocation(permuted)
    assert _peak(permuted) == _peak(original)
    assert _peak(original) >= antichain_pressure(LANE_SENSITIVE, work_budget=None)


def test_thread_permutation_keeps_the_omni_peak_on_a_sync_pattern() -> None:
    allocations = SyncPatternSource(
        num_allocations=48, num_threads=4, pattern="tree", seed=3
    ).get_allocations()
    peak = _peak(allocate(allocations, "omni"))
    for lanes in itertools.permutations(range(4)):
        permuted = _lane_permuted(allocations, lanes)
        placed = allocate(permuted, "omni")
        validate_allocation(placed)
        assert _peak(placed) == peak


def test_uniform_size_scaling_scales_the_peak() -> None:
    for factor in (3, 11):
        for seed in SEEDS:
            for allocations in (_scalar_instance(seed), _vector_instance(seed)):
                scaled = _scaled(allocations, factor)
                assert _peak(allocate(scaled, "omni")) == factor * _peak(
                    allocate(allocations, "omni")
                )


def test_uniform_size_scaling_scales_the_pressure() -> None:
    for factor in (3, 11):
        for seed in SEEDS:
            for allocations in (_scalar_instance(seed), _vector_instance(seed)):
                scaled = _scaled(allocations, factor)
                assert antichain_pressure(
                    scaled, work_budget=None
                ) == factor * antichain_pressure(allocations, work_budget=None)
                assert closure_pressure(
                    scaled, closure_cap=None
                ) == factor * closure_pressure(allocations, closure_cap=None)


def test_time_dilation_preserves_the_conflict_relation() -> None:
    for factor in (2, 7):
        for seed in SEEDS:
            allocations = _vector_instance(seed)
            dilated = _dilated(allocations, factor)
            assert conflicts(dilated, None) == conflicts(allocations, None)


def test_time_dilation_preserves_the_peak() -> None:
    for factor in (2, 7):
        for seed in SEEDS:
            allocations = _vector_instance(seed)
            dilated = _dilated(allocations, factor)
            assert _peak(allocate(dilated, "omni")) == _peak(
                allocate(allocations, "omni")
            )


def test_removing_an_allocation_cannot_raise_the_optimum() -> None:
    rng = random.Random(11)
    for _ in range(5):
        allocations = _tiny_instance(rng)
        optimum = _optimum(allocations)
        for i in range(len(allocations)):
            reduced = allocations[:i] + allocations[i + 1 :]
            assert _optimum(reduced) <= optimum


def test_adding_an_allocation_cannot_lower_the_bound() -> None:
    for seed in SEEDS:
        allocations = _scalar_instance(seed, count=20)
        bound = antichain_pressure(allocations, work_budget=None)
        for i in range(len(allocations)):
            reduced = allocations[:i] + allocations[i + 1 :]
            assert antichain_pressure(reduced, work_budget=None) <= bound


def test_linearization_preserves_the_conflict_relation() -> None:
    for seed in SEEDS:
        allocations = _lockstep_instance(seed)
        linearized = try_linearize(allocations, work_budget=None)
        assert linearized is not None
        assert conflicts(tuple(linearized), None) == conflicts(allocations, None)


def test_linearization_preserves_the_antichain_pressure() -> None:
    for seed in SEEDS:
        allocations = _lockstep_instance(seed)
        linearized = try_linearize(allocations, work_budget=None)
        assert linearized is not None
        assert antichain_pressure(
            tuple(linearized), work_budget=None
        ) == antichain_pressure(allocations, work_budget=None)


def test_linearization_of_a_staircase_preserves_conflicts_and_pressure() -> None:
    allocations = _staircase_instance()
    linearized = try_linearize(allocations, work_budget=None)
    assert linearized is not None
    assert all(alloc.dim == 1 for alloc in linearized)
    assert conflicts(tuple(linearized), None) == conflicts(allocations, None)
    assert antichain_pressure(
        tuple(linearized), work_budget=None
    ) == antichain_pressure(allocations, work_budget=None)


def test_scaling_padding_and_dilation_compose_into_one_scaled_instance() -> None:
    factor = 5
    for seed in SEEDS:
        allocations = _vector_instance(seed)
        derived = _scaled(_padded(_dilated(allocations, 3)), factor)
        assert conflict_degrees(derived, None) == conflict_degrees(allocations, None)
        assert antichain_pressure(
            derived, work_budget=None
        ) == factor * antichain_pressure(allocations, work_budget=None)
        assert closure_pressure(derived, closure_cap=None) == factor * closure_pressure(
            allocations, closure_cap=None
        )
        assert _peak(allocate(derived, "omni")) == factor * _peak(
            allocate(allocations, "omni")
        )


def test_every_allocator_stays_between_the_bound_and_the_total_size() -> None:
    for seed in SEEDS[:2]:
        for allocations in (
            TilingSource(num_allocations=64, seed=seed).get_allocations(),
            PinwheelSource(num_allocations=65, seed=seed).get_allocations(),
            SyncPatternSource(
                num_allocations=40, num_threads=3, pattern="barrier", seed=seed
            ).get_allocations(),
        ):
            bound = antichain_pressure(allocations, work_budget=None)
            total = sum(alloc.size for alloc in allocations)
            graph = conflict_graph(allocations, None)
            assert len(graph) == len(allocations)
            for allocator in _allocators():
                if not allocator.supports(allocations):
                    continue
                placed = allocate(allocations, allocator)
                validate_allocation(placed)
                assert bound <= _peak(placed) <= total
