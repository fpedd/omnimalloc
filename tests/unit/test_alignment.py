#
# SPDX-License-Identifier: Apache-2.0
#

import itertools
import pickle
import random
from pathlib import Path

import pytest
from omnimalloc.allocators import (
    BaseAllocator,
    GreedyAllocator,
    OmniAllocator,
    SupermallocAllocator,
    TelamallocAllocator,
    available_allocators,
)
from omnimalloc.benchmark.sources.base import prefix_ids
from omnimalloc.io import load_allocation, save_allocation
from omnimalloc.primitives import Allocation, Memory, Pool
from omnimalloc.validate import validate_allocation

ALIGNMENTS = (1, 1, 2, 4, 8, 16)


def _supporting_allocators() -> list[BaseAllocator]:
    instances = []
    for name in available_allocators():
        allocator = BaseAllocator.get(name)
        if not allocator.supports_alignment:
            continue
        # Mixed alignments loosen supermalloc's bound, so cap its search
        kwargs = {"timeout": 0.2} if allocator is SupermallocAllocator else {}
        try:
            instances.append(allocator(**kwargs))
        except ImportError:
            continue
    return instances


SUPPORTING = _supporting_allocators()


def _random_problem(
    seed: int, n: int, alignments: tuple[int, ...] = ALIGNMENTS
) -> tuple[Allocation, ...]:
    rng = random.Random(seed)
    allocations = []
    for i in range(n):
        start = rng.randrange(12)
        allocations.append(
            Allocation(
                id=i,
                size=rng.randint(1, 40),
                start=start,
                end=start + rng.randint(1, 6),
                alignment=rng.choice(alignments),
            )
        )
    return tuple(allocations)


def _peak(allocations: tuple[Allocation, ...]) -> int:
    return max(a.height or 0 for a in allocations)


def _reference_first_fit(order: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
    """Lowest aligned collision-free offset per allocation, by exhaustive probe."""
    placed: list[Allocation] = []
    for alloc in order:
        spans = [(p.offset or 0) + p.size for p in placed if p.conflicts_with(alloc)]
        candidates = sorted(
            {0, *(-(-end // alloc.alignment) * alloc.alignment for end in spans)}
        )
        offset = next(
            c
            for c in candidates
            if not any(alloc.with_offset(c).overlaps(p) for p in placed)
        )
        placed.append(alloc.with_offset(offset))
    return tuple(placed)


# Primitive


def test_alignment_defaults_to_one_and_stays_out_of_the_repr() -> None:
    alloc = Allocation(id=1, size=3, start=0, end=2)
    assert alloc.alignment == 1
    assert repr(alloc) == "Allocation(id=1, size=3, start=0, end=2)"


def test_alignment_round_trips_through_repr_and_with_offset() -> None:
    alloc = Allocation(id=1, size=3, start=0, end=2, alignment=4)
    assert repr(alloc) == "Allocation(id=1, size=3, start=0, end=2, alignment=4)"
    assert eval(repr(alloc)) == alloc  # noqa: S307
    assert alloc.with_offset(8).alignment == 4
    assert alloc.with_offset(8).with_offset(None) == alloc


@pytest.mark.parametrize("alignment", [0, -4, 3, 6, 12])
def test_alignment_must_be_a_positive_power_of_two(alignment: int) -> None:
    with pytest.raises(ValueError, match="alignment must be a positive power of two"):
        Allocation(id=1, size=3, start=0, end=2, alignment=alignment)


def test_alignment_takes_part_in_equality_and_hash() -> None:
    plain = Allocation(id=1, size=3, start=0, end=2)
    aligned = Allocation(id=1, size=3, start=0, end=2, alignment=4)
    assert plain != aligned
    assert aligned == Allocation(id=1, size=3, start=0, end=2, alignment=4)
    assert hash(aligned) == hash(Allocation(id=1, size=3, start=0, end=2, alignment=4))
    assert len({plain, aligned}) == 2


def test_alignment_survives_pickling_and_old_pickles_still_load() -> None:
    alloc = Allocation(id="x", size=3, start=(0, 1), end=(2, 2), offset=8, alignment=8)
    assert pickle.loads(pickle.dumps(alloc)) == alloc  # noqa: S301
    old = Allocation.__new__(Allocation)
    old.__setstate__(("x", 3, 0, 2, 4, None))
    assert old == Allocation(id="x", size=3, start=0, end=2, offset=4)


def test_misaligned_offset_constructs_but_fails_validation() -> None:
    alloc = Allocation(id=1, size=3, start=0, end=2, offset=2, alignment=4)
    with pytest.raises(ValueError, match="allocation 1 at address 2 is not 4-byte"):
        validate_allocation([alloc])
    validate_allocation([alloc.with_offset(4)])


# Validation, hierarchy, and IO


def test_validation_checks_the_address_including_the_pool_base() -> None:
    alloc = Allocation(id=1, size=3, start=0, end=2, offset=0, alignment=4)
    validate_allocation(Pool(id="p", allocations=(alloc,), offset=8))
    with pytest.raises(ValueError, match="at address 2 is not 4-byte aligned"):
        validate_allocation(Pool(id="p", allocations=(alloc,), offset=2))


def test_validation_applies_both_the_requested_and_the_own_alignment() -> None:
    alloc = Allocation(id=1, size=3, start=0, end=2, offset=4, alignment=4)
    validate_allocation([alloc], alignment=2)
    with pytest.raises(ValueError, match="is not 8-byte aligned"):
        validate_allocation([alloc], alignment=8)


def test_memory_aligns_the_bases_of_the_pools_it_stacks() -> None:
    pools = (
        Pool(id="odd", allocations=(Allocation(id=0, size=3, start=0, end=2),)),
        Pool(
            id="wide",
            allocations=(
                Allocation(id=1, size=5, start=0, end=2),
                Allocation(id=2, size=8, start=1, end=3, alignment=16),
            ),
        ),
    )
    memory = Memory(id="m", pools=pools).allocate(OmniAllocator())
    assert [pool.offset for pool in memory.pools] == [0, 16]
    validate_allocation(memory)


def test_rejects_a_pinned_pool_base_that_breaks_its_alignment() -> None:
    pool = Pool(
        id="p",
        allocations=(Allocation(id=0, size=4, start=0, end=1, alignment=8),),
        offset=3,
    )
    with pytest.raises(ValueError, match="'p' at base 3 breaks its alignment 8"):
        Memory(id="m", pools=(pool,)).allocate(OmniAllocator())
    with pytest.raises(ValueError, match="'p' at base 3 breaks its alignment 8"):
        pool.allocate(GreedyAllocator())


def test_csv_round_trips_alignment_and_omits_the_column_when_trivial(
    tmp_path: Path,
) -> None:
    aligned = Pool(id="p", allocations=_random_problem(0, 8))
    save_allocation(aligned, tmp_path / "p.csv")
    assert "alignment" in (tmp_path / "p.csv").read_text().splitlines()[0]
    assert load_allocation(tmp_path / "p.csv") == aligned

    plain = Pool(id="q", allocations=_random_problem(0, 8, alignments=(1,)))
    save_allocation(plain, tmp_path / "q.csv")
    assert (tmp_path / "q.csv").read_text().splitlines()[0] == "id,lower,upper,size"


# Allocators


def test_supporting_allocators_cover_the_first_fit_family() -> None:
    names = {allocator.name() for allocator in SUPPORTING}
    assert {"supermalloc", "omni", "greedy", "best_fit", "naive"} <= names
    assert "telamalloc" not in names


@pytest.mark.parametrize("allocator", SUPPORTING, ids=lambda a: a.name())
def test_supporting_allocator_places_every_offset_aligned(
    allocator: BaseAllocator,
) -> None:
    for seed in range(5):
        allocations = _random_problem(seed, 30)
        placed = allocator.allocate(allocations)
        validate_allocation(placed)
        assert {a.id: a.alignment for a in placed} == {
            a.id: a.alignment for a in allocations
        }


@pytest.mark.parametrize(
    "allocator",
    [a for a in SUPPORTING if a.supports_pinned],
    ids=lambda a: a.name(),
)
def test_supporting_allocator_aligns_around_pins(allocator: BaseAllocator) -> None:
    allocations = (
        Allocation(id="pinned", size=5, start=0, end=4, offset=3),
        Allocation(id="free", size=4, start=0, end=4, alignment=8),
    )
    placed = {a.id: a.offset for a in allocator.allocate(allocations)}
    assert placed == {"pinned": 3, "free": 8}


@pytest.mark.parametrize(
    "allocator",
    [a for a in SUPPORTING if a.supports_pinned and a.name() != "naive"],
    ids=lambda a: a.name(),
)
def test_padding_above_the_highest_pin_is_bounded(allocator: BaseAllocator) -> None:
    # The only aligned fit lies past 2^63: without the pins in the size bound
    # the rounding overflowed and landed the free allocation on the top pin
    step = 2**61
    pins = tuple(
        Allocation(id=k, size=1, start=0, end=2, offset=k * step) for k in range(4)
    )
    free = Allocation(id="free", size=1, start=0, end=2, alignment=step)
    # greedy_by_all reports its failed variants as one RuntimeError
    with pytest.raises(
        (ValueError, RuntimeError), match=r"exceeds int64 range|variant failed"
    ):
        allocator.allocate((*pins, free))


def test_prefix_ids_keeps_the_alignment() -> None:
    pool = Pool(
        id="p", allocations=(Allocation(id=1, size=4, start=0, end=1, alignment=8),)
    )
    assert prefix_ids(pool).allocations[0].alignment == 8


@pytest.mark.parametrize("allocator", SUPPORTING, ids=lambda a: a.name())
def test_alignment_of_already_aligned_sizes_changes_nothing(
    allocator: BaseAllocator,
) -> None:
    # Every end lands on a multiple of 8, so no rounding ever happens
    plain = tuple(
        Allocation(id=a.id, size=8 * a.size, start=a.start, end=a.end)
        for a in _random_problem(7, 20, alignments=(1,))
    )
    aligned = tuple(
        Allocation(id=a.id, size=a.size, start=a.start, end=a.end, alignment=8)
        for a in plain
    )
    plain_placed = allocator.allocate(plain)
    aligned_placed = allocator.allocate(aligned)
    if isinstance(allocator, SupermallocAllocator):
        # Racing portfolio threads may return a different optimal placement
        assert _peak(aligned_placed) == _peak(plain_placed)
    else:
        assert [a.offset for a in aligned_placed] == [a.offset for a in plain_placed]


def test_first_fit_kernel_matches_an_exhaustive_reference() -> None:
    for seed in range(20):
        allocations = _random_problem(seed, 25)
        assert GreedyAllocator().allocate(allocations) == _reference_first_fit(
            allocations
        )


def test_supermalloc_proves_the_exhaustive_aligned_optimum() -> None:
    # First-fit over every order reaches an optimum: lowering each allocation
    # in offset order to its lowest aligned fit never raises the peak
    for seed in range(25):
        allocations = _random_problem(seed, 6)
        optimum = min(
            _peak(_reference_first_fit(order))
            for order in itertools.permutations(allocations)
        )
        result = SupermallocAllocator(timeout=None).solve(allocations)
        validate_allocation(result.allocations)
        assert result.proved_optimal
        assert result.peak == optimum, seed


def test_unsupporting_allocator_rejects_alignment_up_front() -> None:
    aligned = (Allocation(id=1, size=10, start=0, end=5, alignment=4),)
    allocator = TelamallocAllocator()
    assert not allocator.supports(aligned)
    with pytest.raises(ValueError, match="telamalloc cannot honor alignments"):
        allocator.allocate(aligned)
    assert allocator.allocate((Allocation(id=1, size=10, start=0, end=5),))


def test_misaligned_pin_is_rejected_up_front() -> None:
    allocations = (Allocation(id="p", size=4, start=0, end=4, offset=2, alignment=4),)
    with pytest.raises(ValueError, match="'p' at offset 2 breaks its alignment 4"):
        OmniAllocator().allocate(allocations)


def test_misaligned_result_is_refused() -> None:
    class Sloppy(BaseAllocator):
        supports_alignment = True

        def _allocate(
            self, allocations: tuple[Allocation, ...]
        ) -> tuple[Allocation, ...]:
            return tuple(a.with_offset(1) for a in allocations)

    allocations = (Allocation(id=1, size=4, start=0, end=4, alignment=2),)
    with pytest.raises(ValueError, match="breaking its alignment 2"):
        Sloppy().allocate(allocations)
