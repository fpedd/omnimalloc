#
# SPDX-License-Identifier: Apache-2.0
#

from dataclasses import replace

import pytest
from omnimalloc import allocate, validate_allocation
from omnimalloc.benchmark.sources import SkewedSource, SyncPatternSource
from omnimalloc.primitives import Allocation, Memory, Pool, System

KINDS = ("workspace", "constant", "input")


def _pool(memory_id: str, kind: str, count: int, seed: int) -> Pool:
    source = SkewedSource(num_allocations=count, distribution="bimodal", seed=seed)
    allocations = tuple(
        Allocation(
            id=f"{memory_id}_{kind}_{a.id}", size=a.size, start=a.start, end=a.end
        )
        for a in source.get_allocations()
    )
    return Pool(id=f"{memory_id}_{kind}", allocations=allocations)


def _accelerator() -> System:
    memories = []
    for index, (memory_id, capacity) in enumerate(
        (
            ("l1_0", 4 << 20),
            ("l1_1", 4 << 20),
            ("l1_2", 4 << 20),
            ("l1_3", 4 << 20),
            ("l2", 32 << 20),
            ("l3", 1 << 30),
        )
    ):
        pools = tuple(
            _pool(memory_id, kind, 40 + 10 * k, seed=index * 10 + k)
            for k, kind in enumerate(KINDS)
        )
        memories.append(Memory(id=memory_id, pools=pools, size=capacity))
    return System(id="accelerator", memories=tuple(memories))


def test_a_six_memory_system_places_and_validates() -> None:
    placed = allocate(_accelerator(), "omni", validate=True)
    assert len(placed.memories) == 6
    assert all(len(memory.pools) == len(KINDS) for memory in placed.memories)
    validate_allocation(placed, require_capacity=True)


def test_every_pool_of_a_memory_gets_a_distinct_base() -> None:
    placed = allocate(_accelerator(), "omni")
    for memory in placed.memories:
        bases = [pool.offset for pool in memory.pools]
        assert len(set(bases)) == len(bases)
        assert all(base is not None for base in bases)


def test_pool_bases_stack_without_gaps() -> None:
    placed = allocate(_accelerator(), "omni")
    for memory in placed.memories:
        cursor = 0
        for pool in memory.pools:
            assert pool.offset == cursor
            cursor += pool.size
        assert memory.extent == cursor


def test_a_pinned_pool_base_survives_placement() -> None:
    system = _accelerator()
    memory = system.memories[4]
    pinned = replace(memory.pools[0], offset=1 << 20)
    system = system.with_memories(
        (*system.memories[:4], memory.with_pools((pinned, *memory.pools[1:])))
    )
    placed = allocate(system, "omni")
    pools = placed.memories[4].pools
    assert pools[0].offset == 1 << 20
    assert min(pool.offset for pool in pools) == 0
    validate_allocation(placed, require_capacity=True)


def test_an_undersized_memory_is_caught_in_the_system() -> None:
    system = _accelerator()
    starved = replace(system.memories[0], size=1024)
    system = system.with_memories((starved, *system.memories[1:]))
    with pytest.raises(ValueError, match=r"in memory 'l1_0'.*exceeds memory size"):
        allocate(system, "omni", validate=True)


def test_an_id_reused_across_pools_of_one_memory_is_caught() -> None:
    system = _accelerator()
    memory = system.memories[0]
    clash = memory.pools[0].allocations[0]
    second = memory.pools[1]
    colliding = second.with_allocations((clash, *second.allocations[1:]))
    system = system.with_memories(
        (
            memory.with_pools((memory.pools[0], colliding, *memory.pools[2:])),
            *system.memories[1:],
        )
    )
    with pytest.raises(ValueError, match="duplicate allocation id"):
        allocate(system, "omni", validate=True)


def test_a_vector_clock_system_places_and_validates() -> None:
    memories = []
    for index in range(4):
        pools = tuple(
            Pool(
                id=f"core{index}_{kind}",
                allocations=tuple(
                    Allocation(
                        id=f"core{index}_{kind}_{a.id}",
                        size=a.size,
                        start=a.start,
                        end=a.end,
                    )
                    for a in SyncPatternSource(
                        num_allocations=60,
                        num_threads=17,
                        pattern="tree",
                        size_distribution="bimodal",
                        seed=index * 3 + k,
                    ).get_allocations()
                ),
            )
            for k, kind in enumerate(KINDS)
        )
        memories.append(Memory(id=f"core{index}", pools=pools))
    placed = allocate(System(id="npu", memories=tuple(memories)), "omni", validate=True)
    assert placed.is_allocated


def test_a_placed_system_keeps_its_offsets_when_allocated_again() -> None:
    placed = allocate(_accelerator(), "omni")
    assert allocate(placed, "omni") == placed
    validate_allocation(placed)
