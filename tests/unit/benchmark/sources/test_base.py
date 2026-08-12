#
# SPDX-License-Identifier: Apache-2.0
#


import pytest
from omnimalloc.benchmark.sources.base import BaseSource
from omnimalloc.benchmark.sources.generator import RandomSource
from omnimalloc.primitives import Allocation


class ProbeSource(BaseSource):
    def get_allocations(
        self, num_allocations: int | None = None, skip: int = 0
    ) -> tuple[Allocation, ...]:
        count = num_allocations or self.num_allocations
        return tuple(
            Allocation(id=i, size=1, start=0, end=1) for i in range(skip, count)
        )


class LabelledProbeSource(ProbeSource):
    _label_fields = ("num_pools", "num_memories")


class OptionalLabelProbeSource(ProbeSource):
    _label_fields = ("tag",)

    def __init__(self, tag: str | None = None) -> None:
        super().__init__()
        self.tag = tag


def test_base_source_initialization_with_defaults() -> None:
    source = RandomSource()
    assert source.num_allocations == 100
    assert source.num_pools == 1
    assert source.num_memories == 1
    assert source.num_systems == 1


def test_base_source_counts_are_plain_assignable_attributes() -> None:
    source = RandomSource()
    source.num_allocations = 7
    source.num_pools = 6
    source.num_memories = 5
    source.num_systems = 4
    assert (source.num_allocations, source.num_pools) == (7, 6)
    assert (source.num_memories, source.num_systems) == (5, 4)


def test_base_source_constructor_validates_num_allocations() -> None:
    with pytest.raises(ValueError, match="num_allocations must be positive"):
        ProbeSource(num_allocations=0)


def test_base_source_constructor_validates_num_pools() -> None:
    with pytest.raises(ValueError, match="num_pools must be positive"):
        ProbeSource(num_pools=0)


def test_base_source_constructor_validates_num_memories() -> None:
    with pytest.raises(ValueError, match="num_memories must be positive"):
        ProbeSource(num_memories=0)


def test_base_source_constructor_validates_num_systems() -> None:
    with pytest.raises(ValueError, match="num_systems must be positive"):
        ProbeSource(num_systems=0)


def test_base_source_is_parameterizable() -> None:
    assert RandomSource().is_parameterizable() is True


def test_base_source_get_allocation() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    allocation = source.get_allocation()

    assert allocation.id == 0
    assert allocation.size > 0
    assert allocation.start >= 0
    assert allocation.end > allocation.start


def test_base_source_get_pool() -> None:
    source = RandomSource(num_allocations=5, seed=42)
    pool = source.get_pool()

    assert pool.id == "random_pool_0"
    assert len(pool.allocations) == 5


def test_base_source_get_pools() -> None:
    source = RandomSource(num_allocations=5, seed=42)
    pools = source.get_pools(num_pools=3)

    assert len(pools) == 3
    assert all(len(pool.allocations) == 5 for pool in pools)
    assert pools[0].id == "random_pool_0"
    assert pools[1].id == "random_pool_1"
    assert pools[2].id == "random_pool_2"


def test_base_source_get_memory() -> None:
    source = RandomSource(num_allocations=5, seed=42)
    memory = source.get_memory()

    assert memory.id == "random_memory_0"
    assert len(memory.pools) == 1


def test_base_source_get_memories() -> None:
    source = RandomSource(num_allocations=5, seed=42)
    memories = source.get_memories(num_memories=2)

    assert len(memories) == 2
    assert memories[0].id == "random_memory_0"
    assert memories[1].id == "random_memory_1"


def test_base_source_get_system() -> None:
    source = RandomSource(num_allocations=5, seed=42)
    system = source.get_system()

    assert system.id == "random_system_0"
    assert len(system.memories) == 1


def test_base_source_get_systems() -> None:
    source = RandomSource(num_allocations=5, seed=42)
    systems = source.get_systems(num_systems=2)

    assert len(systems) == 2
    assert systems[0].id == "random_system_0"
    assert systems[1].id == "random_system_1"


def test_base_source_get_variant_with_int() -> None:
    source = RandomSource(num_allocations=10, seed=42)
    pool = source.get_variant(5)

    assert len(pool.allocations) == 5
    assert source.num_allocations == 10


def test_base_source_get_variant_with_str_raises_error() -> None:
    source = RandomSource(num_allocations=10, seed=42)

    with pytest.raises(ValueError, match="does not support variant ID"):
        source.get_variant("model_name")


def test_base_source_get_pools_with_skip() -> None:
    source = RandomSource(num_allocations=5, seed=42)
    pools_no_skip = source.get_pools(num_pools=2)
    pools_with_skip = source.get_pools(num_pools=2, skip=2)

    assert len(pools_with_skip) == 2
    assert pools_with_skip[0].allocations != pools_no_skip[0].allocations


def test_base_source_hierarchical_structure() -> None:
    source = RandomSource(num_allocations=3, seed=42)
    source.num_pools = 2
    source.num_memories = 2

    systems = source.get_systems(num_systems=1)
    system = systems[0]

    assert len(system.memories) == 2
    assert len(system.memories[0].pools) == 2
    assert len(system.memories[0].pools[0].allocations) == 3


def test_base_source_label_defaults_to_registry_name() -> None:
    assert RandomSource().label() == "random"
    assert ProbeSource().label() == ProbeSource.name()


def test_base_source_label_appends_declared_fields() -> None:
    source = LabelledProbeSource(num_pools=2, num_memories=3)
    assert source.label() == f"{LabelledProbeSource.name()}[num_pools=2,num_memories=3]"


def test_base_source_label_omits_default_none_fields() -> None:
    assert OptionalLabelProbeSource().label() == OptionalLabelProbeSource.name()
    assert (
        OptionalLabelProbeSource(tag="x").label()
        == f"{OptionalLabelProbeSource.name()}[tag=x]"
    )


def test_base_source_label_keeps_none_departing_from_default() -> None:
    assert RandomSource(seed=None).label() == "random[seed=None]"


def test_base_source_label_separates_instances() -> None:
    assert LabelledProbeSource(num_pools=2).label() != (
        LabelledProbeSource(num_pools=4).label()
    )


def test_base_source_known_optimum_is_unknown_by_default() -> None:
    assert RandomSource(num_allocations=10, seed=42).get_known_optimum(10) is None


def test_get_memories_with_skip_returns_requested_count() -> None:
    source = RandomSource(num_allocations=5)
    memories = source.get_memories(num_memories=2, skip=1)
    assert len(memories) == 2


def test_get_systems_with_skip_returns_requested_count() -> None:
    source = RandomSource(num_allocations=5)
    systems = source.get_systems(num_systems=1, skip=1)
    assert len(systems) == 1


def test_get_pools_rejects_a_mutated_non_positive_count() -> None:
    source = RandomSource()
    source.num_pools = 0
    with pytest.raises(ValueError, match="num_pools must be positive"):
        source.get_pools()


def test_get_pools_rejects_an_explicit_zero_count() -> None:
    with pytest.raises(ValueError, match="num_pools must be positive"):
        RandomSource().get_pools(num_pools=0)
