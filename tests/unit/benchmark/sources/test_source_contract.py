#
# SPDX-License-Identifier: Apache-2.0
#
"""The contract every seeded, generative source keeps."""

import inspect

import pytest
from omnimalloc import allocate
from omnimalloc.benchmark.sources import BaseSource, available_sources

GENERATIVE = sorted(
    name
    for name in available_sources()
    if "seed" in inspect.signature(BaseSource.get(name)).parameters
)
WITH_OPTIMUM = [
    name
    for name in GENERATIVE
    if BaseSource.get(name).get_known_optimum is not BaseSource.get_known_optimum
]
# Pinwheels grow in rings of four around a hub; the 2+2 source in groups of four
COUNTS = {"pinwheel": 33}


def _source(name: str, seed: int = 7) -> BaseSource:
    return BaseSource.get(name)(num_allocations=COUNTS.get(name, 32), seed=seed)


def _signature(source: BaseSource, **kwargs: int) -> list[tuple[object, ...]]:
    return [(a.id, a.size, a.start, a.end) for a in source.get_allocations(**kwargs)]


@pytest.mark.parametrize("name", GENERATIVE)
def test_a_source_yields_the_requested_count_numbered_from_zero(name: str) -> None:
    allocations = _source(name).get_allocations()
    assert [a.id for a in allocations] == list(range(COUNTS.get(name, 32)))


@pytest.mark.parametrize("name", GENERATIVE)
def test_a_seed_fixes_the_allocations(name: str) -> None:
    assert _signature(_source(name)) == _signature(_source(name))
    assert _signature(_source(name)) != _signature(_source(name, seed=8))


@pytest.mark.parametrize("name", GENERATIVE)
def test_consecutive_pools_differ(name: str) -> None:
    first, second = _source(name).get_pools(num_pools=2)
    assert [(a.size, a.start, a.end) for a in first.allocations] != [
        (a.size, a.start, a.end) for a in second.allocations
    ]


@pytest.mark.parametrize("name", WITH_OPTIMUM)
def test_no_allocator_beats_the_known_optimum(name: str) -> None:
    source = _source(name)
    placed = allocate(source.get_pool(), "greedy_by_size", validate=True)
    assert placed.size >= source.get_known_optimum()
