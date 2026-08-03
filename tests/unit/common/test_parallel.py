#
# SPDX-License-Identifier: Apache-2.0
#

import os
from collections.abc import Iterator

import pytest
from omnimalloc import Allocation, allocate
from omnimalloc.allocators.greedy_base import _run_in_pool, _worker_ceiling
from omnimalloc.analysis import placement_pressure
from omnimalloc.common.parallel import (
    adopt_max_threads,
    available_cores,
    ensure_valid_num_threads,
    max_threads,
    resolve_num_threads,
    set_max_threads,
)


class _CeilingProbe:
    def allocate(self, allocations: tuple[Allocation, ...]) -> tuple[Allocation, ...]:
        return (allocations[0].with_offset(max_threads()),)


@pytest.fixture(autouse=True)
def restore_max_threads() -> Iterator[None]:
    original = max_threads()
    yield
    set_max_threads(original)


def test_available_cores_is_positive() -> None:
    assert available_cores() >= 1


def test_available_cores_follows_the_affinity_mask() -> None:
    if not hasattr(os, "sched_getaffinity"):
        pytest.skip("no affinity support on this platform")
    assert available_cores() == len(os.sched_getaffinity(0))


def test_available_cores_never_exceeds_the_machine() -> None:
    assert available_cores() <= (os.cpu_count() or 1)


def test_explicit_thread_count_passes_through() -> None:
    assert resolve_num_threads(3) == 3


def test_none_resolves_to_the_ceiling() -> None:
    assert resolve_num_threads(None) == max_threads()


def test_non_positive_thread_count_rejected() -> None:
    with pytest.raises(ValueError, match="num_threads must be positive"):
        ensure_valid_num_threads(0)
    with pytest.raises(ValueError, match="num_threads must be positive"):
        resolve_num_threads(-1)


def test_default_ceiling_is_eight_or_the_usable_cores() -> None:
    assert max_threads() == min(8, available_cores())


def test_ceiling_takes_the_value_set() -> None:
    set_max_threads(2)
    assert max_threads() == 2
    assert resolve_num_threads(None) == 2


def test_none_lifts_the_ceiling_to_the_usable_cores() -> None:
    set_max_threads(None)
    assert max_threads() == available_cores()


def test_ceiling_never_exceeds_the_usable_cores() -> None:
    set_max_threads(4096)
    assert max_threads() == available_cores()


def test_adopt_max_threads_sets_the_ceiling_of_a_worker_process() -> None:
    adopt_max_threads(3)
    assert max_threads() == min(3, available_cores())


def test_worker_ceiling_splits_the_pool_share() -> None:
    set_max_threads(8)
    assert _worker_ceiling(4) == 2
    assert _worker_ceiling(8) == 1
    assert _worker_ceiling(64) == 1


def test_pool_workers_run_under_the_split_ceiling() -> None:
    set_max_threads(8)
    allocations = (Allocation(id=0, size=1, start=0, end=1),)
    results, stranded = _run_in_pool(allocations, [_CeilingProbe()] * 2, workers=2)
    assert not stranded
    assert [placed[0].offset for placed in results] == [min(4, available_cores())] * 2


def test_non_positive_ceiling_rejected() -> None:
    with pytest.raises(ValueError, match="max threads must be positive"):
        set_max_threads(0)


def test_an_explicit_thread_count_still_overrides_the_ceiling() -> None:
    set_max_threads(2)
    assert resolve_num_threads(16) == 16


def test_the_ceiling_does_not_change_the_placement() -> None:
    allocations = tuple(
        Allocation(
            id=i, size=1 + i % 13, start=(i % 40, i % 7), end=(i % 40 + 3, i % 7 + 2)
        )
        for i in range(1500)
    )
    set_max_threads(1)
    serial = allocate(allocations, "omni")
    set_max_threads(None)
    parallel = allocate(allocations, "omni")
    assert [a.offset for a in serial] == [a.offset for a in parallel]
    assert placement_pressure(serial) == placement_pressure(parallel)
