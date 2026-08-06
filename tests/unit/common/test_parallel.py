#
# SPDX-License-Identifier: Apache-2.0
#

import os
from collections.abc import Iterator

import pytest
from omnimalloc import Allocation, allocate
from omnimalloc.analysis import placement_pressure
from omnimalloc.common.parallel import (
    available_cores,
    max_threads,
    resolve_num_threads,
    set_max_threads,
)


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
    with pytest.raises(ValueError, match="num_threads must be positive or None"):
        resolve_num_threads(0)
    with pytest.raises(ValueError, match="num_threads must be positive or None"):
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
