#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.primitives import Allocation, Memory, Pool, System
from omnimalloc.validate import validate_allocation


def _at(
    i: object, size: int, start: object, end: object, offset: int | None = 0
) -> Allocation:
    return Allocation(id=i, size=size, start=start, end=end, offset=offset)


def _unplaced(i: object, size: int, start: object, end: object) -> Allocation:
    return _at(i, size, start, end, offset=None)


def _pool(pool_id: object, *allocations: Allocation, base: int | None = 0) -> Pool:
    return Pool(id=pool_id, allocations=allocations, offset=base)


def _memory(memory_id: object, *pools: Pool, size: int | None = None) -> Memory:
    return Memory(id=memory_id, pools=pools, size=size)


def _system(*memories: Memory) -> System:
    return System(id=1, memories=memories)


ONE = _at(1, 10, 0, 5)
SHARED = _at("shared", 10, 0, 5)
DISJOINT = tuple(_at(i, 50, 5 * i, 5 * (i + 1)) for i in range(10))
BAD_POOL = _pool(1, _at(1, 100, 0, 10), _at(2, 100, 5, 15))


@pytest.mark.parametrize(
    ("entity", "kwargs"),
    [
        pytest.param(_pool(1, _at(1, 100, 0, 10)), {}, id="pool"),
        pytest.param(_pool(1, _at(1, 100, 0, 5), _at(2, 100, 5, 10)), {}, id="reuse"),
        pytest.param(_pool(1), {}, id="empty_pool"),
        pytest.param(_memory(1), {}, id="empty_memory"),
        pytest.param(_system(), {}, id="empty_system"),
        pytest.param(
            _memory(
                1, _pool(1, _at(1, 100, 0, 10)), _pool(2, _at(2, 100, 0, 10), base=200)
            ),
            {},
            id="memory",
        ),
        pytest.param(
            _system(_memory(1, _pool(1, ONE)), _memory(2, _pool(2, _at(2, 10, 0, 5)))),
            {},
            id="system",
        ),
        pytest.param(
            _system(
                _memory(
                    1,
                    _pool(1, _at(1, 100, 0, 5)),
                    _pool(2, _at(2, 100, 5, 10), base=200),
                ),
                _memory(2, _pool(3, _at(3, 50, 0, 10), _at(4, 75, 10, 18))),
            ),
            {},
            id="hierarchy",
        ),
        pytest.param(
            _memory("m", _pool("p", _at(1, 100, 0, 5)), size=100), {}, id="full"
        ),
        pytest.param(
            _pool(1, _at(1, 100, (0, 0), (2, 1)), _at(2, 100, (2, 1), (3, 2))),
            {},
            id="vector_happens_before_reuse",
        ),
        pytest.param((), {}, id="raw_empty"),
        pytest.param(DISJOINT, {}, id="raw"),
        pytest.param(list(DISJOINT), {}, id="raw_list"),
        pytest.param(
            _system(
                _memory("m1", _pool("p", SHARED)), _memory("m2", _pool("p", SHARED))
            ),
            {},
            id="one_id_in_two_memories",
        ),
        pytest.param(_memory("m", _pool("p", ONE)), {}, id="memory_without_size"),
        pytest.param(
            _memory("m", _pool("p", ONE), size=10),
            {"require_capacity": True},
            id="declared_capacity",
        ),
        pytest.param(
            _memory("m", _pool("p", ONE), size=1 << 50),
            {"require_capacity": True},
            id="large_declared_capacity",
        ),
        pytest.param(
            (_at(1, 64, 0, 5), _at(2, 64, 0, 5, 64)), {"alignment": 64}, id="aligned"
        ),
        pytest.param((_at(1, 10, 0, 5, 12),), {}, id="misaligned_without_alignment"),
        pytest.param(
            _memory("m", _pool("p", ONE, base=8)), {"alignment": 8}, id="base"
        ),
        pytest.param(
            _pool("p", _at(1, 10, 0, 5, 4), base=4), {"alignment": 8}, id="pool_base"
        ),
        pytest.param(
            (ONE, _unplaced(2, 100, 0, 10)),
            {"require_allocated": False},
            id="partial",
        ),
        pytest.param(
            _memory(
                "m", _pool("p1", ONE), _pool("p2", _unplaced(2, 10, 0, 5), base=None)
            ),
            {"require_allocated": False},
            id="partial_pool_bases",
        ),
        pytest.param(
            _pool("p", _at(1, 10, 0, 5, 3), base=None),
            {"require_allocated": False, "alignment": 8},
            id="partial_skips_alignment_of_baseless_pools",
        ),
        pytest.param(
            _memory("m", _pool("a", _at(1, 16, 0, 1)), _pool("b", base=8)),
            {},
            id="empty_pool_pinned_inside_another",
        ),
    ],
)
def test_validate_accepts(entity: object, kwargs: dict[str, object]) -> None:
    validate_allocation(entity, **kwargs)


@pytest.mark.parametrize(
    ("entity", "kwargs", "error", "match"),
    [
        (BAD_POOL, {}, ValueError, r"Validation .* failed.*overlaps"),
        (_pool(1, _unplaced(1, 100, 0, 10)), {}, ValueError, "is not allocated"),
        ((ONE, _unplaced(2, 100, 0, 10)), {}, ValueError, "is not allocated"),
        (
            _memory(
                1, _pool(1, _at(1, 100, 0, 10)), _pool(2, _at(2, 100, 0, 10), base=50)
            ),
            {},
            ValueError,
            r"Validation .* failed",
        ),
        (_memory(1, BAD_POOL), {}, ValueError, "in pool 1"),
        (_system(_memory(1, BAD_POOL)), {}, ValueError, "in memory 1"),
        (
            _system(_memory(1, BAD_POOL), _memory(2, _pool(2, _at(3, 50, 0, 10)))),
            {},
            ValueError,
            r"in memory 1.*in pool 1",
        ),
        (
            _memory("m", _pool("p", _unplaced(1, 10, 0, 5))),
            {},
            ValueError,
            r"^Validation of Memory 'm' failed, in pool",
        ),
        ("invalid_entity", {}, TypeError, "Unsupported entity type"),
        (ONE, {}, TypeError, "Unsupported entity type"),
        ([1, 2, 3], {}, TypeError, "Expected Allocation"),
        (
            _memory("m", _pool("p", _at(1, 100, 0, 5)), size=50),
            {},
            ValueError,
            "exceeds memory size",
        ),
        (
            _memory("m", _pool("p", ONE, base=1000), size=100),
            {},
            ValueError,
            "exceeds memory size",
        ),
        (
            _pool(1, _at(1, 100, (0, 5), (1, 6)), _at(2, 100, (2, 0), (3, 1))),
            {},
            ValueError,
            "overlaps",
        ),
        (
            _pool(1, _at(1, 100, 0, 10), _at(2, 100, (20, 0), (30, 1), 200)),
            {},
            ValueError,
            "share one clock dimension",
        ),
        (
            (_at(1, 100, 0, 10), _at(2, 100, 5, 15, 50)),
            {},
            ValueError,
            "Validation of 2 allocations failed",
        ),
        (
            (*DISJOINT, _at(99, 50, 12, 18, 25)),
            {},
            ValueError,
            "Validation of 11 allocations failed",
        ),
        ((_at(1, 10, 0, 5), _at(1, 10, 5, 10)), {}, ValueError, "duplicate id"),
        (
            _memory("m", _pool("p", ONE, base=None), size=100),
            {},
            ValueError,
            "pool 'p' is not placed",
        ),
        (
            _memory(
                "m",
                _pool("p1", ONE, base=None),
                _pool("p2", _at(2, 10, 0, 5), base=None),
            ),
            {},
            ValueError,
            "is not placed",
        ),
        (
            _memory("m", _pool("p1", SHARED), _pool("p2", SHARED, base=100)),
            {},
            ValueError,
            "duplicate allocation id 'shared'",
        ),
        (
            _memory("m", _pool("p", ONE)),
            {"require_capacity": True},
            ValueError,
            "no size declared",
        ),
        ((_at(1, 10, 0, 5, 12),), {"alignment": 64}, ValueError, "not 64-byte aligned"),
        ((ONE,), {"alignment": 0}, ValueError, "alignment must be positive"),
        (
            _system(_memory("m", _pool("p", _at(1, 10, 0, 5, 8)))),
            {"alignment": 16},
            ValueError,
            "not 16-byte aligned",
        ),
        (
            _memory("m", _pool("p", ONE, base=3)),
            {"alignment": 8},
            ValueError,
            "address 3 is not 8-byte aligned",
        ),
        (
            (_at(1, 100, 0, 10), _at(2, 100, 5, 15, 50), _unplaced(3, 100, 0, 10)),
            {"require_allocated": False},
            ValueError,
            "overlaps",
        ),
        (
            _memory("m", _pool("p1", ONE), _pool("p2", _at(2, 10, 0, 5), base=5)),
            {"require_allocated": False},
            ValueError,
            "overlaps with pool",
        ),
        (
            _pool("p", _at(1, 10, 0, 5, 3)),
            {"require_allocated": False, "alignment": 8},
            ValueError,
            "aligned",
        ),
    ],
)
def test_validate_rejects(
    entity: object, kwargs: dict[str, object], error: type[Exception], match: str
) -> None:
    with pytest.raises(error, match=match):
        validate_allocation(entity, **kwargs)


@pytest.mark.parametrize(("base", "fits"), [(90, True), (95, False), (None, True)])
def test_validate_loosened_capacity_counts_placed_pools_only(
    base: int | None, fits: bool
) -> None:
    memory = _memory(
        "m",
        _pool("p", _at(1, 10, 0, 5), base=base),
        _pool("q", _unplaced(2, 500, 0, 5), base=None),
        size=100,
    )
    if fits:
        validate_allocation(memory, require_allocated=False)
    else:
        with pytest.raises(ValueError, match="extent 105 exceeds memory size 100"):
            validate_allocation(memory, require_allocated=False)
