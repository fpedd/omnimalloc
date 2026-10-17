#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.io import load_allocation, save_allocation
from omnimalloc.primitives import Allocation, AllocationKind, Memory, Pool, System

PROBLEM = (
    Allocation(id="a", size=4, start=0, end=3),
    Allocation(id="b", size=8, start=2, end=9),
)
SOLVED = (PROBLEM[0].with_offset(0), PROBLEM[1].with_offset(4))
PARTIAL = (PROBLEM[0].with_offset(0), PROBLEM[1])
VECTOR = (
    Allocation(id="a", size=4, start=(3, 0), end=(5, 2)),
    Allocation(id="b", size=8, start=(0, 1), end=(2, 4)),
)
TYPED = (
    Allocation(id=7, size=10, start=0, end=5, offset=0, kind=AllocationKind.CONSTANT),
    Allocation(id="named", size=4, start=2, end=8, offset=16),
)


def _pool(pool_id: object, allocations: tuple[Allocation, ...] = PROBLEM) -> Pool:
    return Pool(id=pool_id, allocations=allocations)


@pytest.mark.parametrize(
    ("entity", "text"),
    [
        (_pool("p"), "id,lower,upper,size\na,0,3,4\nb,2,9,8\n"),
        (list(PROBLEM), "id,lower,upper,size\na,0,3,4\nb,2,9,8\n"),
        (SOLVED, "id,lower,upper,size,offset\na,0,3,4,0\nb,2,9,8,4\n"),
        (PARTIAL, "id,lower,upper,size,offset\na,0,3,4,0\nb,2,9,8,\n"),
        (VECTOR[:1], "id,lower,upper,size\na,3:0,5:2,4\n"),
    ],
)
def test_save_writes_minimalloc_csv(entity: object, text: str, tmp_path: Path) -> None:
    path = tmp_path / "problem.csv"
    assert save_allocation(entity, path) == (path,)
    assert path.read_text() == text


@pytest.mark.parametrize(
    "pool",
    [
        _pool("p"),
        _pool("p", SOLVED),
        _pool("p", PARTIAL),
        _pool("p", VECTOR),
        _pool("p", TYPED),
        Pool(id="p", allocations=SOLVED[:1], offset=4096),
    ],
)
def test_load_restores_what_save_wrote(pool: Pool, tmp_path: Path) -> None:
    (path,) = save_allocation(pool, tmp_path / "problem.csv")
    loaded = load_allocation(path)
    assert loaded.id == "problem"
    assert loaded.allocations == pool.allocations
    assert loaded.offset == pool.offset


@pytest.mark.parametrize(
    ("ids", "loaded"),
    [(["12"], [12]), (["7", "007"], [7, "007"])],
)
def test_load_reads_canonical_digit_ids_as_integers(
    ids: list[str], loaded: list[object], tmp_path: Path
) -> None:
    path = tmp_path / "pool.csv"
    save_allocation([Allocation(id=i, size=4, start=0, end=1) for i in ids], path)
    assert [a.id for a in load_allocation(path).allocations] == loaded


@pytest.mark.parametrize(
    ("entity", "name", "written"),
    [
        (_pool("p"), "nested/dir/problem.csv", ["nested/dir/problem.csv"]),
        (
            Memory(id="mem", pools=(_pool("p0"), _pool("p1"))),
            "problem.csv",
            ["problem_p0.csv", "problem_p1.csv"],
        ),
        (
            System(
                id="sys",
                memories=(
                    Memory(id="m0", pools=(_pool("p0"),)),
                    Memory(id="m1", pools=(_pool("p0"),)),
                ),
            ),
            "problem.csv",
            ["problem_m0_p0.csv", "problem_m1_p0.csv"],
        ),
    ],
)
def test_save_writes_one_file_per_pool(
    entity: object, name: str, written: list[str], tmp_path: Path
) -> None:
    paths = save_allocation(entity, tmp_path / name)
    assert paths == tuple(tmp_path / path for path in written)
    assert all(load_allocation(path).allocations == PROBLEM for path in paths)


@pytest.mark.parametrize(
    ("entity", "error", "match"),
    [
        ((1, 2), TypeError, "Expected Allocation"),
        (Memory(id="mem", pools=(_pool(1), _pool("1"))), ValueError, "unique"),
        (
            (
                Allocation(id=1, size=4, start=0, end=1),
                Allocation(id="1", size=4, start=0, end=1),
            ),
            ValueError,
            "unique after string conversion",
        ),
        (
            Memory(
                id="mem",
                pools=(
                    _pool("ok"),
                    _pool(
                        "bad",
                        (
                            Allocation(id=1, size=4, start=0, end=1),
                            Allocation(id="1", size=4, start=0, end=1),
                        ),
                    ),
                ),
            ),
            ValueError,
            "unique after string conversion",
        ),
    ],
)
def test_save_rejects_before_writing_anything(
    entity: object, error: type[Exception], match: str, tmp_path: Path
) -> None:
    with pytest.raises(error, match=match):
        save_allocation(entity, tmp_path / "problem.csv")
    assert not any(tmp_path.iterdir())
