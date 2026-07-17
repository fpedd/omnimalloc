#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.io import load_allocation, save_allocation
from omnimalloc.primitives import Allocation, Memory, Pool, System


def make_pool(pool_id: str = "p0") -> Pool:
    allocations = (
        Allocation(id="a", size=4, start=0, end=3),
        Allocation(id="b", size=8, start=2, end=9),
    )
    return Pool(id=pool_id, allocations=allocations)


def make_allocated_pool(pool_id: str = "p0") -> Pool:
    allocations = (
        Allocation(id="a", size=4, start=0, end=3, offset=0),
        Allocation(id="b", size=8, start=2, end=9, offset=4),
    )
    return Pool(id=pool_id, allocations=allocations)


def test_save_pool_writes_exactly_path(tmp_path: Path) -> None:
    file_path = tmp_path / "problem.csv"
    written = save_allocation(make_pool(), file_path)
    assert written == (file_path,)
    assert file_path.read_text() == "id,lower,upper,size\na,0,3,4\nb,2,9,8\n"


def test_save_creates_missing_parent_directories(tmp_path: Path) -> None:
    file_path = tmp_path / "nested" / "dir" / "problem.csv"
    written = save_allocation(make_pool(), file_path)
    assert written == (file_path,)


def test_save_memory_writes_one_file_per_pool(tmp_path: Path) -> None:
    memory = Memory(id="mem", pools=(make_pool("p0"), make_pool("p1")))
    written = save_allocation(memory, tmp_path / "problem.csv")
    assert written == (tmp_path / "problem_p0.csv", tmp_path / "problem_p1.csv")


def test_save_system_qualifies_names_with_memory_id(tmp_path: Path) -> None:
    system = System(
        id="sys",
        memories=(
            Memory(id="m0", pools=(make_pool("p0"),)),
            Memory(id="m1", pools=(make_pool("p0"),)),
        ),
    )
    written = save_allocation(system, tmp_path / "problem.csv")
    assert written == (
        tmp_path / "problem_m0_p0.csv",
        tmp_path / "problem_m1_p0.csv",
    )


def test_save_allocated_pool_emits_offset_column(tmp_path: Path) -> None:
    (file_path,) = save_allocation(make_allocated_pool(), tmp_path / "solved.csv")
    assert file_path.read_text() == "id,lower,upper,size,offset\na,0,3,4,0\nb,2,9,8,4\n"


def test_save_unallocated_pool_omits_offset_column(tmp_path: Path) -> None:
    (file_path,) = save_allocation(make_pool(), tmp_path / "problem.csv")
    assert "offset" not in file_path.read_text()


def test_load_pool_from_csv(tmp_path: Path) -> None:
    file_path = tmp_path / "problem.csv"
    file_path.write_text("id,lower,upper,size\na,0,3,4\nb,2,9,8\n")
    pool = load_allocation(file_path)
    assert pool.id == "problem"
    assert [(a.id, a.start, a.end, a.size) for a in pool.allocations] == [
        ("a", 0, 3, 4),
        ("b", 2, 9, 8),
    ]
    assert all(a.offset is None for a in pool.allocations)


def test_load_keeps_kind_none(tmp_path: Path) -> None:
    file_path = tmp_path / "problem.csv"
    file_path.write_text("id,lower,upper,size\na,0,3,4\n")
    pool = load_allocation(file_path)
    assert pool.allocations[0].kind is None


def test_save_partially_allocated_pool_round_trips_placement(
    tmp_path: Path,
) -> None:
    pool = Pool(
        id="partial",
        allocations=(
            Allocation(id="a", size=4, start=0, end=3, offset=0),
            Allocation(id="b", size=8, start=2, end=9),
        ),
    )
    (file_path,) = save_allocation(pool, tmp_path / "partial.csv")
    assert file_path.read_text() == "id,lower,upper,size,offset\na,0,3,4,0\nb,2,9,8,\n"
    loaded = load_allocation(file_path)
    assert loaded.allocations[0].offset == 0
    assert loaded.allocations[1].offset is None


def test_load_pool_with_offset_column(tmp_path: Path) -> None:
    file_path = tmp_path / "solved.csv"
    file_path.write_text("id,lower,upper,size,offset\na,0,3,4,16\nb,2,9,8,\n")
    pool = load_allocation(file_path)
    assert pool.allocations[0].offset == 16
    assert pool.allocations[1].offset is None


def test_save_load_round_trip_preserves_problem(tmp_path: Path) -> None:
    pool = make_pool("round_trip")
    (file_path,) = save_allocation(pool, tmp_path / "problem.csv")
    loaded = load_allocation(file_path)
    assert loaded.id == "problem"
    original = [(str(a.id), a.start, a.end, a.size) for a in pool.allocations]
    restored = [(str(a.id), a.start, a.end, a.size) for a in loaded.allocations]
    assert restored == original


def test_save_load_round_trip_preserves_placement(tmp_path: Path) -> None:
    pool = make_allocated_pool()
    (file_path,) = save_allocation(pool, tmp_path / "solved.csv")
    loaded = load_allocation(file_path)
    original = [(str(a.id), a.offset) for a in pool.allocations]
    restored = [(str(a.id), a.offset) for a in loaded.allocations]
    assert restored == original


def test_save_system_round_trip(tmp_path: Path) -> None:
    system = System(
        id="sys",
        memories=(
            Memory(id="m0", pools=(make_pool("p0"), make_pool("p1"))),
            Memory(id="m1", pools=(make_pool("p0"),)),
        ),
    )
    written = save_allocation(system, tmp_path / "problem.csv")
    assert len(written) == 3
    for file_path in written:
        loaded = load_allocation(file_path)
        assert len(loaded.allocations) == 2


def test_save_memory_rejects_pool_ids_colliding_after_str(tmp_path: Path) -> None:
    memory = Memory(id="mem", pools=(make_pool(1), make_pool("1")))
    with pytest.raises(ValueError, match="unique"):
        save_allocation(memory, tmp_path / "problem.csv")


def test_save_vector_time_joins_components_with_colons(tmp_path: Path) -> None:
    pool = Pool(
        id="p0",
        allocations=(Allocation(id="a", size=4, start=(3, 0), end=(5, 2)),),
    )
    (file_path,) = save_allocation(pool, tmp_path / "problem.csv")
    assert file_path.read_text() == "id,lower,upper,size\na,3:0,5:2,4\n"


def test_save_load_round_trip_preserves_vector_time(tmp_path: Path) -> None:
    pool = Pool(
        id="p0",
        allocations=(
            Allocation(id="a", size=4, start=(3, 0), end=(5, 2)),
            Allocation(id="b", size=8, start=(0, 1), end=(2, 4)),
        ),
    )
    (file_path,) = save_allocation(pool, tmp_path / "problem.csv")
    loaded = load_allocation(file_path)
    original = [(str(a.id), a.start, a.end, a.size) for a in pool.allocations]
    restored = [(str(a.id), a.start, a.end, a.size) for a in loaded.allocations]
    assert restored == original


def test_save_scalar_files_stay_minimalloc_format(tmp_path: Path) -> None:
    (file_path,) = save_allocation(make_pool(), tmp_path / "problem.csv")
    assert ":" not in file_path.read_text()
