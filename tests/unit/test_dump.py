#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import pytest
from omnimalloc.dump import dump_allocation, load_allocation
from omnimalloc.primitives import Allocation, Memory, Pool, System


def make_pool(pool_id: str = "p0") -> Pool:
    allocations = (
        Allocation(id="a", size=4, start=0, end=3),
        Allocation(id="b", size=8, start=2, end=9),
    )
    return Pool(id=pool_id, allocations=allocations)


def test_dump_pool_uses_stem_prefix(tmp_path: Path) -> None:
    file_path = tmp_path / "problem.csv"
    written = dump_allocation(make_pool(), file_path)
    assert written == (tmp_path / "problem_p0.csv",)
    assert written[0].read_text() == "id,lower,upper,size\na,0,3,4\nb,2,9,8\n"


def test_dump_path_without_suffix_uses_stem_prefix(tmp_path: Path) -> None:
    written = dump_allocation(make_pool("my_pool"), tmp_path / "problem")
    assert written == (tmp_path / "problem_my_pool.csv",)


def test_dump_creates_missing_parent_directories(tmp_path: Path) -> None:
    file_path = tmp_path / "nested" / "dir" / "problem.csv"
    written = dump_allocation(make_pool(), file_path)
    assert written == (tmp_path / "nested" / "dir" / "problem_p0.csv",)


def test_dump_memory_writes_one_file_per_pool(tmp_path: Path) -> None:
    memory = Memory(id="mem", pools=(make_pool("p0"), make_pool("p1")))
    written = dump_allocation(memory, tmp_path / "problem.csv")
    assert written == (tmp_path / "problem_p0.csv", tmp_path / "problem_p1.csv")


def test_dump_system_qualifies_names_with_memory_id(tmp_path: Path) -> None:
    system = System(
        id="sys",
        memories=(
            Memory(id="m0", pools=(make_pool("p0"),)),
            Memory(id="m1", pools=(make_pool("p0"),)),
        ),
    )
    written = dump_allocation(system, tmp_path / "problem.csv")
    assert written == (
        tmp_path / "problem_m0_p0.csv",
        tmp_path / "problem_m1_p0.csv",
    )


def test_dump_omits_offsets_of_allocated_pool(tmp_path: Path) -> None:
    allocation = Allocation(id="a", size=4, start=0, end=3, offset=16)
    pool = Pool(id="p0", allocations=(allocation,))
    (file_path,) = dump_allocation(pool, tmp_path / "problem.csv")
    assert file_path.read_text() == "id,lower,upper,size\na,0,3,4\n"


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


def test_load_pool_with_offset_column(tmp_path: Path) -> None:
    file_path = tmp_path / "solved.csv"
    file_path.write_text("id,lower,upper,size,offset\na,0,3,4,16\nb,2,9,8,\n")
    pool = load_allocation(file_path)
    assert pool.allocations[0].offset == 16
    assert pool.allocations[1].offset is None


def test_dump_load_round_trip_preserves_problem(tmp_path: Path) -> None:
    pool = make_pool("round_trip")
    (file_path,) = dump_allocation(pool, tmp_path / "problem.csv")
    loaded = load_allocation(file_path)
    assert loaded.id == "problem_round_trip"
    original = [(str(a.id), a.start, a.end, a.size) for a in pool.allocations]
    restored = [(str(a.id), a.start, a.end, a.size) for a in loaded.allocations]
    assert restored == original


def test_dump_system_round_trip(tmp_path: Path) -> None:
    system = System(
        id="sys",
        memories=(
            Memory(id="m0", pools=(make_pool("p0"), make_pool("p1"))),
            Memory(id="m1", pools=(make_pool("p0"),)),
        ),
    )
    written = dump_allocation(system, tmp_path / "problem.csv")
    assert len(written) == 3
    for file_path in written:
        loaded = load_allocation(file_path)
        assert len(loaded.allocations) == 2


def test_dump_memory_rejects_pool_ids_colliding_after_str(tmp_path: Path) -> None:
    memory = Memory(id="mem", pools=(make_pool(1), make_pool("1")))
    with pytest.raises(ValueError, match="unique"):
        dump_allocation(memory, tmp_path / "problem.csv")
