#
# SPDX-License-Identifier: Apache-2.0
#
"""Record what every public entry point does on hostile input.

Prints one row per probe rather than asserting, so the whole contract surface
reads at once and an inconsistency shows up as the odd row out.
"""

from collections.abc import Callable

from omnimalloc import Allocation, Memory, Pool, System, allocate, validate_allocation
from omnimalloc.allocators import BaseAllocator
from omnimalloc.analysis import (
    antichain_pressure,
    antichain_pressure_per_allocation,
    closure_pressure,
    conflict_degrees,
    conflict_graph,
    conflicts,
    placement_pressure,
    placement_pressure_per_allocation,
    try_linearize,
)
from omnimalloc.common.constants import KB

Probe = tuple[str, Callable[[], object]]

SCALAR = tuple(Allocation(id=i, size=KB, start=i, end=i + 3) for i in range(4))
VECTOR = tuple(
    Allocation(id=i, size=KB, start=(i, 0), end=(i + 3, 1)) for i in range(4)
)
PLACED = tuple(a.with_offset(0 if i % 2 else KB) for i, a in enumerate(SCALAR))
MIXED = (SCALAR[0], VECTOR[1])
DUPLICATE = (SCALAR[0], SCALAR[0])

# Dense enough that the budgeted entry points have something to refuse
BIG_SCALAR = tuple(Allocation(id=i, size=KB, start=0, end=2) for i in range(2000))
BIG_VECTOR = tuple(
    Allocation(id=i, size=KB, start=(i % 7, i % 5, 0), end=(i % 7 + 2, i % 5 + 2, 2))
    for i in range(2000)
)

MAX_OUTCOME = 90


def probe(call: Callable[[], object]) -> str:
    try:
        value = call()
    except Exception as e:  # noqa: BLE001
        return f"{type(e).__name__}: {str(e).splitlines()[0]}"[:160]
    text = repr(value)
    return f"ok: {text if len(text) <= MAX_OUTCOME else text[:MAX_OUTCOME] + '...'}"


def section(title: str, probes: list[Probe]) -> None:
    print(f"\n===== {title}")
    for label, call in probes:
        print(f"  {label:<50} {probe(call)}")


def allocation_probes() -> list[Probe]:
    return [
        ("size=0", lambda: Allocation(id=0, size=0, start=0, end=1)),
        ("size=-1", lambda: Allocation(id=0, size=-1, start=0, end=1)),
        ("end == start", lambda: Allocation(id=0, size=1, start=5, end=5)),
        ("end < start", lambda: Allocation(id=0, size=1, start=5, end=4)),
        ("start=-1", lambda: Allocation(id=0, size=1, start=-1, end=1)),
        ("offset=-1", lambda: Allocation(id=0, size=1, start=0, end=1, offset=-1)),
        ("size=2**63", lambda: Allocation(id=0, size=2**63, start=0, end=1)),
        (
            "offset + size overflows int64",
            lambda: Allocation(id=0, size=2**62, start=0, end=1, offset=2**62),
        ),
        ("id=None", lambda: Allocation(id=None, size=1, start=0, end=1)),
        ("id=1.5", lambda: Allocation(id=1.5, size=1, start=0, end=1)),
        ("id=''", lambda: Allocation(id="", size=1, start=0, end=1)),
        (
            "mismatched clock dims",
            lambda: Allocation(id=0, size=1, start=(0, 0), end=(1, 1, 1)),
        ),
        ("empty clock", lambda: Allocation(id=0, size=1, start=(), end=())),
        (
            "1-tuple normalizes",
            lambda: Allocation(id=0, size=1, start=(0,), end=(1,)).dim,
        ),
        (
            "scalar start, vector end",
            lambda: Allocation(id=0, size=1, start=0, end=(1, 1)),
        ),
        (
            "vector end not >= start",
            lambda: Allocation(id=0, size=1, start=(1, 1), end=(0, 2)),
        ),
        (
            "vector end == start",
            lambda: Allocation(id=0, size=1, start=(1, 1), end=(1, 1)),
        ),
        ("with_offset(-1)", lambda: SCALAR[0].with_offset(-1)),
        ("with_offset(None)", lambda: PLACED[0].with_offset(None).offset),
        ("height while unplaced", lambda: SCALAR[0].height),
        ("conflicts_with across dims", lambda: SCALAR[0].conflicts_with(VECTOR[0])),
        ("__eq__ against a non-Allocation", lambda: SCALAR[0] == 5),
        (
            "repr distinguishes 1 from '1'",
            lambda: (
                repr(Allocation(id=1, size=1, start=0, end=1))
                != repr(Allocation(id="1", size=1, start=0, end=1))
            ),
        ),
    ]


def analysis_probes() -> list[Probe]:
    return [
        ("antichain_pressure([])", lambda: antichain_pressure([])),
        ("closure_pressure([])", lambda: closure_pressure([])),
        ("conflict_degrees([])", lambda: conflict_degrees([])),
        ("conflicts([])", lambda: conflicts([])),
        ("placement_pressure([])", lambda: placement_pressure([])),
        ("try_linearize([])", lambda: try_linearize([])),
        ("antichain_pressure(mixed dims)", lambda: antichain_pressure(MIXED)),
        ("conflict_degrees(mixed dims)", lambda: conflict_degrees(MIXED)),
        ("try_linearize(mixed dims)", lambda: try_linearize(MIXED)),
        ("conflicts(duplicate ids)", lambda: conflicts(DUPLICATE)),
        ("conflict_degrees(duplicate ids)", lambda: conflict_degrees(DUPLICATE)),
        (
            "antichain_pressure_per_allocation(duplicate ids)",
            lambda: antichain_pressure_per_allocation(DUPLICATE),
        ),
        ("placement_pressure(unplaced)", lambda: placement_pressure(SCALAR)),
        (
            "placement_pressure(partly placed)",
            lambda: placement_pressure(PLACED[:2] + SCALAR[2:]),
        ),
        (
            "placement_pressure_per_allocation(unplaced)",
            lambda: placement_pressure_per_allocation(SCALAR),
        ),
        (
            "scalar antichain, work_budget=0",
            lambda: antichain_pressure(BIG_SCALAR, work_budget=0),
        ),
        (
            "scalar degrees, work_budget=0",
            lambda: max(conflict_degrees(BIG_SCALAR, work_budget=0)),
        ),
        (
            "scalar closure, closure_cap=0",
            lambda: closure_pressure(BIG_SCALAR, closure_cap=0),
        ),
        (
            "vector antichain, work_budget=0",
            lambda: antichain_pressure(BIG_VECTOR, work_budget=0),
        ),
        (
            "vector degrees, work_budget=0",
            lambda: conflict_degrees(BIG_VECTOR, work_budget=0),
        ),
        (
            "vector closure, closure_cap=0",
            lambda: closure_pressure(BIG_VECTOR, closure_cap=0),
        ),
        ("work_budget=-1", lambda: antichain_pressure(BIG_VECTOR, work_budget=-1)),
        ("closure_cap=-1", lambda: closure_pressure(BIG_VECTOR, closure_cap=-1)),
        ("conflicts at its default budget", lambda: len(conflicts(BIG_SCALAR))),
        (
            "conflict_graph(max_entries=0)",
            lambda: conflict_graph(BIG_SCALAR, max_entries=0),
        ),
        (
            "conflict_graph(max_entries=-1)",
            lambda: conflict_graph(BIG_SCALAR, max_entries=-1),
        ),
        ("neighbors(-1)", lambda: conflict_graph(SCALAR).neighbors(-1)),
        ("neighbors(past the end)", lambda: conflict_graph(SCALAR).neighbors(99)),
        ("antichain_pressure(42)", lambda: antichain_pressure(42)),
        ("antichain_pressure([1, 2, 3])", lambda: antichain_pressure([1, 2, 3])),
        (
            "antichain_pressure(generator)",
            lambda: antichain_pressure(a for a in SCALAR),
        ),
        ("conflict_degrees('abc')", lambda: conflict_degrees("abc")),
    ]


def allocate_probes() -> list[Probe]:
    colliding = tuple(a.with_offset(0) for a in SCALAR)
    return [
        ("allocate([])", lambda: allocate([])),
        ("allocate(empty Pool)", lambda: allocate(Pool(id="p", allocations=()))),
        ("allocate(empty Memory)", lambda: allocate(Memory(id="m", pools=()))),
        ("allocate(empty System)", lambda: allocate(System(id="s", memories=()))),
        ("unknown allocator name", lambda: allocate(SCALAR, "nope")),
        ("allocator that is not one", lambda: allocate(SCALAR, dict)),
        (
            "allocator as a class",
            lambda: len(allocate(SCALAR, BaseAllocator.get("omni"))),
        ),
        ("allocate(42)", lambda: allocate(42)),
        ("allocate('abc')", lambda: allocate("abc")),
        ("allocate([1, 2, 3])", lambda: allocate([1, 2, 3])),
        ("allocate(duplicate ids)", lambda: allocate(DUPLICATE)),
        ("allocate(mixed dims)", lambda: allocate(MIXED)),
        ("vector through omni", lambda: len(allocate(VECTOR, "omni"))),
        ("pins through omni", lambda: [a.offset for a in allocate(PLACED, "omni")]),
        ("pins through naive", lambda: allocate(PLACED, "naive")),
        ("pins that already collide", lambda: allocate(colliding, "omni")),
        ("validate=True", lambda: len(allocate(SCALAR, "omni", validate=True))),
        (
            "Pool in, Pool out",
            lambda: type(allocate(Pool(id="p", allocations=SCALAR))).__name__,
        ),
        ("list in, tuple out", lambda: type(allocate(list(SCALAR))).__name__),
    ]


def validate_probes() -> list[Probe]:
    overlapping = (
        Allocation(id=0, size=KB, start=0, end=2, offset=0),
        Allocation(id=1, size=KB, start=1, end=3, offset=0),
    )
    stacked = (
        Allocation(id=0, size=KB, start=0, end=2, offset=0),
        Allocation(id=1, size=KB, start=1, end=3, offset=KB),
    )
    duplicate = (
        Allocation(id="x", size=KB, start=0, end=2, offset=0),
        Allocation(id="x", size=KB, start=5, end=7, offset=0),
    )
    other_pool = (Allocation(id="z", size=KB, start=0, end=2, offset=0),)
    return [
        ("validate([])", lambda: validate_allocation([])),
        ("a clean placement", lambda: validate_allocation(stacked)),
        ("overlapping allocations", lambda: validate_allocation(overlapping)),
        ("duplicate ids", lambda: validate_allocation(duplicate)),
        ("unplaced", lambda: validate_allocation(SCALAR)),
        (
            "unplaced, require_allocated=False",
            lambda: validate_allocation(SCALAR, require_allocated=False),
        ),
        ("alignment=0", lambda: validate_allocation(stacked, alignment=0)),
        ("alignment=-8", lambda: validate_allocation(stacked, alignment=-8)),
        ("alignment honored", lambda: validate_allocation(stacked, alignment=KB)),
        ("alignment violated", lambda: validate_allocation(stacked, alignment=2 * KB)),
        ("validate(42)", lambda: validate_allocation(42)),
        ("validate([1, 2, 3])", lambda: validate_allocation([1, 2, 3])),
        (
            "no size, require_capacity=True",
            lambda: validate_allocation(
                Memory(id="m", pools=(Pool(id="p", allocations=stacked, offset=0),)),
                require_capacity=True,
            ),
        ),
        (
            "memory too small",
            lambda: validate_allocation(
                Memory(
                    id="m", size=1, pools=(Pool(id="p", allocations=stacked, offset=0),)
                )
            ),
        ),
        (
            "memory large enough",
            lambda: validate_allocation(
                Memory(
                    id="m",
                    size=1 << 30,
                    pools=(Pool(id="p", allocations=stacked, offset=0),),
                )
            ),
        ),
        (
            "unplaced pool",
            lambda: validate_allocation(
                Memory(id="m", pools=(Pool(id="p", allocations=stacked),))
            ),
        ),
        (
            "overlapping pools",
            lambda: validate_allocation(
                Memory(
                    id="m",
                    pools=(
                        Pool(id="a", allocations=stacked, offset=0),
                        Pool(id="b", allocations=other_pool, offset=1),
                    ),
                )
            ),
        ),
        (
            "ids repeated across pools",
            lambda: validate_allocation(
                Memory(
                    id="m",
                    pools=(
                        Pool(id="a", allocations=stacked, offset=0),
                        Pool(id="b", allocations=stacked, offset=1 << 20),
                    ),
                )
            ),
        ),
    ]


def hierarchy_probes() -> list[Probe]:
    return [
        ("Pool(offset=-1)", lambda: Pool(id="p", allocations=(), offset=-1)),
        ("Pool(duplicate ids)", lambda: Pool(id="p", allocations=DUPLICATE)),
        ("Pool(non-Allocation)", lambda: Pool(id="p", allocations=(1, 2))),
        ("Pool(str)", lambda: Pool(id="p", allocations="ab")),
        ("Pool.size while unallocated", lambda: Pool(id="p", allocations=SCALAR).size),
        ("Pool.size when empty", lambda: Pool(id="p", allocations=()).size),
        ("Pool.efficiency when empty", lambda: Pool(id="p", allocations=()).efficiency),
        ("Pool.pressure", lambda: Pool(id="p", allocations=SCALAR).pressure),
        (
            "Pool.antichain_pressure(mixed dims)",
            lambda: Pool(id="p", allocations=MIXED).pressure,
        ),
        ("Memory(size=-1)", lambda: Memory(id="m", size=-1, pools=())),
        (
            "Memory(duplicate pool ids)",
            lambda: Memory(
                id="m",
                pools=(Pool(id="p", allocations=()), Pool(id="p", allocations=())),
            ),
        ),
        (
            "Memory.extent while unplaced",
            lambda: Memory(id="m", pools=(Pool(id="p", allocations=PLACED),)).extent,
        ),
        (
            "System(duplicate memory ids)",
            lambda: System(
                id="s", memories=(Memory(id="m", pools=()), Memory(id="m", pools=()))
            ),
        ),
        ("System(non-Memory)", lambda: System(id="s", memories=(1,))),
        (
            "frozen dataclass assignment",
            lambda: setattr(Pool(id="p", allocations=()), "id", "q"),
        ),
    ]


def registry_probes() -> list[Probe]:
    return [
        ("get('omni')", lambda: BaseAllocator.get("omni").__name__),
        ("get('OMNI')", lambda: BaseAllocator.get("OMNI")),
        ("get('')", lambda: BaseAllocator.get("")),
        ("resolve(None)", lambda: BaseAllocator.resolve(None)),
        (
            "registry() hands out a copy",
            lambda: (
                BaseAllocator.registry().pop("omni"),
                "omni" in BaseAllocator.registry(),
            )[1],
        ),
        (
            "allocators taking vector clocks",
            lambda: sorted(
                n for n, c in BaseAllocator.registry().items() if c.supports_vector_time
            ),
        ),
        (
            "allocators honoring pins",
            lambda: sorted(
                n for n, c in BaseAllocator.registry().items() if c.supports_pinned
            ),
        ),
    ]


def main() -> None:
    section("Allocation constructor and accessors", allocation_probes())
    section("Analysis entry points", analysis_probes())
    section("allocate()", allocate_probes())
    section("validate_allocation()", validate_probes())
    section("Hierarchy primitives", hierarchy_probes())
    section("Registry", registry_probes())


if __name__ == "__main__":
    main()
