#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import omnimalloc as om
from omnimalloc.allocators import DEFAULT_ALLOCATOR, available_allocators
from omnimalloc.allocators.minimalloc import HAS_MINIMALLOC


def main() -> None:
    example_dir = Path("03_example_output")

    # Define allocations with temporal bounds
    alloc_0 = om.Allocation(id="alloc_0", size=5, start=0, end=10)
    alloc_1 = om.Allocation(id="alloc_1", size=5, start=12, end=20)
    alloc_2 = om.Allocation(id="alloc_2", size=4, start=5, end=15)
    alloc_3 = om.Allocation(id="alloc_3", size=5, start=15, end=23)

    # Create pool and allocate
    pool = om.Pool(id="pool_0", allocations=(alloc_0, alloc_1, alloc_2, alloc_3))

    # Get and run the default allocator
    print(f"Running allocation with default allocator: {DEFAULT_ALLOCATOR}")
    pool = om.allocate(pool, allocator=DEFAULT_ALLOCATOR, validate=True)
    print(f"Pool {pool.id!r} size: {pool.size}")
    om.plot_allocation(pool, example_dir / f"{DEFAULT_ALLOCATOR}_default.pdf")

    # Run allocation with all available allocators
    for allocator_name in available_allocators():
        # minimalloc is an optional dependency that only builds on some platforms
        if "minimalloc" in allocator_name and not HAS_MINIMALLOC:
            print(f"Skipping unavailable allocator: {allocator_name}")
            continue
        print(f"Running allocation with allocator: {allocator_name}")
        pool = om.allocate(pool, allocator_name, validate=True)
        print(f"Pool {pool.id!r} size: {pool.size}")
        om.plot_allocation(pool, example_dir / f"{allocator_name}.pdf")


if __name__ == "__main__":
    main()
