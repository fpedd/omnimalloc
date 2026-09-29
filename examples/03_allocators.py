#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import omnimalloc as om
from omnimalloc.allocators import DEFAULT_ALLOCATOR, available_allocators


def main() -> None:
    example_dir = Path("03_example_output")

    # Define allocations with temporal bounds
    alloc_0 = om.Allocation(id="alloc_0", size=5, start=0, end=10)
    alloc_1 = om.Allocation(id="alloc_1", size=5, start=12, end=20)
    alloc_2 = om.Allocation(id="alloc_2", size=4, start=5, end=15)
    alloc_3 = om.Allocation(id="alloc_3", size=5, start=15, end=23)

    # Create pool
    pool = om.Pool(id="pool_0", allocations=(alloc_0, alloc_1, alloc_2, alloc_3))

    # Run allocation with every registered allocator; without one,
    # om.allocate uses the default
    print(f"Default allocator: {DEFAULT_ALLOCATOR}")
    for allocator_name in available_allocators():
        try:
            placed = om.allocate(pool, allocator_name, validate=True)
        except ImportError as error:
            # Optional allocators wrap libraries that may not be installed
            print(f"Skipping {allocator_name}: {str(error).splitlines()[0]}")
            continue
        print(f"Pool {placed.id!r} size with {allocator_name}: {placed.size}")
        om.plot_allocation(placed, example_dir / f"{allocator_name}.pdf")


if __name__ == "__main__":
    main()
