#
# SPDX-License-Identifier: Apache-2.0
#

import omnimalloc as om


def main() -> None:
    # Define allocations with temporal bounds
    alloc_0 = om.Allocation(id="alloc_0", size=5, start=0, end=10)
    alloc_1 = om.Allocation(id="alloc_1", size=5, start=12, end=20)
    alloc_2 = om.Allocation(id="alloc_2", size=4, start=5, end=15)
    alloc_3 = om.Allocation(id="alloc_3", size=5, start=15, end=23)

    # Create pool and allocate
    pool = om.Pool(id="pool_0", allocations=(alloc_0, alloc_1, alloc_2, alloc_3))
    pool = om.allocate(pool, validate=True)

    # View results
    print(f"Pool {pool.id!r} size: {pool.size}")
    for alloc in pool.allocations:
        print(f"  {alloc.id!r} offset: {alloc.offset}")

    # Visualize (requires matplotlib)
    om.plot_allocation(pool, "allocation.pdf")


if __name__ == "__main__":
    main()
