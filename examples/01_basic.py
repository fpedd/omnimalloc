#
# SPDX-License-Identifier: Apache-2.0
#

from omnimalloc import Allocation, Pool, run_allocation

# Define allocations with temporal bounds
alloc_0 = Allocation(id="alloc_0", size=5, start=0, end=10)
alloc_1 = Allocation(id="alloc_1", size=5, start=12, end=20)
alloc_2 = Allocation(id="alloc_2", size=4, start=5, end=15)
alloc_3 = Allocation(id="alloc_3", size=5, start=15, end=23)

# Create pool and allocate
pool = Pool(id="pool_0", allocations=(alloc_0, alloc_1, alloc_2, alloc_3))
pool = run_allocation(pool, validate=True)

# View results
print(f"Pool {pool.id!r} size: {pool.size}")
for alloc in pool.allocations:
    print(f"  {alloc.id!r} offset: {alloc.offset}")
