#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

from omnimalloc import (
    Allocation,
    Pool,
    plot_allocation,
    run_allocation,
)
from omnimalloc.allocators import get_available_allocators, get_default_allocator

example_dir = Path("03_example_output")

# Define allocations with temporal bounds
alloc_0 = Allocation(id="alloc_0", size=5, start=0, end=10)
alloc_1 = Allocation(id="alloc_1", size=5, start=12, end=20)
alloc_2 = Allocation(id="alloc_2", size=4, start=5, end=15)
alloc_3 = Allocation(id="alloc_3", size=5, start=15, end=23)

# Create pool and allocate
pool = Pool(id="pool_0", allocations=(alloc_0, alloc_1, alloc_2, alloc_3))

# Get and run the default allocator
default_allocator_name = get_default_allocator()
print(f"Running allocation with default allocator: {default_allocator_name}")
pool = run_allocation(pool, allocator=default_allocator_name, validate=True)
print(f"Pool {pool.id!r} size: {pool.size}")
plot_allocation(pool, example_dir / f"allocation_{default_allocator_name}_default.pdf")

# Run allocation with all available allocators
for allocator_name in get_available_allocators():
    print(f"Running allocation with allocator: {allocator_name}")
    pool = run_allocation(pool, allocator_name, validate=True)

    print(f"Pool {pool.id!r} size: {pool.size}")
    plot_allocation(pool, example_dir / f"allocation_{allocator_name}.pdf")
