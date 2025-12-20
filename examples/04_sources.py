#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

from omnimalloc import plot_allocation, run_allocation
from omnimalloc.benchmark.sources import (
    get_available_sources,
    get_default_source,
    get_source_by_name,
)

example_dir = Path("04_example_output")

# Get and use the default source
default_source_name = get_default_source()
default_source_class = get_source_by_name(default_source_name)
default_source = default_source_class()
print(f"Using default source: {default_source_name}")

# Get allocations from the default source
pool = default_source.get_pool()
pool = run_allocation(pool, validate=True)
print(f"Pool {pool.id!r} size: {pool.size}")
plot_allocation(pool, example_dir / f"source_{default_source_name}_default.pdf")

for source_name in get_available_sources():
    source_class = get_source_by_name(source_name)
    source = source_class()
    print(f"Using source: {source_name}")

    pool = source.get_pool()
    pool = run_allocation(pool, validate=True)
    print(f"Pool {pool.id!r} size: {pool.size}")
    plot_allocation(pool, example_dir / f"source_{source_name}.pdf")
