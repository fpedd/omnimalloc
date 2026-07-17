#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import omnimalloc as om
from omnimalloc.benchmark.sources import (
    DEFAULT_SOURCE,
    BaseSource,
    available_sources,
)


def allocate_and_plot(source: BaseSource, output: Path) -> None:
    pool = source.get_pool()
    pool = om.allocate(pool, validate=True)
    print(f"Pool {pool.id!r} size: {pool.size}")
    om.plot_allocation(pool, output)


def main() -> None:
    example_dir = Path("04_example_output")

    # Get and use the default source
    default_source = BaseSource.get(DEFAULT_SOURCE)()
    print(f"Using default source: {DEFAULT_SOURCE}")
    allocate_and_plot(
        default_source, example_dir / f"source_{DEFAULT_SOURCE}_default.pdf"
    )

    for source_name in available_sources():
        source = BaseSource.get(source_name)()
        print(f"Using source: {source_name}")
        allocate_and_plot(source, example_dir / f"source_{source_name}.pdf")


if __name__ == "__main__":
    main()
