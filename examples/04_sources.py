#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

import omnimalloc as om
from omnimalloc.benchmark.sources import BaseSource


def main() -> None:
    example_dir = Path("04_example_output")

    # A few synthetic sources, each generating a seeded, reproducible pool
    for source_name in ("random", "skewed", "tiling", "pinwheel"):
        source = BaseSource.get(source_name)()
        pool = om.allocate(source.get_pool(), validate=True)
        print(f"Source {source_name!r} pool size: {pool.size}")
        om.plot_allocation(pool, example_dir / f"source_{source_name}.pdf")


if __name__ == "__main__":
    main()
