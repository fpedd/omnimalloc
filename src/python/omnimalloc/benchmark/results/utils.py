#
# SPDX-License-Identifier: Apache-2.0
#

import os
import platform
from datetime import datetime
from typing import Any

from omnimalloc import __version__
from omnimalloc.benchmark.sources import BaseSource


def source_label(source: BaseSource | type[BaseSource] | str) -> str:
    """Per-instance label of a source, so configured variants stay separable."""
    return source.label() if isinstance(source, BaseSource) else str(source)


def get_date_time_snake_case() -> str:
    """Get the current time and date as a formatted string in snake_case."""
    return datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def get_environment_metadata() -> dict[str, Any]:
    """Generate environment metadata for benchmark results."""
    return {
        "date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "omnimalloc_version": str(__version__),
        "os_info": f"{platform.system()} {platform.release()}",
        "cpu_info": platform.processor(),
        "num_cores": os.cpu_count() or 1,
    }
