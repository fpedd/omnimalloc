#
# SPDX-License-Identifier: Apache-2.0
#

import os
import platform
import subprocess
from datetime import datetime
from typing import Any

from omnimalloc import __version__


def get_date_time() -> str:
    """Get the current time and date as a formatted string."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def get_date_time_snake_case() -> str:
    """Get the current time and date as a formatted string in snake_case."""
    return datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def get_package_version() -> str:
    """Get the current omnimalloc version."""

    return str(__version__)


def get_git_hash() -> str:
    """Get the current git hash of the omnimalloc package."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],  # noqa: S607
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.SubprocessError, FileNotFoundError):
        return "unknown"


def get_os_info() -> str:
    """Get a string describing the operating system."""
    return f"{platform.system()} {platform.release()}"


def get_cpu_info() -> str:
    """Get a string describing the CPU."""
    return platform.processor()


def get_num_cores() -> int:
    """Get the number of CPU cores."""
    return os.cpu_count() or 1


def get_environment_metadata() -> dict[str, Any]:
    """Generate environment metadata for benchmark results."""
    return {
        "date_time": get_date_time(),
        "omnimalloc_version": get_package_version(),
        "omnimalloc_git_hash": get_git_hash(),
        "os_info": get_os_info(),
        "cpu_info": get_cpu_info(),
        "num_cores": get_num_cores(),
    }
