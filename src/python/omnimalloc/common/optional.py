#
# SPDX-License-Identifier: Apache-2.0
#

import importlib
from types import ModuleType


class OptionalDependencyError(ImportError):
    """Raised when an optional dependency is required but not installed."""


def require_optional(
    package_name: str,
    feature_name: str,
    install_extra: str = "all",
) -> None:
    """Raise an error indicating a missing optional dependency."""
    raise OptionalDependencyError(
        f"The {feature_name} feature requires '{package_name}' which is not "
        f"installed.\nInstall it with: pip install omnimalloc[{install_extra}]"
    )


def try_import(module_name: str) -> ModuleType | None:
    """Import a module by name, returning None if unavailable."""
    try:
        return importlib.import_module(module_name)
    except (ImportError, ModuleNotFoundError):
        return None
