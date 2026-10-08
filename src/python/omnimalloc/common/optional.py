#
# SPDX-License-Identifier: Apache-2.0
#

from typing import NoReturn


def require_optional(package_name: str, feature_name: str) -> NoReturn:
    """Raise an error indicating a missing optional dependency."""
    raise ImportError(
        f"The {feature_name} feature requires '{package_name}' which is not "
        "installed.\nInstall it with: pip install omnimalloc[all]"
    )
