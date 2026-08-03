#
# SPDX-License-Identifier: Apache-2.0
#


def require_optional(
    package_name: str,
    feature_name: str,
    install_extra: str = "all",
) -> None:
    """Raise an error indicating a missing optional dependency."""
    raise ImportError(
        f"The {feature_name} feature requires '{package_name}' which is not "
        f"installed.\nInstall it with: pip install omnimalloc[{install_extra}]"
    )
