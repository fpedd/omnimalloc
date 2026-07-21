#
# SPDX-License-Identifier: Apache-2.0
#

import inspect
import re
from abc import ABC
from typing import ClassVar

from typing_extensions import Self


class Registered(ABC):
    """Mixin for auto-registering and managing subclasses.

    Each direct subclass of Registered maintains its own registry; its
    non-abstract descendants register automatically. Registry names strip
    the root's `_strip_suffix` (e.g. "Allocator") from the end of the
    class name and snake_case the remainder; roots leave it empty.
    """

    _registry: ClassVar[dict[str, type[Self]]]
    _name: str
    _strip_suffix: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)

        # Direct subclass of Registered - initialize registry, don't register
        if Registered in cls.__bases__:
            cls._name = _camel_to_snake(cls.__name__)
            cls._registry = {}
            return

        # Abstract classes keep their full name and skip registration
        if inspect.isabstract(cls):
            cls._name = _camel_to_snake(cls.__name__)
            return

        # Child class - register in parent's registry
        for base in reversed(cls.__mro__[1:]):
            if Registered in base.__bases__ and issubclass(base, Registered):
                cls._name = _derive_name(cls.__name__, base._strip_suffix)  # noqa: SLF001
                registered = base._registry.get(cls._name)  # noqa: SLF001
                if registered is not None and registered is not cls:
                    raise RuntimeError(
                        f"Registry name '{cls._name}' already taken by "
                        f"{registered.__qualname__}; cannot register "
                        f"{cls.__qualname__}"
                    )
                base._registry[cls._name] = cls  # noqa: SLF001
                return

        raise RuntimeError(f"Could not register class {cls.__name__}")

    def __str__(self) -> str:
        return self._name

    @classmethod
    def name(cls) -> str:
        """Return the registry name for this class."""
        return cls._name

    @classmethod
    def registry(cls) -> dict[str, type[Self]]:
        """Return dict of all registered subclasses: {name: class}."""
        return cls._registry.copy()

    @classmethod
    def get(cls, name: str) -> type[Self]:
        """Get a registered class by name."""
        if name in cls._registry:
            return cls._registry[name]
        available = ", ".join(f"'{n}'" for n in sorted(cls._registry.keys()))
        raise KeyError(
            f"'{name}' not in {cls.__name__} registry. Available: {available}"
        )

    @classmethod
    def resolve(cls, value: "Self | type[Self] | str") -> Self:
        """Normalize a registry name, class, or instance into an instance."""
        if isinstance(value, str):
            value = cls.get(value)
        if isinstance(value, type):
            return value()
        return value


def _derive_name(class_name: str, role_token: str) -> str:
    """Registry key for `class_name`: strip the `role_token` suffix, snake_case."""
    stripped = class_name.removesuffix(role_token) if role_token else class_name
    if not stripped:
        raise RuntimeError(
            f"Registry name for {class_name!r} is empty after stripping {role_token!r}"
        )
    return _camel_to_snake(stripped)


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
