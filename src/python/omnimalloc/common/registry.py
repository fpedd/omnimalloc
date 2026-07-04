#
# SPDX-License-Identifier: Apache-2.0
#

import re
from abc import ABC
from typing import ClassVar

from typing_extensions import Self


class Registered(ABC):
    """Mixin for auto-registering and managing subclasses.

    Any direct subclass of Registered will maintain its own registry. Any
    subclass of that subclass that's not abstract will be registered in the
    direct subclass's registry.
    """

    _registry: ClassVar[dict[str, type[Self]]]
    _name: str

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)

        cls._name = _camel_to_snake(cls.__name__)

        # Direct subclass of Registered - initialize registry, don't register
        if Registered in cls.__bases__:
            cls._registry = {}
            return

        # Skip abstract classes from registration
        if any(
            getattr(getattr(cls, name, None), "__isabstractmethod__", False)
            for name in dir(cls)
        ):
            return

        # Child class - register in parent's registry
        for base in reversed(cls.__mro__[1:]):
            if Registered in base.__bases__ and issubclass(base, Registered):
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


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()
