#
# SPDX-License-Identifier: Apache-2.0
#

import inspect
import re
from abc import ABC
from typing import ClassVar, cast

from typing_extensions import Self


class Registered(ABC):
    """Mixin for auto-registering and managing subclasses.

    Each direct subclass maintains its own registry; non-abstract descendants
    register automatically, named by snake_case minus `_strip_suffix`.
    """

    _registry: ClassVar[dict[str, type[Self]]]
    _name: str
    _strip_suffix: ClassVar[str] = ""

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)

        # Direct subclass of Registered: initialize registry, don't register
        if Registered in cls.__bases__:
            cls._name = _camel_to_snake(cls.__name__)
            cls._registry = {}
            return

        # Abstract classes keep their full name and skip registration
        if inspect.isabstract(cls):
            cls._name = _camel_to_snake(cls.__name__)
            return

        # Child class: register in the root's registry, which plain attribute
        # inheritance already resolves
        cls._name = _derive_name(cls.__name__, cls._strip_suffix)
        registered = cls._registry.get(cls._name)
        if registered is not None and registered is not cls:
            raise RuntimeError(
                f"Registry name '{cls._name}' already taken by "
                f"{registered.__qualname__}; cannot register {cls.__qualname__}"
            )
        cls._registry[cls._name] = cls

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
        raise KeyError(cls._unknown_name_message(name))

    @classmethod
    def resolve(cls, value: "Self | type[Self] | str") -> Self:
        """Normalize a registry name, class, or instance into an instance."""
        if isinstance(value, str):
            if value not in cls._registry:
                raise ValueError(cls._unknown_name_message(value))
            value = cls._registry[value]
        if isinstance(value, type):
            if not issubclass(value, cls):
                raise TypeError(f"{value.__qualname__} is not a {cls.__name__}")
            return value()
        if not isinstance(value, cls):
            raise TypeError(f"{type(value).__qualname__} is not a {cls.__name__}")
        return cast("Self", value)

    @classmethod
    def _unknown_name_message(cls, name: str) -> str:
        available = ", ".join(f"'{n}'" for n in sorted(cls._registry.keys()))
        return f"'{name}' not in {cls.__name__} registry. Available: {available}"


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
