#
# SPDX-License-Identifier: Apache-2.0
#

from abc import abstractmethod
from typing import ClassVar

import pytest
from omnimalloc.allocators import (
    BaseAllocator,
    GreedyAllocator,
    GreedyByAreaAllocator,
    GreedyBySizeAllocator,
    OmniAllocator,
)
from omnimalloc.benchmark.sources import (
    BaseSource,
    RandomSource,
    SequentialSource,
)
from omnimalloc.common.registry import Registered


class ExampleBase(Registered): ...


class FooBar(ExampleBase): ...


class BazQux(ExampleBase): ...


class SimpleAllocator(ExampleBase): ...


class ExampleRoleBase(Registered):
    _strip_suffix: ClassVar[str] = "Widget"


class SpinningWidget(ExampleRoleBase): ...


def test_registry_auto_registration() -> None:
    registry = ExampleBase.registry()
    assert "foo_bar" in registry
    assert "baz_qux" in registry
    assert "simple_allocator" in registry


def test_registry_contains_correct_classes() -> None:
    registry = ExampleBase.registry()
    assert registry["foo_bar"] is FooBar
    assert registry["baz_qux"] is BazQux
    assert registry["simple_allocator"] is SimpleAllocator


def test_get_by_name() -> None:
    assert ExampleBase.get("foo_bar") is FooBar
    assert ExampleBase.get("baz_qux") is BazQux
    assert ExampleBase.get("simple_allocator") is SimpleAllocator


def test_get_invalid_name() -> None:
    with pytest.raises(KeyError, match="'invalid' not in"):
        ExampleBase.get("invalid")


def test_get_error_shows_available() -> None:
    with pytest.raises(KeyError, match=r"Available:.*foo_bar"):
        ExampleBase.get("nonexistent")


def test_resolve_invalid_name_raises_value_error() -> None:
    with pytest.raises(ValueError, match=r"'invalid' not in.*Available:.*foo_bar"):
        ExampleBase.resolve("invalid")


def test_class_name_property() -> None:
    assert FooBar.name() == "foo_bar"
    assert BazQux.name() == "baz_qux"
    assert SimpleAllocator.name() == "simple_allocator"


def test_registry_is_copy() -> None:
    registry1 = ExampleBase.registry()
    registry2 = ExampleBase.registry()
    assert registry1 is not registry2
    assert registry1 == registry2


def test_snake_case_conversion() -> None:
    class HTTPSConnection(ExampleBase):
        pass

    assert HTTPSConnection.name() == "https_connection"


def test_name_with_numbers() -> None:
    class Test123Thing(ExampleBase):
        pass

    assert Test123Thing.name() == "test123_thing"


def test_strip_suffix_stripped_once() -> None:
    assert SpinningWidget.name() == "spinning"
    assert ExampleRoleBase.registry()["spinning"] is SpinningWidget


def test_strip_suffix_absent_keeps_full_name() -> None:
    class PlainThing(ExampleRoleBase):
        pass

    assert PlainThing.name() == "plain_thing"


def test_strip_suffix_ignores_mid_name_token() -> None:
    class WidgetFactoryWidget(ExampleRoleBase):
        pass

    assert WidgetFactoryWidget.name() == "widget_factory"


def test_bare_strip_suffix_name_rejected() -> None:
    with pytest.raises(RuntimeError, match="empty"):

        class Widget(ExampleRoleBase):
            pass


def test_abstract_intermediate_not_registered() -> None:
    class AbstractMid(ExampleBase):
        @abstractmethod
        def compute(self) -> int: ...

    class ConcreteLeaf(AbstractMid):
        def compute(self) -> int:
            return 0

    registry = ExampleBase.registry()
    assert "abstract_mid" not in registry
    assert "concrete_leaf" in registry


def test_allocator_registry() -> None:
    registry = BaseAllocator.registry()
    assert "greedy" in registry
    assert registry["greedy"] is GreedyAllocator


def test_allocator_get() -> None:
    cls = BaseAllocator.get("greedy_by_size")
    assert cls is GreedyBySizeAllocator


def test_allocator_name_drops_strip_suffix() -> None:
    assert GreedyByAreaAllocator.name() == "greedy_by_area"


def test_source_registry() -> None:
    registry = BaseSource.registry()
    assert "random" in registry
    assert registry["random"] is RandomSource


def test_source_get() -> None:
    cls = BaseSource.get("sequential")
    assert cls is SequentialSource


def test_source_name_drops_strip_suffix() -> None:
    assert SequentialSource.name() == "sequential"
    assert RandomSource.name() == "random"


def test_registry_rejects_duplicate_names() -> None:
    class UniqueNameBase(Registered): ...

    class DuplicateName(UniqueNameBase): ...

    first = DuplicateName
    with pytest.raises(RuntimeError, match="already taken"):

        class DuplicateName(UniqueNameBase): ...

    assert UniqueNameBase.registry()["duplicate_name"] is first


def test_resolve_rejects_a_foreign_class() -> None:
    class Foreign:
        pass

    with pytest.raises(TypeError, match="Foreign is not a"):
        BaseAllocator.resolve(Foreign)  # type: ignore[arg-type]


def test_resolve_rejects_a_foreign_instance() -> None:
    with pytest.raises(TypeError, match="object is not a"):
        BaseAllocator.resolve(object())  # type: ignore[arg-type]


def test_resolve_accepts_names_classes_and_instances() -> None:
    assert isinstance(BaseAllocator.resolve("omni"), OmniAllocator)
    assert isinstance(BaseAllocator.resolve(OmniAllocator), OmniAllocator)
    instance = OmniAllocator()
    assert BaseAllocator.resolve(instance) is instance
