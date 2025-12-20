#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.allocators import (
    BaseAllocator,
    GreedyAllocator,
    GreedyByAreaAllocator,
    GreedyBySizeAllocator,
)
from omnimalloc.benchmark.sources import (
    BaseSource,
    RandomSource,
    SequentialSource,
)
from omnimalloc.common.registry import Registered


class ExampleBase(Registered):
    """Test base class."""


class FooBar(ExampleBase):
    """Should register as 'foo_bar'."""


class BazQux(ExampleBase):
    """Should register as 'baz_qux'."""


class SimpleAllocator(ExampleBase):
    """Should register as 'simple_allocator'."""


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


def test_allocator_registry() -> None:
    registry = BaseAllocator.registry()
    assert "greedy_allocator" in registry
    assert registry["greedy_allocator"] is GreedyAllocator


def test_allocator_get() -> None:
    cls = BaseAllocator.get("greedy_by_size_allocator")
    assert cls is GreedyBySizeAllocator


def test_allocator_name() -> None:
    assert GreedyByAreaAllocator.name() == "greedy_by_area_allocator"


def test_source_registry() -> None:
    registry = BaseSource.registry()
    assert "random_source" in registry
    assert registry["random_source"] is RandomSource


def test_source_get() -> None:
    cls = BaseSource.get("sequential_source")
    assert cls is SequentialSource


def test_source_name() -> None:
    assert SequentialSource.name() == "sequential_source"


def test_source_includes_suffix() -> None:
    assert RandomSource.name() == "random_source"
    assert "source" in RandomSource.name()
