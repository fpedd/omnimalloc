#
# SPDX-License-Identifier: Apache-2.0
#

import logging
from pathlib import Path

import pytest
from omnimalloc.benchmark.sources import minimalloc
from omnimalloc.benchmark.sources.minimalloc import MinimallocSource, MinimallocSubset
from omnimalloc.io import save_allocation
from omnimalloc.primitives import Allocation, Pool

from tests.paths import EXTERNAL_DIR


def _source(subset: str) -> MinimallocSource:
    csv_dir = EXTERNAL_DIR / "minimalloc" / subset
    if not csv_dir.is_dir():
        pytest.skip(f"the {subset!r} CSV dataset lives in the repository's external/")
    return MinimallocSource(subset=subset, csv_dir=csv_dir)


def test_minimalloc_source_default_subset_is_challenging() -> None:
    source = MinimallocSource()
    assert source.subset is MinimallocSubset.CHALLENGING


@pytest.mark.skipif(
    minimalloc._checkout_csv_dir(MinimallocSubset.CHALLENGING) is None,  # noqa: SLF001
    reason="dataset discovery only resolves from a source checkout",
)
def test_minimalloc_source_discovers_the_checkout_datasets() -> None:
    assert MinimallocSource().num_allocations > 0


def test_checkout_csv_dir_stops_at_the_project_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    foreign = tmp_path / "external" / "minimalloc" / "small"
    foreign.mkdir(parents=True)
    module_file = tmp_path / "project" / "pkg" / "minimalloc.py"
    module_file.parent.mkdir(parents=True)
    monkeypatch.setattr(minimalloc, "__file__", str(module_file))
    assert minimalloc._checkout_csv_dir(MinimallocSubset.SMALL) is None  # noqa: SLF001
    (tmp_path / "project" / "pyproject.toml").touch()
    checkout = tmp_path / "project" / "external" / "minimalloc" / "small"
    checkout.mkdir(parents=True)
    assert minimalloc._checkout_csv_dir(MinimallocSubset.SMALL) == checkout  # noqa: SLF001


def test_minimalloc_source_accepts_enum_member() -> None:
    source = MinimallocSource(MinimallocSubset.SMALL)
    assert source.subset is MinimallocSubset.SMALL


def test_minimalloc_source_accepts_string_alias() -> None:
    source = MinimallocSource("small")
    assert source.subset is MinimallocSubset.SMALL
    assert source.subset == "small"


def test_minimalloc_source_examples_subset() -> None:
    source = _source("examples")
    assert source.subset == "examples"
    variants = source.get_available_variants()
    assert len(variants) == 1  # Only one example pool


def test_minimalloc_source_small_subset() -> None:
    source = _source("small")
    assert source.subset == "small"
    variants = source.get_available_variants()
    assert len(variants) > 0
    assert all(v[0].islower() for v in variants)


def test_minimalloc_source_challenging_subset() -> None:
    source = _source("challenging")
    variants = source.get_available_variants()
    assert len(variants) > 0


def test_minimalloc_source_subsets_are_disjoint() -> None:
    examples = set(_source("examples").get_available_variants())
    small = set(_source("small").get_available_variants())
    challenging = set(_source("challenging").get_available_variants())
    assert examples
    assert small
    assert challenging
    assert examples.isdisjoint(small)
    assert examples.isdisjoint(challenging)
    assert small.isdisjoint(challenging)


def test_minimalloc_source_invalid_subset() -> None:
    with pytest.raises(ValueError, match="not a valid MinimallocSubset"):
        MinimallocSource(subset="bogus")  # type: ignore[arg-type]


def test_minimalloc_source_get_allocations_skip_past_end() -> None:
    source = _source("examples")
    allocations = source.get_allocations(skip=10**9)
    assert len(allocations) == 0


def test_minimalloc_source_get_pools_with_skip_past_end() -> None:
    source = _source("examples")
    pools = source.get_pools(skip=10)
    assert len(pools) == 0


def test_minimalloc_source_get_pools_rejects_an_explicit_zero_count() -> None:
    source = _source("examples")
    with pytest.raises(ValueError, match="num_pools must be positive"):
        source.get_pools(num_pools=0)


def test_minimalloc_source_get_allocation_keeps_kind_none() -> None:
    source = _source("examples")
    allocation = source.get_allocation()
    assert allocation.kind is None


def test_minimalloc_source_get_variant_by_id() -> None:
    source = _source("small")
    variants = source.get_available_variants()
    pool = source.get_variant(variants[0])
    assert pool.id == variants[0]


def test_minimalloc_source_get_variant_by_index() -> None:
    source = _source("small")
    pool = source.get_variant(0)
    assert pool.id in source.get_available_variants()


def test_minimalloc_source_get_variant_unknown_id() -> None:
    source = _source("examples")
    with pytest.raises(ValueError, match="not found"):
        source.get_variant("does-not-exist")


def test_minimalloc_source_warns_when_dataset_directory_is_missing(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    missing = tmp_path / "absent"
    with caplog.at_level(logging.WARNING, logger=minimalloc.__name__):
        source = MinimallocSource(subset="small", csv_dir=missing)
        variants = source.get_available_variants()
    assert variants == ()
    assert str(missing) in caplog.text


def test_minimalloc_source_reads_an_explicit_csv_dir(tmp_path: Path) -> None:
    save_allocation(
        Pool(id="p", allocations=(Allocation(id=0, size=8, start=0, end=4),)),
        tmp_path / "p.csv",
    )
    source = MinimallocSource(subset="small", csv_dir=tmp_path)
    assert source.get_available_variants() == ("p",)


def test_minimalloc_source_empty_dataset_accessors_raise_clearly(
    tmp_path: Path,
) -> None:
    source = MinimallocSource(subset="small", csv_dir=tmp_path)
    assert source.get_allocations() == ()
    with pytest.raises(ValueError, match="returned no allocations"):
        source.get_allocation()
    with pytest.raises(ValueError, match="returned no pools"):
        source.get_pool()


def test_minimalloc_source_header_only_csv_yields_an_empty_variant(
    tmp_path: Path,
) -> None:
    (tmp_path / "empty.csv").write_text("id,lower,upper,size\n")
    source = MinimallocSource(subset="small", csv_dir=tmp_path)
    assert source.get_available_variants() == ("empty",)
    assert source.get_allocations() == ()


def test_minimalloc_source_warns_outside_a_checkout(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(minimalloc, "_checkout_csv_dir", lambda _subset: None)
    with caplog.at_level(logging.WARNING, logger=minimalloc.__name__):
        source = MinimallocSource()
    assert source.get_available_variants() == ()
    assert "None" not in caplog.text
    assert "csv_dir" in caplog.text


def test_minimalloc_source_label_separates_subsets() -> None:
    small = _source("small").label()
    challenging = _source("challenging").label()
    assert small != challenging


def test_minimalloc_source_label_separates_csv_dirs(tmp_path: Path) -> None:
    first = MinimallocSource(subset="small", csv_dir=tmp_path / "first")
    second = MinimallocSource(subset="small", csv_dir=tmp_path / "second")
    assert first.label() != second.label()


def test_minimalloc_source_default_label_omits_csv_dir() -> None:
    assert MinimallocSource().label() == "minimalloc"
    assert MinimallocSource(subset="small").label() == "minimalloc[subset=small]"


def test_minimalloc_source_get_allocations_ids_are_unique() -> None:
    allocations = _source("small").get_allocations()
    ids = [alloc.id for alloc in allocations]
    assert len(set(ids)) == len(ids)


def test_minimalloc_source_ids_agree_across_accessors() -> None:
    source = _source("small")
    variant_ids = {alloc.id for alloc in source.get_variant(0).allocations}
    all_ids = {alloc.id for alloc in source.get_allocations()}
    assert variant_ids <= all_ids
