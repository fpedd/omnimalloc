#
# SPDX-License-Identifier: Apache-2.0
#


from omnimalloc.common.directories import (
    EXAMPLES_DIR,
    EXTERNAL_DIR,
    NOTEBOOKS_DIR,
    PROJECT_DIR,
)


def test_project_dir_is_the_repository_root() -> None:
    assert (PROJECT_DIR / "pyproject.toml").is_file()
    assert (PROJECT_DIR / "CMakeLists.txt").is_file()


def test_notebooks_dir_holds_the_notebooks() -> None:
    assert sorted(p.name for p in NOTEBOOKS_DIR.glob("*.ipynb"))


def test_external_dir_holds_the_minimalloc_datasets() -> None:
    assert (EXTERNAL_DIR / "minimalloc" / "examples").is_dir()


def test_examples_dir_holds_the_numbered_examples() -> None:
    assert sorted(p.name for p in EXAMPLES_DIR.glob("*.py")) == [
        "01_basic.py",
        "02_plotting.py",
        "03_allocators.py",
        "04_sources.py",
        "05_benchmark.py",
    ]
