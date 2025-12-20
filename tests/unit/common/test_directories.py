#
# SPDX-License-Identifier: Apache-2.0
#


from omnimalloc.common.directories import (
    CPP_DIR,
    EXAMPLES_DIR,
    EXTERNAL_DIR,
    NOTEBOOKS_DIR,
    PROJECT_DIR,
    PYTHON_DIR,
)


def test_project_dir() -> None:
    """Test PROJECT_DIR is correctly set."""
    assert PROJECT_DIR.exists()


def test_python_dir() -> None:
    """Test PYTHON_DIR is correctly set."""
    assert PYTHON_DIR.exists()


def test_cpp_dir() -> None:
    """Test CPP_DIR is correctly set."""
    assert CPP_DIR.exists()


def test_notebooks_dir() -> None:
    """Test NOTEBOOKS_DIR is correctly set."""
    assert NOTEBOOKS_DIR.exists()


def test_external_dir() -> None:
    """Test EXTERNAL_DIR is correctly set."""
    assert EXTERNAL_DIR.exists()


def test_examples_dir() -> None:
    """Test EXAMPLES_DIR is correctly set."""
    assert EXAMPLES_DIR.exists()
