#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.visualize import HAS_MATPLOTLIB

from tests.paths import EXAMPLES_DIR, NOTEBOOKS_DIR

needs_matplotlib = pytest.mark.skipif(
    not HAS_MATPLOTLIB, reason="matplotlib not installed"
)
needs_examples = pytest.mark.skipif(
    not EXAMPLES_DIR.is_dir(), reason="examples/ lives in the repository"
)
needs_notebooks = pytest.mark.skipif(
    not NOTEBOOKS_DIR.is_dir(), reason="notebooks/ lives in the repository"
)
