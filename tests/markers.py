#
# SPDX-License-Identifier: Apache-2.0
#

import pytest
from omnimalloc.visualize import HAS_MATPLOTLIB

needs_matplotlib = pytest.mark.skipif(
    not HAS_MATPLOTLIB, reason="matplotlib not installed"
)
