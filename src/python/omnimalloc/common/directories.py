#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path
from typing import Final

# Only meaningful in a source checkout: from an installed wheel this resolves
# above site-packages, and the repo layout it points into is not shipped.
PROJECT_DIR: Final[Path] = Path(__file__).parent.parent.parent.parent.parent
NOTEBOOKS_DIR: Final[Path] = PROJECT_DIR / "notebooks"
EXTERNAL_DIR: Final[Path] = PROJECT_DIR / "external"
EXAMPLES_DIR: Final[Path] = PROJECT_DIR / "examples"
