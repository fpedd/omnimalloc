#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path
from typing import Final

PROJECT_DIR: Final[Path] = Path(__file__).parent.parent.parent.parent.parent
PYTHON_DIR: Final[Path] = PROJECT_DIR / "src" / "python" / "omnimalloc"
CPP_DIR: Final[Path] = PROJECT_DIR / "src" / "cpp"
NOTEBOOKS_DIR: Final[Path] = PROJECT_DIR / "notebooks"
EXTERNAL_DIR: Final[Path] = PROJECT_DIR / "external"
EXAMPLES_DIR: Final[Path] = PROJECT_DIR / "examples"
