#
# SPDX-License-Identifier: Apache-2.0
#

from pathlib import Path

# Repository layout: the installed package ships none of these directories.
REPO_DIR = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = REPO_DIR / "examples"
NOTEBOOKS_DIR = REPO_DIR / "notebooks"
EXTERNAL_DIR = REPO_DIR / "external"
