"""Sphinx configuration for the vfs-pg documentation."""

import os
import sys

sys.path.insert(0, os.path.abspath("../src"))

project = "vfs-pg"
copyright = "2026, Andreas Jung"
author = "Andreas Jung"
release = "0.2.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

html_theme = "furo"
html_static_path = []
