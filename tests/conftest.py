"""
Pytest configuration and shared fixtures.

Adds the project root to sys.path so that `src.*` imports resolve
correctly when running pytest from any working directory.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
