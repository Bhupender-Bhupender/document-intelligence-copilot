from __future__ import annotations

import sys

from pathlib import Path


REPO_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

repo_root_text = str(
    REPO_ROOT
)

if repo_root_text not in sys.path:
    sys.path.insert(
        0,
        repo_root_text,
    )


from src.llmops.regression_gate_cli import (
    main,
)


if __name__ == "__main__":
    raise SystemExit(
        main()
    )