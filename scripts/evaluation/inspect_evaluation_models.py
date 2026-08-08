from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

TARGET_CLASSES = {
    "EvalExample",
    "EvalReport",
    "SemanticScore",
    "SemanticEvalReport",
}


def annotation_text(node: ast.AST | None) -> str:
    return ast.unparse(node) if node is not None else "Any"


def inspect_file(path: Path) -> bool:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return False

    found = False

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue

        if node.name not in TARGET_CLASSES:
            continue

        if not found:
            print(f"\nFILE: {path.relative_to(PROJECT_ROOT)}")
            found = True

        print(f"\nCLASS: {node.name}")

        for child in node.body:
            if not isinstance(child, ast.AnnAssign):
                continue

            if not isinstance(child.target, ast.Name):
                continue

            default = (
                ast.unparse(child.value)
                if child.value is not None
                else "<required>"
            )

            print(
                f"  - {child.target.id}: "
                f"{annotation_text(child.annotation)} "
                f"| default={default}"
            )

    return found


def main() -> None:
    files_found = 0

    for path in sorted(PROJECT_ROOT.rglob("*.py")):
        if any(
            part in {
                ".venv",
                "venv",
                "__pycache__",
                ".git",
            }
            for part in path.parts
        ):
            continue

        if inspect_file(path):
            files_found += 1

    if files_found == 0:
        print("No target evaluation classes found.")


if __name__ == "__main__":
    main()
