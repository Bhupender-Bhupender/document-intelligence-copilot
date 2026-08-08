from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

TARGETS = [
    PROJECT_ROOT / "app" / "service.py",
    PROJECT_ROOT / "src" / "evaluation",
    PROJECT_ROOT / "src" / "retrieval",
    PROJECT_ROOT / "src" / "generation",
    PROJECT_ROOT / "src" / "citations",
    PROJECT_ROOT / "src" / "validation",
    PROJECT_ROOT / "src" / "indexing",
]


def format_arguments(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    args: list[str] = []

    positional = list(node.args.posonlyargs) + list(node.args.args)

    for argument in positional:
        args.append(argument.arg)

    if node.args.vararg:
        args.append(f"*{node.args.vararg.arg}")
    elif node.args.kwonlyargs:
        args.append("*")

    for argument in node.args.kwonlyargs:
        args.append(argument.arg)

    if node.args.kwarg:
        args.append(f"**{node.args.kwarg.arg}")

    return ", ".join(args)


def inspect_file(path: Path) -> None:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError) as exc:
        print(f"\nFILE: {path.relative_to(PROJECT_ROOT)}")
        print(f"  ERROR: {type(exc).__name__}")
        return

    records: list[str] = []

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            prefix = "async function" if isinstance(
                node,
                ast.AsyncFunctionDef,
            ) else "function"

            records.append(
                f"  {prefix}: {node.name}({format_arguments(node)})"
            )

        elif isinstance(node, ast.ClassDef):
            records.append(f"  class: {node.name}")

            for child in node.body:
                if isinstance(
                    child,
                    (ast.FunctionDef, ast.AsyncFunctionDef),
                ):
                    if child.name.startswith("__") and child.name != "__init__":
                        continue

                    records.append(
                        f"    method: {child.name}"
                        f"({format_arguments(child)})"
                    )

    if records:
        print(f"\nFILE: {path.relative_to(PROJECT_ROOT)}")

        for record in records:
            print(record)


def main() -> None:
    for target in TARGETS:
        if target.is_file():
            inspect_file(target)

        elif target.is_dir():
            for path in sorted(target.rglob("*.py")):
                if "__pycache__" not in path.parts:
                    inspect_file(path)


if __name__ == "__main__":
    main()
