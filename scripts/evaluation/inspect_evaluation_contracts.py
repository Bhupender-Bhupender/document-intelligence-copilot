from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

FUNCTION_TARGETS = {
    PROJECT_ROOT / "app" / "service.py": {
        "index_document",
        "answer_query",
    },
    PROJECT_ROOT / "src" / "evaluation" / "evaluator.py": {
        "run_evaluation",
        "_compute_metrics",
    },
    PROJECT_ROOT / "src" / "evaluation" / "semantic_evaluator.py": {
        "run_semantic_evaluation",
        "_score_one",
        "_aggregate_scores",
    },
}

SCHEMA_FILE = PROJECT_ROOT / "src" / "schema" / "models.py"


def annotation_text(node: ast.AST | None) -> str:
    if node is None:
        return "Any"

    return ast.unparse(node)


def function_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    arguments: list[str] = []

    positional = list(node.args.posonlyargs) + list(node.args.args)

    for argument in positional:
        arguments.append(
            f"{argument.arg}: {annotation_text(argument.annotation)}"
        )

    if node.args.vararg:
        arguments.append(f"*{node.args.vararg.arg}")
    elif node.args.kwonlyargs:
        arguments.append("*")

    for argument in node.args.kwonlyargs:
        arguments.append(
            f"{argument.arg}: {annotation_text(argument.annotation)}"
        )

    if node.args.kwarg:
        arguments.append(f"**{node.args.kwarg.arg}")

    return_annotation = annotation_text(node.returns)

    return (
        f"{node.name}({', '.join(arguments)}) "
        f"-> {return_annotation}"
    )


def collect_string_keys(node: ast.AST) -> list[str]:
    keys: set[str] = set()

    for child in ast.walk(node):
        if isinstance(child, ast.Subscript):
            slice_node = child.slice

            if isinstance(slice_node, ast.Constant):
                if isinstance(slice_node.value, str):
                    keys.add(slice_node.value)

        if isinstance(child, ast.Call):
            if isinstance(child.func, ast.Attribute):
                if child.func.attr == "get" and child.args:
                    first_arg = child.args[0]

                    if (
                        isinstance(first_arg, ast.Constant)
                        and isinstance(first_arg.value, str)
                    ):
                        keys.add(first_arg.value)

    return sorted(keys)


def inspect_functions(path: Path, names: set[str]) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))

    print(f"\nFILE: {path.relative_to(PROJECT_ROOT)}")

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name not in names:
                continue

            print(f"\nFUNCTION: {function_signature(node)}")

            keys = collect_string_keys(node)

            if keys:
                print("STRING KEYS:")
                for key in keys:
                    print(f"  - {key}")
            else:
                print("STRING KEYS: none")


def inspect_schema(path: Path) -> None:
    tree = ast.parse(path.read_text(encoding="utf-8"))

    print(f"\nFILE: {path.relative_to(PROJECT_ROOT)}")

    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue

        fields: list[tuple[str, str, str]] = []

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

            fields.append(
                (
                    child.target.id,
                    annotation_text(child.annotation),
                    default,
                )
            )

        if not fields:
            continue

        print(f"\nCLASS: {node.name}")

        for name, annotation, default in fields:
            print(
                f"  - {name}: {annotation} "
                f"| default={default}"
            )


def main() -> None:
    for path, function_names in FUNCTION_TARGETS.items():
        inspect_functions(path, function_names)

    inspect_schema(SCHEMA_FILE)


if __name__ == "__main__":
    main()
