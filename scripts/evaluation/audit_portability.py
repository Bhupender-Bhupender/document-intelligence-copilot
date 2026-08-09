from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

SCAN_DIRS = [
    ROOT / "src",
    ROOT / "app",
]

PATTERNS = {
    "hardcoded_windows_path": re.compile(
        r"[A-Za-z]:\\\\"
    ),
    "localhost": re.compile(
        r"localhost|127\.0\.0\.1",
        re.IGNORECASE,
    ),
    "ollama": re.compile(
        r"\bollama\b",
        re.IGNORECASE,
    ),
    "direct_filesystem": re.compile(
        r"\b(open|Path|read_text|write_text|read_bytes|write_bytes)\b"
    ),
    "local_index_path": re.compile(
        r"data[/\\]index|chroma|SimpleVectorStore",
        re.IGNORECASE,
    ),
    "azure_specific": re.compile(
        r"azure_|DefaultAzureCredential|BlobServiceClient|SearchClient",
        re.IGNORECASE,
    ),
}


def scan_text(path: Path) -> list[tuple[int, str, str]]:
    findings = []

    try:
        lines = path.read_text(
            encoding="utf-8"
        ).splitlines()
    except (UnicodeDecodeError, OSError):
        return findings

    for number, line in enumerate(lines, start=1):
        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            continue

        for category, pattern in PATTERNS.items():
            if pattern.search(line):
                findings.append(
                    (
                        number,
                        category,
                        stripped[:180],
                    )
                )

    return findings


def inspect_imports(path: Path) -> list[str]:
    try:
        tree = ast.parse(
            path.read_text(encoding="utf-8")
        )
    except (SyntaxError, UnicodeDecodeError, OSError):
        return []

    imports = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(
                alias.name
                for alias in node.names
            )

        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)

    interesting = [
        name
        for name in imports
        if any(
            token in name.lower()
            for token in (
                "ollama",
                "azure",
                "chromadb",
                "llama_index",
                "gradio",
                "docling",
            )
        )
    ]

    return sorted(set(interesting))


def main() -> None:
    total_findings = 0

    for directory in SCAN_DIRS:
        for path in sorted(
            directory.rglob("*.py")
        ):
            findings = scan_text(path)
            imports = inspect_imports(path)

            if not findings and not imports:
                continue

            relative = path.relative_to(ROOT)

            print(f"\nFILE: {relative}")

            if imports:
                print("  IMPORTANT IMPORTS:")
                for item in imports:
                    print(f"    - {item}")

            if findings:
                print("  FINDINGS:")

                for line_no, category, text in findings:
                    total_findings += 1

                    # Do not print full hardcoded paths.
                    if category == "hardcoded_windows_path":
                        text = "<redacted local path>"

                    print(
                        f"    L{line_no} "
                        f"[{category}] {text}"
                    )

    print(
        f"\nTOTAL FINDINGS: "
        f"{total_findings}"
    )


if __name__ == "__main__":
    main()
