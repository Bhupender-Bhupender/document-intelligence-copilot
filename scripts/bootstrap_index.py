"""
Bootstrap the default local index.

Purpose
-------
Build the default on-disk index under ``data/index/`` so the "Ask a Question"
tab of the Gradio UI works on a fresh checkout. Retrieval requires a persisted
index; without one, ``answer_query`` raises ``FileNotFoundError`` on the first
question.

This script is a thin, safe wrapper over the existing service layer
(``app.service.index_document``) and the existing indexing pipeline. It does
NOT introduce a new vector store, embedding path, or storage format — it simply
runs the pipeline that already exists and persists to ``config.index_dir``.

Vector store note
-----------------
The active local vector store is LlamaIndex's ``SimpleVectorStore`` persisted
to disk (``data/index/child_index/`` + ``data/index/parent_store/``). ChromaDB
is NOT the active vector store in the current pipeline.

Usage
-----
    python scripts/bootstrap_index.py
    python scripts/bootstrap_index.py --file docs/sample_docs/company_policy.txt
    python scripts/bootstrap_index.py --file data/raw/test_pdfs/adani.pdf

Behaviour
---------
- With no ``--file``, indexes the default sample document
  (``docs/sample_docs/company_policy.txt``) — a small text file that needs no
  Docling/OCR and is fast to embed.
- ``--index-dir`` overrides the output directory (defaults to
  ``config.index_dir`` = ``data/index/``).
- Running repeatedly is safe: the existing ``build_indexes`` overwrites the
  stores at the target directory (deterministic chunk IDs mean re-indexing the
  same file reproduces the same node IDs).

Errors
------
The script performs explicit pre-checks (file exists, supported suffix) so the
top-level message is readable. First-run model download or a Docling parse
failure surfaces as a clear one-line explanation followed by the underlying
exception text — tracebacks are not fully hidden, but the cause is spelled out.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

# --------------------------------------------------------------------------- #
# Path bootstrap — make ``src`` / ``app`` importable when run as a script      #
# --------------------------------------------------------------------------- #
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Default sample document — small, fast, no Docling/OCR required.
DEFAULT_SAMPLE = PROJECT_ROOT / "docs" / "sample_docs" / "company_policy.txt"

# File suffixes the ingestion router accepts today (see src/ingestion/router.py).
SUPPORTED_SUFFIXES = frozenset({".txt", ".md", ".pdf", ".docx"})


class BootstrapError(Exception):
    """Raised when the bootstrap cannot proceed, with a readable message."""


# --------------------------------------------------------------------------- #
# Argument parsing                                                             #
# --------------------------------------------------------------------------- #


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse command-line arguments for the bootstrap script."""
    parser = argparse.ArgumentParser(
        prog="bootstrap_index.py",
        description=(
            "Build the default local index under data/index/ using the existing "
            "indexing pipeline (LlamaIndex SimpleVectorStore, persisted on disk)."
        ),
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=None,
        help=(
            "Document to index (.txt, .md, .pdf, .docx). "
            "Defaults to docs/sample_docs/company_policy.txt when omitted."
        ),
    )
    parser.add_argument(
        "--index-dir",
        type=Path,
        default=None,
        help=(
            "Output directory for the index. Defaults to config.index_dir "
            "(data/index/) when omitted."
        ),
    )
    return parser.parse_args(argv)


# --------------------------------------------------------------------------- #
# Resolution + validation helpers                                             #
# --------------------------------------------------------------------------- #


def resolve_default_sample() -> Path:
    """Return the default sample document path."""
    return DEFAULT_SAMPLE


def resolve_file(file_arg: Optional[Path]) -> Path:
    """
    Resolve the document to index.

    Uses ``file_arg`` when provided, otherwise the default sample. Relative
    paths are resolved against the current working directory (argparse gives
    them to us as-is) so that the usage examples in the docstring work when the
    script is run from the project root.
    """
    return file_arg if file_arg is not None else resolve_default_sample()


def validate_input_file(path: Path) -> Path:
    """
    Validate that ``path`` exists and has a supported suffix.

    Raises
    ------
    BootstrapError
        With a readable message when the file is missing, is a directory, or
        has an unsupported extension.
    """
    if not path.exists():
        raise BootstrapError(
            f"Input file not found: {path}\n"
            "  Check the path, or omit --file to index the default sample "
            f"({resolve_default_sample().relative_to(PROJECT_ROOT)})."
        )
    if path.is_dir():
        raise BootstrapError(
            f"Input path is a directory, not a file: {path}\n"
            "  Pass a single document with --file."
        )
    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise BootstrapError(
            f"Unsupported file type: {suffix!r} ({path.name})\n"
            f"  Supported types: {', '.join(sorted(SUPPORTED_SUFFIXES))}."
        )
    return path


# --------------------------------------------------------------------------- #
# Core bootstrap                                                               #
# --------------------------------------------------------------------------- #


def run_bootstrap(
    file_arg: Optional[Path] = None,
    index_dir: Optional[Path] = None,
    *,
    _index_document: Optional[Callable[..., Any]] = None,
    _printer: Callable[[str], None] = print,
) -> Any:
    """
    Build the local index for a single document.

    Parameters
    ----------
    file_arg:
        Document to index, or None to use the default sample.
    index_dir:
        Output directory, or None to use ``config.index_dir``.
    _index_document:
        Test-injection seam. When provided, replaces
        ``app.service.index_document``. Receives ``(path, index_dir=...)`` and
        must return an object with the IndexManifest fields
        (parent_count, child_count, doc_ids).
    _printer:
        Injectable print function (defaults to builtin ``print``).

    Returns
    -------
    IndexManifest
        The manifest returned by the indexing pipeline.

    Raises
    ------
    BootstrapError
        With a readable, classified message on any failure.
    """
    path = validate_input_file(resolve_file(file_arg))

    # Resolve the output directory for the progress message. When index_dir is
    # None the service layer falls back to config.index_dir; we read the same
    # value here only for display, without changing the call semantics.
    target_dir = index_dir
    if target_dir is None:
        from src.core.config import config  # deferred — keeps import light for tests

        target_dir = config.index_dir

    _printer(f"Bootstrapping index")
    _printer(f"  Document : {path}")
    _printer(f"  Index dir: {target_dir}")
    if Path(target_dir).exists():
        _printer(
            "  Note     : index directory already exists; it will be rebuilt "
            "(existing stores overwritten)."
        )
    _printer("  Loading embedding model and building index (first run may "
             "download the model)...")

    index_document = _index_document
    if index_document is None:
        from app.service import index_document as _real_index_document  # deferred

        index_document = _real_index_document

    try:
        manifest = index_document(path, index_dir=index_dir)
    except BootstrapError:
        raise
    except Exception as exc:  # noqa: BLE001 — classify and re-raise readably
        raise BootstrapError(_classify_failure(path, exc)) from exc

    _printer("Index built successfully.")
    _printer(f"  Parents indexed: {getattr(manifest, 'parent_count', '?')}")
    _printer(f"  Chunks indexed : {getattr(manifest, 'child_count', '?')}")
    doc_ids = getattr(manifest, "doc_ids", None) or []
    _printer(f"  Documents      : {', '.join(doc_ids) if doc_ids else '(none)'}")
    _printer(f"  Output         : {target_dir}")
    return manifest


def _classify_failure(path: Path, exc: Exception) -> str:
    """
    Produce a readable, best-effort explanation of an indexing failure.

    The underlying pipeline wraps most errors in a ServiceError, so we inspect
    the exception text for well-known signals. The original exception is always
    attached as ``__cause__`` (raise ... from exc) so the traceback is not lost.
    """
    text = str(exc).lower()
    header: str

    if "unsupported" in text or "unrecognised" in text or "unrecognized" in text:
        header = "Unsupported file type for the ingestion router."
    elif any(
        s in text
        for s in ("huggingface", "connection", "download", "network",
                  "getaddrinfo", "temporarily unavailable", "hf_hub")
    ):
        header = (
            "Embedding model could not be loaded/downloaded. Check internet "
            "access and that the HuggingFace model is reachable."
        )
    elif "docling" in text or "torch" in text or "c10.dll" in text:
        header = "Document parsing failed (Docling/torch). See details below."
    elif any(
        s in text
        for s in ("permission", "winerror 5", "eacces", "read-only", "errno 13")
    ):
        header = (
            "Could not write the index files. Check write permissions for the "
            "index directory."
        )
    else:
        header = "Indexing failed."

    return (
        f"{header}\n"
        f"  File : {path}\n"
        f"  Cause: {type(exc).__name__}: {exc}"
    )


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entry point.

    Returns a process exit code: 0 on success, 1 on a readable BootstrapError,
    2 on an unexpected error.
    """
    args = parse_args(argv)
    try:
        run_bootstrap(args.file, args.index_dir)
    except BootstrapError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 — last-resort guard, still readable
        print(
            f"\nUNEXPECTED ERROR: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
