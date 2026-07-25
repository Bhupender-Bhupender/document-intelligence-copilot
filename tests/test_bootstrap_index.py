"""
Tests for scripts/bootstrap_index.py.

These tests are lightweight: they never trigger a real model download, Docling
model download, or Ollama call. The indexing step is exercised only through an
injected fake (``_index_document``) so no HuggingFace/Qwen weights are loaded.

Covered
-------
    - default sample file path resolution
    - custom --file / --index-dir argument parsing
    - readable error when the input file does not exist
    - readable error for an unsupported file type
    - run_bootstrap succeeds (via injected fake) even when index_dir already exists
    - main() exit codes for success and for a missing file
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

# Ensure the scripts/ directory is importable as a module.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = _PROJECT_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import bootstrap_index  # noqa: E402
from bootstrap_index import (  # noqa: E402
    BootstrapError,
    main,
    parse_args,
    resolve_default_sample,
    resolve_file,
    run_bootstrap,
    validate_input_file,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _fake_manifest() -> SimpleNamespace:
    """A stand-in for IndexManifest carrying only the fields the script reads."""
    return SimpleNamespace(parent_count=1, child_count=2, doc_ids=["doc-1"])


def _make_fake_index_document(calls: list):
    """Return a fake index_document that records its arguments and succeeds."""

    def _fake(path, *, index_dir=None):
        calls.append((Path(path), index_dir))
        return _fake_manifest()

    return _fake


# --------------------------------------------------------------------------- #
# Path resolution                                                             #
# --------------------------------------------------------------------------- #


class TestPathResolution:
    def test_default_sample_points_to_company_policy(self) -> None:
        sample = resolve_default_sample()
        assert sample.name == "company_policy.txt"
        assert sample.parent.name == "sample_docs"

    def test_default_sample_exists_in_repo(self) -> None:
        # The default sample must ship with the repo, otherwise the no-arg
        # invocation would fail for a fresh clone.
        assert resolve_default_sample().exists()

    def test_resolve_file_uses_default_when_none(self) -> None:
        assert resolve_file(None) == resolve_default_sample()

    def test_resolve_file_uses_argument_when_provided(self) -> None:
        custom = Path("data/raw/test_pdfs/adani.pdf")
        assert resolve_file(custom) == custom


# --------------------------------------------------------------------------- #
# Argument parsing                                                            #
# --------------------------------------------------------------------------- #


class TestArgumentParsing:
    def test_no_args_gives_none_file_and_index_dir(self) -> None:
        ns = parse_args([])
        assert ns.file is None
        assert ns.index_dir is None

    def test_file_argument_parsed_as_path(self) -> None:
        ns = parse_args(["--file", "sample_docs/company_policy.txt"])
        assert ns.file == Path("sample_docs/company_policy.txt")

    def test_index_dir_argument_parsed_as_path(self) -> None:
        ns = parse_args(["--index-dir", "tmp/index"])
        assert ns.index_dir == Path("tmp/index")


# --------------------------------------------------------------------------- #
# Input validation                                                            #
# --------------------------------------------------------------------------- #


class TestValidateInputFile:
    def test_missing_file_raises_readable_error(self, tmp_path: Path) -> None:
        missing = tmp_path / "nope.txt"
        with pytest.raises(BootstrapError) as exc:
            validate_input_file(missing)
        assert "not found" in str(exc.value).lower()

    def test_directory_raises_readable_error(self, tmp_path: Path) -> None:
        with pytest.raises(BootstrapError) as exc:
            validate_input_file(tmp_path)
        assert "directory" in str(exc.value).lower()

    def test_unsupported_suffix_raises_readable_error(self, tmp_path: Path) -> None:
        bad = tmp_path / "data.xyz"
        bad.write_text("x", encoding="utf-8")
        with pytest.raises(BootstrapError) as exc:
            validate_input_file(bad)
        assert "unsupported" in str(exc.value).lower()

    def test_supported_suffix_passes(self, tmp_path: Path) -> None:
        good = tmp_path / "doc.txt"
        good.write_text("hello", encoding="utf-8")
        assert validate_input_file(good) == good


# --------------------------------------------------------------------------- #
# run_bootstrap (fake indexing — no downloads)                                #
# --------------------------------------------------------------------------- #


class TestRunBootstrap:
    def test_success_with_injected_index_document(self, tmp_path: Path) -> None:
        doc = tmp_path / "doc.txt"
        doc.write_text("some content", encoding="utf-8")
        calls: list = []
        messages: list = []

        manifest = run_bootstrap(
            doc,
            index_dir=tmp_path / "index",
            _index_document=_make_fake_index_document(calls),
            _printer=messages.append,
        )

        assert manifest.child_count == 2
        assert calls == [(doc, tmp_path / "index")]
        assert any("successfully" in m.lower() for m in messages)

    def test_does_not_crash_when_index_dir_already_exists(self, tmp_path: Path) -> None:
        doc = tmp_path / "doc.txt"
        doc.write_text("content", encoding="utf-8")
        existing_index = tmp_path / "index"
        existing_index.mkdir()  # pre-existing directory
        messages: list = []

        run_bootstrap(
            doc,
            index_dir=existing_index,
            _index_document=_make_fake_index_document([]),
            _printer=messages.append,
        )

        # An informational note about rebuilding should be printed, and no error.
        assert any("already exists" in m.lower() for m in messages)

    def test_missing_file_raises_before_indexing(self, tmp_path: Path) -> None:
        calls: list = []
        with pytest.raises(BootstrapError):
            run_bootstrap(
                tmp_path / "missing.txt",
                _index_document=_make_fake_index_document(calls),
                _printer=lambda _m: None,
            )
        assert calls == []  # indexing must not be attempted

    def test_pipeline_failure_is_classified(self, tmp_path: Path) -> None:
        doc = tmp_path / "doc.txt"
        doc.write_text("content", encoding="utf-8")

        def _boom(_path, *, index_dir=None):
            raise RuntimeError("could not reach huggingface hub")

        with pytest.raises(BootstrapError) as exc:
            run_bootstrap(
                doc,
                index_dir=tmp_path / "index",
                _index_document=_boom,
                _printer=lambda _m: None,
            )
        assert "embedding model" in str(exc.value).lower()


# --------------------------------------------------------------------------- #
# main() exit codes                                                           #
# --------------------------------------------------------------------------- #


class TestMain:
    def test_missing_file_returns_exit_code_1(self, tmp_path: Path) -> None:
        code = main(["--file", str(tmp_path / "does_not_exist.txt")])
        assert code == 1

    def test_success_returns_exit_code_0(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        doc = tmp_path / "doc.txt"
        doc.write_text("content", encoding="utf-8")

        # Patch the service import target so main()'s real path is exercised
        # without loading any model.
        import app.service as service

        monkeypatch.setattr(
            service, "index_document", lambda path, *, index_dir=None: _fake_manifest()
        )

        code = main(["--file", str(doc), "--index-dir", str(tmp_path / "index")])
        assert code == 0
