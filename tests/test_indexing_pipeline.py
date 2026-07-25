"""
Tests for src/indexing/embed_config.py and src/indexing/indexing_pipeline.py.

All tests use isolated output directories (tmp_path) and MockEmbedding — no
test writes to the shared project index directory (data/index/) and no test
triggers a real model download unless INTEGRATION_TESTS=1 is set.

Test classes
------------
    TestConfigureSettings         — configure_settings() wiring with injected model
    TestGetEmbedModel             — get_embed_model() uses config model name (mocked HF)
    TestPipelineOrchestration     — run_indexing_pipeline() orchestration with mocked internals
    TestPipelineWithTextFile      — real pipeline on company_policy.txt with MockEmbedding
    TestIntegrationRealEmbedding  — gated by INTEGRATION_TESTS=1; real HF embedding
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from llama_index.core import Settings
from llama_index.core.embeddings.mock_embed_model import MockEmbedding

from src.core.config import config
from src.indexing.embed_config import configure_settings, get_embed_model
from src.indexing.index_builder import IndexManifest
from src.indexing.indexing_pipeline import run_indexing_pipeline

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EMBED = MockEmbedding(embed_dim=384)

_SAMPLE_TXT = Path(__file__).resolve().parent.parent / "docs" / "sample_docs" / "company_policy.txt"

_INTEGRATION = bool(os.environ.get("INTEGRATION_TESTS"))


def _make_manifest(index_dir: Path) -> IndexManifest:
    """Return a minimal IndexManifest for use as a mock return value."""
    import datetime

    return IndexManifest(
        run_id="test-run",
        built_at=datetime.datetime.now(datetime.timezone.utc),
        index_dir=str(index_dir),
        embedding_model="mock",
        parent_count=1,
        child_count=1,
        doc_ids=["doc-1"],
    )


# ---------------------------------------------------------------------------
# TestConfigureSettings
# ---------------------------------------------------------------------------


class TestConfigureSettings:
    """configure_settings() with an injected model — no HF import needed."""

    def setup_method(self) -> None:
        # Read the private field directly to avoid triggering the default model
        # resolver, which requires llama-index-embeddings-openai (not installed).
        # Settings is itself the singleton _Settings instance; _embed_model is
        # its private backing field.
        self._original_embed = getattr(Settings, "_embed_model", None)

    def teardown_method(self) -> None:
        # Restore the private field directly so we never call the getter/setter.
        Settings._embed_model = self._original_embed

    def test_returns_the_injected_model(self) -> None:
        result = configure_settings(embed_model=_EMBED)
        assert result is _EMBED

    def test_sets_settings_embed_model(self) -> None:
        configure_settings(embed_model=_EMBED)
        assert Settings.embed_model is _EMBED

    def test_returns_the_model_that_was_set(self) -> None:
        other = MockEmbedding(embed_dim=128)
        result = configure_settings(embed_model=other)
        assert result is other
        assert Settings.embed_model is other

    def test_does_not_call_get_embed_model_when_model_injected(self) -> None:
        with patch("src.indexing.embed_config.get_embed_model") as mock_get:
            configure_settings(embed_model=_EMBED)
            mock_get.assert_not_called()


# ---------------------------------------------------------------------------
# TestGetEmbedModel
# ---------------------------------------------------------------------------


class TestGetEmbedModel:
    """get_embed_model() — HuggingFaceEmbedding is mocked; no model download."""

    def test_uses_config_embedding_model_by_default(self) -> None:
        with patch("llama_index.embeddings.huggingface.HuggingFaceEmbedding") as mock_cls:
            mock_cls.return_value = _EMBED
            get_embed_model()
            mock_cls.assert_called_once_with(model_name=config.embedding_model)

    def test_accepts_explicit_model_name(self) -> None:
        with patch("llama_index.embeddings.huggingface.HuggingFaceEmbedding") as mock_cls:
            mock_cls.return_value = _EMBED
            get_embed_model(model_name="sentence-transformers/all-MiniLM-L6-v2")
            mock_cls.assert_called_once_with(
                model_name="sentence-transformers/all-MiniLM-L6-v2"
            )

    def test_explicit_model_name_overrides_config(self) -> None:
        override = "custom/model"
        with patch("llama_index.embeddings.huggingface.HuggingFaceEmbedding") as mock_cls:
            mock_cls.return_value = _EMBED
            get_embed_model(model_name=override)
            args, kwargs = mock_cls.call_args
            assert kwargs.get("model_name") == override

    def test_returns_embedding_instance(self) -> None:
        with patch("llama_index.embeddings.huggingface.HuggingFaceEmbedding") as mock_cls:
            mock_cls.return_value = _EMBED
            result = get_embed_model()
            assert result is _EMBED


# ---------------------------------------------------------------------------
# TestPipelineOrchestration
# ---------------------------------------------------------------------------


class TestPipelineOrchestration:
    """
    Unit tests for run_indexing_pipeline() orchestration logic.

    All three internal steps (route_file, build_hierarchical_chunks, build_indexes)
    are mocked — these tests only verify that the pipeline calls the right
    functions with the right arguments in the right order.
    """

    def _mock_chunks(self) -> tuple:
        """Return (parent_chunks, child_chunks) mock lists."""
        parent = MagicMock()
        child = MagicMock()
        return [parent], [child]

    def test_returns_index_manifest(self, tmp_path: Path) -> None:
        expected = _make_manifest(tmp_path)
        with (
            patch("src.indexing.indexing_pipeline.route_file", return_value=(MagicMock(), [])),
            patch("src.indexing.indexing_pipeline.build_hierarchical_chunks", return_value=self._mock_chunks()),
            patch("src.indexing.index_gateway.route_index", return_value=expected),
        ):
            result = run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        assert result is expected

    def test_route_file_called_with_path(self, tmp_path: Path) -> None:
        with (
            patch("src.indexing.indexing_pipeline.route_file", return_value=(MagicMock(), [])) as mock_route,
            patch("src.indexing.indexing_pipeline.build_hierarchical_chunks", return_value=self._mock_chunks()),
            patch("src.indexing.index_gateway.route_index", return_value=_make_manifest(tmp_path)),
        ):
            run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        mock_route.assert_called_once_with(Path(_SAMPLE_TXT))

    def test_build_hierarchical_chunks_receives_route_output(self, tmp_path: Path) -> None:
        raw_doc = MagicMock()
        pages = [MagicMock()]
        with (
            patch("src.indexing.indexing_pipeline.route_file", return_value=(raw_doc, pages)),
            patch("src.indexing.indexing_pipeline.build_hierarchical_chunks", return_value=self._mock_chunks()) as mock_chunk,
            patch("src.indexing.index_gateway.route_index", return_value=_make_manifest(tmp_path)),
        ):
            run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        mock_chunk.assert_called_once_with(raw_doc, pages)

    def test_build_indexes_receives_chunks_and_embed_model(self, tmp_path: Path) -> None:
        parents, children = self._mock_chunks()
        with (
            patch("src.indexing.indexing_pipeline.route_file", return_value=(MagicMock(), [])),
            patch("src.indexing.indexing_pipeline.build_hierarchical_chunks", return_value=(parents, children)),
            patch("src.indexing.index_gateway.route_index", return_value=_make_manifest(tmp_path)) as mock_route_idx,
        ):
            run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        mock_route_idx.assert_called_once_with(
            parents,
            children,
            index_dir=tmp_path,
            embed_model=_EMBED,
        )

    def test_explicit_embed_model_bypasses_get_embed_model(self, tmp_path: Path) -> None:
        with (
            patch("src.indexing.indexing_pipeline.route_file", return_value=(MagicMock(), [])),
            patch("src.indexing.indexing_pipeline.build_hierarchical_chunks", return_value=self._mock_chunks()),
            patch("src.indexing.index_gateway.route_index", return_value=_make_manifest(tmp_path)),
            patch("src.indexing.indexing_pipeline.get_embed_model") as mock_get,
        ):
            run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        mock_get.assert_not_called()

    def test_get_embed_model_called_when_embed_model_is_none(self, tmp_path: Path) -> None:
        with (
            patch("src.indexing.indexing_pipeline.route_file", return_value=(MagicMock(), [])),
            patch("src.indexing.indexing_pipeline.build_hierarchical_chunks", return_value=self._mock_chunks()),
            patch("src.indexing.index_gateway.route_index", return_value=_make_manifest(tmp_path)),
            patch("src.indexing.indexing_pipeline.get_embed_model", return_value=_EMBED) as mock_get,
        ):
            run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=None)
        mock_get.assert_called_once()

    def test_file_path_is_coerced_to_path(self, tmp_path: Path) -> None:
        """Passing a string file path is accepted and converted to Path."""
        with (
            patch("src.indexing.indexing_pipeline.route_file", return_value=(MagicMock(), [])) as mock_route,
            patch("src.indexing.indexing_pipeline.build_hierarchical_chunks", return_value=self._mock_chunks()),
            patch("src.indexing.index_gateway.route_index", return_value=_make_manifest(tmp_path)),
        ):
            run_indexing_pipeline(str(_SAMPLE_TXT), index_dir=tmp_path, embed_model=_EMBED)
        called_path = mock_route.call_args[0][0]
        assert isinstance(called_path, Path)


# ---------------------------------------------------------------------------
# TestPipelineWithTextFile
# ---------------------------------------------------------------------------


class TestPipelineWithTextFile:
    """
    Full end-to-end pipeline run on a real .txt file using MockEmbedding.

    Uses company_policy.txt (text reader — no Docling, no OCR, no model warm-up).
    index_dir is always tmp_path — never writes to data/index/.
    """

    def test_sample_file_exists(self) -> None:
        assert _SAMPLE_TXT.exists(), f"Sample file not found: {_SAMPLE_TXT}"

    def test_returns_index_manifest(self, tmp_path: Path) -> None:
        manifest = run_indexing_pipeline(
            _SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED
        )
        assert isinstance(manifest, IndexManifest)

    def test_child_index_directory_created(self, tmp_path: Path) -> None:
        run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "child_index").is_dir()

    def test_parent_store_created(self, tmp_path: Path) -> None:
        run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "parent_store" / "docstore.json").is_file()

    def test_manifest_written_to_isolated_dir(self, tmp_path: Path) -> None:
        run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "build_manifest.json").is_file()

    def test_manifest_has_nonzero_counts(self, tmp_path: Path) -> None:
        manifest = run_indexing_pipeline(
            _SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED
        )
        assert manifest.parent_count > 0
        assert manifest.child_count > 0

    def test_manifest_doc_ids_nonempty(self, tmp_path: Path) -> None:
        manifest = run_indexing_pipeline(
            _SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED
        )
        assert len(manifest.doc_ids) > 0

    def test_index_dir_is_not_project_default(self, tmp_path: Path) -> None:
        """Confirm test output never lands in the real project index directory."""
        manifest = run_indexing_pipeline(
            _SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED
        )
        assert Path(manifest.index_dir) == tmp_path
        assert Path(manifest.index_dir) != config.index_dir

    def test_project_index_dir_not_created_by_test(self, tmp_path: Path) -> None:
        """The pipeline must not write to data/index/ when index_dir=tmp_path is explicit."""
        already_existed = config.index_dir.exists()
        run_indexing_pipeline(_SAMPLE_TXT, index_dir=tmp_path, embed_model=_EMBED)
        # Positive assertion: the manifest was written to tmp_path, not elsewhere.
        assert (tmp_path / "build_manifest.json").exists()
        # Negative assertion: if data/index/ did not exist before this test it
        # must not have been created as a side-effect of running with
        # index_dir=tmp_path (proving the pipeline respects explicit injection).
        # When data/index/ already exists (e.g. from running
        # scripts/bootstrap_index.py), this pre-condition check is not
        # meaningful — isolation is proven by the manifest-path assertion above
        # and by test_index_dir_is_not_project_default.
        if not already_existed:
            assert not config.index_dir.exists()

    def test_pipeline_produces_consistent_structure(self, tmp_path: Path) -> None:
        """Same file → same parent/child counts and same doc_id count on consecutive runs.

        Note: doc_id values differ between runs (UUID generated per route_file call).
        Structural consistency — counts and document cardinality — is verified here.
        """
        dir_a = tmp_path / "run_a"
        dir_b = tmp_path / "run_b"
        m_a = run_indexing_pipeline(_SAMPLE_TXT, index_dir=dir_a, embed_model=_EMBED)
        m_b = run_indexing_pipeline(_SAMPLE_TXT, index_dir=dir_b, embed_model=_EMBED)
        assert m_a.parent_count == m_b.parent_count
        assert m_a.child_count == m_b.child_count
        assert len(m_a.doc_ids) == len(m_b.doc_ids)


# ---------------------------------------------------------------------------
# TestIntegrationRealEmbedding
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _INTEGRATION,
    reason="Set INTEGRATION_TESTS=1 to run real-embedding integration tests.",
)
class TestIntegrationRealEmbedding:
    """
    Real HuggingFace embedding, real text file, isolated tmp_path output.

    Requires:
        - INTEGRATION_TESTS=1 environment variable
        - llama-index-embeddings-huggingface installed
        - sentence-transformers installed
        - Internet access for first-time model download

    Uses a small HuggingFace model to keep the test fast relative to
    the Qwen3-Embedding-0.6B production model. The test verifies that
    get_embed_model() and run_indexing_pipeline() work end-to-end with
    a real embedding model and produce a valid, loadable index.
    """

    _SMALL_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

    def test_real_embedding_pipeline_end_to_end(self, tmp_path: Path) -> None:
        real_embed = get_embed_model(model_name=self._SMALL_MODEL)

        manifest = run_indexing_pipeline(
            _SAMPLE_TXT,
            index_dir=tmp_path,
            embed_model=real_embed,
        )

        assert isinstance(manifest, IndexManifest)
        assert manifest.parent_count > 0
        assert manifest.child_count > 0
        assert (tmp_path / "child_index").is_dir()
        assert (tmp_path / "parent_store" / "docstore.json").is_file()
        assert Path(manifest.index_dir) == tmp_path
        # Confirm we never touched the real project index directory.
        assert Path(manifest.index_dir) != config.index_dir
