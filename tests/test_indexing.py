"""
Tests for src/indexing/index_builder.py

All tests use synthetic DocumentChunk fixtures and MockEmbedding — no
document parsing, no model downloads, no file I/O beyond tmp_path.

Test classes
------------
    TestBuildManifest          — IndexManifest fields populated correctly
    TestParentStorePersisted   — parent_store/docstore.json created and populated
    TestChildIndexPersisted    — child_index/ directory and expected files created
    TestMetadataPreserved      — all 9 required metadata fields on nodes and docs
    TestParentChunkLinkage     — parent_chunk_id on child nodes matches parent
    TestDeterministicBuild     — same input → same node/doc IDs on rebuild
    TestEmptyInputs            — build with empty lists succeeds (counts = 0)
    TestLegacyChromaUntouched  — legacy chroma_db is not modified by indexing
    TestLoadChildIndex         — load_child_index() returns correct type
    TestLoadParentStore        — load_parent_store() returns correct doc count
    TestManifestDocIds         — doc_ids deduplicated across parents and children
    TestLoadErrors             — load functions raise FileNotFoundError when missing
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.embeddings.mock_embed_model import MockEmbedding
from llama_index.core.storage.docstore import SimpleDocumentStore

from src.core.config import config
from src.indexing.index_builder import (
    IndexManifest,
    build_indexes,
    load_child_index,
    load_parent_store,
)
from src.schema.models import DocumentChunk

# ---------------------------------------------------------------------------
# Shared test fixtures
# ---------------------------------------------------------------------------

_EMBED = MockEmbedding(embed_dim=384)  # no model download; deterministic vectors

_REQUIRED_META_KEYS = {
    "chunk_id",
    "doc_id",
    "page_id",
    "page_number",
    "file_name",
    "file_type",
    "section_title",
    "chunk_level",
    "parent_chunk_id",
}


def _make_parent(
    chunk_id: str = "par-001",
    doc_id: str = "doc-1",
    page_id: str = "pg-1",
    page_number: int = 1,
    section_title: str | None = "Introduction",
) -> DocumentChunk:
    return DocumentChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id=page_id,
        page_number=page_number,
        file_name="sample.pdf",
        file_type="pdf",
        section_title=section_title,
        text="Parent chunk text providing broad synthesis context for the section.",
        word_count=12,
        chunk_index=0,
        chunk_level="parent",
        parent_chunk_id=None,
    )


def _make_child(
    chunk_id: str = "chi-001",
    doc_id: str = "doc-1",
    page_id: str = "pg-1",
    parent_chunk_id: str = "par-001",
    page_number: int = 1,
    section_title: str | None = "Introduction",
) -> DocumentChunk:
    return DocumentChunk(
        chunk_id=chunk_id,
        doc_id=doc_id,
        page_id=page_id,
        page_number=page_number,
        file_name="sample.pdf",
        file_type="pdf",
        section_title=section_title,
        text="Child chunk text used as the fine-grained retrieval unit.",
        word_count=11,
        chunk_index=0,
        chunk_level="child",
        parent_chunk_id=parent_chunk_id,
    )


# ---------------------------------------------------------------------------
# TestBuildManifest
# ---------------------------------------------------------------------------


class TestBuildManifest:
    def test_returns_index_manifest(self, tmp_path: Path) -> None:
        manifest = build_indexes(
            [_make_parent()], [_make_child()],
            index_dir=tmp_path, embed_model=_EMBED,
        )
        assert isinstance(manifest, IndexManifest)

    def test_parent_count_matches_input(self, tmp_path: Path) -> None:
        parents = [_make_parent("p1"), _make_parent("p2")]
        children = [_make_child("c1", parent_chunk_id="p1")]
        manifest = build_indexes(parents, children, index_dir=tmp_path, embed_model=_EMBED)
        assert manifest.parent_count == 2

    def test_child_count_matches_input(self, tmp_path: Path) -> None:
        parents = [_make_parent()]
        children = [_make_child("c1"), _make_child("c2")]
        manifest = build_indexes(parents, children, index_dir=tmp_path, embed_model=_EMBED)
        assert manifest.child_count == 2

    def test_manifest_has_built_at(self, tmp_path: Path) -> None:
        from datetime import datetime
        manifest = build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        assert isinstance(manifest.built_at, datetime)

    def test_manifest_index_dir_is_string_of_tmp_path(self, tmp_path: Path) -> None:
        manifest = build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        assert manifest.index_dir == str(tmp_path)

    def test_manifest_json_written_to_disk(self, tmp_path: Path) -> None:
        build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        manifest_path = tmp_path / "build_manifest.json"
        assert manifest_path.exists()
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert "parent_count" in data
        assert "child_count" in data


# ---------------------------------------------------------------------------
# TestParentStorePersisted
# ---------------------------------------------------------------------------


class TestParentStorePersisted:
    def test_docstore_json_exists_after_build(self, tmp_path: Path) -> None:
        build_indexes([_make_parent()], [], index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "parent_store" / "docstore.json").exists()

    def test_parent_doc_count_correct(self, tmp_path: Path) -> None:
        parents = [_make_parent("p1"), _make_parent("p2")]
        build_indexes(parents, [], index_dir=tmp_path, embed_model=_EMBED)
        store = load_parent_store(tmp_path)
        assert len(store.docs) == 2

    def test_parent_doc_keyed_by_chunk_id(self, tmp_path: Path) -> None:
        parent = _make_parent("par-xyz")
        build_indexes([parent], [], index_dir=tmp_path, embed_model=_EMBED)
        store = load_parent_store(tmp_path)
        assert "par-xyz" in store.docs


# ---------------------------------------------------------------------------
# TestChildIndexPersisted
# ---------------------------------------------------------------------------


class TestChildIndexPersisted:
    def test_child_index_dir_created(self, tmp_path: Path) -> None:
        build_indexes([], [_make_child()], index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "child_index").is_dir()

    def test_vector_store_json_exists(self, tmp_path: Path) -> None:
        build_indexes([], [_make_child()], index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "child_index" / "default__vector_store.json").exists()

    def test_index_store_json_exists(self, tmp_path: Path) -> None:
        build_indexes([], [_make_child()], index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "child_index" / "index_store.json").exists()

    def test_docstore_json_exists(self, tmp_path: Path) -> None:
        build_indexes([], [_make_child()], index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "child_index" / "docstore.json").exists()


# ---------------------------------------------------------------------------
# TestMetadataPreserved
# ---------------------------------------------------------------------------


class TestMetadataPreserved:
    def test_all_nine_fields_on_child_node(self, tmp_path: Path) -> None:
        child = _make_child()
        build_indexes([], [child], index_dir=tmp_path, embed_model=_EMBED)
        sc = StorageContext.from_defaults(persist_dir=str(tmp_path / "child_index"))
        node = sc.docstore.docs[child.chunk_id]
        assert _REQUIRED_META_KEYS <= set(node.metadata.keys())

    def test_child_metadata_values_correct(self, tmp_path: Path) -> None:
        child = _make_child(chunk_id="c99", doc_id="d99", page_id="pg-99",
                            page_number=3, section_title="Results")
        build_indexes([], [child], index_dir=tmp_path, embed_model=_EMBED)
        sc = StorageContext.from_defaults(persist_dir=str(tmp_path / "child_index"))
        node = sc.docstore.docs["c99"]
        assert node.metadata["doc_id"] == "d99"
        assert node.metadata["page_id"] == "pg-99"
        assert node.metadata["page_number"] == 3
        assert node.metadata["file_name"] == "sample.pdf"
        assert node.metadata["file_type"] == "pdf"
        assert node.metadata["section_title"] == "Results"
        assert node.metadata["chunk_level"] == "child"

    def test_all_nine_fields_on_parent_doc(self, tmp_path: Path) -> None:
        parent = _make_parent()
        build_indexes([parent], [], index_dir=tmp_path, embed_model=_EMBED)
        store = load_parent_store(tmp_path)
        doc = store.docs[parent.chunk_id]
        assert _REQUIRED_META_KEYS <= set(doc.metadata.keys())

    def test_section_title_none_stored_as_empty_string(self, tmp_path: Path) -> None:
        child = _make_child(section_title=None)
        build_indexes([], [child], index_dir=tmp_path, embed_model=_EMBED)
        sc = StorageContext.from_defaults(persist_dir=str(tmp_path / "child_index"))
        node = sc.docstore.docs[child.chunk_id]
        assert node.metadata["section_title"] == ""


# ---------------------------------------------------------------------------
# TestParentChunkLinkage
# ---------------------------------------------------------------------------


class TestParentChunkLinkage:
    def test_child_parent_chunk_id_preserved(self, tmp_path: Path) -> None:
        parent = _make_parent(chunk_id="par-A")
        child = _make_child(chunk_id="chi-A", parent_chunk_id="par-A")
        build_indexes([parent], [child], index_dir=tmp_path, embed_model=_EMBED)
        sc = StorageContext.from_defaults(persist_dir=str(tmp_path / "child_index"))
        node = sc.docstore.docs["chi-A"]
        assert node.metadata["parent_chunk_id"] == "par-A"

    def test_parent_has_empty_parent_chunk_id(self, tmp_path: Path) -> None:
        parent = _make_parent()
        build_indexes([parent], [], index_dir=tmp_path, embed_model=_EMBED)
        store = load_parent_store(tmp_path)
        doc = store.docs[parent.chunk_id]
        assert doc.metadata["parent_chunk_id"] == ""


# ---------------------------------------------------------------------------
# TestDeterministicBuild
# ---------------------------------------------------------------------------


class TestDeterministicBuild:
    def test_same_chunks_same_child_node_ids_on_rebuild(self, tmp_path: Path) -> None:
        child = _make_child(chunk_id="det-chi-001")
        build_indexes([], [child], index_dir=tmp_path / "r1", embed_model=_EMBED)
        build_indexes([], [child], index_dir=tmp_path / "r2", embed_model=_EMBED)
        sc1 = StorageContext.from_defaults(persist_dir=str(tmp_path / "r1" / "child_index"))
        sc2 = StorageContext.from_defaults(persist_dir=str(tmp_path / "r2" / "child_index"))
        assert set(sc1.docstore.docs.keys()) == set(sc2.docstore.docs.keys())

    def test_same_chunks_same_parent_doc_ids_on_rebuild(self, tmp_path: Path) -> None:
        parent = _make_parent(chunk_id="det-par-001")
        build_indexes([parent], [], index_dir=tmp_path / "r1", embed_model=_EMBED)
        build_indexes([parent], [], index_dir=tmp_path / "r2", embed_model=_EMBED)
        s1 = load_parent_store(tmp_path / "r1")
        s2 = load_parent_store(tmp_path / "r2")
        assert set(s1.docs.keys()) == set(s2.docs.keys())


# ---------------------------------------------------------------------------
# TestEmptyInputs
# ---------------------------------------------------------------------------


class TestEmptyInputs:
    def test_empty_lists_build_without_error(self, tmp_path: Path) -> None:
        manifest = build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        assert manifest is not None

    def test_empty_manifest_counts_are_zero(self, tmp_path: Path) -> None:
        manifest = build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        assert manifest.parent_count == 0
        assert manifest.child_count == 0

    def test_empty_build_creates_parent_store_file(self, tmp_path: Path) -> None:
        build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        assert (tmp_path / "parent_store" / "docstore.json").exists()

    def test_empty_parent_store_has_zero_docs(self, tmp_path: Path) -> None:
        build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        store = load_parent_store(tmp_path)
        assert len(store.docs) == 0


# ---------------------------------------------------------------------------
# TestLegacyChromaUntouched
# ---------------------------------------------------------------------------


class TestLegacyChromaUntouched:
    def test_chroma_sqlite_not_modified_by_build(self, tmp_path: Path) -> None:
        chroma_path = config.legacy_chroma_dir / "chroma.sqlite3"
        if not chroma_path.exists():
            pytest.skip("Legacy chroma.sqlite3 not present in this environment")
        mtime_before = chroma_path.stat().st_mtime
        build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        mtime_after = chroma_path.stat().st_mtime
        assert mtime_before == mtime_after, (
            "build_indexes() must not touch the legacy chroma_db"
        )


# ---------------------------------------------------------------------------
# TestLoadChildIndex
# ---------------------------------------------------------------------------


class TestLoadChildIndex:
    def test_load_returns_vector_store_index(self, tmp_path: Path) -> None:
        build_indexes([], [_make_child()], index_dir=tmp_path, embed_model=_EMBED)
        idx = load_child_index(tmp_path, embed_model=_EMBED)
        assert isinstance(idx, VectorStoreIndex)

    def test_load_raises_file_not_found_if_not_built(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_child_index(tmp_path, embed_model=_EMBED)


# ---------------------------------------------------------------------------
# TestLoadParentStore
# ---------------------------------------------------------------------------


class TestLoadParentStore:
    def test_load_returns_simple_document_store(self, tmp_path: Path) -> None:
        build_indexes([_make_parent()], [], index_dir=tmp_path, embed_model=_EMBED)
        store = load_parent_store(tmp_path)
        assert isinstance(store, SimpleDocumentStore)

    def test_load_returns_correct_doc_count(self, tmp_path: Path) -> None:
        parents = [_make_parent("p1"), _make_parent("p2"), _make_parent("p3")]
        build_indexes(parents, [], index_dir=tmp_path, embed_model=_EMBED)
        store = load_parent_store(tmp_path)
        assert len(store.docs) == 3

    def test_load_raises_file_not_found_if_not_built(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_parent_store(tmp_path)


# ---------------------------------------------------------------------------
# TestManifestDocIds
# ---------------------------------------------------------------------------


class TestManifestDocIds:
    def test_doc_ids_deduplicated(self, tmp_path: Path) -> None:
        parents = [_make_parent("p1", doc_id="doc-A")]
        children = [
            _make_child("c1", doc_id="doc-A"),
            _make_child("c2", doc_id="doc-B"),
        ]
        manifest = build_indexes(parents, children, index_dir=tmp_path, embed_model=_EMBED)
        assert sorted(manifest.doc_ids) == ["doc-A", "doc-B"]
        assert len(manifest.doc_ids) == len(set(manifest.doc_ids))

    def test_doc_ids_empty_when_no_chunks(self, tmp_path: Path) -> None:
        manifest = build_indexes([], [], index_dir=tmp_path, embed_model=_EMBED)
        assert manifest.doc_ids == []
