"""
Tests for the hierarchical chunker (src/chunking/hierarchical_chunker.py).

All tests use synthetic ParsedPage fixtures — no file I/O, no Docling.
The full parsing pipeline is exercised by test_parsing.py and test_ingestion.py.

Test classes:
    TestContract               — public API returns correct types
    TestParentChunks           — parent chunk fields and invariants
    TestChildChunks            — child chunk fields and parent linkage
    TestPageLinkage            — doc_id, page_id, page_number, file_name, file_type
    TestEmptyPages             — empty pages produce zero chunks
    TestWeakPages              — weak pages produce at least 1 parent + 1 child
    TestFallbackPath           — no layout blocks → unstructured path works correctly
    TestStructuredPath         — layout blocks with headings → structured path
    TestDeterminism            — same input → identical chunk_ids and text
    TestSectionTitle           — section_title propagated correctly
    TestMultiPage              — multi-page documents maintain correct linkage
    TestConfigOverrides        — per-call size overrides work
"""
from __future__ import annotations

from typing import List, Optional

import pytest

from src.chunking.hierarchical_chunker import build_hierarchical_chunks
from src.schema.models import DocumentChunk, ParsedBlock, ParsedPage, RawDocument

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _raw_doc(file_name: str = "test.pdf", file_type: str = "pdf") -> RawDocument:
    return RawDocument(
        source_path=f"/tmp/{file_name}",
        file_name=file_name,
        file_type=file_type,
        byte_size=1024,
        checksum="a" * 64,
    )


def _page(
    doc_id: str,
    page_number: int = 1,
    text: str = "",
    status: str = "ok",
    section_title: Optional[str] = None,
    blocks: Optional[List[ParsedBlock]] = None,
) -> ParsedPage:
    """Create a synthetic ParsedPage."""
    norm = text.strip()
    wc = len(norm.split()) if norm else 0
    return ParsedPage(
        doc_id=doc_id,
        page_number=page_number,
        raw_text=text,
        normalized_text=norm,
        word_count=wc,
        char_count=len(norm),
        parse_method="pypdf",
        extraction_status=status,
        section_title=section_title,
        layout_blocks=blocks or [],
    )


def _long_text(n_words: int = 500) -> str:
    """Generate deterministic text with n_words words."""
    words = [f"word{i % 100}" for i in range(n_words)]
    return " ".join(words)


def _block(
    doc_id: str,
    page_number: int,
    block_type: str,
    text: str,
    reading_order: int = 0,
) -> ParsedBlock:
    return ParsedBlock(
        doc_id=doc_id,
        page_number=page_number,
        block_type=block_type,
        text=text,
        reading_order=reading_order,
    )


# ---------------------------------------------------------------------------
# TestContract
# ---------------------------------------------------------------------------


class TestContract:
    def test_returns_tuple_of_two_lists(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        result = build_hierarchical_chunks(doc, [page])
        assert isinstance(result, tuple)
        assert len(result) == 2
        parents, children = result
        assert isinstance(parents, list)
        assert isinstance(children, list)

    def test_all_parents_are_document_chunks(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert all(isinstance(c, DocumentChunk) for c in parents)

    def test_all_children_are_document_chunks(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        _, children = build_hierarchical_chunks(doc, [page])
        assert all(isinstance(c, DocumentChunk) for c in children)

    def test_empty_page_list(self):
        doc = _raw_doc()
        parents, children = build_hierarchical_chunks(doc, [])
        assert parents == []
        assert children == []


# ---------------------------------------------------------------------------
# TestParentChunks
# ---------------------------------------------------------------------------


class TestParentChunks:
    def test_parent_chunk_level(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert all(p.chunk_level == "parent" for p in parents)

    def test_parent_chunk_id_is_none(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert all(p.parent_chunk_id is None for p in parents)

    def test_parent_text_non_empty(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert all(p.text.strip() for p in parents)

    def test_parent_word_count_positive(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert all(p.word_count > 0 for p in parents)

    def test_parent_chunk_ids_unique(self):
        doc = _raw_doc()
        # Use enough text to produce multiple parents
        page = _page(doc.doc_id, text=_long_text(900))
        parents, _ = build_hierarchical_chunks(
            doc, [page], parent_chunk_size_words=200
        )
        ids = [p.chunk_id for p in parents]
        assert len(ids) == len(set(ids)), "parent chunk_ids must be unique"


# ---------------------------------------------------------------------------
# TestChildChunks
# ---------------------------------------------------------------------------


class TestChildChunks:
    def test_child_chunk_level(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(200))
        _, children = build_hierarchical_chunks(doc, [page])
        assert all(c.chunk_level == "child" for c in children)

    def test_child_has_parent_chunk_id(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(200))
        _, children = build_hierarchical_chunks(doc, [page])
        assert all(c.parent_chunk_id is not None for c in children)

    def test_child_parent_chunk_id_valid(self):
        """Every child's parent_chunk_id must match a real parent chunk_id."""
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(500))
        parents, children = build_hierarchical_chunks(
            doc, [page], parent_chunk_size_words=200
        )
        parent_ids = {p.chunk_id for p in parents}
        for child in children:
            assert child.parent_chunk_id in parent_ids, (
                f"child.parent_chunk_id={child.parent_chunk_id!r} "
                f"not found in parent ids"
            )

    def test_child_text_non_empty(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(200))
        _, children = build_hierarchical_chunks(doc, [page])
        assert all(c.text.strip() for c in children)

    def test_child_chunk_ids_unique(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(600))
        _, children = build_hierarchical_chunks(
            doc, [page], parent_chunk_size_words=200, child_chunk_size_words=80
        )
        ids = [c.chunk_id for c in children]
        assert len(ids) == len(set(ids)), "child chunk_ids must be unique"

    def test_parent_and_child_ids_disjoint(self):
        """Parent chunk_ids and child chunk_ids must not overlap."""
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(600))
        parents, children = build_hierarchical_chunks(
            doc, [page], parent_chunk_size_words=200
        )
        parent_ids = {p.chunk_id for p in parents}
        child_ids = {c.chunk_id for c in children}
        assert parent_ids.isdisjoint(child_ids)


# ---------------------------------------------------------------------------
# TestPageLinkage
# ---------------------------------------------------------------------------


class TestPageLinkage:
    def test_doc_id_preserved_in_parents(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert all(p.doc_id == doc.doc_id for p in parents)

    def test_doc_id_preserved_in_children(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        _, children = build_hierarchical_chunks(doc, [page])
        assert all(c.doc_id == doc.doc_id for c in children)

    def test_page_number_preserved(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, page_number=7, text=_long_text(100))
        parents, children = build_hierarchical_chunks(doc, [page])
        assert all(p.page_number == 7 for p in parents)
        assert all(c.page_number == 7 for c in children)

    def test_file_name_preserved(self):
        doc = _raw_doc(file_name="report.pdf")
        page = _page(doc.doc_id, text=_long_text(100))
        parents, children = build_hierarchical_chunks(doc, [page])
        assert all(p.file_name == "report.pdf" for p in parents)
        assert all(c.file_name == "report.pdf" for c in children)

    def test_file_type_preserved(self):
        doc = _raw_doc(file_type="docx")
        page = _page(doc.doc_id, text=_long_text(100))
        parents, children = build_hierarchical_chunks(doc, [page])
        assert all(p.file_type == "docx" for p in parents)
        assert all(c.file_type == "docx" for c in children)

    def test_page_id_preserved(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100))
        page_id = page.page_id
        parents, children = build_hierarchical_chunks(doc, [page])
        assert all(p.page_id == page_id for p in parents)
        assert all(c.page_id == page_id for c in children)


# ---------------------------------------------------------------------------
# TestEmptyPages
# ---------------------------------------------------------------------------


class TestEmptyPages:
    def test_empty_page_produces_no_parents(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text="", status="empty")
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert parents == []

    def test_empty_page_produces_no_children(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text="", status="empty")
        _, children = build_hierarchical_chunks(doc, [page])
        assert children == []

    def test_mixed_empty_and_ok(self):
        """Empty page contributes 0 chunks; ok page contributes chunks."""
        doc = _raw_doc()
        empty = _page(doc.doc_id, page_number=1, text="", status="empty")
        ok = _page(doc.doc_id, page_number=2, text=_long_text(100))
        parents, children = build_hierarchical_chunks(doc, [empty, ok])
        assert len(parents) >= 1
        assert len(children) >= 1
        assert all(c.page_number == 2 for c in parents)


# ---------------------------------------------------------------------------
# TestWeakPages
# ---------------------------------------------------------------------------


class TestWeakPages:
    def test_weak_page_produces_at_least_one_parent(self):
        doc = _raw_doc()
        # 5 words — below weak threshold (20), but text is non-empty
        page = _page(doc.doc_id, text="short text with few words", status="weak")
        parents, _ = build_hierarchical_chunks(doc, [page])
        assert len(parents) >= 1

    def test_weak_page_produces_at_least_one_child(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text="short text with few words", status="weak")
        _, children = build_hierarchical_chunks(doc, [page])
        assert len(children) >= 1

    def test_weak_page_child_links_to_parent(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text="short text with few words", status="weak")
        parents, children = build_hierarchical_chunks(doc, [page])
        parent_ids = {p.chunk_id for p in parents}
        for child in children:
            assert child.parent_chunk_id in parent_ids


# ---------------------------------------------------------------------------
# TestFallbackPath
# ---------------------------------------------------------------------------


class TestFallbackPath:
    def test_no_blocks_uses_fallback(self):
        """Page with no layout_blocks must still produce chunks."""
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(200), blocks=[])
        parents, children = build_hierarchical_chunks(doc, [page])
        assert len(parents) >= 1
        assert len(children) >= 1

    def test_blocks_without_headings_uses_fallback(self):
        """Blocks present but no headings → fallback (word-window) path."""
        doc = _raw_doc()
        blocks = [
            _block(doc.doc_id, 1, "paragraph", "first paragraph text.", 0),
            _block(doc.doc_id, 1, "paragraph", "second paragraph text.", 1),
        ]
        page = _page(doc.doc_id, text=_long_text(200), blocks=blocks)
        parents, children = build_hierarchical_chunks(doc, [page])
        assert len(parents) >= 1
        assert len(children) >= 1

    def test_fallback_section_title_from_page(self):
        """In the fallback path, section_title comes from ParsedPage.section_title."""
        doc = _raw_doc()
        page = _page(
            doc.doc_id,
            text=_long_text(100),
            section_title="My Section",
            blocks=[],
        )
        parents, children = build_hierarchical_chunks(doc, [page])
        assert all(p.section_title == "My Section" for p in parents)
        assert all(c.section_title == "My Section" for c in children)


# ---------------------------------------------------------------------------
# TestStructuredPath
# ---------------------------------------------------------------------------


class TestStructuredPath:
    def test_headings_trigger_structured_path(self):
        """A page with heading blocks should produce chunks from the structured path."""
        doc = _raw_doc()
        blocks = [
            _block(doc.doc_id, 1, "heading", "Section One", 0),
            _block(doc.doc_id, 1, "paragraph", "Content under section one. " * 20, 1),
            _block(doc.doc_id, 1, "heading", "Section Two", 2),
            _block(doc.doc_id, 1, "paragraph", "Content under section two. " * 20, 3),
        ]
        page = _page(doc.doc_id, text="", blocks=blocks)
        parents, children = build_hierarchical_chunks(doc, [page])
        assert len(parents) >= 1
        assert len(children) >= 1

    def test_structured_section_title_from_block(self):
        """section_title on parent comes from the block heading, not page.section_title."""
        doc = _raw_doc()
        blocks = [
            _block(doc.doc_id, 1, "heading", "Block Heading", 0),
            _block(doc.doc_id, 1, "paragraph", "paragraph content " * 30, 1),
        ]
        page = _page(
            doc.doc_id,
            text="",
            section_title="Page Level Title",  # different from block heading
            blocks=blocks,
        )
        parents, children = build_hierarchical_chunks(doc, [page])
        # Structured path should use "Block Heading" from the block, not "Page Level Title"
        assert any(p.section_title == "Block Heading" for p in parents)

    def test_children_inherit_parent_section_title(self):
        doc = _raw_doc()
        blocks = [
            _block(doc.doc_id, 1, "heading", "My Heading", 0),
            _block(doc.doc_id, 1, "paragraph", "body text " * 50, 1),
        ]
        page = _page(doc.doc_id, text="", blocks=blocks)
        parents, children = build_hierarchical_chunks(doc, [page])
        # All children of a parent with section_title should share that section_title
        for parent in parents:
            parent_children = [c for c in children if c.parent_chunk_id == parent.chunk_id]
            for child in parent_children:
                assert child.section_title == parent.section_title


# ---------------------------------------------------------------------------
# TestDeterminism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_input_same_chunk_ids(self):
        """Two calls with identical input must produce identical chunk_ids."""
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(500))

        parents_a, children_a = build_hierarchical_chunks(doc, [page])
        parents_b, children_b = build_hierarchical_chunks(doc, [page])

        assert [p.chunk_id for p in parents_a] == [p.chunk_id for p in parents_b]
        assert [c.chunk_id for c in children_a] == [c.chunk_id for c in children_b]

    def test_same_input_same_text(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(500))

        parents_a, children_a = build_hierarchical_chunks(doc, [page])
        parents_b, children_b = build_hierarchical_chunks(doc, [page])

        assert [p.text for p in parents_a] == [p.text for p in parents_b]
        assert [c.text for c in children_a] == [c.text for c in children_b]

    def test_different_doc_ids_different_chunk_ids(self):
        """Two documents with the same text must produce different chunk_ids."""
        doc_a = _raw_doc()
        doc_b = _raw_doc()
        text = _long_text(200)
        page_a = _page(doc_a.doc_id, text=text)
        page_b = _page(doc_b.doc_id, text=text)

        parents_a, _ = build_hierarchical_chunks(doc_a, [page_a])
        parents_b, _ = build_hierarchical_chunks(doc_b, [page_b])

        ids_a = {p.chunk_id for p in parents_a}
        ids_b = {p.chunk_id for p in parents_b}
        assert ids_a.isdisjoint(ids_b), "Different docs must produce different chunk_ids"


# ---------------------------------------------------------------------------
# TestSectionTitle
# ---------------------------------------------------------------------------


class TestSectionTitle:
    def test_section_title_none_when_absent(self):
        """No section_title on page and no heading blocks → section_title=None."""
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(100), section_title=None, blocks=[])
        parents, children = build_hierarchical_chunks(doc, [page])
        assert all(p.section_title is None for p in parents)
        assert all(c.section_title is None for c in children)

    def test_section_title_propagated_to_children(self):
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(200), section_title="Overview")
        parents, children = build_hierarchical_chunks(doc, [page])
        for parent in parents:
            my_children = [c for c in children if c.parent_chunk_id == parent.chunk_id]
            for child in my_children:
                assert child.section_title == parent.section_title


# ---------------------------------------------------------------------------
# TestMultiPage
# ---------------------------------------------------------------------------


class TestMultiPage:
    def test_multi_page_total_chunk_count(self):
        doc = _raw_doc()
        pages = [
            _page(doc.doc_id, page_number=i, text=_long_text(100))
            for i in range(1, 4)
        ]
        parents, children = build_hierarchical_chunks(doc, pages)
        assert len(parents) >= 3  # at least 1 parent per page
        assert len(children) >= 3  # at least 1 child per page

    def test_multi_page_page_number_correct(self):
        doc = _raw_doc()
        pages = [
            _page(doc.doc_id, page_number=1, text=_long_text(100)),
            _page(doc.doc_id, page_number=2, text=_long_text(100)),
        ]
        parents, children = build_hierarchical_chunks(doc, pages)
        parent_page_numbers = {p.page_number for p in parents}
        child_page_numbers = {c.page_number for c in children}
        assert 1 in parent_page_numbers
        assert 2 in parent_page_numbers
        assert 1 in child_page_numbers
        assert 2 in child_page_numbers

    def test_multi_page_all_ids_unique(self):
        doc = _raw_doc()
        pages = [
            _page(doc.doc_id, page_number=i, text=_long_text(200))
            for i in range(1, 5)
        ]
        parents, children = build_hierarchical_chunks(doc, pages)
        all_ids = [p.chunk_id for p in parents] + [c.chunk_id for c in children]
        assert len(all_ids) == len(set(all_ids)), "All chunk_ids must be unique across pages"


# ---------------------------------------------------------------------------
# TestConfigOverrides
# ---------------------------------------------------------------------------


class TestConfigOverrides:
    def test_parent_size_override(self):
        """Smaller parent_chunk_size_words → more parent chunks."""
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(600))

        parents_default, _ = build_hierarchical_chunks(
            doc, [page], parent_chunk_size_words=400
        )
        parents_small, _ = build_hierarchical_chunks(
            doc, [page], parent_chunk_size_words=100
        )
        assert len(parents_small) > len(parents_default)

    def test_child_size_override(self):
        """Smaller child_chunk_size_words → more child chunks per parent."""
        doc = _raw_doc()
        page = _page(doc.doc_id, text=_long_text(400))

        _, children_default = build_hierarchical_chunks(
            doc, [page], child_chunk_size_words=150, child_chunk_overlap_words=0
        )
        _, children_small = build_hierarchical_chunks(
            doc, [page], child_chunk_size_words=50, child_chunk_overlap_words=0
        )
        assert len(children_small) > len(children_default)
