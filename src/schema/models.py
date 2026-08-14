"""
Canonical schema for the Document Intelligence Copilot.

Every stage of the pipeline produces and consumes these typed models.
They are the single source of truth for data contracts between modules.

Data flow through the pipeline:

    File on disk
        │
        ▼
    RawDocument          — document-level metadata and provenance (1 per file)
    List[ParsedPage]     — page-level extracted content and quality signal
        │
        ▼
    List[ParsedBlock]    — layout blocks within a page (Phase 2)
        │
        ▼
    List[DocumentChunk]  — retrieval units produced by the chunker
        │
        ▼
    List[RetrievedChunk] — scored retrieval results
        │
        ▼
    List[CitationRecord] — deterministic citations with validation (Phase 7)
        │
        ▼
    AnswerResponse       — final pipeline output contract
    RunManifest          — pipeline run metadata for observability

Phase coverage notes:
    - Phase 1: RawDocument, ParsedPage, DocumentChunk are fully active.
    - Phase 2: ParsedBlock.bounding_box, ocr_confidence, ocr_engine populated.
    - Phase 3: DocumentChunk.parent_chunk_id, chunk_level=parent/child populated.
    - Phase 4: DocumentChunk.embedding_model, is_indexed populated.
    - Phase 5: RetrievedChunk scores populated.
    - Phase 6: AnswerResponse populated.
    - Phase 7: CitationRecord populated.
    - Phase 8: CitationRecord.validation_status, AnswerResponse.validation_flags.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


def _new_id() -> str:
    """Generate a unique hex ID."""
    return uuid.uuid4().hex


# --------------------------------------------------------------------------- #
# Document-level provenance                                                    #
# --------------------------------------------------------------------------- #


class RawDocument(BaseModel):
    """
    Metadata and provenance record for a single ingested file.

    This model carries no text content. Extracted text lives in ParsedPage.
    One RawDocument is created per file, regardless of page count.

    Fields:
        doc_id:       Unique identifier, generated at ingestion time.
        source_path:  Absolute path as a string, for serialisation safety.
        file_name:    Original file name including extension.
        file_type:    Lowercase extension without dot: "txt", "md", "pdf", "docx".
        ingested_at:  UTC timestamp of ingestion.
        byte_size:    File size in bytes.
        checksum:     SHA-256 hex digest of the raw file bytes.
        total_pages:  Page count, set after reading. None for single-unit formats.
    """

    doc_id: str = Field(default_factory=_new_id)
    source_path: str
    file_name: str
    file_type: str
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    byte_size: int
    checksum: str
    total_pages: Optional[int] = None


# --------------------------------------------------------------------------- #
# Page-level content                                                           #
# --------------------------------------------------------------------------- #


class ParsedBlock(BaseModel):
    """
    A layout-aware text block within a single page.

    Populated by the Docling/OCR parsing layer in Phase 2.
    In Phase 1, ParsedPage.layout_blocks is always an empty list.

    Fields:
        block_id:       Unique identifier for this block.
        doc_id:         Parent document identifier.
        page_number:    1-indexed page number within the document.
        block_type:     Semantic type of the block content.
        text:           Raw text content of the block.
        reading_order:  Integer position in reading order on the page.
        bounding_box:   [x0, y0, x1, y1] in page coordinate units (Phase 2).
        section_title:  Nearest ancestor heading, if detectable (Phase 2/3).
    """

    block_id: str = Field(default_factory=_new_id)
    doc_id: str
    page_number: int
    block_type: Literal[
        "heading", "paragraph", "table", "list", "caption", "unknown"
    ] = "unknown"
    text: str
    reading_order: int
    bounding_box: Optional[List[float]] = None
    section_title: Optional[str] = None


class ParsedPage(BaseModel):
    """
    Page-level extracted content with quality classification.

    One ParsedPage per PDF page; one ParsedPage (page_number=1) for
    flat text/md files. Produced by ingestion readers in Phase 1 and
    enriched with layout_blocks by the Docling parser in Phase 2.

    The extraction_status field is the primary routing signal for Phase 2:
        "ok"    — sufficient text for chunking and retrieval
        "weak"  — below threshold; may benefit from OCR
        "empty" — no usable text extracted; OCR required for this page

    Fields:
        page_id:           Unique identifier for this page record.
        doc_id:            Parent document identifier.
        page_number:       1-indexed page number within the document.
        raw_text:          Text as extracted, before any normalisation.
        normalized_text:   Cleaned and whitespace-normalised text.
        word_count:        Word count of normalized_text.
        char_count:        Character count of normalized_text.
        parse_method:      Which extractor produced this page's text.
        extraction_status: Quality signal for downstream OCR routing.
        ocr_confidence:    Mean OCR confidence score 0–1 (Phase 2).
        ocr_engine:        Name of the OCR engine used (Phase 2).
        section_title:     Section heading for this page (Phase 2/3).
        layout_blocks:     Ordered list of layout-aware blocks (Phase 2).
    """

    page_id: str = Field(default_factory=_new_id)
    doc_id: str
    page_number: int
    raw_text: str
    normalized_text: str
    word_count: int
    char_count: int
    parse_method: Literal[
        "pypdf",       # pypdf text-extraction pass (born-digital PDFs)
        "text_read",   # plain text / markdown reader
        "docling",     # Docling layout-aware extraction (.docx, .pdf direct)
        "rapidocr",    # Docling-routed RapidOCR recovery (scanned/empty PDF pages)
        "paddleocr",   # PaddleOCR standalone path (deferred)
        "azure_di",    # Azure AI Document Intelligence prebuilt-read
        "databricks_ai_parse_document"
    ] = "text_read"
    extraction_status: Literal["ok", "weak", "empty"] = "ok"
    ocr_confidence: Optional[float] = None
    ocr_engine: Optional[str] = None
    section_title: Optional[str] = None
    layout_blocks: List[ParsedBlock] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Chunking output                                                              #
# --------------------------------------------------------------------------- #


class DocumentChunk(BaseModel):
    """
    A retrieval unit produced by the chunking layer.

    chunk_level describes the chunk's role in the hierarchy:
        "flat"   — Phase 1 baseline; flat sliding-window chunk.
        "parent" — Phase 3; broader-context window for synthesis.
        "child"  — Phase 3; fine-grained unit for retrieval precision.

    parent_chunk_id is None for flat chunks and for parent-level chunks.
    embedding_model and is_indexed are set by the indexing layer (Phase 4).

    Fields:
        chunk_id:        Unique identifier for this chunk.
        doc_id:          Source document identifier.
        page_id:         Source ParsedPage identifier.
        page_number:     Source page number.
        file_name:       Source file name, for display and citation.
        file_type:       Source file type.
        section_title:   Section context, if available (Phase 2/3).
        text:            The chunk text content.
        word_count:      Word count of text.
        chunk_index:     Zero-indexed position within its page.
        chunk_level:     Hierarchy role of this chunk.
        parent_chunk_id: Identifier of the parent chunk (Phase 3).
        embedding_model: Model used to embed this chunk (Phase 4).
        is_indexed:      Whether this chunk has been added to the vector store.
    """

    chunk_id: str = Field(default_factory=_new_id)
    doc_id: str
    page_id: str
    page_number: int
    file_name: str
    file_type: str
    section_title: Optional[str] = None
    text: str
    word_count: int
    chunk_index: int
    chunk_level: Literal["flat", "parent", "child"] = "flat"
    parent_chunk_id: Optional[str] = None
    embedding_model: Optional[str] = None
    is_indexed: bool = False


# --------------------------------------------------------------------------- #
# Retrieval output                                                             #
# --------------------------------------------------------------------------- #


class RetrievedChunk(BaseModel):
    """
    A retrieved chunk enriched with scoring metadata from the retrieval layer.

    Score fields are Optional because different retrieval strategies populate
    different subsets. fusion_score is the normalised combined score used for
    final ranking. rerank_score is set by the Qwen3-Reranker (Phase 5).

    Fields:
        retrieval_method: Which retrieval path produced this result.
        vector_score:     Cosine or dot-product similarity score (Phase 4+).
        bm25_score:       BM25 lexical match score (Phase 5+).
        fusion_score:     Reciprocal rank fusion score (Phase 5+).
        rerank_score:     Reranker score after postprocessing (Phase 5+).
        parent_chunk_id:  chunk_id of the parent DocumentChunk; populated by
                          the vector retriever for use with lookup_parents().
                          None for flat chunks or when parent linkage is absent.
        file_type:        Lowercase file extension without dot (e.g. "pdf",
                          "txt", "docx"). Preserved from source chunk metadata
                          for downstream display and citation building.
    """

    chunk_id: str
    doc_id: str
    page_id: str
    file_name: str
    page_number: int
    section_title: Optional[str] = None
    text: str
    word_count: int
    retrieval_method: Literal["vector", "bm25", "hybrid"] = "vector"
    vector_score: Optional[float] = None
    bm25_score: Optional[float] = None
    fusion_score: Optional[float] = None
    rerank_score: Optional[float] = None
    parent_chunk_id: Optional[str] = None
    file_type: Optional[str] = None


# --------------------------------------------------------------------------- #
# Citation record                                                              #
# --------------------------------------------------------------------------- #


class CitationRecord(BaseModel):
    """
    A deterministic citation produced by the citation builder (Phase 7).

    is_verbatim is True only when quote_text appears verbatim in the source
    chunk text. validation_status is set by the rule-based validator (Phase 8).

    Fields:
        citation_id:      Unique identifier for this citation.
        source_chunk_id:  Chunk from which the quote was extracted.
        quote_start_char: Character offset of quote start in chunk text.
        quote_end_char:   Character offset of quote end in chunk text.
        is_verbatim:      Whether quote_text is a verbatim substring of source.
        validation_status: Rule-based validation result (Phase 8).
    """

    citation_id: str = Field(default_factory=_new_id)
    doc_id: str
    file_name: str
    page_number: int
    section_title: Optional[str] = None
    quote_text: str
    source_chunk_id: Optional[str] = None
    quote_start_char: Optional[int] = None
    quote_end_char: Optional[int] = None
    is_verbatim: bool = False
    validation_status: Literal["valid", "invalid", "unverified"] = "unverified"


# --------------------------------------------------------------------------- #
# Query routing                                                                #
# --------------------------------------------------------------------------- #

QueryType = Literal[
    "exact_lookup",
    "focused_question",
    "broad_summary",
    "comparison_or_multi_aspect",
    "insufficient_or_ambiguous",
]


class RoutingPlan(BaseModel):
    """
    Deterministic routing plan produced by the query router.

    Carries concrete retrieval parameters derived from query-type
    classification. Consumed by run_pipeline() to override its default
    top-k values and parent-context behaviour.

    Fields:
        query_type:               Classified query category.
        retrieval_top_k:          Candidate count for hybrid retrieval.
        rerank_top_k:             Survivor count after cross-encoder reranking.
        emphasize_parent_context: When True, parent chunks are passed into
                                  synthesis; when False, synthesis uses child
                                  text only (parents suppressed).
        notes:                    Plain-English strings explaining why this
                                  route was chosen — for logging and debugging.
    """

    query_type: QueryType
    retrieval_top_k: int
    rerank_top_k: int
    emphasize_parent_context: bool
    notes: List[str] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Answer response                                                              #
# --------------------------------------------------------------------------- #


class AnswerResponse(BaseModel):
    """
    Final output contract of the full pipeline (Phase 6+).

    Produced by the answer synthesis layer and enriched by the citation
    builder (Phase 7) and validator (Phase 8).

    Fields:
        run_id:            Unique identifier for this answer generation run.
        query:             The original user query.
        answer_text:       The generated answer text.
        model_used:        Identifier of the generation model used.
        sources:           Deterministic citations supporting the answer.
        supporting_chunks: Retrieved chunks used as context.
        validation_flags:  List of validation warning/error messages (Phase 8).
        latency_ms:        End-to-end generation latency in milliseconds.
    """

    run_id: str = Field(default_factory=_new_id)
    query: str
    answer_text: str
    model_used: str
    sources: List[CitationRecord] = Field(default_factory=list)
    supporting_chunks: List[RetrievedChunk] = Field(default_factory=list)
    validation_flags: List[str] = Field(default_factory=list)
    latency_ms: Optional[float] = None


# --------------------------------------------------------------------------- #
# Run manifest                                                                 #
# --------------------------------------------------------------------------- #


class RunManifest(BaseModel):
    """
    Pipeline run metadata for observability and artifact traceability.

    Created at the start of a pipeline run and updated on completion.
    Written to data/processed/manifests/ by the run_manifest module.

    Fields:
        run_id:              Unique identifier for this run.
        run_type:            Which pipeline stage was executed.
        started_at:          UTC start timestamp.
        completed_at:        UTC completion timestamp. None while running.
        index_version:       Version tag of the vector index (Phase 4+).
        doc_ids_processed:   List of doc_ids processed in this run.
        artifacts_written:   List of output file paths written.
        errors:              Non-fatal error messages encountered.
        status:              Current run status.
    """

    run_id: str = Field(default_factory=_new_id)
    run_type: Literal["ingest", "chunk", "index", "retrieve", "answer", "full"]
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None
    index_version: Optional[str] = None
    doc_ids_processed: List[str] = Field(default_factory=list)
    artifacts_written: List[str] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    status: Literal["running", "completed", "failed"] = "running"
