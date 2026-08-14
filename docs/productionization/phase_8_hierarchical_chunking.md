# Phase 8 — Hierarchical Chunking and Citation Anchors

## Status

Complete

## Objective

Transform validated Silver document content into deterministic hierarchical
retrieval units while preserving document, page, section, and parent-child
lineage required for grounded retrieval and citations.

## Input

- `silver.documents`
- `silver.pages`
- `silver.blocks`

Only current documents with `EXTRACTED` status are eligible.

## Architecture

Silver canonical content
→ Silver-to-project-native adapter
→ existing hierarchical chunker
→ parent chunks
→ child chunks
→ Gold Delta tables
→ chunking manifest

The existing local hierarchical chunking implementation remains the
canonical chunking engine. Databricks provides persistence and execution.

## Chunking Configuration

- Parent chunk size: 400 words
- Child chunk size: 150 words
- Child overlap: 30 words
- Chunking version: `hierarchical_v1`

## Structured Chunking

Silver layout blocks are reconstructed into the project-native
`ParsedBlock` and `ParsedPage` models.

Block types from the Databricks canonical layer are normalized to the
project-native block vocabulary before chunking.

The chunker prefers structured block-based chunking when layout information
exists and falls back to normalized page text when structure is unavailable.

## Hierarchy

Parent chunks:

- `chunk_level = parent`
- `parent_chunk_id = NULL`

Child chunks:

- `chunk_level = child`
- `parent_chunk_id` references the corresponding parent chunk

Every child was validated to reference an existing parent.

## Citation Lineage

Every persisted chunk retains:

- document ID
- page ID
- page number
- section title when available

Gold chunk page references were validated against `silver.pages`.

## Gold Tables

- `gold.parent_chunks`
- `gold.child_chunks`
- `gold.chunking_manifest`

The manifest records:

- source SHA-256
- chunking version
- parent chunk size
- child chunk size
- overlap
- parent/child counts
- processing status

## Idempotency

Chunk processing is keyed by:

- document identity
- source SHA-256
- chunking version
- chunking configuration

A repeated execution without source or configuration changes produced:

- documents discovered: 5
- documents processed: 0
- documents unchanged: 5
- documents failed: 0
- parent chunks written: 0
- child chunks written: 0
- status: NOOP

## Validation

The Phase 8 validation confirmed:

- unique parent chunk IDs
- unique child chunk IDs
- valid parent-child references
- valid parent and child hierarchy semantics
- non-empty chunks
- valid citation anchors
- Gold-to-Silver page lineage
- manifest-to-Gold count reconciliation
- deterministic chunk IDs
- idempotent reruns

## Outcome

The canonical retrieval corpus is now persisted in Gold and ready for
embedding generation.

## Next

Phase 9 — Embeddings and Vectorization.