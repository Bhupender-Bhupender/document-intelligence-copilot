# Phase 4 — Manifest-Driven Incremental Ingestion

## Status

Phase 4: Complete

## Objective

Implement deterministic, idempotent ingestion of unstructured documents
from a Unity Catalog managed landing volume into the Bronze control layer.

## Architecture

Source documents arrive in:

`bronze.document_landing/incoming`

Each supported document receives a SHA-256 fingerprint.

The fingerprint provides:

- stable document identity
- duplicate detection
- content-addressed archival
- protection against unnecessary downstream reprocessing

## Landing Lifecycle

incoming
? newly arrived files

archive
? immutable content-addressed source documents

quarantine
? unsupported source files

## Bronze Control Tables

### document_manifest

Tracks:

- stable document ID
- original filename
- archived source path
- file type
- byte size
- SHA-256 fingerprint
- ingestion batch
- ingestion state
- downstream processing state
- current document-version flag
- source metadata

### ingestion_runs

Tracks:

- pipeline run ID
- execution timestamps
- discovered files
- new files
- unchanged files
- quarantined files
- failed files
- execution status

## Idempotency Validation

The same document content was submitted more than once.

The second submission produced:

- zero new document records
- one unchanged record
- no duplicate manifest entry

This proves that downstream OCR, parsing, chunking, embedding and indexing
can later operate only on new or changed content.

## Repository Design

The ingestion implementation is owned by:

`databricks/src/bronze_ingestion.py`

The Databricks notebook acts only as the orchestration layer.

## Real Batch Validation

A small non-sensitive document batch was ingested through the managed
Unity Catalog volume.

All supported unique documents were:

- fingerprinted
- registered in the manifest
- archived using content-addressed paths
- assigned `processing_status = PENDING`

## Next

Phase 5 — Document Routing and Bronze-to-Silver Processing.
