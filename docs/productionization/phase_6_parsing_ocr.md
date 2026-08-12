## Status

Phase 6: Complete

## Objective

Reuse the existing document parsing architecture in Databricks, persist normalized page and layout data to Silver Delta tables, and introduce selective OCR recovery for pages where native extraction quality is insufficient.

## Native Extraction

The existing project parsing layer was validated against files stored in Unity Catalog Volumes.

Supported processing paths validated during this phase included:

* DOCX → Docling
* PDF → PyPDF
* TXT/Markdown → native text reader

Parser outputs were normalized into:

* `silver.documents`
* `silver.pages`
* `silver.blocks`

## Extraction Quality

Page quality is classified using word-count thresholds:

* `empty` — zero usable words
* `weak` — fewer than 20 words
* `good` — at least 20 words

Pages classified as weak or empty become OCR recovery candidates.

## Selective OCR Architecture

Local execution retains the existing Docling/RapidOCR recovery path.

The Databricks Free Edition runtime could not reliably initialize the full local Docling OCR stack for the test PDF, so Databricks managed document parsing was introduced as the Databricks-specific recovery adapter.

The final architecture is:

Native PDF extraction → quality assessment → selective managed recovery only for weak/empty pages.

Good native pages are never unnecessarily reprocessed.

## Recovery Acceptance Rule

Managed extraction replaces native extraction only when the recovered page contains more usable words than the existing native page.

This prevents OCR from degrading an already useful native extraction.

## Validation

Five documents entered extraction.

Initial extraction produced:

* 4 fully extracted documents
* 1 OCR-required document
* 0 failed documents
* 79 good pages
* 3 weak pages

The OCR-required PDF contained three weak pages.

Managed recovery was restricted to those three pages.

All three pages improved and crossed the quality threshold:

* 10 → 58 words
* 1 → 313 words
* 10 → 60 words

After recovery:

* all 82 pages passed extraction-quality requirements
* no weak pages remained
* all five documents reached `EXTRACTED`
* recovered pages retained explicit managed-extraction provenance
* a repeated recovery execution produced a no-op

## Idempotency

Only documents in `OCR_REQUIRED` state are eligible for recovery.

After all candidate pages are resolved, both Silver and Bronze states transition to `EXTRACTED`.

Subsequent runs find no eligible documents and perform no duplicate processing.

## Runtime Strategy

Local:

* PyPDF / Docling
* Docling / RapidOCR recovery

Databricks:

* native extraction first
* managed document parsing only for weak/empty pages

Future Azure enterprise architecture can additionally use Azure AI Document Intelligence through the existing OCR adapter boundary.

## Next

Phase 7 — Canonical Silver Validation and Data Quality Contracts.
