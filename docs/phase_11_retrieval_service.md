# Phase 11 — Retrieval Service

## Status

PASS

## Objective

Expose retrieval through a stable service contract that returns filtered,
citation-ready evidence rather than backend-specific search results.

## Architecture

RetrievalRequest
→ filter validation
→ retrieval gateway
→ Databricks AI Search HYBRID retrieval
→ document allow-list validation
→ final_k selection
→ optional parent-context lookup
→ citation-ready evidence construction
→ RetrievalResponse

The existing project retrieval gateway remains the backend-selection boundary.
No Databricks SDK object crosses the service contract.

## Stable request contract

Phase 11 introduces:

- RetrievalRequest
- RetrievalDateRange
- RetrievalEvidence
- RetrievalResponse

The request supports:

- query
- document_ids
- tenant_id
- allowed_groups
- date_range
- top_k
- final_k
- include_parent_context

## Current filter capabilities

### Implemented

document_ids

The document allow-list is pushed into Databricks AI Search using metadata
filtering before evidence is returned.

The service also performs a defense-in-depth validation that every returned
chunk belongs to the requested document allow-list.

### Present in the contract but not currently enforceable

tenant_id
allowed_groups
date_range

The current Gold retrieval corpus does not contain the required tenant,
access-group, or date metadata.

These filters therefore fail closed instead of being silently ignored.

Full access-control metadata propagation and enterprise authorization remain
a later security/governance phase.

## Citation semantics

Retrieved child chunks remain the authoritative citation anchors.

For every RetrievalEvidence result:

- chunk_id identifies the matched child chunk.
- document_id identifies the source document.
- page_start and page_end are anchored to the matched child page.
- citation_payload is generated from the matched child.
- parent_text is optional contextual expansion only.
- parent context never expands or changes the citation page.
- parent/child document and chunk lineage are validated.

This protects against wrong-page citation expansion when a larger parent
context spans unrelated material.

## Ranking behavior

The configured retrieval backend determines the ranking strategy.

For Databricks:

- AI Search HYBRID retrieval supplies the ranked child results.
- top_k controls backend retrieval depth.
- final_k controls the final evidence count.
- Phase 11 does not introduce a second duplicate reranking layer.

## Zero-result behavior

A successful AI Search query with zero matches is a valid retrieval outcome.

The Databricks adapter distinguishes:

valid response + zero matches
→ []

malformed response
→ DatabricksSearchRetrievalError

Strict validation remains enabled for non-empty responses, including:

- required response columns
- manifest/row-width consistency
- integral page numbers
- child-chunk semantics
- parent lineage

## Local validation

Targeted Phase 11 tests:

33 passed
0 failed

Full repository regression:

807 passed
24 skipped
0 failed
3 dependency warnings

## Live Databricks validation

Validated on the Phase 11 branch through the reusable retrieval service.

Results:

PHASE11_ALLOWED_RESULT_COUNT: 3
PHASE11_ALLOWED_DOCUMENT_ONLY: True
PHASE11_CITATIONS_ALIGNED: True
PHASE11_PARENT_CONTEXT_PRESENT: True
PHASE11_FILTER_RECORDED: True
PHASE11_BLOCKED_RESULT_COUNT: 0
PHASE11_NO_UNAUTHORIZED_LEAKAGE: True
PHASE11_STABLE_RESPONSE: True
PHASE11C_LIVE_PASS: True

No private query text, retrieved document text, filenames, or document
identifiers are stored in this evidence document.

## Known limitations

The development Gold corpus does not yet expose:

- tenant_id
- access_groups
- published_date
- document_version_id
- is_current

Therefore Phase 11 does not claim full tenant or group authorization.

index_version is currently left unset rather than performing an additional
index-description network request for every query.

Latency is captured by the service contract, but performance tuning and
capacity claims are deferred to the dedicated performance phase.

## Acceptance decision

PASS

Phase 11 checkpoint achieved:

Filtered, citation-ready evidence is returned through a stable contract.

The implementation is ready to serve as the evidence input boundary for
Phase 12 generation.
