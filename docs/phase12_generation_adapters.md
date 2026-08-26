# Phase 12 — Generation Adapters and Provider Comparison

Status: Complete

## Objective

Decouple retrieval from generation so the same retrieval evidence contract can
be consumed by either a local model or a managed Databricks model without
changing ingestion, indexing, or search.

Architecture:

Databricks AI Search
    -> Phase 11 RetrievalResponse / RetrievalEvidence[]
        -> Local Ollama / Qwen3-8B
        -> Databricks managed model service

The generator receives only the original question, retrieved evidence,
citation payloads, and response rules. It has no unrestricted direct access
to lakehouse tables.

## Implementation

Phase 12 added:

- Evidence-oriented generation request/response contracts.
- Evidence prompt construction using child text as authoritative evidence.
- Parent text as contextual background only.
- Existing citation payloads reused rather than rebuilt independently.
- Local Qwen3-8B generation through the same evidence contract.
- Databricks managed-generation adapter using the Unity AI Gateway
  OpenAI-compatible chat interface.
- Managed model selected through configuration/environment rather than
  hardcoding the provider model into retrieval or ingestion code.
- Local AI Search authentication support using transient Databricks
  credentials, while preserving Databricks-runtime authentication behavior.
- Local SQL Statement Execution fallback for parent lookup outside the
  Databricks notebook runtime.
- Controlled local-versus-managed comparison harnesses.

## Development and Production Authentication

Current development mode uses user-to-machine OAuth credentials obtained
through the Databricks CLI and stored only in transient environment variables.

No credential values are committed.

Enterprise target state is machine-to-machine OAuth with a Databricks service
principal / workload identity and least-privilege permissions.

## Phase 12D Managed Generation Validation

Managed Databricks generation passed:

- Backend configured: true
- Managed model configured: true
- Non-empty answer: true
- Prompt token usage available: true
- Completion token usage available: true
- Total token usage available: true
- Finish reason available: true
- Managed smoke pass: true

The controlled managed comparison used:

system.ai.qwen3-next-80b-a3b-instruct

## Phase 12E-1 Same-Evidence Acceptance

One retrieval produced three RetrievalEvidence objects.

The exact same evidence objects were supplied to:

- Local Ollama / Qwen3-8B
- Databricks managed generation

Results:

- Retrieval calls: 1
- Retrieval results: 3
- Parent context available: true
- Local answer non-empty: true
- Managed answer non-empty: true
- Exact same evidence: true
- Exact same citation payloads: true
- Source counts equal: true
- Managed token metadata available: true
- Same-evidence acceptance pass: true

This satisfies the principal Phase 12 architectural checkpoint.

## Phase 12E-2 Controlled Provider Comparison

Cases: 3

Architecture and evidence integrity:

- Retrieval calls: 3
- Local generation calls: 3
- Managed generation calls: 3
- Same evidence rate: 1.0
- Same citation payload rate: 1.0
- Local source-payload consistency: 1.0
- Managed source-payload consistency: 1.0

Generation behavior:

- Local inline citation rate: 0.0
- Managed inline citation rate: 1.0

The inline-citation difference is model behavior only. Structured citation
payload integrity remained 1.0 for both providers.

Mean latency:

- Retrieval: 16133.91 ms
- Local generation: 60994.87 ms
- Managed generation: 10020.05 ms

Semantic evaluation using the same local Qwen3-8B judge:

- Local groundedness: 1.0000
- Managed groundedness: 0.9333
- Local answer relevance: 1.0000
- Managed answer relevance: 1.0000
- Local completeness: 1.0000
- Managed completeness: 0.9667
- Local judge parse failures: 0
- Managed judge parse failures: 0

Managed generation token usage:

- Mean total tokens per comparison case: 2023.33
- Total tokens across three cases: 6070

Local token usage is unavailable because the current Ollama adapter returns
answer text only.

No currency cost was fabricated for this Free Edition development run.

## Interpretation

The comparison demonstrates provider interchangeability, not model superiority.

The semantic scores must not be interpreted as proof that one model is more
accurate because:

1. Only three controlled examples were used.
2. Qwen3-8B judged both providers and may have model-family/self-evaluation bias.
3. The current evaluation contract has no reference expected answer.
4. Therefore no gold-answer correctness score is claimed.

A later evaluation/LLMOps phase should add reference-answer scoring using the
planned question-answer gold set.

## Regression Evidence

Final Phase 12 repository regression:

830 passed
24 skipped
3 warnings

git diff --check: PASS

## Free Edition Limitation

The current implementation is validated in Databricks Free Edition.

Free Edition is appropriate for functional and architectural validation but is
not the enterprise target state. Model use is subject to Free Edition fair-use
and quota constraints.

Production target state includes stronger identity, networking, governance,
observability, workload scheduling, and deployment controls.

## Phase 12 Decision

Phase 12 accepted.

Local and managed generators successfully consume the same evidence contract,
and ingestion/search code remains independent of the generation provider.

Next phase: Phase 13 — Serving, application, and API layer.
