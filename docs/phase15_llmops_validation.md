# Phase 15 — MLflow LLMOps, Evaluation, and Versioning

## Status

Phase 15 is complete.

This phase adds an MLflow-based LLMOps layer around the existing
Document Intelligence serving, retrieval, generation, and evaluation
contracts without replacing those production boundaries.

The implementation was validated locally and against live Databricks
resources.

---

## Scope

Phase 15 implements:

- LLMOps version identity
- MLflow experiment configuration and run metadata
- privacy-safe MLflow tracing
- canonical evaluation dataset integration
- deterministic evaluation scorers
- privacy-safe MLflow GenAI evaluation
- serving-boundary evaluation workflow
- explicit baseline-versus-candidate regression gating
- CLI entry points for evaluation and regression gates
- live Databricks validation

Production observability remains Phase 16.

---

## Version identity

Each full evaluation run records the relevant LLMOps identity:

- retrieval configuration version
- prompt contract version
- chunking contract version
- evaluation contract version
- generation model identity
- embedding model identity
- AI Search index identity
- evaluation dataset version
- Git code revision
- execution environment

Regression compatibility intentionally depends on evaluation identity,
not on requiring candidate and baseline runs to use identical runtime,
model, retrieval, index, or code revisions.

---

## Canonical evaluation dataset

Canonical evaluation collection:

`data/eval/canonical/evaluation_cases_v1.jsonl`

Corpus identity source:

`docs/baseline/corpus_manifest.csv`

Validated active case count:

`21`

The canonical adapter preserves:

- stable case identity
- canonical query
- expected document identity
- source dataset provenance
- dataset version
- case schema version

Reference answers are not fabricated when the source dataset does not
contain them.

---

## MLflow privacy boundary

Raw RAG content is not intentionally sent to MLflow evaluation.

The MLflow-safe projection contains only:

- evaluation case ordinal
- answer-present boolean
- per-evaluation HMAC document fingerprints
- evidence count
- citation count
- expected document fingerprint
- non-empty-answer expectation

The HMAC key is generated per evaluation and is not persisted.

The following values are excluded from the MLflow-safe evaluation
projection:

- raw query
- raw answer
- raw case ID
- raw document ID
- evidence text
- parent text
- citation text
- prompts and messages
- file names and page content
- credentials and tokens

---

## Deterministic evaluation metrics

The permanent deterministic MLflow metrics are:

- `answer_expectation_met/mean`
- `expected_document_fingerprint_hit/mean`
- `evidence_present/mean`
- `citation_present/mean`

The regression gate validates that required metrics are finite and
within the range `[0, 1]`.

Candidate runs are compared against an explicitly supplied baseline run.
The gate never silently selects a "latest" run.

---

## Tracing contract

The serving trace hierarchy contains:

`rag_request`
→ `retrieval`
→ `evidence_build`
→ `generation`
→ `prompt_build`
→ `llm_call`

Tracing records operational metadata rather than raw RAG payloads.

Examples include:

- runtime mode
- backend identity
- result counts
- evidence counts
- citation counts
- model identity
- latency
- version metadata

Raw query, answer, evidence, prompt, and response content are excluded
from the tracing contract.

---

## Databricks live validation

Live validation was performed using the Databricks runtime profile with:

- Databricks AI Search
- Unity Catalog parent chunk table
- SQL warehouse parent lookup
- Databricks Model Serving generation
- Databricks MLflow tracking

A one-case live smoke test passed end-to-end before the full evaluation.

The smoke test validated:

- live retrieval
- parent lookup
- managed generation
- serving response
- privacy-safe MLflow projection
- all four deterministic metrics
- one persisted evaluation trace
- persisted raw-value absence
- rejection of a partial smoke run by the promotion gate

Final result:

`PHASE15H1_ONE_CASE_LIVE_PASS: True`

---

## Full 21-case Databricks evaluation

The permanent Phase 15 serving-evaluation workflow was executed against
all 21 active canonical cases.

Execution result:

- evaluated cases: 21
- execution duration: 169.45 seconds
- MLflow traces: 21
- dataset version validation: passed
- version tag alignment: passed
- safe projection parameter validation: passed
- promotion snapshot validation: passed

Aggregate metrics:

| Metric | Result |
| --- | ---: |
| `answer_expectation_met/mean` | 1.0 |
| `expected_document_fingerprint_hit/mean` | 1.0 |
| `evidence_present/mean` | 1.0 |
| `citation_present/mean` | 1.0 |

Persistence checks passed for absence of:

- raw canonical case IDs
- raw canonical queries
- raw expected document IDs

Final result:

`PHASE15H2_FULL_LIVE_PASS: True`

The live MLflow run ID is intentionally not committed to the repository.

---

## Generation endpoint validation note

During live validation, generation initially returned `404` /
`ResourceDoesNotExist`.

The configured generation value was audited against the current
workspace Model Serving endpoint inventory and was found not to resolve
to an existing serving endpoint.

After configuring the actual Model Serving endpoint name:

- endpoint identity validation passed
- one-case live serving validation passed
- full 21-case evaluation passed

Endpoint names and other workspace-specific identifiers are intentionally
not recorded in this document.

---

## Final local regression

Final Phase 15 repository regression:

`1057 passed, 24 skipped, 3 warnings`

Result:

`FULL_REPOSITORY_REGRESSION_PASS: True`

The three warnings were non-blocking dependency/deprecation warnings and
did not cause test failures.

---

## Phase boundary

Phase 15 owns:

- MLflow LLMOps metadata
- version identity
- tracing contracts
- deterministic evaluation
- evaluation privacy projection
- regression gating
- live LLMOps validation

Phase 16 will add production observability and operational monitoring.
