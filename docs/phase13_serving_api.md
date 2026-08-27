@'
from pathlib import Path

path = Path(
    "docs/phase13_serving_api.md"
)

path.parent.mkdir(
    parents=True,
    exist_ok=True,
)

content = r'''# Phase 13 — Serving API and Databricks App

## Status

**Complete**

Phase 13 productionizes the query-serving layer of the Document
Intelligence Copilot.

The deployed application provides:

- FastAPI serving endpoints
- a Gradio query interface
- managed Databricks AI Search retrieval
- parent-context retrieval through Databricks SQL
- managed generation through a Databricks serving endpoint
- evidence-preserving citations
- readiness and health contracts
- Databricks App deployment
- application service-principal authentication

The deployed Databricks App is intentionally query-only. Document ingestion,
parsing, chunking, and indexing are handled outside the serving application.

---

## Architecture

```text
Client / Gradio UI
        |
        v
FastAPI
        |
        v
ServingService
        |
        +-----------------------------+
        |                             |
        v                             |
RetrievalRequest                      |
        |                             |
        v                             |
Databricks AI Search                  |
        |                             |
        v                             |
Authorized child evidence             |
        |                             |
        v                             |
Parent-context lookup                 |
Databricks SQL                        |
        |                             |
        +-------------+---------------+
                      |
                      v
              GenerationRequest
                      |
                      v
             DatabricksOpenAI
                      |
                      v
          Managed serving endpoint
                      |
                      v
               Grounded answer
                      |
                      v
         Exact CitationRecord payloads