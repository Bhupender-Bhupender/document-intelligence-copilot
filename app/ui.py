"""
Local Gradio Blocks application for the Document Intelligence Copilot.

Public entry point
------------------
    build_ui() -> gr.Blocks
        Build and return the Gradio Blocks app.  The caller is responsible for
        calling ``demo.launch()`` when ready to serve.

        Example::

            from app.ui import build_ui

            demo = build_ui()
            demo.launch()

Internal helpers (importable for testing without Gradio)
---------------------------------------------------------
    _handle_index(file_path)          Event handler — indexing tab.
    _handle_answer(...)               Event handler — question tab.
    _format_index_result(manifest)    Format IndexManifest for display.
    _format_citations(response)       Format citation list for display.
    _format_flags(response)           Format validation flags for display.

Import policy (hard rule)
-------------------------
This module must stay lightweight at import time.

* ``from __future__ import annotations`` defers all annotation evaluation so
  that type hints in function signatures are never executed at runtime.
* Heavy project types (``IndexManifest``, ``AnswerResponse``) are imported
  only under ``TYPE_CHECKING`` — they are never reachable at runtime.
* ``import gradio as gr`` is lazy — executed only inside ``build_ui()``.
* The only runtime imports are ``app.service`` symbols, which are themselves
  lightweight (no LlamaIndex, Ollama, or Torch at import time).

File-input contract
-------------------
The indexing tab uses ``gr.File(..., type="filepath")``.  Gradio guarantees
that the handler receives either a plain ``str`` path or ``None``.
``_handle_index`` is typed and implemented against that explicit contract;
no duck-typing or version-dependent file-object handling is used.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Tuple

from app.service import ServiceError, answer_query, index_document

if TYPE_CHECKING:
    from src.indexing.index_builder import IndexManifest
    from src.schema.models import AnswerResponse


# ---------------------------------------------------------------------------
# Format helpers — pure functions, no Gradio dependency, directly testable
# ---------------------------------------------------------------------------


def _format_index_result(manifest: IndexManifest) -> str:
    """Render an IndexManifest as a human-readable status string."""
    doc_list = ", ".join(manifest.doc_ids) if manifest.doc_ids else "(none)"
    return (
        "Indexed successfully\n"
        f"  Parents indexed : {manifest.parent_count}\n"
        f"  Chunks indexed  : {manifest.child_count}\n"
        f"  Documents       : {doc_list}\n"
        f"  Embedding model : {manifest.embedding_model}\n"
        f"  Run ID          : {manifest.run_id}"
    )


def _format_citations(response: AnswerResponse) -> str:
    """Render the citation list from an AnswerResponse as a human-readable string."""
    if not response.sources:
        return "(No citations)"
    lines: list[str] = []
    for i, citation in enumerate(response.sources, 1):
        status = citation.validation_status.upper()
        section_line = (
            f"  Section: {citation.section_title}\n" if citation.section_title else ""
        )
        preview = citation.quote_text[:120]
        if len(citation.quote_text) > 120:
            preview += "\u2026"  # horizontal ellipsis
        lines.append(
            f"[{i}] {citation.file_name} \u2014 page {citation.page_number} [{status}]\n"
            f"{section_line}"
            f'  "{preview}"'
        )
    return "\n\n".join(lines)


def _format_flags(response: AnswerResponse) -> str:
    """Render validation flags from an AnswerResponse as a human-readable string."""
    if not response.validation_flags:
        return "No validation issues."
    return "\n".join(f"\u2022 {flag}" for flag in response.validation_flags)


# ---------------------------------------------------------------------------
# Event handlers — call the service layer, surface ServiceError cleanly
# ---------------------------------------------------------------------------


def _handle_index(file_path: Optional[str]) -> str:
    """
    Handle a file-upload event from the indexing tab.

    ``file_path`` is the value produced by ``gr.File(..., type="filepath")``:
    a plain ``str`` path when a file is selected, or ``None`` when the widget
    is empty.  No duck-typing or version-dependent handling is performed.
    """
    if file_path is None:
        return "No file selected. Please upload a document."
    try:
        manifest = index_document(file_path)
        return _format_index_result(manifest)
    except ServiceError as exc:
        return f"Indexing failed: {exc}"
    except Exception as exc:  # noqa: BLE001
        return f"Unexpected error during indexing: {exc}"


def _handle_answer(
    query: str,
    retrieval_top_k: int,
    rerank_top_k: int,
    model: str,
) -> Tuple[str, str, str]:
    """
    Handle a query submission from the question tab.

    Returns a three-tuple of plain strings: ``(answer_text, citations, flags)``.
    On error the error message occupies the first element and the remaining
    two elements are empty strings — no raw tracebacks are ever surfaced.
    """
    model_val: Optional[str] = model.strip() if model.strip() else None
    try:
        response = answer_query(
            query,
            retrieval_top_k=int(retrieval_top_k),
            rerank_top_k=int(rerank_top_k),
            model=model_val,
        )
        return (
            response.answer_text,
            _format_citations(response),
            _format_flags(response),
        )
    except ServiceError as exc:
        return (f"Query failed: {exc}", "", "")
    except Exception as exc:  # noqa: BLE001
        return (f"Unexpected error: {exc}", "", "")


# ---------------------------------------------------------------------------
# UI builder — lazy Gradio import
# ---------------------------------------------------------------------------


def build_ui():  # return type is gr.Blocks; annotated lazily to stay import-light
    """
    Build and return the Gradio Blocks application.

    ``import gradio as gr`` is executed here and only here so that importing
    ``app.ui`` at test time does not pull in the Gradio runtime.

    Two tabs are provided:

    * **Index Document** — file upload (``type="filepath"``) → ``index_document()``
      → concise status summary.
    * **Ask a Question** — query text box → ``answer_query()`` → answer text,
      citation list, and validation flags rendered in separate output boxes.
    """
    import gradio as gr  # lazy — only runs when the UI is actually constructed

    with gr.Blocks(title="Document Intelligence Copilot") as demo:
        gr.Markdown(
            "# Document Intelligence Copilot\n"
            "Local RAG pipeline with hybrid retrieval and grounded answer synthesis."
        )

        # ------------------------------------------------------------------ #
        # Tab 1: Index Document                                               #
        # ------------------------------------------------------------------ #
        with gr.Tab("Index Document"):
            gr.Markdown("Upload a document to add it to the retrieval index.")
            index_file = gr.File(
                label="Select document",
                file_types=[".txt", ".md", ".pdf", ".docx"],
                type="filepath",
            )
            index_btn = gr.Button("Index Document", variant="primary")
            index_status = gr.Textbox(
                label="Indexing result",
                interactive=False,
                lines=6,
            )
            index_btn.click(
                fn=_handle_index,
                inputs=[index_file],
                outputs=[index_status],
            )

        # ------------------------------------------------------------------ #
        # Tab 2: Ask a Question                                               #
        # ------------------------------------------------------------------ #
        with gr.Tab("Ask a Question"):
            gr.Markdown("Ask a question against your indexed documents.")
            query_input = gr.Textbox(
                label="Question",
                placeholder="What does the document say about\u2026",
                lines=2,
            )

            with gr.Accordion("Advanced options", open=False):
                retrieval_top_k = gr.Slider(
                    minimum=5,
                    maximum=30,
                    value=10,
                    step=1,
                    label="Retrieval top-k",
                )
                rerank_top_k = gr.Slider(
                    minimum=1,
                    maximum=15,
                    value=5,
                    step=1,
                    label="Rerank top-k",
                )
                model_input = gr.Textbox(
                    label="Model override",
                    placeholder="Leave empty to use the configured default",
                    value="",
                )

            ask_btn = gr.Button("Ask", variant="primary")
            answer_output = gr.Textbox(
                label="Answer",
                interactive=False,
                lines=8,
            )
            citations_output = gr.Textbox(
                label="Citations",
                interactive=False,
                lines=8,
            )
            flags_output = gr.Textbox(
                label="Validation flags",
                interactive=False,
                lines=3,
            )

            ask_btn.click(
                fn=_handle_answer,
                inputs=[query_input, retrieval_top_k, rerank_top_k, model_input],
                outputs=[answer_output, citations_output, flags_output],
            )

    return demo
