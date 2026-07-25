"""
Local KPI batch harness.

Pairs PDFs in a directory with KPI/query JSON specs, runs the existing
indexing + answer pipelines once per PDF, and writes one Excel workbook
per PDF into the results directory.

This module is a *testing harness only*. It does not modify any pipeline
behaviour. It calls the public service-layer functions
(`app.service.index_document`, `app.service.answer_query`) by default,
and both callables are injectable for tests.

Public API
----------
    discover_pairs(pdf_dir, json_dir) -> list[PdfKpiPair]
    load_kpi_spec(json_path) -> KpiSpec
    iter_active_rows(spec) -> Iterator[KpiRow]
    extract_row(pdf_name, kpi_row, response) -> ResultRow
    shorten_quote(text, max_len=300) -> str
    write_workbook(rows, out_path) -> Path
    run_batch(pdf_dir, json_dir, out_dir, *, index_doc=..., answer=...) -> list[Path]
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, List, Optional

from pydantic import BaseModel, Field

from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


class PdfKpiPair(BaseModel):
    """A PDF paired with its KPI/query JSON spec by exact stem."""

    stem: str
    pdf_path: Path
    json_path: Path


class KpiRow(BaseModel):
    """An active KPI/query row extracted from a JSON spec."""

    kpi_id: str
    kpi_label: str
    query: str
    template_id: str


class KpiSpec(BaseModel):
    """The active rows of a KPI/query JSON spec for one PDF."""

    stem: str
    rows: List[KpiRow] = Field(default_factory=list)


class ResultRow(BaseModel):
    """One row of the per-PDF results workbook."""

    pdf_name: str
    kpi_id: str
    kpi_label: str
    query: str
    summary: str
    source_quote: str
    page_number: str  # str so empty-string fallback fits the same column
    validation_status: str
    template_id: str
    timestamp: str


EXCEL_HEADERS: List[str] = [
    "pdf_name",
    "kpi_id",
    "kpi_label",
    "query",
    "summary",
    "source_quote",
    "page_number",
    "validation_status",
    "template_id",
    "timestamp",
]

QUOTE_MAX_LEN: int = 300


# ---------------------------------------------------------------------------
# Discovery and loading
# ---------------------------------------------------------------------------


def discover_pairs(pdf_dir: Path, json_dir: Path) -> List[PdfKpiPair]:
    """
    Pair PDFs by exact stem with JSONs in json_dir.

    PDFs without a matching JSON are skipped with a warning.
    """
    pdf_dir = Path(pdf_dir)
    json_dir = Path(json_dir)

    pairs: List[PdfKpiPair] = []
    for pdf_path in sorted(pdf_dir.glob("*.pdf")):
        json_path = json_dir / f"{pdf_path.stem}.json"
        if not json_path.exists():
            logger.warning(
                "kpi_batch.unpaired_pdf",
                pdf_path=str(pdf_path),
                expected_json=str(json_path),
            )
            continue
        pairs.append(
            PdfKpiPair(stem=pdf_path.stem, pdf_path=pdf_path, json_path=json_path)
        )
    return pairs


def load_kpi_spec(json_path: Path) -> KpiSpec:
    """
    Load and filter a KPI JSON spec, returning only active rows.

    Filters:
      - isActive must be true (defaults to true when absent)
      - query must be non-empty after strip()
      - empty KPI label falls back to the query text
    """
    json_path = Path(json_path)
    with json_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    kpis = raw.get("KPIs", {}) or {}
    queries = raw.get("queries", {}) or {}
    active = raw.get("isActive", {}) or {}
    mapping = raw.get("templates_mapping", {}) or {}

    # Stable ordering by integer interpretation of the key, falling back to
    # string sort for non-numeric keys.
    def _sort_key(k: str):
        try:
            return (0, int(k))
        except ValueError:
            return (1, k)

    ordered_keys = sorted(queries.keys(), key=_sort_key)

    rows: List[KpiRow] = []
    for kpi_id in ordered_keys:
        is_active = active.get(kpi_id, True)
        if not is_active:
            continue

        query = (queries.get(kpi_id) or "").strip()
        if not query:
            continue

        label = (kpis.get(kpi_id) or "").strip() or query
        template_id = str(mapping.get(kpi_id, "")).strip()

        rows.append(
            KpiRow(
                kpi_id=str(kpi_id),
                kpi_label=label,
                query=query,
                template_id=template_id,
            )
        )

    return KpiSpec(stem=json_path.stem, rows=rows)


def iter_active_rows(spec: KpiSpec) -> Iterator[KpiRow]:
    """Yield the active KPI rows from a spec in original order."""
    yield from spec.rows


# ---------------------------------------------------------------------------
# Row extraction
# ---------------------------------------------------------------------------


def shorten_quote(text: str, max_len: int = QUOTE_MAX_LEN) -> str:
    """
    Head-truncate a quote to max_len characters.

    The returned string is an exact substring of the input (no paraphrase).
    When truncation occurs, an ellipsis is appended to mark the cut. The
    ellipsis is appended *after* the substring, so the substring itself
    remains verbatim.
    """
    if text is None:
        return ""
    if len(text) <= max_len:
        return text
    return text[:max_len] + "..."


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def extract_row(pdf_name: str, kpi_row: KpiRow, response: Any) -> ResultRow:
    """
    Convert an AnswerResponse into a deterministic Excel row.

    Rules (locked):
      - summary           = response.answer_text
      - source_quote      = shorten_quote(response.sources[0].quote_text)
      - page_number       = str(response.sources[0].page_number)
      - validation_status = response.sources[0].validation_status
      - when sources is empty:
            source_quote      = ""
            page_number       = ""
            validation_status = "no_citation"
    """
    sources = getattr(response, "sources", None) or []
    if sources:
        first = sources[0]
        quote = shorten_quote(getattr(first, "quote_text", "") or "")
        page = getattr(first, "page_number", None)
        page_str = "" if page is None else str(page)
        status = getattr(first, "validation_status", "") or ""
    else:
        quote = ""
        page_str = ""
        status = "no_citation"

    return ResultRow(
        pdf_name=pdf_name,
        kpi_id=kpi_row.kpi_id,
        kpi_label=kpi_row.kpi_label,
        query=kpi_row.query,
        summary=getattr(response, "answer_text", "") or "",
        source_quote=quote,
        page_number=page_str,
        validation_status=status,
        template_id=kpi_row.template_id,
        timestamp=_utc_now_iso(),
    )


def _error_row(pdf_name: str, kpi_row: KpiRow, message: str) -> ResultRow:
    return ResultRow(
        pdf_name=pdf_name,
        kpi_id=kpi_row.kpi_id,
        kpi_label=kpi_row.kpi_label,
        query=kpi_row.query,
        summary=f"ERROR: {message}",
        source_quote="",
        page_number="",
        validation_status="error",
        template_id=kpi_row.template_id,
        timestamp=_utc_now_iso(),
    )


# ---------------------------------------------------------------------------
# Excel writer
# ---------------------------------------------------------------------------


def write_workbook(rows: List[ResultRow], out_path: Path) -> Path:
    """Write a single-sheet workbook ('results') with the locked header order."""
    from openpyxl import Workbook  # deferred — keeps module-level import light
    from openpyxl.styles import Font

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    wb = Workbook()
    ws = wb.active
    ws.title = "results"

    ws.append(EXCEL_HEADERS)
    header_font = Font(bold=True)
    for cell in ws[1]:
        cell.font = header_font

    for row in rows:
        ws.append([getattr(row, h) for h in EXCEL_HEADERS])

    ws.freeze_panes = "A2"

    # Readable default column widths.
    widths = {
        "A": 28, "B": 8, "C": 40, "D": 60, "E": 80,
        "F": 60, "G": 12, "H": 18, "I": 12, "J": 22,
    }
    for col, w in widths.items():
        ws.column_dimensions[col].width = w

    wb.save(out_path)
    return out_path


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


IndexCallable = Callable[..., Any]
AnswerCallable = Callable[..., Any]


def _default_index() -> IndexCallable:
    from app.service import index_document
    return index_document


def _default_answer() -> AnswerCallable:
    from app.service import answer_query
    return answer_query


def run_batch(
    pdf_dir: Path,
    json_dir: Path,
    out_dir: Path,
    *,
    index_doc: Optional[IndexCallable] = None,
    answer: Optional[AnswerCallable] = None,
) -> List[Path]:
    """
    Run the batch end-to-end and return the list of written workbook paths.

    For each paired PDF/JSON:
      1. index the PDF into out_dir/_indexes/<stem>/
      2. iterate active KPI rows
      3. call answer(query, index_dir=<stem>) per row
      4. extract a row from the AnswerResponse (or capture an error row)
      5. write <stem>_results.xlsx

    A failure on any single PDF or any single query is logged and recorded
    as an error row; the batch continues.

    Parameters
    ----------
    pdf_dir, json_dir, out_dir:
        Filesystem inputs and output target.
    index_doc, answer:
        Injectable callables matching the signatures of
        app.service.index_document / app.service.answer_query.
        Defaults bind lazily to the real service functions.
    """
    pdf_dir = Path(pdf_dir)
    json_dir = Path(json_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    index_doc = index_doc if index_doc is not None else _default_index()
    answer = answer if answer is not None else _default_answer()

    written: List[Path] = []
    pairs = discover_pairs(pdf_dir, json_dir)

    for pair in pairs:
        spec = load_kpi_spec(pair.json_path)
        index_dir = out_dir / "_indexes" / pair.stem
        pdf_name = pair.pdf_path.name
        rows: List[ResultRow] = []

        try:
            index_doc(pair.pdf_path, index_dir=index_dir)
        except Exception as exc:  # noqa: BLE001 — harness-level isolation
            logger.warning(
                "kpi_batch.index_failed", stem=pair.stem, error=str(exc)
            )
            for kpi_row in iter_active_rows(spec):
                rows.append(_error_row(pdf_name, kpi_row, str(exc)))
            out_path = out_dir / f"{pair.stem}_results.xlsx"
            written.append(write_workbook(rows, out_path))
            continue

        for kpi_row in iter_active_rows(spec):
            try:
                response = answer(kpi_row.query, index_dir=index_dir)
                rows.append(extract_row(pdf_name, kpi_row, response))
            except Exception as exc:  # noqa: BLE001 — per-row isolation
                logger.warning(
                    "kpi_batch.query_failed",
                    stem=pair.stem,
                    kpi_id=kpi_row.kpi_id,
                    error=str(exc),
                )
                rows.append(_error_row(pdf_name, kpi_row, str(exc)))

        out_path = out_dir / f"{pair.stem}_results.xlsx"
        written.append(write_workbook(rows, out_path))
        logger.info(
            "kpi_batch.pdf_done",
            stem=pair.stem,
            rows=len(rows),
            out_path=str(out_path),
        )

    return written
